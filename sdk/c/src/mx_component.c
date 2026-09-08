/* Matrix C SDK: component session (framing, reader, calls, dispatcher). */
#define _POSIX_C_SOURCE 200809L

#include "mx_component.h"
#include "mx_json.h"

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

const char *mx_strerror(mx_status_t st) {
    switch (st) {
    case MX_OK:
        return "ok";
    case MX_ERR_TRANSPORT:
        return "transport error";
    case MX_ERR_PROTOCOL:
        return "protocol error";
    case MX_ERR_TIMEOUT:
        return "timeout (outcome unknown, never retried)";
    case MX_ERR_CANCELLED:
        return "cancelled";
    case MX_ERR_DENIED:
        return "refused (wire code attached)";
    case MX_ERR_UNKNOWN:
        return "outcome-unknown";
    case MX_ERR_UNSUPPORTED:
        return "unsupported-feature (local refusal, wire untouched)";
    case MX_ERR_INVALID:
        return "invalid argument";
    case MX_ERR_THREAD:
        return "blocking call on reader/dispatcher thread";
    case MX_ERR_CLOSED:
        return "component closed";
    case MX_ERR_NOMEM:
        return "out of memory";
    default:
        return "unknown status";
    }
}

void mx_free(void *p) {
    free(p);
}

/* -- framing --------------------------------------------------------- */

typedef struct {
    int fd;
    pthread_mutex_t wmu;
    size_t max_frame;
} transport_t;

static int send_all(int fd, const void *buf, size_t n) {
    const char *p = (const char *)buf;
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (w == 0)
            return -1;
        p += w;
        n -= (size_t)w;
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t n) {
    char *p = (char *)buf;
    while (n) {
        ssize_t r = read(fd, p, n);
        if (r < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (r == 0)
            return 1; /* EOF */
        p += r;
        n -= (size_t)r;
    }
    return 0;
}

/* Frame with parsed DOM (single parse, handed over). */
/* NOTE: read_frame above double-parses; the real path below parses
 * once and hands the DOM over. */
typedef struct {
    char *raw;   /* NUL-terminated payload (owned) */
    mxj_t *dom;  /* parsed DOM (owned) */
} frame_t;

static void frame_free(frame_t *f) {
    if (!f)
        return;
    free(f->raw);
    mxj_free(f->dom);
    free(f);
}

static int read_frame_dom(int fd, size_t max_frame, frame_t **out) {
    unsigned char hdr[4];
    int rc = recv_all(fd, hdr, 4);
    if (rc != 0)
        return rc == 1 ? -1 : -2;
    uint32_t n = ((uint32_t)hdr[0] << 24) | ((uint32_t)hdr[1] << 16) |
                 ((uint32_t)hdr[2] << 8) | hdr[3];
    if (n == 0 || n > max_frame)
        return -2;
    char *buf = (char *)malloc(n + 1);
    if (!buf)
        return -2;
    rc = recv_all(fd, buf, n);
    if (rc != 0) {
        free(buf);
        return rc == 1 ? -1 : -2;
    }
    buf[n] = '\0';
    frame_t *f = NULL;
    if (!mxj_valid_utf8(buf, n)) {
        free(buf);
        return 0;
    }
    mxj_t *dom = mxj_parse(buf);
    if (!dom) {
        free(buf);
        return 0;
    }
    f = (frame_t *)calloc(1, sizeof(*f));
    if (!f) {
        free(buf);
        mxj_free(dom);
        return -2;
    }
    f->raw = buf;
    f->dom = dom;
    *out = f;
    return 1;
}

static char *xstrdup(const char *s) {
    if (!s)
        return NULL;
    size_t n = strlen(s) + 1;
    char *c = (char *)malloc(n);
    if (c)
        memcpy(c, s, n);
    return c;
}

/* Builds {"protocol":..,"version":..,"type":..,"message_id":..,
 * "session_id":..,"instance_id":..,"generation":..,
 * ["request_id":..,]"body":<body_raw>} with proper escaping. */
static char *build_envelope(const char *type, const char *mid,
                            const char *session, const char *instance,
                            const char *generation, const char *rid,
                            const char *body_raw) {
    char *q_type = mxj_quote(type, strlen(type));
    char *q_mid = mxj_quote(mid, strlen(mid));
    char *q_sess = mxj_quote(session, strlen(session));
    char *q_inst = mxj_quote(instance, strlen(instance));
    char *q_gen = mxj_quote(generation, strlen(generation));
    char *q_rid = rid ? mxj_quote(rid, strlen(rid)) : NULL;
    if (!q_type || !q_mid || !q_sess || !q_inst || !q_gen || (rid && !q_rid)) {
        free(q_type);
        free(q_mid);
        free(q_sess);
        free(q_inst);
        free(q_gen);
        free(q_rid);
        return NULL;
    }
    const char *fmt = rid ? "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
                            "\"type\":%s,\"message_id\":%s,\"session_id\":%s,"
                            "\"instance_id\":%s,\"generation\":%s,\"request_id\":%s,"
                            "\"body\":%s}"
                          : "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
                            "\"type\":%s,\"message_id\":%s,\"session_id\":%s,"
                            "\"instance_id\":%s,\"generation\":%s,"
                            "\"body\":%s}";
    int n;
    if (rid)
        n = snprintf(NULL, 0, fmt, q_type, q_mid, q_sess, q_inst, q_gen,
                     q_rid, body_raw ? body_raw : "null");
    else
        n = snprintf(NULL, 0, fmt, q_type, q_mid, q_sess, q_inst, q_gen,
                     body_raw ? body_raw : "null");
    if (n < 0) {
        free(q_type);
        free(q_mid);
        free(q_sess);
        free(q_inst);
        free(q_gen);
        free(q_rid);
        return NULL;
    }
    char *out = (char *)malloc((size_t)n + 1);
    if (out) {
        if (rid)
            snprintf(out, (size_t)n + 1, fmt, q_type, q_mid, q_sess,
                     q_inst, q_gen, q_rid, body_raw ? body_raw : "null");
        else
            snprintf(out, (size_t)n + 1, fmt, q_type, q_mid, q_sess,
                     q_inst, q_gen, body_raw ? body_raw : "null");
    }
    free(q_type);
    free(q_mid);
    free(q_sess);
    free(q_inst);
    free(q_gen);
    free(q_rid);
    return out;
}

static int send_envelope(transport_t *t, const char *type, const char *mid,
                         const char *session, const char *instance,
                         const char *generation, const char *rid,
                         const char *body_raw) {
    char *doc = build_envelope(type, mid, session, instance, generation,
                               rid, body_raw);
    if (!doc)
        return -1;
    size_t n = strlen(doc);
    int rc = -1;
    if (n <= t->max_frame) {
        unsigned char hdr[4];
        hdr[0] = (unsigned char)((n >> 24) & 0xFF);
        hdr[1] = (unsigned char)((n >> 16) & 0xFF);
        hdr[2] = (unsigned char)((n >> 8) & 0xFF);
        hdr[3] = (unsigned char)(n & 0xFF);
        pthread_mutex_lock(&t->wmu);
        if (send_all(t->fd, hdr, 4) == 0 && send_all(t->fd, doc, n) == 0)
            rc = 0;
        pthread_mutex_unlock(&t->wmu);
    }
    free(doc);
    return rc;
}

/* -- component ------------------------------------------------------- */

typedef struct waiter {
    char *rid;
    int done;
    int is_dep; /* 1 dep, 0 resource */
    char *output_json; /* dep ok: raw output; res ok: raw extra object */
    char *code, *msg;  /* error */
    pthread_cond_t cond;
    struct waiter *next;
} waiter_t;

typedef struct callrec {
    char *ticket;
    int *flag; /* single shared cancel storage for record+ctx+job */
    pthread_mutex_t *mu;
    struct callrec *next;
} callrec_t;

typedef struct evitem {
    int is_stream;
    char *topic;    /* event */
    char *payload;  /* event: raw JSON */
    char *stream_id;/* stream */
    unsigned long long seq;
    char *text;     /* stream payload */
    size_t text_len;
    struct evitem *next;
} evitem_t;

struct mx_call_ctx {
    mx_component_t *comp;
    char *ticket;
    int *cancel_flag; /* points into the call record (borrowed) */
    pthread_mutex_t *cancel_mu;
};

struct mx_component {
    int fd;
    transport_t tp;
    char session[128];
    char instance[128];
    char generation[32];
    size_t max_frame;
    char **features;
    size_t nfeatures;
    mx_binding_t *bindings;
    size_t nbindings;
    mx_handler_t handler;

    pthread_mutex_t mu; /* waiters + calls */
    waiter_t *waiters;
    callrec_t *calls;

    pthread_mutex_t evmu;
    pthread_cond_t evcond;
    evitem_t *evhead, *evtail;
    size_t evlen;
    unsigned long long evdropped;
    int ev_stop;

    pthread_t reader_tid;
    pthread_t disp_tid;
    int reader_set, disp_set;

    pthread_mutex_t donemu;
    pthread_cond_t donecond;
    int done;
    const char *reason; /* static */

    unsigned long long seq;
    pthread_mutex_t seqmu;
};

static unsigned long long next_seq(mx_component_t *c, const char *prefix,
                                   char *buf, size_t cap) {
    (void)prefix;
    pthread_mutex_lock(&c->seqmu);
    unsigned long long n = ++c->seq;
    pthread_mutex_unlock(&c->seqmu);
    snprintf(buf, cap, "%llu", n);
    return n;
}

static int has_feature(mx_component_t *c, const char *f) {
    for (size_t i = 0; i < c->nfeatures; i++)
        if (!strcmp(c->features[i], f))
            return 1;
    return 0;
}

static int on_special_thread(mx_component_t *c) {
    pthread_t self = pthread_self();
    if (c->reader_set && pthread_equal(self, c->reader_tid))
        return 1;
    if (c->disp_set && pthread_equal(self, c->disp_tid))
        return 1;
    return 0;
}

/* -- waiters --------------------------------------------------------- */

static waiter_t *waiter_add(mx_component_t *c, const char *rid, int is_dep) {
    waiter_t *w = (waiter_t *)calloc(1, sizeof(*w));
    if (!w)
        return NULL;
    w->rid = xstrdup(rid);
    w->is_dep = is_dep;
    /* The waiter deadline uses CLOCK_MONOTONIC throughout
     * (waiter_wait builds a monotonic abstime): the condvar must run
     * on the same clock, otherwise every slice expires instantly and
     * the "wait" degrades into a millisecond spin that only wins
     * against in-process hosts by luck. */
    pthread_condattr_t wattr;
    pthread_condattr_init(&wattr);
    pthread_condattr_setclock(&wattr, CLOCK_MONOTONIC);
    int wrc = pthread_cond_init(&w->cond, &wattr);
    pthread_condattr_destroy(&wattr);
    if (!w->rid || wrc != 0) {
        free(w->rid);
        free(w);
        return NULL;
    }
    pthread_mutex_lock(&c->mu);
    w->next = c->waiters;
    c->waiters = w;
    pthread_mutex_unlock(&c->mu);
    return w;
}

static waiter_t *waiter_take(mx_component_t *c, const char *rid) {
    waiter_t *w = NULL;
    pthread_mutex_lock(&c->mu);
    waiter_t **pp = &c->waiters;
    while (*pp) {
        if (!strcmp((*pp)->rid, rid)) {
            w = *pp;
            *pp = w->next;
            w->next = NULL;
            break;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&c->mu);
    return w;
}

static void waiter_free(waiter_t *w) {
    if (!w)
        return;
    free(w->rid);
    free(w->output_json);
    free(w->code);
    free(w->msg);
    pthread_cond_destroy(&w->cond);
    free(w);
}

/* Waits until signalled, cancelled via *cancel_flag (may be NULL),
 * or timeout_ms expires. Polls in 50ms slices so cancel/deadline
 * preempt the wait. Returns 1 signalled, 0 expired, -1 cancelled. */
static int waiter_wait(mx_component_t *c, waiter_t *w,
                       unsigned long long timeout_ms, int *cancel_flag,
                       pthread_mutex_t *cancel_mu) {
    unsigned long long waited = 0;
    pthread_mutex_lock(&c->mu);
    for (;;) {
        if (w->done) {
            pthread_mutex_unlock(&c->mu);
            return 1;
        }
        if (cancel_flag && cancel_mu) {
            pthread_mutex_lock(cancel_mu);
            int cancelled = *cancel_flag;
            pthread_mutex_unlock(cancel_mu);
            if (cancelled) {
                pthread_mutex_unlock(&c->mu);
                return -1;
            }
        }
        if (waited >= timeout_ms) {
            pthread_mutex_unlock(&c->mu);
            return 0;
        }
        unsigned long long slice = timeout_ms - waited;
        if (slice > 50)
            slice = 50;
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        unsigned long long ns = (unsigned long long)ts.tv_sec * 1000000000ull +
                                (unsigned long long)ts.tv_nsec + slice * 1000000ull;
        ts.tv_sec = (time_t)(ns / 1000000000ull);
        ts.tv_nsec = (long)(ns % 1000000000ull);
        pthread_cond_timedwait(&w->cond, &c->mu, &ts);
        waited += slice;
    }
}

/* -- call context ---------------------------------------------------- */

int mx_cancelled(mx_call_ctx_t *ctx) {
    int v = 0;
    if (!ctx)
        return 0;
    pthread_mutex_lock(ctx->cancel_mu);
    v = *ctx->cancel_flag;
    pthread_mutex_unlock(ctx->cancel_mu);
    return v;
}

const char *mx_ticket(mx_call_ctx_t *ctx) {
    return ctx ? ctx->ticket : "";
}

const mx_binding_t *mx_dependencies(mx_call_ctx_t *ctx, size_t *out_n) {
    if (!ctx)
        return NULL;
    if (out_n)
        *out_n = ctx->comp->nbindings;
    return ctx->comp->bindings;
}

unsigned long long mx_event_dropped_count(mx_call_ctx_t *ctx) {
    unsigned long long v = 0;
    if (!ctx)
        return 0;
    pthread_mutex_lock(&ctx->comp->evmu);
    v = ctx->comp->evdropped;
    pthread_mutex_unlock(&ctx->comp->evmu);
    return v;
}

size_t mx_pending_stream_count(mx_call_ctx_t *ctx) {
    size_t n = 0;
    if (!ctx)
        return 0;
    pthread_mutex_lock(&ctx->comp->evmu);
    for (evitem_t *it = ctx->comp->evhead; it; it = it->next)
        if (it->is_stream)
            n++;
    pthread_mutex_unlock(&ctx->comp->evmu);
    return n;
}

size_t mx_features(const mx_component_t *c, const char ***out) {
    if (!c)
        return 0;
    if (out)
        *out = (const char **)c->features;
    return c->nfeatures;
}

mx_status_t mx_invoke_dependency(mx_call_ctx_t *ctx, const char *binding_id,
                                 const char *input_json,
                                 unsigned long long timeout_ms,
                                 char **output_json_out, char **code_out,
                                 char **msg_out) {
    if (output_json_out)
        *output_json_out = NULL;
    if (code_out)
        *code_out = NULL;
    if (msg_out)
        *msg_out = NULL;
    if (!ctx || !binding_id || !input_json)
        return MX_ERR_INVALID;
    mx_component_t *c = ctx->comp;
    if (!has_feature(c, MX_DEPENDENCY_CALLS_FEATURE))
        return MX_ERR_UNSUPPORTED;
    if (timeout_ms == 0)
        return MX_ERR_INVALID;
    if (on_special_thread(c))
        return MX_ERR_THREAD;
    char rid[64], mbuf[64];
    next_seq(c, "r-dep", rid, sizeof(rid));
    next_seq(c, "m-dep", mbuf, sizeof(mbuf));
    waiter_t *w = waiter_add(c, rid, 1);
    if (!w)
        return MX_ERR_NOMEM;
    /* body: {"parent_ticket":..,"binding_id":..,"timeout_ms":N,"input":..};
     * timeout_ms is a JSON number (reference SDKs send int). */
    char *q_ticket = mxj_quote(ctx->ticket, strlen(ctx->ticket));
    char *q_bind = mxj_quote(binding_id, strlen(binding_id));
    char *body = NULL;
    if (q_ticket && q_bind) {
        size_t n = strlen(q_ticket) + strlen(q_bind) + strlen(input_json) + 128;
        body = (char *)malloc(n);
        if (body)
            snprintf(body, n,
                     "{\"parent_ticket\":%s,\"binding_id\":%s,"
                     "\"timeout_ms\":%llu,\"input\":%s}",
                     q_ticket, q_bind, timeout_ms, input_json);
    }
    free(q_ticket);
    free(q_bind);
    mx_status_t st = MX_ERR_NOMEM;
    if (body) {
        if (send_envelope(&c->tp, "dependency.open", mbuf, c->session,
                          c->instance, c->generation, rid, body) == 0) {
            /* Local deadline = request + transport slack; expiry
             * cancels on the wire (never retries). */
            unsigned long long budget = timeout_ms + 10000;
            int r = waiter_wait(c, w, budget, ctx->cancel_flag,
                                ctx->cancel_mu);
            if (r == 1) {
                if (w->output_json) {
                    if (output_json_out)
                        *output_json_out = w->output_json;
                    else
                        free(w->output_json);
                    w->output_json = NULL;
                    st = MX_OK;
                } else {
                    if (code_out)
                        *code_out = w->code;
                    else
                        free(w->code);
                    if (msg_out)
                        *msg_out = w->msg;
                    else
                        free(w->msg);
                    w->code = w->msg = NULL;
                    st = MX_ERR_DENIED;
                }
            } else if (r == -1) {
                /* parent cancelled wins: cancel on the wire. */
                char cm[64], cr[64];
                next_seq(c, "m-dep-cancel", cm, sizeof(cm));
                next_seq(c, "r-dep-cancel", cr, sizeof(cr));
                char *q_t = mxj_quote(rid, strlen(rid));
                if (q_t) {
                    char cb[128];
                    snprintf(cb, sizeof(cb),
                             "{\"target_request_id\":%s}", q_t);
                    send_envelope(&c->tp, "dependency.cancel", cm,
                                  c->session, c->instance, c->generation,
                                  cr, cb);
                    free(q_t);
                }
                if (code_out)
                    *code_out = xstrdup("cancelled");
                if (msg_out)
                    *msg_out = xstrdup("parent cancelled");
                st = MX_ERR_CANCELLED;
            } else {
                char cm[64], cr[64];
                next_seq(c, "m-dep-cancel", cm, sizeof(cm));
                next_seq(c, "r-dep-cancel", cr, sizeof(cr));
                char *q_t = mxj_quote(rid, strlen(rid));
                if (q_t) {
                    char cb[128];
                    snprintf(cb, sizeof(cb),
                             "{\"target_request_id\":%s}", q_t);
                    send_envelope(&c->tp, "dependency.cancel", cm,
                                  c->session, c->instance, c->generation,
                                  cr, cb);
                    free(q_t);
                }
                if (code_out)
                    *code_out = xstrdup("outcome-unknown");
                if (msg_out)
                    *msg_out = xstrdup("sdk wait timeout");
                st = MX_ERR_UNKNOWN;
            }
        } else {
            st = MX_ERR_TRANSPORT;
        }
        free(body);
    }
    waiter_take(c, rid);
    waiter_free(w);
    return st;
}

mx_status_t mx_send_stream(mx_call_ctx_t *ctx, const char *stream_id,
                           unsigned long long seq, const char *payload,
                           size_t payload_len) {
    if (!ctx || !stream_id || !*stream_id || (!payload && payload_len))
        return MX_ERR_INVALID;
    if (payload_len && !mxj_valid_utf8(payload, payload_len))
        return MX_ERR_INVALID; /* binary refused, never lossy-converted */
    if (payload_len && memchr(payload, '\0', payload_len))
        return MX_ERR_INVALID; /* JSON text has no NULs */
    mx_component_t *c = ctx->comp;
    char mbuf[64];
    next_seq(c, "m", mbuf, sizeof(mbuf));
    char *q_sid = mxj_quote(stream_id, strlen(stream_id));
    char *q_pay = mxj_quote(payload ? payload : "", payload_len);
    char seqbuf[32];
    snprintf(seqbuf, sizeof(seqbuf), "%llu", seq);
    char *q_seq = mxj_quote(seqbuf, strlen(seqbuf));
    char *body = NULL;
    if (q_sid && q_pay && q_seq) {
        size_t n = strlen(q_sid) + strlen(q_pay) + strlen(q_seq) + 64;
        body = (char *)malloc(n);
        if (body)
            snprintf(body, n,
                     "{\"stream_id\":%s,\"seq\":%s,\"payload\":%s}",
                     q_sid, q_seq, q_pay);
    }
    free(q_sid);
    free(q_pay);
    free(q_seq);
    if (!body)
        return MX_ERR_NOMEM;
    int rc = send_envelope(&c->tp, "stream.data", mbuf, c->session,
                           c->instance, c->generation, NULL, body);
    free(body);
    return rc == 0 ? MX_OK : MX_ERR_TRANSPORT;
}

static mx_status_t resource_roundtrip(mx_call_ctx_t *ctx,
                                      const char *operation,
                                      const char *fields_json,
                                      char **extra_out, char **code_out,
                                      char **msg_out) {
    mx_component_t *c = ctx->comp;
    char rid[64], mbuf[64], opid[64];
    next_seq(c, "r-res", rid, sizeof(rid));
    next_seq(c, "m-res", mbuf, sizeof(mbuf));
    next_seq(c, "op-res", opid, sizeof(opid));
    waiter_t *w = waiter_add(c, rid, 0);
    if (!w)
        return MX_ERR_NOMEM;
    char *q_opid = mxj_quote(opid, strlen(opid));
    char *body = NULL;
    if (q_opid && fields_json) {
        size_t n = strlen(q_opid) + strlen(fields_json) + 64;
        body = (char *)malloc(n);
        if (body)
            snprintf(body, n, "{\"operation_id\":%s,%s}", q_opid,
                     fields_json);
    }
    free(q_opid);
    mx_status_t st = MX_ERR_NOMEM;
    if (body) {
        char type[32];
        snprintf(type, sizeof(type), "resource.%s", operation);
        if (send_envelope(&c->tp, type, mbuf, c->session, c->instance,
                          c->generation, rid, body) == 0) {
            int r = waiter_wait(c, w, 10000, ctx->cancel_flag,
                                ctx->cancel_mu);
            if (r == 1) {
                if (w->output_json) {
                    if (extra_out)
                        *extra_out = w->output_json;
                    else
                        free(w->output_json);
                    w->output_json = NULL;
                    st = MX_OK;
                } else {
                    if (code_out)
                        *code_out = w->code;
                    else
                        free(w->code);
                    if (msg_out)
                        *msg_out = w->msg;
                    else
                        free(w->msg);
                    w->code = w->msg = NULL;
                    st = MX_ERR_DENIED;
                }
            } else if (r == -1) {
                if (code_out)
                    *code_out = xstrdup("cancelled");
                if (msg_out)
                    *msg_out = xstrdup("parent cancelled");
                st = MX_ERR_CANCELLED;
            } else {
                if (code_out)
                    *code_out = xstrdup("outcome-unknown");
                if (msg_out)
                    *msg_out = xstrdup("resource wait timeout");
                st = MX_ERR_UNKNOWN;
            }
        } else {
            st = MX_ERR_TRANSPORT;
        }
        free(body);
    }
    waiter_take(c, rid);
    waiter_free(w);
    return st;
}

mx_status_t mx_acquire_resource(mx_call_ctx_t *ctx, const char *kind,
                                const char *label, int has_interval_ms,
                                unsigned long long interval_ms,
                                unsigned long long *handle_out,
                                char **code_out, char **msg_out) {
    if (code_out)
        *code_out = NULL;
    if (msg_out)
        *msg_out = NULL;
    if (!ctx || !kind || !label)
        return MX_ERR_INVALID;
    if (on_special_thread(ctx->comp))
        return MX_ERR_THREAD;
    char *q_kind = mxj_quote(kind, strlen(kind));
    char *q_label = mxj_quote(label, strlen(label));
    char fields[512];
    if (!q_kind || !q_label) {
        free(q_kind);
        free(q_label);
        return MX_ERR_NOMEM;
    }
    if (has_interval_ms)
        snprintf(fields, sizeof(fields),
                 "\"kind\":%s,\"label\":%s,\"interval_ms\":%llu", q_kind,
                 q_label, interval_ms);
    /* interval_ms is a JSON number (reference SDKs send int). */
    else
        snprintf(fields, sizeof(fields), "\"kind\":%s,\"label\":%s",
                 q_kind, q_label);
    free(q_kind);
    free(q_label);
    char *extra = NULL;
    mx_status_t st = resource_roundtrip(ctx, "acquire", fields, &extra,
                                        code_out, msg_out);
    if (st == MX_OK) {
        mxj_t *dom = extra ? mxj_parse(extra) : NULL;
        const char *h = dom ? mxj_str(mxj_field(dom, "handle")) : NULL;
        int ok = 0;
        unsigned long long n = h ? mxj_parse_u64(h, &ok) : 0;
        if (!ok) {
            free(extra);
            mxj_free(dom);
            return MX_ERR_PROTOCOL;
        }
        if (handle_out)
            *handle_out = n;
        free(extra);
        mxj_free(dom);
    }
    return st;
}

mx_status_t mx_release_resource(mx_call_ctx_t *ctx, unsigned long long handle,
                                char **code_out, char **msg_out) {
    if (code_out)
        *code_out = NULL;
    if (msg_out)
        *msg_out = NULL;
    if (!ctx)
        return MX_ERR_INVALID;
    if (on_special_thread(ctx->comp))
        return MX_ERR_THREAD;
    char fields[128];
    snprintf(fields, sizeof(fields), "\"handle\":\"%llu\"", handle);
    return resource_roundtrip(ctx, "release", fields, NULL, code_out,
                              msg_out);
}

/* -- events ---------------------------------------------------------- */

static void ev_free(evitem_t *it) {
    if (!it)
        return;
    free(it->topic);
    free(it->payload);
    free(it->stream_id);
    free(it->text);
    free(it);
}

static void enqueue_event(mx_component_t *c, evitem_t *it) {
    pthread_mutex_lock(&c->evmu);
    if (c->evlen >= MX_EVENT_CAP) {
        evitem_t *old = c->evhead;
        if (old) {
            c->evhead = old->next;
            if (!c->evhead)
                c->evtail = NULL;
            ev_free(old);
            c->evlen--;
        }
        c->evdropped++;
    }
    it->next = NULL;
    if (c->evtail)
        c->evtail->next = it;
    else
        c->evhead = it;
    c->evtail = it;
    c->evlen++;
    pthread_cond_signal(&c->evcond);
    pthread_mutex_unlock(&c->evmu);
}

static void *dispatcher_main(void *arg) {
    mx_component_t *c = (mx_component_t *)arg;
    c->disp_tid = pthread_self();
    c->disp_set = 1;
    for (;;) {
        pthread_mutex_lock(&c->evmu);
        while (!c->evhead && !c->ev_stop)
            pthread_cond_wait(&c->evcond, &c->evmu);
        if (c->ev_stop && !c->evhead) {
            pthread_mutex_unlock(&c->evmu);
            break;
        }
        evitem_t *it = c->evhead;
        if (it) {
            c->evhead = it->next;
            if (!c->evhead)
                c->evtail = NULL;
            c->evlen--;
        }
        pthread_mutex_unlock(&c->evmu);
        if (!it)
            continue;
        /* Handler bugs must not kill the session: no unchecked
         * unwinding across the callback in C, just call it. */
        if (it->is_stream) {
            if (c->handler.on_stream)
                c->handler.on_stream(it->stream_id, it->seq, it->text,
                                     it->text_len, c->handler.userdata);
        } else {
            if (c->handler.on_event)
                c->handler.on_event(it->topic, it->payload,
                                    c->handler.userdata);
        }
        ev_free(it);
    }
    return NULL;
}

/* -- calls ----------------------------------------------------------- */

typedef struct {
    mx_component_t *comp;
    mx_call_ctx_t *ctx;
    char *ticket;
    char *cap;
    char *input_json;
    char *open_rid; /* may be NULL */
    int *cancel_flag;
    pthread_mutex_t *cancel_mu;
    callrec_t *rec;
} calljob_t;

static void complete_call(mx_component_t *c, const char *ticket,
                          const char *open_rid, const char *output_json,
                          const char *err_code, const char *err_msg) {
    char mbuf[96];
    snprintf(mbuf, sizeof(mbuf), "m-call-%s", ticket);
    char *q_ticket = mxj_quote(ticket, strlen(ticket));
    char *body = NULL;
    if (q_ticket) {
        if (output_json) {
            size_t n = strlen(q_ticket) + strlen(output_json) + 64;
            body = (char *)malloc(n);
            if (body)
                snprintf(body, n, "{\"ticket\":%s,\"status\":\"ok\","
                                 "\"output\":%s}",
                         q_ticket, output_json);
        } else {
            char *q_code = mxj_quote(err_code ? err_code : "internal",
                                     strlen(err_code ? err_code : "internal"));
            char *q_msg = mxj_quote(err_msg ? err_msg : "remote error",
                                    strlen(err_msg ? err_msg : "remote error"));
            if (q_code && q_msg) {
                size_t n = strlen(q_ticket) + strlen(q_code) +
                           strlen(q_msg) + 96;
                body = (char *)malloc(n);
                if (body)
                    snprintf(body, n, "{\"ticket\":%s,\"status\":\"error\","
                                     "\"error\":{\"code\":%s,\"message\":%s}}",
                             q_ticket, q_code, q_msg);
            }
            free(q_code);
            free(q_msg);
        }
        free(q_ticket);
    }
    if (body) {
        send_envelope(&c->tp, "call.result", mbuf, c->session,
                      c->instance, c->generation, open_rid, body);
        free(body);
    }
}

static void *call_main(void *arg) {
    calljob_t *job = (calljob_t *)arg;
    mx_component_t *c = job->comp;
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    if (c->handler.on_call)
        r = c->handler.on_call(job->ctx, job->ticket, job->cap,
                               job->input_json, c->handler.userdata);
    else {
        r.err_code = xstrdup("internal");
        r.err_msg = xstrdup("no handler");
    }
    /* drop the call record (idempotent wrt cancel) */
    pthread_mutex_lock(&c->mu);
    callrec_t **pp = &c->calls;
    while (*pp) {
        if (*pp == job->rec) {
            *pp = job->rec->next;
            break;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&c->mu);
    /* late after cancel: stays silent (no false success) */
    pthread_mutex_lock(job->cancel_mu);
    int cancelled = job->cancel_flag ? *job->cancel_flag : 0;
    pthread_mutex_unlock(job->cancel_mu);
    if (!cancelled) {
        if (r.output_json)
            complete_call(c, job->ticket, job->open_rid, r.output_json,
                          NULL, NULL);
        else
            complete_call(c, job->ticket, job->open_rid, NULL,
                          r.err_code ? r.err_code : "internal",
                          r.err_msg ? r.err_msg : "handler failed");
    }
    free(r.output_json);
    free(r.err_code);
    free(r.err_msg);
    free(job->ticket);
    free(job->cap);
    free(job->input_json);
    free(job->open_rid);
    /* shared cancel storage dies with the call (last owner) */
    pthread_mutex_destroy(job->cancel_mu);
    free(job->cancel_mu);
    free(job->cancel_flag);
    free(job->rec->ticket);
    free(job->rec);
    free(job->ctx->ticket);
    free(job->ctx);
    free(job);
    return NULL;
}

/* -- reader ---------------------------------------------------------- */

static int bound_ok(mx_component_t *c, const mxj_t *env) {
    const mxj_t *s = mxj_field(env, "session_id");
    const mxj_t *i = mxj_field(env, "instance_id");
    const mxj_t *g = mxj_field(env, "generation");
    if (!s || s->type != MXJ_STR || strcmp(s->str, c->session))
        return 0;
    if (i && i->type == MXJ_STR && strcmp(i->str, c->instance))
        return 0;
    if (g && g->type == MXJ_STR) {
        int ok = 0;
        unsigned long long a = mxj_parse_u64(g->str, &ok);
        if (!ok)
            return 0;
        int ok2 = 0;
        unsigned long long b = mxj_parse_u64(c->generation, &ok2);
        if (!ok2 || a != b)
            return 0;
    }
    return 1;
}

static void reply_lifecycle(mx_component_t *c, const mxj_t *body,
                            const char *request_id) {
    const mxj_t *op = body ? mxj_field(body, "operation_id") : NULL;
    char *op_raw = op ? mxj_print(op) : NULL;
    char mbuf[64];
    next_seq(c, "m-lc", mbuf, sizeof(mbuf));
    char fixed[256];
    snprintf(fixed, sizeof(fixed),
             "{\"operation_id\":%s,\"status\":\"ok\",\"pending\":[]}",
             op_raw ? op_raw : "\"op?\"");
    free(op_raw);
    send_envelope(&c->tp, "lifecycle.result", mbuf, c->session,
                  c->instance, c->generation, request_id, fixed);
}

static void route_dep_result(mx_component_t *c, const mxj_t *body,
                             const char *request_id) {
    if (!request_id)
        return;
    waiter_t *w = waiter_take(c, request_id);
    if (!w)
        return; /* late answer without a waiter: drop */
    const mxj_t *st = mxj_field(body, "status");
    const char *status = (st && st->type == MXJ_STR) ? st->str : "";
    pthread_mutex_lock(&c->mu);
    if (!strcmp(status, "ok")) {
        const mxj_t *out = mxj_field(body, "output");
        char *raw = out ? mxj_print(out) : NULL;
        w->output_json = raw ? raw : xstrdup("null");
    } else {
        const mxj_t *err = mxj_field(body, "error");
        const mxj_t *code = err ? mxj_field(err, "code") : NULL;
        const mxj_t *msg = err ? mxj_field(err, "message") : NULL;
        w->code = xstrdup((code && code->type == MXJ_STR && code->str) ?
                              code->str :
                              "internal");
        w->msg = xstrdup((msg && msg->type == MXJ_STR && msg->str) ?
                             msg->str :
                             "remote error");
    }
    w->done = 1;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&c->mu);
}

static void route_res_result(mx_component_t *c, const mxj_t *body,
                             const char *request_id) {
    if (!request_id)
        return;
    waiter_t *w = waiter_take(c, request_id);
    if (!w)
        return;
    const mxj_t *st = mxj_field(body, "status");
    const char *status = (st && st->type == MXJ_STR) ? st->str : "";
    pthread_mutex_lock(&c->mu);
    if (!strcmp(status, "ok")) {
        /* forward every field but operation_id/status as the extra map */
        size_t cap = 128, len = 1;
        char *extra = (char *)malloc(cap);
        int first = 1;
#define EMIT(s_, n_)                                                          \
    do {                                                                  \
        while (len + (n_) + 2 > cap) {                                \
            cap *= 2;                                             \
            char *nb = (char *)realloc(extra, cap);               \
            if (!nb) {                                            \
                free(extra);                                      \
                extra = NULL;                                     \
                break;                                            \
            }                                                     \
            extra = nb;                                           \
        }                                                         \
        if (!extra)                                               \
            break;                                                \
        memcpy(extra + len, (s_), (n_));                          \
        len += (n_);                                              \
        extra[len] = '\0';                                           \
    } while (0)
        if (extra) {
            extra[0] = '{';
            extra[1] = '\0';
            for (mxj_kv_t *kv = body->type == MXJ_OBJ ? body->obj : NULL;
                 kv && extra; kv = kv->next) {
                if (!strcmp(kv->key, "operation_id") || !strcmp(kv->key, "status"))
                    continue;
                char *q_k = mxj_quote(kv->key, strlen(kv->key));
                char *v_raw = mxj_print(kv->val);
                if (!q_k || !v_raw) {
                    free(q_k);
                    free(v_raw);
                    continue;
                }
                if (!first)
                    EMIT(",", 1);
                first = 0;
                EMIT(q_k, strlen(q_k));
                EMIT(":", 1);
                EMIT(v_raw, strlen(v_raw));
                free(q_k);
                free(v_raw);
            }
            if (extra)
                EMIT("}", 1);
        }
#undef EMIT
        w->output_json = extra ? extra : xstrdup("{}");
    } else {
        const mxj_t *code = mxj_field(body, "code");
        const mxj_t *msg = mxj_field(body, "message");
        w->code = xstrdup((code && code->type == MXJ_STR && code->str) ?
                              code->str :
                              "internal");
        w->msg = xstrdup((msg && msg->type == MXJ_STR && msg->str) ?
                             msg->str :
                             "remote error");
    }
    w->done = 1;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&c->mu);
}

static void *reader_main(void *arg) {
    mx_component_t *c = (mx_component_t *)arg;
    c->reader_tid = pthread_self();
    c->reader_set = 1;
    const char *reason = "eof";
    for (;;) {
        frame_t *f = NULL;
        int rc = read_frame_dom(c->fd, c->max_frame, &f);
        if (rc == -1) {
            reason = "eof";
            break;
        }
        if (rc == -2) {
            reason = "eof";
            break;
        }
        if (rc == 0)
            continue; /* malformed: dropped silently */
        const mxj_t *env = f->dom;
        if (!bound_ok(c, env)) {
            frame_free(f);
            continue;
        }
        const mxj_t *type_v = mxj_field(env, "type");
        const mxj_t *body = mxj_field(env, "body");
        const mxj_t *rid_v = mxj_field(env, "request_id");
        const char *type = (type_v && type_v->type == MXJ_STR) ? type_v->str : "";
        const char *rid =
            (rid_v && rid_v->type == MXJ_STR) ? rid_v->str : NULL;
        if (!strcmp(type, "lifecycle.prepare") ||
            !strcmp(type, "lifecycle.activate") ||
            !strcmp(type, "lifecycle.quiesce")) {
            reply_lifecycle(c, body, rid);
        } else if (!strcmp(type, "lifecycle.dispose")) {
            reply_lifecycle(c, body, rid);
            frame_free(f);
            reason = "dispose";
            break;
        } else if (!strcmp(type, "call.open")) {
            const mxj_t *t = body ? mxj_field(body, "ticket") : NULL;
            const mxj_t *cp = body ? mxj_field(body, "capability") : NULL;
            const mxj_t *in = body ? mxj_field(body, "input") : NULL;
            calljob_t *job = (calljob_t *)calloc(1, sizeof(*job));
            callrec_t *rec = (callrec_t *)calloc(1, sizeof(*rec));
            int *flag = (int *)calloc(1, sizeof(int));
            pthread_mutex_t *cmu =
                (pthread_mutex_t *)malloc(sizeof(*cmu));
            mx_call_ctx_t *ctx =
                (mx_call_ctx_t *)calloc(1, sizeof(*ctx));
            char *in_raw = in ? mxj_print(in) : NULL;
            if (!job || !rec || !flag || !cmu || !ctx || !in_raw) {
                free(job);
                free(rec);
                free(flag);
                free(cmu);
                free(ctx);
                free(in_raw);
                frame_free(f);
                continue;
            }
            pthread_mutex_init(cmu, NULL);
            /* one flag+mutex shared by record, ctx and job */
            rec->ticket = NULL;
            rec->flag = flag;
            rec->mu = cmu;
            job->comp = c;
            job->ctx = ctx;
            job->ticket =
                xstrdup((t && t->type == MXJ_STR && t->str) ? t->str : "");
            job->cap =
                xstrdup((cp && cp->type == MXJ_STR && cp->str) ? cp->str : "");
            job->input_json = in_raw;
            job->open_rid = rid ? xstrdup(rid) : NULL;
            job->cancel_flag = flag;
            job->cancel_mu = cmu;
            job->rec = rec;
            rec->ticket = xstrdup(job->ticket);
            ctx->comp = c;
            ctx->ticket = xstrdup(job->ticket);
            ctx->cancel_flag = flag;
            ctx->cancel_mu = cmu;
            /* snapshot bindings count only; handles are session-stable */
            pthread_mutex_lock(&c->mu);
            rec->next = c->calls;
            c->calls = rec;
            pthread_mutex_unlock(&c->mu);
            pthread_t th;
            pthread_attr_t at;
            pthread_attr_init(&at);
            pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
            if (pthread_create(&th, &at, call_main, job) != 0) {
                pthread_mutex_lock(&c->mu);
                callrec_t **pp = &c->calls;
                while (*pp) {
                    if (*pp == rec) {
                        *pp = rec->next;
                        break;
                    }
                    pp = &(*pp)->next;
                }
                pthread_mutex_unlock(&c->mu);
                free(job->ticket);
                free(job->cap);
                free(job->input_json);
                free(job->open_rid);
                pthread_mutex_destroy(cmu);
                free(cmu);
                free(flag);
                free(rec->ticket);
                free(rec);
                free(ctx->ticket);
                free(ctx);
                free(job);
            }
            /* NOTE: on success the call thread owns job/rec/ctx. */
            pthread_attr_destroy(&at);
        } else if (!strcmp(type, "call.cancel")) {
            const mxj_t *t = body ? mxj_field(body, "ticket") : NULL;
            const char *ticket =
                (t && t->type == MXJ_STR && t->str) ? t->str : "";
            pthread_mutex_lock(&c->mu);
            for (callrec_t *r = c->calls; r; r = r->next) {
                if (!strcmp(r->ticket, ticket)) {
                    pthread_mutex_lock(r->mu);
                    *r->flag = 1;
                    pthread_mutex_unlock(r->mu);
                }
            }
            pthread_mutex_unlock(&c->mu);
            if (c->handler.on_cancel)
                c->handler.on_cancel(ticket, c->handler.userdata);
        } else if (!strcmp(type, "dependency.result")) {
            route_dep_result(c, body, rid);
        } else if (!strcmp(type, "dependency.accepted") ||
                   !strcmp(type, "dependency.cancel.result")) {
            /* progress only; no answer needed */
        } else if (!strcmp(type, "resource.result")) {
            route_res_result(c, body, rid);
        } else if (!strcmp(type, "event.deliver")) {
            const mxj_t *t = body ? mxj_field(body, "topic") : NULL;
            const char *topic =
                (t && t->type == MXJ_STR && t->str) ? t->str : "";
            if (*topic) {
                const mxj_t *pl = body ? mxj_field(body, "payload") : NULL;
                char *praw = pl ? mxj_print(pl) : NULL;
                evitem_t *it = (evitem_t *)calloc(1, sizeof(*it));
                if (it && praw) {
                    it->topic = xstrdup(topic);
                    it->payload = praw;
                    if (it->topic)
                        enqueue_event(c, it);
                    else {
                        free(it->topic);
                        free(it->payload);
                        free(it);
                    }
                } else {
                    free(praw);
                    free(it);
                }
            }
        } else if (!strcmp(type, "stream.data")) {
            const mxj_t *s = body ? mxj_field(body, "stream_id") : NULL;
            const mxj_t *q = body ? mxj_field(body, "seq") : NULL;
            const mxj_t *pl = body ? mxj_field(body, "payload") : NULL;
            const char *sid =
                (s && s->type == MXJ_STR && s->str) ? s->str : "";
            const char *seqs =
                (q && q->type == MXJ_STR && q->str) ? q->str : "";
            const char *pay =
                (pl && pl->type == MXJ_STR && pl->str) ? pl->str : NULL;
            int ok = 0;
            unsigned long long seq = mxj_parse_u64(seqs, &ok);
            if (*sid && ok && pay) {
                evitem_t *it = (evitem_t *)calloc(1, sizeof(*it));
                if (it) {
                    it->is_stream = 1;
                    it->stream_id = xstrdup(sid);
                    it->seq = seq;
                    it->text_len = strlen(pay);
                    it->text = (char *)malloc(it->text_len + 1);
                    if (it->text)
                        memcpy(it->text, pay, it->text_len + 1);
                    if (it->stream_id && it->text)
                        enqueue_event(c, it);
                    else
                        ev_free(it);
                }
            }
            /* non-text chunk payloads are dropped at the edge (never
             * lossy-converted); the leg lives host-side. */
        }
        /* other types: ignored without dropping the session */
        frame_free(f);
    }
    pthread_mutex_lock(&c->donemu);
    c->done = 1;
    c->reason = reason;
    pthread_cond_signal(&c->donecond);
    pthread_mutex_unlock(&c->donemu);
    /* stop the dispatcher (it drains what is queued, then exits) */
    pthread_mutex_lock(&c->evmu);
    c->ev_stop = 1;
    pthread_cond_signal(&c->evcond);
    pthread_mutex_unlock(&c->evmu);
    return NULL;
}

/* -- connect / serve / close ----------------------------------------- */

static int unix_connect(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    size_t n = strlen(path);
    if (n >= sizeof(addr.sun_path)) {
        close(fd);
        return -1;
    }
    memcpy(addr.sun_path, path, n + 1);
    if (connect(fd, (struct sockaddr *)&addr,
                (socklen_t)(sizeof(addr.sun_family) + n + 1)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static char *read_token(void) {
    const char *e = getenv("MATRIX_LAUNCH_TOKEN");
    return xstrdup(e ? e : "");
}

/* Synchronous request/response used only during the handshake (no
 * threads yet, no waiter needed). */
static mxj_t *handshake_roundtrip(int fd, size_t max_frame, const char *doc,
                                  const char *want_type) {
    size_t n = strlen(doc);
    if (n > max_frame)
        return NULL;
    unsigned char hdr[4];
    hdr[0] = (unsigned char)((n >> 24) & 0xFF);
    hdr[1] = (unsigned char)((n >> 16) & 0xFF);
    hdr[2] = (unsigned char)((n >> 8) & 0xFF);
    hdr[3] = (unsigned char)(n & 0xFF);
    if (send_all(fd, hdr, 4) != 0 || send_all(fd, doc, n) != 0)
        return NULL;
    /* bounded wait: 30s */
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    struct timeval tv;
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    if (select(fd + 1, &rfds, NULL, NULL, &tv) <= 0)
        return NULL;
    frame_t *f = NULL;
    int rc = read_frame_dom(fd, max_frame, &f);
    if (rc != 1) {
        frame_free(f);
        return NULL;
    }
    const mxj_t *t = mxj_field(f->dom, "type");
    if (!t || t->type != MXJ_STR || strcmp(t->str, want_type)) {
        frame_free(f);
        return NULL;
    }
    mxj_t *dom = f->dom;
    f->dom = NULL;
    frame_free(f);
    return dom;
}

mx_status_t mx_connect(const char *sock_path, const char *logical,
                       const mx_handler_t *handler, mx_component_t **out) {
    if (out)
        *out = NULL;
    if (!sock_path || !logical || !handler)
        return MX_ERR_INVALID;
    int fd = unix_connect(sock_path);
    if (fd < 0)
        return MX_ERR_TRANSPORT;
    mx_component_t *c = (mx_component_t *)calloc(1, sizeof(*c));
    if (!c) {
        close(fd);
        return MX_ERR_NOMEM;
    }
    c->fd = fd;
    c->tp.fd = fd;
    pthread_mutex_init(&c->tp.wmu, NULL);
    c->max_frame = MX_DEFAULT_MAX_FRAME;
    c->tp.max_frame = MX_DEFAULT_MAX_FRAME;
    pthread_mutex_init(&c->mu, NULL);
    pthread_mutex_init(&c->evmu, NULL);
    pthread_cond_init(&c->evcond, NULL);
    pthread_mutex_init(&c->donemu, NULL);
    pthread_cond_init(&c->donecond, NULL);
    pthread_mutex_init(&c->seqmu, NULL);
    c->handler = *handler;
    c->seq = 0;

    char *token = read_token();
    char *q_token = token ? mxj_quote(token, strlen(token)) : NULL;
    free(token);
    mx_status_t st = MX_ERR_NOMEM;
    mxj_t *welcome = NULL, *reg = NULL, *act = NULL;
    char *hello = NULL;
    if (q_token) {
        size_t n = strlen(q_token) + 256;
        hello = (char *)malloc(n);
        if (hello)
            snprintf(hello, n,
                     "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
                     "\"type\":\"hello\",\"message_id\":\"h1\","
                     "\"body\":{\"launch_token\":%s,\"versions\":[\"0.1\"],"
                     "\"max_frame\":%u,\"client\":\"matrix-component-c\","
                     "\"features\":[\"dependency-calls/1\"]}}",
                     q_token, MX_DEFAULT_MAX_FRAME);
    }
    free(q_token);
    if (!hello)
        goto fail;
    welcome = handshake_roundtrip(fd, c->max_frame, hello, "welcome");
    free(hello);
    hello = NULL;
    if (!welcome) {
        st = MX_ERR_PROTOCOL;
        goto fail;
    }
    {
        const mxj_t *s = mxj_field(welcome, "session_id");
        const mxj_t *mf =
            mxj_field(welcome->type == MXJ_OBJ ? welcome : NULL, "body");
        const mxj_t *maxf = mf ? mxj_field(mf, "max_frame") : NULL;
        /* max_frame may be a JSON number (raw token) */
        if (maxf && maxf->type == MXJ_NUM) {
            unsigned long long m = strtoull(maxf->str, NULL, 10);
            if (m > 0 && m <= (unsigned long long)MX_DEFAULT_MAX_FRAME * 4) {
                c->max_frame = (size_t)m;
                c->tp.max_frame = (size_t)m;
            }
        }
        if (!s || s->type != MXJ_STR || !s->str ||
            strlen(s->str) >= sizeof(c->session)) {
            st = MX_ERR_PROTOCOL;
            goto fail;
        }
        strcpy(c->session, s->str);
        const mxj_t *body = mxj_field(welcome, "body");
        const mxj_t *feats = body ? mxj_field(body, "features") : NULL;
        if (feats && feats->type == MXJ_ARR) {
            size_t cnt = 0;
            for (mxj_item_t *it = feats->arr; it; it = it->next)
                if (it->val && it->val->type == MXJ_STR)
                    cnt++;
            c->features = (char **)calloc(cnt ? cnt : 1, sizeof(char *));
            if (!c->features)
                goto fail;
            for (mxj_item_t *it = feats->arr; it; it = it->next)
                if (it->val && it->val->type == MXJ_STR && it->val->str)
                    c->features[c->nfeatures++] = xstrdup(it->val->str);
        }
    }
    {
        char *q_id = mxj_quote(logical, strlen(logical));
        char *q_sess = mxj_quote(c->session, strlen(c->session));
        char *doc = NULL;
        if (q_id && q_sess) {
            size_t n = strlen(q_id) + strlen(q_sess) + 256;
            doc = (char *)malloc(n);
            if (doc)
                snprintf(doc, n,
                         "{\"protocol\":\"matrix.component\",\"version\":"
                         "\"0.1\",\"type\":\"component.register\","
                         "\"message_id\":\"reg1\",\"session_id\":%s,"
                         "\"body\":{\"manifest\":{\"id\":%s}}}",
                         q_sess, q_id);
        }
        free(q_id);
        free(q_sess);
        if (!doc)
            goto fail;
        reg = handshake_roundtrip(fd, c->max_frame, doc, "registered");
        free(doc);
        if (!reg) {
            st = MX_ERR_PROTOCOL;
            goto fail;
        }
        const mxj_t *i = mxj_field(reg, "instance_id");
        const mxj_t *g = mxj_field(reg, "generation");
        if (!i || i->type != MXJ_STR || !i->str ||
            strlen(i->str) >= sizeof(c->instance) || !g ||
            g->type != MXJ_STR || !g->str ||
            strlen(g->str) >= sizeof(c->generation)) {
            st = MX_ERR_PROTOCOL;
            goto fail;
        }
        strcpy(c->instance, i->str);
        strcpy(c->generation, g->str);
    }
    {
        /* wait for lifecycle.activate (bounded 30s via roundtrip's
         * select would need a send; do a plain bounded read here) */
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        struct timeval tv;
        tv.tv_sec = 30;
        tv.tv_usec = 0;
        if (select(fd + 1, &rfds, NULL, NULL, &tv) <= 0) {
            st = MX_ERR_PROTOCOL;
            goto fail;
        }
        frame_t *f = NULL;
        int rc = read_frame_dom(fd, c->max_frame, &f);
        if (rc != 1) {
            frame_free(f);
            st = MX_ERR_PROTOCOL;
            goto fail;
        }
        const mxj_t *t = mxj_field(f->dom, "type");
        if (!t || t->type != MXJ_STR || strcmp(t->str, "lifecycle.activate")) {
            frame_free(f);
            st = MX_ERR_PROTOCOL;
            goto fail;
        }
        act = f->dom;
        f->dom = NULL;
        frame_free(f);
        const mxj_t *body = mxj_field(act, "body");
        const mxj_t *dbs =
            body ? mxj_field(body, "dependency_bindings") : NULL;
        if (dbs && dbs->type == MXJ_ARR) {
            size_t cnt = 0;
            for (mxj_item_t *it = dbs->arr; it; it = it->next)
                cnt++;
            c->bindings =
                (mx_binding_t *)calloc(cnt ? cnt : 1, sizeof(mx_binding_t));
            if (!c->bindings) {
                st = MX_ERR_NOMEM;
                goto fail;
            }
            for (mxj_item_t *it = dbs->arr; it; it = it->next) {
                const mxj_t *id = mxj_field(it->val, "binding_id");
                const mxj_t *cp = mxj_field(it->val, "capability");
                if (id && id->type == MXJ_STR && id->str && *id->str &&
                    cp && cp->type == MXJ_STR && cp->str && *cp->str) {
                    c->bindings[c->nbindings].id = xstrdup(id->str);
                    c->bindings[c->nbindings].capability =
                        xstrdup(cp->str);
                    c->nbindings++;
                }
            }
        }
        const mxj_t *op =
            body ? mxj_field(body, "operation_id") : NULL;
        char *op_raw = op ? mxj_print(op) : NULL;
        const mxj_t *ridv = mxj_field(act, "request_id");
        const char *arid =
            (ridv && ridv->type == MXJ_STR) ? ridv->str : NULL;
        char fixed[512];
        snprintf(fixed, sizeof(fixed),
                 "{\"operation_id\":%s,\"status\":\"ok\",\"pending\":[]}",
                 op_raw ? op_raw : "\"op?\"");
        free(op_raw);
        if (send_envelope(&c->tp, "lifecycle.result", "lc1", c->session,
                          c->instance, c->generation, arid,
                          fixed) != 0) {
            st = MX_ERR_TRANSPORT;
            goto fail;
        }
    }
    mxj_free(welcome);
    mxj_free(reg);
    mxj_free(act);
    if (pthread_create(&c->disp_tid, NULL, dispatcher_main, c) != 0) {
        st = MX_ERR_TRANSPORT;
        goto fail2;
    }
    c->disp_set = 1;
    if (pthread_create(&c->reader_tid, NULL, reader_main, c) != 0) {
        pthread_mutex_lock(&c->evmu);
        c->ev_stop = 1;
        pthread_cond_signal(&c->evcond);
        pthread_mutex_unlock(&c->evmu);
        pthread_join(c->disp_tid, NULL);
        st = MX_ERR_TRANSPORT;
        goto fail2;
    }
    c->reader_set = 1;
    *out = c;
    return MX_OK;

fail:
    st = (st == MX_ERR_NOMEM) ? MX_ERR_NOMEM : MX_ERR_PROTOCOL;
fail2:
    free(hello);
    mxj_free(welcome);
    mxj_free(reg);
    mxj_free(act);
    if (c->features) {
        for (size_t i = 0; i < c->nfeatures; i++)
            free(c->features[i]);
        free(c->features);
    }
    if (c->bindings) {
        for (size_t i = 0; i < c->nbindings; i++) {
            free(c->bindings[i].id);
            free(c->bindings[i].capability);
        }
        free(c->bindings);
    }
    pthread_mutex_destroy(&c->tp.wmu);
    pthread_mutex_destroy(&c->mu);
    pthread_mutex_destroy(&c->evmu);
    pthread_cond_destroy(&c->evcond);
    pthread_mutex_destroy(&c->donemu);
    pthread_cond_destroy(&c->donecond);
    pthread_mutex_destroy(&c->seqmu);
    close(fd);
    free(c);
    return st;
}

mx_status_t mx_serve(mx_component_t *c, const char **reason_out) {
    if (!c)
        return MX_ERR_INVALID;
    pthread_mutex_lock(&c->donemu);
    while (!c->done)
        pthread_cond_wait(&c->donecond, &c->donemu);
    pthread_mutex_unlock(&c->donemu);
    pthread_join(c->reader_tid, NULL);
    pthread_join(c->disp_tid, NULL);
    c->reader_set = 0;
    c->disp_set = 0;
    if (reason_out)
        *reason_out = c->reason ? c->reason : "eof";
    return MX_OK;
}

void mx_close(mx_component_t *c) {
    if (!c)
        return;
    /* serve must have returned (threads joined); be tolerant: shut the
     * socket down so stray readers observe EOF. */
    shutdown(c->fd, SHUT_RDWR);
    close(c->fd);
    for (size_t i = 0; i < c->nfeatures; i++)
        free(c->features[i]);
    free(c->features);
    for (size_t i = 0; i < c->nbindings; i++) {
        free(c->bindings[i].id);
        free(c->bindings[i].capability);
    }
    free(c->bindings);
    /* waiters without terminals (shouldn't happen post-serve): wake */
    pthread_mutex_lock(&c->mu);
    for (waiter_t *w = c->waiters; w; w = w->next) {
        w->done = 1;
        pthread_cond_signal(&w->cond);
    }
    /* calls are owned by their threads (all joined via serve) */
    for (callrec_t *r = c->calls; r;) {
        callrec_t *nx = r->next;
        free(r->ticket);
        free(r);
        r = nx;
    }
    pthread_mutex_unlock(&c->mu);
    for (waiter_t *w = c->waiters; w;) {
        waiter_t *nx = w->next;
        waiter_free(w);
        w = nx;
    }
    pthread_mutex_lock(&c->evmu);
    for (evitem_t *it = c->evhead; it;) {
        evitem_t *nx = it->next;
        ev_free(it);
        it = nx;
    }
    pthread_mutex_unlock(&c->evmu);
    pthread_mutex_destroy(&c->tp.wmu);
    pthread_mutex_destroy(&c->mu);
    pthread_mutex_destroy(&c->evmu);
    pthread_cond_destroy(&c->evcond);
    pthread_mutex_destroy(&c->donemu);
    pthread_cond_destroy(&c->donecond);
    pthread_mutex_destroy(&c->seqmu);
    free(c);
}
