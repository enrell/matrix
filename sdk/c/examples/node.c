/* Generic Matrix test node for C (ML1 contract: docs/ML1-NODE.md).
 *
 * Usage: mx-node --matrix-sock <sock> --id <logical>
 *        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
 */
#define _POSIX_C_SOURCE 200809L

#include "mx_component.h"
#include "mx_json.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    char *id;
    char *event_log;
    char *stream_log;
    long stream_slow_ms;
} node_cfg_t;

static node_cfg_t G;

static void append_line(const char *path, const char *line) {
    if (!path)
        return;
    FILE *f = fopen(path, "a");
    if (!f)
        return;
    fprintf(f, "%s\n", line);
    fclose(f);
}

static const char *get_str(const mxj_t *obj, const char *key) {
    const mxj_t *v = obj ? mxj_field(obj, key) : NULL;
    return (v && v->type == MXJ_STR && v->str) ? v->str : NULL;
}

static long long get_int(const mxj_t *obj, const char *key, long long dflt) {
    const mxj_t *v = obj ? mxj_field(obj, key) : NULL;
    if (!v)
        return dflt;
    if (v->type == MXJ_NUM && v->str)
        return strtoll(v->str, NULL, 10);
    return dflt;
}

static int get_bool(const mxj_t *obj, const char *key) {
    const mxj_t *v = obj ? mxj_field(obj, key) : NULL;
    return v && v->type == MXJ_BOOL && v->boolean;
}

/* Abortable sleep in 5ms slices; returns 1 when cancelled. */
static int abortable_sleep(long long ms, mx_call_ctx_t *ctx) {
    long long slept = 0;
    while (slept < ms) {
        if (mx_cancelled(ctx))
            return 1;
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 5000000;
        nanosleep(&ts, NULL);
        slept += 5;
    }
    return 0;
}

static void on_cancel(const char *ticket, void *ud) {
    (void)ticket;
    (void)ud;
}

static void on_event(const char *topic, const char *payload_json,
                     void *ud) {
    (void)ud;
    size_t n = strlen(topic) + strlen(payload_json) + 2;
    char *line = (char *)malloc(n);
    if (!line)
        return;
    snprintf(line, n, "%s\t%s", topic, payload_json);
    append_line(G.event_log, line);
    free(line);
}

static void on_stream(const char *stream_id, unsigned long long seq,
                      const char *payload, size_t payload_len, void *ud) {
    (void)ud;
    if (G.stream_slow_ms > 0) {
        struct timespec ts;
        ts.tv_sec = G.stream_slow_ms / 1000;
        ts.tv_nsec = (G.stream_slow_ms % 1000) * 1000000;
        nanosleep(&ts, NULL);
    }
    char line[256];
    snprintf(line, sizeof(line), "%s\t%llu\t%zu", stream_id, seq,
             payload_len);
    (void)payload;
    append_line(G.stream_log, line);
}

static mx_result_t business(const char *code, const char *msg) {
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = code ? strdup(code) : NULL;
    r.err_msg = msg ? strdup(msg) : NULL;
    return r;
}

static mx_result_t chain_with_streams(mx_call_ctx_t *ctx, mxj_t *spec);

static mx_result_t on_call(mx_call_ctx_t *ctx, const char *ticket,
                           const char *cap, const char *input_json,
                           void *ud) {
    (void)ticket;
    (void)cap;
    (void)ud;
    mxj_t *in = mxj_parse(input_json ? input_json : "{}");
    if (!in || in->type != MXJ_OBJ) {
        mxj_free(in);
        in = mxj_parse("{}");
    }
    long long sleep_ms = get_int(in, "sleep_ms", 0);
    if (sleep_ms > 0 && abortable_sleep(sleep_ms, ctx)) {
        mxj_free(in);
        return business("cancelled", "aborted");
    }
    const char *fail = get_str(in, "fail");
    if (fail && *fail) {
        char msg[256];
        snprintf(msg, sizeof(msg), "remote %s", fail);
        mx_result_t r = business(fail, msg);
        mxj_free(in);
        return r;
    }
    const mxj_t *amp_v = mxj_field(in, "amplify");
    if (amp_v && (amp_v->type == MXJ_NUM ||
                  (amp_v->type == MXJ_BOOL))) {
        long long n = amp_v->type == MXJ_NUM && amp_v->str ?
                          strtoll(amp_v->str, NULL, 10) :
                          0;
        if (n < 0)
            n = 0;
        if (n > 1 << 20)
            n = 1 << 20;
        char *blob = (char *)malloc((size_t)n + 1);
        if (blob) {
            memset(blob, 'x', (size_t)n);
            blob[n] = '\0';
        }
        char *q_id = mxj_quote(G.id, strlen(G.id));
        size_t m = (size_t)(n + 64 + (q_id ? strlen(q_id) : 0));
        char *out = (char *)malloc(m);
        if (out && blob && q_id)
            snprintf(out, m, "{\"blob\":\"%s\",\"via\":%s}", blob, q_id);
        free(blob);
        free(q_id);
        mx_result_t r;
        r.output_json = out;
        r.err_code = NULL;
        r.err_msg = NULL;
        mxj_free(in);
        return r;
    }
    if (get_bool(in, "chain")) {
        size_t nb = 0;
        const mx_binding_t *b = mx_dependencies(ctx, &nb);
        if (!nb) {
            mxj_free(in);
            return business("dependency-unavailable", "no binding");
        }
        const mxj_t *inner = mxj_field(in, "input");
        char *inner_raw = inner ? mxj_print(inner) : NULL;
        long long timeout = get_int(in, "timeout_ms", 5000);
        if (timeout < 1)
            timeout = 1;
        char *out = NULL, *code = NULL, *msg = NULL;
        mx_status_t st = mx_invoke_dependency(
            ctx, b[0].id, inner_raw ? inner_raw : "{}",
            (unsigned long long)timeout, &out, &code, &msg);
        free(inner_raw);
        char *q_id = mxj_quote(G.id, strlen(G.id));
        mx_result_t r;
        r.output_json = NULL;
        r.err_code = NULL;
        r.err_msg = NULL;
        if (st == MX_OK && out && q_id) {
            size_t m = strlen(out) + strlen(q_id) + 32;
            r.output_json = (char *)malloc(m);
            if (r.output_json)
                snprintf(r.output_json, m, "{\"chained\":%s,\"via\":%s}",
                         out, q_id);
        } else {
            r.err_code = code ? code : strdup("internal");
            r.err_msg = msg ? msg : strdup("invoke failed");
            code = msg = NULL;
        }
        free(out);
        free(code);
        free(msg);
        free(q_id);
        mxj_free(in);
        return r;
    }
    const mxj_t *acq = mxj_field(in, "acquire");
    if (acq && acq->type == MXJ_OBJ) {
        const char *kind = get_str(acq, "kind");
        const char *label = get_str(acq, "label");
        const mxj_t *msv = mxj_field(acq, "interval_ms");
        int has_ms = msv && msv->type == MXJ_NUM && msv->str;
        unsigned long long ms =
            has_ms ? strtoull(msv->str, NULL, 10) : 0;
        unsigned long long h = 0;
        char *code = NULL, *msg = NULL;
        mx_status_t st = mx_acquire_resource(ctx, kind ? kind : "",
                                             label ? label : "", has_ms,
                                             ms, &h, &code, &msg);
        char *q_id = mxj_quote(G.id, strlen(G.id));
        mx_result_t r;
        r.output_json = NULL;
        r.err_code = NULL;
        r.err_msg = NULL;
        if (st == MX_OK && q_id) {
            char eb[128];
            snprintf(eb, sizeof(eb),
                     "{\"acquired\":{\"handle\":\"%llu\"},\"via\":%s}", h,
                     q_id);
            r.output_json = strdup(eb);
        } else {
            r.err_code = code ? code : strdup("internal");
            r.err_msg = msg ? msg : strdup("acquire failed");
            code = msg = NULL;
        }
        free(code);
        free(msg);
        free(q_id);
        mxj_free(in);
        return r;
    }
    const mxj_t *rel = mxj_field(in, "release");
    if (rel) {
        unsigned long long h = 0;
        int ok = 0;
        if (rel->type == MXJ_NUM && rel->str) {
            h = strtoull(rel->str, NULL, 10);
            ok = 1;
        } else if (rel->type == MXJ_STR && rel->str) {
            char *end = NULL;
            h = strtoull(rel->str, &end, 10);
            ok = end && !*end;
        }
        if (!ok) {
            mxj_free(in);
            return business("invalid-message", "bad release");
        }
        char *code = NULL, *msg = NULL;
        mx_status_t st =
            mx_release_resource(ctx, h, &code, &msg);
        char *q_id = mxj_quote(G.id, strlen(G.id));
        mx_result_t r;
        r.output_json = NULL;
        r.err_code = NULL;
        r.err_msg = NULL;
        if (st == MX_OK && q_id) {
            char eb[160];
            snprintf(eb, sizeof(eb), "{\"released\":\"%llu\",\"via\":%s}",
                     h, q_id);
            r.output_json = strdup(eb);
        } else {
            r.err_code = code ? code : strdup("internal");
            r.err_msg = msg ? msg : strdup("release failed");
            code = msg = NULL;
        }
        free(code);
        free(msg);
        free(q_id);
        mxj_free(in);
        return r;
    }
    const mxj_t *spec = mxj_field(in, "stream_send");
    if (spec && spec->type == MXJ_OBJ) {
        const char *sid = get_str(spec, "stream_id");
        long long chunks = get_int(spec, "chunks", 0);
        long long nbytes = get_int(spec, "chunk_bytes", 0);
        long long slp = get_int(spec, "sleep_ms", 0);
        if (!sid || !*sid)
            sid = "s-test";
        if (chunks < 0)
            chunks = 0;
        if (chunks > 256)
            chunks = 256;
        if (nbytes < 0)
            nbytes = 0;
        if (nbytes > 4096)
            nbytes = 4096;
        char *payload = (char *)malloc((size_t)nbytes + 1);
        if (payload) {
            memset(payload, 'x', (size_t)nbytes);
            payload[nbytes] = '\0';
        }
        long long sent = 0;
        mx_result_t r;
        r.output_json = NULL;
        r.err_code = NULL;
        r.err_msg = NULL;
        for (long long seq = 0; seq < chunks; seq++) {
            if (mx_cancelled(ctx)) {
                r.err_code = strdup("cancelled");
                r.err_msg = strdup("aborted");
                break;
            }
            mx_status_t sst = mx_send_stream(ctx, sid,
                                             (unsigned long long)seq,
                                             payload ? payload : "",
                                             (size_t)nbytes);
            if (sst != MX_OK) {
                r.err_code = strdup("stream-refused");
                r.err_msg = strdup(mx_strerror(sst));
                break;
            }
            sent++;
            if (slp > 0) {
                long long s = slp > 50 ? 50 : slp;
                if (abortable_sleep(s, ctx)) {
                    r.err_code = strdup("cancelled");
                    r.err_msg = strdup("aborted");
                    break;
                }
            }
        }
        free(payload);
        if (!r.err_code) {
            char *q_id = mxj_quote(G.id, strlen(G.id));
            char eb[160];
            snprintf(eb, sizeof(eb), "{\"stream_sent\":%lld,\"via\":%s}",
                     sent, q_id ? q_id : "\"?\"");
            free(q_id);
            r.output_json = strdup(eb);
        }
        mxj_free(in);
        return r;
    }
    const mxj_t *cws = mxj_field(in, "chain_with_streams");
    if (cws && cws->type == MXJ_OBJ) {
        mx_result_t r = chain_with_streams(ctx, (mxj_t *)cws);
        mxj_free(in);
        return r;
    }
    /* default echo */
    char *in_raw = mxj_print(in);
    char *q_id = mxj_quote(G.id, strlen(G.id));
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    if (in_raw && q_id) {
        size_t m = strlen(in_raw) + strlen(q_id) + 32;
        r.output_json = (char *)malloc(m);
        if (r.output_json)
            snprintf(r.output_json, m, "{\"echo\":%s,\"via\":%s}",
                     in_raw, q_id);
    }
    free(in_raw);
    free(q_id);
    mxj_free(in);
    return r;
}

/* Concurrent chain + streams (M7 bidi legs): a helper thread streams
 * while the child leg is in flight on this same session. */
typedef struct {
    mx_call_ctx_t *ctx;
    char *stream_id;
    char *payload;
    long long chunks;
    long long interval_ms;
    long long sent;
} streamer_arg_t;

static void *streamer_main(void *a) {
    streamer_arg_t *s = (streamer_arg_t *)a;
    for (long long seq = 0; seq < s->chunks; seq++) {
        if (mx_send_stream(s->ctx, s->stream_id,
                           (unsigned long long)seq, s->payload,
                           strlen(s->payload)) == MX_OK)
            s->sent++;
        else
            break;
        if (s->interval_ms > 0) {
            struct timespec ts;
            ts.tv_sec = s->interval_ms / 1000;
            ts.tv_nsec = (s->interval_ms % 1000) * 1000000;
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

static mx_result_t chain_with_streams(mx_call_ctx_t *ctx, mxj_t *spec) {
    const char *sid = get_str(spec, "stream_id");
    long long chunks = get_int(spec, "chunks", 0);
    long long nbytes = get_int(spec, "chunk_bytes", 0);
    long long interval = get_int(spec, "interval_ms", 20);
    long long prime = get_int(spec, "prime_ms", 50);
    if (!sid || !*sid)
        sid = "s-bidi";
    if (chunks < 0)
        chunks = 0;
    if (chunks > 32)
        chunks = 32;
    if (nbytes < 0)
        nbytes = 0;
    if (nbytes > 1024)
        nbytes = 1024;
    if (interval < 0)
        interval = 0;
    if (interval > 50)
        interval = 50;
    if (prime < 0)
        prime = 0;
    if (prime > 1000)
        prime = 1000;
    /* prime delay so admission+dispatch can map the leg first */
    if (prime > 0) {
        struct timespec ts;
        ts.tv_sec = prime / 1000;
        ts.tv_nsec = (prime % 1000) * 1000000;
        nanosleep(&ts, NULL);
    }
    char *payload = (char *)malloc((size_t)nbytes + 1);
    if (payload) {
        memset(payload, 'x', (size_t)nbytes);
        payload[nbytes] = '\0';
    }
    streamer_arg_t arg;
    arg.ctx = ctx;
    arg.stream_id = strdup(sid);
    arg.payload = payload ? payload : strdup("");
    arg.chunks = chunks;
    arg.interval_ms = interval;
    arg.sent = 0;
    pthread_t th;
    int joined = 0;
    if (pthread_create(&th, NULL, streamer_main, &arg) != 0) {
        /* no streamer: chain alone still answers */
        arg.sent = 0;
        joined = 1;
    }
    size_t nb = 0;
    const mx_binding_t *b = mx_dependencies(ctx, &nb);
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    if (!nb) {
        if (!joined) {
            pthread_join(th, NULL);
            joined = 1;
        }
        r.err_code = strdup("dependency-unavailable");
        r.err_msg = strdup("no binding");
    } else {
        const mxj_t *inner = mxj_field(spec, "input");
        char *inner_raw = inner ? mxj_print(inner) : NULL;
        long long timeout = get_int(spec, "timeout_ms", 8000);
        if (timeout < 1)
            timeout = 1;
        char *out = NULL, *code = NULL, *msg = NULL;
        /* NOTE: ctx is shared with the streamer thread for
         * send-only use; invoke owns the waiter map entry. The
         * transport mutex serializes the actual writes. */
        mx_status_t st = mx_invoke_dependency(
            ctx, b[0].id, inner_raw ? inner_raw : "{}",
            (unsigned long long)timeout, &out, &code, &msg);
        free(inner_raw);
        if (!joined) {
            pthread_join(th, NULL);
            joined = 1;
        }
        char *q_id = mxj_quote(G.id, strlen(G.id));
        if (st == MX_OK && out && q_id) {
            size_t m = strlen(out) + strlen(q_id) + 64;
            r.output_json = (char *)malloc(m);
            if (r.output_json)
                snprintf(r.output_json, m,
                         "{\"chained\":%s,\"via\":%s,\"stream_sent\":%lld}",
                         out, q_id, arg.sent);
        } else {
            r.err_code = code ? code : strdup("internal");
            r.err_msg = msg ? msg : strdup("invoke failed");
            code = msg = NULL;
        }
        free(out);
        free(code);
        free(msg);
        free(q_id);
    }
    free(arg.stream_id);
    free(arg.payload);
    return r;
}

int main(int argc, char **argv) {
    const char *sock = NULL;
    G.id = "dep-node";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--matrix-sock") && i + 1 < argc)
            sock = argv[++i];
        else if (!strcmp(argv[i], "--id") && i + 1 < argc)
            G.id = argv[++i];
        else if (!strcmp(argv[i], "--event-log") && i + 1 < argc)
            G.event_log = argv[++i];
        else if (!strcmp(argv[i], "--stream-log") && i + 1 < argc)
            G.stream_log = argv[++i];
        else if (!strcmp(argv[i], "--stream-slow-ms") && i + 1 < argc)
            G.stream_slow_ms = strtol(argv[++i], NULL, 10);
    }
    if (!sock) {
        fprintf(stderr,
                "usage: mx-node --matrix-sock <sock> [--id <logical>] ...\n");
        return 2;
    }
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = on_call;
    h.on_cancel = on_cancel;
    h.on_event = on_event;
    h.on_stream = on_stream;
    mx_component_t *c = NULL;
    if (mx_connect(sock, G.id, &h, &c) != MX_OK) {
        fprintf(stderr, "connect failed\n");
        return 2;
    }
    const char *reason = NULL;
    mx_status_t st = mx_serve(c, &reason);
    mx_close(c);
    if (st != MX_OK) {
        fprintf(stderr, "serve: %s\n", mx_strerror(st));
        return 1;
    }
    if (strcmp(reason, "dispose") && strcmp(reason, "eof")) {
        fprintf(stderr, "serve: %s\n", reason);
        return 1;
    }
    return 0;
}
