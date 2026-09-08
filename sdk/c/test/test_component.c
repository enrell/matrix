/* Loopback fake host for the C SDK tests (POSIX only, no deps).
 * Serves one connection through the handshake, then runs the script.
 * Prints TAP-ish "ok <name>" lines; any failure aborts nonzero. */
#define _POSIX_C_SOURCE 200809L

#include "mx_component.h"
#include "mx_json.h"

#include <arpa/inet.h>
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define CHECK(c)                                                                \
    do {                                                                        \
        if (!(c)) {                                                             \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
            exit(1);                                                            \
        }                                                                       \
    } while (0)

static void send_msg(int fd, const char *doc) {
    uint32_t n = (uint32_t)strlen(doc);
    unsigned char hdr[4];
    hdr[0] = (unsigned char)((n >> 24) & 0xFF);
    hdr[1] = (unsigned char)((n >> 16) & 0xFF);
    hdr[2] = (unsigned char)((n >> 8) & 0xFF);
    hdr[3] = (unsigned char)(n & 0xFF);
    CHECK(write(fd, hdr, 4) == 4);
    CHECK(write(fd, doc, n) == (ssize_t)n);
}

static char *read_msg(int fd) {
    unsigned char hdr[4];
    size_t got = 0;
    while (got < 4) {
        ssize_t r = read(fd, hdr + got, 4 - got);
        CHECK(r > 0);
        got += (size_t)r;
    }
    uint32_t n = ((uint32_t)hdr[0] << 24) | ((uint32_t)hdr[1] << 16) |
                 ((uint32_t)hdr[2] << 8) | hdr[3];
    CHECK(n > 0 && n <= 1024 * 1024);
    char *buf = (char *)malloc(n + 1);
    CHECK(buf);
    got = 0;
    while (got < n) {
        ssize_t r = read(fd, buf + got, n - got);
        CHECK(r > 0);
        got += (size_t)r;
    }
    buf[n] = '\0';
    return buf;
}

/* Reads with a 10s deadline (0 on timeout). */
static char *read_msg_timeout(int fd, int *timed_out) {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);
    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    int r = select(fd + 1, &rfds, NULL, NULL, &tv);
    if (r <= 0) {
        *timed_out = 1;
        return NULL;
    }
    *timed_out = 0;
    return read_msg(fd);
}

static char *sock_path;
static int srv_fd;

static void loopback_setup(void) {
    char tmpl[] = "/tmp/cunits-XXXXXX";
    CHECK(mkdtemp(tmpl));
    sock_path = strdup(tmpl);
    char sp[512];
    snprintf(sp, sizeof(sp), "%s/t.sock", tmpl);
    free(sock_path);
    sock_path = strdup(sp);
    srv_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK(srv_fd >= 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, sp);
    CHECK(bind(srv_fd, (struct sockaddr *)&addr,
               (socklen_t)(sizeof(addr.sun_family) + strlen(sp) + 1)) == 0);
    CHECK(listen(srv_fd, 1) == 0);
}

static int loopback_accept(const char *features_json,
                           const char *bindings_json) {
    int conn = accept(srv_fd, NULL, NULL);
    CHECK(conn >= 0);
    char *hello = read_msg(conn);
    CHECK(strstr(hello, "\"type\":\"hello\""));
    free(hello);
    char welcome[1024];
    snprintf(welcome, sizeof(welcome),
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"welcome\",\"message_id\":\"h1\",\"session_id\":"
             "\"s1\",\"body\":{\"version\":\"0.1\",\"max_frame\":1048576,"
             "\"limits\":{},\"features\":%s}}",
             features_json);
    send_msg(conn, welcome);
    char *reg = read_msg(conn);
    CHECK(strstr(reg, "\"type\":\"component.register\""));
    free(reg);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"registered\",\"message_id\":\"r\",\"session_id\":"
             "\"s1\",\"instance_id\":\"1\",\"generation\":\"1\",\"body\":{"
             "\"logical\":\"t\"}}");
    char act[2048];
    snprintf(act, sizeof(act),
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.activate\",\"message_id\":\"m1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"q\",\"body\":{\"operation_id\":\"op\","
             "\"manifest\":{},\"bindings\":[],\"dependency_bindings\":%s}}",
             bindings_json);
    send_msg(conn, act);
    char *lc = read_msg(conn);
    CHECK(strstr(lc, "\"type\":\"lifecycle.result\""));
    free(lc);
    return conn;
}

static void loopback_teardown(int conn) {
    close(conn);
    close(srv_fd);
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", sock_path);
    char *slash = strrchr(sock_path, '/');
    if (slash) {
        *slash = '\0';
        snprintf(cmd, sizeof(cmd), "rm -rf %s", sock_path);
    }
    int rc = system(cmd);
    (void)rc;
    free(sock_path);
}

static void call_open(char *buf, size_t cap, const char *ticket,
                      const char *input) {
    snprintf(buf, cap,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"call.open\",\"message_id\":\"m-%s\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"r-%s\",\"body\":{\"ticket\": \"%s\","
             "\"capability\":\"c@1\",\"input\":%s}}",
             ticket, ticket, ticket, input);
}

/* -- handlers ---------------------------------------------------------- */

static mx_result_t echo_call(mx_call_ctx_t *ctx, const char *ticket,
                             const char *cap, const char *input_json,
                             void *ud) {
    (void)ctx;
    (void)ticket;
    (void)cap;
    (void)ud;
    mx_result_t r;
    size_t n = strlen(input_json) + 16;
    r.output_json = (char *)malloc(n);
    if (r.output_json)
        snprintf(r.output_json, n, "{\"echo\":%s}", input_json);
    r.err_code = NULL;
    r.err_msg = NULL;
    return r;
}

static mx_result_t gate_call(mx_call_ctx_t *ctx, const char *ticket,
                             const char *cap, const char *input_json,
                             void *ud) {
    (void)ticket;
    (void)cap;
    (void)input_json;
    (void)ud;
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    char *out = NULL, *code = NULL, *msg = NULL;
    mx_status_t st =
        mx_invoke_dependency(ctx, "bind-x", "{}", 2000, &out, &code, &msg);
    if (st == MX_ERR_UNSUPPORTED) {
        r.output_json = strdup("{\"refused\":\"unsupported-feature\"}");
    } else {
        char eb[128];
        snprintf(eb, sizeof(eb), "{\"unexpected\":%d}", (int)st);
        r.output_json = strdup(eb);
    }
    free(out);
    free(code);
    free(msg);
    return r;
}

static mx_result_t chain_call(mx_call_ctx_t *ctx, const char *ticket,
                              const char *cap, const char *input_json,
                              void *ud) {
    (void)ticket;
    (void)cap;
    (void)input_json;
    (void)ud;
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    size_t n = 0;
    const mx_binding_t *b = mx_dependencies(ctx, &n);
    if (!n) {
        r.err_code = strdup("dependency-unavailable");
        r.err_msg = strdup("no binding");
        return r;
    }
    char *out = NULL, *code = NULL, *msg = NULL;
    mx_status_t st = mx_invoke_dependency(ctx, b[0].id, "{\"v\":1}", 5000,
                                          &out, &code, &msg);
    if (st == MX_OK) {
        size_t m = strlen(out) + 16;
        r.output_json = (char *)malloc(m);
        if (r.output_json)
            snprintf(r.output_json, m, "{\"got\":%s}", out);
    } else {
        r.err_code = code ? code : strdup("internal");
        r.err_msg = msg ? msg : strdup("invoke failed");
        code = msg = NULL;
    }
    free(out);
    free(code);
    free(msg);
    return r;
}

static int flood_drops_seen(const char *rep) {
    /* crude: find "dropped":N with N>=1 */
    const char *p = strstr(rep, "\"dropped\":");
    if (!p)
        return 0;
    return atoi(p + 10) >= 1;
}

static mx_result_t flood_call(mx_call_ctx_t *ctx, const char *ticket,
                              const char *cap, const char *input_json,
                              void *ud) {
    (void)ticket;
    (void)cap;
    (void)ud;
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    if (strstr(input_json, "report_drops")) {
        unsigned long long d = mx_event_dropped_count(ctx);
        char eb[64];
        snprintf(eb, sizeof(eb), "{\"dropped\":%llu}", d);
        r.output_json = strdup(eb);
    } else {
        r.output_json = strdup("{\"ok\":true}");
    }
    return r;
}

static void slow_event(const char *topic, const char *payload_json,
                       void *ud) {
    (void)topic;
    (void)payload_json;
    int *count = (int *)ud;
    /* slow observer yields; reader keeps flowing */
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 30000000; /* 30ms */
    nanosleep(&ts, NULL);
    if (count)
        (*count)++;
}

static mx_result_t bin_call(mx_call_ctx_t *ctx, const char *ticket,
                            const char *cap, const char *input_json,
                            void *ud) {
    (void)ticket;
    (void)cap;
    (void)input_json;
    (void)ud;
    mx_result_t r;
    r.output_json = NULL;
    r.err_code = NULL;
    r.err_msg = NULL;
    /* invalid UTF-8 must refuse explicitly, never lossy-convert */
    static const char bad[2] = {(char)0xFF, (char)0xFE};
    mx_status_t st = mx_send_stream(ctx, "s1", 0, bad, 2);
    if (st == MX_ERR_INVALID) {
        r.err_code = strdup("invalid-message");
        r.err_msg = strdup("binary refused");
    } else {
        r.err_code = strdup("internal");
        r.err_msg = strdup("binary accepted?!");
    }
    return r;
}

/* -- cases ------------------------------------------------------------- */

static void t_gen_equal(void) {
    int ok = 0;
    CHECK(mxj_parse_u64("18446744073709551615", &ok) ==
              18446744073709551615ull &&
          ok);
    CHECK(!mxj_parse_u64("18446744073709551616", &ok) && !ok);
    CHECK(!mxj_parse_u64("nope", &ok) && !ok);
    CHECK(!mxj_parse_u64("", &ok) && !ok);
    printf("ok u64-precision\n");
}

typedef struct {
    mx_component_t **comp;
    char *path;
    mx_handler_t h;
    const char *reason;
} serve_args_t;

static void *serve_main(void *a) {
    serve_args_t *p = (serve_args_t *)a;
    mx_component_t *c = NULL;
    if (mx_connect(p->path, "t", &p->h, &c) != MX_OK)
        return (void *)1;
    *p->comp = c;
    const char *reason = NULL;
    if (mx_serve(c, &reason) != MX_OK)
        return (void *)1;
    p->reason = reason;
    return NULL;
}

static void t_malformed_survives(void) {
    loopback_setup();
    /* child thread runs the SDK; main drives the fake host */
    char *path = strdup(sock_path);
    mx_component_t *comp = NULL;
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = echo_call;
    serve_args_t args = {&comp, path, h, NULL};
    pthread_t th;
    CHECK(pthread_create(&th, NULL, serve_main, &args) == 0);
    int conn = loopback_accept("[]", "[]");
    /* garbage frame (valid length, invalid JSON) then a real call */
    const char *garbage = "{oops";
    uint32_t n = (uint32_t)strlen(garbage);
    unsigned char hdr[4];
    hdr[0] = (unsigned char)((n >> 24) & 0xFF);
    hdr[1] = (unsigned char)((n >> 16) & 0xFF);
    hdr[2] = (unsigned char)((n >> 8) & 0xFF);
    hdr[3] = (unsigned char)(n & 0xFF);
    CHECK(write(conn, hdr, 4) == 4);
    CHECK(write(conn, garbage, n) == (ssize_t)n);
    char open[1024];
    call_open(open, sizeof(open), "tkt-9", "{\"ping\":1}");
    send_msg(conn, open);
    char *ans = read_msg(conn);
    CHECK(strstr(ans, "\"type\":\"call.result\""));
    CHECK(strstr(ans, "\"ping\":1"));
    free(ans);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"rd1\",\"body\":{\"operation_id\":"
             "\"op\",\"deadline_ms\":100}}");
    char *lc = read_msg(conn);
    free(lc);
    void *rv = NULL;
    CHECK(pthread_join(th, &rv) == 0);
    CHECK(rv == NULL);
    CHECK(args.reason && !strcmp(args.reason, "dispose"));
    CHECK(comp);
    mx_close(comp);
    free(path);
    loopback_teardown(conn);
    printf("ok malformed-survives\n");
}

static mx_component_t *g_comp;
static const char *g_reason;
static void *serve_thread(void *a) {
    mx_handler_t *h = ((void **)a)[0];
    char *path = ((void **)a)[1];
    mx_component_t *c = NULL;
    if (mx_connect(path, "t", h, &c) != MX_OK)
        return (void *)1;
    g_comp = c;
    if (mx_serve(c, &g_reason) != MX_OK)
        return (void *)1;
    return NULL;
}

static void run_case(mx_handler_t *h, const char *features,
                     const char *bindings,
                     void (*script)(int conn)) {
    loopback_setup();
    char *path = strdup(sock_path);
    void *a[2] = {h, path};
    g_comp = NULL;
    g_reason = NULL;
    pthread_t th;
    CHECK(pthread_create(&th, NULL, serve_thread, a) == 0);
    int conn = loopback_accept(features, bindings);
    script(conn);
    void *rv = NULL;
    CHECK(pthread_join(th, &rv) == 0);
    CHECK(rv == NULL);
    CHECK(g_reason && !strcmp(g_reason, "dispose"));
    mx_close(g_comp);
    g_comp = NULL;
    free(path);
    loopback_teardown(conn);
}

static void script_stale(int conn) {
    char open[1024];
    call_open(open, sizeof(open), "tkt-stale", "{}");
    /* rewrite generation to 999 */
    char *g = strstr(open, "\"generation\":\"1\"");
    CHECK(g);
    memcpy(g + 14, "999", 3);
    send_msg(conn, open);
    call_open(open, sizeof(open), "tkt-9", "{}");
    send_msg(conn, open);
    char *ans = read_msg(conn);
    /* the stale open is ignored: the only answer correlates to r-tkt-9 */
    CHECK(strstr(ans, "r-tkt-9"));
    free(ans);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"rd1\",\"body\":{\"operation_id\":"
             "\"op\",\"deadline_ms\":100}}");
    char *lc = read_msg(conn);
    free(lc);
}

static void t_stale_ignored(void) {
    /* echo answers {"echo": input}; the script asserts the current
     * ticket's answer arrives (the stale generation is ignored). */
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = echo_call;
    run_case(&h, "[]", "[]", script_stale);
    printf("ok stale-generation-ignored\n");
}

static void script_gate(int conn) {
    char open[1024];
    call_open(open, sizeof(open), "tkt-9", "{}");
    send_msg(conn, open);
    char *ans = read_msg(conn);
    CHECK(strstr(ans, "\"refused\":\"unsupported-feature\"") ||
          strstr(ans, "\"refused\": \"unsupported-feature\""));
    free(ans);
    /* anything else from the SDK now would be a wire touch */
    int timed_out = 0;
    char *extra = read_msg_timeout(conn, &timed_out);
    CHECK(timed_out == 1);
    free(extra);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"rd1\",\"body\":{\"operation_id\":"
             "\"op\",\"deadline_ms\":100}}");
    char *lc = read_msg(conn);
    free(lc);
}

static void t_feature_gate(void) {
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = gate_call;
    run_case(&h, "[]", "[]", script_gate);
    printf("ok feature-gate-local\n");
}

static void script_chain(int conn) {
    char open[1024];
    call_open(open, sizeof(open), "tkt-9", "{\"chain_it\":true}");
    send_msg(conn, open);
    char *opened = read_msg(conn);
    CHECK(strstr(opened, "\"type\":\"dependency.open\""));
    CHECK(strstr(opened, "\"binding_id\":\"bind-1\"") ||
          strstr(opened, "\"binding_id\": \"bind-1\""));
    CHECK(strstr(opened, "\"parent_ticket\":\"tkt-9\"") ||
          strstr(opened, "\"parent_ticket\": \"tkt-9\""));
    /* correlate to the open's request_id */
    const char *p = strstr(opened, "\"request_id\":\"");
    CHECK(p);
    p += 14;
    char rid[64];
    size_t i = 0;
    while (*p && *p != '"' && i + 1 < sizeof(rid))
        rid[i++] = *p++;
    rid[i] = '\0';
    free(opened);
    char res[1024];
    snprintf(res, sizeof(res),
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"dependency.result\",\"message_id\":\"mres\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"%s\",\"body\":{\"status\":\"ok\","
             "\"output\":{\"deep\":1}}}",
             rid);
    send_msg(conn, res);
    char *ans = read_msg(conn);
    CHECK(strstr(ans, "\"deep\":1"));
    free(ans);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"rd1\",\"body\":{\"operation_id\":"
             "\"op\",\"deadline_ms\":100}}");
    char *lc = read_msg(conn);
    free(lc);
}

static void t_dep_roundtrip(void) {
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = chain_call;
    run_case(&h, "[\"dependency-calls/1\"]",
             "[{\"binding_id\":\"bind-1\",\"capability\":\"c@1\"}]",
             script_chain);
    printf("ok dependency-roundtrip\n");
}

static void script_flood(int conn) {
    char ev[512];
    for (int i = 0; i < 120; i++) {
        snprintf(ev, sizeof(ev),
                 "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
                 "\"type\":\"event.deliver\",\"message_id\":\"e%d\","
                 "\"session_id\":\"s1\",\"instance_id\":\"1\","
                 "\"generation\":\"1\",\"request_id\":\"re%d\",\"body\":{"
                 "\"topic\":\"t\",\"payload\":{\"n\":%d}}}",
                 i, i, i);
        send_msg(conn, ev);
    }
    char open[1024];
    call_open(open, sizeof(open), "tkt-9", "{}");
    send_msg(conn, open);
    char *ans = read_msg(conn);
    CHECK(strstr(ans, "\"type\":\"call.result\""));
    free(ans);
    call_open(open, sizeof(open), "tkt-10", "{\"report_drops\":true}");
    send_msg(conn, open);
    char *rep = read_msg(conn);
    CHECK(flood_drops_seen(rep));
    free(rep);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"rd1\",\"body\":{\"operation_id\":"
             "\"op\",\"deadline_ms\":100}}");
    char *lc = read_msg(conn);
    free(lc);
}

static void t_flood(void) {
    static int nevents = 0;
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = flood_call;
    h.on_event = slow_event;
    h.userdata = &nevents;
    run_case(&h, "[]", "[]", script_flood);
    printf("ok flood-reader-independent\n");
}

static void script_bin(int conn) {
    char open[1024];
    call_open(open, sizeof(open), "tkt-9", "{\"send_bytes\":true}");
    send_msg(conn, open);
    char *ans = read_msg(conn);
    CHECK(strstr(ans, "\"status\":\"error\"") ||
          strstr(ans, "\"status\": \"error\""));
    CHECK(strstr(ans, "invalid-message"));
    CHECK(!strstr(ans, "�"));
    free(ans);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"rd1\",\"body\":{\"operation_id\":"
             "\"op\",\"deadline_ms\":100}}");
    char *lc = read_msg(conn);
    free(lc);
}

static void t_binary_refused(void) {
    mx_handler_t h;
    memset(&h, 0, sizeof(h));
    h.on_call = bin_call;
    run_case(&h, "[]", "[]", script_bin);
    printf("ok binary-refused\n");
}

int main(void) {
    /* no locale tricks: bytes are bytes; UTF-8 validated explicitly */
    t_gen_equal();
    t_malformed_survives();
    t_stale_ignored();
    t_feature_gate();
    t_dep_roundtrip();
    t_flood();
    t_binary_refused();
    printf("selftest: all pass\n");
    return 0;
}
