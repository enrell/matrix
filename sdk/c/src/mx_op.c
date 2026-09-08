/* Matrix C SDK: operator/application surface (via matrix-managed). */
#define _POSIX_C_SOURCE 200809L

#include "mx_component.h"
#include "mx_json.h"

#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

struct mx_op_client {
    char *binary;
    char *listen;
    char *ca, *cert, *key;
    char *server_name;
    int owned;
};

struct mx_op_kernel {
    pid_t pid;
    char *workdir;
    mx_op_client_t *client;
    char *epoch;
    char *api;
    char *profile;
    int closed;
};

static char *xdup(const char *s) {
    if (!s)
        return NULL;
    size_t n = strlen(s) + 1;
    char *c = (char *)malloc(n);
    if (c)
        memcpy(c, s, n);
    return c;
}

static const char *KNOWN_CODES[] = {
    "permission-denied", "stale-generation", "outcome-unknown",
    "unauthenticated", "invalid-message", "unsupported-version",
    "dependency-unavailable", "ambiguous-provider", "context-not-active",
    "resource-exhausted", "deadline-exceeded", "cancelled",
    "cleanup-pending", "internal", NULL
};

static char *guess_code(const char *text) {
    if (!text)
        return xdup("internal");
    size_t n = strlen(text);
    char *low = (char *)malloc(n + 1);
    if (!low)
        return xdup("transport");
    for (size_t i = 0; i < n; i++)
        low[i] = (char)tolower((unsigned char)text[i]);
    low[n] = '\0';
    const char *found = NULL;
    for (int i = 0; KNOWN_CODES[i]; i++)
        if (strstr(low, KNOWN_CODES[i])) {
            found = KNOWN_CODES[i];
            break;
        }
    free(low);
    if (found)
        return xdup(found);
    /* blank -> internal, anything else -> transport */
    for (const char *p = text; *p; p++)
        if (*p != ' ' && *p != '\t' && *p != '\n' && *p != '\r')
            return xdup("transport");
    return xdup("internal");
}

/* Runs binary argv with an absolute deadline (merged streams:
 * `request` prints pure JSON on success). Returns exit code in
 * *code_out and malloc'd output in *out_out. *timed_out set on
 * deadline kill. Never blocks past the deadline. */
static int run_capture(char *const argv[], unsigned long long timeout_ms,
                       int *code_out, char **out_out, int *timed_out) {
    int pipefd[2];
    if (pipe(pipefd) != 0)
        return -1;
    pid_t pid = fork();
    if (pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    if (pid == 0) {
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[0]);
        close(pipefd[1]);
        execv(argv[0], argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t cap = 4096, len = 0;
    char *buf = (char *)malloc(cap);
    if (!buf) {
        close(pipefd[0]);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        return -1;
    }
    int done = 0, status = 0, killed = 0;
    int out_eof = 0;
    unsigned long long waited = 0;
    for (;;) {
        if (!out_eof) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(pipefd[0], &rfds);
            struct timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 50000; /* 50ms slices */
            int r = select(pipefd[0] + 1, &rfds, NULL, NULL, &tv);
            if (r > 0) {
                if (len + 1024 > cap) {
                    cap *= 2;
                    char *nb = (char *)realloc(buf, cap);
                    if (!nb) {
                        free(buf);
                        close(pipefd[0]);
                        kill(pid, SIGKILL);
                        waitpid(pid, NULL, 0);
                        return -1;
                    }
                    buf = nb;
                }
                ssize_t n = read(pipefd[0], buf + len, 1024);
                if (n < 0 && errno == EINTR) {
                    /* retry */
                } else if (n <= 0) {
                    out_eof = 1; /* EOF: stop selecting, poll exit */
                } else {
                    len += (size_t)n;
                }
            }
        } else {
            struct timespec ts;
            ts.tv_sec = 0;
            ts.tv_nsec = 50000000;
            nanosleep(&ts, NULL);
            waited += 50;
        }
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid) {
            if (!out_eof) {
                /* drain what is left, then finish */
                for (;;) {
                    if (len + 1024 > cap) {
                        cap += 1024;
                        char *nb = (char *)realloc(buf, cap);
                        if (!nb)
                            break;
                        buf = nb;
                    }
                    ssize_t n = read(pipefd[0], buf + len, 1024);
                    if (n <= 0)
                        break;
                    len += (size_t)n;
                }
            }
            done = 1;
            break;
        }
        if (!out_eof)
            waited += 50;
        if (waited >= timeout_ms) {
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
            killed = 1;
            break;
        }
    }
    close(pipefd[0]);
    buf[len] = '\0';
    if (code_out)
        *code_out = done && WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    if (out_out)
        *out_out = buf;
    else
        free(buf);
    if (timed_out)
        *timed_out = killed;
    return 0;
}

static mx_status_t op_request_impl(mx_op_client_t *c, const char *action_json,
                                   unsigned long long timeout_ms,
                                   char **resp_out, char **code_out,
                                   char **msg_out) {
    if (resp_out)
        *resp_out = NULL;
    if (code_out)
        *code_out = NULL;
    if (msg_out)
        *msg_out = NULL;
    if (!c)
        return MX_ERR_CLOSED;
    if (!action_json)
        return MX_ERR_INVALID;
    /* Local shape gate (mirrors the other SDKs): an action object
     * with a string 'action' is required, refused before any spawn. */
    {
        mxj_t *dom = mxj_parse(action_json);
        const mxj_t *a = dom ? mxj_field(dom, "action") : NULL;
        int ok = dom && dom->type == MXJ_OBJ && a && a->type == MXJ_STR;
        mxj_free(dom);
        if (!ok) {
            if (code_out)
                *code_out = xdup("invalid-message");
            if (msg_out)
                *msg_out = xdup("action object with 'action' required");
            return MX_ERR_DENIED;
        }
    }
    if (timeout_ms == 0)
        timeout_ms = 30000;
    char *argv[9];
    argv[0] = c->binary;
    argv[1] = "request";
    argv[2] = c->ca;
    argv[3] = c->cert;
    argv[4] = c->key;
    argv[5] = c->listen;
    argv[6] = c->server_name;
    argv[7] = (char *)action_json;
    argv[8] = NULL;
    int code = -1, timed_out = 0;
    char *text = NULL;
    /* merged streams: pure JSON on success, denial text on failure */
    if (run_capture(argv, timeout_ms + 10000, &code, &text, &timed_out) != 0)
        return MX_ERR_TRANSPORT;
    mx_status_t st;
    if (timed_out) {
        free(text);
        return MX_ERR_UNKNOWN; /* never retried implicitly */
    }
    if (code == 0) {
        mxj_t *dom = mxj_parse(text ? text : "");
        free(text);
        if (!dom || dom->type != MXJ_OBJ) {
            mxj_free(dom);
            return MX_ERR_PROTOCOL;
        }
        char *raw = mxj_print(dom);
        mxj_free(dom);
        if (!raw)
            return MX_ERR_NOMEM;
        if (resp_out)
            *resp_out = raw;
        else
            free(raw);
        st = MX_OK;
    } else {
        char *first = NULL;
        if (text) {
            char *nl = strchr(text, '\n');
            size_t n = nl ? (size_t)(nl - text) : strlen(text);
            while (n && (text[n - 1] == '\r' || text[n - 1] == ' ' || text[n - 1] == '\t'))
                n--;
            first = (char *)malloc(n + 1);
            if (first) {
                memcpy(first, text, n);
                first[n] = '\0';
            }
        }
        char *g = guess_code(text ? text : "");
        free(text);
        if (code_out)
            *code_out = g;
        else
            free(g);
        if (msg_out)
            *msg_out = (first && *first) ? first : xdup("request refused");
        else
            free(first);
        st = MX_ERR_DENIED;
    }
    return st;
}

mx_status_t mx_op_request(mx_op_client_t *c, const char *action_json,
                          unsigned long long timeout_ms, char **resp_out,
                          char **code_out, char **msg_out) {
    return op_request_impl(c, action_json, timeout_ms, resp_out, code_out,
                           msg_out);
}

static mx_op_client_t *client_new(const char *binary, const char *listen,
                                  const char *ca, const char *cert,
                                  const char *key, const char *server_name,
                                  int owned) {
    mx_op_client_t *c = (mx_op_client_t *)calloc(1, sizeof(*c));
    if (!c)
        return NULL;
    c->binary = xdup(binary);
    c->listen = xdup(listen);
    c->ca = xdup(ca);
    c->cert = xdup(cert);
    c->key = xdup(key);
    c->server_name = xdup(server_name && *server_name ? server_name : "localhost");
    c->owned = owned;
    if (!c->binary || !c->listen || !c->ca || !c->cert || !c->key || !c->server_name) {
        mx_op_client_close(c);
        return NULL;
    }
    return c;
}

static int exists(const char *p) {
    struct stat st;
    return p && !stat(p, &st);
}

mx_status_t mx_op_connect(const char *binary, const char *listen,
                          const char *ca, const char *cert, const char *key,
                          const char *server_name, mx_op_client_t **out,
                          char **err_out) {
    if (out)
        *out = NULL;
    if (err_out)
        *err_out = NULL;
    const char *labels[4] = {"binary", "ca", "cert", "key"};
    const char *paths[4] = {binary, ca, cert, key};
    for (int i = 0; i < 4; i++) {
        if (!paths[i] || !exists(paths[i])) {
            if (err_out) {
                char eb[256];
                snprintf(eb, sizeof(eb), "%s not found: %s", labels[i],
                         paths[i] ? paths[i] : "(null)");
                *err_out = xdup(eb);
            }
            return MX_ERR_TRANSPORT;
        }
    }
    if (!listen || !*listen) {
        if (err_out)
            *err_out = xdup("listen address required");
        return MX_ERR_INVALID;
    }
    mx_op_client_t *c =
        client_new(binary, listen, ca, cert, key, server_name, 0);
    if (!c) {
        if (err_out)
            *err_out = xdup("out of memory");
        return MX_ERR_NOMEM;
    }
    *out = c;
    return MX_OK;
}

void mx_op_client_close(mx_op_client_t *c) {
    if (!c)
        return;
    free(c->binary);
    free(c->listen);
    free(c->ca);
    free(c->cert);
    free(c->key);
    free(c->server_name);
    free(c);
}

/* --- owned kernel ---------------------------------------------------- */

static int executable(const char *p) {
    struct stat st;
    return p && !stat(p, &st) && S_ISREG(st.st_mode) && (st.st_mode & 0111);
}

static int on_path(const char *name) {
    const char *path = getenv("PATH");
    if (!path || !name)
        return 0;
    const char *p = path;
    char dir[1024];
    for (;;) {
        const char *c = strchr(p, ':');
        size_t n = c ? (size_t)(c - p) : strlen(p);
        if (n > 0 && n < sizeof(dir) - 64) {
            memcpy(dir, p, n);
            dir[n] = '\0';
            size_t m = strlen(dir);
            dir[m++] = '/';
            strcpy(dir + m, name);
            if (!access(dir, X_OK))
                return 1;
        }
        if (!c)
            break;
        p = c + 1;
    }
    return 0;
}

static void rm_rf(const char *dir) {
    /* best-effort recursive remove via forked rm (no nftw dependency
     * games; the dir is ours, created by mkdtemp). */
    if (!dir)
        return;
    pid_t pid = fork();
    if (pid == 0) {
        execl("/bin/rm", "rm", "-rf", "--", dir, (char *)NULL);
        _exit(127);
    }
    if (pid > 0) {
        int status = 0;
        waitpid(pid, &status, 0);
    }
}

static void stop_kernel(mx_op_kernel_t *k) {
    /* The daemon is our child: WNOHANG first (already exited costs
     * no signal), then SIGTERM, bounded poll, SIGKILL, blocking reap
     * (a SIGKILLed child cannot hang the reap). */
    int status = 0;
    if (!k || k->pid <= 0)
        return;
    if (waitpid(k->pid, &status, WNOHANG) == k->pid) {
        k->pid = -1;
        return;
    }
    kill(k->pid, SIGTERM);
    for (int i = 0; i < 50; i++) {
        if (waitpid(k->pid, &status, WNOHANG) == k->pid) {
            k->pid = -1;
            return;
        }
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 100000000; /* 100ms */
        nanosleep(&ts, NULL);
    }
    kill(k->pid, SIGKILL);
    while (waitpid(k->pid, &status, 0) < 0 && errno == EINTR) {
    }
    k->pid = -1;
}

mx_status_t mx_op_start(const char *binary, const char *config_path,
                        const char *ca, const char *cert, const char *key,
                        const char *server_name, mx_op_kernel_t **out,
                        char **err_out) {
    if (out)
        *out = NULL;
    if (err_out)
        *err_out = NULL;
    if (!executable(binary ? binary : "")) {
        if (err_out) {
            char eb[256];
            snprintf(eb, sizeof(eb), "binary not executable: %s",
                     binary ? binary : "(null)");
            *err_out = xdup(eb);
        }
        return MX_ERR_TRANSPORT;
    }
    if (!config_path || !exists(config_path)) {
        if (err_out)
            *err_out = xdup("config file required");
        return MX_ERR_INVALID;
    }
    const char *labels[3] = {"ca", "cert", "key"};
    const char *paths[3] = {ca, cert, key};
    for (int i = 0; i < 3; i++) {
        if (!paths[i] || !*paths[i] || !exists(paths[i])) {
            if (err_out) {
                char eb[256];
                snprintf(eb, sizeof(eb),
                         "operator %s is required: server identity never "
                         "implies caller authority",
                         labels[i]);
                *err_out = xdup(eb);
            }
            return MX_ERR_INVALID;
        }
    }
    char tmpl[] = "/tmp/mx-c-XXXXXX";
    if (!mkdtemp(tmpl)) {
        if (err_out)
            *err_out = xdup("mkdtemp failed");
        return MX_ERR_TRANSPORT;
    }
    char cfgdst[512];
    snprintf(cfgdst, sizeof(cfgdst), "%s/config.json", tmpl);
    /* copy config in (never mutate the caller's file) */
    {
        FILE *in = fopen(config_path, "rb");
        FILE *dst = in ? fopen(cfgdst, "wb") : NULL;
        if (!in || !dst) {
            if (in)
                fclose(in);
            if (dst)
                fclose(dst);
            rm_rf(tmpl);
            if (err_out)
                *err_out = xdup("config copy failed");
            return MX_ERR_TRANSPORT;
        }
        char chunk[8192];
        size_t n;
        while ((n = fread(chunk, 1, sizeof(chunk), in)) > 0)
            if (fwrite(chunk, 1, n, dst) != n)
                break;
        fclose(in);
        fclose(dst);
    }
    int outpipe[2], errpipe[2];
    if (pipe(outpipe) != 0 || pipe(errpipe) != 0) {
        if (err_out)
            *err_out = xdup("pipe failed");
        rm_rf(tmpl);
        return MX_ERR_TRANSPORT;
    }
    pid_t pid = fork();
    if (pid < 0) {
        close(outpipe[0]);
        close(outpipe[1]);
        close(errpipe[0]);
        close(errpipe[1]);
        rm_rf(tmpl);
        if (err_out)
            *err_out = xdup("fork failed");
        return MX_ERR_TRANSPORT;
    }
    if (pid == 0) {
        dup2(outpipe[1], STDOUT_FILENO);
        dup2(errpipe[1], STDERR_FILENO);
        close(outpipe[0]);
        close(outpipe[1]);
        close(errpipe[0]);
        close(errpipe[1]);
        execl(binary, binary, "serve", cfgdst, (char *)NULL);
        _exit(127);
    }
    close(outpipe[1]);
    close(errpipe[1]);
    /* read the ready line (bounded 30s), keep stderr tail for errors */
    char line[4096];
    size_t llen = 0;
    char errtail[2048];
    size_t elen = 0;
    int got_line = 0, exited = 0, exit_code = -1;
    unsigned long long waited = 0;
    while (!got_line && !exited && waited < 30000) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(outpipe[0], &rfds);
        FD_SET(errpipe[0], &rfds);
        int nfds = outpipe[0] > errpipe[0] ? outpipe[0] : errpipe[0];
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100000;
        int r = select(nfds + 1, &rfds, NULL, NULL, &tv);
        if (r > 0) {
            if (FD_ISSET(outpipe[0], &rfds)) {
                char ch;
                ssize_t n = read(outpipe[0], &ch, 1);
                if (n <= 0) {
                    /* stdout EOF before ready line */
                } else if (ch == '\n') {
                    got_line = 1;
                } else if (llen + 1 < sizeof(line)) {
                    line[llen++] = ch;
                }
            }
            if (FD_ISSET(errpipe[0], &rfds)) {
                char chunk[512];
                ssize_t n = read(errpipe[0], chunk, sizeof(chunk));
                if (n > 0) {
                    if (elen + (size_t)n >= sizeof(errtail)) {
                        size_t drop = elen + (size_t)n - sizeof(errtail) + 1;
                        memmove(errtail, errtail + drop, elen - drop);
                        elen -= drop;
                    }
                    memcpy(errtail + elen, chunk, (size_t)n);
                    elen += (size_t)n;
                    errtail[elen] = '\0';
                }
            }
        }
        int status = 0;
        pid_t w = waitpid(pid, &status, WNOHANG);
        if (w == pid) {
            exited = 1;
            if (WIFEXITED(status))
                exit_code = WEXITSTATUS(status);
        }
        if (!got_line && !exited)
            waited += 100;
    }
    line[llen] = '\0';
    if (!got_line) {
        /* reap everything created (no orphans) */
        if (!exited) {
            kill(pid, SIGKILL);
            waitpid(pid, NULL, 0);
        }
        close(outpipe[0]);
        close(errpipe[0]);
        rm_rf(tmpl);
        if (err_out) {
            if (exited) {
                char eb[1024];
                const char *tail = elen ? errtail : "daemon exited";
                snprintf(eb, sizeof(eb), "daemon refused config: %.900s",
                         tail);
                *err_out = xdup(eb);
            } else {
                *err_out = xdup("no ready line in budget");
            }
        }
        (void)exit_code;
        return exited ? MX_ERR_INVALID : MX_ERR_TRANSPORT;
    }
    /* parse the ready line */
    mxj_t *ready = mxj_parse(line);
    const mxj_t *rv = ready ? mxj_field(ready, "ready") : NULL;
    const mxj_t *av = ready ? mxj_field(ready, "api") : NULL;
    int ok = ready && rv && rv->type == MXJ_BOOL && rv->boolean;
    char *api = (ok && av && av->type == MXJ_STR && av->str) ? xdup(av->str) : xdup("");
    int vers_ok = 1;
    if (api && *api && strncmp(api, "0.1.", 4))
        vers_ok = 0;
    if (!ok || !vers_ok) {
        mxj_free(ready);
        free(api);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        close(outpipe[0]);
        close(errpipe[0]);
        rm_rf(tmpl);
        if (err_out)
            *err_out = xdup(!ok ? "daemon not ready" : "binary api outside 0.1.x");
        return MX_ERR_PROTOCOL;
    }
    const mxj_t *lv = mxj_field(ready, "listen");
    const mxj_t *ev = mxj_field(ready, "epoch");
    const mxj_t *pv = mxj_field(ready, "profile");
    char *listen = (lv && lv->type == MXJ_STR && lv->str) ? xdup(lv->str) : xdup("");
    char *epoch = ev ? mxj_print(ev) : xdup("null");
    char *profile =
        (pv && pv->type == MXJ_STR && pv->str) ? xdup(pv->str) : xdup("");
    mxj_free(ready);
    close(outpipe[0]);
    close(errpipe[0]);
    mx_op_client_t *client =
        client_new(binary, listen, ca, cert, key, server_name, 1);
    free(listen);
    if (!client) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        rm_rf(tmpl);
        free(api);
        free(epoch);
        free(profile);
        if (err_out)
            *err_out = xdup("out of memory");
        return MX_ERR_NOMEM;
    }
    mx_op_kernel_t *k = (mx_op_kernel_t *)calloc(1, sizeof(*k));
    if (!k) {
        mx_op_client_close(client);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        rm_rf(tmpl);
        free(api);
        free(epoch);
        free(profile);
        if (err_out)
            *err_out = xdup("out of memory");
        return MX_ERR_NOMEM;
    }
    k->pid = pid;
    k->workdir = xdup(tmpl);
    k->client = client;
    k->epoch = epoch;
    k->api = api;
    k->profile = profile;
    *out = k;
    return MX_OK;
}

void mx_op_kernel_close(mx_op_kernel_t *k) {
    /* Single-shot and NULL-safe: reaps exactly the spawned daemon,
     * frees the client, the private directory record and the struct.
     * Do not touch the handle afterwards (C has no second-close). */
    if (!k)
        return;
    stop_kernel(k);
    rm_rf(k->workdir);
    mx_op_client_close(k->client);
    free(k->workdir);
    free(k->epoch);
    free(k->api);
    free(k->profile);
    free(k);
}

mx_op_client_t *mx_op_kernel_client(mx_op_kernel_t *k) {
    return k ? k->client : NULL;
}

const char *mx_op_kernel_listen(mx_op_kernel_t *k) {
    return (k && k->client) ? k->client->listen : "";
}

const char *mx_op_kernel_api(mx_op_kernel_t *k) {
    return (k && k->api) ? k->api : "";
}

char *mx_op_doctor(const char *binary) {
    /* Environment diagnosis as JSON (no secrets). Always succeeds;
     * check the "errors" array. */
    int found = binary && exists(binary);
    int execable = found && executable(binary);
    int shape_ok = 0;
    size_t ecap = 4, elen = 0;
    char **errs = (char **)calloc(ecap, sizeof(char *));
    if (execable) {
        char *argv[2];
        argv[0] = (char *)binary;
        argv[1] = NULL;
        int code = -1, timed_out = 0;
        char *text = NULL;
        if (run_capture(argv, 10000, &code, &text, &timed_out) == 0 && text) {
            if (strstr(text, "matrix-managed serve"))
                shape_ok = 1;
            else if (elen < ecap)
                errs[elen++] = xdup("binary does not speak the managed CLI shape");
        } else if (elen < ecap) {
            errs[elen++] = xdup("binary probe failed");
        }
        free(text);
    } else if (!found) {
        if (elen < ecap)
            errs[elen++] = xdup("binary not found: set it explicitly or via PATH (no silent download)");
    } else {
        if (elen < ecap)
            errs[elen++] = xdup("binary not executable");
    }
    /* socket dir probe */
    char dtmpl[] = "/tmp/mx-doc-XXXXXX";
    int sock_ok = 0;
    if (mkdtemp(dtmpl)) {
        char sp[512];
        snprintf(sp, sizeof(sp), "%s/t.sock", dtmpl);
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd >= 0) {
            struct sockaddr_un addr;
            memset(&addr, 0, sizeof(addr));
            addr.sun_family = AF_UNIX;
            size_t sl = strlen(sp);
            if (sl >= sizeof(addr.sun_path)) {
                close(fd);
            } else {
                memcpy(addr.sun_path, sp, sl + 1);
                if (bind(fd, (struct sockaddr *)&addr,
                         (socklen_t)(sizeof(addr.sun_family) + sl + 1)) == 0)
                    sock_ok = 1;
                else if (elen < ecap) {
                    char eb[256];
                    snprintf(eb, sizeof(eb), "unix socket probe failed: %s",
                             strerror(errno));
                    errs[elen++] = xdup(eb);
                }
                close(fd);
            }
        }
        rm_rf(dtmpl);
    }
    const char *acc = on_path("openssl") ? "true" : "false";
    const char *bwr = on_path("bwrap") ? "true" : "false";
    char *q_bin = mxj_quote(binary ? binary : "", strlen(binary ? binary : ""));
    size_t n = 1024 + (q_bin ? strlen(q_bin) : 0);
    for (size_t i = 0; i < elen; i++)
        n += strlen(errs[i]) + 16;
    char *out = (char *)malloc(n);
    if (out) {
        int w = snprintf(out, n,
                         "{\"binary\":%s,\"binary_found\":%s,"
                         "\"binary_executable\":%s,\"cli_shape_ok\":%s,"
                         "\"openssl\":%s,\"bwrap\":%s,"
                         "\"socket_dir_writable\":%s,\"errors\":[",
                         q_bin ? q_bin : "\"\"",
                         found ? "true" : "false",
                         execable ? "true" : "false",
                         shape_ok ? "true" : "false", acc, bwr,
                         sock_ok ? "true" : "false");
        for (size_t i = 0; i < elen && w > 0; i++) {
            char *q = mxj_quote(errs[i], strlen(errs[i]));
            w += snprintf(out + w, w < (int)n ? n - (size_t)w : 0,
                          "%s%s", i ? "," : "", q ? q : "\"\"");
            free(q);
        }
        if ((size_t)w < n)
            snprintf(out + w, n - (size_t)w, "]}");
    }
    free(q_bin);
    for (size_t i = 0; i < elen; i++)
        free(errs[i]);
    free(errs);
    return out;
}
