/* Operator-surface tests for the C SDK (POSIX only, no deps).
 * Fake-binary mapping, bootstrap phases, doctor shape; live parts need
 * MX_MATRIX_MANAGED + MX_DEV_PKI. Prints "ok <name>", nonzero on failure. */
#define _POSIX_C_SOURCE 200809L

#include "mx_component.h"
#include "mx_json.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHECK(c)                                                                \
    do {                                                                        \
        if (!(c)) {                                                             \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
            exit(1);                                                            \
        }                                                                       \
    } while (0)

static char *xstrdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *c = (char *)malloc(n);
    if (c)
        memcpy(c, s, n);
    return c;
}

static void write_fake_binary(const char *dir, char *fake_out,
                              size_t fake_cap) {
    snprintf(fake_out, fake_cap, "%s/matrix-managed", dir);
    FILE *f = fopen(fake_out, "w");
    CHECK(f);
    fprintf(f,
            "#!/bin/sh\n"
            "if [ \"$1\" = \"request\" ]; then\n"
            "  case \"$7\" in\n"
            "    *sleep*) exec sleep 30;;\n"
            "    *badjson*) echo 'not json';;\n"
            "    *denied*) echo 'permission-denied: nope' >&2; exit 1;;\n"
            "    *) echo '{\"ok\":true}';;\n"
            "  esac\n"
            "else echo 'usage: matrix-managed serve <config>' >&2; exit 1\n"
            "fi\n");
    fclose(f);
    CHECK(chmod(fake_out, 0755) == 0);
    const char *names[3] = {"ca", "cert", "key"};
    for (int i = 0; i < 3; i++) {
        char p[1024];
        snprintf(p, sizeof(p), "%s/%s", dir, names[i]);
        FILE *g = fopen(p, "w");
        CHECK(g);
        fclose(g);
    }
}

static void t_mapping(void) {
    char tmpl[] = "/tmp/opfake-XXXXXX";
    CHECK(mkdtemp(tmpl));
    char fake[1024];
    write_fake_binary(tmpl, fake, sizeof(fake));
    char ca[1024], cert[1024], key[1024];
    snprintf(ca, sizeof(ca), "%s/ca", tmpl);
    snprintf(cert, sizeof(cert), "%s/cert", tmpl);
    snprintf(key, sizeof(key), "%s/key", tmpl);
    mx_op_client_t *c = NULL;
    char *err = NULL;
    CHECK(mx_op_connect(fake, "127.0.0.1:9", ca, cert, key, "localhost",
                        &c, &err) == MX_OK);
    free(err);
    char *resp = NULL, *code = NULL, *msg = NULL;
    CHECK(mx_op_request(c, "{\"action\":\"ping\"}", 30000, &resp, &code,
                        &msg) == MX_OK);
    CHECK(resp && strstr(resp, "\"ok\":true"));
    free(resp);
    resp = NULL;
    CHECK(mx_op_request(c, "{\"action\":\"denied-op\"}", 30000, &resp,
                        &code, &msg) == MX_ERR_DENIED);
    CHECK(code && !strcmp(code, "permission-denied"));
    free(code);
    free(msg);
    code = msg = NULL;
    CHECK(mx_op_request(c, "{\"action\":\"badjson\"}", 30000, &resp,
                        &code, &msg) == MX_ERR_PROTOCOL);
    CHECK(mx_op_request(c, "{\"action\":\"sleep\"}", 1000, &resp, &code,
                        &msg) == MX_ERR_UNKNOWN);
    CHECK(mx_op_request(c, "{\"no-action\":true}", 30000, &resp, &code,
                        &msg) == MX_ERR_DENIED);
    CHECK(code && !strcmp(code, "invalid-message"));
    free(code);
    free(msg);
    mx_op_client_close(c);
    c = NULL;
    CHECK(mx_op_request(c, "{\"action\":\"ping\"}", 30000, &resp, &code,
                        &msg) == MX_ERR_CLOSED);
    char cmd[1100];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpl);
    int rc = system(cmd);
    (void)rc;
    printf("ok mapping\n");
}

static void t_bootstrap_phases(void) {
    mx_op_kernel_t *k = NULL;
    char *err = NULL;
    CHECK(mx_op_start("/nonexistent/matrix-managed", "/tmp/x", "a", "b",
                      "c", "localhost", &k, &err) == MX_ERR_TRANSPORT);
    free(err);
    err = NULL;
    CHECK(mx_op_start("/bin/true", "/tmp/x", "a", "b", "c", "localhost",
                      &k, &err) == MX_ERR_INVALID);
    free(err);
    err = NULL;
    mx_op_client_t *c = NULL;
    CHECK(mx_op_connect("/nonexistent/x", "127.0.0.1:1", "a", "b", "c",
                        "localhost", &c, &err) == MX_ERR_TRANSPORT);
    free(err);
    printf("ok bootstrap-phases\n");
}

static void t_doctor_shape(void) {
    char *rep = mx_op_doctor("/nonexistent/binary");
    CHECK(rep);
    CHECK(strstr(rep, "\"binary_found\":false"));
    CHECK(strstr(rep, "\"errors\""));
    /* no secrets adjacent (shape only; redaction is vacuous here) */
    CHECK(!strstr(rep, "lease_token_secret"));
    free(rep);
    const char *bin = getenv("MX_MATRIX_MANAGED");
    if (bin) {
        char *rep2 = mx_op_doctor(bin);
        CHECK(rep2);
        CHECK(strstr(rep2, "\"binary_found\":true"));
        CHECK(strstr(rep2, "\"cli_shape_ok\":true"));
        free(rep2);
    }
    printf("ok doctor-shape\n");
}

static void sha256_file(const char *path, char out[65]) {
    /* fingerprint via sha256sum (test-only; SDK never shells out) */
    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "sha256sum %s", path);
    FILE *p = popen(cmd, "r");
    CHECK(p);
    CHECK(fscanf(p, "%64s", out) == 1);
    pclose(p);
}

static void t_live(void) {
    const char *bin = getenv("MX_MATRIX_MANAGED");
    const char *pki_script = getenv("MX_DEV_PKI");
    if (!bin || !pki_script) {
        printf("skip live-lifecycle (needs MX_MATRIX_MANAGED + MX_DEV_PKI)\n");
        return;
    }
    char tmpl[] = "/tmp/oplive-XXXXXX";
    CHECK(mkdtemp(tmpl));
    char pki[1024];
    snprintf(pki, sizeof(pki), "%s/pki", tmpl);
    char cmd[2048];
    snprintf(cmd, sizeof(cmd),
             "python3 %s %s --server-name localhost >/dev/null 2>&1",
             pki_script, pki);
    CHECK(system(cmd) == 0);
    char client_der[1100];
    snprintf(client_der, sizeof(client_der), "%s/client.der", pki);
    char fp[65];
    sha256_file(client_der, fp);
    char home[1024];
    snprintf(home, sizeof(home), "%s/home", tmpl);
    char cfg[1100];
    snprintf(cfg, sizeof(cfg), "%s/config.json", tmpl);
    FILE *f = fopen(cfg, "w");
    CHECK(f);
    fprintf(f,
            "{\"home\":\"%s\","
            "\"components\":[{\"manifest\":{\"id\":\"echo\","
            "\"capabilities\":[\"echo.msg@1\"],\"reducer\":\"echo\"},"
            "\"trusted\":true}],"
            "\"grants\":{\"%s\":{\"components\":[\"echo\"],"
            "\"capabilities\":[\"echo.msg@1\"]}},"
            "\"tls\":{\"listen\":\"127.0.0.1:0\",\"ca\":\"%s/ca.der\","
            "\"cert\":\"%s/server.der\",\"key\":\"%s/server-key.der\"}}",
            home, fp, pki, pki, pki);
    fclose(f);
    char ca[1100], cert[1100], key[1100];
    snprintf(ca, sizeof(ca), "%s/ca.der", pki);
    snprintf(cert, sizeof(cert), "%s/client.der", pki);
    snprintf(key, sizeof(key), "%s/client-key.der", pki);
    mx_op_kernel_t *k = NULL;
    char *err = NULL;
    CHECK(mx_op_start(bin, cfg, ca, cert, key, "localhost", &k, &err) ==
          MX_OK);
    free(err);
    mx_op_client_t *client = mx_op_kernel_client(k);
    CHECK(client);
    char *resp = NULL, *code = NULL, *msg = NULL;
    CHECK(mx_op_request(client,
                        "{\"action\":\"activate\",\"component\":\"echo\","
                        "\"ttl_ms\":20000}",
                        30000, &resp, &code, &msg) == MX_OK);
    mxj_t *act = mxj_parse(resp);
    free(resp);
    CHECK(act);
    const char *lease = mxj_str(mxj_field(act, "lease"));
    const char *fence = mxj_str(mxj_field(act, "fence"));
    CHECK(lease && fence);
    char *lease_c = xstrdup(lease);
    char *fence_c = xstrdup(fence);
    mxj_free(act);
    char invoke[2048];
    snprintf(invoke, sizeof(invoke),
             "{\"action\":\"invoke\",\"lease\":\"%s\",\"fence\":\"%s\","
             "\"operation\":\"op-live-1\",\"cap\":\"echo.msg@1\",\"input\":"
             "{\"ping\":1}}",
             lease_c, fence_c);
    CHECK(mx_op_request(client, invoke, 30000, &resp, &code, &msg) ==
          MX_OK);
    CHECK(strstr(resp, "\"ok\":true"));
    free(resp);
    /* attach shares the daemon: closing the attachment kills nothing */
    const char *listen = mx_op_kernel_listen(k);
    CHECK(listen && *listen);
    mx_op_client_t *attached = NULL;
    CHECK(mx_op_connect(bin, listen, ca, cert, key, "localhost",
                        &attached, &err) == MX_OK);
    free(err);
    snprintf(invoke, sizeof(invoke),
             "{\"action\":\"invoke\",\"lease\":\"%s\",\"fence\":"
             "\"%s\",\"operation\":\"op-live-2\",\"cap\":"
             "\"echo.msg@1\",\"input\":{}}",
             lease_c, fence_c);
    CHECK(mx_op_request(attached, invoke, 30000, &resp, &code, &msg) ==
          MX_OK);
    free(resp);
    mx_op_client_close(attached);
    snprintf(invoke, sizeof(invoke),
             "{\"action\":\"invoke\",\"lease\":\"%s\",\"fence\":"
             "\"%s\",\"operation\":\"op-live-3\",\"cap\":"
             "\"echo.msg@1\",\"input\":{}}",
             lease_c, fence_c);
    CHECK(mx_op_request(client, invoke, 30000, &resp, &code, &msg) ==
          MX_OK);
    free(resp);
    snprintf(invoke, sizeof(invoke),
             "{\"action\":\"invoke\",\"lease\":\"dead\",\"fence\":\"1\","
             "\"operation\":\"op-x\",\"cap\":\"echo.msg@1\",\"input\":{}}");
    CHECK(mx_op_request(client, invoke, 30000, &resp, &code, &msg) ==
          MX_ERR_DENIED);
    free(code);
    free(msg);
    code = msg = NULL;
    snprintf(invoke, sizeof(invoke),
             "{\"action\":\"release\",\"lease\":\"%s\",\"fence\":\"%s\"}",
             lease_c, fence_c);
    CHECK(mx_op_request(client, invoke, 30000, &resp, &code, &msg) ==
          MX_OK);
    free(resp);
    free(lease_c);
    free(fence_c);
    mx_op_kernel_close(k);
    k = NULL;
    snprintf(cmd, sizeof(cmd), "rm -rf %s", tmpl);
    int rc = system(cmd);
    (void)rc;
    printf("ok live-lifecycle\n");
}

int main(void) {
    t_mapping();
    t_bootstrap_phases();
    t_doctor_shape();
    t_live();
    printf("selftest: all pass\n");
    return 0;
}
