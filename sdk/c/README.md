# matrix-component (C SDK) + matrix.hpp (C++ SDK)

Experimental Matrix SDKs for C and C++ (ML1 contract surface, version
`0.1.0`). One package, two SDK entries: the C SDK (`mx_component.h`,
C11 + POSIX) does the protocol work; the C++ SDK (`cpp/matrix.hpp`,
C++17, header-only RAII) owns lifetimes over the same transport.
Dual-licensed MIT OR Apache-2.0 (see LICENSE-MIT, LICENSE-APACHE-2.0);
registry publication in progress.

Contract reference: `docs/SDK.md`, `docs/ML1-NODE.md`,
`docs/ML1-MATRIX.md` in the Matrix repository. Same observable
behavior as the reference SDKs. The JSON subset parser
(`src/mx_json.{h,c}`) is SDK-internal (no third-party code).

## Install (CMake and/or pkg-config, offline)

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DMATRIX_ENABLE_CPP=ON
cmake --build build
ctest --test-dir build   # loopback + operator suites (live need env)
cmake --install build --prefix /path/to/stage
```

C++ users add `cpp/` to the include path and link the C library
(`matrix-component.pc` covers both: `-lmatrix-component -lpthread`).
Compilers: GCC >= 11 or Clang >= 13 (tested GCC 16, linux/x86_64);
C++17 for the header.

## Component side (C)

```c
static mx_result_t on_call(mx_call_ctx_t *ctx, const char *ticket,
                           const char *cap, const char *input_json,
                           void *ud) {
    mx_result_t r = {0};
    if (strstr(input_json, "\"chain\":true")) {
        size_t n = 0;
        const mx_binding_t *b = mx_dependencies(ctx, &n);
        char *out = NULL;
        mx_status_t st = mx_invoke_dependency(ctx, b[0].id, "{}",
                                              5000, &out, NULL, NULL);
        if (st == MX_OK) { /* format {"chained":<out>} into r.output_json */ }
        mx_free(out);
        return r;
    }
    /* ... */
}
mx_handler_t h = {.on_call = on_call, /* ... */ };
mx_component_t *c = NULL;
mx_connect(sock_path, "echo", &h, &c); /* MATRIX_LAUNCH_TOKEN via env */
const char *reason = NULL;
mx_serve(c, &reason); /* "dispose" | "eof" */
mx_close(c);
```

## Component side (C++)

```cpp
struct Echo : mx::Handler {
    std::string on_call(mx::CallCtx &ctx, const std::string &,
                        const std::string &, const std::string &in) override {
        if (in.find("\"chain\":true") != std::string::npos) {
            auto deps = ctx.dependencies();
            std::string out = ctx.invoke_dependency(deps[0].id, "{}", 5000);
            return "{\"chained\":" + out + "}";
        }
        return "{\"echo\":" + in + "}";
    }
};
Echo handler;
mx::Component comp = mx::Component::connect(sock, "echo", handler);
std::string reason = comp.serve(); // "dispose" | "eof"
comp.close();                      // verifiable, idempotent, noexcept
```

- One thread per call with cooperative `mx_cancelled(ctx)` /
  `ctx.cancelled()`; late answers after cancel stay silent. Handlers
  return business errors (C: `mx_result_t`; C++: throw
  `mx::BusinessError`) with the provider code preserved.
- `mx_send_stream` / `send_stream`: text only — invalid UTF-8 or NUL
  bytes refuse (`MX_ERR_INVALID` / `invalid-message`), never
  lossy-converted.
- `u64` wire values are decimal strings at full precision
  (`mxj_parse_u64` refuses overflow, never wraps).
- Events/streams share a bounded edge queue (64, drop-oldest, counted
  in `mx_event_dropped_count()`); the dispatcher thread never runs on
  the reader, so slow observers throttle via host credit instead of
  stalling calls.
- Blocking calls refuse on the reader/dispatcher thread
  (`MX_ERR_THREAD`) instead of deadlocking the delivery they wait for.
- Without a negotiated `dependency-calls/1`, `mx_invoke_dependency`
  returns `MX_ERR_UNSUPPORTED` without touching the wire.
- Threading/affinity and ownership (who frees what, buffer lifetimes)
  are documented in `mx_component.h` — read it before embedding.

## Operator side

```c
mx_op_kernel_t *k = NULL;
char *err = NULL;
mx_op_start("/path/to/matrix-managed", cfg_path, ca, cert, key,
            "localhost", &k, &err);
mx_op_client_t *c = mx_op_kernel_client(k);
char *resp = NULL;
mx_op_request(c, "{\"action\":\"activate\",\"component\":\"prov\","
                 "\"ttl_ms\":20000}", 30000, &resp, NULL, NULL);
/* ... invoke ... */
mx_op_kernel_close(k); /* owned: reaps only this daemon */
```

```cpp
mx::OwnedKernel kernel = mx::OwnedKernel::start(
    "/path/to/matrix-managed", cfg_path, ca, cert, key);
std::string act = kernel.request(
    "{\"action\":\"activate\",\"component\":\"prov\",\"ttl_ms\":20000}");
```

Operator calls travel through the staged `matrix-managed` binary
(`serve`/`request`, mutual TLS). `mx_op_start` owns its daemon
(SIGTERM, bounded wait, SIGKILL — close frees the kernel and its
client, single-shot); `mx_op_connect` attaches (closing never stops a
shared kernel). PKI paths are always explicit. Timeouts report
`outcome-unknown`, never retry.

## Scaffold, doctor, tests

```sh
./scaffold.sh myapp ./myapp --pki ./pki --home ./priv-home [--cxx]
./build/matrix-doctor --binary /path/to/matrix-managed
ctest --test-dir build   # live parts need MX_MATRIX_MANAGED + MX_DEV_PKI
```

`matrix-doctor` prints environment diagnosis as JSON and carries no
secrets (one binary covers C and C++, same transport). Scaffolded
projects are covered by `templates/README.md`.
