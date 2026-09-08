/* Matrix external-component SDK for C (ML1, C11 + POSIX).
 *
 * Speaks matrix.component/0.1 with the local host: handshake,
 * registration, activation, and call serving with cooperative
 * cancellation. Same observable behavior as the reference SDKs.
 *
 * Threading (epic table: explicit threads, documented affinity):
 * - reader thread: frames in, never runs handler code;
 * - one detached thread per call runs on_call;
 * - dispatcher thread runs on_event/on_stream (bounded queue 64,
 *   drop-oldest, counted).
 * Blocking mx_*_wait calls made on the reader or dispatcher thread
 * itself refuse with MX_ERR_THREAD (they would deadlock the delivery
 * they wait for). on_event/on_stream must observe fast: slowness
 * throttles the sender via host credit instead of growing memory.
 *
 * Ownership: the SDK owns the component and all session strings it
 * returns (valid until mx_close). Handler-returned mx_result_t strings
 * are malloc'd by the handler and freed by the SDK. input_json and
 * ctx are borrowed (valid during the call only). invoke/acquire
 * outputs are malloc'd for the caller (mx_free, which is free).
 *
 * u64 values cross the wire as decimal strings at full precision;
 * out-of-range input is refused, never wrapped.
 */
#ifndef MX_COMPONENT_H
#define MX_COMPONENT_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MX_PROTOCOL_ID "matrix.component"
#define MX_PROTOCOL_VERSION "0.1"
#define MX_DEFAULT_MAX_FRAME (1024u * 1024u)
#define MX_EVENT_CAP 64
#define MX_DEPENDENCY_CALLS_FEATURE "dependency-calls/1"

/* Stable error codes (wire codes pass through as strings separately). */
typedef enum {
    MX_OK = 0,
    MX_ERR_TRANSPORT = 1,   /* socket/EOF/framing failure */
    MX_ERR_PROTOCOL = 2,    /* handshake/shape violation */
    MX_ERR_TIMEOUT = 3,     /* local deadline (outcome unknown, never retried) */
    MX_ERR_CANCELLED = 4,   /* parent cancelled */
    MX_ERR_DENIED = 5,      /* wire refusal with code/message attached */
    MX_ERR_UNKNOWN = 6,     /* outcome-unknown (wait expired, late terminal dropped) */
    MX_ERR_UNSUPPORTED = 7, /* feature not negotiated (local refusal, wire untouched) */
    MX_ERR_INVALID = 8,     /* bad argument / binary where text required */
    MX_ERR_THREAD = 9,      /* blocking call on reader/dispatcher thread */
    MX_ERR_CLOSED = 10,     /* component closed */
    MX_ERR_NOMEM = 11
} mx_status_t;

const char *mx_strerror(mx_status_t st);

typedef struct mx_component mx_component_t;
typedef struct mx_call_ctx mx_call_ctx_t;

/* Opaque activation binding handle (M6.1). */
typedef struct {
    char *id;         /* owned by SDK, valid during the call */
    char *capability; /* owned by SDK, valid during the call */
} mx_binding_t;

/* Business result from on_call. Exactly one of output_json or
 * (err_code, err_msg) must be set; all malloc'd (SDK frees). */
typedef struct {
    char *output_json; /* raw JSON value, answered as output */
    char *err_code;    /* business error code */
    char *err_msg;     /* business error message */
} mx_result_t;

typedef struct mx_handler {
    /* Runs on its own thread per call. cancelled polls via
     * mx_cancelled(ctx). Return output or business error. */
    mx_result_t (*on_call)(mx_call_ctx_t *ctx, const char *ticket,
                           const char *cap, const char *input_json,
                           void *userdata);
    /* Observability only (runs off the call thread; return fast). */
    void (*on_cancel)(const char *ticket, void *userdata);
    /* Dispatcher thread: observe fast, never block. */
    void (*on_event)(const char *topic, const char *payload_json,
                     void *userdata);
    void (*on_stream)(const char *stream_id, unsigned long long seq,
                      const char *payload, size_t payload_len,
                      void *userdata);
    void *userdata;
} mx_handler_t;

/* Connects, negotiates, registers logical, confirms activation.
 * Returns MX_OK and *out on success. Launch token from
 * MATRIX_LAUNCH_TOKEN like the other SDKs. */
mx_status_t mx_connect(const char *sock_path, const char *logical,
                       const mx_handler_t *handler, mx_component_t **out);

/* Serves until dispose/EOF/error. Returns MX_OK with *reason_out set to
 * "dispose" or "eof" (static strings), or the terminal status. */
mx_status_t mx_serve(mx_component_t *c, const char **reason_out);

/* Closes the session, joins threads, frees everything. Idempotent-ish:
 * safe to call once after serve; NULL-safe. */
void mx_close(mx_component_t *c);

/* Frees handler/SDK-returned heap strings (== free, documented here so
 * allocators never cross module boundaries implicitly). */
void mx_free(void *p);

/* -- call context (borrowed, valid during the call only) ------------- */

/* Nonzero when the call was cancelled (poll in long handlers). */
int mx_cancelled(mx_call_ctx_t *ctx);

/* This call's ticket (parent of children; never from payload). */
const char *mx_ticket(mx_call_ctx_t *ctx);

/* Opaque bindings of this activation (count via out_n). */
const mx_binding_t *mx_dependencies(mx_call_ctx_t *ctx, size_t *out_n);

/* Edge-queue drops (slow observers) and queued stream chunks. */
unsigned long long mx_event_dropped_count(mx_call_ctx_t *ctx);
size_t mx_pending_stream_count(mx_call_ctx_t *ctx);

/* Invokes a dependency by opaque handle. Blocks until terminal,
 * inheriting call cancellation. Without local negotiation returns
 * MX_ERR_UNSUPPORTED without touching the wire. Timeout is request
 * time; transport slack is added; expiry cancels on the wire and
 * returns MX_ERR_UNKNOWN (never retries). *output_json_out is
 * malloc'd JSON on MX_OK. On MX_ERR_DENIED, code/message_out are set. */
mx_status_t mx_invoke_dependency(mx_call_ctx_t *ctx, const char *binding_id,
                                 const char *input_json,
                                 unsigned long long timeout_ms,
                                 char **output_json_out, char **code_out,
                                 char **msg_out);

/* Sends one text chunk. payload may contain NULs? No: payload is
 * (pointer, len) but must be valid UTF-8 without NUL (JSON text);
 * binary garbage returns MX_ERR_INVALID, never lossy-converted. */
mx_status_t mx_send_stream(mx_call_ctx_t *ctx, const char *stream_id,
                           unsigned long long seq, const char *payload,
                           size_t payload_len);

/* Activation resources (cap/sub/timer/task). Returns the wire handle. */
mx_status_t mx_acquire_resource(mx_call_ctx_t *ctx, const char *kind,
                                const char *label,
                                int has_interval_ms,
                                unsigned long long interval_ms,
                                unsigned long long *handle_out,
                                char **code_out, char **msg_out);
mx_status_t mx_release_resource(mx_call_ctx_t *ctx, unsigned long long handle,
                                char **code_out, char **msg_out);

/* Negotiated extensions (empty on legacy sessions). */
size_t mx_features(const mx_component_t *c, const char ***out);

/* -- operator/application surface (ML1) ------------------------------
 * Same contract as the other SDKs: calls travel through the staged
 * matrix-managed binary (serve to own a kernel, request for
 * authenticated admin actions over mutual TLS). mx_op_start owns its
 * daemon; mx_op_connect only attaches. operator PKI paths are always
 * explicit: server identity never implies caller authority. */

typedef struct mx_op_client mx_op_client_t;
typedef struct mx_op_kernel mx_op_kernel_t;

/* Starts an owned kernel from a config file path. On failure returns
 * nonzero status and sets *err_out (malloc'd, mx_free). Reaps
 * everything created (no orphans, no partial operation). */
mx_status_t mx_op_start(const char *binary, const char *config_path,
                        const char *ca, const char *cert, const char *key,
                        const char *server_name, mx_op_kernel_t **out,
                        char **err_out);

/* Attaches to an existing kernel (owns no process). */
mx_status_t mx_op_connect(const char *binary, const char *listen,
                          const char *ca, const char *cert, const char *key,
                          const char *server_name, mx_op_client_t **out,
                          char **err_out);

/* One authenticated admin action. action_json is the request object;
 * *resp_out is malloc'd JSON on success. Refusals return
 * MX_ERR_DENIED with code/msg; timeouts MX_ERR_UNKNOWN (never retry). */
mx_status_t mx_op_request(mx_op_client_t *c, const char *action_json,
                          unsigned long long timeout_ms, char **resp_out,
                          char **code_out, char **msg_out);

/* Closes a client handle (never stops any daemon). Single-shot and
 * NULL-safe: frees the handle. Kernel-owned clients die with the
 * kernel; attached clients die here. */
void mx_op_client_close(mx_op_client_t *c);

/* Kernel close: SIGTERM, bounded wait, SIGKILL, remove the private
 * directory. Reaps exactly the spawned daemon, then frees the kernel
 * and its client. Single-shot and NULL-safe (C has no second-close:
 * do not touch the handle afterwards). */
void mx_op_kernel_close(mx_op_kernel_t *k);

/* The owned client (borrowed, valid until kernel close). */
mx_op_client_t *mx_op_kernel_client(mx_op_kernel_t *k);

/* Management address + ready-line facts (borrowed strings, valid
 * until kernel close). The address is not secret; leases never appear
 * here. */
const char *mx_op_kernel_listen(mx_op_kernel_t *k);
const char *mx_op_kernel_api(mx_op_kernel_t *k);

/* Environment diagnosis as malloc'd JSON (no secrets). Always
 * succeeds; check the "errors" array. */
char *mx_op_doctor(const char *binary);

#endif

#ifdef __cplusplus
}
#endif
