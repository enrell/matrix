// Matrix C++ SDK: RAII layer over the C client (ML1, C++17, header-only).
//
// Same contract as the C SDK (docs/ML1-NODE.md); the C library does the
// protocol work, this header only owns lifetimes and translates errors
// into exceptions. Destructors never throw and never block
// indefinitely: close() joins bonded threads with the SDK's bounded
// waits; a second close is a no-op via an explicit flag.
//
// Threading/affinity, ownership and error semantics: see mx_component.h.
// Callbacks run on SDK threads (never the reader); blocking C calls
// from inside callbacks refuse with Error("internal", ...) instead of
// deadlocking — same guarantee, C++ spelling.
#pragma once

#include "mx_component.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mx {

// Stable error: wire/host code preserved verbatim.
class Error : public std::runtime_error {
public:
    std::string code;
    explicit Error(std::string code_, std::string msg)
        : std::runtime_error(msg), code(std::move(code_)) {}
};

// Business error thrown by handlers: answered with this code/message.
class BusinessError {
public:
    std::string code;
    std::string message;
    BusinessError(std::string c, std::string m) : code(std::move(c)), message(std::move(m)) {}
};

inline void throw_status(mx_status_t st, const char *code, const char *msg, const char *what) {
    if (st == MX_OK)
        return;
    // Local statuses carry no wire code: map them explicitly so they
    // never surface as a misleading "internal".
    std::string c;
    if (code && *code)
        c = code;
    else if (st == MX_ERR_UNSUPPORTED)
        c = "unsupported-feature";
    else if (st == MX_ERR_CANCELLED)
        c = "cancelled";
    else if (st == MX_ERR_UNKNOWN || st == MX_ERR_TIMEOUT)
        c = "outcome-unknown";
    else if (st == MX_ERR_THREAD)
        c = "internal";
    else
        c = "internal";
    std::string m = msg && *msg ? msg : (what ? what : mx_strerror(st));
    mx_free(const_cast<char *>(code));
    mx_free(const_cast<char *>(msg));
    throw Error(std::move(c), std::move(m));
}

struct DepBinding {
    std::string id;
    std::string capability;
};

// Non-owning per-call view (valid during the call only).
class CallCtx {
public:
    explicit CallCtx(mx_call_ctx_t *p) : p_(p) {}

    std::string ticket() const { return mx_ticket(p_); }
    bool cancelled() const { return mx_cancelled(p_) != 0; }
    std::uint64_t event_dropped_count() const { return mx_event_dropped_count(p_); }
    std::size_t pending_stream_count() const { return mx_pending_stream_count(p_); }

    std::vector<DepBinding> dependencies() const {
        std::size_t n = 0;
        const mx_binding_t *b = mx_dependencies(p_, &n);
        std::vector<DepBinding> out;
        for (std::size_t i = 0; i < n; ++i)
            out.push_back({b[i].id ? b[i].id : "", b[i].capability ? b[i].capability : ""});
        return out;
    }

    void send_stream(const std::string &id, std::uint64_t seq, const std::string &payload) const {
        if (id.empty())
            throw Error("invalid-message", "empty stream id");
        mx_status_t st = mx_send_stream(p_, id.c_str(), seq, payload.data(), payload.size());
        if (st != MX_OK)
            throw Error("invalid-message", mx_strerror(st));
    }

    void send_stream_bytes(const std::string &, std::uint64_t, const std::vector<unsigned char> &) const {
        throw Error("invalid-message",
                    "stream payloads are text; binary must be refused, never lossy-converted");
    }

    std::string invoke_dependency(const std::string &binding, const std::string &input_json,
                                  unsigned long long timeout_ms) const {
        char *out = nullptr, *code = nullptr, *msg = nullptr;
        mx_status_t st = mx_invoke_dependency(p_, binding.c_str(), input_json.c_str(), timeout_ms,
                                              &out, &code, &msg);
        if (st == MX_OK) {
            std::string r = out ? out : "null";
            mx_free(out);
            return r;
        }
        throw_status(st, code, msg, "invoke failed");
        return "null"; // unreachable
    }

    std::uint64_t acquire_resource(const std::string &kind, const std::string &label,
                                   bool has_interval, unsigned long long interval_ms) const {
        unsigned long long h = 0;
        char *code = nullptr, *msg = nullptr;
        mx_status_t st = mx_acquire_resource(p_, kind.c_str(), label.c_str(), has_interval ? 1 : 0,
                                             interval_ms, &h, &code, &msg);
        if (st == MX_OK)
            return h;
        throw_status(st, code, msg, "acquire failed");
        return 0; // unreachable
    }

    void release_resource(std::uint64_t h) const {
        char *code = nullptr, *msg = nullptr;
        mx_status_t st = mx_release_resource(p_, h, &code, &msg);
        if (st == MX_OK)
            return;
        throw_status(st, code, msg, "release failed");
    }

private:
    mx_call_ctx_t *p_;
};

class Handler {
public:
    virtual ~Handler() = default;
    // Return output JSON; throw BusinessError for business errors.
    virtual std::string on_call(CallCtx &ctx, const std::string &ticket, const std::string &cap,
                                const std::string &input_json) = 0;
    virtual void on_cancel(const std::string &) {}
    virtual void on_event(const std::string &, const std::string &) {}
    virtual void on_stream(const std::string &, std::uint64_t, const std::string &) {}
};

namespace detail {
inline char *dup_str(const std::string &s) {
    char *p = static_cast<char *>(std::malloc(s.size() + 1));
    if (p) {
        s.copy(p, s.size());
        p[s.size()] = '\0';
    }
    return p;
}

inline mx_result_t trampoline_call(mx_call_ctx_t *ctx, const char *ticket, const char *cap,
                                   const char *input, void *ud) {
    auto *h = static_cast<Handler *>(ud);
    mx_result_t r{nullptr, nullptr, nullptr};
    try {
        CallCtx view(ctx);
        std::string out = h->on_call(view, ticket ? ticket : "", cap ? cap : "",
                                     input ? input : "{}");
        char *p = static_cast<char *>(std::malloc(out.size() + 1));
        if (p) {
            out.copy(p, out.size());
            p[out.size()] = '\0';
        }
        r.output_json = p;
    } catch (const BusinessError &e) {
        r.err_code = dup_str(e.code);
        r.err_msg = dup_str(e.message);
    } catch (const Error &e) {
        r.err_code = dup_str(e.code);
        r.err_msg = dup_str(e.what());
    } catch (const std::exception &e) {
        r.err_code = dup_str("internal");
        r.err_msg = dup_str(std::string("handler: ") + e.what());
    } catch (...) {
        r.err_code = dup_str("internal");
        r.err_msg = dup_str("handler threw");
    }
    return r;
}

inline void trampoline_cancel(const char *t, void *ud) {
    try {
        static_cast<Handler *>(ud)->on_cancel(t ? t : "");
    } catch (...) {
    }
}
inline void trampoline_event(const char *topic, const char *payload, void *ud) {
    try {
        static_cast<Handler *>(ud)->on_event(topic ? topic : "", payload ? payload : "null");
    } catch (...) {
    }
}
inline void trampoline_stream(const char *id, unsigned long long seq, const char *payload,
                              std::size_t n, void *ud) {
    try {
        static_cast<Handler *>(ud)->on_stream(id ? id : "", seq,
                                              std::string(payload ? payload : "", n));
    } catch (...) {
    }
}
} // namespace detail

// Owning component session. Movable, non-copyable. The destructor
// closes verifiably (serve must have returned; otherwise it shuts the
// socket so stray threads observe EOF) and never throws.
class Component {
public:
    Component() : c_(nullptr), closed_(true) {}

    static Component connect(const std::string &sock, const std::string &logical, Handler &h) {
        mx_handler_t hh{};
        hh.on_call = detail::trampoline_call;
        hh.on_cancel = detail::trampoline_cancel;
        hh.on_event = detail::trampoline_event;
        hh.on_stream = detail::trampoline_stream;
        hh.userdata = &h;
        mx_component_t *c = nullptr;
        mx_status_t st = mx_connect(sock.c_str(), logical.c_str(), &hh, &c);
        if (st != MX_OK)
            throw Error("transport", mx_strerror(st));
        Component out;
        out.c_ = c;
        out.closed_ = false;
        return out;
    }

    Component(Component &&o) noexcept : c_(o.c_), closed_(o.closed_) {
        o.c_ = nullptr;
        o.closed_ = true;
    }
    Component &operator=(Component &&o) noexcept {
        if (this != &o) {
            close();
            c_ = o.c_;
            closed_ = o.closed_;
            o.c_ = nullptr;
            o.closed_ = true;
        }
        return *this;
    }
    Component(const Component &) = delete;
    Component &operator=(const Component &) = delete;

    ~Component() noexcept {
        try {
            close();
        } catch (...) {
        }
    }

    // Blocks until dispose/EOF/error. Returns the reason.
    std::string serve() {
        if (!c_)
            throw Error("internal", "component is closed");
        const char *reason = nullptr;
        mx_status_t st = mx_serve(c_, &reason);
        if (st != MX_OK)
            throw Error("transport", mx_strerror(st));
        return reason ? reason : "eof";
    }

    // Verifiable explicit close (idempotent flag; the C close is
    // single-shot, so the flag makes the second call a no-op here).
    void close() noexcept {
        if (closed_)
            return;
        closed_ = true;
        if (c_) {
            mx_close(c_);
            c_ = nullptr;
        }
    }

    bool closed() const { return closed_; }

private:
    mx_component_t *c_;
    bool closed_;
};

// --- operator surface (thin RAII over mx_op_*) ---------------------------

class OpClient {
public:
    OpClient() : c_(nullptr) {}
    static OpClient connect(const std::string &binary, const std::string &listen,
                            const std::string &ca, const std::string &cert,
                            const std::string &key, const std::string &server = "localhost") {
        mx_op_client_t *c = nullptr;
        char *err = nullptr;
        mx_status_t st = mx_op_connect(binary.c_str(), listen.c_str(), ca.c_str(), cert.c_str(),
                                       key.c_str(), server.c_str(), &c, &err);
        if (st != MX_OK) {
            std::string m = err && *err ? err : mx_strerror(st);
            mx_free(err);
            throw Error("transport", m);
        }
        OpClient out;
        out.c_ = c;
        return out;
    }
    OpClient(OpClient &&o) noexcept : c_(o.c_) { o.c_ = nullptr; }
    OpClient &operator=(OpClient &&o) noexcept {
        if (this != &o) {
            close();
            c_ = o.c_;
            o.c_ = nullptr;
        }
        return *this;
    }
    OpClient(const OpClient &) = delete;
    OpClient &operator=(const OpClient &) = delete;
    ~OpClient() noexcept { close(); }

    std::string request(const std::string &action_json, unsigned long long timeout_ms = 30000) {
        if (!c_)
            throw Error("internal", "client is closed");
        char *resp = nullptr, *code = nullptr, *msg = nullptr;
        mx_status_t st = mx_op_request(c_, action_json.c_str(), timeout_ms, &resp, &code, &msg);
        if (st == MX_OK) {
            std::string r = resp ? resp : "{}";
            mx_free(resp);
            return r;
        }
        throw_status(st, code, msg, "request refused");
        return "{}"; // unreachable
    }

    void close() noexcept {
        if (c_) {
            mx_op_client_close(c_);
            c_ = nullptr;
        }
    }

    // Borrowed handle for APIs that need it (valid while *this lives).
    mx_op_client_t *get() const { return c_; }

private:
    mx_op_client_t *c_;
};

class OwnedKernel {
public:
    OwnedKernel() : k_(nullptr) {}
    static OwnedKernel start(const std::string &binary, const std::string &config_path,
                             const std::string &ca, const std::string &cert,
                             const std::string &key, const std::string &server = "localhost") {
        mx_op_kernel_t *k = nullptr;
        char *err = nullptr;
        mx_status_t st = mx_op_start(binary.c_str(), config_path.c_str(), ca.c_str(), cert.c_str(),
                                     key.c_str(), server.c_str(), &k, &err);
        if (st != MX_OK) {
            std::string m = err && *err ? err : mx_strerror(st);
            mx_free(err);
            throw Error("transport", m);
        }
        OwnedKernel out;
        out.k_ = k;
        return out;
    }
    OwnedKernel(OwnedKernel &&o) noexcept : k_(o.k_) { o.k_ = nullptr; }
    OwnedKernel &operator=(OwnedKernel &&o) noexcept {
        if (this != &o) {
            close();
            k_ = o.k_;
            o.k_ = nullptr;
        }
        return *this;
    }
    OwnedKernel(const OwnedKernel &) = delete;
    OwnedKernel &operator=(const OwnedKernel &) = delete;
    ~OwnedKernel() noexcept { close(); }

    std::string listen() const { return mx_op_kernel_listen(k_); }
    std::string api() const { return mx_op_kernel_api(k_); }
    mx_op_client_t *client() const { return mx_op_kernel_client(k_); }

    std::string request(const std::string &action_json,
                        unsigned long long timeout_ms = 30000) const {
        if (!k_)
            throw Error("internal", "kernel is closed");
        char *resp = nullptr, *code = nullptr, *msg = nullptr;
        mx_status_t st =
            mx_op_request(mx_op_kernel_client(k_), action_json.c_str(), timeout_ms, &resp, &code, &msg);
        if (st == MX_OK) {
            std::string r = resp ? resp : "{}";
            mx_free(resp);
            return r;
        }
        throw_status(st, code, msg, "request refused");
        return "{}"; // unreachable
    }

    void close() noexcept {
        if (k_) {
            mx_op_kernel_close(k_);
            k_ = nullptr;
        }
    }

private:
    mx_op_kernel_t *k_;
};

} // namespace mx
