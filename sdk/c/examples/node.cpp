// Generic Matrix test node for C++ (ML1 contract: docs/ML1-NODE.md).
//
// Usage: mx-node-cpp --matrix-sock <sock> --id <logical>
//        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
#include "matrix.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>

namespace {

std::string g_id = "dep-node";
std::string g_event_log;
std::string g_stream_log;
long g_stream_slow_ms = 0;

void append_line(const std::string &path, const std::string &line) {
    if (path.empty())
        return;
    std::ofstream f(path, std::ios::app);
    if (f)
        f << line << "\n";
}

// Top-level member lookup over the raw input. The node only needs a few
// shapes, but nested objects (chain `input`, stream specs) may repeat a
// top-level key (e.g. spec `sleep_ms` shadowing the sleep branch), so
// first-occurrence search is wrong: only depth-1 members match. Strings
// (with backslash escapes) are skipped while scanning.
std::size_t find_top(const std::string &in, const std::string &key) {
    int depth = 0;
    bool in_str = false;
    for (std::size_t i = 0; i < in.size(); ++i) {
        char c = in[i];
        if (in_str) {
            if (c == '\\')
                ++i;
            else if (c == '"')
                in_str = false;
            continue;
        }
        if (c == '"') {
            if (depth == 1 && in.compare(i + 1, key.size(), key) == 0 &&
                i + 2 + key.size() <= in.size() && in[i + 1 + key.size()] == '"') {
                std::size_t j = i + 2 + key.size();
                while (j < in.size() && (in[j] == ' ' || in[j] == '\t' || in[j] == '\n' || in[j] == '\r'))
                    ++j;
                if (j < in.size() && in[j] == ':')
                    return j + 1;
            }
            in_str = true;
            continue;
        }
        if (c == '{' || c == '[')
            ++depth;
        else if (c == '}' || c == ']')
            --depth;
    }
    return std::string::npos;
}
static std::size_t skip_ws(const std::string &in, std::size_t i) {
    while (i < in.size() && (in[i] == ' ' || in[i] == '\t' || in[i] == '\n' || in[i] == '\r'))
        ++i;
    return i;
}
bool has_true(const std::string &in, const std::string &key) {
    auto p = find_top(in, key);
    if (p == std::string::npos)
        return false;
    return in.compare(skip_ws(in, p), 4, "true") == 0;
}
std::string get_str(const std::string &in, const std::string &key) {
    auto p = find_top(in, key);
    if (p == std::string::npos)
        return "";
    p = skip_ws(in, p);
    if (p >= in.size() || in[p] != '"')
        return "";
    std::string o;
    for (std::size_t i = p + 1; i < in.size(); ++i) {
        if (in[i] == '\\' && i + 1 < in.size()) {
            o += in[i + 1];
            ++i;
        } else if (in[i] == '"') {
            return o;
        } else {
            o += in[i];
        }
    }
    return "";
}
long long get_int(const std::string &in, const std::string &key, long long dflt) {
    auto p = find_top(in, key);
    if (p == std::string::npos)
        return dflt;
    try {
        return std::stoll(in.substr(skip_ws(in, p)));
    } catch (...) {
        return dflt;
    }
}
// Numeric-only u64 (Rust dep_node reference: only a non-negative
// integer JSON number takes the release branch; strings, floats,
// bools and bad shapes fall through to echo).
bool get_release(const std::string &in, std::uint64_t &out) {
    auto p = find_top(in, "release");
    if (p == std::string::npos)
        return false;
    p = skip_ws(in, p);
    if (p >= in.size() || in[p] < '0' || in[p] > '9')
        return false;
    std::size_t q = p;
    while (q < in.size() && in[q] >= '0' && in[q] <= '9')
        ++q;
    if (q < in.size() && (in[q] == '.' || in[q] == 'e' || in[q] == 'E'))
        return false;
    try {
        std::size_t pos = 0;
        unsigned long long v = std::stoull(in.substr(p, q - p), &pos);
        if (pos != q - p)
            return false;
        out = static_cast<std::uint64_t>(v);
        return true;
    } catch (...) {
        return false;
    }
}
std::string sub_object(const std::string &in, const std::string &key) {
    auto p = find_top(in, key);
    if (p == std::string::npos)
        return "{}";
    p = skip_ws(in, p);
    if (p >= in.size() || in[p] != '{')
        return "{}";
    int depth = 0;
    bool in_str = false;
    for (std::size_t i = p; i < in.size(); ++i) {
        char c = in[i];
        if (in_str) {
            if (c == '\\')
                ++i;
            else if (c == '"')
                in_str = false;
            continue;
        }
        if (c == '"') {
            in_str = true;
        } else if (c == '{') {
            ++depth;
        } else if (c == '}') {
            if (--depth == 0)
                return in.substr(p, i - p + 1);
        }
    }
    return "{}";
}
std::string quote(const std::string &s) {
    std::string o = "\"";
    for (char ch : s) {
        if (ch == '"' || ch == '\\')
            o += '\\';
        o += ch;
    }
    return o + "\"";
}

class Node : public mx::Handler {
public:
    std::string on_call(mx::CallCtx &ctx, const std::string &,
                        const std::string &, const std::string &input) override {
        long long sleep_ms = get_int(input, "sleep_ms", 0);
        for (long long s = 0; s < sleep_ms; s += 5) {
            if (ctx.cancelled())
                throw mx::BusinessError("cancelled", "aborted");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        std::string fail = get_str(input, "fail");
        if (!fail.empty())
            throw mx::BusinessError(fail, "remote " + fail);
        // amplify / chain / acquire / release / streams mirror node.c.
        if (find_top(input, "amplify") != std::string::npos) {
            long long n = get_int(input, "amplify", 0);
            n = std::max<long long>(0, std::min<long long>(n, 1 << 20));
            return "{\"blob\":\"" + std::string(static_cast<std::size_t>(n), 'x') +
                   "\",\"via\":" + quote(g_id) + "}";
        }
        if (has_true(input, "chain")) {
            auto deps = ctx.dependencies();
            if (deps.empty())
                throw mx::BusinessError("dependency-unavailable", "no binding");
            long long timeout = std::max<long long>(1, get_int(input, "timeout_ms", 5000));
            std::string out = ctx.invoke_dependency(deps[0].id, sub_object(input, "input"),
                                                    static_cast<unsigned long long>(timeout));
            return "{\"chained\":" + out + ",\"via\":" + quote(g_id) + "}";
        }
        if (find_top(input, "acquire") != std::string::npos) {
            std::string acq = sub_object(input, "acquire");
            long long ms = get_int(acq, "interval_ms", -1);
            std::uint64_t h = ctx.acquire_resource(get_str(acq, "kind"), get_str(acq, "label"),
                                                   ms >= 0, static_cast<unsigned long long>(ms));
            return "{\"acquired\":{\"handle\":\"" + std::to_string(h) +
                   "\"},\"via\":" + quote(g_id) + "}";
        }
        if (find_top(input, "release") != std::string::npos) {
            std::uint64_t h = 0;
            if (get_release(input, h)) {
                ctx.release_resource(h);
                return "{\"released\":\"" + std::to_string(h) + "\",\"via\":" + quote(g_id) + "}";
            }
        }
        if (find_top(input, "stream_send") != std::string::npos) {
            std::string spec = sub_object(input, "stream_send");
            std::string sid = get_str(spec, "stream_id");
            if (sid.empty())
                sid = "s-test";
            long long chunks = std::min<long long>(std::max<long long>(0, get_int(spec, "chunks", 0)), 256);
            long long nbytes = std::min<long long>(std::max<long long>(0, get_int(spec, "chunk_bytes", 0)), 4096);
            long long slp = std::max<long long>(0, get_int(spec, "sleep_ms", 0));
            std::string payload(static_cast<std::size_t>(nbytes), 'x');
            long long sent = 0;
            for (long long seq = 0; seq < chunks; ++seq) {
                if (ctx.cancelled())
                    throw mx::BusinessError("cancelled", "aborted");
                try {
                    ctx.send_stream(sid, static_cast<std::uint64_t>(seq), payload);
                } catch (const mx::Error &e) {
                    throw mx::BusinessError("stream-refused", e.what());
                }
                ++sent;
                if (slp > 0) {
                    long long s = std::min(slp, 50LL);
                    for (long long w = 0; w < s; w += 5) {
                        if (ctx.cancelled())
                            throw mx::BusinessError("cancelled", "aborted");
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                }
            }
            return "{\"stream_sent\":" + std::to_string(sent) + ",\"via\":" + quote(g_id) + "}";
        }
        if (find_top(input, "chain_with_streams") != std::string::npos) {
            // Concurrent chain + streams (M7 bidi legs): streams while
            // the child leg is in flight on this same session.
            std::string spec = sub_object(input, "chain_with_streams");
            std::string sid = get_str(spec, "stream_id");
            if (sid.empty())
                sid = "s-bidi";
            long long chunks = std::min<long long>(std::max<long long>(0, get_int(spec, "chunks", 0)), 32);
            long long nbytes = std::min<long long>(std::max<long long>(0, get_int(spec, "chunk_bytes", 0)), 1024);
            long long interval = std::min<long long>(std::max<long long>(0, get_int(spec, "interval_ms", 20)), 50);
            long long prime = std::min<long long>(std::max<long long>(0, get_int(spec, "prime_ms", 50)), 1000);
            std::string payload(static_cast<std::size_t>(nbytes), 'x');
            long long sent = 0;
            std::thread streamer([&] {
                if (prime > 0)
                    std::this_thread::sleep_for(std::chrono::milliseconds(prime));
                for (long long seq = 0; seq < chunks; ++seq) {
                    try {
                        ctx.send_stream(sid, static_cast<std::uint64_t>(seq), payload);
                        ++sent;
                    } catch (...) {
                        break;
                    }
                    if (interval > 0)
                        std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                }
            });
            auto deps = ctx.dependencies();
            if (deps.empty()) {
                streamer.join();
                throw mx::BusinessError("dependency-unavailable", "no binding");
            }
            long long timeout = std::max<long long>(1, get_int(spec, "timeout_ms", 8000));
            std::string chainedOut;
            try {
                chainedOut = ctx.invoke_dependency(deps[0].id, sub_object(spec, "input"),
                                                   static_cast<unsigned long long>(timeout));
            } catch (const mx::Error &e) {
                streamer.join();
                throw mx::BusinessError(e.code, e.what());
            }
            streamer.join();
            return "{\"chained\":" + chainedOut + ",\"via\":" + quote(g_id) +
                   ",\"stream_sent\":" + std::to_string(sent) + "}";
        }
        return "{\"echo\":" + input + ",\"via\":" + quote(g_id) + "}";
    }

    void on_event(const std::string &topic, const std::string &payload) override {
        append_line(g_event_log, topic + "\t" + payload);
    }
    void on_stream(const std::string &id, std::uint64_t seq,
                   const std::string &payload) override {
        if (g_stream_slow_ms > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(g_stream_slow_ms));
        append_line(g_stream_log, id + "\t" + std::to_string(seq) + "\t" +
                                      std::to_string(payload.size()));
    }
};

} // namespace

int main(int argc, char **argv) {
    std::string sock;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto need = [&](const char *k, std::string &out) {
            if (a == k && i + 1 < argc)
                out = argv[++i];
        };
        need("--matrix-sock", sock);
        need("--id", g_id);
        need("--event-log", g_event_log);
        need("--stream-log", g_stream_log);
        if (a == "--stream-slow-ms" && i + 1 < argc)
            g_stream_slow_ms = std::stoll(argv[++i]);
    }
    if (sock.empty()) {
        std::fprintf(stderr, "usage: mx-node-cpp --matrix-sock <sock> [--id <logical>] ...\n");
        return 2;
    }
    try {
        Node handler;
        mx::Component comp = mx::Component::connect(sock, g_id, handler);
        std::string reason = comp.serve();
        comp.close();
        if (reason != "dispose" && reason != "eof") {
            std::fprintf(stderr, "serve: %s\n", reason.c_str());
            return 1;
        }
        return 0;
    } catch (const std::exception &e) {
        std::fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
