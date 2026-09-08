// Generic Matrix test node for C++ (ML1 contract: docs/ML1-NODE.md).
//
// Usage: mx-node-cpp --matrix-sock <sock> --id <logical>
//        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
#include "matrix.hpp"

#include <chrono>
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

// Minimal JSON field helpers over the raw input (the node only needs
// a few shapes; the SDK owns the real parser).
bool has_true(const std::string &in, const std::string &key) {
    auto p = in.find("\"" + key + "\"");
    if (p == std::string::npos)
        return false;
    auto q = in.find("true", p);
    auto e = in.find_first_of(",}", p);
    return q != std::string::npos && e != std::string::npos && q < e;
}
std::string get_str(const std::string &in, const std::string &key) {
    auto p = in.find("\"" + key + "\"");
    if (p == std::string::npos)
        return "";
    auto c = in.find(':', p);
    if (c == std::string::npos)
        return "";
    auto q1 = in.find('"', c);
    if (q1 == std::string::npos)
        return "";
    auto q2 = in.find('"', q1 + 1);
    if (q2 == std::string::npos)
        return "";
    return in.substr(q1 + 1, q2 - q1 - 1);
}
long long get_int(const std::string &in, const std::string &key, long long dflt) {
    auto p = in.find("\"" + key + "\"");
    if (p == std::string::npos)
        return dflt;
    auto c = in.find(':', p);
    if (c == std::string::npos)
        return dflt;
    try {
        return std::stoll(in.substr(c + 1));
    } catch (...) {
        return dflt;
    }
}
std::string sub_object(const std::string &in, const std::string &key) {
    auto p = in.find("\"" + key + "\"");
    if (p == std::string::npos)
        return "{}";
    auto c = in.find(':', p);
    if (c == std::string::npos)
        return "{}";
    auto b = in.find('{', c);
    if (b == std::string::npos)
        return "{}";
    int depth = 0;
    for (std::size_t i = b; i < in.size(); ++i) {
        if (in[i] == '{')
            ++depth;
        else if (in[i] == '}') {
            if (--depth == 0)
                return in.substr(b, i - b + 1);
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
        auto amp_p = input.find("\"amplify\"");
        if (amp_p != std::string::npos) {
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
        auto acq_p = input.find("\"acquire\"");
        if (acq_p != std::string::npos) {
            std::string acq = sub_object(input, "acquire");
            long long ms = get_int(acq, "interval_ms", -1);
            std::uint64_t h = ctx.acquire_resource(get_str(acq, "kind"), get_str(acq, "label"),
                                                   ms >= 0, static_cast<unsigned long long>(ms));
            return "{\"acquired\":{\"handle\":\"" + std::to_string(h) +
                   "\"},\"via\":" + quote(g_id) + "}";
        }
        if (input.find("\"release\"") != std::string::npos) {
            std::uint64_t h = static_cast<std::uint64_t>(get_int(input, "release", -1));
            ctx.release_resource(h);
            return "{\"released\":\"" + std::to_string(h) + "\",\"via\":" + quote(g_id) + "}";
        }
        auto ss_p = input.find("\"stream_send\"");
        if (ss_p != std::string::npos) {
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
                ctx.send_stream(sid, static_cast<std::uint64_t>(seq), payload);
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
        if (input.find("\"chain_with_streams\"") != std::string::npos) {
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
