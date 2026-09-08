// C++ surface self-test: RAII wrapper over the C loopback (no deps).
// Mirrors the C suite's core cases through matrix.hpp. Prints
// "ok <name>", nonzero on failure.
#include "matrix.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#define CHECK(c)                                                                \
    do {                                                                        \
        if (!(c)) {                                                             \
            std::fprintf(stderr, "FAIL %d: %s\n", __LINE__, #c);                \
            std::exit(1);                                                       \
        }                                                                       \
    } while (0)

namespace {

std::string g_path;

void send_msg(int fd, const std::string &doc) {
    std::uint32_t n = static_cast<std::uint32_t>(doc.size());
    unsigned char hdr[4] = {
        static_cast<unsigned char>((n >> 24) & 0xFF),
        static_cast<unsigned char>((n >> 16) & 0xFF),
        static_cast<unsigned char>((n >> 8) & 0xFF),
        static_cast<unsigned char>(n & 0xFF),
    };
    CHECK(write(fd, hdr, 4) == 4);
    CHECK(write(fd, doc.data(), n) == (ssize_t)n);
}

std::string read_msg(int fd) {
    unsigned char hdr[4];
    std::size_t got = 0;
    while (got < 4) {
        ssize_t r = read(fd, hdr + got, 4 - got);
        CHECK(r > 0);
        got += static_cast<std::size_t>(r);
    }
    std::uint32_t n = (std::uint32_t(hdr[0]) << 24) | (std::uint32_t(hdr[1]) << 16) |
                      (std::uint32_t(hdr[2]) << 8) | hdr[3];
    CHECK(n > 0 && n <= 1024 * 1024);
    std::string buf(n, '\0');
    got = 0;
    while (got < n) {
        ssize_t r = read(fd, &buf[got], n - got);
        CHECK(r > 0);
        got += static_cast<std::size_t>(r);
    }
    return buf;
}

const char *DISPOSE =
    "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
    "\"type\":\"lifecycle.dispose\",\"message_id\":\"d1\","
    "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":\"1\","
    "\"request_id\":\"rd1\",\"body\":{\"operation_id\":\"op\","
    "\"deadline_ms\":100}}";

int loopback_accept(const std::string &features, const std::string &bindings) {
    (void)features;
    (void)bindings;
    int srv = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK(srv >= 0);
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, g_path.c_str(), sizeof(addr.sun_path) - 1);
    CHECK(bind(srv, reinterpret_cast<sockaddr *>(&addr),
               static_cast<socklen_t>(sizeof(addr.sun_family) + g_path.size() + 1)) == 0);
    CHECK(listen(srv, 1) == 0);
    // NOTE: the SDK connects after accept is listening (same-thread
    // ordering handled by the caller driving both ends in sequence).
    return srv;
}

class Echo : public mx::Handler {
public:
    std::string on_call(mx::CallCtx &, const std::string &, const std::string &,
                        const std::string &input) override {
        return "{\"echo\":" + input + "}";
    }
};

class Gate : public mx::Handler {
public:
    std::string on_call(mx::CallCtx &ctx, const std::string &, const std::string &,
                        const std::string &) override {
        try {
            ctx.invoke_dependency("bind-x", "{}", 2000);
            return "{\"unexpected\":\"wire-touched\"}";
        } catch (const mx::Error &e) {
            CHECK(e.code == "unsupported-feature");
            return "{\"refused\":\"" + e.code + "\"}";
        }
    }
};

class Chain : public mx::Handler {
public:
    std::string on_call(mx::CallCtx &ctx, const std::string &, const std::string &,
                        const std::string &) override {
        auto deps = ctx.dependencies();
        CHECK(!deps.empty());
        std::string out = ctx.invoke_dependency(deps[0].id, "{\"v\":1}", 5000);
        return "{\"got\":" + out + "}";
    }
};

void handshake(int conn, const std::string &features, const std::string &bindings) {
    std::string hello = read_msg(conn);
    CHECK(hello.find("\"type\":\"hello\"") != std::string::npos);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"welcome\",\"message_id\":\"h1\",\"session_id\":"
             "\"s1\",\"body\":{\"version\":\"0.1\",\"max_frame\":1048576,"
             "\"limits\":{},\"features\":" +
                 features + "}}");
    std::string reg = read_msg(conn);
    CHECK(reg.find("\"type\":\"component.register\"") != std::string::npos);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"registered\",\"message_id\":\"r\",\"session_id\":"
             "\"s1\",\"instance_id\":\"1\",\"generation\":\"1\",\"body\":{"
             "\"logical\":\"t\"}}");
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"lifecycle.activate\",\"message_id\":\"m1\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"q\",\"body\":{\"operation_id\":\"op\","
             "\"manifest\":{},\"bindings\":[],\"dependency_bindings\":" +
                 bindings + "}}");
    std::string lc = read_msg(conn);
    CHECK(lc.find("\"type\":\"lifecycle.result\"") != std::string::npos);
}

std::string call_open(const std::string &ticket, const std::string &input) {
    return "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
           "\"type\":\"call.open\",\"message_id\":\"m-" +
           ticket + "\",\"session_id\":\"s1\",\"instance_id\":\"1\","
                    "\"generation\":\"1\",\"request_id\":\"r-" +
           ticket + "\",\"body\":{\"ticket\":\"" + ticket +
           "\",\"capability\":\"c@1\",\"input\":" + input + "}}";
}

// Each case forks: child runs the SDK (connect+serve), parent drives
// the fake host. Deterministic, no threads in the test harness.
template <typename H>
void run_case(H &handler, const std::string &features, const std::string &bindings,
              void (*script)(int)) {
    char tmpl[] = "/tmp/cppts-XXXXXX";
    CHECK(mkdtemp(tmpl));
    g_path = std::string(tmpl) + "/t.sock";
    int srv = loopback_accept(features, bindings);
    pid_t pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        close(srv);
        try {
            mx::Component comp = mx::Component::connect(g_path, "t", handler);
            std::string reason = comp.serve();
            comp.close();
            comp.close(); // second close is a no-op
            _exit(reason == "dispose" ? 0 : 3);
        } catch (...) {
            _exit(4);
        }
    }
    int conn = accept(srv, nullptr, nullptr);
    CHECK(conn >= 0);
    handshake(conn, features, bindings);
    script(conn);
    int status = 0;
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    close(conn);
    close(srv);
    std::string cmd = "rm -rf " + std::string(tmpl);
    CHECK(system(cmd.c_str()) == 0);
}

void script_echo(int conn) {
    send_msg(conn, call_open("tkt-9", "{\"ping\":1}"));
    std::string ans = read_msg(conn);
    CHECK(ans.find("\"type\":\"call.result\"") != std::string::npos);
    CHECK(ans.find("\"ping\":1") != std::string::npos);
    send_msg(conn, DISPOSE);
    read_msg(conn);
}

void script_gate(int conn) {
    send_msg(conn, call_open("tkt-9", "{}"));
    std::string ans = read_msg(conn);
    CHECK(ans.find("unsupported-feature") != std::string::npos);
    // Wire untouched afterwards: no second frame within the window.
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(conn, &rfds);
    timeval tv{1, 0};
    CHECK(select(conn + 1, &rfds, nullptr, nullptr, &tv) == 0);
    send_msg(conn, DISPOSE);
    read_msg(conn);
}

void script_chain(int conn) {
    send_msg(conn, call_open("tkt-9", "{\"chain_it\":true}"));
    std::string opened = read_msg(conn);
    CHECK(opened.find("\"type\":\"dependency.open\"") != std::string::npos);
    CHECK(opened.find("bind-1") != std::string::npos);
    auto p = opened.find("\"request_id\":\"");
    CHECK(p != std::string::npos);
    p += 14;
    auto q = opened.find('"', p);
    std::string rid = opened.substr(p, q - p);
    send_msg(conn,
             "{\"protocol\":\"matrix.component\",\"version\":\"0.1\","
             "\"type\":\"dependency.result\",\"message_id\":\"mres\","
             "\"session_id\":\"s1\",\"instance_id\":\"1\",\"generation\":"
             "\"1\",\"request_id\":\"" +
                 rid + "\",\"body\":{\"status\":\"ok\",\"output\":{\"deep\":1}}}");
    std::string ans = read_msg(conn);
    CHECK(ans.find("\"deep\":1") != std::string::npos);
    send_msg(conn, DISPOSE);
    read_msg(conn);
}

void script_stale(int conn) {
    std::string stale = call_open("tkt-stale", "{}");
    auto p = stale.find("\"generation\":\"1\"");
    CHECK(p != std::string::npos);
    stale.replace(p + 14, 1, "999");
    send_msg(conn, stale);
    send_msg(conn, call_open("tkt-9", "{}"));
    std::string ans = read_msg(conn);
    CHECK(ans.find("r-tkt-9") != std::string::npos);
    send_msg(conn, DISPOSE);
    read_msg(conn);
}

} // namespace

int main() {
    {
        Echo h;
        run_case(h, "[]", "[]", script_echo);
        std::printf("ok echo-roundtrip\n");
    }
    {
        Gate h;
        run_case(h, "[]", "[]", script_gate);
        std::printf("ok feature-gate-local\n");
    }
    {
        Chain h;
        run_case(h, "[\"dependency-calls/1\"]",
                 "[{\"binding_id\":\"bind-1\",\"capability\":\"c@1\"}]", script_chain);
        std::printf("ok dependency-roundtrip\n");
    }
    {
        Echo h;
        run_case(h, "[]", "[]", script_stale);
        std::printf("ok stale-generation-ignored\n");
    }
    std::printf("selftest: all pass\n");
    return 0;
}
