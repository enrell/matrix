"""SDK Python unit tests over a loopback fake host (M6 closing, stdlib only).

Mirrors matrix-host/tests/sdk_units.rs: reader independence, flood drops
with counter, reader-thread guard, dependency invoke end-to-end.
Run: python3 sdk-python/test_units.py (also wired into `make test`).
"""

import json
import os
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from matrix_component import Component, DepError, Handler, ResError  # noqa: E402

MAX_FRAME = 1024 * 1024
_seq = [1]
_seq_lock = threading.Lock()


def _fresh(prefix):
    with _seq_lock:
        i = _seq[0]
        _seq[0] += 1
    return f"{prefix}-{i}"


def _send(sock, msg):
    raw = json.dumps(msg).encode("utf-8")
    sock.sendall(struct.pack(">I", len(raw)) + raw)


def _read(sock, timeout=20):
    sock.settimeout(timeout)
    hdr = sock.recv(4, socket.MSG_WAITALL)
    if not hdr:
        raise IOError("eof")
    (ln,) = struct.unpack(">I", hdr)
    buf = b""
    while len(buf) < ln:
        chunk = sock.recv(ln - len(buf))
        if not chunk:
            raise IOError("truncated")
        buf += chunk
    return json.loads(buf)


def _dep_env(ty, body):
    n = _fresh("m")
    return {
        "protocol": "matrix.component", "version": "0.1", "type": ty,
        "message_id": n, "session_id": "s1", "instance_id": "1",
        "generation": "1", "request_id": _fresh("r"), "body": body,
    }


def _call_open(ticket, input):
    return {
        "protocol": "matrix.component", "version": "0.1", "type": "call.open",
        "message_id": f"m-{ticket}", "session_id": "s1", "instance_id": "1",
        "generation": "1", "request_id": f"r-{ticket}",
        "body": {"ticket": ticket, "capability": "c@1", "input": input},
    }


def _dispose():
    return {
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.dispose",
        "message_id": "d1", "session_id": "s1", "instance_id": "1",
        "generation": "1", "request_id": "rd1",
        "body": {"operation_id": "op", "deadline_ms": 100},
    }


class Probe(Handler):
    def __init__(self):
        self.events = []
        self.events_lock = threading.Lock()
        self.slow = [0.0]
        self.streams = []
        self.streams_lock = threading.Lock()
        self.stream_slow = [0.0]
        self.stashed = {}
        self.stash_lock = threading.Lock()
        self.guard_hits = []
        self.hits_lock = threading.Lock()

    def on_call(self, ctx, ticket, cap, input, cancel):
        with self.stash_lock:
            self.stashed["ctx"] = ctx
        if isinstance(input, dict) and input.get("report_drops"):
            return {"dropped": ctx.event_dropped_count()}
        if isinstance(input, dict) and input.get("chain_it"):
            bindings = ctx.dependencies()
            bid = bindings[0]["id"] if bindings else ""
            try:
                out = ctx.invoke_dependency(bid, {"v": 1}, 5.0)
            except DepError as exc:
                return ("error", exc.code, exc.message)
            return {"got": out}
        return {"echo": input}

    def on_cancel(self, ticket):
        with self.stash_lock:
            ctx = self.stashed.get("ctx")
        if ctx is not None:
            try:
                ctx.invoke_dependency("bind-x", {}, 2.0)
                outcome = "ok-unexpected"
            except DepError as exc:
                outcome = f"dep:{exc.code}"
            with self.hits_lock:
                self.guard_hits.append(outcome)
            try:
                ctx.acquire_resource("timer", "t", 10)
                outcome = "ok-unexpected"
            except ResError as exc:
                outcome = f"res:{exc.code}"
            with self.hits_lock:
                self.guard_hits.append(outcome)

    def on_event(self, topic, payload):
        ms = self.slow[0]
        if ms > 0:
            time.sleep(ms)
        with self.events_lock:
            self.events.append((topic, payload))

    def on_stream(self, stream_id, seq, payload):
        ms = self.stream_slow[0]
        if ms > 0:
            time.sleep(ms)
        with self.streams_lock:
            self.streams.append((stream_id, seq, len(payload)))


def _loopback(bindings):
    """Returns (path, serve_thread_starter). Starts a fake host thread."""
    tmp = tempfile.mkdtemp(prefix="pyunits-")
    path = os.path.join(tmp, "t.sock")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(1)

    def handshake(conn):
        hello = _read(conn)
        assert hello["type"] == "hello", hello
        _send(conn, {
            "protocol": "matrix.component", "version": "0.1", "type": "welcome",
            "message_id": "h1", "session_id": "s1",
            "body": {"version": "0.1", "max_frame": MAX_FRAME, "limits": {},
                     "features": ["dependency-calls/1"]},
        })
        reg = _read(conn)
        assert reg["type"] == "component.register", reg
        _send(conn, {
            "protocol": "matrix.component", "version": "0.1", "type": "registered",
            "message_id": "r", "session_id": "s1", "instance_id": "1",
            "generation": "1", "body": {"logical": "t"},
        })
        act = dict(_dep_env(
            "lifecycle.activate",
            {"operation_id": "op", "manifest": {}, "bindings": [],
             "dependency_bindings": bindings}))
        act["request_id"] = "q"
        _send(conn, act)
        lc = _read(conn)
        assert lc["type"] == "lifecycle.result", lc

    return srv, handshake, path


class SdkUnits(unittest.TestCase):
    def test_events_do_not_block_reader(self):
        srv, handshake, path = _loopback([])
        handler = Probe()
        handler.slow[0] = 0.3
        box: dict = {}

        def host():
            conn, _ = srv.accept()
            conn.settimeout(20)
            handshake(conn)
            _send(conn, _dep_env("event.deliver", {"topic": "t", "payload": {"n": 1}}))
            _send(conn, _dep_env("event.deliver", {"topic": "t", "payload": {"n": 2}}))
            _send(conn, _call_open("tkt-9", {}))
            t0 = time.monotonic()
            ans = _read(conn)
            box["dt"] = time.monotonic() - t0
            box["ans"] = ans
            _send(conn, _dispose())
            _read(conn)
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        self.assertIn("dependency-calls/1", comp.features)
        rc = comp.serve(handler)
        ht.join(timeout=20)
        self.assertLess(box["dt"], 3.0, "reader independent")
        self.assertEqual(box["ans"]["type"], "call.result")
        deadline = time.monotonic() + 10
        while len(handler.events) < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        got = sorted(n for _, p in handler.events for n in [p["n"]])
        self.assertEqual(got, [1, 2])
        self.assertEqual(rc, "dispose")

    def test_event_flood_drops_oldest_and_counts(self):
        srv, handshake, path = _loopback([])
        handler = Probe()
        handler.slow[0] = 0.03
        box: dict = {}

        def host():
            conn, _ = srv.accept()
            conn.settimeout(30)
            handshake(conn)
            for i in range(120):
                _send(conn, _dep_env("event.deliver", {"topic": "t", "payload": {"n": i}}))
            _send(conn, _call_open("tkt-9", {}))
            t0 = time.monotonic()
            ans = _read(conn)
            box["dt"] = time.monotonic() - t0
            box["ans"] = ans
            _send(conn, _call_open("tkt-10", {"report_drops": True}))
            rep = _read(conn)
            box["dropped"] = rep["body"]["output"]["dropped"]
            _send(conn, _dispose())
            _read(conn)
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        comp.serve(handler)
        ht.join(timeout=30)
        self.assertLess(box["dt"], 5.0, "call answered under flood")
        self.assertEqual(box["ans"]["type"], "call.result")
        self.assertGreaterEqual(box["dropped"], 1, "overflow counted")

    def test_streams_do_not_block_reader(self):
        srv, handshake, path = _loopback([])
        handler = Probe()
        handler.stream_slow[0] = 0.2
        box: dict = {}

        def chunk(sid, seq, payload):
            return {
                "protocol": "matrix.component", "version": "0.1", "type": "stream.data",
                "message_id": f"s-{seq}", "session_id": "s1", "instance_id": "1",
                "generation": "1",
                "body": {"stream_id": sid, "seq": str(seq), "payload": payload},
            }

        def host():
            conn, _ = srv.accept()
            conn.settimeout(20)
            handshake(conn)
            _send(conn, chunk("s1", 0, "hello"))
            _send(conn, chunk("s1", 1, "world"))
            _send(conn, _call_open("tkt-9", {}))
            t0 = time.monotonic()
            ans = _read(conn)
            box["dt"] = time.monotonic() - t0
            box["ans"] = ans
            deadline = time.monotonic() + 10
            while len(handler.streams) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            _send(conn, _dispose())
            _read(conn)
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        rc = comp.serve(handler)
        ht.join(timeout=20)
        self.assertLess(box["dt"], 3.0, "reader independent")
        self.assertEqual(box["ans"]["type"], "call.result")
        self.assertEqual(sorted(s for _, s, _ in handler.streams), [0, 1])
        self.assertEqual(rc, "dispose")

    def test_stream_flood_drops_oldest_and_counts(self):
        srv, handshake, path = _loopback([])
        handler = Probe()
        handler.stream_slow[0] = 0.02
        box: dict = {}

        def chunk(sid, seq):
            return {
                "protocol": "matrix.component", "version": "0.1", "type": "stream.data",
                "message_id": f"s-{seq}", "session_id": "s1", "instance_id": "1",
                "generation": "1",
                "body": {"stream_id": sid, "seq": str(seq), "payload": "x"},
            }

        def host():
            conn, _ = srv.accept()
            conn.settimeout(30)
            handshake(conn)
            for i in range(120):
                _send(conn, chunk("s9", i))
            _send(conn, _call_open("tkt-9", {}))
            t0 = time.monotonic()
            ans = _read(conn)
            box["dt"] = time.monotonic() - t0
            box["ans"] = ans
            _send(conn, _call_open("tkt-10", {"report_drops": True}))
            rep = _read(conn)
            box["dropped"] = rep["body"]["output"]["dropped"]
            _send(conn, _dispose())
            _read(conn)
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        comp.serve(handler)
        ht.join(timeout=30)
        self.assertLess(box["dt"], 5.0, "call answered under stream flood")
        self.assertEqual(box["ans"]["type"], "call.result")
        self.assertGreaterEqual(box["dropped"], 1, "stream overflow counted")

    def test_reader_thread_guard_refuses_fast(self):
        srv, handshake, path = _loopback([])
        handler = Probe()

        def host():
            conn, _ = srv.accept()
            conn.settimeout(20)
            handshake(conn)
            _send(conn, _call_open("tkt-9", {}))
            _read(conn)  # call.result
            _send(conn, {
                "protocol": "matrix.component", "version": "0.1", "type": "call.cancel",
                "message_id": "c2", "session_id": "s1", "instance_id": "1",
                "generation": "1", "request_id": "rc2",
                "body": {"ticket": "tkt-9", "reason": "test"},
            })
            _send(conn, _dispose())
            _read(conn)
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        t0 = time.monotonic()
        rc = comp.serve(handler)
        ht.join(timeout=20)
        self.assertEqual(rc, "dispose")
        self.assertLess(time.monotonic() - t0, 15.0)
        self.assertEqual(len(handler.guard_hits), 2, handler.guard_hits)
        for hit in handler.guard_hits:
            self.assertIn("internal", hit, handler.guard_hits)

    def test_invoke_dependency_end_to_end(self):
        srv, handshake, path = _loopback(
            [{"binding_id": "bind-1", "capability": "c@1"}])
        handler = Probe()
        box: dict = {}

        def host():
            conn, _ = srv.accept()
            conn.settimeout(20)
            handshake(conn)
            _send(conn, _call_open("tkt-9", {"chain_it": True}))
            opened = _read(conn)
            self.assertEqual(opened["type"], "dependency.open")
            self.assertEqual(opened["body"]["binding_id"], "bind-1")
            self.assertEqual(opened["body"]["parent_ticket"], "tkt-9")
            open_rid = opened["request_id"]
            _send(conn, _dep_env("dependency.accepted", {"child_ticket": "7"}))
            _send(conn, {
                "protocol": "matrix.component", "version": "0.1",
                "type": "dependency.result",
                "message_id": "mres", "session_id": "s1", "instance_id": "1",
                "generation": "1", "request_id": open_rid,
                "body": {"status": "ok", "output": {"deep": 1}},
            })
            ans = _read(conn)
            box["ans"] = ans
            _send(conn, _dispose())
            _read(conn)
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        rc = comp.serve(handler)
        ht.join(timeout=20)
        self.assertEqual(rc, "dispose")
        self.assertEqual(box["ans"]["type"], "call.result")
        self.assertEqual(box["ans"]["body"]["output"]["got"]["deep"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
