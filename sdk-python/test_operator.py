"""Operator-surface tests (ML1, stdlib only).

Unit parts run anywhere: feature gate over a loopback fake host,
bootstrap failure phases, CLI error mapping over a fake binary,
doctor shape. Live parts need a real ``matrix-managed`` binary
(``MX_MATRIX_MANAGED`` or ``../../target/release/matrix-managed``)
plus ``openssl`` for dev PKI; otherwise they skip.
Run: python3 sdk-python/test_operator.py (also wired into `make test`
for the unit parts; live parts skip without a binary).
"""

import hashlib
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = __file__.rsplit("/", 1)[0]
sys.path.insert(0, HERE)
from matrix_component import Component, DepError, Handler  # noqa: E402
from matrix_operator import (  # noqa: E402
    BootstrapError, Client, OperatorError, connect, doctor, start,
)

MAX_FRAME = 1024 * 1024

BIN = os.environ.get("MX_MATRIX_MANAGED", "") or os.path.join(
    HERE, "..", "target", "release", "matrix-managed")
DEV_PKI = os.path.join(HERE, "..", "scripts", "dev-pki.py")
HAVE_BIN = os.access(BIN, os.X_OK)
HAVE_OPENSSL = shutil.which("openssl") is not None
LIVE = HAVE_BIN and HAVE_OPENSSL and os.path.exists(DEV_PKI)


def _send(sock, msg):
    raw = json.dumps(msg).encode("utf-8")
    sock.sendall(struct.pack(">I", len(raw)) + raw)


def _read(sock, timeout=10):
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


def _env(ty, body, rid="r1"):
    return {"protocol": "matrix.component", "version": "0.1", "type": ty,
            "message_id": "m1", "session_id": "s1", "instance_id": "1",
            "generation": "1", "request_id": rid, "body": body}


class GateCaller(Handler):
    def on_call(self, ctx, ticket, cap, input, cancel):
        try:
            ctx.invoke_dependency("bind-x", {}, 2.0)
            return {"unexpected": "wire-touched"}
        except DepError as exc:
            return {"refused": exc.code}


class OperatorUnits(unittest.TestCase):
    def test_invoke_without_feature_refuses_locally(self):
        """No negotiated ``dependency-calls/1``: local
        ``unsupported-feature``, wire untouched (L04/L11)."""
        tmp = tempfile.mkdtemp(prefix="opgate-")
        self.addCleanup(shutil.rmtree, tmp, True)
        path = os.path.join(tmp, "t.sock")
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(path)
        srv.listen(1)
        box: dict = {}

        def host():
            conn, _ = srv.accept()
            conn.settimeout(10)
            hello = _read(conn)
            assert hello["type"] == "hello", hello
            _send(conn, {"protocol": "matrix.component", "version": "0.1",
                         "type": "welcome", "message_id": "h1",
                         "session_id": "s1",
                         "body": {"version": "0.1", "max_frame": MAX_FRAME,
                                  "limits": {}, "features": []}})
            reg = _read(conn)
            assert reg["type"] == "component.register", reg
            _send(conn, {"protocol": "matrix.component", "version": "0.1",
                         "type": "registered", "message_id": "r",
                         "session_id": "s1", "instance_id": "1",
                         "generation": "1", "body": {"logical": "t"}})
            act = _env("lifecycle.activate",
                       {"operation_id": "op", "manifest": {}, "bindings": [],
                        "dependency_bindings": []}, rid="q")
            _send(conn, act)
            lc = _read(conn)
            assert lc["type"] == "lifecycle.result", lc
            _send(conn, {"protocol": "matrix.component", "version": "0.1",
                         "type": "call.open", "message_id": "m9",
                         "session_id": "s1", "instance_id": "1",
                         "generation": "1", "request_id": "rq9",
                         "body": {"ticket": "tkt-1", "capability": "c@1",
                                  "input": {}}})
            ans = _read(conn)
            box["ans"] = ans
            # Anything else from the SDK now would be a wire touch.
            conn.settimeout(1.0)
            try:
                box["extra"] = _read(conn, timeout=1.0)
            except Exception as exc:
                box["extra"] = f"quiet:{type(exc).__name__}"
            _send(conn, {"protocol": "matrix.component", "version": "0.1",
                         "type": "lifecycle.dispose", "message_id": "d1",
                         "session_id": "s1", "instance_id": "1",
                         "generation": "1", "request_id": "rd1",
                         "body": {"operation_id": "op", "deadline_ms": 100}})
            try:
                _read(conn, timeout=5)
            except Exception:
                pass
            conn.close()
            srv.close()

        ht = threading.Thread(target=host, daemon=True)
        ht.start()
        comp = Component.connect(path, "t")
        self.assertNotIn("dependency-calls/1", comp.features)
        rc = comp.serve(GateCaller())
        ht.join(timeout=15)
        self.assertEqual(rc, "dispose")
        out = box["ans"]["body"]["output"]
        self.assertEqual(out, {"refused": "unsupported-feature"}, out)
        self.assertTrue(str(box.get("extra", "")).startswith("quiet:"),
                        box.get("extra"))

    def test_bootstrap_phases(self):
        with self.assertRaises(BootstrapError) as cm:
            start("/nonexistent/matrix-managed", {"home": "/tmp/x"})
        self.assertEqual(cm.exception.phase, "spawn")
        with self.assertRaises(BootstrapError) as cm:
            start(sys.executable, {"components": []})
        self.assertEqual(cm.exception.phase, "config")
        with self.assertRaises(BootstrapError):
            connect("/nonexistent/x", "127.0.0.1:1", "a", "b", "c")

    def test_error_mapping_over_fake_binary(self):
        tmp = tempfile.mkdtemp(prefix="opfake-")
        self.addCleanup(shutil.rmtree, tmp, True)
        fake = os.path.join(tmp, "matrix-managed")
        with open(fake, "w", encoding="utf-8") as f:
            f.write("#!/bin/sh\n"
                    "if [ \"$1\" = \"request\" ]; then\n"
                    "  case \"$7\" in\n"
                    "    *sleep*) exec sleep 30;;\n"
                    "    *badjson*) echo 'not json';;\n"
                    "    *denied*) echo 'permission-denied: nope' >&2; exit 1;;\n"
                    "    *) echo '{\"ok\":true}';;\n"
                    "  esac\n"
                    "else echo 'usage: matrix-managed serve <config>' >&2; exit 1\n"
                    "fi\n")
        os.chmod(fake, 0o755)
        for name in ("ca", "cert", "key"):
            open(os.path.join(tmp, name), "w").close()
        c = Client(fake, "127.0.0.1:9", os.path.join(tmp, "ca"),
                   os.path.join(tmp, "cert"), os.path.join(tmp, "key"))
        self.assertEqual(c.request({"action": "ping"}), {"ok": True})
        with self.assertRaises(OperatorError) as cm:
            c.request({"action": "denied-op"})
        self.assertEqual(cm.exception.code, "permission-denied")
        with self.assertRaises(OperatorError) as cm:
            c.request({"action": "badjson"})
        self.assertEqual(cm.exception.code, "internal")
        with self.assertRaises(OperatorError) as cm:
            c.request({"action": "sleep"}, timeout_s=1.0)
        self.assertEqual(cm.exception.code, "outcome-unknown")
        with self.assertRaises(OperatorError):
            c.request({"no-action": True})
        c.close()
        with self.assertRaises(Exception):
            c.request({"action": "ping"})

    def test_doctor_shape_and_redaction(self):
        rep = doctor("/nonexistent/binary")
        for key in ("python", "binary", "binary_found", "cli_shape_ok",
                    "openssl", "bwrap", "socket_dir_writable", "errors"):
            self.assertIn(key, rep, key)
        blob = json.dumps(rep)
        self.assertNotIn("lease", blob.replace("socket_dir_writable", ""))
        if HAVE_BIN:
            rep = doctor(BIN)
            self.assertTrue(rep["binary_found"])
            self.assertTrue(rep["cli_shape_ok"], rep)


@unittest.skipUnless(LIVE, "needs matrix-managed + openssl + dev-pki.py")
class OperatorLive(unittest.TestCase):
    def test_start_attach_lifecycle(self):
        tmp = tempfile.mkdtemp(prefix="oplive-")
        self.addCleanup(shutil.rmtree, tmp, True)
        pki = os.path.join(tmp, "pki")
        subprocess.run([sys.executable, DEV_PKI, pki, "--server-name", "localhost"],
                       check=True, capture_output=True)
        with open(os.path.join(pki, "client.der"), "rb") as f:
            fp = hashlib.sha256(f.read()).hexdigest()
        home = os.path.join(tmp, "home")
        cfg = {
            "home": home,
            "components": [{"manifest": {"id": "echo",
                                         "capabilities": ["echo.msg@1"],
                                         "reducer": "echo"},
                            "trusted": True}],
            "grants": {fp: {"components": ["echo"],
                             "capabilities": ["echo.msg@1"]}},
            "tls": {"listen": "127.0.0.1:0",
                    "ca": os.path.join(pki, "ca.der"),
                    "cert": os.path.join(pki, "server.der"),
                    "key": os.path.join(pki, "server-key.der")},
        }
        opki = {"ca": os.path.join(pki, "ca.der"),
                "cert": os.path.join(pki, "client.der"),
                "key": os.path.join(pki, "client-key.der")}
        kernel = start(BIN, cfg, opki)
        self.addCleanup(kernel.close)
        self.assertTrue(kernel.api.startswith("0.1."), kernel.api)
        act = kernel.client.activate("echo", 20000)
        token, fence = act["lease"], act["fence"]
        v = kernel.client.invoke("echo" and token, fence, "op-live-1",
                                 "echo.msg@1", {"ping": 1})
        self.assertTrue(v.get("ok"), v)
        # Attach shares the daemon: closing the attachment kills nothing.
        attached = connect(BIN, kernel.listen,
                           os.path.join(pki, "ca.der"),
                           os.path.join(pki, "client.der"),
                           os.path.join(pki, "client-key.der"))
        v2 = attached.invoke(token, fence, "op-live-2", "echo.msg@1", {})
        self.assertTrue(v2.get("ok"), v2)
        attached.close()
        v3 = kernel.client.invoke(token, fence, "op-live-3", "echo.msg@1", {})
        self.assertTrue(v3.get("ok"), v3)
        # Denials keep their wire codes (L05 sample at the SDK level).
        with self.assertRaises(OperatorError) as cm:
            kernel.client.invoke("dead", "1", "op-x", "echo.msg@1", {})
        self.assertIn(cm.exception.code, ("permission-denied", "stale-generation",
                                          "context-not-active", "transport"))
        kernel.client.release(token, fence)
        kernel.close()
        with self.assertRaises(Exception):
            kernel.client.invoke(token, fence, "op-x", "echo.msg@1", {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
