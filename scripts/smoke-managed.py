#!/usr/bin/env python3
"""Exercise the real CLI, mTLS, durable call/effect, cleanup and offline snapshot."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

root = Path(__file__).resolve().parent.parent
binary = root / "target/release/matrix-managed"
with tempfile.TemporaryDirectory(prefix="matrix-managed-smoke-") as directory:
    home = Path(directory)
    pki = home / "pki"
    subprocess.run([str(root / "scripts/dev-pki.py"), str(pki)], check=True, stdout=subprocess.DEVNULL)
    fingerprint = hashlib.sha256((pki / "client.der").read_bytes()).hexdigest()
    config = {
        "home": str(home / "state"),
        "components": [{"manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}, "trusted": True}],
        "grants": {fingerprint: {"components": ["echo"], "capabilities": ["echo.msg@1", "matrix.effect.write"]}},
        "tls": {"listen": "127.0.0.1:0", "ca": str(pki / "ca.der"), "cert": str(pki / "server.der"), "key": str(pki / "server-key.der")},
    }
    path = home / "config.json"
    path.write_text(json.dumps(config))
    server = subprocess.Popen([str(binary), "serve", str(path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        import selectors
        selector = selectors.DefaultSelector()
        selector.register(server.stdout, selectors.EVENT_READ)
        if not selector.select(10):
            raise RuntimeError("server did not become ready")
        ready = json.loads(server.stdout.readline())
        assert ready["ready"]
        def request(body):
            command = [str(binary), "request", str(pki / "ca.der"), str(pki / "client.der"), str(pki / "client-key.der"), ready["listen"], "localhost", json.dumps(body)]
            return json.loads(subprocess.check_output(command, text=True, timeout=10))
        lease = request({"action": "activate", "component": "echo", "ttl_ms": 10000})
        credentials = {"lease": lease["lease"], "fence": lease["fence"]}
        call = {"action": "invoke", **credentials, "operation": "smoke-call", "cap": "echo.msg@1", "input": {"ping": True}}
        result = request(call)
        assert result["ok"] and result["value"]["echo"]["ping"]
        assert request(call) == result
        assert request({"action": "effect.commit", **credentials, "operation": "smoke-effect", "key": "result", "value": 42})["committed"]
        assert request({"action": "release", **credentials})["state"] == "Disposed"
    finally:
        server.terminate()
        try:
            server.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.communicate()
            raise
    assert server.returncode == 0
    subprocess.run([str(binary), "snapshot", str(home / "state"), str(home / "snapshot.sqlite")], check=True, stdout=subprocess.DEVNULL)
    assert (home / "snapshot.sqlite").stat().st_size > 0
print("managed CLI/mTLS/dedup/fenced-effect/cleanup/snapshot PASS")
