#!/usr/bin/env python3
"""M7 demo: remote composition across two managed hosts.

Controller (Python cons) chains to an executor (Rust prov) over
`matrix.remote/0.1` without either component implementing transport,
authority propagation or recovery. Generic fixtures only.
Usage: python3 scripts/demo-remote.py
"""
import hashlib
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
binary = root / "target/release/matrix-managed"
rust_node = root / "target/release/examples/dep_node"
py_node = root / "sdk-python/dep_node.py"
for artifact, name in [(binary, "matrix-managed"), (rust_node, "dep_node"),
                       (py_node, "dep_node.py")]:
    assert artifact.exists(), f"missing: {name} ({artifact})"

OUTBOUND = {
    "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
    "max_calls_global": 64, "max_seen_requests": 64,
    "max_queued_bytes": 65536, "max_deadline_ms": 12000,
}


def ext_manifest(mid, caps, entrypoint, args, extra=None):
    manifest = {
        "id": mid, "version": "1.0.0", "capabilities": caps,
        "subscriptions": [], "reducer": "external", "init_state": {},
        "tier": "process", "trust": "trusted", "restart": "permanent",
        "execution": {"kind": "process", "entrypoint": entrypoint,
                      "args": args, "timeout_ms": 30000},
    }
    manifest.update(extra or {})
    return {"manifest": manifest, "trusted": True}


def start(config_path):
    proc = subprocess.Popen([str(binary), "serve", str(config_path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    import selectors
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ)
    if not sel.select(15):
        proc.kill()
        raise RuntimeError(f"server did not start: {config_path}")
    ready = json.loads(proc.stdout.readline())
    assert ready.get("ready"), ready
    return proc, ready


def request(pki, listen, body):
    cmd = [str(binary), "request", str(pki / "ca.der"), str(pki / "client.der"),
           str(pki / "client-key.der"), listen, "localhost", json.dumps(body)]
    return json.loads(subprocess.check_output(cmd, text=True, timeout=15))


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="matrix-m7demo-") as directory:
        home = Path(directory)
        pki = home / "pki"
        subprocess.run([str(root / "scripts/dev-pki.py"), str(pki)],
                       check=True, stdout=subprocess.DEVNULL)
        fp = hashlib.sha256((pki / "client.der").read_bytes()).hexdigest()
        # Executor: Rust provider, unary + session listeners.
        exec_home = home / "exec"
        exec_cfg = {
            "home": str(exec_home),
            "components": [ext_manifest("prov", ["prov.api@1"], str(rust_node),
                                        ["--matrix-sock", "{sock}", "--id", "{id}"])],
            "grants": {fp: {"components": ["prov"],
                            "capabilities": ["prov.api@1", "matrix.effect.write"]}},
            "tls": {"listen": "127.0.0.1:0", "ca": str(pki / "ca.der"),
                    "cert": str(pki / "server.der"), "key": str(pki / "server-key.der")},
            "remotes": {
                "authority": fp,
                "domain": "demo",
                "peers": [], "routes": [],
                "session_listen": "127.0.0.1:0",
                "session_ca": str(pki / "ca.der"),
                "session_cert": str(pki / "server.der"),
                "session_key": str(pki / "server-key.der"),
            },
        }
        exec_cfg_path = home / "exec.json"
        exec_cfg_path.write_text(json.dumps(exec_cfg))
        exec_proc, exec_ready = start(exec_cfg_path)
        try:
            # Controller: Python consumer + remote provider snapshot.
            ctrl_home = home / "ctrl"
            ctrl_cfg = {
                "home": str(ctrl_home),
                "components": [
                    ext_manifest("cons", ["cons.chain@1"], sys.executable or "python3",
                                           [str(py_node), "--matrix-sock", "{sock}", "--id", "{id}"],
                                           {"requires": [{"interface": "prov.api@1", "provider": "prov"}],
                                            "outbound": {"request": ["prov.api@1"], "limits": OUTBOUND}}),
                ],
                "grants": {fp: {"components": ["cons"],
                                "capabilities": ["cons.chain@1"]}},
                "outbound_grants": {"cons": ["prov.api@1"]},
                "tls": {"listen": "127.0.0.1:0", "ca": str(pki / "ca.der"),
                        "cert": str(pki / "server.der"), "key": str(pki / "server-key.der")},
                "remotes": {
                    "authority": fp,
                    "domain": "demo",
                    "peers": [{
                        "name": "exec-A", "address": exec_ready["session"],
                        "server_name": "localhost",
                        "ca": str(pki / "ca.der"), "cert": str(pki / "client.der"),
                        "key": str(pki / "client-key.der"),
                        "mgmt_address": exec_ready["listen"],
                        "domain": "demo", "lease_ttl_ms": 8000,
                    }],
                    "routes": [{"consumer": "cons", "provider": "prov", "peer": "exec-A",
                                "capabilities": ["prov.api@1"]}],
                },
            }
            ctrl_cfg_path = home / "ctrl.json"
            ctrl_cfg_path.write_text(json.dumps(ctrl_cfg))
            ctrl_proc, ctrl_ready = start(ctrl_cfg_path)
            try:
                # Operator activates the consumer, then chains across hosts.
                act = request(pki, ctrl_ready["listen"],
                              {"action": "activate", "component": "cons", "ttl_ms": 20000})
                creds = {"lease": act["lease"], "fence": act["fence"]}
                # Wait for the route to register (reconcile before publish).
                # Fresh operations per attempt: a denied attempt persists
                # under its id (replay, never re-execution).
                deadline = time.monotonic() + 20
                result = None
                attempt = 0
                time.sleep(2)
                while time.monotonic() < deadline:
                    attempt += 1
                    call = {"action": "invoke", **creds, "operation": f"demo-remote-{attempt}",
                            "cap": "cons.chain@1",
                            "input": {"chain": True, "input": {"value": 42}}}
                    result = request(pki, ctrl_ready["listen"], call)
                    if result.get("ok"):
                        break
                    time.sleep(0.5)
                assert result and result.get("ok"), f"remote chain failed: {result}"
                chained = result["value"]["chained"]
                assert chained["echo"]["value"] == 42, result
                assert chained["via"] == "prov" and result["value"]["via"] == "cons", result
                print("1. Python → Rust remote chain OK: cons → exec-A/prov")
                print("M7 remote demo PASS")
            finally:
                ctrl_proc.terminate()
                try:
                    ctrl_proc.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    ctrl_proc.kill()
                    ctrl_proc.communicate()
        finally:
            exec_proc.terminate()
            try:
                exec_proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                exec_proc.kill()
                exec_proc.communicate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
