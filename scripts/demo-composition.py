#!/usr/bin/env python3
"""M6 demo: local composition across external components.

Managed profile with a Rust → Python chain, a Rust independent, operator
outbound grants, withdraw during execution, and reintroduction.
Generic fixtures (`dep_node` / `dep_node.py`); no application.
Uso: python3 scripts/demo-composition.py
"""
import hashlib
import json
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
binary = root / "target/release/matrix-managed"
rust_node = root / "target/release/examples/dep_node"
py_node = root / "sdk-python/dep_node.py"
for artifact, name in [(binary, "matrix-managed"), (rust_node, "dep_node"),
                       (py_node, "dep_node.py")]:
    assert artifact.exists(), f"ausente: {name} ({artifact})"

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


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="matrix-m6demo-") as directory:
        home = Path(directory)
        pki = home / "pki"
        subprocess.run([str(root / "scripts/dev-pki.py"), str(pki)],
                       check=True, stdout=subprocess.DEVNULL)
        fingerprint = hashlib.sha256((pki / "client.der").read_bytes()).hexdigest()
        cons = ext_manifest("cons", ["cons.chain@1"], str(rust_node),
                            ["--matrix-sock", "{sock}", "--id", "{id}"],
                            {"requires": [{"interface": "prov.api@1", "provider": "prov"}],
                             "outbound": {"request": ["prov.api@1"], "limits": OUTBOUND}})
        prov = ext_manifest("prov", ["prov.api@1"], sys.executable,
                            [str(py_node), "--matrix-sock", "{sock}", "--id", "{id}"])
        indep = ext_manifest("indep", ["indep.echo@1"], str(rust_node),
                             ["--matrix-sock", "{sock}", "--id", "{id}"])
        config = {
            "home": str(home / "state"),
            "components": [cons, prov, indep],
            "grants": {fingerprint: {"components": ["cons", "prov", "indep"],
                                     "capabilities": ["cons.chain@1", "prov.api@1",
                                                      "indep.echo@1", "matrix.effect.write"]}},
            "outbound_grants": {"cons": ["prov.api@1"]},
            "tls": {"listen": "127.0.0.1:0", "ca": str(pki / "ca.der"),
                    "cert": str(pki / "server.der"), "key": str(pki / "server-key.der")},
        }
        path = home / "config.json"
        path.write_text(json.dumps(config))
        server = subprocess.Popen([str(binary), "serve", str(path)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            import selectors
            selector = selectors.DefaultSelector()
            selector.register(server.stdout, selectors.EVENT_READ)
            if not selector.select(15):
                raise RuntimeError("server did not become ready")
            ready = json.loads(server.stdout.readline())
            assert ready["ready"], ready

            def request(body):
                command = [str(binary), "request", str(pki / "ca.der"),
                           str(pki / "client.der"), str(pki / "client-key.der"),
                           ready["listen"], "localhost", json.dumps(body)]
                return json.loads(subprocess.check_output(command, text=True, timeout=15))

            def activate(logical):
                lease = request({"action": "activate", "component": logical, "ttl_ms": 25000})
                assert "lease" in lease, lease
                return {"lease": lease["lease"], "fence": lease["fence"]}

            def invoke(creds, cap, input, op):
                return request({"action": "invoke", **creds, "operation": op,
                                "cap": cap, "input": input})

            # Provider first (cons requires it); outbound grants already in config.
            prov_creds = activate("prov")
            cons_creds = activate("cons")
            indep_creds = activate("indep")

            # 1. Rust → Python chain with a verifiable result.
            v = invoke(cons_creds, "cons.chain@1", {"chain": True, "input": {"value": 42}}, "demo-1")
            assert v["ok"], v
            assert v["value"]["chained"]["echo"]["value"] == 42, v
            assert v["value"]["chained"]["via"] == "prov", v
            assert v["value"]["via"] == "cons", v
            print("1. Rust → Python chain OK:", v["value"]["via"], "→", v["value"]["chained"]["via"])

            # 2. Withdraw during execution: release the provider mid-child.
            box: dict = {}
            slow = threading.Thread(
                target=lambda: box.update(invoke(
                    cons_creds, "cons.chain@1",
                    {"chain": True, "input": {"sleep_ms": 15000}}, "demo-2")))
            slow.start()
            time.sleep(1.0)
            # Trabalho em voo segura a retirada (I07): estaciona e revoga.
            state = request({"action": "release", **prov_creds})["state"]
            assert state in ("Disposed", "CleanupPending"), state
            slow.join(timeout=25)
            assert not slow.is_alive(), "child never settled after withdraw"
            assert not box.get("ok", False), f"sucesso falso: {box}"
            assert box.get("value", {}).get("code") == "cancelled", box
            print("2. mid-execution withdraw denies with no false success:", box.get("value"))
            w = invoke(indep_creds, "indep.echo@1", {"ping": 1}, "demo-indep")
            assert w["ok"], f"independent still serving: {w}"
            print("   independent stays responsive during withdraw")

            # 3. Reintroduction under a new identity: withdraw created another
            # generation; old credentials are stale (I03). Fresh lease.
            request({"action": "release", **cons_creds})
            prov_creds = activate("prov")
            cons_creds = activate("cons")
            v = invoke(cons_creds, "cons.chain@1", {"chain": True, "input": {"value": 7}}, "demo-3")
            assert v["ok"] and v["value"]["chained"]["echo"]["value"] == 7, v
            print("3. reintroduction recomposes the chain OK (new generation, fresh lease)")

            # 4. Activation resources via SDK: acquire timer, release, double-release denies.
            v = invoke(cons_creds, "cons.chain@1",
                       {"acquire": {"kind": "timer", "label": "demo-t", "interval_ms": 50}}, "demo-4")
            assert v["ok"], v
            handle = int(v["value"]["acquired"]["handle"])
            v = invoke(cons_creds, "cons.chain@1", {"release": handle}, "demo-5")
            assert v["ok"], v
            v = invoke(cons_creds, "cons.chain@1", {"release": handle}, "demo-6")
            assert not v["ok"], f"duplo release deveria negar: {v}"
            assert v["value"]["code"] == "invalid-message", v
            print("4. external resources acquire/release/deny OK (handle %d)" % handle)

            # 5. Bounded streams + bidi legs (C26): exact chunk counts, and
            # the bidi leg keeps the child open so chunks bind to it.
            # stream_send needs no bindings: drive the Python node directly.
            v = invoke(prov_creds, "prov.api@1",
                       {"stream_send": {"stream_id": "s-demo", "chunks": 8, "chunk_bytes": 64}},
                       "demo-7")
            assert v["ok"] and v["value"]["stream_sent"] == 8, v
            print("5. stream_send 8x64 OK (stream_sent 8)")
            v = invoke(cons_creds, "cons.chain@1",
                       {"chain_with_streams": {"stream_id": "s-bidi", "chunks": 8,
                                              "chunk_bytes": 64,
                                              "input": {"sleep_ms": 500, "value": 9}}},
                       "demo-8")
            assert v["ok"], v
            assert v["value"]["stream_sent"] == 8, v
            assert v["value"]["chained"]["echo"]["value"] == 9, v
            assert v["value"]["chained"]["via"] == "prov", v
            print("6. chain_with_streams bidi leg OK (stream_sent 8, chained via prov)")
        finally:
            server.terminate()
            try:
                server.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.communicate()
                raise
        assert server.returncode == 0, server.returncode
    print("M6 composition demo PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
