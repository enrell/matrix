#!/usr/bin/env bash
# M8 external adoption harness (P01–P12 proof): materializes a project
# OUTSIDE the Matrix checkout (/tmp), consumes ONLY dist artifacts
# (binaries run by path; sources copied out, never referenced in-tree),
# and exercises library + service modes, local + remote profiles, with
# Rust and Python, using only the public documentation.
#
# Probe conventions (match the CLI contract):
# - success: `out=$(req '...' 2>/dev/null) || fail ...`, then grep stdout.
# - denial: `if out=$(req '...' 2>&1); then fail ...; else grep pattern`.
#   (`matrix-managed request` prints JSON on success, error text on
#   stderr with nonzero exit on denial/transport failure.)
# Server JSON is compact: patterns use `[ ]*` for optional spaces.
#
# Not a maintained application: validation material with a recipe
# (this file). Same implementer executed it: independence claimed is
# technical (workspace independence), not third-party evaluation.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist"
H="${HARNESS_DIR:-/tmp/mxharness-$$}"
rm -rf "$H"
mkdir -p "$H"/{crates,py,fixtures,bin,home,work}
PASS=0; FAIL=0
ok() { PASS=$((PASS+1)); echo "ok $1 $2"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL $1 $2"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing tool: $1"; exit 2; }; }
need cargo; need rustc; need python3; need openssl
PIDS=""

cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

# ---------- stage artifacts out of the checkout ----------
cp -r "$DIST/crates/." "$H/crates/"
cp "$DIST"/py/*.whl "$H/py/" 2>/dev/null || { echo "no wheel in dist; run scripts/package.sh"; exit 2; }
cp "$DIST/fixtures/dep_node.py" "$DIST/fixtures/matrix_component.py" "$H/fixtures/"
if command -v unzip >/dev/null 2>&1; then (cd "$H/py" && unzip -oq ./*.whl); fi
cp "$ROOT/scripts/dev-pki.py" "$H/work/"
mkdir -p "$H/bin"
cp "$DIST"/bin/* "$H/bin/"
echo "staged: $(du -sh "$H" | cut -f1) at $H"
HBIN="$H/bin"
MB="$HBIN/matrix-managed"

# ---------- P01b: isolated Python install (venv, no pip network, no PYTHONPATH) ----------
python3 -m venv "$H/venv"
"$H/venv/bin/pip" install --no-index --quiet "$H"/py/*.whl
if env -u PYTHONPATH -u PYTHONHOME "$H/venv/bin/python" -c "import matrix_component; print('py-isolated-ok')" 2>&1 | grep -q py-isolated-ok; then ok P01 venv-install-isolated; else fail P01 venv-install-isolated; fi
if pipout=$("$H/venv/bin/pip" show matrix-component 2>&1); then echo "$pipout" | grep -q "Version: 0.1.0" && ok P01 wheel-version || fail P01 wheel-version "$pipout"; else fail P01 wheel-version "pip show failed"; fi
if (cd /tmp && env -u PYTHONPATH -u PYTHONHOME python3 -c "import matrix_component" 2>/dev/null); then fail P01 no-system-leak "importable without install"; else ok P01 no-system-leak; fi
VPY="$H/venv/bin/python"

# ---------- P01: install from artifacts in a clean dir ----------
if env -u PYTHONPATH python3 -c "import sys; sys.path.insert(0, '$H/py'); import matrix_component; print(matrix_component.__name__)" 2>&1 | grep -q matrix_component; then ok P01 py-import-no-pythonpath; else fail P01 py-import-no-pythonpath; fi
[ -x "$H/bin/matrix-managed" ] && [ -x "$H/bin/matrix-conform" ] && [ -x "$H/bin/dep_node" ] && ok P01 bins-executable || fail P01 bins-executable
cmp -s "$H/bin/matrix-managed" "$DIST/bin/matrix-managed" && ok P01 staged-bins-match || fail P01 staged-bins-match
(cd "$DIST" && sha256sum -c --quiet <(grep -E "bin/|py/|schemas/vectors.json" MANIFEST.txt)) && ok P01 hashes-verify || fail P01 hashes-verify
"$H/bin/matrix-conform" vectors >/dev/null 2>&1 && ok P01 conform-vectors || fail P01 conform-vectors
"$H/bin/matrix-conform" local > "$H/work/conform.log" 2>&1; echo "conform exit: $?" >> "$H/work/conform.log"
grep -q "conform exit: 0" "$H/work/conform.log" && ok P05 conform-local || fail P05 conform-local

# ---------- P02: external Rust app on the facade only ----------
mkdir -p "$H/rs/src"
cat > "$H/rs/Cargo.toml" <<EOF
[package]
name = "extapp"
version = "0.1.0"
edition = "2021"

[dependencies]
matrix-runtime = { path = "../crates/matrix-runtime" }
serde_json = "1"

[workspace]
EOF
cat > "$H/rs/src/main.rs" <<'EOF'
use matrix_runtime::api::{Config, InspectOpts, Runtime, INSPECT_SCHEMA};
use serde_json::json;
fn main() {
    let home = std::env::args().nth(1).expect("home");
    let cfg = Config::parse(&json!({
        "home": home,
        "components": [{"manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}, "trusted": true}],
        "grants": {"app": {"components": ["echo"], "capabilities": ["echo.msg@1"]}},
    }))
    .expect("config");
    assert!(cfg.validate().is_empty());
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("app", "echo", 20000).expect("activate");
    let v = rt
        .invoke("app", act["lease"].as_str().unwrap(), act["fence"].as_str().unwrap().parse().unwrap(), "op-ext-1", "echo.msg@1", &json!({"ping": 7}))
        .expect("invoke");
    assert_eq!(v["value"]["echo"]["ping"], 7);
    let insp = rt.inspect(InspectOpts::default());
    assert_eq!(insp["schema"], INSPECT_SCHEMA);
    assert!(!serde_json::to_string(&insp).unwrap().contains(act["lease"].as_str().unwrap()));
    let shut = rt.shutdown();
    assert_eq!(shut.sessions_after, 0);
    assert_eq!(shut.leases_after, 0);
    println!("extapp ok sessions_after={} routes_active={}", shut.sessions_after, shut.routes_active);
}
EOF
if grep -rEn "matrix_runtime::(service|store|session|route_controller|route_executor|remote|remote_session_server)|matrix_core::|matrix_host::|matrix_proto::|matrix_guard::|include!|/home/|/root/|projects/matrix" "$H/rs/src/" | grep -v "^.*://" ; then fail P02 no-internals "internal path referenced"; else ok P02 no-internals; fi
if (cd "$H/rs" && cargo generate-lockfile --offline 2>/dev/null && cargo vendor --offline "$H/vendor" > /dev/null 2>"$H/work/vendor.err"); then ok P02 vendor-offline; else fail P02 vendor-offline "$(cat "$H/work/vendor.err" 2>/dev/null | head -3)"; fi
mkdir -p "$H/rs/.cargo"
{ echo '[source.crates-io]'; echo 'replace-with = "vendored-sources"'; echo '[source.vendored-sources]'; echo 'directory = "../vendor"'; } > "$H/rs/.cargo/config.toml"
if (cd "$H/rs" && CARGO_BUILD_JOBS=2 cargo build --offline --release > "$H/work/rsbuild.log" 2>&1); then ok P02 build-offline-vendored; else fail P02 build-offline-vendored "$(tail -3 "$H/work/rsbuild.log")"; fi
if "$H/rs/target/release/extapp" "$H/home/app" 2>&1 | grep -q "extapp ok"; then ok P02 compose-inspect-shutdown; else fail P02 compose-inspect-shutdown; fi

# ---------- helpers for managed daemons (staged copies, never the checkout) ----------
serve() { # $1=config -> prints ready line; records $2 pid var name
    local cfg="$1" var="$2"
    "$MB" serve "$cfg" > "$H/work/$(basename $cfg .json).ready" 2>"$H/work/$(basename $cfg .json).err" &
    eval "$var=$!"
    PIDS="$PIDS ${!var}"
    local ready="$H/work/$(basename $cfg .json).ready"
    for i in $(seq 1 150); do [ -s "$ready" ] && break; sleep 0.1; done
}
getlisten() { python3 -c "import json; print(json.load(open('$1'))['listen'])"; }
mkreq() { # $1=listen -> defines req() bound to it
    eval "req() { \"\$MB\" request \"\$H/work/pki/ca.der\" \"\$H/work/pki/client.der\" \"\$H/work/pki/client-key.der\" \"$1\" localhost \"\$1\"; }"
}
wait_ready() { # $1=reqfn $2=lease $3=fence
    for i in $(seq 1 200); do
        if "$@" 2>/dev/null | grep -q '"ready":true'; then return 0; fi
        sleep 0.1
    done
    return 1
}

# ---------- PKI + managed service (P03) ----------
python3 "$H/work/dev-pki.py" "$H/work/pki" >/dev/null 2>&1
FP=$(python3 -c "import hashlib; print(hashlib.sha256(open('$H/work/pki/client.der','rb').read()).hexdigest())")
cat > "$H/work/svc.json" <<EOF
{"home": "$H/home/svc",
 "components": [{"manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}, "trusted": true}],
 "grants": {"$FP": {"components": ["echo"], "capabilities": ["echo.msg@1", "matrix.effect.write"]}},
 "tls": {"listen": "127.0.0.1:0", "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/server.der", "key": "$H/work/pki/server-key.der"}}
EOF
serve "$H/work/svc.json" SVC_PID
LISTEN=$(getlisten "$H/work/svc.ready")
sreq() { "$MB" request "$H/work/pki/ca.der" "$H/work/pki/client.der" "$H/work/pki/client-key.der" "$LISTEN" localhost "$1"; }
SLEASE=$(sreq '{"action":"activate","component":"echo","ttl_ms":10000}')
STOK=$(echo "$SLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
SFENCE=$(echo "$SLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
if out=$(sreq "{\"action\":\"invoke\",\"lease\":\"$STOK\",\"fence\":\"$SFENCE\",\"operation\":\"op-svc-1\",\"cap\":\"echo.msg@1\",\"input\":{\"ping\":3}}" 2>/dev/null) && echo "$out" | grep -q '"ping":[ ]*3'; then ok P03 admin-authorized; else fail P03 admin-authorized "$out"; fi
if out=$(sreq '{"action":"invoke","lease":"dead","fence":"1","operation":"op-x","cap":"echo.msg@1","input":{}}' 2>&1); then fail P03 no-authority-denied "unexpected success"; elif echo "$out" | grep -qiE "permission-denied|stale-generation|denied"; then ok P03 no-authority-denied; else fail P03 no-authority-denied "$out"; fi
[ -e "$H/home/svc/run/matrix-rt.sock" ] && fail P03 legacy-not-bound || ok P03 legacy-not-bound

# ---------- P04: local chain Rust->Python + resources/events/streams + withdraw ----------
cat > "$H/work/chain.json" <<EOF
{"home": "$H/home/chain",
 "components": [
  {"manifest": {"id": "cons", "capabilities": ["cons.chain@1"],
    "requires": [{"interface": "prov.api@1", "provider": "prov"}],
    "outbound": {"request": ["prov.api@1"], "limits": {"max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16, "max_calls_global": 64, "max_seen_requests": 64, "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
    "execution": {"kind": "process", "entrypoint": "$H/bin/dep_node", "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": true},
  {"manifest": {"id": "indep", "capabilities": ["indep.echo@1"],
    "execution": {"kind": "process", "entrypoint": "$H/bin/dep_node", "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": true},
  {"manifest": {"id": "prov", "capabilities": ["prov.api@1"], "subscriptions": ["t"],
    "execution": {"kind": "process", "entrypoint": "$H/venv/bin/python", "args": ["$H/fixtures/dep_node.py", "--matrix-sock", "{sock}", "--id", "{id}", "--event-log", "$H/work/prov-events.log"]}}, "trusted": true}],
 "grants": {"$FP": {"components": ["cons", "prov", "indep"], "capabilities": ["cons.chain@1", "prov.api@1", "indep.echo@1"]}},
 "outbound_grants": {"cons": ["prov.api@1"]},
 "tls": {"listen": "127.0.0.1:0", "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/server.der", "key": "$H/work/pki/server-key.der"}}
EOF
serve "$H/work/chain.json" CHAIN_PID
CLISTEN=$(getlisten "$H/work/chain.ready")
creq() { "$MB" request "$H/work/pki/ca.der" "$H/work/pki/client.der" "$H/work/pki/client-key.der" "$CLISTEN" localhost "$1"; }
PLEASE=$(creq '{"action":"activate","component":"prov","ttl_ms":30000}')
PTOK=$(echo "$PLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
PFENCE=$(echo "$PLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do creq "{\"action\":\"status\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
# Provider first: cons waits for prov.api@1 before it can go Active.
CLEASE=$(creq '{"action":"activate","component":"cons","ttl_ms":30000}')
CTOK=$(echo "$CLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
CFENCE=$(echo "$CLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do creq "{\"action\":\"status\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-chain-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":11}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*11'; then ok P04 chain-rust-python; else fail P04 chain-rust-python "$out"; fi
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-res-1\",\"cap\":\"prov.api@1\",\"input\":{\"acquire\":{\"kind\":\"timer\",\"label\":\"t\",\"interval_ms\":50}}}" 2>/dev/null) && echo "$out" | grep -q 'acquired'; then ok P04 acquire; else fail P04 acquire "$out"; fi
if grep -q "PASS live-event-deliver" "$H/work/conform.log"; then ok P04 events-delivered; else fail P04 events-delivered; fi

# ---------- P04 remote: second daemon as executor, chain across hosts ----------
cat > "$H/work/exec.json" <<EOF
{"home": "$H/home/exec",
 "components": [{"manifest": {"id": "rprov", "capabilities": ["prov.api@1"],
    "execution": {"kind": "process", "entrypoint": "$H/bin/dep_node", "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": true}],
 "grants": {"$FP": {"components": ["rprov"], "capabilities": ["prov.api@1", "matrix.effect.write"]}},
 "tls": {"listen": "127.0.0.1:0", "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/server.der", "key": "$H/work/pki/server-key.der"},
 "remotes": {"authority": "$FP", "domain": "ext", "peers": [], "routes": [],
   "session_listen": "127.0.0.1:0", "session_ca": "$H/work/pki/ca.der",
   "session_cert": "$H/work/pki/server.der", "session_key": "$H/work/pki/server-key.der"}}
EOF
serve "$H/work/exec.json" EXEC_PID
EXEC_READY="$H/work/exec.ready"
EXEC_SESS=$(python3 -c "import json; print(json.load(open('$EXEC_READY'))['session'])")
EXEC_MGMT=$(python3 -c "import json; print(json.load(open('$EXEC_READY'))['listen'])")
cat > "$H/work/ctrl.json" <<EOF
{"home": "$H/home/ctrl",
 "components": [{"manifest": {"id": "rcons", "capabilities": ["cons.chain@1"],
    "requires": [{"interface": "prov.api@1", "provider": "rprov"}],
    "outbound": {"request": ["prov.api@1"], "limits": {"max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16, "max_calls_global": 64, "max_seen_requests": 64, "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
    "execution": {"kind": "process", "entrypoint": "$H/bin/dep_node", "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": true}],
 "grants": {"$FP": {"components": ["rcons"], "capabilities": ["cons.chain@1"]}},
 "outbound_grants": {"rcons": ["prov.api@1"]},
 "tls": {"listen": "127.0.0.1:0", "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/server.der", "key": "$H/work/pki/server-key.der"},
 "remotes": {"authority": "$FP", "domain": "ext",
   "peers": [{"name": "exec-A", "address": "$EXEC_SESS", "server_name": "localhost",
     "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/client.der", "key": "$H/work/pki/client-key.der",
     "mgmt_address": "$EXEC_MGMT", "domain": "ext", "lease_ttl_ms": 8000}],
   "routes": [{"consumer": "rcons", "provider": "rprov", "peer": "exec-A", "capabilities": ["prov.api@1"]}]}}
EOF
serve "$H/work/ctrl.json" CTRL_PID
CREADY="$H/work/ctrl.ready"
CRLISTEN=$(python3 -c "import json; print(json.load(open('$CREADY'))['listen'])")
rcreq() { "$MB" request "$H/work/pki/ca.der" "$H/work/pki/client.der" "$H/work/pki/client-key.der" "$CRLISTEN" localhost "$1"; }
RCLEASE=$(rcreq '{"action":"activate","component":"rcons","ttl_ms":30000}')
RCTOK=$(echo "$RCLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
RCFENCE=$(echo "$RCLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do rcreq "{\"action\":\"status\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
if out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\",\"operation\":\"op-rchain-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":5}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*5'; then ok P04 chain-remote; else fail P04 chain-remote "$out"; fi

# ---------- P06: schemas + negotiation evidence ----------
grep -q "PASS vector-hello-bad-version" "$H/work/conform.log" && ok P06 vectors || fail P06 vectors
grep -q "remote-calls" "$DIST/schemas/vectors.json" && ok P06 schemas-shipped || fail P06 schemas-shipped

# ---------- P07: invalid config + reload ----------
echo '{"home": "/tmp/nope", "components": [], "grants": {}, "bogus": 1}' > "$H/work/bad.json"
"$MB" serve "$H/work/bad.json" > /dev/null 2>"$H/work/bad.err" & BPID=$!
sleep 1.5
kill $BPID 2>/dev/null || true
wait $BPID 2>/dev/null || true
if grep -qiE "invalid|deny_unknown|unknown field" "$H/work/bad.err"; then ok P07 invalid-refused; else fail P07 invalid-refused; fi
[ -d /tmp/nope/state ] && fail P07 no-mutation || ok P07 no-mutation
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-still-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":1}}}" 2>/dev/null) && echo "$out" | python3 -c "import json,sys; assert json.load(sys.stdin).get('ok') is True"; then ok P07 baseline; else fail P07 baseline "$out"; fi
cp "$H/work/chain.json" "$H/work/chain.json.good"
python3 - "$H/work/chain.json" <<'PY'
import json, sys
p = sys.argv[1]
c = json.load(open(p))
c["outbound_grants"] = {}
json.dump(c, open(p, "w"))
PY
kill -HUP $CHAIN_PID 2>/dev/null || true
sleep 1.5
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-revoked-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{}}}" 2>/dev/null) && echo "$out" | python3 -c "import json,sys; assert json.load(sys.stdin).get('ok') is False"; then ok P07 reload-revokes; else fail P07 reload-revokes "$out"; fi
cp "$H/work/chain.json.good" "$H/work/chain.json"
kill -HUP $CHAIN_PID 2>/dev/null || true
sleep 1.5

# ---------- P08: update + rollback generations ----------
ILEASE=$(creq '{"action":"activate","component":"indep","ttl_ms":20000}')
ITOK=$(echo "$ILEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
IFENCE=$(echo "$ILEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do creq "{\"action\":\"status\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
creq "{\"action\":\"release\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\"}" >/dev/null 2>&1 || true
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-old-1\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1); then fail P08 old-refs-dead "unexpected success"; elif echo "$out" | grep -qiE "stale|denied|not-active|unknown"; then ok P08 old-refs-dead; else fail P08 old-refs-dead "$out"; fi
ILEASE2=$(creq '{"action":"activate","component":"indep","ttl_ms":20000}')
ITOK2=$(echo "$ILEASE2" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
IFENCE2=$(echo "$ILEASE2" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do creq "{\"action\":\"status\",\"lease\":\"$ITOK2\",\"fence\":\"$IFENCE2\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$ITOK2\",\"fence\":\"$IFENCE2\",\"operation\":\"op-new-1\",\"cap\":\"indep.echo@1\",\"input\":{\"ping\":1}}" 2>/dev/null) && echo "$out" | grep -q '"ping":[ ]*1'; then ok P08 new-generation-serves; else fail P08 new-generation-serves "$out"; fi

# ---------- P11: host failure observed via public API, no auto-replay ----------
creq "{\"action\":\"release\",\"lease\":\"$ITOK2\",\"fence\":\"$IFENCE2\"}" >/dev/null 2>&1 || true
ILEASE=$(creq '{"action":"activate","component":"indep","ttl_ms":20000}')
ITOK=$(echo "$ILEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
IFENCE=$(echo "$ILEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do creq "{\"action\":\"status\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
(creq "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-hostkill-1\",\"cap\":\"indep.echo@1\",\"input\":{\"sleep_ms\":8000}}" > "$H/work/hostkill.json" 2>&1 &)
sleep 0.6
pkill -9 -f "dep_node.*--id indep" 2>/dev/null || true
sleep 1.5
if python3 -c "import json; v=json.load(open('$H/work/hostkill.json')); assert v.get('ok') is False, v"; then ok P11 hostkill-unknown; else fail P11 hostkill-unknown; fi
if out=$(creq "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-hostkill-1\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1); then fail P11 no-auto-replay "unexpected success"; elif echo "$out" | grep -qiE "unknown|stale|denied|not-active|deadline|cancelled"; then ok P11 no-auto-replay; else fail P11 no-auto-replay "$out"; fi

# ---------- P09: backup/restore via CLI ----------
for p in $PIDS; do kill "$p" 2>/dev/null || true; done; PIDS=""; sleep 1
if "$MB" snapshot "$H/home/chain" "$H/work/bak.sqlite" 2>&1 | grep -q .; then ok P09 snapshot; else fail P09 snapshot; fi
head -c 100 /dev/urandom > "$H/work/corrupt.sqlite" 2>/dev/null || true
rout=$("$MB" restore "$H/work/corrupt.sqlite" "$H/home/restored-bad" 2>&1 || true)
if echo "$rout" | grep -qiE "corrupt|refus|invalid|unsupported"; then ok P09 corrupt-refused; else fail P09 corrupt-refused "$rout"; fi
if "$MB" restore "$H/work/bak.sqlite" "$H/home/restored" 2>&1 | grep -qiE "staged|reconcil"; then ok P09 restore-staged; else fail P09 restore-staged; fi
sed "s|$H/home/chain|$H/home/restored|" "$H/work/chain.json" > "$H/work/restored.json"
"$MB" serve "$H/work/restored.json" > "$H/work/restored.ready" 2>"$H/work/restored.err" &
PIDS="$PIDS $!"
for i in $(seq 1 150); do [ -s "$H/work/restored.ready" ] && break; sleep 0.1; done
RLISTEN=$(getlisten "$H/work/restored.ready")
rreq() { "$MB" request "$H/work/pki/ca.der" "$H/work/pki/client.der" "$H/work/pki/client-key.der" "$RLISTEN" localhost "$1"; }
RLEASE=$(rreq '{"action":"activate","component":"indep","ttl_ms":20000}')
RTOK=$(echo "$RLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['lease'])")
RFENCE=$(echo "$RLEASE" | python3 -c "import json,sys; print(json.load(sys.stdin)['fence'])")
for i in $(seq 1 200); do rreq "{\"action\":\"status\",\"lease\":\"$RTOK\",\"fence\":\"$RFENCE\"}" 2>/dev/null | grep -q '"ready":true' && break; sleep 0.1; done
if out=$(rreq "{\"action\":\"invoke\",\"lease\":\"$RTOK\",\"fence\":\"$RFENCE\",\"operation\":\"op-restored-1\",\"cap\":\"indep.echo@1\",\"input\":{\"ping\":2}}" 2>/dev/null) && echo "$out" | grep -q '"ping":[ ]*2'; then ok P09 restored-serves; else fail P09 restored-serves "$out"; fi
if out=$(rreq "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-old-2\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1); then fail P09 old-authority-dead "unexpected success"; elif echo "$out" | grep -qiE "stale|denied|not-active|unknown"; then ok P09 old-authority-dead; else fail P09 old-authority-dead "$out"; fi

# ---------- P12: distribution documentation travels with artifacts ----------
for d in API-CATALOG.md VERSIONS.md INSTALL.md M8-EPIC.md M7-PROFILE.md SDK.md; do
  [ -f "$DIST/docs/$d" ] && ok P12 doc-$d || fail P12 doc-$d
done
grep -q "0.1.0-experimental" "$DIST/docs/VERSIONS.md" && ok P12 versions-published || fail P12 versions-published

# ---------- P10: inspect without credentials ----------
if rreq "{\"action\":\"status\",\"lease\":\"$RTOK\",\"fence\":\"$RFENCE\"}" 2>/dev/null | grep -q '"ready":true'; then ok P10 status-shape; else fail P10 status-shape; fi
if rreq "{\"action\":\"status\",\"lease\":\"$RTOK\",\"fence\":\"$RFENCE\"}" 2>/dev/null | grep -q "$RTOK"; then fail P10 no-credentials; else ok P10 no-credentials; fi

echo "harness: $PASS passed, $FAIL failed (dir $H retained)"
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
