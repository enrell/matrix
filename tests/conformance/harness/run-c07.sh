#!/usr/bin/env bash
# c07 chain: cons -> prov via outbound grant, staged matrix-managed.
# Probe conventions mirror scripts/harness-ml1.sh lines 10-13:
# - success: out=$(req '...' 2>/dev/null) || fail ..., then grep stdout.
# - denial: if out=$(req '...' 2>&1); then fail ...; else grep pattern.
# - Server JSON is compact: patterns use [ ]* for optional spaces.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DIST="$ROOT/dist"
MB="$DIST/bin/matrix-managed"
NODE="$DIST/bin/dep_node"
# c07 reuses the staged Rust reference node for both roles (cf. ENTRY_rs).
H="${HARNESS_DIR:-/tmp/mxc07-$$}"
rm -rf "$H"; mkdir -p "$H"/work/home
LOG="$H/work/harness.log"
exec > >(tee "$LOG") 2>&1
PASS=0; FAIL=0
ok() { PASS=$((PASS+1)); echo "ok $*"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL $*"; }
PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
[ -x "$MB" ] || { echo "missing $MB; run scripts/package.sh"; exit 2; }
[ -x "$NODE" ] || { echo "missing $NODE Rust reference; run scripts/package.sh"; exit 2; }
python3 "$ROOT/scripts/dev-pki.py" "$H/work/pki" >/dev/null 2>&1
FP=$(python3 -c "import hashlib; print(hashlib.sha256(open('$H/work/pki/client.der','rb').read()).hexdigest())")
FP="$FP" NODE="$NODE" python3 - "$H/work/c07.json" "$H/home/c07" "$H/work/pki" <<'PY'
import json, os, sys
_, out, home, pki = sys.argv
fp, entry = os.environ["FP"], os.environ["NODE"]
args = ["--matrix-sock", "{sock}", "--id", "{id}"]
r = {"max_restarts": 5, "window_ms": 30000, "backoff_ms": 200}
lim = {"max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16, "max_calls_global": 64, "max_seen_requests": 64, "max_queued_bytes": 65536, "max_deadline_ms": 12000}
prov = {"manifest": {"id": "prov", "capabilities": ["prov.echo@1"], "execution": {"kind": "process", "entrypoint": entry, "args": args}}, "trusted": True, "restart": r}
cons = {"manifest": {"id": "cons", "capabilities": ["cons.chain@1"], "execution": {"kind": "process", "entrypoint": entry, "args": args}, "requires": [{"interface": "prov.echo@1", "provider": "prov"}], "outbound": {"request": ["prov.echo@1"], "limits": lim}}, "trusted": True, "restart": r}
cfg = {"home": home, "components": [prov, cons], "grants": {fp: {"components": ["prov", "cons"], "capabilities": ["prov.echo@1", "cons.chain@1"]}}, "outbound_grants": {"cons": ["prov.echo@1"]}, "tls": {"listen": "127.0.0.1:0", "ca": f"{pki}/ca.der", "cert": f"{pki}/server.der", "key": f"{pki}/server-key.der"}}
json.dump(cfg, open(out, "w"))
PY
serve() { # $1=config -> records $2 pid var name (mirrors harness-ml1.sh:123)
  local cfg="$1" var="$2"
  "$MB" serve "$cfg" > "$H/work/$(basename $cfg .json).ready" 2>"$H/work/$(basename $cfg .json).err" &
  eval "$var=$!"
  PIDS="$PIDS ${!var}"
  local ready="$H/work/$(basename $cfg .json).ready"
  for i in $(seq 1 150); do [ -s "$ready" ] && break; sleep 0.1; done
}
getlisten() { python3 -c "import json; print(json.load(open('$1'))['listen'])"; }
mkreq() { eval "$1() { \"\$MB\" request \"\$H/work/pki/ca.der\" \"\$H/work/pki/client.der\" \"\$H/work/pki/client-key.der\" \"$2\" localhost \"\$1\"; }"; }
activate() { # $1=reqfn $2=component -> prints "lease fence"
  local out
  out=$($1 "{\"action\":\"activate\",\"component\":\"$2\",\"ttl_ms\":30000}") || return 1
  echo "$out" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['lease'], d['fence'])"
}
wait_ready() { # $1=reqfn $2=lease $3=fence
  local req="$1" tok="$2" fn="$3"
  for i in $(seq 1 200); do
    "$req" "{\"action\":\"status\",\"lease\":\"$tok\",\"fence\":\"$fn\"}" 2>/dev/null | grep -q '"ready":true' && return 0
    sleep 0.1
  done
  return 1
}
serve "$H/work/c07.json" SERVE_PID
LISTEN=$(getlisten "$H/work/c07.ready")
SESSION=$(python3 -c "import json; print(json.load(open('$H/work/c07.ready')).get('session',''))")
mkreq req "$LISTEN"
read PTOK PFENCE <<< "$(activate req prov)" && [ -n "$PTOK" ] && [ -n "$PFENCE" ] && ok c07-activate-prov || fail c07-activate-prov
wait_ready req "$PTOK" "$PFENCE" && ok c07-ready-prov || fail c07-ready-prov
read CTOK CFENCE <<< "$(activate req cons)" && [ -n "$CTOK" ] && [ -n "$CFENCE" ] && ok c07-activate-cons || fail c07-activate-cons
wait_ready req "$CTOK" "$CFENCE" && ok c07-ready-cons || fail c07-ready-cons
if out=$(req "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-c07-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"hello\":\"world\"}}}" 2>/dev/null) && echo "$out" | grep -q '"ok":[ ]*true' && echo "$out" | grep -q '"chained"' && echo "$out" | grep -q '"hello":[ ]*"world"'; then ok c07-invoke-chain; else fail c07-invoke-chain; fi
if out=$(req "{\"action\":\"release\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\"}" 2>/dev/null) && echo "$out" | grep -q '"state":[ ]*"Disposed"'; then ok c07-release-cons; else fail c07-release-cons; fi
if out=$(req "{\"action\":\"release\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\"}" 2>/dev/null) && echo "$out" | grep -q '"state":[ ]*"Disposed"'; then ok c07-release-prov; else fail c07-release-prov; fi
if out=$(req "{\"action\":\"status\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\"}" 2>&1); then fail c07-lease-zero-cons "released cons lease still live"; elif echo "$out" | grep -qiE 'stale|denied|not-active|unknown|expired'; then ok c07-lease-zero-cons; else fail c07-lease-zero-cons; fi
if out=$(req "{\"action\":\"status\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\"}" 2>&1); then fail c07-lease-zero-prov "released prov lease still live"; elif echo "$out" | grep -qiE 'stale|denied|not-active|unknown|expired'; then ok c07-lease-zero-prov; else fail c07-lease-zero-prov; fi
kill "$SERVE_PID" 2>/dev/null || true; wait 2>/dev/null || true; sleep 0.5
if pgrep -f "matrix-managed.*$H/work/c07" >/dev/null 2>&1; then fail c07-no-orphan-managed; else ok c07-no-orphan-managed; fi
if pgrep -f "dep_node.*$H" >/dev/null 2>&1; then fail c07-no-orphan-node; else ok c07-no-orphan-node; fi
# host.sock is never unlinked on shutdown (known runtime gap, file only): any ss
# entry is a live socket (FAIL); a dead file is litter — log it and remove it.
if ss -xa 2>/dev/null | grep -q "$H"; then fail c07-no-orphan-socks-live; else ok c07-no-orphan-socks-live; fi
find "$H" -type s 2>/dev/null | while read -r s; do echo "stale-sock-removed $s"; rm -f "$s"; done
if grep -rE 'BEGIN (RSA |EC |OPENSSH |)PRIVATE KEY' "$LOG" "$H/work/c07.ready" "$H/work/c07.json" >/dev/null 2>&1; then fail c07-no-secrets; else ok c07-no-secrets; fi
echo "c07 done: PASS=$PASS FAIL=$FAIL"; [ "$FAIL" -eq 0 ]
