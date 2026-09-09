#!/usr/bin/env bash
# c20 fault: SIGKILL prov mid sleep_ms call; terminal non-ok, no replay, restart.
# Probe conventions mirror scripts/harness-ml1.sh lines 10-13:
# - success: out=$(req '...' 2>/dev/null) || fail ..., then grep stdout.
# - denial: if out=$(req '...' 2>&1); then fail ...; else grep pattern.
# - Server JSON is compact: patterns use [ ]* for optional spaces.
# Kill/restart shape mirrors harness-ml1.sh L06/L07 (killpat, cancel file,
# no-auto-replay, 150x0.2s re-activate poll, old-refs denial).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DIST="$ROOT/dist"
MB="$DIST/bin/matrix-managed"
NODE="$DIST/bin/dep_node"
# c20 pins the staged Rust reference node (cf. harness-ml1.sh ENTRY_rs).
H="${HARNESS_DIR:-/tmp/mxc20-$$}"
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
FP="$FP" NODE="$NODE" python3 - "$H/work/c20.json" "$H/home/c20" "$H/work/pki" <<'PY'
import json, os, sys
_, out, home, pki = sys.argv
fp, entry = os.environ["FP"], os.environ["NODE"]
prov = {"manifest": {"id": "prov", "capabilities": ["prov.echo@1"], "execution": {"kind": "process", "entrypoint": entry, "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": True, "restart": {"max_restarts": 5, "window_ms": 30000, "backoff_ms": 200}}
cfg = {"home": home, "components": [prov], "grants": {fp: {"components": ["prov"], "capabilities": ["prov.echo@1"]}}, "outbound_grants": {}, "tls": {"listen": "127.0.0.1:0", "ca": f"{pki}/ca.der", "cert": f"{pki}/server.der", "key": f"{pki}/server-key.der"}}
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
serve "$H/work/c20.json" SERVE_PID
LISTEN=$(getlisten "$H/work/c20.ready")
SESSION=$(python3 -c "import json; print(json.load(open('$H/work/c20.ready')).get('session',''))")
mkreq req "$LISTEN"
read PTOK PFENCE <<< "$(activate req prov)" && [ -n "$PTOK" ] && [ -n "$PFENCE" ] && ok c20-activate || fail c20-activate
wait_ready req "$PTOK" "$PFENCE" && ok c20-ready || fail c20-ready
(req "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-c20-1\",\"cap\":\"prov.echo@1\",\"input\":{\"sleep_ms\":8000}}" > "$H/work/call.json" 2>&1 &)
sleep 0.6
pkill -9 -f "dep_node.*$H.*--id prov" 2>/dev/null || true
sleep 1.5
if python3 -c "import json; v=json.load(open('$H/work/call.json')); assert v.get('ok') is False, v" 2>/dev/null; then ok c20-terminal-non-ok; else fail c20-terminal-non-ok; fi
out=$(req "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-c20-1\",\"cap\":\"prov.echo@1\",\"input\":{}}" 2>&1) || true
if echo "$out" | grep -q '"ok":true'; then fail c20-no-auto-replay "phantom success"; else ok c20-no-auto-replay; fi
NPTOK=""; NPFENCE=""; ok_restart=0
for i in $(seq 1 150); do
  if tmp=$(activate req prov 2>/dev/null); then
    read NPTOK NPFENCE <<< "$tmp"
    wait_ready req "$NPTOK" "$NPFENCE" 2>/dev/null && { ok_restart=1; break; }
  fi
  sleep 0.2
done
[ "$ok_restart" = 1 ] && ok c20-restart || fail c20-restart
if out=$(req "{\"action\":\"invoke\",\"lease\":\"$NPTOK\",\"fence\":\"$NPFENCE\",\"operation\":\"op-c20-2\",\"cap\":\"prov.echo@1\",\"input\":{\"hello\":\"again\"}}" 2>/dev/null) && echo "$out" | grep -q '"ok":[ ]*true' && echo "$out" | grep -q '"echo":[ ]*{"hello":[ ]*"again"}'; then ok c20-serves-new-op; else fail c20-serves-new-op; fi
if out=$(req "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-c20-3\",\"cap\":\"prov.echo@1\",\"input\":{}}" 2>&1); then fail c20-old-refs-dead "unexpected success"; elif echo "$out" | grep -qiE 'stale|denied|not-active|unknown'; then ok c20-old-refs-dead; else fail c20-old-refs-dead; fi
if out=$(req "{\"action\":\"release\",\"lease\":\"$NPTOK\",\"fence\":\"$NPFENCE\"}" 2>/dev/null) && echo "$out" | grep -q '"state":[ ]*"Disposed"'; then ok c20-release; else fail c20-release; fi
kill "$SERVE_PID" 2>/dev/null || true; wait 2>/dev/null || true; sleep 0.5
if pgrep -f "matrix-managed.*$H/work/c20" >/dev/null 2>&1; then fail c20-no-orphan-managed; else ok c20-no-orphan-managed; fi
if pgrep -f "dep_node.*$H" >/dev/null 2>&1; then fail c20-no-orphan-node; else ok c20-no-orphan-node; fi
# host.sock is never unlinked on shutdown (known runtime gap, file only): any ss
# entry is a live socket (FAIL); a dead file is litter — log it and remove it.
if ss -xa 2>/dev/null | grep -q "$H"; then fail c20-no-orphan-socks-live; else ok c20-no-orphan-socks-live; fi
find "$H" -type s 2>/dev/null | while read -r s; do echo "stale-sock-removed $s"; rm -f "$s"; done
if grep -rE 'BEGIN (RSA |EC |OPENSSH |)PRIVATE KEY' "$LOG" "$H/work/c20.ready" "$H/work/c20.json" >/dev/null 2>&1; then fail c20-no-secrets; else ok c20-no-secrets; fi
echo "c20 done: PASS=$PASS FAIL=$FAIL"; [ "$FAIL" -eq 0 ]
