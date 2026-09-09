#!/usr/bin/env bash
# c01 smoke: start -> invoke echo -> close via staged matrix-managed.
# Probe conventions mirror scripts/harness-ml1.sh lines 10-13:
# - success: out=$(req '...' 2>/dev/null) || fail ..., then grep stdout.
# - denial: if out=$(req '...' 2>&1); then fail ...; else grep pattern.
# - Server JSON is compact: patterns use [ ]* for optional spaces.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DIST="$ROOT/dist"
MB="$DIST/bin/matrix-managed"
PROV="$DIST/bin/dep_node"
# c01 pins the staged Rust reference node (cf. harness-ml1.sh ENTRY_rs).
H="${HARNESS_DIR:-/tmp/mxc01-$$}"
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
[ -x "$PROV" ] || { echo "missing $PROV Rust reference; run scripts/package.sh"; exit 2; }
python3 "$ROOT/scripts/dev-pki.py" "$H/work/pki" >/dev/null 2>&1
FP=$(python3 -c "import hashlib; print(hashlib.sha256(open('$H/work/pki/client.der','rb').read()).hexdigest())")
FP="$FP" PROV="$PROV" python3 - "$H/work/c01.json" "$H/home/c01" "$H/work/pki" <<'PY'
import json, os, sys
_, out, home, pki = sys.argv
fp, entry = os.environ["FP"], os.environ["PROV"]
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
serve "$H/work/c01.json" SERVE_PID
LISTEN=$(getlisten "$H/work/c01.ready")
SESSION=$(python3 -c "import json; print(json.load(open('$H/work/c01.ready')).get('session',''))")
mkreq req "$LISTEN"
read LTOK LFENCE <<< "$(activate req prov)" && [ -n "$LTOK" ] && [ -n "$LFENCE" ] && ok c01-activate || fail c01-activate
wait_ready req "$LTOK" "$LFENCE" && ok c01-ready || fail c01-ready
if out=$(req "{\"action\":\"invoke\",\"lease\":\"$LTOK\",\"fence\":\"$LFENCE\",\"operation\":\"op-c01-1\",\"cap\":\"prov.echo@1\",\"input\":{\"hello\":\"world\"}}" 2>/dev/null) && echo "$out" | grep -q '"ok":[ ]*true' && echo "$out" | grep -q '"echo":[ ]*{"hello":[ ]*"world"}'; then ok c01-invoke-echo; else fail c01-invoke-echo; fi
if out=$(req "{\"action\":\"release\",\"lease\":\"$LTOK\",\"fence\":\"$LFENCE\"}" 2>/dev/null) && echo "$out" | grep -q '"state":[ ]*"Disposed"'; then ok c01-release; else fail c01-release; fi
if out=$(req "{\"action\":\"status\",\"lease\":\"$LTOK\",\"fence\":\"$LFENCE\"}" 2>&1); then fail c01-lease-zero "released lease still live"; elif echo "$out" | grep -qiE 'stale|denied|not-active|unknown|expired'; then ok c01-lease-zero; else fail c01-lease-zero; fi
kill "$SERVE_PID" 2>/dev/null || true; wait 2>/dev/null || true; sleep 0.5
if pgrep -f "matrix-managed.*$H/work/c01" >/dev/null 2>&1; then fail c01-no-orphan-managed; else ok c01-no-orphan-managed; fi
if pgrep -f "dep_node.*$H" >/dev/null 2>&1; then fail c01-no-orphan-node; else ok c01-no-orphan-node; fi
# host.sock is never unlinked on shutdown (known runtime gap, file only): any ss
# entry is a live socket (FAIL); a dead file is litter — log it and remove it.
if ss -xa 2>/dev/null | grep -q "$H"; then fail c01-no-orphan-socks-live; else ok c01-no-orphan-socks-live; fi
find "$H" -type s 2>/dev/null | while read -r s; do echo "stale-sock-removed $s"; rm -f "$s"; done
if grep -rE 'BEGIN (RSA |EC |OPENSSH |)PRIVATE KEY' "$LOG" "$H/work/c01.ready" "$H/work/c01.json" >/dev/null 2>&1; then fail c01-no-secrets; else ok c01-no-secrets; fi
echo "c01 done: PASS=$PASS FAIL=$FAIL"; [ "$FAIL" -eq 0 ]
