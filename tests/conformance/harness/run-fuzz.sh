#!/usr/bin/env bash
# fuzz live: replay seeded generator cases against staged matrix-managed.
# Triage notes: bare release + raw_bytes JSON echo per otherwise-branch
# (expect ok+echo); binary refusal is frame-level (matrix-conform vectors).
# Probe conventions mirror scripts/harness-ml1.sh lines 10-13 (see run.sh):
# - success: out=$(req '...' 2>/dev/null) || fail, then grep stdout.
# - denial: if out=$(req '...' 2>&1); then fail; else grep pattern.
# - Server JSON is compact: patterns use [ ]* for optional spaces.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DIST="$ROOT/dist"
MB="$DIST/bin/matrix-managed"
PROV="$DIST/bin/dep_node"
H="${HARNESS_DIR:-/tmp/mxfuzz-$$}"
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
FP="$FP" PROV="$PROV" python3 - "$H/work/fuzz.json" "$H/home/fuzz" "$H/work/pki" <<'PY'
import json, os, sys
_, out, home, pki = sys.argv
fp, entry = os.environ["FP"], os.environ["PROV"]
prov = {"manifest": {"id": "prov", "capabilities": ["prov.echo@1"], "execution": {"kind": "process", "entrypoint": entry, "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": True, "restart": {"max_restarts": 5, "window_ms": 30000, "backoff_ms": 200}}
cfg = {"home": home, "components": [prov], "grants": {fp: {"components": ["prov"], "capabilities": ["prov.echo@1"]}}, "outbound_grants": {}, "tls": {"listen": "127.0.0.1:0", "ca": f"{pki}/ca.der", "cert": f"{pki}/server.der", "key": f"{pki}/server-key.der"}}
json.dump(cfg, open(out, "w"))
PY
serve() { "$MB" serve "$1" > "$H/work/$(basename $1 .json).ready" 2>"$H/work/$(basename $1 .json).err" & PIDS="$PIDS $!"; for i in $(seq 1 150); do [ -s "$H/work/$(basename $1 .json).ready" ] && break; sleep 0.1; done; }
getlisten() { python3 -c "import json; print(json.load(open('$1'))['listen'])"; }
mkreq() { eval "$1() { \"\$MB\" request \"\$H/work/pki/ca.der\" \"\$H/work/pki/client.der\" \"\$H/work/pki/client-key.der\" \"$2\" localhost \"\$1\"; }"; }
activate() { local out; out=$($1 "{\"action\":\"activate\",\"component\":\"$2\",\"ttl_ms\":30000}") || return 1; echo "$out" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['lease'], d['fence'])"; }
wait_ready() { for i in $(seq 1 200); do "$1" "{\"action\":\"status\",\"lease\":\"$2\",\"fence\":\"$3\"}" 2>/dev/null | grep -q '"ready":true' && return 0; sleep 0.1; done; return 1; }
serve "$H/work/fuzz.json"
LISTEN=$(getlisten "$H/work/fuzz.ready")
mkreq req "$LISTEN"
read LTOK LFENCE <<< "$(activate req prov)" && [ -n "$LTOK" ] && ok fuzz-activate || fail fuzz-activate
wait_ready req "$LTOK" "$LFENCE" && ok fuzz-ready || fail fuzz-ready
(cd "$ROOT" && python3 tests/conformance/scenarios/fuzz.py --seed 0 --count 50 > "$H/work/cases.json") && ok fuzz-generate || fail fuzz-generate
N=$(python3 -c "import json; print(len(json.load(open('$H/work/cases.json'))))")
SHRUNK=0
for i in $(seq 0 $((N-1))); do
  meta=$(python3 - "$H/work/cases.json" "$i" "$LTOK" "$LFENCE" <<'PY'
import json, sys
_, path, idx, tok, fence = sys.argv; c = json.load(open(path))[int(idx)]
print(json.dumps({"req": {"action": "invoke", "lease": tok, "fence": fence, "operation": c["operation"], "cap": c["cap"], "input": c["input"]}, "name": c["name"], "expect": c["expect"], "match": c["match"], "case": c}))
PY
)
  line=$(echo "$meta" | python3 -c "import json,sys; m=json.load(sys.stdin); print(m['name']+'\t'+m['expect']+'\t'+m['match'])")
  IFS=$'\t' read -r name expect match <<< "$line"
  echo "$meta" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['req']))" > "$H/work/req.json"
  out=$(req "$(cat "$H/work/req.json")" 2>&1) || true
  if [ -z "$out" ]; then fail "fuzz-$name empty-reply"; continue; fi
  okline=0; echo "$out" | grep -q '"ok":[ ]*true' && okline=1 || true
  matchline=0; echo "$out" | grep -qiE "$match" && matchline=1 || true
  pass=0; case "$expect" in ok) [ "$okline" = 1 ] && [ "$matchline" = 1 ] && pass=1 || true;; deny) [ "$okline" = 0 ] && [ "$matchline" = 1 ] && pass=1 || true;; *) [ "$matchline" = 1 ] && pass=1 || true;; esac
  if [ "$pass" = 1 ]; then ok "fuzz-$name"; else fail "fuzz-$name ${out:0:300}"; [ "$SHRUNK" = 0 ] && { SHRUNK=1; echo "shrink seed=0 idx=$i name=$name"; echo "$meta" | python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['case']))"; } || true; fi
done
if out=$(req "{\"action\":\"release\",\"lease\":\"$LTOK\",\"fence\":\"$LFENCE\"}" 2>/dev/null) && echo "$out" | grep -q '"state":[ ]*"Disposed"'; then ok fuzz-release; else fail fuzz-release; fi
if out=$(req "{\"action\":\"status\",\"lease\":\"$LTOK\",\"fence\":\"$LFENCE\"}" 2>&1); then fail fuzz-lease-zero "released lease still live"; elif echo "$out" | grep -qiE 'stale|denied|not-active|unknown|expired'; then ok fuzz-lease-zero; else fail fuzz-lease-zero; fi
for p in $PIDS; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; sleep 0.5
if pgrep -f "matrix-managed.*$H/work/fuzz" >/dev/null 2>&1; then fail fuzz-no-orphan-managed; else ok fuzz-no-orphan-managed; fi
if pgrep -f "dep_node.*$H" >/dev/null 2>&1; then fail fuzz-no-orphan-node; else ok fuzz-no-orphan-node; fi
if ss -xa 2>/dev/null | grep -q "$H"; then fail fuzz-no-orphan-socks-live; else ok fuzz-no-orphan-socks-live; fi
find "$H" -type s 2>/dev/null | while read -r s; do echo "stale-sock-removed $s"; rm -f "$s"; done
if grep -rE 'BEGIN (RSA |EC |OPENSSH |)PRIVATE KEY' "$LOG" "$H/work/fuzz.ready" "$H/work/fuzz.json" >/dev/null 2>&1; then fail fuzz-no-secrets; else ok fuzz-no-secrets; fi
echo "fuzz done: PASS=$PASS FAIL=$FAIL"; [ "$FAIL" -eq 0 ]
