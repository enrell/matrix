#!/usr/bin/env bash
# nxn interop (tests/conformance/interoperability/nxn.yaml): default runs the
# mandated subset only (NOT the full 81): rs->rs reference smoke always, plus
# py<->js live iff both staged nodes exist, plus chain3 js->go->rs iff staged.
# --full enumerates all 81 ordered pairs but runs only staged-available ones.
# Missing toolchain/staged node => SKIP (never FAIL); FAIL only on live mismatch.
# Helpers mirror run-c07.sh: serve/mkreq/activate/wait_ready/invoke/release,
# ss -xa live-socket gate, stale-sock log-and-remove, no-secrets scan.
set -euo pipefail
FULL=0
if [ "${1:-}" = "--full" ]; then FULL=1; fi
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DIST="$ROOT/dist"
MB="$DIST/bin/matrix-managed"
RS="$DIST/bin/dep_node"
STAGED="${MX_STAGED_DIR:-$ROOT/dist/staged}"
H="${HARNESS_DIR:-/tmp/mxnxn-$$}"
rm -rf "$H"; mkdir -p "$H"/work/home
LOG="$H/work/harness.log"
exec > >(tee "$LOG") 2>&1
PASS=0; FAIL=0; SKIP=0
ok() { PASS=$((PASS+1)); echo "ok $*"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL $*"; }
skip() { SKIP=$((SKIP+1)); echo "SKIP $*"; }
PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT
[ -x "$MB" ] || { echo "missing $MB; run scripts/package.sh"; exit 2; }
[ -x "$RS" ] || { echo "missing $RS Rust reference; run scripts/package.sh"; exit 2; }
python3 "$ROOT/scripts/dev-pki.py" "$H/work/pki" >/dev/null 2>&1
FP=$(python3 -c "import hashlib; print(hashlib.sha256(open('$H/work/pki/client.der','rb').read()).hexdigest())")
entry() { # $1=lang -> entrypoint path, or nonzero when toolchain/node missing
  case "$1" in
    py) command -v python3 >/dev/null || return 1 ;;
    js) command -v node >/dev/null || return 1 ;;
  esac
  if [ "$1" = "rs" ]; then echo "$RS"; return 0; fi
  [ -x "$STAGED/mx-node-$1" ] && echo "$STAGED/mx-node-$1"
}
mkcfg() { # $1=name $2=cons-entry $3=prov-entry [$4=mid-entry]: cons->mid/prov chain
  E_CONS="$2" E_PROV="$3" E_MID="${4:-}" FP="$FP" python3 - "$H/work/$1.json" "$H/home/$1" "$H/work/pki" <<'PY'
import json,os,sys
_,out,home,pki=sys.argv; ec,ep,em=os.environ["E_CONS"],os.environ["E_PROV"],os.environ["E_MID"]
r={"max_restarts":5,"window_ms":30000,"backoff_ms":200}
lim={"max_depth":3,"max_children_per_parent":4,"max_calls_per_session":16,"max_calls_global":64,"max_seen_requests":64,"max_queued_bytes":65536,"max_deadline_ms":12000}
a=["--matrix-sock","{sock}","--id","{id}"]
def comp(cid,caps,e,req=None,rp=None):
 m={"id":cid,"capabilities":caps,"execution":{"kind":"process","entrypoint":e,"args":a}}
 if req: m["requires"]=[{"interface":req,"provider":rp}]; m["outbound"]={"request":[req],"limits":lim}
 return {"manifest":m,"trusted":True,"restart":r}
t,rt=("mid.chain@1","mid") if em else ("prov.echo@1","prov")
cs=[comp("cons",["cons.chain@1"],ec,t,rt)]
if em: cs.append(comp("mid",["mid.chain@1"],em,"prov.echo@1","prov"))
cs.append(comp("prov",["prov.echo@1"],ep))
cp=["cons.chain@1","prov.echo@1"]+(["mid.chain@1"] if em else [])
og={"cons":[t]}
if em: og["mid"]=["prov.echo@1"]
ids=[c["manifest"]["id"] for c in cs]
cfg={"home":home,"components":cs,"grants":{os.environ["FP"]:{"components":ids,"capabilities":cp}},"outbound_grants":og,"tls":{"listen":"127.0.0.1:0","ca":f"{pki}/ca.der","cert":f"{pki}/server.der","key":f"{pki}/server-key.der"}}
json.dump(cfg,open(out,"w"))
PY
}
serve() { local cfg="$1" var="$2"; "$MB" serve "$cfg" >"$H/work/$(basename $cfg .json).ready" 2>"$H/work/$(basename $cfg .json).err" & eval "$var=$!"; PIDS="$PIDS ${!var}"; local ready="$H/work/$(basename $cfg .json).ready"; for i in $(seq 1 150); do [ -s "$ready" ] && break; sleep 0.1; done; }
getlisten() { python3 -c "import json; print(json.load(open('$1'))['listen'])"; }
mkreq() { eval "$1() { \"\$MB\" request \"\$H/work/pki/ca.der\" \"\$H/work/pki/client.der\" \"\$H/work/pki/client-key.der\" \"$2\" localhost \"\$1\"; }"; }
activate() { local out; out=$($1 "{\"action\":\"activate\",\"component\":\"$2\",\"ttl_ms\":30000}") || return 1; echo "$out" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['lease'], d['fence'])"; }
wait_ready() { local req="$1" tok="$2" fn="$3"; for i in $(seq 1 200); do "$req" "{\"action\":\"status\",\"lease\":\"$tok\",\"fence\":\"$fn\"}" 2>/dev/null | grep -q '"ready":true' && return 0; sleep 0.1; done; return 1; }
invoke() { # $1=reqfn $2=lease $3=fence $4=op $5=via-pattern: chain invoke, asserts chained+via
  local out; out=$($1 "{\"action\":\"invoke\",\"lease\":\"$2\",\"fence\":\"$3\",\"operation\":\"$4\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"hello\":\"world\"}}}" 2>/dev/null) || return 1; echo "$out" | grep -q '"chained":[ ]*{' && echo "$out" | grep -q "$5"
}
release() { local out; out=$($1 "{\"action\":\"release\",\"lease\":\"$2\",\"fence\":\"$3\"}" 2>/dev/null) || return 1; echo "$out" | grep -q '"state":[ ]*"Disposed"'; }
run_pair() { # $1=id $2=cons-lang $3=prov-lang: SKIP unless both ends staged
  local ec ep L PTOK PFENCE CTOK CFENCE out
  ec=$(entry "$2") || { skip "$1 cons-$2 not staged"; return 0; }
  ep=$(entry "$3") || { skip "$1 prov-$3 not staged"; return 0; }
  mkcfg "$1" "$ec" "$ep"; serve "$H/work/$1.json" P; L=$(getlisten "$H/work/$1.ready"); mkreq qq "$L"
  read PTOK PFENCE <<< "$(activate qq prov)" && wait_ready qq "$PTOK" "$PFENCE" || { fail "$1-activate-prov"; return 0; }
  read CTOK CFENCE <<< "$(activate qq cons)" && wait_ready qq "$CTOK" "$CFENCE" || { fail "$1-activate-cons"; return 0; }
  if invoke qq "$CTOK" "$CFENCE" "op-$1" '"via":[ ]*"prov"'; then ok "$1 $2->$3"; else fail "$1 $2->$3"; fi
  release qq "$CTOK" "$CFENCE" || fail "$1-release-cons"; release qq "$PTOK" "$PFENCE" || fail "$1-release-prov"
  if out=$(qq "{\"action\":\"status\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\"}" 2>&1); then fail "$1-lease-zero"; elif echo "$out" | grep -qiE 'stale|denied|not-active|unknown|expired'; then ok "$1-lease-zero"; else fail "$1-lease-zero"; fi
}
run_chain3() { # js->go->rs triple-nested chain; SKIP unless all three staged
  local ec em ep L PTOK PFENCE MTOK MFENCE CTOK CFENCE out n
  ec=$(entry js) || { skip "chain3 cons-js not staged"; return 0; }
  em=$(entry go) || { skip "chain3 mid-go not staged"; return 0; }
  ep=$(entry rs) || { skip "chain3 prov-rs not staged"; return 0; }
  mkcfg chain3 "$ec" "$ep" "$em"; serve "$H/work/chain3.json" P; L=$(getlisten "$H/work/chain3.ready"); mkreq qq "$L"
  read PTOK PFENCE <<< "$(activate qq prov)" && wait_ready qq "$PTOK" "$PFENCE" || { fail "chain3-activate-prov"; return 0; }
  read MTOK MFENCE <<< "$(activate qq mid)" && wait_ready qq "$MTOK" "$MFENCE" || { fail "chain3-activate-mid"; return 0; }
  read CTOK CFENCE <<< "$(activate qq cons)" && wait_ready qq "$CTOK" "$CFENCE" || { fail "chain3-activate-cons"; return 0; }
  out=$(qq "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-chain3-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"chain\":true,\"input\":{\"hello\":\"world\"}}}}" 2>/dev/null) || { fail "chain3-invoke"; return 0; }
  n=$(echo "$out" | grep -o '"chained":[ ]*{' | wc -l)
  if [ "$n" -ge 3 ] && echo "$out" | grep -q '"via":[ ]*"prov"' && echo "$out" | grep -q '"hello":[ ]*"world"'; then ok "chain3 js->go->rs"; else fail "chain3 js->go->rs"; fi
  release qq "$CTOK" "$CFENCE" || fail "chain3-release-cons"; release qq "$MTOK" "$MFENCE" || fail "chain3-release-mid"; release qq "$PTOK" "$PFENCE" || fail "chain3-release-prov"
}
if [ "$FULL" -eq 1 ]; then
  for c in py js go cr ex cs c cpp rs; do for p in py js go cr ex cs c cpp rs; do run_pair "full-$c-$p" "$c" "$p"; done; done
  run_chain3
else
  run_pair smoke-rsrs rs rs
  run_pair pair-pyjs py js
  run_pair pair-jspy js py
  run_chain3
fi
for p in $PIDS; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; sleep 0.5
if pgrep -f "matrix-managed.*$H" >/dev/null 2>&1; then fail nxn-no-orphan-managed; else ok nxn-no-orphan-managed; fi
if pgrep -f "dep_node.*$H|mx-node.*$H" >/dev/null 2>&1; then fail nxn-no-orphan-node; else ok nxn-no-orphan-node; fi
if ss -xa 2>/dev/null | grep -q "$H"; then fail nxn-no-orphan-socks-live; else ok nxn-no-orphan-socks-live; fi
find "$H" -type s 2>/dev/null | while read -r s; do echo "stale-sock-removed $s"; rm -f "$s"; done
if grep -E 'BEGIN (RSA |EC |OPENSSH |)PRIVATE KEY' "$LOG" "$H"/work/*.ready "$H"/work/*.json >/dev/null 2>&1; then fail nxn-no-secrets; else ok nxn-no-secrets; fi
echo "nxn done: PASS=$PASS FAIL=$FAIL SKIP=$SKIP FULL=$FULL"; [ "$FAIL" -eq 0 ]
