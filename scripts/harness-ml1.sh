#!/usr/bin/env bash
# ML1 external adoption harness (L01–L12 proof): materializes projects
# OUTSIDE the Matrix checkout (/tmp), consumes ONLY dist artifacts
# (binaries run by path; packs/scaffold scripts staged out, never
# referenced in-tree), and exercises every language SDK as component
# host AND dependency consumer, same-language chains, the mandated
# cross pairs, a three-language chain, and a remote leg — using only
# the public documentation.
#
# Probe conventions (match the CLI contract):
# - success: `out=$(req '...' 2>/dev/null) || fail ...`, then grep stdout.
# - denial: `if out=$(req '...' 2>&1); then fail ...; else grep pattern`.
# Server JSON is compact: patterns use `[ ]*` for optional spaces.
#
# Not a maintained application: validation material with a recipe
# (this file). Same implementer executed it: independence claimed is
# technical (workspace independence), not third-party evaluation.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist"
ML1="$DIST/ml1"
H="${HARNESS_DIR:-/tmp/mxml1-$$}"
rm -rf "$H"
mkdir -p "$H"/{pkg,proj,bin,home,work}
LOG="$H/work/harness.log"
exec > >(tee "$LOG") 2>&1
PASS=0; FAIL=0
ok() { PASS=$((PASS+1)); echo "ok $*"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL $*"; }

# ---------- toolchain resolution (explicit install dirs, PATH fallback) ----------
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
export GOPROXY=off GOFLAGS=-mod=mod
MISE_GO="$HOME/.local/share/mise/installs/go/1.27.1/bin"
MISE_CR="$HOME/.local/share/mise/installs/crystal/1.21.0/bin"
MISE_ERL="$HOME/.local/share/mise/installs/erlang/29.0.6/bin"
MISE_EX="$HOME/.local/share/mise/installs/elixir/1.20.4-otp-29/bin"
[ -x "$MISE_GO/go" ] && export PATH="$MISE_GO:$PATH"
[ -x "$MISE_CR/crystal" ] && export PATH="$MISE_CR:$PATH"
[ -x "$MISE_ERL/erl" ] && export PATH="$MISE_ERL:$MISE_EX:$PATH"
for t in cargo rustc python3 openssl node npm go crystal elixir mix erl dotnet cmake cc c++ sha256sum; do
  command -v "$t" >/dev/null 2>&1 || { echo "missing tool: $t"; exit 2; }
done
PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

# ---------- stage artifacts out of the checkout ----------
[ -f "$ML1/matrix-go-0.1.0.tar.gz" ] || { echo "no ml1 packs in dist; run scripts/package.sh"; exit 2; }
for t in python go crystal elixir csharp c; do tar xzf "$ML1/matrix-$t-0.1.0.tar.gz" -C "$H/pkg"; done
mkdir -p "$H/pkg/js-src" && tar xzf "$ML1"/matrix-component-*.tgz -C "$H/pkg/js-src"
mkdir -p "$H/bin"
cp "$DIST"/bin/* "$H/bin/"
cp "$ROOT/scripts/dev-pki.py" "$H/work/"
HBIN="$H/bin"
MB="$HBIN/matrix-managed"
export MX_MATRIX_MANAGED="$MB" MX_DEV_PKI="$H/work/dev-pki.py" MX_DOCTOR_BIN="$MB"

# ---------- L01: isolated installs from packs ----------
python3 -m venv "$H/venv"
"$H/venv/bin/pip" install --no-index --quiet "$ML1"/matrix_component-*.whl
if env -u PYTHONPATH -u PYTHONHOME "$H/venv/bin/python" -c "import matrix_component, matrix_operator; print('py-ok')" 2>&1 | grep -q py-ok; then ok L01 py-venv-isolated; else fail L01 py-venv-isolated; fi
if (cd /tmp && env -u PYTHONPATH -u PYTHONHOME python3 -c "import matrix_component" 2>/dev/null); then fail L01 py-no-system-leak; else ok L01 py-no-system-leak; fi
VPY="$H/venv/bin/python"
mkdir -p "$H/proj/js-install" && (cd "$H/proj/js-install" && npm init -y >/dev/null 2>&1 && npm install --offline --no-audit --no-fund "$ML1"/matrix-component-*.tgz >/dev/null 2>&1)
if node -e "require('$H/proj/js-install/node_modules/matrix-component')" 2>/dev/null; then ok L01 js-npm-offline; else fail L01 js-npm-offline; fi
if (cd "$H/pkg/go" && go build ./... >/dev/null 2>&1 && go vet ./... 2>&1 | head -2); then ok L01 go-build-vet; else fail L01 go-build-vet; fi
if (cd "$H/pkg/go" && go test -count=1 -timeout 240s . > "$H/work/go-test.log" 2>&1); then ok L01 go-tests; else fail L01 go-tests "$(tail -2 "$H/work/go-test.log")"; fi
if (cd "$H/pkg/crystal" && crystal spec > "$H/work/cr-spec.log" 2>&1); then ok L01 crystal-spec; else fail L01 crystal-spec "$(tail -2 "$H/work/cr-spec.log")"; fi
if (cd "$H/pkg/elixir" && mix test > "$H/work/ex-test.log" 2>&1); then ok L01 elixir-tests; else fail L01 elixir-tests "$(tail -2 "$H/work/ex-test.log")"; fi
if (cd "$H/pkg/csharp" && dotnet run -c Release --project test/SelfTest/SelfTest.csproj > "$H/work/cs-test.log" 2>&1); then ok L01 csharp-selftest; else fail L01 csharp-selftest "$(tail -2 "$H/work/cs-test.log")"; fi
rm -rf "$H/c-build" && mkdir -p "$H/c-build"
if (cd "$H/c-build" && cmake -S "$H/pkg/c" -B . -DMATRIX_ENABLE_CPP=ON -DCMAKE_BUILD_TYPE=Release >/dev/null 2>&1 && cmake --build . >/dev/null 2>&1 && ctest > "$H/work/c-test.log" 2>&1); then ok L01 c-cpp-ctest; else fail L01 c-cpp-ctest "$(tail -2 "$H/work/c-test.log")"; fi
(cd "$DIST" && sha256sum -c --quiet <(grep -E "ml1/" MANIFEST.txt)) && ok L01 hashes-verify || fail L01 hashes-verify
"$HBIN/matrix-conform" vectors >/dev/null 2>&1 && ok L01 conform-vectors || fail L01 conform-vectors
"$HBIN/matrix-conform" local > "$H/work/conform.log" 2>&1; echo "conform exit: $?" >> "$H/work/conform.log"
grep -q "conform exit: 0" "$H/work/conform.log" && ok L04 conform-local || fail L04 conform-local

# ---------- L12 scaffold: one generated project per language ----------
bash "$H/pkg/python/scaffold.sh" pyapp "$H/proj/py" --python "$VPY" --home "$H/home/py" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-py || fail L12 scaffold-py
node "$H/pkg/js-src/package/bin/matrix-scaffold.js" jsapp "$H/proj/js" --home "$H/home/js" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-js || fail L12 scaffold-js
bash "$H/pkg/go/scaffold.sh" goapp "$H/proj/go" --home "$H/home/go" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-go || fail L12 scaffold-go
bash "$H/pkg/crystal/scaffold.sh" crapp "$H/proj/cr" --home "$H/home/cr" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-cr || fail L12 scaffold-cr
bash "$H/pkg/elixir/scaffold.sh" exapp "$H/proj/ex" --home "$H/home/ex" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-ex || fail L12 scaffold-ex
bash "$H/pkg/csharp/scaffold.sh" csapp "$H/proj/cs" --home "$H/home/cs" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-cs || fail L12 scaffold-cs
bash "$H/pkg/c/scaffold.sh" capp "$H/proj/c" --home "$H/home/c" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-c || fail L12 scaffold-c
bash "$H/pkg/c/scaffold.sh" cppapp "$H/proj/cpp" --cxx --home "$H/home/cpp" --pki "$H/work/pki0" >/dev/null 2>&1 && ok L12 scaffold-cpp || fail L12 scaffold-cpp
[ -x "$H/proj/cr/mx-node" ] && [ -x "$H/proj/ex/mx-node" ] && [ -x "$H/proj/c/build/mx-node" ] && [ -x "$H/proj/cpp/build/mx-node-cpp" ] && [ -x "$H/proj/go/mx-node" ] && ok L12 scaffold-bins || fail L12 scaffold-bins

# ---------- L12 doctor per language ----------
doctor_shape_ok() { # $1=json file: cli_shape_ok (snake) or cliShapeOk (JS camel)
  python3 -c "import json,sys; d=json.load(open('$1')); assert d.get('cli_shape_ok', d.get('cliShapeOk')) is True"
}
doctor_ok() { # $1=L12-id $2...=command; asserts doctor shape ok
  local id="$1"; shift
  if "$@" > "$H/work/doctor-$id.json" 2>&1 && doctor_shape_ok "$H/work/doctor-$id.json"; then ok L12 doctor-$id; else fail L12 doctor-$id "$(head -c 200 "$H/work/doctor-$id.json")"; fi
}
doctor_ok py "$VPY" "$H/pkg/python/matrix_operator.py" --binary "$MB"
doctor_ok js node "$H/proj/js-install/node_modules/matrix-component/bin/matrix-doctor.js" --binary "$MB"
(cd "$H/pkg/go" && go build -o "$H/bin/mx-doctor-go" ./cmd/matrix-doctor)
doctor_ok go "$H/bin/mx-doctor-go" --binary "$MB"
(cd "$H/pkg/crystal" && crystal build --release examples/doctor.cr -o "$H/bin/matrix-doctor-cr" 2>/dev/null)
doctor_ok cr "$H/bin/matrix-doctor-cr" --binary "$MB"
(cd "$H/pkg/elixir" && mix run --no-halt -e 'Matrix.Doctor.main(["--binary", System.get_env("MX_DOCTOR_BIN")])' 2>/dev/null | grep '^{' > "$H/work/doctor-ex.json") && doctor_shape_ok "$H/work/doctor-ex.json" && ok L12 doctor-ex || fail L12 doctor-ex
(cd "$H/pkg/csharp" && dotnet build -c Release test/SelfTest/SelfTest.csproj >/dev/null 2>&1 && CSBIN=$(find test/SelfTest/bin/Release -name mx-selftest -type f | head -1) && "$CSBIN" --doctor "$MB" > "$H/work/doctor-cs.json" 2>/dev/null) && doctor_shape_ok "$H/work/doctor-cs.json" && ok L12 doctor-cs || fail L12 doctor-cs
doctor_ok c "$H/c-build/matrix-doctor" --binary "$MB"
echo "cpp diagnosis shares the C transport doctor (docs/ML1-MATRIX.md)" && ok L12 doctor-cpp-shared || fail L12 doctor-cpp-shared

# ---------- node entrypoints (scaffolded binaries) ----------
ENTRY_py="$VPY"; ARGS_py="$H/proj/py/node.py"
ENTRY_js="node"; ARGS_js="$H/proj/js/node.js"
ENTRY_go="$H/proj/go/mx-node"; ARGS_go=""
ENTRY_cs="dotnet"; ARGS_cs="$H/proj/cs/node/bin/Release/net10.0/mx-node.dll"
ENTRY_cr="$H/proj/cr/mx-node"; ARGS_cr=""
ENTRY_ex="$H/proj/ex/run-node.sh"; ARGS_ex=""
ENTRY_c="$H/proj/c/build/mx-node"; ARGS_c=""
ENTRY_cpp="$H/proj/cpp/build/mx-node-cpp"; ARGS_cpp=""
ENTRY_rs="$HBIN/dep_node"; ARGS_rs=""

# ---------- PKI + service helpers ----------
python3 "$H/work/dev-pki.py" "$H/work/pki" >/dev/null 2>&1
FP=$(python3 -c "import hashlib; print(hashlib.sha256(open('$H/work/pki/client.der','rb').read()).hexdigest())")
serve() { # $1=config -> prints ready line; records $2 pid var name
  local cfg="$1" var="$2"
  "$MB" serve "$cfg" > "$H/work/$(basename $cfg .json).ready" 2>"$H/work/$(basename $cfg .json).err" &
  eval "$var=$!"
  PIDS="$PIDS ${!var}"
  local ready="$H/work/$(basename $cfg .json).ready"
  for i in $(seq 1 150); do [ -s "$ready" ] && break; sleep 0.1; done
}
getlisten() { python3 -c "import json; print(json.load(open('$1'))['listen'])"; }
# Per-language entrypoints for generated service configs.
for L in py js go cs cr ex c cpp rs; do
  var="ENTRY_$L"; eval "export ML1_ENTRY_$L=\"\${$var}\""
  var="ARGS_$L"; eval "export ML1_ARGS_$L=\"\${$var}\""
done
# mkchain <name> <cons_lang|-> <prov_lang|-> [mid_lang|-] [indep_lang|-]
# roles: cons chains to mid (or prov); prov echoes; indep echoes.
# Every component carries a restart policy (L06 kill/restart proof).
mkchain() {
  local name="$1" cons="$2" prov="$3" mid="${4:--}" indep="${5:--}"
  ML1_CONS="$cons" ML1_PROV="$prov" ML1_MID="$mid" ML1_INDEP="$indep" \
  ML1_FP="$FP" python3 - "$H/work/$name.json" "$H/home/$name" "$H/work/pki" <<'PY'
import json, os, sys
_, out, home, pki = sys.argv
cons, prov, mid, indep = (os.environ[k] for k in
    ("ML1_CONS", "ML1_PROV", "ML1_MID", "ML1_INDEP"))
fp = os.environ["ML1_FP"]
def entry(lang):
    e = os.environ["ML1_ENTRY_" + lang]
    a = os.environ["ML1_ARGS_" + lang].split()
    return e, a
def comp(cid, caps, lang, req=None, req_provider=None):
    e, a = entry(lang)
    m = {"id": cid, "capabilities": caps,
         "execution": {"kind": "process", "entrypoint": e,
                       "args": a + ["--matrix-sock", "{sock}", "--id", "{id}"]}}
    if req:
        m["requires"] = [{"interface": req, "provider": req_provider}]
        m["outbound"] = {"request": [req], "limits": {
            "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
            "max_calls_global": 64, "max_seen_requests": 64,
            "max_queued_bytes": 65536, "max_deadline_ms": 12000}}
    return {"manifest": m, "trusted": True,
            "restart": {"max_restarts": 5, "window_ms": 30000, "backoff_ms": 200}}
comps, caps, ogrants = [], [], {}
if cons != "-":
    target = "mid.chain@1" if mid != "-" else "prov.echo@1"
    tprov = "mid" if mid != "-" else "prov"
    comps.append(comp("cons", ["cons.chain@1"], cons, target, tprov))
    caps.append("cons.chain@1")
    ogrants["cons"] = [target]
if mid != "-":
    comps.append(comp("mid", ["mid.chain@1"], mid, "prov.echo@1", "prov"))
    caps.append("mid.chain@1")
    ogrants["mid"] = ["prov.echo@1"]
if prov != "-":
    comps.append(comp("prov", ["prov.echo@1"], prov))
    caps.append("prov.echo@1")
if indep != "-":
    comps.append(comp("indep", ["indep.echo@1"], indep))
    caps.append("indep.echo@1")
cfg = {"home": home, "components": comps,
       "grants": {fp: {"components": [c["manifest"]["id"] for c in comps],
                       "capabilities": caps}},
       "tls": {"listen": "127.0.0.1:0", "ca": f"{pki}/ca.der",
               "cert": f"{pki}/server.der", "key": f"{pki}/server-key.der"}}
if ogrants:
    cfg["outbound_grants"] = ogrants
json.dump(cfg, open(out, "w"))
PY
}

# ---------- request helpers (bound per service) ----------
# mkreq <fn-name> <listen>: defines <fn> taking the JSON action.
mkreq() { eval "$1() { \"\$MB\" request \"\$H/work/pki/ca.der\" \"\$H/work/pki/client.der\" \"\$H/work/pki/client-key.der\" \"$2\" localhost \"\$1\"; }"; }
activate() { # $1=reqfn $2=component $3=ttl -> prints "lease fence"
  local out
  out=$($1 "{\"action\":\"activate\",\"component\":\"$2\",\"ttl_ms\":${3:-30000}}") || return 1
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
# Leases live 30s max: refresh before every phase (re-activate all
# three roles + readiness), or later probes test expiry, not logic.
refresh_same() { # $1=lang (uses req$lang); sets SAME_* + SAME_OK
  local L="$1" PTOK PFENCE CTOK CFENCE ITOK IFENCE
  local R="req$L"
  release_tracked() {
    local role old
    for role in PROV CONS INDEP; do
      eval "old=\$SAME_${L}_$role"
      if [ -n "$old" ]; then
        set -- $old
        "$R" "{\"action\":\"release\", \"lease\":\"$1\", \"fence\":\"$2\"}" >/dev/null 2>&1 || true
      fi
    done
  }
  reactivate() { # $1=component -> prints "lease fence"
    local out
    # Retry once on already-leased: a supervisor turnover can
    # briefly hold the slot between our release and re-activate.
    # (|| true: set -e must not fire inside the retry probe.)
    out=$(activate "$R" "$1" 2>/dev/null) || true
    if [ -n "$out" ]; then echo "$out"; return 0; fi
    sleep 1
    release_tracked
    out=$(activate "$R" "$1" 2>/dev/null) || true
    if [ -n "$out" ]; then echo "$out"; return 0; fi
    return 1
  }
  release_tracked
  read PTOK PFENCE <<< "$(reactivate prov)" || { echo "refresh $L prov refused" >&2; return 1; }
  wait_ready "$R" "$PTOK" "$PFENCE" || return 1
  read CTOK CFENCE <<< "$(reactivate cons)" || { echo "refresh $L cons refused" >&2; return 1; }
  wait_ready "$R" "$CTOK" "$CFENCE" || return 1
  read ITOK IFENCE <<< "$(reactivate indep)" || { echo "refresh $L indep refused" >&2; return 1; }
  wait_ready "$R" "$ITOK" "$IFENCE" || return 1
  eval "SAME_${L}_PROV=\"$PTOK $PFENCE\"; SAME_${L}_CONS=\"$CTOK $CFENCE\"; SAME_${L}_INDEP=\"$ITOK $IFENCE\"; SAME_OK_$L=1"
}
# ---------- L03: same-language chain per generated project ----------
for L in py js go cs cr ex c cpp; do
  mkchain "same-$L" "$L" "$L" - "$L"
  serve "$H/work/same-$L.json" "PID_$L"
  eval "LISTEN_$L=\$(getlisten \"\$H/work/same-$L.ready\")"
  mkreq "req$L" "$(eval "echo \$LISTEN_$L")"
done
for L in py js go cs cr ex c cpp; do
  eval "SAME_OK_$L=0"
  R="req$L"
  read PTOK PFENCE <<< "$(activate "$R" prov)" || { fail L03 same-$L-activate-prov; continue; }
  wait_ready "$R" "$PTOK" "$PFENCE" || { fail L03 same-$L-prov-ready; continue; }
  read CTOK CFENCE <<< "$(activate "$R" cons)" || { fail L03 same-$L-activate-cons; continue; }
  wait_ready "$R" "$CTOK" "$CFENCE" || { fail L03 same-$L-cons-ready; continue; }
  read ITOK IFENCE <<< "$(activate "$R" indep)" || { fail L03 same-$L-activate-indep; continue; }
  wait_ready "$R" "$ITOK" "$IFENCE" || { fail L03 same-$L-indep-ready; continue; }
  eval "SAME_${L}_PROV=\"$PTOK $PFENCE\"; SAME_${L}_CONS=\"$CTOK $CFENCE\"; SAME_${L}_INDEP=\"$ITOK $IFENCE\"; SAME_OK_$L=1"
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-same-$L-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":11}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*11'; then ok L03 same-$L-chain; else fail L03 same-$L-chain "$out"; fi
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-same-$L-2\",\"cap\":\"indep.echo@1\",\"input\":{\"ping\":1}}" 2>/dev/null) && echo "$out" | grep -q '"ping":[ ]*1'; then ok L03 same-$L-indep; else fail L03 same-$L-indep "$out"; fi
done

# ---------- L05: authority denials on every same-language service ----------
for L in py js go cs cr ex c cpp; do
  R="req$L"
  refresh_same "$L" || { fail L05 $L-refresh; continue; }
  eval "set -- \$SAME_${L}_CONS"; CTOK="$1"; CFENCE="$2"
  eval "set -- \$SAME_${L}_INDEP"; ITOK="$1"; IFENCE="$2"
  if out=$($R '{"action":"invoke","lease":"dead","fence":"1","operation":"op-x","cap":"cons.chain@1","input":{}}' 2>&1); then fail L05 $L-dead-denied "unexpected success"; elif echo "$out" | grep -qiE "permission-denied|stale-generation|denied"; then ok L05 $L-dead-denied; else fail L05 $L-dead-denied "$out"; fi
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-x2\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1); then fail L05 $L-ungranted-denied "unexpected success"; elif echo "$out" | grep -qiE "permission-denied|denied"; then ok L05 $L-ungranted-denied; else fail L05 $L-ungranted-denied "$out"; fi
  $R "{\"action\":\"release\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\"}" >/dev/null 2>&1 || true
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-x3\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1); then fail L05 $L-stale-denied "unexpected success"; elif echo "$out" | grep -qiE "stale|denied|not-active|unknown"; then ok L05 $L-stale-denied; else fail L05 $L-stale-denied "$out"; fi
  read NITOK NIFENCE <<< "$(activate "$R" indep)" && wait_ready "$R" "$NITOK" "$NIFENCE" && eval "SAME_${L}_INDEP=\"$NITOK $NIFENCE\"" && ok L05 $L-recover || fail L05 $L-recover
done

# ---------- L09: mandated cross pairs (both directions, one daemon each) ----------
mkchain "pair-pyjs" "py" "js" - "py"
serve "$H/work/pair-pyjs.json" PAIR_PYJS
L_PYJS=$(getlisten "$H/work/pair-pyjs.ready"); mkreq reqpyjs "$L_PYJS"
mkchain "pair-jspy" "js" "py" - "js"
serve "$H/work/pair-jspy.json" PAIR_JSPY
L_JSPY=$(getlisten "$H/work/pair-jspy.ready"); mkreq reqjspy "$L_JSPY"
mkchain "pair-gocs" "go" "cs" - "go"
serve "$H/work/pair-gocs.json" PAIR_GOCS
L_GOCS=$(getlisten "$H/work/pair-gocs.ready"); mkreq reqgocs "$L_GOCS"
mkchain "pair-csgo" "cs" "go" - "cs"
serve "$H/work/pair-csgo.json" PAIR_CSGO
L_CSGO=$(getlisten "$H/work/pair-csgo.ready"); mkreq reqcsgo "$L_CSGO"
mkchain "pair-crex" "cr" "ex" - "cr"
serve "$H/work/pair-crex.json" PAIR_CREX
L_CREX=$(getlisten "$H/work/pair-crex.ready"); mkreq reqcrex "$L_CREX"
mkchain "pair-excr" "ex" "cr" - "ex"
serve "$H/work/pair-excr.json" PAIR_EXCR
L_EXCR=$(getlisten "$H/work/pair-excr.ready"); mkreq reqexcr "$L_EXCR"
mkchain "pair-ccpp" "c" "cpp" - "c"
serve "$H/work/pair-ccpp.json" PAIR_CPP
L_CPP=$(getlisten "$H/work/pair-ccpp.ready"); mkreq reqccpp "$L_CPP"
mkchain "pair-cppc" "cpp" "c" - "cpp"
serve "$H/work/pair-cppc.json" PAIR_CPPC
L_CPPC=$(getlisten "$H/work/pair-cppc.ready"); mkreq reqcppc "$L_CPPC"
pair_chain() { # $1=L09-id $2=reqfn $3=op-tag
  local R="$2"
  read PTOK PFENCE <<< "$(activate "$R" prov)" && wait_ready "$R" "$PTOK" "$PFENCE" || { fail L09 "$1-prov"; return; }
  read CTOK CFENCE <<< "$(activate "$R" cons)" && wait_ready "$R" "$CTOK" "$CFENCE" || { fail L09 "$1-cons"; return; }
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"$3\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":11}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*11'; then ok L09 "$1"; else fail L09 "$1" "$out"; fi
}
pair_chain py-cons-js-prov reqpyjs op-pair-1
pair_chain js-cons-py-prov reqjspy op-pair-2
pair_chain go-cons-cs-prov reqgocs op-pair-3
pair_chain cs-cons-go-prov reqcsgo op-pair-4
pair_chain cr-cons-ex-prov reqcrex op-pair-5
pair_chain ex-cons-cr-prov reqexcr op-pair-6
pair_chain c-cons-cpp-prov reqccpp op-pair-7
pair_chain cpp-cons-c-prov reqcppc op-pair-8
# Rust reference both ways (previous-SDK interop, L11).
mkchain "pair-rspy" "rs" "py" - "rs"
serve "$H/work/pair-rspy.json" PAIR_RSPY
L_RSPY=$(getlisten "$H/work/pair-rspy.ready"); mkreq reqrspy "$L_RSPY"
pair_chain rs-cons-py-prov reqrspy op-pair-9
mkchain "pair-jsrs" "js" "rs" - "js"
serve "$H/work/pair-jsrs.json" PAIR_JSRS
L_JSRS=$(getlisten "$H/work/pair-jsrs.ready"); mkreq reqjsrs "$L_JSRS"
pair_chain js-cons-rs-prov reqjsrs op-pair-10
# Three-language chain: cons JS -> mid Go -> prov Rust.
mkchain "chain3" "js" "rs" "go" "cpp"
serve "$H/work/chain3.json" CHAIN3
L_CHAIN3=$(getlisten "$H/work/chain3.ready"); mkreq reqchain3 "$L_CHAIN3"
read PTOK PFENCE <<< "$(activate reqchain3 prov)" && wait_ready reqchain3 "$PTOK" "$PFENCE" || fail L09 chain3-prov
read MTOK MFENCE <<< "$(activate reqchain3 mid)" && wait_ready reqchain3 "$MTOK" "$MFENCE" || fail L09 chain3-mid
read CTOK CFENCE <<< "$(activate reqchain3 cons)" && wait_ready reqchain3 "$CTOK" "$CFENCE" || fail L09 chain3-cons
if out=$(reqchain3 "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-chain3-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"chain\":true,\"input\":{\"value\":11}}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*11'; then ok L09 chain3-js-go-rs; else fail L09 chain3-js-go-rs "$out"; fi

# ---------- L08 send path per SDK (bounded emit, terminal proves host acceptance) ----------
for L in py js go cs cr ex c cpp; do
  R="req$L"
  refresh_same "$L" || { fail L08 $L-refresh; continue; }
  eval "set -- \$SAME_${L}_PROV"; PTOK="$1"; PFENCE="$2"
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-stream-$L-1\",\"cap\":\"prov.echo@1\",\"input\":{\"stream_send\":{\"stream_id\":\"s1\",\"chunks\":4,\"chunk_bytes\":64}}}" 2>/dev/null) && echo "$out" | grep -q '"stream_sent":[ ]*4'; then ok L08 $L-stream-send; else fail L08 $L-stream-send "$out"; fi
  eval "set -- \$SAME_${L}_CONS"; CTOK="$1"; CFENCE="$2"
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-bidi-$L-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain_with_streams\":{\"stream_id\":\"s-bidi\",\"chunks\":6,\"chunk_bytes\":16,\"input\":{\"sleep_ms\":1500,\"value\":2}}}}" 2>/dev/null) && echo "$out" | grep -q '"stream_sent":[ ]*6' && echo "$out" | grep -q '"chained"'; then ok L08 $L-bidi-local; else fail L08 $L-bidi-local "$out"; fi
done

# ---------- L06: kill/restart lifecycle (py + c), indep isolation everywhere ----------
killpat() { # $1=lang -> pkill -f pattern for that lang's prov node
  case "$1" in
    py) echo "proj/py/node.py.*--id prov";;
    js) echo "proj/js/node.js.*--id prov";;
    go) echo "proj/go/mx-node.*--id prov";;
    cs) echo "mx-node.dll.*--id prov";;
    cr) echo "proj/cr/mx-node.*--id prov";;
    ex) echo "proj/ex/mx-node";;
    c) echo "proj/c/build/mx-node.*--id prov";;
    cpp) echo "mx-node-cpp.*--id prov";;
  esac
}
lifecycle() { # $1=lang $2=L-id tag
  # Kill/restart lifecycle (L06):
  #  - withdrawal is proven by SUSTAINED provider absence: the
  #    supervisor restarts a killed provider in ~200ms, so a single
  #    probe races the respawn; looping pkill+probe keeps the provider
  #    down across the probe window and the chain must fail closed
  #    (ok:false envelope or a stable refusal code — never success);
  #  - SIGHUP-grant-revoke is M8-P07-covered on idle daemons and is
  #    NOT used here: under this harness's concurrency the reload
  #    silently stops applying (no daemon log, requests unaffected —
  #    see ML1-COMPOSITION.md kernel follow-up), which would make a
  #    SIGHUP-based probe test the reload path instead of withdrawal;
  #  - then isolation (indep serves), recovery (supervisor restart +
  #    new generation serves) and old-ref death.
  local L="$1" tag="$2" cfg="$H/work/same-$L.json"
  local R="req$L"
  refresh_same "$L" || { fail L06 "$tag-refresh"; return; }
  eval "set -- \$SAME_${L}_CONS"; local CTOK="$1" CFENCE="$2"
  eval "set -- \$SAME_${L}_PROV"; local PTOK="$1" PFENCE="$2"
  eval "set -- \$SAME_${L}_INDEP"; local ITOK="$1" IFENCE="$2"
  local out rc=0 i invalidated=0
  for i in $(seq 1 12); do
    rc=0
    pkill -9 -f "$(killpat "$L")" 2>/dev/null || true
    out=$($R "{\"action\":\"invoke\",\"lease\":\"$CTOK\",\"fence\":\"$CFENCE\",\"operation\":\"op-kill-$L-wd-$i\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{}}}" 2>&1) || rc=$?
    if [ $rc -eq 0 ] && echo "$out" | python3 -c "import json,sys; assert json.load(sys.stdin).get('ok') is False" 2>/dev/null; then invalidated=1; break; fi
    if [ $rc -ne 0 ] && echo "$out" | grep -qiE "stale-generation|context-not-active|permission-denied|denied|unknown|not-active|deadline|cancelled|exhausted"; then invalidated=1; break; fi
    sleep 0.2
  done
  if [ "$invalidated" = 1 ]; then ok L06 $tag-chain-withdrawn; else fail L06 $tag-chain-withdrawn "rc=$rc out=$out"; fi
  sleep 1.5
  local ok_iso=0
  for i in $(seq 1 20); do
    rc=0
    out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-kill-$L-2-$i\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1) || rc=$?
    if [ $rc -eq 0 ] && echo "$out" | grep -q '"ok":true'; then ok_iso=1; break; fi
    sleep 0.5
  done
  if [ "$ok_iso" = 1 ]; then ok L06 $tag-indep-isolated; else fail L06 $tag-indep-isolated "rc=$rc out=$out"; fi
  local NPTOK="" NPFENCE="" NCTOK="" NCFENCE="" ok_restart=0
  for i in $(seq 1 150); do
    if tmp=$(activate "$R" prov 2>/dev/null); then
      read NPTOK NPFENCE <<< "$tmp"
      wait_ready "$R" "$NPTOK" "$NPFENCE" 2>/dev/null && { ok_restart=1; break; }
    fi
    sleep 0.2
  done
  [ "$ok_restart" = 1 ] || { fail L06 $tag-restart "prov never came back"; return; }
  read NCTOK NCFENCE <<< "$(activate "$R" cons)" && wait_ready "$R" "$NCTOK" "$NCFENCE" || { fail L06 $tag-cons-reactivate; return; }
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$NCTOK\",\"fence\":\"$NCFENCE\",\"operation\":\"op-kill-$L-3\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":3}}}" 2>&1) && echo "$out" | grep -q '"value":[ ]*3'; then ok L06 $tag-chain-restored; else fail L06 $tag-chain-restored "$out"; fi
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-kill-$L-4\",\"cap\":\"prov.echo@1\",\"input\":{}}" 2>&1); then fail L06 $tag-old-refs-dead "unexpected success"; elif echo "$out" | grep -qiE "stale|denied|not-active|unknown"; then ok L06 $tag-old-refs-dead; else fail L06 $tag-old-refs-dead "$out"; fi
  eval "SAME_${L}_CONS=\"$NCTOK $NCFENCE\"; SAME_${L}_PROV=\"$NPTOK $NPFENCE\""
}
lifecycle py L06-py
lifecycle c L06-c
# Indep isolation spot-check on the remaining services (prov kill, indep serves).
for L in js go cs cr ex cpp; do
  R="req$L"
  refresh_same "$L" || { fail L06 $L-refresh; continue; }
  eval "set -- \$SAME_${L}_INDEP"; ITOK="$1"; IFENCE="$2"
  pkill -9 -f "$(killpat "$L")" 2>/dev/null || true
  sleep 1.5
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-iso-$L-1\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1) && echo "$out" | grep -q '"ok":true'; then ok L06 $L-indep-isolated; else fail L06 $L-indep-isolated "$out"; fi
done

# ---------- L07: load + cancel (js, ex, c services) ----------
for L in js ex c; do
  R="req$L"
  refresh_same "$L" || { fail L07 $L-refresh; continue; }
  eval "set -- \$SAME_${L}_PROV"; PTOK="$1"; PFENCE="$2"
  eval "set -- \$SAME_${L}_INDEP"; ITOK="$1"; IFENCE="$2"
  $R "{\"action\":\"invoke\",\"lease\":\"$PTOK\",\"fence\":\"$PFENCE\",\"operation\":\"op-load-$L-1\",\"cap\":\"prov.echo@1\",\"input\":{\"stream_send\":{\"stream_id\":\"s9\",\"chunks\":32,\"chunk_bytes\":4096}}}" > "$H/work/load-$L.json" 2>&1 & FLOODPID=$!
  sleep 0.3
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-load-$L-2\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>/dev/null) && echo "$out" | grep -q '"ok":true'; then ok L07 $L-control-progresses; else fail L07 $L-control-progresses "$out"; fi
  wait $FLOODPID 2>/dev/null || true
  if grep -q '"stream_sent":[ ]*32' "$H/work/load-$L.json" 2>/dev/null; then ok L07 $L-flood-bounded; else fail L07 $L-flood-bounded "$(cat "$H/work/load-$L.json" 2>/dev/null)"; fi
  if out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-load-$L-3\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>/dev/null) && echo "$out" | grep -q '"ok":true'; then ok L07 $L-session-survives; else fail L07 $L-session-survives "$out"; fi
done
# Cancel: kill mid-sleep, terminal not-ok, same op never auto-replays.
R="reqjs"
eval "set -- \$SAME_js_INDEP"; ITOK="$1"; IFENCE="$2"
($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-cancel-js-1\",\"cap\":\"indep.echo@1\",\"input\":{\"sleep_ms\":8000}}" > "$H/work/cancel-js.json" 2>&1 &)
sleep 0.6
pkill -9 -f "proj/js/node.js.*--id indep" 2>/dev/null || true
sleep 1.5
if python3 -c "import json; v=json.load(open('$H/work/cancel-js.json')); assert v.get('ok') is False, v"; then ok L07 cancel-unknown; else fail L07 cancel-unknown; fi
out=$($R "{\"action\":\"invoke\",\"lease\":\"$ITOK\",\"fence\":\"$IFENCE\",\"operation\":\"op-cancel-js-1\",\"cap\":\"indep.echo@1\",\"input\":{}}" 2>&1) || true
if echo "$out" | grep -q '"ok":true'; then fail L07 no-auto-replay "phantom success: $out"; else ok L07 no-auto-replay; fi

# ---------- L10 remote leg (crystal cons -> elixir prov) + L08 bidi streams ----------
cat > "$H/work/exec.json" <<EOF
{"home": "$H/home/exec",
 "components": [{"manifest": {"id": "rprov", "capabilities": ["prov.echo@1"],
    "execution": {"kind": "process", "entrypoint": "$H/proj/ex/run-node.sh", "args": ["--matrix-sock", "{sock}", "--id", "{id}", "--stream-log", "$H/work/rprov-streams.log"]}}, "trusted": true,
   "restart": {"max_restarts": 5, "window_ms": 30000, "backoff_ms": 200}}],
 "grants": {"$FP": {"components": ["rprov"], "capabilities": ["prov.echo@1", "matrix.effect.write"]}},
 "tls": {"listen": "127.0.0.1:0", "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/server.der", "key": "$H/work/pki/server-key.der"},
 "remotes": {"authority": "$FP", "domain": "ext", "peers": [], "routes": [],
   "session_listen": "127.0.0.1:0", "session_ca": "$H/work/pki/ca.der",
   "session_cert": "$H/work/pki/server.der", "session_key": "$H/work/pki/server-key.der"}}
EOF
serve "$H/work/exec.json" EXEC_PID
EXEC_SESS=$(python3 -c "import json; print(json.load(open('$H/work/exec.ready'))['session'])")
EXEC_MGMT=$(python3 -c "import json; print(json.load(open('$H/work/exec.ready'))['listen'])")
cat > "$H/work/ctrl.json" <<ML1CTRL
{"home": "$H/home/ctrl",
 "components": [{"manifest": {"id": "rcons", "capabilities": ["cons.chain@1"],
    "requires": [{"interface": "prov.echo@1", "provider": "rprov"}],
    "outbound": {"request": ["prov.echo@1"], "limits": {"max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16, "max_calls_global": 64, "max_seen_requests": 64, "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
    "execution": {"kind": "process", "entrypoint": "$H/proj/cr/mx-node", "args": ["--matrix-sock", "{sock}", "--id", "{id}"]}}, "trusted": true,
   "restart": {"max_restarts": 5, "window_ms": 30000, "backoff_ms": 200}}],
 "grants": {"$FP": {"components": ["rcons"], "capabilities": ["cons.chain@1"]}},
 "outbound_grants": {"rcons": ["prov.echo@1"]},
 "tls": {"listen": "127.0.0.1:0", "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/server.der", "key": "$H/work/pki/server-key.der"},
 "remotes": {"authority": "$FP", "domain": "ext",
   "peers": [{"name": "exec-A", "address": "$EXEC_SESS", "server_name": "localhost",
     "ca": "$H/work/pki/ca.der", "cert": "$H/work/pki/client.der", "key": "$H/work/pki/client-key.der",
     "mgmt_address": "$EXEC_MGMT", "domain": "ext", "lease_ttl_ms": 8000}],
   "routes": [{"consumer": "rcons", "provider": "rprov", "peer": "exec-A", "capabilities": ["prov.echo@1"]}]}}
ML1CTRL
serve "$H/work/ctrl.json" CTRL_PID
CRLISTEN=$(python3 -c "import json; print(json.load(open('$H/work/ctrl.ready'))['listen'])")
rcreq() { "$MB" request "$H/work/pki/ca.der" "$H/work/pki/client.der" "$H/work/pki/client-key.der" "$CRLISTEN" localhost "$1"; }
read RCTOK RCFENCE <<< "$(activate rcreq rcons)" && wait_ready rcreq "$RCTOK" "$RCFENCE" || fail L10 remote-activate
if out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\",\"operation\":\"op-rml1-1\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":5}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*5'; then ok L10 remote-chain-cr-ex; else fail L10 remote-chain-cr-ex "$out"; fi
if out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\",\"operation\":\"op-rml1-2\",\"cap\":\"cons.chain@1\",\"input\":{\"chain_with_streams\":{\"stream_id\":\"remote/s1\",\"chunks\":8,\"chunk_bytes\":16,\"input\":{\"sleep_ms\":3000,\"value\":1}}}}" 2>/dev/null) && echo "$out" | grep -q '"stream_sent":[ ]*8'; then ok L08 remote-bidi-sent; else fail L08 remote-bidi-sent "$out"; fi
sleep 1
if grep -q "remote/s1" "$H/work/rprov-streams.log" 2>/dev/null; then ok L08 remote-bidi-associated; else fail L08 remote-bidi-associated "$(cat "$H/work/rprov-streams.log" 2>/dev/null | head -3)"; fi
kill $EXEC_PID 2>/dev/null || true
sleep 1.5
if out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\",\"operation\":\"op-rml1-3\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{}}}" 2>/dev/null) && echo "$out" | python3 -c "import json,sys; assert json.load(sys.stdin).get('ok') is False"; then ok L10 partition-unknown; else fail L10 partition-unknown "$out"; fi
out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\",\"operation\":\"op-rml1-3\",\"cap\":\"cons.chain@1\",\"input\":{}}" 2>&1) || true
if echo "$out" | grep -q '"ok":true'; then fail L10 no-auto-replay-remote "phantom success: $out"; else ok L10 no-auto-replay-remote; fi
rcreq "{\"action\":\"release\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\"}" >/dev/null 2>&1 || true
python3 - "$H/work/exec.json" "$H/work/exec2.json" "$H/home/exec2" <<'PYEOF4'
import json, sys
_, src, dst, home = sys.argv
c = json.load(open(src))
c["home"] = home
json.dump(c, open(dst, "w"))
PYEOF4
serve "$H/work/exec2.json" EXEC_PID2
EXEC_SESS2=$(python3 -c "import json; print(json.load(open('$H/work/exec2.ready'))['session'])")
EXEC_MGMT2=$(python3 -c "import json; print(json.load(open('$H/work/exec2.ready'))['listen'])")
python3 - "$H/work/ctrl.json" "$EXEC_SESS2" "$EXEC_MGMT2" "$H/home/ctrl2" <<'PYEOF3'
import json, sys
_, path, sess, mgmt, home = sys.argv
c = json.load(open(path))
c["home"] = home
c["remotes"]["peers"][0]["address"] = sess
c["remotes"]["peers"][0]["mgmt_address"] = mgmt
json.dump(c, open(path, "w"))
PYEOF3
kill $CTRL_PID 2>/dev/null || true
sleep 2
serve "$H/work/ctrl.json" CTRL_PID2
CRLISTEN=$(python3 -c "import json; print(json.load(open('$H/work/ctrl.ready'))['listen'])")
rcreq() { "$MB" request "$H/work/pki/ca.der" "$H/work/pki/client.der" "$H/work/pki/client-key.der" "$CRLISTEN" localhost "$1"; }
sleep 4
if read RCTOK2 RCFENCE2 <<< "$(activate rcreq rcons 2>&1)" && wait_ready rcreq "$RCTOK2" "$RCFENCE2"; then :; else fail L10 remote-reactivate "out=$RCTOK2 $RCFENCE2"; fi
if out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK2\",\"fence\":\"$RCFENCE2\",\"operation\":\"op-rml1-4\",\"cap\":\"cons.chain@1\",\"input\":{\"chain\":true,\"input\":{\"value\":6}}}" 2>/dev/null) && echo "$out" | grep -q '"value":[ ]*6'; then ok L10 reconnect-serves; else fail L10 reconnect-serves "$out"; fi
if out=$(rcreq "{\"action\":\"invoke\",\"lease\":\"$RCTOK\",\"fence\":\"$RCFENCE\",\"operation\":\"op-rml1-5\",\"cap\":\"cons.chain@1\",\"input\":{}}" 2>&1); then fail L10 old-refs-dead "unexpected success"; elif echo "$out" | grep -qiE "stale|denied|not-active|unknown"; then ok L10 old-refs-dead; else fail L10 old-refs-dead "$out"; fi
for p in $PIDS; do kill "$p" 2>/dev/null || true; done; PIDS=""; sleep 2
if pgrep -af "matrix-managed serve" 2>/dev/null | grep -q "$H"; then fail L02 orphans "$(pgrep -af 'matrix-managed serve' | grep "$H" | head -3)"; else ok L02 no-orphans; fi
if grep -Eo '"lease":"[0-9a-f]{16,}"' "$LOG" | head -1 | grep -q .; then fail L12 no-secrets "lease material in harness log"; else ok L12 no-secrets; fi
grep -q "0.1.0-experimental" "$DIST/docs/VERSIONS.md" && ok L11 versions-published || fail L11 versions-published
echo "harness-ml1: $PASS passed, $FAIL failed (dir $H retained)"
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
