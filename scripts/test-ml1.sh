#!/usr/bin/env bash
# ML1 in-repo SDK suites (unit + live-against-local-build).
# Hermetic: only the repo toolchain + the locally built matrix-managed.
# Live parts run against the in-repo release binary (no network).
# Toolchains resolve explicit-install-dir FIRST: bare `command -v` can
# find a version-manager shim that exists but fails at runtime.
# Usage: scripts/test-ml1.sh (called by `make test`).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1
export GOPROXY=off GOFLAGS=-mod=mod
MISE="$HOME/.local/share/mise/installs"

# pick <explicit> <name> <probe-args...>: first runnable binary wins.
pick() {
  local explicit="$1" name="$2"; shift 2
  if [ -x "$explicit" ] && "$explicit" "$@" >/dev/null 2>&1; then echo "$explicit"; return 0; fi
  local p
  p="$(command -v "$name" 2>/dev/null)" || true
  if [ -n "$p" ] && [ -x "$p" ] && "$p" "$@" >/dev/null 2>&1; then echo "$p"; return 0; fi
  return 1
}
GOCMD="$(pick "$MISE/go/1.27.1/bin/go" go version || true)"
CRYSTAL="$(pick "$MISE/crystal/1.21.0/bin/crystal" crystal --version || true)"
ELIXIR_BIN="$MISE/elixir/1.20.4-otp-29/bin"
ERL_BIN="$MISE/erlang/29.0.6/bin"
export PATH="$ERL_BIN:$ELIXIR_BIN:$PATH"
MIX="$(pick "$ELIXIR_BIN/mix" mix --version || true)"
ELIXIR="$(pick "$ELIXIR_BIN/elixir" elixir --version || true)"
NODE="$(pick "" node --version || true)"
DOTNET="$(pick "" dotnet --version || true)"

# Managed binary for live parts (build if missing).
MB="$ROOT/target/release/matrix-managed"
if [ ! -x "$MB" ]; then
  echo "-- building matrix-managed for ML1 live parts"
  (cd "$ROOT" && cargo build --release -p matrix-runtime --bins 2>&1 | tail -1)
fi
HAVE_MB=0
[ -x "$MB" ] && HAVE_MB=1
export MX_MATRIX_MANAGED="$MB"
export MX_DEV_PKI="$ROOT/scripts/dev-pki.py"

PASS=0; FAIL=0; SKIP=0
ok() { PASS=$((PASS+1)); echo "ok ml1 $*"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL ml1 $*"; }
skip() { SKIP=$((SKIP+1)); echo "skip ml1 $1 ($2)"; }

# ---------- Python ----------
if [ "$HAVE_MB" = 1 ]; then
  if python3 "$ROOT/sdk-python/test_units.py" >/dev/null 2>&1; then ok py-units; else fail py-units; fi
  if python3 "$ROOT/sdk-python/test_operator.py" >/dev/null 2>&1; then ok py-operator; else fail py-operator; fi
else
  skip py "no matrix-managed"
fi

# ---------- JS (unit + live; live skips without binary) ----------
if [ -n "$NODE" ]; then
  if (cd "$ROOT/sdk/js" && node --test test/ > /tmp/opencode/ml1-js.log 2>&1); then ok js; else fail js "$(tail -2 /tmp/opencode/ml1-js.log)"; fi
else
  skip js "no node"
fi

# ---------- Go ----------
if [ -n "$GOCMD" ]; then
  if (cd "$ROOT/sdk/go" && "$GOCMD" vet ./... > /tmp/opencode/ml1-go.log 2>&1 && "$GOCMD" test -count=1 -timeout 300s . >> /tmp/opencode/ml1-go.log 2>&1); then ok go; else fail go "$(tail -2 /tmp/opencode/ml1-go.log)"; fi
else
  skip go "no go toolchain"
fi

# ---------- Crystal ----------
if [ -n "$CRYSTAL" ]; then
  if (cd "$ROOT/sdk/crystal" && "$CRYSTAL" spec > /tmp/opencode/ml1-cr.log 2>&1); then ok crystal; else fail crystal "$(tail -3 /tmp/opencode/ml1-cr.log)"; fi
else
  skip crystal "no crystal"
fi

# ---------- Elixir ----------
if [ -n "$MIX" ] && [ -n "$ELIXIR" ]; then
  if (cd "$ROOT/sdk/elixir" && mix test > /tmp/opencode/ml1-ex.log 2>&1); then ok elixir; else fail elixir "$(tail -3 /tmp/opencode/ml1-ex.log)"; fi
else
  skip elixir "no elixir/mix"
fi

# ---------- C# ----------
if [ -n "$DOTNET" ]; then
  if (cd "$ROOT/sdk/csharp" && dotnet run -c Release --project test/SelfTest/SelfTest.csproj > /tmp/opencode/ml1-cs.log 2>&1); then ok csharp; else fail csharp "$(tail -3 /tmp/opencode/ml1-cs.log)"; fi
else
  skip csharp "no dotnet"
fi

# ---------- C / C++ ----------
if command -v cmake >/dev/null 2>&1 && command -v cc >/dev/null 2>&1; then
  rm -rf /tmp/opencode/ml1-cbuild
  mkdir -p /tmp/opencode/ml1-cbuild
  if (cd /tmp/opencode/ml1-cbuild && cmake -S "$ROOT/sdk/c" -B . -DMATRIX_ENABLE_CPP=ON -DCMAKE_BUILD_TYPE=Release > /tmp/opencode/ml1-c.log 2>&1 && cmake --build . >> /tmp/opencode/ml1-c.log 2>&1 && ctest >> /tmp/opencode/ml1-c.log 2>&1); then ok c-cpp; else fail c-cpp "$(tail -3 /tmp/opencode/ml1-c.log)"; fi
  rm -rf /tmp/opencode/ml1-cbuild
else
  skip c-cpp "no cmake/cc"
fi

echo "ml1: $PASS passed, $FAIL failed, $SKIP skipped"
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
