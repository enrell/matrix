#!/usr/bin/env bash
# Scaffold a Matrix Elixir node project (ML1). Self-contained and offline:
# SDK sources are staged into the project, the escript node is built
# (needs Elixir >= 1.17 + OTP >= 27 for :json, no Hex deps) and the
# config points at a launcher wrapper. No network, no registry.
# Usage: scaffold.sh <name> <dir> [--pki <dir>] [--home <dir>]
set -euo pipefail
NAME="${1:?usage: scaffold.sh <name> <dir> [options]}"
DIR="$2"; shift 2 || true
PKI="@PKI@"; HOME_DIR="@HOME@"
while [ $# -gt 0 ]; do
  case "$1" in
    --pki) PKI="$2"; shift 2;;
    --home) HOME_DIR="$2"; shift 2;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done
command -v elixir >/dev/null 2>&1 || { echo "elixir (>= 1.17, OTP >= 27) required to scaffold" >&2; exit 1; }
command -v mix >/dev/null 2>&1 || { echo "mix required to scaffold" >&2; exit 1; }
ROOT="$(cd "$(dirname "$0")" && pwd)"
FP="@FINGERPRINT@"
if [ -f "$PKI/client.der" ]; then
  FP="$(python3 -c "import hashlib; print(hashlib.sha256(open('$PKI/client.der','rb').read()).hexdigest())")"
fi
ERL_BIN="$(dirname "$(command -v erl)")"
ELIXIR_BIN="$(dirname "$(command -v elixir)")"
# Never scaffold onto SDK sources (a DIR typo must not wipe the pack).
if [ -e "$DIR/scaffold.sh" ] || [ -e "$DIR/shard.yml" ] || [ -e "$DIR/mix.exs" ] || [ -e "$DIR/go.mod" ] || [ -e "$DIR/CMakeLists.txt" ] || [ -e "$DIR/package.json" ]; then
  echo "refusing to scaffold onto SDK sources: $DIR" >&2; exit 2
fi
rm -rf "$DIR"
mkdir -p "$DIR"
cp -r "$ROOT/lib" "$ROOT/mix.exs" "$DIR/"
rm -f "$DIR/mx-node" "$DIR/mx-node.exe"
sed -e "s|<NAME>|$NAME|g" "$ROOT/templates/README.md" > "$DIR/README.md"
# Launcher wrapper: escript needs OTP's `escript` on PATH (absolute
# staged paths; same-machine validation, no silent downloads).
cat > "$DIR/run-node.sh" <<EOF
#!/bin/sh
export PATH="$ERL_BIN:$ELIXIR_BIN:/usr/bin:/bin"
export ELIXIR_ERL_OPTIONS="\${ELIXIR_ERL_OPTIONS:+\$ELIXIR_ERL_OPTIONS }+fnu"
exec "$DIR/mx-node" "\$@"
EOF
chmod +x "$DIR/run-node.sh"
sed -e "s|@NODE@|$DIR/run-node.sh|g; s|@PKI@|$PKI|g; s|@HOME@|$HOME_DIR|g; s|@FINGERPRINT@|$FP|g" \
  "$ROOT/templates/config.json" > "$DIR/config.json"
(cd "$DIR" && MIX_ENV=prod mix escript.build) || { echo "scaffold build failed" >&2; exit 1; }
python3 -c "import json; json.load(open('$DIR/config.json'))" \
  || { echo "scaffold produced invalid config" >&2; exit 1; }
echo "scaffolded $NAME at $DIR (fingerprint: $FP)"
