#!/usr/bin/env bash
# Scaffold a Matrix Crystal node project (ML1). Self-contained and offline:
# SDK sources are vendored into the project, then the node is built
# and the config points at it. Needs the Crystal toolchain (>= 1.0).
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
command -v crystal >/dev/null 2>&1 || { echo "crystal toolchain (>= 1.0) required to scaffold" >&2; exit 1; }
ROOT="$(cd "$(dirname "$0")" && pwd)"
FP="@FINGERPRINT@"
if [ -f "$PKI/client.der" ]; then
  FP="$(python3 -c "import hashlib; print(hashlib.sha256(open('$PKI/client.der','rb').read()).hexdigest())")"
fi
# Never scaffold onto SDK sources (a DIR typo must not wipe the pack).
if [ -e "$DIR/scaffold.sh" ] || [ -e "$DIR/shard.yml" ] || [ -e "$DIR/mix.exs" ] || [ -e "$DIR/go.mod" ] || [ -e "$DIR/CMakeLists.txt" ] || [ -e "$DIR/package.json" ]; then
  echo "refusing to scaffold onto SDK sources: $DIR" >&2; exit 2
fi
rm -rf "$DIR"
mkdir -p "$DIR/src"
cp "$ROOT/src/matrix-component.cr" "$ROOT/src/operator.cr" "$DIR/src/"
cp "$ROOT/examples/node.cr" "$ROOT/examples/doctor.cr" "$DIR/"
sed -e 's|require "../src/matrix-component"|require "./src/matrix-component"|' "$DIR/node.cr" > "$DIR/node.cr.tmp" && mv "$DIR/node.cr.tmp" "$DIR/node.cr"
sed -e 's|require "../src/matrix-component"|require "./src/matrix-component"|' "$DIR/doctor.cr" > "$DIR/doctor.cr.tmp" && mv "$DIR/doctor.cr.tmp" "$DIR/doctor.cr"
sed -e "s|<NAME>|$NAME|g" "$ROOT/templates/README.md" > "$DIR/README.md"
sed -e "s|@NODE@|$DIR/mx-node|g; s|@PKI@|$PKI|g; s|@HOME@|$HOME_DIR|g; s|@FINGERPRINT@|$FP|g" \
  "$ROOT/templates/config.json" > "$DIR/config.json"
(cd "$DIR" && crystal build --release node.cr -o mx-node) || { echo "scaffold build failed" >&2; exit 1; }
python3 -c "import json; json.load(open('$DIR/config.json'))" \
  || { echo "scaffold produced invalid config" >&2; exit 1; }
echo "scaffolded $NAME at $DIR (fingerprint: $FP)"
