#!/usr/bin/env bash
# Scaffold a Matrix Python node project (ML1): generic test component
# (provider/consumer/independent roles in one program) + operator
# config template + recipe README. No network, no registry.
# Usage: scaffold.sh <name> <dir> [--python <venv-python>] [--pki <dir>] [--home <dir>]
set -euo pipefail
NAME="${1:?usage: scaffold.sh <name> <dir> [options]}"
DIR="$2"; shift 2
PYBIN="$(command -v python3)"
PKI="@PKI@"; HOME_DIR="@HOME@"
while [ $# -gt 0 ]; do
  case "$1" in
    --python) PYBIN="$2"; shift 2;;
    --pki) PKI="$2"; shift 2;;
    --home) HOME_DIR="$2"; shift 2;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done
ROOT="$(cd "$(dirname "$0")" && pwd)"
FP="@FINGERPRINT@"
if [ -f "$PKI/client.der" ] && command -v python3 >/dev/null; then
  FP="$(python3 -c "import hashlib; print(hashlib.sha256(open('$PKI/client.der','rb').read()).hexdigest())")"
fi
if [ -e "$DIR/scaffold.sh" ] || [ -e "$DIR/pyproject.toml" ]; then
  echo "refusing to scaffold onto SDK sources: $DIR" >&2; exit 2
fi
mkdir -p "$DIR"
cp "$ROOT/dep_node.py" "$DIR/node.py"
sed -e "s|<NAME>|$NAME|g" "$ROOT/templates/README.md" > "$DIR/README.md"
sed -e "s|@PYTHON@|$PYBIN|g; s|@DIR@|$DIR|g; s|@PKI@|$PKI|g; s|@HOME@|$HOME_DIR|g; s|@FINGERPRINT@|$FP|g" \
  "$ROOT/templates/config.json" > "$DIR/config.json"
python3 -c "import json; json.load(open('$DIR/config.json'))" \
  || { echo "scaffold produced invalid config" >&2; exit 1; }
echo "scaffolded $NAME at $DIR (fingerprint: $FP)"
