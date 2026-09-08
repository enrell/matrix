#!/usr/bin/env bash
# Scaffold a Matrix C/C++ node project (ML1). Self-contained and offline:
# SDK sources are vendored into the project, then the node is built
# with CMake (needs cmake + cc/c++; C++ node needs a C++17 compiler)
# and the config points at it. No network, no registry.
# Usage: scaffold.sh <name> <dir> [--pki <dir>] [--home <dir>] [--cxx]
set -euo pipefail
NAME="${1:?usage: scaffold.sh <name> <dir> [options]}"
DIR="$2"; shift 2 || true
PKI="@PKI@"; HOME_DIR="@HOME@"; CXX=0
while [ $# -gt 0 ]; do
  case "$1" in
    --pki) PKI="$2"; shift 2;;
    --home) HOME_DIR="$2"; shift 2;;
    --cxx) CXX=1; shift;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done
command -v cmake >/dev/null 2>&1 || { echo "cmake required to scaffold" >&2; exit 1; }
command -v cc >/dev/null 2>&1 || command -v gcc >/dev/null 2>&1 || { echo "a C compiler required to scaffold" >&2; exit 1; }
if [ "$CXX" = 1 ]; then command -v c++ >/dev/null 2>&1 || command -v g++ >/dev/null 2>&1 || { echo "a C++17 compiler required for --cxx" >&2; exit 1; }; fi
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
mkdir -p "$DIR/src" "$DIR/include" "$DIR/cpp" "$DIR/build"
cp "$ROOT/src/mx_json.c" "$ROOT/src/mx_json.h" "$ROOT/src/mx_component.c" "$ROOT/src/mx_op.c" "$DIR/src/"
cp "$ROOT/include/mx_component.h" "$DIR/include/"
cp "$ROOT/cpp/matrix.hpp" "$DIR/cpp/"
cp "$ROOT/examples/node.c" "$DIR/node.c"
cp "$ROOT/examples/node.cpp" "$DIR/node.cpp"
if [ "$CXX" = 1 ]; then
  NODE_BIN="$DIR/build/mx-node-cpp"
  CMAKE_CXX=ON
else
  NODE_BIN="$DIR/build/mx-node"
  CMAKE_CXX=OFF
fi
sed -e "s|<NAME>|$NAME|g; s|@CXX@|$CXX|g" "$ROOT/templates/README.md" > "$DIR/README.md"
sed -e "s|@NODE@|$NODE_BIN|g; s|@PKI@|$PKI|g; s|@HOME@|$HOME_DIR|g; s|@FINGERPRINT@|$FP|g" \
  "$ROOT/templates/config.json" > "$DIR/config.json"
cp "$ROOT/templates/CMakeLists.txt" "$DIR/CMakeLists.txt"
cp "$ROOT/matrix-component.pc.in" "$DIR/"
(cd "$DIR" && cmake -B build -DMATRIX_ENABLE_CPP=$CMAKE_CXX -DCMAKE_BUILD_TYPE=Release >/dev/null && cmake --build build) || { echo "scaffold build failed" >&2; exit 1; }
[ -x "$NODE_BIN" ] || { echo "node binary missing after build" >&2; exit 1; }
python3 -c "import json; json.load(open('$DIR/config.json'))" \
  || { echo "scaffold produced invalid config" >&2; exit 1; }
echo "scaffolded $NAME at $DIR (fingerprint: $FP)"
