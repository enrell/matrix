#!/usr/bin/env bash
# M8 distribution packager (P01/P12 infrastructure): builds local artifacts
# from this checkout into dist/ with hashes and provenance. No registry
# publication, no releases, no network: everything here is a local file
# the external harness consumes by path (see scripts/harness-external.sh).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist"
BIN="$DIST/bin"
PY="$DIST/py"
SCHEMAS="$DIST/schemas"
CRATES="$DIST/crates"

rm -rf "$DIST"
mkdir -p "$BIN" "$PY" "$SCHEMAS" "$CRATES"

echo "-- release build (bins + examples)"
cargo build --release --workspace --examples 2>&1 | tail -1
cp "$ROOT/target/release/matrix-managed" "$BIN/"
cp "$ROOT/target/release/matrix-conform" "$BIN/"
cp "$ROOT/target/release/examples/dep_node" "$BIN/dep_node"

echo "-- ML1 language SDKs (staged sources + offline-built packs)"
ML1="$DIST/ml1"
mkdir -p "$ML1"
# Python: scaffold/node/operator sources (the wheel ships only the
# module; the wheel itself is copied into ml1/ after the build below).
rm -rf "$ML1/python"
mkdir -p "$ML1/python/templates"
cp "$ROOT"/sdk-python/matrix_component.py "$ROOT"/sdk-python/matrix_operator.py \
  "$ROOT"/sdk-python/dep_node.py "$ROOT"/sdk-python/scaffold.sh \
  "$ROOT"/sdk-python/README.md "$ROOT"/sdk-python/pyproject.toml \
  "$ROOT"/sdk-python/LICENSE-MIT "$ROOT"/sdk-python/LICENSE-APACHE-2.0 "$ML1/python/"
# JS: npm pack (offline, no registry).
if command -v npm >/dev/null 2>&1; then
  (cd "$ROOT/sdk/js" && rm -f matrix-kernel-*.tgz && npm pack --silent >/dev/null 2>&1) || true
  cp "$ROOT"/sdk/js/matrix-kernel-*.tgz "$ML1/" 2>/dev/null || true
  rm -f "$ROOT"/sdk/js/matrix-kernel-*.tgz
fi
# Go / Crystal / Elixir / C# / C: staged source trees (hermetic builds).
for s in go crystal elixir csharp c; do
  rm -rf "$ML1/$s"
  mkdir -p "$ML1/$s"
  cp "$ROOT"/sdk/$s/LICENSE-MIT "$ROOT"/sdk/$s/LICENSE-APACHE-2.0 "$ML1/$s/"
  case "$s" in
    go) cp "$ROOT"/sdk/go/go.mod "$ROOT"/sdk/go/*.go "$ML1/go/"
        mkdir -p "$ML1/go/cmd/mx-node" "$ML1/go/cmd/matrix-doctor" "$ML1/go/templates"
        cp "$ROOT"/sdk/go/*_test.go "$ML1/go/" 2>/dev/null || true
        cp "$ROOT"/sdk/go/cmd/mx-node/main.go "$ML1/go/cmd/mx-node/"
        cp "$ROOT"/sdk/go/cmd/matrix-doctor/main.go "$ML1/go/cmd/matrix-doctor/"
        cp "$ROOT"/sdk/go/scaffold.sh "$ROOT"/sdk/go/README.md "$ML1/go/"
        cp "$ROOT"/sdk/go/templates/* "$ML1/go/templates/";;
    crystal) cp "$ROOT"/sdk/crystal/shard.yml "$ML1/crystal/"
        mkdir -p "$ML1/crystal/src" "$ML1/crystal/examples" "$ML1/crystal/spec" "$ML1/crystal/templates"
        cp "$ROOT"/sdk/crystal/src/*.cr "$ML1/crystal/src/"
        cp "$ROOT"/sdk/crystal/examples/*.cr "$ML1/crystal/examples/"
        cp "$ROOT"/sdk/crystal/spec/*.cr "$ML1/crystal/spec/"
        cp "$ROOT"/sdk/crystal/scaffold.sh "$ROOT"/sdk/crystal/README.md "$ML1/crystal/"
        cp "$ROOT"/sdk/crystal/templates/* "$ML1/crystal/templates/";;
    elixir) mkdir -p "$ML1/elixir/lib/matrix" "$ML1/elixir/test/matrix" "$ML1/elixir/test/support" "$ML1/elixir/templates"
        cp "$ROOT"/sdk/elixir/mix.exs "$ML1/elixir/"
        cp "$ROOT"/sdk/elixir/lib/matrix/*.ex "$ML1/elixir/lib/matrix/"
        cp "$ROOT"/sdk/elixir/test/matrix/*.exs "$ML1/elixir/test/matrix/"
        cp "$ROOT"/sdk/elixir/test/test_helper.exs "$ML1/elixir/test/"
        cp "$ROOT"/sdk/elixir/test/support/*.ex "$ML1/elixir/test/support/"
        cp "$ROOT"/sdk/elixir/scaffold.sh "$ROOT"/sdk/elixir/README.md "$ML1/elixir/"
        cp "$ROOT"/sdk/elixir/templates/* "$ML1/elixir/templates/";;
    csharp) mkdir -p "$ML1/csharp/src/Matrix.Component" "$ML1/csharp/examples/Node" "$ML1/csharp/test/SelfTest" "$ML1/csharp/templates"
        cp "$ROOT"/sdk/csharp/src/Matrix.Component/*.cs "$ROOT"/sdk/csharp/src/Matrix.Component/*.csproj "$ML1/csharp/src/Matrix.Component/"
        cp "$ROOT"/sdk/csharp/examples/Node/*.cs "$ROOT"/sdk/csharp/examples/Node/*.csproj "$ML1/csharp/examples/Node/"
        cp "$ROOT"/sdk/csharp/test/SelfTest/*.cs "$ROOT"/sdk/csharp/test/SelfTest/*.csproj "$ML1/csharp/test/SelfTest/"
        cp "$ROOT"/sdk/csharp/scaffold.sh "$ML1/csharp/"
        cp "$ROOT"/sdk/csharp/templates/* "$ML1/csharp/templates/";;
    c) mkdir -p "$ML1/c/include" "$ML1/c/src" "$ML1/c/cpp" "$ML1/c/examples" "$ML1/c/test" "$ML1/c/templates"
        cp "$ROOT"/sdk/c/include/*.h "$ML1/c/include/"
        cp "$ROOT"/sdk/c/src/*.[ch] "$ML1/c/src/"
        cp "$ROOT"/sdk/c/cpp/*.hpp "$ML1/c/cpp/"
        cp "$ROOT"/sdk/c/examples/node.c "$ROOT"/sdk/c/examples/node.cpp "$ROOT"/sdk/c/examples/doctor.c "$ML1/c/examples/"
        cp "$ROOT"/sdk/c/test/test_component.c "$ROOT"/sdk/c/test/test_operator.c "$ROOT"/sdk/c/test/test_cpp.cpp "$ML1/c/test/"
        cp "$ROOT"/sdk/c/CMakeLists.txt "$ROOT"/sdk/c/matrix-component.pc.in "$ROOT"/sdk/c/scaffold.sh "$ML1/c/"
        cp "$ROOT"/sdk/c/templates/* "$ML1/c/templates/";;
  esac
done
# Per-language tarballs (one installable pack per ecosystem).
(cd "$ML1" && for s in python go crystal elixir csharp c; do
  tar czf "matrix-$s-0.1.0.tar.gz" "$s" 2>/dev/null || true
done)
ls "$ML1"/

echo "-- python wheel (setuptools backend, no pip needed)"
rm -rf "$ROOT/sdk-python/build" "$ROOT/sdk-python/"*.egg-info
python3 -c "
from setuptools import build_meta as b
import os
os.chdir('$ROOT/sdk-python')
print(b.build_wheel('$PY'))
" > /dev/null
rm -rf "$ROOT/sdk-python/build" "$ROOT/sdk-python/"*.egg-info
cp "$PY"/matrix_kernel-*.whl "$ML1/" 2>/dev/null || true

echo "-- schemas and vectors"
"$BIN/matrix-conform" --dump-vectors > "$SCHEMAS/vectors.json"

echo "-- fixtures (generic test components, no product app)"
mkdir -p "$DIST/fixtures"
cp "$ROOT/sdk-python/dep_node.py" "$DIST/fixtures/"
cp "$ROOT/sdk-python/matrix_component.py" "$DIST/fixtures/"
cp "$PY"/matrix_kernel-*.whl "$DIST/fixtures/" 2>/dev/null || true

echo "-- extracted crate sources (complete artifacts for path use)"
for c in matrix-component matrix-core matrix-guard matrix-host matrix-proto matrix-runtime; do
  rm -rf "$CRATES/$c"
  cp -r "$ROOT/crates/$c" "$CRATES/$c"
  rm -rf "$CRATES/$c/target"
done

echo "-- docs (contract reference travels with artifacts)"
mkdir -p "$DIST/docs"
cp "$ROOT"/docs/API-CATALOG.md "$ROOT"/docs/VERSIONS.md "$ROOT"/docs/INSTALL.md \
  "$ROOT"/docs/MANAGED-RUNTIME.md "$ROOT"/docs/M6-COMPOSITION.md "$ROOT"/docs/M7-COMPOSITION.md \
  "$ROOT"/docs/M7-EPIC.md "$ROOT"/docs/M7-PROFILE.md "$ROOT"/docs/M8-EPIC.md \
  "$ROOT"/docs/SDK.md "$ROOT"/docs/CONTRACT.md "$ROOT"/docs/ML1-NODE.md \
  "$ROOT"/docs/ML1-MATRIX.md "$ROOT"/docs/ML1-COMPOSITION.md \
  "$ROOT"/docs/MULTILANGUAGE-EPIC.md "$DIST/docs/" 2>/dev/null || true
[ -f "$DIST/docs/M8-COMPOSITION.md" ] || echo "M8-COMPOSITION pending at pack time" > "$DIST/docs/M8-PENDING.txt"

echo "-- manifest"
{
  echo "# Matrix M8+ML1 local distribution (dual-licensed MIT OR Apache-2.0, see LICENSE-*)"
  echo "built: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_rev: $(git -C "$ROOT" rev-parse HEAD)"
  echo "git_status_clean: $([ -z "$(git -C "$ROOT" status --short)" ] && echo yes || echo no)"
  echo "rustc: $(rustc --version)"
  echo "python: $(python3 --version 2>&1)"
  echo "node: $(node --version 2>&1)"
  echo "go: $(go version 2>&1)"
  echo "crystal: $(crystal --version 2>&1 | head -1)"
  echo "elixir: $(elixir --version 2>&1 | tail -1)"
  echo "dotnet: $(dotnet --version 2>&1)"
  echo "cc: $(cc --version 2>&1 | head -1)"
  echo "platform: $(uname -sm)"
  echo "api: $(grep -o 'API_VERSION[^;]*' "$ROOT/crates/matrix-runtime/src/api.rs" | head -1)"
  echo "inspect_schema: matrix.inspect/1"
  echo "---"
  (cd "$DIST" && sha256sum $(find bin py schemas crates ml1 -type f | sort))
} > "$DIST/MANIFEST.txt"

echo "dist ready: $DIST ($(du -sh "$DIST" | cut -f1))"
