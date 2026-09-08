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

echo "-- python wheel (setuptools backend, no pip needed)"
rm -rf "$ROOT/sdk-python/build" "$ROOT/sdk-python/"*.egg-info
python3 -c "
from setuptools import build_meta as b
import os
os.chdir('$ROOT/sdk-python')
print(b.build_wheel('$PY'))
" > /dev/null
rm -rf "$ROOT/sdk-python/build" "$ROOT/sdk-python/"*.egg-info

echo "-- schemas and vectors"
"$BIN/matrix-conform" --dump-vectors > "$SCHEMAS/vectors.json"

echo "-- fixtures (generic test components, no product app)"
mkdir -p "$DIST/fixtures"
cp "$ROOT/sdk-python/dep_node.py" "$DIST/fixtures/"
cp "$ROOT/sdk-python/matrix_component.py" "$DIST/fixtures/"
cp "$PY"/matrix_component-*.whl "$DIST/fixtures/" 2>/dev/null || true

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
  "$ROOT"/docs/SDK.md "$ROOT"/docs/CONTRACT.md "$DIST/docs/" 2>/dev/null || true
[ -f "$DIST/docs/M8-COMPOSITION.md" ] || echo "M8-COMPOSITION pending at pack time" > "$DIST/docs/M8-PENDING.txt"

echo "-- manifest"
{
  echo "# Matrix M8 local distribution (validation only, no license conveyed)"
  echo "built: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_rev: $(git -C "$ROOT" rev-parse HEAD)"
  echo "git_status_clean: $([ -z "$(git -C "$ROOT" status --short)" ] && echo yes || echo no)"
  echo "rustc: $(rustc --version)"
  echo "python: $(python3 --version 2>&1)"
  echo "platform: $(uname -sm)"
  echo "api: $(grep -o 'API_VERSION[^;]*' "$ROOT/crates/matrix-runtime/src/api.rs" | head -1)"
  echo "inspect_schema: matrix.inspect/1"
  echo "---"
  (cd "$DIST" && sha256sum $(find bin py schemas crates -type f | sort))
} > "$DIST/MANIFEST.txt"

echo "dist ready: $DIST ($(du -sh "$DIST" | cut -f1))"
