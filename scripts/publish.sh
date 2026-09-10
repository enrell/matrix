#!/usr/bin/env bash
# Publish Matrix SDK packages to public registries (dual-licensed MIT OR Apache-2.0).
# Scope: SDKs only (python js go crystal elixir csharp c). Kernel crates stay
# unpublished (`publish = false`) until a separate decision.
#
# Secrets come ONLY from the environment (never paste tokens here or in chat):
#   NPM_TOKEN            npmjs automation token (2FA: use a granular automation token)
#   TWINE_USERNAME=__token__  TWINE_PASSWORD=pypi-...   (PyPI API token)
#   HEX_API_KEY          `mix hex.user auth` already stores it; this script only publishes
#   NUGET_API_KEY        nuget.org API key (dotnet nuget push)
#   SHARDS: Crystal has no central upload; tag the release (shards resolve from git)
#   GO: no registry; tag the release (proxy serves from git). Module path must
#       first become a full import path (github.com/<org>/...) — still bare.
#
# Usage: ./scripts/publish.sh [ecosystem...]   (default: all, in dependency-free order)
#   Each step prints what it will do and asks for confirmation unless PUBLISH_YES=1.
#   Dry-run first: PUBLISH_DRYRUN=1 ./scripts/publish.sh   (pack + validate, no upload)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
test -z "${PUBLISH_YES:-}" && CONFIRM=1 || CONFIRM=0
DRYRUN="${PUBLISH_DRYRUN:-0}"

confirm() { # $1=label
  if [ "$CONFIRM" = 1 ]; then
    printf 'publish %s? [y/N] ' "$1"; read -r ans
    [ "$ans" = y ] || { echo "skipped $1"; return 1; }
  fi
  return 0
}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing tool: $1"; exit 2; }; }

step_python() {
  need python3; confirm python || return 0
  (cd "$ROOT/sdk-python" && rm -rf build dist *.egg-info && python3 -c "
from setuptools import build_meta as b
print(b.build_wheel('dist'))" && rm -rf build *.egg-info)
  if [ "$DRYRUN" = 1 ]; then echo "python dry-run ok (wheel built, not uploaded)"; return 0; fi
  : "${TWINE_PASSWORD:?set TWINE_USERNAME=__token__ and TWINE_PASSWORD}"
  need twine
  TWINE_USERNAME="${TWINE_USERNAME:-__token__}" twine upload "$ROOT/sdk-python/dist/"*
}

step_js() {
  need npm; confirm js || return 0
  (cd "$ROOT/sdk/js" && npm pack --dry-run >/dev/null && echo "js dry-run ok")
  if [ "$DRYRUN" = 1 ]; then return 0; fi
  : "${NPM_TOKEN:?set NPM_TOKEN}"
  (cd "$ROOT/sdk/js" && npm publish --access public)
}

step_go() {
  confirm go || return 0
  echo "go: no registry upload. Tag the release instead: git tag sdk-go/v0.1.0 && git push origin sdk-go/v0.1.0"
  echo "BLOCKED until go.mod becomes a full import path (today: bare module matrix-component-go)."
}

step_crystal() {
  confirm crystal || return 0
  echo "crystal: no central upload. Tag the release instead: git tag sdk-crystal/v0.1.0 && git push origin sdk-crystal/v0.1.0"
}

step_elixir() {
  need mix; confirm elixir || return 0
  (cd "$ROOT/sdk/elixir" && mix hex.package --dry-run 2>/dev/null || mix hex.build --unpack 2>/dev/null || echo "hex tooling not installed; skipping build check")
  if [ "$DRYRUN" = 1 ]; then return 0; fi
  (cd "$ROOT/sdk/elixir" && mix hex.publish --yes)
}

step_csharp() {
  need dotnet; confirm csharp || return 0
  (cd "$ROOT/sdk/csharp/src/Matrix.Component" && dotnet pack -c Release --no-restore 2>/dev/null || dotnet pack -c Release)
  if [ "$DRYRUN" = 1 ]; then echo "csharp dry-run ok (nupkg built, not pushed)"; return 0; fi
  : "${NUGET_API_KEY:?set NUGET_API_KEY}"
  dotnet nuget push "$ROOT/sdk/csharp/src/Matrix.Component/bin/Release/"*.nupkg --api-key "$NUGET_API_KEY" --source https://api.nuget.org/v3/index.json
}

step_c() {
  confirm c || return 0
  echo "c: header+static-lib via CMake tarball (dist/ml1/matrix-c-*.tar.gz already carries LICENSE-*); no registry. Attach the tarball to the release."
}

STEPS="${*:-python js go crystal elixir csharp c}"
for s in $STEPS; do "step_$s"; done
echo "publish done: $STEPS"
