#!/usr/bin/env bash
# Scaffold a Matrix JS node project (ML1). No network, no registry.
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
ROOT="$(cd "$(dirname "$0")" && pwd)"
node "$ROOT/bin/matrix-scaffold.js" "$NAME" "$DIR" --pki "$PKI" --home "$HOME_DIR"
