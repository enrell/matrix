#!/usr/bin/env bash
# Verifies the external harness (and the facade test) touch only the
# supported contract surface: `matrix_runtime::api`, `matrix_component`
# (+ `matrix_component.py`), shipped binaries and published schemas.
# Internal paths, absolute checkout references and source inclusion of
# private modules fail the check.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FAIL=0
check() { # $1=label $2...=files/dirs (grep -rEn, fail on match)
  local label="$1"; shift
  if grep -rEn "$FORBID" "$@" 2>/dev/null | grep -v "^.*://"; then
    echo "FAIL bounds $label"
    FAIL=1
  else
    echo "ok bounds $label"
  fi
}
FORBID='matrix_runtime::(service|store|session|route_controller|route_executor|remote|remote_session_server)|matrix_core::|matrix_host::|matrix_proto::|matrix_guard::|matrix_sdk::|matrix_rt::|#[path\s*=|include!|/home/|/root/|projects/matrix'
check "harness-rs" "$ROOT/scripts/harness-external.sh"
check "facade-test" "$ROOT/crates/matrix-runtime/tests/api_facade.rs" "$ROOT/crates/matrix-runtime/tests/backup_restore.rs"
exit $FAIL
