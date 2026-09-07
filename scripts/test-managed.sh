#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")/.."
cargo build --release -p matrix-host --examples
cargo test --release -- --test-threads=1
