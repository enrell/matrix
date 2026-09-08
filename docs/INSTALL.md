# Install guide (M8, local artifacts only)

There is no registry publication and no `1.0`: everything here installs
from a `dist/` tree built by `scripts/package.sh` in the Matrix
checkout (Rust 1.98+, Python 3.10+, Linux x86_64, OpenSSL CLI).
`dist/MANIFEST.txt` records hashes, versions and provenance — verify
first:

```sh
./scripts/package.sh
(cd dist && sha256sum -c --quiet <(grep -E "bin/|py/|schemas/vectors.json" MANIFEST.txt))
```

## Binaries

`dist/bin/` holds `matrix-managed` (service daemon + admin CLI),
`matrix-conform` (protocol conformance) and `dep_node` (generic test
fixture, not a product). Run from any directory; no install step, no
hidden checkout dependency (the harness asserts this from `/tmp`).

## Python SDK

No PyPI upload (the repository ships no license file). Install from the
shipped wheel without any `PYTHONPATH` pointing at a checkout:

```sh
# option A: unzip next to your project and import from the file path
unzip dist/py/matrix_component-*.whl -d third_party/matrix
# option B: point sys.path at the wheel itself (zipimport)
python3 -c "import sys; sys.path.insert(0, 'third_party/matrix_component-0.1.0-py3-none-any.whl'); import matrix_component"
```

Contract reference is `docs/SDK.md`; parity with the Rust crate is
covered by `test_units.py` on both sides.

## ML1 language SDKs (per-language packs in `dist/ml1/`)

Same offline rules per ecosystem (see `docs/ML1-MATRIX.md` for versions
and `templates/README.md` inside each generated project for the recipe):

```sh
python3 -m venv .venv && .venv/bin/pip install --no-index dist/ml1/matrix_component-*.whl
npm install --offline --no-audit --no-fund dist/ml1/matrix-component-*.tgz
# Go: copy dist/ml1/go, add `replace matrix-component-go => ./go`, GOPROXY=off
# Crystal: copy dist/ml1/crystal (zero shard deps, `shards build` offline)
# Elixir: `{:matrix_component, path: "dist/ml1/elixir"}` (no Hex deps, OTP >= 27)
# C#: copy dist/ml1/csharp (`dotnet build`, zero NuGet packages)
# C/C++: cmake -B build -DMATRIX_ENABLE_CPP=ON && cmake --build build (no deps)
```

Each SDK also ships a scaffold (`scaffold.sh`, or `matrix-scaffold`
for JS) that vendors SDK sources into a fresh project and builds the
generic test node, plus an environment doctor (`matrix-doctor`) that
prints JSON diagnosis with secrets redacted.

## Rust crates

Depend on extracted copies (complete artifacts — `dist/crates/*`),
never on the checkout tree:

```toml
[dependencies]
matrix-runtime = { path = "../third_party/crates/matrix-runtime" }
```

Only `matrix_runtime::api` and `matrix-component` are supported
surfaces (`docs/API-CATALOG.md`); `scripts/check-harness-bounds.sh`
fails any other `matrix_*` path in dependent code. Build offline after
the first fetch: `cargo build --offline --release`.

## First service

```sh
./scripts/dev-pki.py /tmp/pki --server-name localhost
FP=$(./target/release/matrix-managed fingerprint /tmp/pki/client.der)
# write config (see docs/MANAGED-RUNTIME.md), then:
./target/release/matrix-managed serve /tmp/config.json
./target/release/matrix-managed request /tmp/pki/ca.der /tmp/pki/client.der \
  /tmp/pki/client-key.der 127.0.0.1:7443 localhost \
  '{"action":"activate","component":"echo","ttl_ms":10000}'
```

Grants map client-certificate fingerprints to components and
capabilities; the fingerprint above goes in the config. Leases are
short-lived credentials: never log them, renew or re-activate.

## What is NOT offered

Registry packages, public releases, Windows/macOS builds, WASM,
performance figures (M9 measures; nothing announced here), or exactly-once
across hosts. See `docs/M8-COMPOSITION.md` for the full limitation list.
