# Matrix

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](LICENSE-MIT)

A Rust kernel for spatiotemporal component composition, with
language-independent plugins and local or remote execution. Applications
and business components live in separate repositories — this one holds the
core: context, ownership, dependencies, authorization and lifecycle.

**Status: experimental runtime (`0.1.0`).** No `1.0` stability promise.
What each surface guarantees is in [docs/VERSIONS.md](docs/VERSIONS.md);
known limitations in [docs/M8-COMPOSITION.md](docs/M8-COMPOSITION.md).

## Install

SDKs in 9 languages, dual-licensed MIT OR Apache-2.0. Registry publication
is in progress; for now, install from local artifacts:

```sh
./scripts/package.sh
```

Then follow the per-ecosystem guide in [docs/INSTALL.md](docs/INSTALL.md)
(offline venv + wheel, `npm install --offline` from the tarball,
`GOPROXY=off`, network-free `shards`/`mix`/`dotnet`/`cmake`). Each SDK ships
a `scaffold.sh` that generates a working project from a template.

## Five minutes

```sh
./scripts/dev-pki.py /tmp/pki --server-name localhost
# edit a config.json (example at sdk-python/templates/config.json)
./target/release/matrix-managed serve /tmp/config.json
```

Or via the Python SDK:

```python
from matrix_operator import start
kernel = start("/path/to/matrix-managed", config_dict, operator_pki)
act = kernel.client.activate("prov", 30000)
v = kernel.client.invoke(act["lease"], act["fence"], "op-1",
                         "prov.echo@1", {"ping": 1})
kernel.close()
```

The end-to-end demo (`python3 scripts/demo-composition.py`) shows a
Rust→Python chain with withdraw and reintroduction in about a minute.

## Docs

- [Documentation map](docs/README.md) · [Actual code status](docs/STATUS.md)
- [Contract and invariants](docs/CONTRACT.md) · [Protocol](docs/PROTOCOL.md)
- [Managed profile (operations)](docs/MANAGED-RUNTIME.md) · [Plugins](PLUGIN.md)
- [SDK conformance](CONFORMANCE.md) · [Kernel verification](KERNEL_VERIFICATION.md)

## Layout

| Crate | Role |
|---|---|
| `matrix-core` | Contexts, resources, dependencies, tickets and local lifecycle |
| `matrix-rt` | Daemon and CLI over a Unix socket, trusted profile (legacy) |
| `matrix-host` / `matrix-component` | Local process host and SDK |
| `matrix-guard` | Linux sandbox and supervision budget |
| `matrix-runtime` | Managed service: SQLite, mutual TLS, leases (`api` is the public facade) |
| `matrix-sdk` | Legacy daemon client (frozen compatibility) |

Canonical validation: `make test`. Compatibility: `make compat`.

## License

MIT OR Apache-2.0 — see [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE-2.0](LICENSE-APACHE-2.0). Applies to all SDKs and crates;
pick whichever you prefer, no copyleft: closed-source use is allowed,
just preserve the notices.
