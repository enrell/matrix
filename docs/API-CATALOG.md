# API catalog (M8 contract surfaces)

Status words: **supported** (may be relied upon per `VERSIONS.md`),
**experimental** (supported but `0.x`: may break with migration +
explicit refusal), **internal** (reachable, no stability promise),
**legacy** (frozen, compatibility only).

Everything supported is experimental until a `1.0` declaration that
this epic explicitly does not make.

## Supported (via `matrix_runtime::api`)

Construction/start/inspect/shutdown, operator activate/invoke/release,
grant/revoke, outbound sync, config parse/validate/reload, typed
`ErrorCode` errors, versioned inspect snapshots, shutdown
reports. Handles are opaque; no locks, registries, ticket tables or
ledger cross this boundary.

## Supported (components)

- Rust `matrix-component`: `Component`, `CallCtx`, `Handler`
  (`on_call`/`on_cancel`/`on_event`/`on_stream`), `invoke_dependency`,
  `acquire/release_resource`, `send_stream`, `event_dropped_count`,
  `pending_stream_count`. Threading/cancel/backpressure: `docs/SDK.md`.
- Python `matrix_component` (`sdk-python/`, pip wheel): same surface
  (`Handler.on_call/on_cancel/on_event/on_stream`,
  `ctx.invoke_dependency/acquire_resource/release_resource/send_stream`,
  `event_dropped_count/pending_stream_count`).
- Wire protocols (implementable without our SDKs):
  `matrix.component/0.1` (`docs/PROTOCOL.md`, `matrix-proto` schemas +
  vectors) and `matrix.remote/0.1` (`docs/M7-PROFILE.md`).

## Supported (operations)

- `matrix-managed serve <config>` (+ `snapshot`, `request`,
  `fingerprint`), config schema (`api::Config`), SIGHUP reload
  semantics, readiness/shutdown reporting (`docs/MANAGED-RUNTIME.md`).
- `matrix-conform` conformance command (schemas, vectors, behavior
  subset; environment prerequisites declared per check).

## Internal (explicitly not contracted)

`matrix_core::{bus, calls, context, deps, envelope, external, fsm,
identity, journal, kernel, leases, registry, resources}` internals,
`matrix_runtime::{service, store, session, route_controller,
route_executor, remote, remote_session_server}` engines,
`matrix_host::{Host, RemoteTransport}` mechanics and test-only helpers
(`bind_remote_stream`, `remote_op_of`, stream/credit injectors),
`matrix_guard` isolation primitives. Cross-crate visibility where it
exists serves the build, not adopters; depending on it voids the
contract. `scripts/check-harness-bounds.sh` enforces this on the
external harness.

## Legacy (frozen)

`matrix-sdk` (`MatrixClient`, byte-identical) and the `matrix-rt`
daemon speak the pre-managed envelope protocol for compatibility
(`make compat`). They are not the managed authority path and must not
be used to bypass it. Removal ships with migration + refusal, never a
silent alias.
