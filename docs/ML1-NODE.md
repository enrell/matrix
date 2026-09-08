# ML1 test-node contract (all language SDKs)

Status: normative for the ML1 delivery. A *test node* is the generic
fixture every ML1 SDK ships (`examples/node` + scaffold template): one
program that can play provider, consumer and independent roles, so every
language proves the same observable behavior without a product app.

Wire behavior (hello, register, activate, calls, dependencies,
resources, events, streams, cancel, dispose) is `matrix.component/0.1`
as implemented by the reference SDKs (`matrix-component` Rust,
`matrix_component` Python) and described in `docs/SDK.md`. This file
only pins the *business* contract the harnesses drive.

## Command line

```text
mx-node --matrix-sock <sock> --id <logical>
        [--event-log <path>] [--stream-log <path>]
        [--stream-slow-ms <n>]
```

- `--matrix-sock`: host Unix socket (the `{sock}` placeholder).
- `--id`: logical component id (the `{id}` placeholder).
- `--event-log`: appends `topic<TAB>payload-json` per `on_event`.
- `--stream-log`: appends `stream_id<TAB>seq<TAB>payload-len` per
  `on_stream`.
- `--stream-slow-ms`: sleeps N ms per received chunk (slow-consumer
  tests; backpressure stays host-managed, the node only observes).
- Exit `0` on dispose/EOF, nonzero with a stderr diagnostic otherwise.
  Missing `--matrix-sock`/`--id` is exit `2` (usage error).

Launch token comes from `MATRIX_LAUNCH_TOKEN` like the reference SDKs.

## Call contract (JSON input → JSON output or business error)

Inputs are matched in this order; the first matching key wins:

| Input | Behavior |
|---|---|
| `sleep_ms: N` (any call) | Abortable sleep first (5 ms slices honouring cancel); aborted calls answer business error `cancelled/aborted`. |
| `fail: "CODE"` | Business error `CODE` / `remote CODE`. |
| `amplify: N` | `{"blob": "x"*min(N,1MiB), "via": id}` (bounded test output). |
| `chain: true` | Invokes the first activation binding with `input.input` (`input.timeout_ms`, default 5000, min 1) and answers `{"chained": <child output>, "via": id}`. No bindings → `dependency-unavailable/no binding`. Child errors propagate as business errors with the provider's code/message. |
| `acquire: {kind, label, interval_ms?}` | `{"acquired": {"handle": "<n>"}, "via": id}`; failures propagate as business errors with the wire code. |
| `release: N` | `{"released": "<N>", "via": id}`; failures propagate likewise. |
| `stream_send: {stream_id, chunks, chunk_bytes, sleep_ms?}` | Sends up to 256 chunks of up to 4096 `x` bytes; answers `{"stream_sent": N, "via": id}`. Cancel mid-send → `cancelled/aborted`. Send failure → `stream-refused/<why>`. |
| (otherwise) | `{"echo": <input>, "via": id}`. |

`chain_with_streams` (M7 bidi legs) is implemented by every node:
`{"chained", "via", "stream_sent"}`. Streams bind to the in-flight
operation leg (M7 single-leg rule): send while the child call runs —
a leg that completes before the first chunk sinks it locally.

## Types and limits

- Payloads are text (`string`). A node asked to emit non-text bytes
  must refuse with an explicit error, never corrupt (lossy UTF-8
  conversion is forbidden by the epic).
- `seq`/`generation`/handles cross the wire as decimal strings; SDKs
  must not lose precision (u64 range; JS uses BigInt/string).
- Event/stream edge queues are bounded (64, drop-oldest, counted in
  `event_dropped_count()`); `pending_stream_count()` exposes pressure.
- Blocking dependency/resource calls refuse on the reader thread with
  `internal/blocking call on reader thread` instead of deadlocking.
- Without a negotiated `dependency-calls/1`, `invoke_dependency`
  refuses locally with `unsupported-feature` and never touches the wire.

## Roles in the harnesses

- Provider: serves `echo`/`acquire`/streams; killed mid-`sleep_ms`
  call for the crash path (L06/L11).
- Consumer: `chain: true` with an outbound grant; used in same-language
  chains, the L09 cross pairs and the three-language chain.
- Independent: no bindings; must keep serving while providers die and
  after re-provisioning (new generation, old refs dead).
