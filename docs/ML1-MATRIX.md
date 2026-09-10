# ML1 language/profile matrix (normative for the delivery)

Status: ML1 implemented; this matrix records what each SDK proves, on
which profiles, with which differences. Everything not listed here is
not claimed. Wire behavior is `matrix.component/0.1` everywhere
(`docs/SDK.md`); business behavior is `docs/ML1-NODE.md`.

## Packages

| Language | Package / sources | Version | Install (offline, isolated) |
|---|---|---|---|
| Python | `matrix-component` wheel (`sdk-python/`) + `matrix_operator` module | 0.1.0 | `pip install --no-index <wheel>` in a venv |
| JS/TS | `matrix-component` tarball (`sdk/js/`, `npm pack`) + `index.d.ts` | 0.1.0 | `npm install --offline <tgz>`; plain JS needs no compiler |
| Go | module `matrix-component-go` (`sdk/go/`) | 0.1.0 | `replace` to staged dir, `GOPROXY=off` |
| Crystal | shard `matrix-component` (`sdk/crystal/`) | 0.1.0 | copy dir (or scaffold vendors); `shards build` needs no network |
| Elixir | Mix package `:matrix_component` (`sdk/elixir/`) | 0.1.0 | `path:` dep; `mix test` / `mix escript.build` offline |
| C# | project `Matrix.Component` (`sdk/csharp/`, no NuGet packages) | 0.1.0 | copy dir; `dotnet build` restores nothing |
| C | CMake package `matrix-component` (`sdk/c/`, static lib + `matrix-component.pc`) | 0.1.0 | `cmake -B build && cmake --build build` |
| C++ | header `cpp/matrix.hpp` over the C library (same package) | 0.1.0 | include path + link C lib |
| Rust | crates `matrix-component` (+ facade `matrix-runtime`) | 0.1.0 | reference (M8); parity target |

Licensed MIT OR Apache-2.0 (see `LICENSE-MIT`, `LICENSE-APACHE-2.0`);
registry publication per ecosystem is in progress — until then,
distribution is staged directories/tarballs with hashes
(`scripts/package.sh`).

## Runtimes tested (minimums enforced where the toolchain allows)

| Language | Tested on | Minimum declared |
|---|---|---|
| Python | 3.14.7, Linux x86_64 | >= 3.10 (wheel metadata) |
| JS/TS | Node 26.8.1; `index.d.ts` checked with TS 5.6 `--strict` | Node >= 20 (`engines`); TS types advisory |
| Go | 1.27.1, linux/amd64 | >= 1.24 (`go.mod`) |
| Crystal | 1.21.0, linux x86_64 | >= 1.0, < 2.0 (`shard.yml`) |
| Elixir | 1.20.4 / OTP 29, linux x86_64 | Elixir >= 1.17, OTP >= 27 (`:json` is stdlib) |
| C# | .NET SDK 10.0.400, linux x86_64 | net10.0 (`TargetFramework`) |
| C | GCC 16.2.1, linux x86_64 | C11 + POSIX (pthreads); GCC >= 11 / Clang >= 13 expected |
| C++ | GCC 16.2.1, linux x86_64 | C++17 |
| Rust | 1.98.1 (M8 baseline) | reference |

Only Linux x86_64 is tested. Browser/Deno/Bun (JS), Windows/macOS,
and WASM are not claimed. The managed profile itself needs Linux +
`bwrap` for sandboxed components (`trusted` used by the harnesses).

## Integration mode (all languages)

- Default: local managed service. The application starts its own
  kernel (`start`, owns the daemon) or attaches to a shared one
  (`connect`, closing never stops it). Operator calls travel through
  the staged `matrix-managed` binary (`serve`/`request`, mutual TLS):
  no SDK reimplements remote mTLS, and no SDK touches kernel
  internals. Bootstrap failure reaps only what it created.
- The managed service launches one node process per logical id; the
  node SDK speaks `matrix.component/0.1` over the instance socket
  with `MATRIX_LAUNCH_TOKEN`.
- Remote profile (`matrix.remote/0.1`): same business code, routes
  configured operator-side. SDKs without their own remote-network
  implementation are exercised through it (L10): every new SDK proves
  at least one remote leg as provider or consumer.

## Common semantics (per language)

| # | Rule | Python | JS/TS | Go | Crystal | Elixir | C# | C | C++ |
|---|---|---|---|---|---|---|---|---|---|
| 1 | Authority: caller from session, dep from activation binding; request ≠ grant; no fallback | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 2 | Lifecycle: explicit idempotent close reports pendings; GC/finalizer/destructor is convenience only | ✓ | ✓ | ✓ | ✓ | ✓ (linked reader dies with owner) | ✓ (`DisposeAsync`) | ✓ (single-shot, NULL-safe) | ✓ (noexcept dtor + explicit `close`) |
| 3 | Cancel: inherits context deadline, revokes new actions, tells wait/execution/unknown apart; timeout never retries | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 4 | Reader/control independent of handlers; bounded lanes; exact identity to delivery | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 5 | u64 without precision loss; interoperable null/bool/number/error; no silent lossy bytes | ✓ | ✓ (`BigInt`) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 6 | Streams: explicit owner/operation, credit/backpressure, terminals, cancel; events best-effort, losses counted | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 7 | Recovery: reconnect negotiates + reconciles; new generation adopts nothing old; unknown stays unknown; query never re-executes | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 8 | Diagnosis: structured code + phase + safe correlation; actionable missing-binary/version/permission/feature errors | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (shared doctor) |

Documented idiomatic differences (same guarantee):

- Reader-thread guard: Rust/Python refuse blocking calls on the
  reader thread (`internal`). JS has no reader thread (event loop;
  handlers must still never busy-block). Go handlers always run off
  the read loop (no guard needed). C#/Crystal/Elixir/C/C++ run
  handlers off the reader by construction (C enforces with
  `MX_ERR_THREAD`).
- Cancel observation: `threading.Event` / `AbortSignal` /
  `context.Context` / `Atomic(Bool)` fibers / `{:matrix_cancel}`
  mailbox + `cancelled?/1` / `CancellationToken` / `mx_cancelled()` /
  same as C.
- Elixir supervision: the reader links to the connecting process, so
  an owner crash takes the session down (no orphan context); the SDK
  never auto-restarts into an old generation.
- Elixir node config travels via `:persistent_term` (one node per
  VM/escript process).
- Elixir process lifecycle uses OS signals via the `kill` binary
  (Linux); `Port.close/1` alone never signals — documented in code.
- Crystal `Process#wait` closes stdio pipes: the SDK drains them
  concurrently before reaping.
- Elixir `:json.encode` returns iodata: flattened before framing/argv.
- C/C++ allocators never cross module boundaries implicitly
  (`mx_free`, header documents ownership per function).
- C# `Client.Request` passes the JSON as one argv element (no shell).
- Doctor report keys are idiomatic per language (JS `cliShapeOk`,
  others `cli_shape_ok`); the harness accepts both spellings.

## Streams matrix (M7 limits apply to all)

- Text payloads only, everywhere. Binary input is refused explicitly
  (`invalid-message` family), never corrupted — proven per SDK by a
  dedicated test.
- Edge queues bounded (64, drop-oldest, counted); `pending` pressure
  observable; credit stays host-managed.
- `chain_with_streams` (concurrent chain + streams on one session,
  M7 bidi legs) is implemented by every node and proven live over a
  remote leg (Crystal consumer → Elixir provider) plus the M7 suite;
  the single-leg association limit of `matrix.remote/0.1` still
  applies (see `docs/M7-COMPOSITION.md`).
- Streams bind to the in-flight operation leg: chunks sent while no
  leg is in flight sink locally (M7 single-leg rule, verified live:
  a fast-echo leg completes before the first chunk and delivers
  nothing; a slow leg delivers all chunks). Nodes that stream
  concurrently must keep the operation running (e.g. slow provider
  or longer prime), like the reference fixtures.
- No SDK presents concurrent generic binary streaming beyond what the
  kernel offers.

## Conformance pairs (L09)

Mandatory cross-language compositions, all with real components
(provider/consumer/independent roles per `docs/ML1-NODE.md`):

- Python ↔ JS/TS (both directions; JS also runs without a TS
  compiler as a distinct entry, typed TS as another)
- Go ↔ C#
- Crystal ↔ Elixir
- C ↔ C++
- Rust reference in the three-language chain and in the remote leg
- One chain mixing three languages (consumer → mid → provider in
  three different SDKs)
- Remote profile with SDKs that implement no remote networking of
  their own (all new SDKs: the remote leg is operator-configured)

Fixtures are controlled (`prov`/`cons`/`indep` nodes); pair coverage
is pairwise-per-mandate, not quadratic. `scripts/harness-ml1.sh` is
the recipe; `docs/ML1-COMPOSITION.md` records results.

## What is out of scope (epic)

C ABI for kernel embedding (the C SDK is an IPC client; C++ layers
over it), browsers, new platforms, business plugins, model
integrations, marketplace, performance promises (M9 measures).
`matrix-conform` wire vectors are reused unchanged; the common SDK
contract is proven by identical node behavior, not by new vectors.
