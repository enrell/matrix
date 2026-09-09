# Conformance suite (ML1 + managed runtime)

Status: **experimental** (`0.1.0-experimental`, cf. `docs/VERSIONS.md`). Normative behavior:
`docs/CONTRACT.md` invariants **I01–I12**, wire `docs/PROTOCOL.md`, lifecycle `docs/LIFECYCLE.md`.
Business roles/node CLI: `docs/ML1-NODE.md`; per-language rules: `docs/ML1-MATRIX.md`;
composition record: `docs/ML1-COMPOSITION.md`; operator: `docs/MANAGED-RUNTIME.md`.

## 1. Feature matrix

`✓` proven live · `≈` proven with documented idiomatic difference · `—` gap (see §4).
One line of expected behavior per row; all SDKs: `0.1.0`, Linux x86_64, `trusted` profile.

| Feature | Expected behavior | python | js | go | crystal | elixir | csharp | c | c++ | rust |
|---|---|---|---|---|---|---|---|---|---|---|
| Smoke / bootstrap | `start` owns daemon, `connect` attaches, close reaps only owned | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Capability registration | Provision+activate exposes `prov.api@1`; unprovisioned invoke refused | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Invocation | `echo` round-trips input JSON, answers `{"echo","via"}` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Serialization / u64 | u64 max crosses wire exactly (decimal string/BigInt), no precision loss | ✓ | ≈ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Text-only payloads | Non-text bytes refused explicitly (`invalid-message`), never corrupted | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Nested chain | `chain:true` consumer→provider answers `{"chained","via"}` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Requires / grants | `outbound_grants` authorizes dep call; absent entry → `permission-denied` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Unauthorized call | Ungranted cap / dead lease / stale generation denied with stable code | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Lease acquire/release | `acquire` mints handle, `release` frees; double-release is business error | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Cleanup success | Shutdown disposes all handles; report lists zero pending (I01/I07) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Cleanup exception | Handler error still runs reverse-order cleanup; pending → `CleanupPending` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Op-id uniqueness/replay | Fresh UUID per probe; identical re-emit returns recorded terminal, never re-executes | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Lifecycle startup/shutdown | SIGTERM/SIGINT retires leases, ends group, reports open sessions | ✓ | ✓ | ✓ | ✓ | ≈ | ✓ | ✓ | ✓ | ✓ |
| Invalid config | Whole config rejected pre-mutate, state preserved, diagnostic on stderr | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Invalid CLI / doctor | Bad flags exit 2; `doctor` reports `cli_shape_ok` vs binary | ✓ | ≈ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Remote errors | Partition → `unknown` (never replay); reconnect = new generation, old refs dead | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Concurrent calls | N parallel invokes all settle; bounded lanes, exact identity delivery | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Cancellation / timeout | Deadline aborts sleep/stream (`cancelled`); timeout never blind-retries | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Caller crash | Caller SIGKILL mid-call → callee observes abort, no orphan lease/op | — | — | — | — | — | — | — | — | — |
| Callee crash | Provider SIGKILL mid-`sleep_ms` → terminal non-ok, op never ghost-succeeds | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Orphan detection | Post-run `pgrep matrix-managed` empty; no leases/childs/sockets/ops orphaned | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Interop pairs + chain3 | Mandated pairs both directions + 3-language chain green | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Clean install | Offline staged packs only (`--no-index`/`--offline`/`GOPROXY=off`), hashes verify | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Fault injection | Kill-loop provider, malformed frame, revoked-mid-flight grant all denied/counted | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Property / fuzz | Randomized grammar + shrink over frames/ops with determinism seed | — | — | — | — | — | — | — | — | — |

JS `≈`: `BigInt`/string for u64, `cliShapeOk` camel key, no reader thread (event loop).
Elixir `≈`: linked reader dies with owner; config via `:persistent_term`; lifecycle via `kill` binary.
C/C++ share one CMake package (`matrix.hpp` RAII over C transport); C++ doctor reuses C doctor.

## 2. Harness architecture

- Layout: `tests/conformance/{spec,harness,fixtures,scenarios,interoperability,failures}`.
- Reuse: staged `matrix-managed` binary + per-language `mx-node` (`docs/ML1-NODE.md` CLI:
  `--matrix-sock/--id/--event-log/--stream-log/--stream-slow-ms`, exit 0/2).
- Spec-defined-once: each C-scenario states GIVEN/WHEN/THEN/INVARIANTS/CLEANUP; per-language
  adapter only maps entrypoint (`ENTRY_<lang>`), argv, and doctor-key spelling.
- CLEANUP invariant (every scenario): no leases/children/sockets/ops orphaned
  (`pgrep` empty, inspect shows zero pending); bootstrap failure reaps only what it created.
- Hygiene: `no-secrets` grep over full harness log (no lease token / PEM / fingerprint-secret).
- Op ids: fresh UUID per state-changing probe; identical re-emit reserved for the replay test.
- Offline: all installs from staged packs, no network (`--offline`, `GOPROXY=off`, venv `--no-index`).

## 3. Scenarios C01–C28

**C01 smoke/bootstrap**
GIVEN staged packs+binary WHEN start own daemon, attach second client, close both THEN both serve, owner reaped, attacher leaves daemon alive INVARIANTS I02,I12 CLEANUP kill owned, pgrep empty
**C02 capability registration**
GIVEN provisioned `prov` WHEN activate then invoke `prov.api@1` THEN ok with `via:prov` INVARIANTS I04,I12 CLEANUP release+remove, pgrep empty
**C03 basic invocation**
GIVEN active `prov` WHEN invoke echo `{"ping":true}` THEN `{"echo":{"ping":true}}` INVARIANTS I02,I12 CLEANUP release lease, pgrep empty
**C04 serialization/u64**
GIVEN active `prov` WHEN invoke with u64-max as decimal string THEN exact round-trip, no precision loss INVARIANTS I09 CLEANUP release lease, pgrep empty
**C05 text-only**
GIVEN active `prov` WHEN asked to emit non-text bytes THEN explicit `invalid-message` refusal, no corruption INVARIANTS I09 CLEANUP release lease, pgrep empty
**C06 nested chain**
GIVEN `cons→prov` grant WHEN invoke `cons` with `chain:true` THEN `{"chained":{"echo":…}}` INVARIANTS I04,I10 CLEANUP release both, pgrep empty
**C07 requires/grants**
GIVEN `cons` with/without `outbound_grants` WHEN chain THEN ok iff grant present, else `permission-denied` INVARIANTS I04 CLEANUP release, pgrep empty
**C08 unauthorized**
GIVEN dead lease / ungranted cap / stale generation WHEN invoke THEN stable denial code, no execution INVARIANTS I02,I03 CLEANUP release, pgrep empty
**C09 lease acquire/release**
GIVEN active session WHEN `acquire` then `release` handle THEN minted then freed; double-release errors INVARIANTS I01,I05 CLEANUP release remainder, pgrep empty
**C10 cleanup success**
GIVEN handles+deps live WHEN orderly shutdown THEN zero pending in report INVARIANTS I01,I07 CLEANUP assert pgrep empty, homes removed
**C11 cleanup exception**
GIVEN handler fails mid-acquire WHEN dispose THEN reverse-order cleanup, residue → `CleanupPending` INVARIANTS I05,I06,I07 CLEANUP force-release, pgrep empty
**C12 op-id replay**
GIVEN completed op `O` WHEN re-emit identical id/payload THEN recorded terminal; divergent payload → error INVARIANTS I10 CLEANUP release, pgrep empty
**C13 lifecycle startup/shutdown**
GIVEN running service WHEN SIGTERM THEN leases retired, group reaped, open sessions reported INVARIANTS I01,I08 CLEANUP pgrep empty, homes removed
**C14 invalid config**
GIVEN bad config (unknown field/bad TLS) WHEN `serve`/SIGHUP THEN whole change rejected, state preserved INVARIANTS I02 CLEANUP daemon on old config, pgrep checked
**C15 invalid CLI/doctor**
GIVEN staged binary WHEN bad flags / `doctor --binary` THEN exit 2 resp. `cli_shape_ok:true` JSON INVARIANTS I12 CLEANUP no daemon started, temp dirs removed
**C16 remote errors**
GIVEN remote route WHEN partition THEN `unknown`; restart peer with fresh homes THEN new generation serves INVARIANTS I03,I07,I10 CLEANUP stop both peers, pgrep empty
**C17 concurrent**
GIVEN active `prov` WHEN N parallel invokes THEN all settle with exact identity, lanes bounded INVARIANTS I09,I12 CLEANUP release, pgrep empty
**C18 cancellation/timeout**
GIVEN `sleep_ms`/stream call WHEN deadline fires THEN `cancelled`, no retry; unknown stays query-only INVARIANTS I10 CLEANUP release, pgrep empty
**C19 caller crash [GAP]**
GIVEN in-flight call WHEN caller SIGKILL THEN callee aborts, lease/op reclaimed, no ghost success INVARIANTS I01,I08 CLEANUP reap callee, pgrep empty
**C20 callee crash**
GIVEN `sleep_ms` call WHEN provider SIGKILL THEN terminal non-ok, same op never ghost-succeeds INVARIANTS I08,I10 CLEANUP reap+restart supervised, pgrep empty
**C21 orphan detection**
GIVEN any scenario WHEN finished THEN no `matrix-managed`/node procs, sockets, leases remain INVARIANTS I01,I07 CLEANUP kill `$PIDS`, `wait`, assert empty
**C22 interop pairs**
GIVEN mandated pair (py↔js, go↔cs, cr↔ex, c↔cpp) WHEN cross-language chain THEN `chained` with provider `via` INVARIANTS I04 CLEANUP release both, pgrep empty
**C23 chain of three**
GIVEN js→go→rs bindings WHEN consumer chains THEN triple-nested `chained` answer INVARIANTS I04 CLEANUP release all, pgrep empty
**C24 clean install**
GIVEN empty container WHEN offline install from staged packs THEN all SDK suites pass, `sha256sum -c` green INVARIANTS — CLEANUP remove proj dirs, pgrep empty
**C25 fault injection**
GIVEN live chain WHEN kill-loop provider / malformed frame / revoke mid-flight THEN counted denials, indep serves INVARIANTS I03,I08,I11 CLEANUP restore grants, pgrep empty
**C26 streams/events**
GIVEN `stream_send` 32×4KiB + `chain_with_streams` WHEN run THEN `stream_sent:32`, drops counted, bidi leg observed INVARIANTS I09 CLEANUP release, pgrep empty
**C27 property/fuzz [GAP]**
GIVEN seeded grammar WHEN randomized frames/ops THEN no crash/corruption; failures shrink to minimal case INVARIANTS I09 CLEANUP release, pgrep empty
**C28 teardown under load [GAP]**
GIVEN ~19 daemons + active probes WHEN SIGHUP reload THEN observable revoke or explicit `partial` report INVARIANTS I02 CLEANUP restart affected daemons, pgrep empty

## 4. Known gaps (not claimed)

- `docs/PLUGIN.md`: stale stream paragraphs predate M7 single-leg rule; conformance follows ML1-NODE/M7.
- `docs/PROTOCOL.md`: stale schema text vs `matrix-proto` vectors; vectors (`matrix-conform vectors`) rule.
- `docs/MANAGED-RUNTIME.md`: stale "gateway is unary / streams stay local (M2)" sentence; M7 bidi legs proven live.
- SIGHUP-under-load suspect: HUP lost under ~19-daemon concurrency, works idle; withdrawal proven by
  sustained-kill instead, reconnect by fresh-home restart (cf. `docs/ML1-COMPOSITION.md` follow-up). C28 open.
- Rust-component SDK has no standalone operator client (facade `matrix-runtime` is the reference path).
- `matrix-sdk`/`matrix-rt` legacy envelope confusion: frozen compat only (`make compat`), never the authority path.
- Missing coverage: caller-crash teardown (C19), teardown-concurrent reload (C28), property/fuzz harness (C27).
- `host.sock` unlink gap (file-only, live socket still fails): SIGTERM `shutdown` never unlinks (`crates/matrix-host/src/lib.rs:516-535`); unlink lives only in `Drop for Inner` (`crates/matrix-host/src/lib.rs:2757-2767`), missed on daemon kill path; `ShutdownReport` covers sessions/leases/pending/routes only, no sock files (`crates/matrix-runtime/src/api.rs:897-924`); harness keeps `ss -xa` live-socket as hard FAIL and dead file as litter (`stale-sock-removed` + `rm`) (`tests/conformance/harness/run.sh:70-73`).

## 5. Interop N×N

Today: pairwise-per-mandate — py↔js, go↔cs, cr↔ex, c↔cpp (both directions where listed),
Rust in both roles, one js→go→rs chain, one Crystal→Elixir remote leg. This is linear in mandates,
not quadratic: full 9×9=81 ordered pairs are NOT run by default. `--full` flag runs all 81
(9 providers × 9 consumers) plus chain3 permutations; CI gates the mandated subset for time.
