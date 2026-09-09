# Kernel Verification — experimental

Status: experimental. Non-goal: line coverage is not correctness. This doc specifies
invariant-first verification of the real kernel; passing suites without invariant
checks proves nothing.

## 2. Normative invariants (FIRST)

Source: `docs/CONTRACT.md:21-34`. One line each, normative:

- I01 (`docs/CONTRACT.md:23`): every managed resource has exactly one live owner or an explicit pending-cleanup record.
- I02 (`docs/CONTRACT.md:24`): no call is admitted in a context that left Active.
- I03 (`docs/CONTRACT.md:25`): stale-generation operations are rejected at the effect-controlling boundary.
- I04 (`docs/CONTRACT.md:26`): an Active instance holds valid bindings for all mandatory dependencies.
- I05 (`docs/CONTRACT.md:27`): acquisition publishes a resource only after recording ownership; partial failure triggers cleanup.
- I06 (`docs/CONTRACT.md:28`): repeated dispose is idempotent and never removes another instance's resources.
- I07 (`docs/CONTRACT.md:29`): Disposed means zero pending managed resources; uncertainty surfaces as CleanupPending.
- I08 (`docs/CONTRACT.md:30`): cleanup never depends solely on plugin cooperation; host guarantees are declared per tier.
- I09 (`docs/CONTRACT.md:31`): queues, messages, streams, resources, calls have finite configured bounds.
- I10 (`docs/CONTRACT.md:32`): unknown outcome is never reclassified as "not executed" without evidence.
- I11 (`docs/CONTRACT.md:33`): rollback of one component preserves independent changes of others.
- I12 (`docs/CONTRACT.md:34`): lifecycle events and errors are attributable to instance, generation, operation.

Linearization rule (normative):

- `claim_dispose` is atomic: exactly one caller observes `Fresh`, losers observe `InFlight`
  (`crates/matrix-core/src/context.rs:220`, `crates/matrix-core/src/kernel.rs:2870-2879`).
- `commit_effect` is the effect boundary: revalidates ticket+generation+state, rejects late attempts AND journals
  `call.rejected` (`crates/matrix-core/src/kernel.rs:1540-1559`, `crates/matrix-core/src/calls.rs:13-15`).
- `settle_ticket` has a single conclusion: `remove` + revoke-descendants/release-reservation once
  (`crates/matrix-core/src/kernel.rs:1719-1743`); terminal accept is single via
  `mark_committed_if_admitted` (`crates/matrix-core/src/calls.rs:365-371`).
- Disposed ⇔ zero-pending: `retire` reports `Disposed` iff kernel outcome ≠ `CleanupPending`
  AND no child running AND `active_resources_for == 0`, else `CleanupPending`
  (`crates/matrix-runtime/src/service.rs:566-577`).

## 3. Real kernel architecture (test-relevant)

- Lifecycle FSM: `Registered → Waiting | Preparing → Active → Quiescing → CleanupPending → Disposed`,
  preparation failure → `Failed` (`crates/matrix-core/src/fsm.rs:2-20`).
  Only `Active` admits calls (`crates/matrix-core/src/fsm.rs:46-48`, `crates/matrix-core/src/context.rs:180-190`).
- Tickets: `Admitted → Committed | Cancelled | Expired` (`crates/matrix-core/src/calls.rs:93-98`).
  `Admitted` holds authority; `reap` refuses `Admitted`, accepts terminal only
  (`crates/matrix-core/src/kernel.rs:1730-1743`).
  Parent end revokes open descendants (`Cancelled` + signal, late commit stays rejected,
  `crates/matrix-core/src/calls.rs:293-302`); expiry marks `Expired` without assuming execution ended
  (`crates/matrix-core/src/kernel.rs:1571-1572`).
- Service leases: live = unrevoked + token/fence match + `now < deadline` + `!failed` + `retry.is_none()`
  (`crates/matrix-runtime/src/service.rs:226-238`); activation issues token+fence+owner+epoch
  (`crates/matrix-runtime/src/service.rs:166-225`); retirement disposes kernel + kills child + audits
  `lease.retired` (`crates/matrix-runtime/src/service.rs:566-577`).
  Derived states: live → renewed (token rotation, `service.rs:272-284`; seq-renew `service.rs:293-335`)
  | retired (explicit release, `service.rs:552-564`) | expired (deadline sweep → `retire`,
  `service.rs:645-656`) | failed (`failed=true` after budget exhaustion, `service.rs:683-688`)
  | revoked (principal revoked, leases drained via `retire`, `service.rs:609-623`).
- Bindings: opaque handle per resolved requirement of the just-`Active` activation
  (`crates/matrix-core/src/kernel.rs:846-848`); id `bind-N` local / `rb-N` remote carries no authority,
  admission revalidates everything, either side reintroduced invalidates it
  (`crates/matrix-core/src/kernel.rs:170-190`).
- Generations/epochs/fences: per-logical monotonic generation +1, fresh ids never reused
  (`crates/matrix-core/src/context.rs:112-122`); boot-fresh epoch (`crates/matrix-core/src/identity.rs:99-100`,
  `crates/matrix-core/src/context.rs:96-97`); full ref `(epoch, instance, context, logical, generation)`
  validated at boundary (`crates/matrix-core/src/identity.rs:53-62`); fence check + effect mutation share one
  SQLite tx so old writers cannot slip between validation and commit
  (`crates/matrix-runtime/src/store.rs:242-271`); `next_fence` monotonic per resource
  (`crates/matrix-runtime/src/store.rs:322-337`).

## 4. State/transition catalog (compact)

FSM (normative states; `Pending/Loading/Unloading` are legacy aliases, `crates/matrix-core/src/fsm.rs:51-58`):

| State | Admits? | Exits to |
|---|---|---|
| `Registered` | no | `Waiting`, `Preparing` |
| `Waiting` | no | `Preparing` (deps satisfied), `Failed` |
| `Preparing` | no | `Active`, `Failed` (provisional cleanup first) |
| `Active` | YES (only) | `Quiescing` (via `claim_dispose`) |
| `Quiescing` | no | `CleanupPending`, `Disposed` |
| `CleanupPending` | no | `Disposed` (cause retained, I12) |
| `Disposed` | no | terminal |
| `Failed` | no | terminal (cause retained) |

Tickets (`crates/matrix-core/src/calls.rs:93-98` + transitions):

| From → To | Trigger | File |
|---|---|---|
| `Admitted → Committed` | `mark_committed_if_admitted` / `commit_effect` single accept | `calls.rs:365-371`, `kernel.rs:1658-1659` |
| `Admitted → Cancelled` | `revoke_descendants`, `set_state`, parent end, withdraw | `calls.rs:293-324`, `kernel.rs:1719-1722` |
| `Admitted → Expired` | `expire_ticket` (deadline/drain) | `calls.rs:384-388`, `kernel.rs:1480-1481` |
| any terminal → removed | `settle_ticket` / `reap` (refuses `Admitted`) | `kernel.rs:1719-1743` |

Leases (`service.rs`): `activate → live` (`:166-225`); `live → renewed` (`:272-335`);
`live → retired` (`:552-577`); `live → expired → retired` (`:645-656`);
`live → failed` (`:683-692`); `any → revoked → retired` (`:609-623`).

## 5. Failure-point catalog

Axes: point ∈ {before/after send, receive, auth, acquire, handler, commit, release} ×
fault ∈ {crash, timeout, cancel, drop, delay, duplicate, reorder, invalid} × class ∈ {deterministic, timing}.

Deterministic (must hold every run): stale-gen at commit boundary rejected+journaled (I03);
double-commit → `already-committed` (`kernel.rs:1545-1546`); `reap` refuses `Admitted` (`kernel.rs:1740-1743`);
dispose idempotent, never cross-instance (`resources.rs:3-7`, `registry.rs:98-100`);
unknown never reclassified (I10; recovery open marks `admitted→unknown`, `api.rs:522-525`).
Timing (flaky by nature; harness must force, not hope): cancel-vs-commit race (single accept decides);
withdraw-vs-admit race (revalidate-after-register, `calls.rs:6-8`); renew-vs-expire (deadline authoritative,
`service.rs:234-235`); kill-vs-restart supervision window (`service.rs:657-692`); SIGHUP under load (open, §6).

## 6. Current-suite gaps

- C19 caller-crash teardown: missing (`CONFORMANCE.md:126`); callee-crash (C20) exists, caller side unproven.
- C27 property/fuzz: `[GAP]` (`CONFORMANCE.md:112-113`); `run-fuzz.sh` exists but is seed-0/count-50 smoke, no shrink gate.
- C28 SIGHUP-under-load: `[GAP]` (`CONFORMANCE.md:114-115`); HUP lost under ~19-daemon concurrency, works idle
  (`CONFORMANCE.md:122-123`).
- Duplicate / reordered / delayed-old-gen frames: no deterministic injector; only live kill-loop coverage.
- Revoke-during-call: partial (kill-loop provider, `CONFORMANCE.md:109`); no systematic revoke × {admit, commit} matrix.
- Fan-out/fan-in, dependency cycles: cycle rejection exists in kernel (`kernel.rs:4-8`) but no adversarial topology suite.
- Cancel-vs-terminal: single-accept unit exists; no concurrent cancel/commit/expiry race harness.
- Soak: no multi-hour lease churn / epoch-advance / backup-restore-under-load run.
- `host.sock` unlink file-only gap (`CONFORMANCE.md:127`): SIGTERM `shutdown` never unlinks
  (`crates/matrix-host/src/lib.rs:516-535`); unlink only in `Drop for Inner`
  (`crates/matrix-host/src/lib.rs:2757-2767`); `ShutdownReport` covers sessions/leases/pending/routes only
  (`crates/matrix-runtime/src/api.rs:897-924`).
- Masking notes (do not copy): `run-nxn.sh:6` — missing toolchain/staged node ⇒ SKIP, never FAIL;
  `run.sh:70-73`, `run-c07.sh`, `run-c20.sh` — `ss -xa` live-socket = hard FAIL, dead file = litter
  (`stale-sock-removed` + `rm`); broad `grep -qiE 'stale|denied|not-active|unknown|expired'` acceptance
  (`run-nxn.sh:81`, `harness-ml1.sh:277`) proves denial happened, not the right denial — verification tests
  must assert exact code + journal event.
- M5.1 R-classification (source of truth: `docs/M5.1-AUDIT.md:59-113` repros, `:115-125` classes, `:180-190` regtests; M5.1 closed `:203-205`):
  | R | Status | Evidence |
  |---|---|---|
  | R1 | FIXED+REGTEST | DEFEITO (`M5.1-AUDIT.md:59-71`) → B1; `crates/matrix-core/tests/m1_5.rs:t1_concurrent_dispose_single_transition` |
  | R2 | FIXED+REGTEST | DEFEITO (`M5.1-AUDIT.md:73-84`) → B2; `m1_5.rs:t2_ephemeral_invoke_settles_after_withdraw` (+ `t3_cleanup_cause_cites_only_live_tickets`) |
  | R3 | CONTRACT (+`call_reap` mechanism) | CONTRATO (`M5.1-AUDIT.md:86-90`) → B4; `m1_5.rs:t4_dead_worker_opener_still_closes` (contract, passed pre-fix) + `t6_owner_reaps_revoked_ticket_of_dead_owner` (`call.reaped`) |
  | R4 | FIXED+REGTEST | DEFEITO (`M5.1-AUDIT.md:92-104`) → B3; `m1_5.rs:t5_acquire_withdraw_never_publishes_dead_instance` |
  | R5 | LIMIT-documented | LIMITE DOCUMENTADO (`M5.1-AUDIT.md:106-113`, class F5 `:123`, backlog `:197-198`) → B5 (`MANAGED-RUNTIME.md` + CLI help); no regtest |
  Note: R3 is labeled CONTRATO in the audit; `call_reap` (t6) is the B4 dead-owner mechanism, not a change to R3's Expired-needs-close rule.

## 7. Reference model design

- Pure data model, no kernel imports: `Fsm × TicketState × LeaseState × GenMap × Journal` as plain enums/maps.
- Op alphabet: `Create, Bind, Admit, Commit, Cancel, Expire, Dispose, Reap, Renew, Revoke, Release, Restart, Tick`.
- First trace (mandatory): `spawn A/B → register → grant → call(A→B) → kill B → restart B → delayed old response`
  must end `Committed≤1`, old-gen commit rejected+journaled, new gen serves.
- Dual execution via public API only: drive the same op trace through (a) `matrix-managed` request surface and
  (b) `mx-node` public component API; compare terminal states + journal kinds, never internals.
- Shrink to 1-minimal: delta-debug failing trace by removing ops while failure persists; report the smallest
  trace + seed.

## 8. Generative harness + snapshot

- Terminals asserted per op: `status` (`ready:true/false` + exact error code), `invoke` (`ok:true` + value or
  exact `code`), shutdown report (`sessions_after==0`, `leases_after==0`, `routes_active` explicit).
- Process/socket truth: `pgrep -f matrix-managed`, `pgrep -f dep_node|mx-node` must be empty;
  `ss -xa` live-socket = FAIL; dead file = litter + remove (per §6 masking rule).
- `wait_ready` polling (200×0.1s, cf. `tests/conformance/harness/run.sh:50-53`) gates readiness, never success.
- One optional read-only hook: `MATRIX_TEST_SNAPSHOT` — justified solely to let an external oracle capture
  `inspect` dumps on failure without touching kernel state (page-level copy, no epoch advance, no state flips,
  cf. `api.rs:837-840`); forbidden in the passing path. It is test-only, out-of-contract, never a stable surface (compiled out / env-gated, forbidden in the passing path).

## 9. Fault injector + concurrency strategy

- Proxy shim (component ↔ kernel socket): deterministic `duplicate | reorder | delay(ms, seed) | drop | corrupt`
  per frame class (`call.open`, `stream.data`, `call.accepted`, renew); default pass-through; every mutation logged
  with `(seq, rule, seed)` for replay.
- 4 linearization points under test: `claim_dispose` (B1), in-flight pin check (B2), terminal accept
  (`mark_committed_if_admitted`), fence-tx commit (`store.rs:242-271`).
- Seeded op ordering: PRNG(seed) interleaves `{admit, commit, cancel, expire, dispose, renew, revoke}`;
  same seed ⇒ same order; failing seed is the bug report's first line.
- Barrier: N workers block on admission gate, released together to force withdraw×admit and cancel×commit races;
  assert single terminal + journal exactly once per ticket.

## 10. Adversarial app catalog (12 apps, 2 lines each)

1. `crasher`: exits mid-handler on Nth call. Proves callee-crash pins `CleanupPending`, never false `Disposed`.
2. `caller-vanisher`: caller dies after `Admit` before `close`. Proves `reap` path settles terminal tickets only.
3. `slow-loris`: sleeps past deadline then commits. Proves late commit rejected + `Expired` journaled.
4. `double-committer`: commits same ticket twice. Proves `already-committed`, effect applied exactly once.
5. `stale-impersonator`: replays old `(instance, generation, fence)`. Proves boundary rejects with `stale-generation`.
6. `cancel-racer`: cancels concurrently with commit. Proves single terminal, loser journaled, no residue.
7. `revoke-gamer`: operator revokes mid-call. Proves in-flight denied, independent leases untouched (I11).
8. `lease-hoarder`: holds 128 leases then one more. Proves `resource-exhausted` bound (I09, `service.rs:182-184`).
9. `fence-forger`: commits with wrong/old fence. Proves store-tx rejects, no partial effect (I11).
10. `dep-cycler`: A→B→C→A requirements. Proves cycle rejection, consumers stay `Waiting` with cause.
11. `fanout-storm`: one call fans to K providers, K-1 die. Proves independent results preserved, failures counted.
12. `sock-squatter`: holds `host.sock` across SIGTERM. Proves live-socket gate FAILs; characterizes file-only gap (§6).

## 11. Fast CI (<5min) / nightly (≤60min) / release (hours) split

| Tier | Budget | Trace depth | Contents |
|---|---|---|---|
| Fast CI | <5min | depth ≤6, seeds ≤5 | I01–I08 unit + model cross-check; first trace (§7); single-accept + double-commit + stale-gen boundary; C01/C07 smoke |
| Nightly | ≤60min | depth ≤20, seeds ~50 | full op-alphabet generative + shrink; proxy-chaos matrix (dup/reorder/delay × all points); revoke-during-call; fan-out/cycles; C20 + fuzz-50 + NxN mandated subset |
| Release | hours | depth ≤100, soak ≥4h | all nightly at 10× seeds; SIGHUP-under-load until C28 closes; lease-churn soak + backup/restore; full 81-pair `--full` NxN; host.sock unlink fix verification |

## 12. Bug protocol

`reproduce → minimize → regression → verify-fail → fix → verify-pass`, never silent:

1. Reproduce: failing seed + trace + exact code/journal, checked in as pending test.
2. Minimize: shrink to 1-minimal trace (§7); attach proxy log `(seq, rule, seed)`.
3. Regression: test asserts invariant (Ixx) + terminal + journal, pinned to the minimal trace.
4. Verify-fail: test FAILs on current code (record output); no fix without this evidence.
5. Fix: source change at the linearization point, never broader.
6. Verify-pass: tier-appropriate suite green; never silent — log-and-remove/SKIP/broad-grep acceptance forbidden
   for this path; exact code + journal required.
