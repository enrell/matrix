# ARCHITECTURE — Matrix (derivação auditável de master3)

Engine base: **master3-rt síntese v3** — FINAL 0.812, 1º no ranking v2
(`agentlab/analysis/EVAL.md:17`), lean ~1MB, boot ~51ms, S7 4/4 contido,
S9 fidelity 1.0, S10/S15 compat pass, S11 leak-free
(`agentlab/master3/docs/REPORT.md:37-42`).

```text
TUI/SDK/desktop/CLI ──UDS (run/matrix-rt.sock, {"v":1,...})──▶ matrix-rt
  invoke/emit/status/reload/reset/quit                         │
                                                               ▼
                                   ┌─────────────────────────────────┐
                                   │ matrix-core::Kernel             │
                                   │ registry │ FSM │ leases │ OTP   │
                                   │ trust router │ generations │ tx │
                                   └───────┬─────────────────────────┘
                                           │ in-proc dispatch (default)
                                           │ 6 reducers + noop
                                           │ (process/wasm tiers: roadmap,
                                           │  contrato tier/trust reservado)
                                           ▼
                                     journal run/journal.jsonl
                                     seq-ordered, replay=fold
```

Decisões (prova em `agentlab/analysis/properties.json` + `master3/docs/DESIGN.md`):

1. Default in-proc + blocking accept (ban poll-sleep).
2. `untrusted-heavy` → tier processo (roadmap: fork+RLIMIT_AS; hoje contido
   sem matar o daemon, mesma semântica observável p/ S7).
3. `loop` → wasm epoch-trap (roadmap `--features wasm`; hoje watchdog).
4. OTP {5,10s}; reload generacional com adoção de estado; journal único;
   envelopes `v`; leases unilaterais.

O que o score não mede e o scaffold já prepara: SDK (`Model`/`Tool`/`Agent`
+ ReAct 5-passos), TUI/CLI/desktop no mesmo protocolo, ancient congelado
com gate `make compat`.
