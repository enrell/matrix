> Documento histórico do scaffold, arquivado em 2026-09-05. Não é a especificação vigente. Afirmações de garantias e benchmarks devem ser verificadas no projeto de origem.

# Matrix — AI Agent Engine (infra, não um agent)

Infra para construir code agents (TUI, SDK, daemon, desktop) sobre a melhor
engine medida no `agentlab` BPC: **master3-rt / síntese v3, FINAL 0.812**
(`agentlab/analysis/EVAL.md:17`, `agentlab/master3/docs/REPORT.md`).

Núcleo herdado (conceitos, reescritos — sem copiar crate):

* D-core: reducers puros + journal write-ahead + replay + tx + dry-run
* Tier B-processo p/ `untrusted-heavy` (segv 1.4–2.7ms, bomb delta 0KB)
* Tier A-wasm opt-in (`--features wasm`, epoch-trap p/ loop)
* OTP `{intensity 5, period 10s} x {permanent,transient,temporary}`
* Reload generacional único (S6 100/100/0), leases unilaterais,
  envelopes versionados `"v":1` (compat S10/S15 pass)

## Layout

```text
crates/matrix-core  # kernel lib: fsm/registry/journal/bus/kernel/leases
crates/matrix-rt    # daemon + CLI (UDS run/matrix-rt.sock)
crates/matrix-sdk   # client lib + traits Agent/Tool/Model + ReAct loop
crates/matrix-tui   # REPL/TUI stub (ratatui entra aqui depois)
plugins/            # manifests (ancient congelado)
docs/ARCHITECTURE.md
```

## Quickstart (≤5 min)

```sh
cargo build --release
./target/release/matrix-rt run --json &
sleep 0.5
./target/release/matrix-rt status --json
./target/release/matrix-rt invoke 'echo.msg@1' '{"ping":true}' --json
./target/release/matrix-rt invoke 'ancient.api@1' '{"in":41}' --json
./target/release/matrix-rt emit sys.tick '{}' --json
./target/release/matrix-rt invoke 'count.state@1' '{}' --json
./target/release/matrix-rt reload --json
./target/release/matrix-rt journal --tail 3
./target/release/matrix-rt quit --json

cargo test -- --test-threads=1
make compat
```

Verbos (SPEC §1, cf. `master3/src/main.rs:158`): 
`run|status|invoke|emit|reload|journal|reset|quit`.

Docs: `CHARTER.md`, `PLUGIN.md`, `docs/ARCHITECTURE.md`.
Proveniência: `agentlab/master3/{CHARTER,docs/DESIGN,docs/REPORT}` +
`agentlab/analysis/{EVAL.md,properties.json}`.
