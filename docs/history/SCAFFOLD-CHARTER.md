> Documento histórico do scaffold, arquivado em 2026-09-05. Não é a especificação vigente. Afirmações de garantias e benchmarks devem ser verificadas no projeto de origem.

# CHARTER — Matrix engine (FINAL v1)

## Tese

Núcleo D (reducers + journal write-ahead + replay + tx + dry-run) com duas
tiers de isolamento (B-processo, A-wasm opt-in) atrás de roteamento por
confiança, sob política OTP, reload generacional, leases e envelopes
versionados — i.e. a síntese v3 validada (`master3-rt`, FINAL 0.812).

## Decisões travadas (evidência agentlab entre parênteses)

1. **Default tier = in-proc reducers** (D: +1.3MB@1000; master3 S2 ~5MB@1000,
   ativação µs) com **blocking accept** (ban poll-sleep; D pagou 10× no S3).
2. **Tier processo** p/ untrusted-heavy/syscall (B: segv 2.7ms, bomb 0KB;
   master3 fork+nocore 1.4ms).
3. **Tier wasm opt-in** p/ untrusted-light (A: traps, epoch 40–45ms@5 ticks,
   33KB/inst; lean default sem wasmtime: ~1MB, build segundos).
4. **Roteamento por confiança** no manifest; crasher com variantes por tier.
5. **OTP** {intensity 5, period 10s}x{permanent,transient,temporary}x
   {one_for_one,rest_for_one} (C: panic-restart ~2.7ms).
6. **Reload generacional único** p/ todas tiers (swap estilo A; S6 100/100).
7. **Journal único** (buffered default + batch-fsync opt-in; snapshot+prune
   no roadmap) — fan-in das tiers.
8. **Envelopes versionados** cross-tier (`v`-field estilo B) + ancient próprio
   congelado (hash + gate `make compat`).
9. **Proibido herdar**: accept poll-sleep, journal sem prune, restart sem
   intensity/period.

## O que o Matrix adiciona sobre master3

* `matrix-sdk`: traits `Model`/`Tool`/`Agent` + loop ReAct + `MatrixClient`
  (UDS) — base p/ qualquer code agent.
* `matrix-tui`: REPL agora, ratatui depois — mesmo protocolo, mesma PID.
* `matrix-rt`: daemon+CLI estáveis (verbo = contrato; desktop usa o mesmo).
* Roadmap: snapshot+prune, batch-fsync durável, wasm tier (`--features wasm`),
  bindings SDK (TS/Python via JSON/UDS primeiro, FFI depois).

## Critério de vitória

Paridade com master3 no BPC neutro (S1–S17 verdes, S9 fidelity 1.0,
S10/S15 pass, S11 leak-free) + demos 1-3 do agentlab portadas e PASS.
