> Documento histórico do scaffold, arquivado em 2026-09-05. Não é a especificação vigente. Afirmações de garantias e benchmarks devem ser verificadas no projeto de origem.

# PLUGIN.md — author a Matrix plugin in 5 minutes

Manifest + reducer puro compilado no daemon (`matrix-core::kernel::reducers`).
Sem SDK/codegen no tier default.

## Minimal manifest (`plugins/<id>.json`)

```json
{
  "id": "greeter",
  "version": "1.0.0",
  "capabilities": ["greet.hello@1"],
  "subscriptions": ["sys.tick"],
  "reducer": "echo",
  "init_state": {},
  "tier": "inproc",
  "trust": "trusted",
  "restart": "permanent"
}
```

| field | meaning |
|---|---|
| `capabilities` | `ns.name@MAJOR` que você provê; `invoke <id> <json>` |
| `subscriptions` | tópicos (`sys.tick`, …); at-most-once cada |
| `reducer` | chave compilada (`clock`, `echo`, `counter`, `crasher`, `ancient`, `model`, `noop`) |
| `tier` | `inproc` (default) \| `process` (untrusted-heavy, roadmap: spawn real) |
| `trust` | `trusted` → in-proc; `untrusted-heavy` → tier processo |
| `restart` | `permanent` / `transient` / `temporary` (OTP: >5/10s → Failed) |

## Reducer contract

`fn(&state, &event) -> (new_state, effects)`: puro — efeitos como dados;
o kernel valida → deriva undo → journaliza **antes** de executar.
Panics são contidas (OTP restart); loop infinito in-proc = marque
`untrusted-heavy`.

## Failure vocabulary (`code` em todo `ok:false`)

| code | meaning | first check |
|---|---|---|
| `no-such-capability` | sem provedor | `status --json` |
| `plugin-not-loaded` | id desconhecido | manifest em `plugins/` |
| `plugin-not-active` | fiber Failed/Disposed | journal `sys.fault`; `reload` |
| `reducer-missing` | `reducer` ruim | tabela em `kernel.rs` |
| `plugin-panicked` | panic contido | journal `sys.fault` |

## Check

```sh
./target/release/matrix-rt reload --json
./target/release/matrix-rt status --json
./target/release/matrix-rt invoke 'greet.hello@1' '{"ping":true}' --json
./target/release/matrix-rt journal --tail 3
```
