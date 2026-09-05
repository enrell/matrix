# Estado observado do Matrix

Inspeção de código em 2026-09-05. Nenhum teste de runtime foi executado nesta revisão documental. Os testes presentes no repositório não provam todas as propriedades abaixo.

| Área | Implementação observada | Lacuna |
|---|---|---|
| Daemon | Socket Unix, JSON por linha, thread por conexão | Sem protocolo novo negociado, limites e autorização completos |
| Registry | Mapa global de capacidade para id de plugin | Sem dependências reativas, escopos ou seleção contratada de provedor |
| Plugins | Manifest seleciona reducer compilado | Sem carregamento de código externo |
| Contextos | Estado e contador de geração por plugin | Sem propriedade efetiva de operações e recursos |
| Leases | Tipo e rótulo; vetor limpo no dispose | Nenhum handle real ou mecanismo de cleanup associado |
| Eventos | Inscrições e entrega síncrona específica para counter/clock | `Bus::publish` vazio; não há handlers genéricos |
| Remoção | Revoga capacidades e inscrições por id | Sem espera/cancelamento de chamadas e limpeza geral |
| Reload | Relê manifests; preserva counter/clock | Não revoga registros antigos, não remove manifests desaparecidos, sem troca transacional |
| Falhas | Panic injetado é capturado; mais de 5 falhas em 10s marca Failed | Não é contenção geral de panic, segfault, loops ou exaustão |
| Supervisão | Campo restart e contador de falhas | Políticas declaradas não são um supervisor completo |
| Journal | JSONL, sequência, leitura e sync explícito | Erros de escrita ignorados; batch-fsync não implementado |
| Replay | Reconstrói ticks especificamente no plugin counter | Sem recuperação geral de contextos, efeitos ou execuções |
| Undo/dry-run | Campo undo; invoke omite um registro em dry-run | Sem rollback; dry-run não é simulação segura do sistema |
| Isolamento | tier/trust são metadados | Processo e WASM ainda não implementados |
| SDK/agente | Model/Tool, CannedModel, duas chamadas de ferramenta em sequência fixa | Sem LLM real, planejamento ou execução completa de código |
| TUI | REPL | Sem interface ratatui |

## Evidência no checkout

- [Kernel e dispatch](../crates/matrix-core/src/kernel.rs)
- [Leases](../crates/matrix-core/src/leases.rs), [registry](../crates/matrix-core/src/registry.rs), [bus](../crates/matrix-core/src/bus.rs)
- [Journal](../crates/matrix-core/src/journal.rs), [daemon](../crates/matrix-rt/src/main.rs)
- [SDK](../crates/matrix-sdk/src/lib.rs), [testes existentes](../crates/matrix-core/tests/kernel.rs)

Há seis testes de kernel no arquivo inspecionado: echo, ancient, contador, preservação no reload, panic injetado e capacidade ausente. Não exercitam composição de dependências, cleanup real, rede ou recuperação geral. Benchmarks do master3 são antecedentes históricos, não medições deste checkout.
