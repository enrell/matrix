# Plano de remoção do código de aplicação

Inventário verificado em 2026-09-06. Status: executado em 2026-09-06 (ver [registro de execução](#registro-de-execução-2026-09-06)). Matrix é exclusivamente o kernel, e aplicações serão desenvolvidas em repositórios separados.

## Origem e limite da remoção

M5 implementa infraestrutura remota em `matrix-runtime`: TLS, identidade, grants, leases, proxies e retirada. Não implementa um code agent. A proposta anterior de agente estava em M6 e foi substituída pelo roadmap genérico.

Código de agente já existente pertence ao scaffold legado: `matrix-sdk` mistura um cliente genérico com abstrações de agente; `matrix-tui` consome ambas. A busca de referências no repositório encontrou a dependência de aplicação em `matrix-tui`, não nos crates do núcleo ou do runtime gerenciado. Isso não comprova ausência de consumidores externos.

## Inventário e destino

| Alvo | Ação planejada | Justificativa |
|---|---|---|
| `crates/matrix-sdk/src/lib.rs`: `Model`, `Tool`, `EchoTool`, `CannedModel`, `Agent` e suas implementações | Remover | Abstrações de aplicação e loop fixo, sem responsabilidade de kernel |
| Mesmo arquivo: `MatrixClient`, resolução do socket, `rpc`, `invoke`, `emit`, `status` | Preservar e documentar como cliente do daemon legado | Interface genérica, independente de domínio; não confundir com SDK de componentes |
| `crates/matrix-tui/` | Remover crate completo | REPL de aplicação e entrada `--agent`; administração já dispõe de CLI no runtime |
| `Cargo.toml` | Retirar apenas `crates/matrix-tui` dos membros | Manter os demais crates, inclusive cliente genérico |
| `Cargo.lock` | Atualizar via Cargo e revisar diff | Eliminar pacote retirado sem atualização ampla de dependências |
| README e documentação corrente | Atualizar mapa de crates e registrar quebra | Não anunciar TUI ou abstrações de agente como API disponível |
| `docs/history/` | Preservar como histórico identificado | Não é código executável nem escopo atual |
| `matrix-core`, `matrix-proto`, `matrix-host`, `matrix-component`, SDK Python | Preservar | Contratos, hosts e SDKs genéricos |
| `matrix-guard`, `matrix-runtime`, `matrix-rt` | Preservar | Isolamento, durabilidade, remoto e administração do kernel |
| Fixtures echo/ancient, testes e scripts de conformidade | Preservar | Exercitam infraestrutura; uma fixture mínima não é uma aplicação de produto |

O campo `Sandbox.workspace` representa diretório privado montado para um processo, não um workspace de code agent. Não removê-lo por associação de nome. Da mesma forma, prefixo temporário `matrix-sdk-...` em teste do host não indica dependência do loop de agente.

## Execução em um lote coerente

1. Registrar diff e estado inicial, preservando alterações existentes. Antes de excluir arquivos, garantir uma cópia recuperável dos alvos com conteúdo atual, inclusive alterações não commitadas; indicar sua localização no relatório. Não criar aplicação ou repositório de destino sem solicitação.
2. Retirar as cinco abstrações de agente do SDK e ajustar sua documentação de módulo. Preservar assinatura e comportamento de `MatrixClient`; manter o nome do crate nesta mudança.
3. Remover o crate TUI e sua entrada no workspace no mesmo lote. Não deixar um consumidor compilando contra APIs removidas.
4. Atualizar lockfile por Cargo e documentação. Registrar que `matrix-tui`, `--agent` e os cinco símbolos Rust deixam de existir, sem shim que mantenha código de aplicação no kernel.
5. Executar verificações e revisar o diff para confirmar que nenhuma implementação genérica M1–M5 foi removida ou alterada incidentalmente.

Não há necessidade de fase longa de depreciação dentro do repositório experimental: a quebra é explícita e delimitada. Consumidores externos dos símbolos removidos precisarão manter seu próprio código de aplicação; compatibilidade do protocolo do daemon e de plugins continua sendo requisito.

## Verificação e critério de conclusão

- `cargo metadata --no-deps`: TUI ausente e grafo de crates genéricos preservado.
- `cargo build --release --workspace`: workspace completo compila.
- `make test`: regressão canônica; comparar testes efetivamente executados com baseline, não apenas a contagem final.
- `make compat`: contrato ancient preservado; conferir sucesso real de manifesto e invocação, além do exit code.
- `scripts/smoke-managed.py`: fluxo gerenciado continua operacional, inclusive remoto/durabilidade que o smoke cobre.
- Smoke do cliente preservado contra daemon temporário: `status`, `invoke` de echo e `emit`, verificando respostas esperadas. Como a TUI será removida, ela não pode ser o único exercício do cliente.
- Busca no código ativo por símbolos removidos, imports, dependência TUI e `--agent`: nenhuma referência executável restante. Menções explicativas no histórico e no plano são permitidas.
- `git diff --check` e revisão do lockfile: nenhuma mudança incidental ou perda de alterações anteriores.

Conclusão: o workspace executável oferece somente infraestrutura genérica, SDKs/clientes, administração e fixtures. Nenhum loop de agente ou trait de modelo/ferramenta permanece na API do kernel. Relatório final identifica arquivos removidos, quebra de API, cópia recuperável e verificações realizadas.

## Registro de execução (2026-09-06)

Arquivos removidos: `crates/matrix-tui/Cargo.toml`, `crates/matrix-tui/src/main.rs` (crate completo). Arquivo reduzido: `crates/matrix-sdk/src/lib.rs` (removidos `Model`, `Tool`, `EchoTool`, `CannedModel`, `Agent`; `MatrixClient` com `rpc`/`invoke`/`emit`/`status` preservado sem alteração). Workspace: `crates/matrix-tui` retirado de `Cargo.toml`; `Cargo.lock` regenerado via Cargo (única remoção: pacote `matrix-tui`; nenhuma atualização ampla de dependências). Documentação: mapa de crates e quebra registrados em `README.md`; `docs/STATUS.md` e `docs/NEXT-PHASES.md` atualizados para remoção concluída.

Quebra de API: `matrix-tui`, `--agent` e os cinco símbolos Rust deixam de existir, sem shim. Consumidores externos desses símbolos precisam manter seu próprio código de aplicação; compatibilidade do protocolo do daemon e de plugins continua sendo requisito.

Cópia recuperável (conteúdo pré-remoção, incluindo estado não commitado): `/tmp/opencode/kernel-cleanup-backup/` (`crates/matrix-sdk/src/lib.rs`, `crates/matrix-tui/src/main.rs`, `crates/matrix-tui/Cargo.toml`, `Cargo.toml`, `git-status-inicial.txt`, `git-head.txt`). Alterações preexistentes no working tree foram preservadas; nenhum commit foi criado.

Preservados por decisão explícita: reducer `model`/`canned-response` em `matrix-core` (fixture mínima de infraestrutura, não aplicação); exemplo conceitual `agent-loop` em `docs/SDK.md` (manifest não executável de componente externo); `Sandbox.workspace` e prefixo `matrix-sdk-…` em teste do host (sem relação com o loop de agente).

Verificações: `cargo metadata --no-deps` (8 crates, TUI ausente); `cargo build --release --workspace` OK; `make test` OK (100 passed, 0 failed; nenhum alvo de teste referenciava os símbolos removidos); `make compat` PASS (manifest + live); `scripts/smoke-managed.py` PASS; smoke do `MatrixClient` contra daemon temporário PASS (`status` com `harness=matrix` e `echo.msg@1`, `invoke` echo com eco, `emit sys.tick` com `ok`); busca por símbolos/imports/`--agent` sem referência executável restante; `git diff --check` limpo.

## Ordem no roadmap

### Limpeza dos planos antigos — revisão de 2026-09-06

A remoção inclui requisitos e exemplos de aplicação na documentação ativa, além do código. `ROADMAP.md` e `NEXT-PHASES.md` são as fontes vigentes do planejamento; planos anteriores de agente não são backlog adiado.

- Já substituídos: fases de workspace, modelo real, loop de agente e execução de código por fases genéricas do kernel em `NEXT-PHASES.md` e `ROADMAP.md`.
- Removida de `SDK.md` a seção que exigia aplicação de agente; manifest conceitual convertido para provedor/consumidor genéricos. Esta revisão substitui a decisão anterior deste relatório de preservar o exemplo `agent-loop`.
- Substituído B04 em `VALIDATION.md`: composição remota genérica com chamadas e streams, sem agente de referência.
- Histórico em `docs/history/` permanece apenas como registro, com revogação explícita de seus planos. Nenhuma tarefa deve ser derivada dele; não é documentação normativa.
- Critério de saída documental: nenhum plano vigente exige desenvolver uma aplicação; links apontam para o roadmap atual. Descrições factuais de código legado, evidências de testes e fixtures genéricas não são removidas por conter palavras como `model` ou `workspace`.

Este lote precede a ampliação de M6 e pode ser executado antes da auditoria M5.1. Não reabre nem remove M5. Uma eventual reorganização ou renomeação de `matrix-sdk` para diferenciar cliente administrativo e SDK de componentes fica em mudança separada, evitando ampliar a quebra desnecessariamente.
