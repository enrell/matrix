# Roadmap e primeiro trabalho

Status atualizado: M1 e M2.5 implementados. M3–M5 implementados no [perfil gerenciado](MANAGED-RUNTIME.md), com limites explícitos (sandbox sem subprocessos, recuperação de ledger e remoto unary). O detalhamento abaixo preserva os critérios de projeto; extensões fora desses perfis permanecem abertas.

## M0 — Contrato revisável

Entregas: documentos de arquitetura, invariantes, lifecycle, protocolo, segurança, remoto, persistência e aceitação; histórico anterior preservado e estado atual explicitado. Critério: cobertura das áreas, links íntegros e separação entre proposta e implementação.

## M1 — Composição local e propriedade real

Marco local concluído na base M2.5. Seus testes continuam no gate de regressão.

Escopo: componentes Rust confiáveis em um único processo; sem alegação de isolamento de código malicioso. Contratos já carregam identidade e geração para futura extensão.

| Incremento | Trabalho | Aceitação |
|---|---|---|
| M1.1 | Introduzir InstanceId/ContextId/generation, tabela de recursos e descarte idempotente | C01–C03 |
| M1.2 | Registrar requisitos, bindings, Waiting/Preparing/Active e rejeitar ciclos/ambiguidade | C04–C05 |
| M1.3 | Admissão por ticket, revogação, cancelamento e commits mediados | C06, C08–C09 no perfil confiável |
| M1.4 | Substituição coordenada, inventário e diagnóstico de cleanup | C07, C19 |

Primeiros alvos de código: `matrix-core` recebe módulos de contexto/recursos e grafo de dependências; registry deixa de ser global por string; kernel delega lifecycle ao coordenador. Preservar a CLI legacy por adaptador quando possível; não fingir que seus campos antigos têm a nova semântica.

### Demonstração de saída

Três componentes: provedor `workspace`, consumidor `search` e independente `echo`. Search só ativa com workspace, adquire inscrição/timer/task e recebe uma chamada bloqueada num ponto controlado pelo teste.

Retirar workspace durante a chamada deve impedir novas admissões, retirar search, cancelar trabalho gerenciado, limpar recursos e rejeitar commit tardio. Echo continua respondendo. Reintroduzir workspace cria novas instâncias/bindings e search volta a ativar. O inventário não contém recursos da geração retirada. Se limpeza falhar, estado é CleanupPending, nunca falso Disposed.

O demo usa recursos reais onde aplicável e interleavings reproduzíveis; não se limita a comparar contadores fictícios.

## M2 — Protocolo e plugins em processos

Depende de M1. Entregar schemas de mensagens, framing, sessões, quotas, erros e SDKs Rust/Python. Processo é ainda classificado conforme isolamento realmente disponível. Critério: C12–C14, C24 e reexecução dos invariantes locais aplicáveis pela fronteira IPC. Não exigir rede para provar independência de linguagem.

## M3 — Isolamento, autorização e supervisão

Depende de M2. Host local administra recursos e descendentes; grants e limites são impostos fora do plugin. Critério: C10–C11 e C15. Só então aceitar plugins não confiáveis no perfil validado. WASM é subprojeto opcional, não bloqueia o perfil de processo.

## M4 — Recuperação durável

Depende de M1–M3 para saber o que persistir. Escolher armazenamento e semântica de ack; implementar reconciliação, snapshot e retenção. Critério: C16–C18 e nenhuma repetição implícita de efeitos externos desconhecidos.

## M5 — Hosts remotos

Depende de M2–M4. Transporte autenticado, identidade operacional, autorizações temporárias, fencing e reconciliação. Critério: C20–C23 e contratos equivalentes onde possível; diferenças remotas documentadas. Limites de relógio/atraso resolvidos antes de prometer expiração rígida.

## Próxima sequência — M5.1 a M9

O [plano detalhado das próximas fases](NEXT-PHASES.md) define incrementos, dependências e critérios de saída. Todas as fases abaixo estão propostas, não implementadas.

| Marco | Resultado |
|---|---|
| M5.1 | Auditoria de conformidade, encerramento dos SDKs, quotas e recuperação |
| M6 | Composição genérica entre processos: chamadas, recursos, eventos e retirada |
| M7 | Composição remota: streams, controle de fluxo, reconexão e autoridade |
| M8 | API pública do kernel, SDKs, conformidade e operação |
| M9 | Otimização com medições equivalentes e extensões justificadas |

M5.1 e M6 concluídos ([auditoria](NEXT-PHASES.md), [entrega](M6-COMPOSITION.md)); próximo: M7. Matrix é exclusivamente o kernel genérico; aplicações vivem em repositórios separados. Fixtures mínimas validam composição sem exigir uma aplicação de produto.

## Adiados deliberadamente

Federação/múltiplos kernels ativos, transferência arbitrária de ownership, dependências cíclicas, hot swap sem pausa, memória compartilhada, ABI nativa dinâmica universal e promessa de exactly-once externo. Cada extensão exige novos invariantes/testes antes de entrar no contrato.
