# Roadmap e primeiro trabalho

Status: plano proposto em 2026-09-05. M0 documental concluído nesta revisão; demais marcos não iniciados por esta revisão. Sem estimativas de prazo antes de executar M1.

## M0 — Contrato revisável

Entregas: documentos de arquitetura, invariantes, lifecycle, protocolo, segurança, remoto, persistência e aceitação; histórico anterior preservado e estado atual explicitado. Critério: cobertura das áreas, links íntegros e separação entre proposta e implementação.

## M1 — Composição local e propriedade real

**Este é o ponto de partida da implementação.** Não começar pelo codec, pela rede ou pelo loop de agente.

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

## M6 — Code agent composto

Uma primeira aplicação local pode começar após M3, declarando recuperação limitada até M4. Demonstração final usa modelo real, workspace privado, ferramentas e UI como plugins. Editar, testar na revisão correta e apresentar alteração aplicável. Integração de executor remoto depende de M5.

## Adiados deliberadamente

Federação/múltiplos kernels ativos, transferência arbitrária de ownership, dependências cíclicas, hot swap sem pausa, memória compartilhada, ABI nativa dinâmica universal e promessa de exactly-once externo. Cada extensão exige novos invariantes/testes antes de entrar no contrato.
