# Próximas fases do kernel

Plano revisado em 2026-09-06. Fases propostas, não implementadas. Base: [estado atual](STATUS.md) e [perfil gerenciado](MANAGED-RUNTIME.md).

## Escopo definitivo

Matrix é exclusivamente um kernel genérico de composição espacial e temporal em Rust. Aplicações vivem em repositórios separados. O objetivo é fornecer uma base reutilizável para aplicações que precisam compor componentes e gerenciar sua evolução durante execução, sem construir seu próprio runtime de composição ou depender de Cordis.

A analogia com Linux define a separação de responsabilidades. Matrix é um runtime em espaço de usuário, apoiado no sistema operacional; não o substitui. Independência de domínio não implica suporte universal a ambientes: garantias permanecem vinculadas aos perfis implementados e testados.

Este repositório contém núcleo semântico, hosts, protocolo, SDKs genéricos, ferramentas administrativas, conformidade, benchmarks e fixtures mínimas. Aplicações externas definem modelos, agentes, workspaces, ferramentas de negócio, memória semântica e interfaces. Nenhuma aplicação específica é condição de conclusão do kernel.

Identidade, autoridade, admissão, ownership e fiscalização permanecem sob controle da base confiável. Extensibilidade não permite a um plugin desativar as regras que o limitam.

## Sequência e critérios de saída

| Fase | Entrega | Dependência | Aceitação |
|---|---|---|---|
| M5.1 | Consolidar lifecycle, quotas e recuperação | M3–M5 atual | **Fechado em 2026-09-06** ([auditoria e tratamento](M5.1-AUDIT.md)): regressão verde, descarte linearizado, assentamento e diagnóstico verificáveis |
| M6 | Composição genérica entre processos | M5.1 | Rust/Python compõem dependências e preservam autoridade durante retirada e substituição |
| M7 | Composição remota com controle de fluxo | M6 e M5 | Falhas de rede e consumidores lentos têm comportamento limitado e documentado |
| M8 | Contrato público e integração | M6; remoto após M7 | Harness externo integra o kernel pelo contrato publicado |
| M9 | Eficiência demonstrada | Baselines dos perfis relevantes | Ganhos reproduzíveis preservam invariantes |

Documentação, diagnóstico e testes acompanham cada incremento. Medições começam cedo; M9 concentra otimizações justificadas pelos resultados.

## M5.1 — Consolidação

- Mapear requisito → perfil → teste → limite conhecido, distinguindo daemon confiável, protocolo de componentes e perfil gerenciado.
- Auditar quiesce, EOF, dispose, workers vivos, erros de escrita e cancelamento nos SDKs. Reconhecimento de limpeza deve corresponder ao trabalho encerrado.
- Exercitar retirada, renovação e chamadas concorrentes; filas saturadas; crash durante admissão, resultado e limpeza; falhas de armazenamento.
- Definir retenção e esgotamento do ledger sem perder silenciosamente informação de deduplicação e recuperação.
- Separar cancelamento da espera, encerramento da execução e rejeição de commit. Fencing no KV não cobre automaticamente destinos externos.

Saída: regressão e compatibilidade verdes; falhas reproduzíveis; descarte bem-sucedido retorna recursos ao baseline. Achados da auditoria viram correções e testes antes de fechar o marco.

## M6 — Composição genérica entre processos

Contrato de implementação: [M6.1 — chamadas a dependências](M6.1-SPEC.md), incluindo negociação, autoridade, linearização, encerramento e aceitação D01–D12. Especificação proposta; não indica implementação concluída.

| Incremento | Entrega | Aceitação |
|---|---|---|
| M6.1 | Consumidor externo chama dependência vinculada através do kernel | **Entregue em 2026-09-06** ([M6-COMPOSITION](M6-COMPOSITION.md)): cadeia Rust↔Python, grants, quotas, despacho e revogação |
| M6.2 | Relação pai/filha, prazo, orçamento, cancelamento e quotas | **Entregue com M6.1** (tickets, prazos, cancelamento imediato, quotas atômicas; ver entrega) |
| M6.3 | APIs genéricas de recursos e eventos pertencentes ao contexto | **Entregue no fechamento M6** (recursos externos com teto e liberação; `event.deliver` a assinantes; SDKs com paridade) |
| M6.4 | Conformidade temporal através de IPC | **Entregue com M6.1** (cascata, substituição e reintrodução em `dep_flow`; conformidade formal futura) |

M6.1 inclui segurança mínima das chamadas filhas desde o início; M6.2 completa concorrência e limites. Resolução usa o binding da geração do consumidor, sem permitir que o payload escolha outra autoridade. Negociar funcionalidades e definir comportamento de clientes antigos antes de implementar extensões do protocolo.

Demonstração: provedor, consumidor e independente como fixtures genéricas. Retirar o provedor durante trabalho real, observar cascata e limpeza, rejeitar resultado tardio e reintroduzir com identidade nova. Não requer modelo de IA nem aplicação de produto.

## M7 — Composição remota

Plano de execução: [Epic M7](M7-EPIC.md). M7.1–M7.4 abaixo são áreas de uma única entrega integrada, com decisões de implementação autônomas e revisão ao final; não exigem aprovações intermediárias. Entrega: [M7-COMPOSITION](M7-COMPOSITION.md) (evidência R01–R13, garantias local×remoto, runbook).

| Incremento | Entrega | Aceitação |
|---|---|---|
| M7.1 | Streams com crédito, quotas e encerramento | Consumidor lento mantém filas e memória limitadas; cancelamento libera recursos |
| M7.2 | Reconexão e consulta de operações | Perda de resposta não repete automaticamente efeito desconhecido |
| M7.3 | Autoridade e retirada através da fronteira remota | Expiração, revogação e partição invalidam bindings e resultados conforme o perfil |
| M7.4 | Conformidade local/remota e diagnóstico correlacionado | Mesma fixture roda em ambos os perfis; diferenças são enumeradas e verificadas |

Fencing deve ser fiscalizado no destino de cada efeito protegido. O kernel fornece mecanismos e contratos genéricos; aplicações integram seus destinos. Recuperação cobre estado gerenciado e operações, sem prometer serializar memória arbitrária de plugins.

## M8 — Integração pública

Plano de execução: [Epic M8](M8-EPIC.md). Entrega integrada com APIs, artefatos, conformidade e harness externo; M8.1–M8.4 são áreas de trabalho, sem aprovação intermediária. Aceitação P01–P12 e revisão ao final. Entrega: [M8-COMPOSITION](M8-COMPOSITION.md) (evidência P01–P12, limitações, runbooks).

| Incremento | Entrega | Aceitação |
|---|---|---|
| M8.1 | API de biblioteca e modo serviço documentados | Harness externo usa modos suportados sem depender de módulos internos |
| M8.2 | Protocolo, schemas, SDKs e evolução | Implementação independente passa conformidade; incompatibilidade falha explicitamente |
| M8.3 | Configuração, carregamento e atualização genéricos | Autoridade vem do operador; atualização e retorno de versão respeitam gerações e cleanup |
| M8.4 | Inspeção e operação | Diagnosticar contextos, bindings, operações e recursos; verificar backup, restauração e revogação sem expor segredos |

Harness externo pode ser temporário e mínimo. Marketplace, UX de instalação e ferramentas de domínio não são pré-requisitos. As abstrações legadas Model/Tool, o loop simulado e a interface de aplicação foram removidos do kernel em 2026-09-06 conforme o [plano de remoção](KERNEL-CLEANUP.md); compatibilidade do protocolo do daemon e de plugins continua sendo requisito.

## M9 — Eficiência

Medir latência p50/p95/p99, throughput, CPU, memória, filas, lifecycle, IPC, transporte e armazenamento. Publicar ambiente, carga e dados brutos; separar custo do kernel do trabalho dos plugins.

Investigar conexões persistentes, cópias, scheduler ou formato de mensagens a partir de gargalos medidos. Repetir conformidade após otimizações. Comparações exigem semântica, isolamento, durabilidade e carga equivalentes; Rust por si só não comprova vantagem ponta a ponta.

## Primeiro lote e extensões adiadas

Antes de ampliar M6, foi executado o [plano de remoção do código de aplicação](KERNEL-CLEANUP.md): abstrações de agente e TUI retiradas em 2026-09-06, com o cliente genérico e toda a infraestrutura M5 preservados.

1. ~~Fechar a matriz de M5.1 e reproduzir achados de lifecycle e limites.~~
   Concluído em 2026-09-06: [auditoria, tratamento B1–B4 e fechamento](M5.1-AUDIT.md).
2. ~~Especificar M6.1: identidade do chamador, binding, autoridade e chamada filha.~~
   Concluído em 2026-09-06: [contrato de implementação](M6.1-SPEC.md) proposto (negociação, autoridade, linearização, encerramento, D01–D12); sem implementação.
3. ~~Implementar consumidor → kernel → provedor em Rust/Python, com retirada durante execução.~~
   Concluído em 2026-09-06: [M6 implementado](M6-COMPOSITION.md) com [demo]( ../../scripts/demo-composition.py).

Novos perfis de isolamento, como árvores de processos ou WASM, exigem requisito genérico e contenção verificável. Não dependem de construir um executor de code agent. Federação, ciclos, transferência arbitrária de ownership, hot swap sem pausa e exactly-once externo permanecem adiados.
