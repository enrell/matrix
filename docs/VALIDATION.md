# Validação, operação e desempenho

> Implementação disponível: [perfil gerenciado M3–M5](MANAGED-RUNTIME.md). Este documento preserva o contrato de projeto; consulte o perfil para saber quais mecanismos e transportes estão implementados.

Status: plano de aceitação; testes abaixo ainda não implementados. Nenhum PASS deve ser inferido da existência deste documento.

## Suíte de conformidade

| Id | Cenário | Resultado exigido |
|---|---|---|
| C01 | Descartar geração e reutilizar id lógico | Handles antigos rejeitados |
| C02 | Adquirir/revogar capacidade, inscrição, timer e task | Inventário retorna ao baseline após cleanup |
| C03 | Descartar duas vezes e falhar a meio da aquisição | Sem dupla liberação, órfãos ou corrupção |
| C04 | Consumidor antes/depois do provedor | Waiting/Active conforme dependências |
| C05 | Remover provedor compartilhado | Consumidores afetados retirados; independente segue ativo |
| C06 | Remover durante chamada e retornar resposta atrasada | Nenhum commit não autorizado após revogação |
| C07 | Reload remove capability e candidato falha | Nenhum registro antigo; indisponibilidade explícita |
| C08 | Efeitos intercalados de A e B; remover A | Efeitos independentes de B preservados |
| C09 | Cancelamento, drenagem e callback bloqueado | Prazos respeitados pelo perfil; cleanup pendente visível |
| C10 | Forjar principal/owner/grant ou chamar após revogação | Acesso negado no destino do efeito |
| C11 | Loop, crash, memory bomb e processos descendentes | Limites/isolamento do perfil demonstrados |
| C12 | Frames inválidos, grandes, truncados e versões incompatíveis | Rejeição limitada sem crash ou alocação descontrolada |
| C13 | Consumidor lento e produtor excessivo | Filas limitadas, backpressure e controle ainda responsivo |
| C14 | Duplicatas e mensagens reordenadas entre sessões | Idempotência de lifecycle e rejeição de geração antiga |
| C15 | Falhas repetidas e cascatas | Orçamento de restart respeitado, diagnóstico terminal |
| C16 | Queda em cada fronteira de persistência | Estado confirmado durável recuperado conforme perfil |
| C17 | Efeito executado com resposta perdida | Resultado desconhecido/reconciliação; sem retry cego |
| C18 | Snapshot, prune e armazenamento corrompido | Pendências preservadas; corrupção explicitada |
| C19 | Inspecionar instância parada e recurso pendente | Razão, dependência, owner, geração e operação identificáveis |
| C20 | Partição sem morte do host | Participação revogada; cleanup remoto não declarado sem evidência |
| C21 | Host antigo tenta escrever após época nova | Destino com fencing rejeita operação |
| C22 | Reconectar com órfãos e operações desconhecidas | Reconciliação antes de publicar capacidades |
| C23 | Credencial revogada e peer não autorizado | Registro/chamadas negados; sem grants residuais |
| C24 | Mesmo componente de referência em Rust/Python | Mesmos observáveis sem exigir igualdade de latência |

## Estratégia

Unitários para ownership/FSM; testes de integração para recursos reais; testes de propriedades para sequências load/invoke/remove; escalonamento controlado para interleavings; fuzzing de parser; falhas injetadas para crash/rede/disco. Cada fixture tem workspace/socket próprios e cleanup final. Não depender de sleeps arbitrários para provar ordem.

Escolha de bibliotecas de model checking/fuzzing é tarefa da implementação. Relatório deve registrar commit, versão da suíte, host, perfil, comandos e limitações. Compilação e testes do scaffold não substituem esta suíte.

## Inspeção operacional

Estado deve mostrar instâncias, gerações, dependências resolvidas/ausentes, grants, recursos por owner, chamadas em andamento e cleanup pendente. Trace correlaciona request_id, operação, contexto e geração sem registrar conteúdo sensível por padrão.

Métricas mínimas: latência de admissão/dispatch, tempo de ativação/descarte, bytes de fila, rejeições por quota, recursos pendentes, falhas e restarts, duração de reconciliação. Histórico tem limites de retenção; telemetria não bloqueia cleanup.

Runbook futuro: diagnosticar Waiting pelo binding, CleanupPending pelo recurso/host e Failed pelo orçamento de supervisão. Operador não deve resolver tudo com restart/reset que apaga a evidência.

## Benchmarks

| Id | Medição | Condição |
|---|---|---|
| B01 | Latência p50/p95/p99 de chamada vazia e payloads crescentes | Separar in-process, IPC e rede |
| B02 | Throughput e memória sob consumidores lentos | Mesmos limites e pressão de carga |
| B03 | Ativação, remoção e reload com muitos componentes | Confirmar invariantes além de tempo |
| B04 | Composição remota com chamadas encadeadas e streams | Separar custo do kernel, transporte e trabalho da fixture; verificar quotas e cancelamento |

Usar builds release, recursos do host registrados, aquecimento documentado, repetições e dispersão. Comparação com outro runtime exige comportamento e isolamento equivalentes. Nenhuma meta numérica ou superioridade sobre TypeScript é afirmada antes de baseline medido.
