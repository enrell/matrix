# Registro de decisões de projeto

Status: baseline proposta para implementação, documentada a partir da direção solicitada pelo usuário. Não descreve funcionalidades já entregues.

| Id | Decisão | Razão e custo |
|---|---|---|
| D01 | Kernel Rust; aplicação como componentes | Controle de recursos e base reutilizável; desempenho precisa ser medido |
| D02 | Contexto como fronteira de propriedade/autoridade | Permite cleanup verificável; efeitos precisam passar pela mediação |
| D03 | Separar árvore de ownership e grafo de dependências | Composição entre componentes independentes sem herança artificial |
| D04 | Uma autoridade de composição no primeiro perfil | Simplifica linearização; disponibilidade distribuída fica limitada |
| D05 | Coordenador de metadados, workers para I/O | Reduz interleavings sem bloquear controle em código de plugin |
| D06 | Processo + IPC como primeira fronteira de linguagem | Permite Rust/Python; adiciona cópia e escalonamento |
| D07 | JSON enquadrado como primeiro perfil de wire | Facilita conformance; codec binário posterior depende de medição |
| D08 | Reload coordenado com possível pausa | Evita prometer migração/transação geral antes de implementá-la |
| D09 | Perfis explícitos de isolamento e durabilidade | Nome de feature não substitui garantia |
| D10 | Remoto por host responsável pelos recursos | Autoridade local de cleanup; host entra na base confiável |
| D11 | Sem retry automático de efeito desconhecido | Evita duplicação silenciosa; exige reconciliação |
| D12 | Agente como aplicação posterior ao M1 | Valida composição antes de misturá-la com LLM e ferramentas |

## Decisões ainda abertas

| Questão | Resolver até | Evidência necessária |
|---|---|---|
| Crates de async, coordenação e testes de interleaving | M1 | Protótipo de cancelamento/cleanup e mantenibilidade |
| Schemas completos e política de evolução do wire | M2 | Vetores compartilhados Rust/Python |
| Implementação de sandbox Linux | M3 | Contenção real de SO e descendentes |
| Armazenamento durável | M4 | Fault injection, fsync/commit e recuperação |
| Biblioteca TLS, provisionamento/rotação de identidade | M5 | Procedimento operacional e testes de revogação |
| Renovação remota e premissas de relógio/atraso | M5 | Análise de mensagens atrasadas e fencing no destino |
| WASM Component Model e linguagens suportadas | Extensão M3 | SDK e runtime executáveis com conformance |
| Orçamentos numéricos de latência/memória | Após baseline | B01–B04 reproduzíveis |

Mudanças devem atualizar contrato, testes e este registro juntos. Não tratar decisões do master3 como irrevogáveis para o novo objetivo do Matrix.
