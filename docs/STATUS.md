# Estado implementado

Atualização de 2026-09-05 sobre a base final de M2.5. O Matrix é experimental; não há alegação de equivalência formal ao Cordis ou benchmark WAN.

| Área | Estado |
|---|---|
| M1.1 | Identidades, contextos, recursos reais e descarte idempotente |
| M1.2 | Dependências reativas, bindings, ciclos/ambiguidade e cascata |
| M1.3 | Tickets, bloqueio de admissão, cancelamento/drenagem e commits mediados |
| M1.4 | Reload coordenado e retirada de manifests ausentes |
| M2.1–M2.5 | Protocolo local, host de processos, SDKs Rust/Python e teste integrado |
| M3 | Perfil autorizado de host, token por lançamento, sandbox Linux x86_64 restrita e supervisão limitada |
| M4 | Serviço com SQLite FULL, ledger sem replay cego, snapshot e fencing no destino KV |
| M5 | Hosts por TLS mútuo, grants por fingerprint, leases, proxy e retirada em cascata por perda de autoridade |
| M5.1 | **Fechado em 2026-09-06**: lifecycle linearizado, assentamento e diagnóstico verificáveis ([auditoria](M5.1-AUDIT.md)) |
| M6 | **Implementado em 2026-09-06**: composição local Rust/Python com autoridade ([entrega](M6-COMPOSITION.md)) |

Escopo revisado em 2026-09-06: Matrix é somente o kernel; aplicações vivem em repositórios separados. As abstrações de agente do scaffold (`Model`, `Tool`, `EchoTool`, `CannedModel`, `Agent`) e o crate `matrix-tui` foram removidos conforme o [plano de remoção](KERNEL-CLEANUP.md). [Plano revisado](NEXT-PHASES.md).

## Fronteiras que importam

`matrix-rt`/`Host::attach` preservam o perfil confiável legado. Para M3–M5 use `matrix-managed`/`Service` com `HostPolicy.secure=true`. Sandbox não confiável é Linux x86_64 e não permite subprocessos. WASM e sandbox de builds com árvore de processos não foram implementados.

Durabilidade pertence ao serviço gerenciado. Não há restauração automática da memória de plugins nem atomicidade universal de efeitos externos. Snapshot/ledger não substituem reconciliação. Remoto é um perfil de controle unary; streams grandes e federação ficam fora desta entrega.

[Configuração, verificação e limites detalhados](MANAGED-RUNTIME.md) · [Relatório de validação](M3-M5-VALIDATION.md)
