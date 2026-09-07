# M7 — Composição remota: entrega integrada

Data: 2026-09-07. Referências normativas: [M7-EPIC](M7-EPIC.md) (aceitação
R01–R13), [M7-PROFILE](M7-PROFILE.md) (`matrix.remote/0.1`), base
[M6](M6-COMPOSITION.md). Demonstração: `scripts/demo-remote.py` (gerenciado,
cadeia Python → Rust entre hosts). Fixtures genéricas: `dep_node`
(Rust), `dep_node.py` (Python); nenhuma aplicação de produto.

## Resultado

Uma aplicação externa compõe componentes locais e remotos pelo mesmo
modelo de dependências, sem implementar transporte, propagação de
autoridade, reconciliação ou cleanup distribuído. O kernel mantém
identidade e ownership ao atravessar hosts; mudar a localização não
reescreve a lógica dos componentes. M7.1–M7.4 do roadmap foram áreas de
trabalho, não checkpoints: decisões internas ficaram com a implementação;
a revisão é desta epic completa.

## O que foi entregue, por camada

| Camada | Entrega | Código |
|---|---|---|
| Perfil | `matrix.remote/0.1`: identidades, catálogo, ownership, limites; validação executável + vetores | `matrix-proto/src/remote.rs`, [M7-PROFILE](M7-PROFILE.md) |
| Transporte | Sessão TLS multiplexada persistente (1 conexão/sessão, 1 thread I/O), filas controle/dados limitadas com prioridade de controle, deadlines absolutos (handshake, frame, poll sem SNDTIMEO), veneno em parcial, Suspect/Detached, heartbeat, `request` cancelável + assinaturas multi-resposta | `matrix-runtime/src/session.rs`, `tests/remote_session.rs` + `tests/common/proxy.rs` |
| Kernel | Definições `remote: true` (nunca ativam local), `register/unregister_remote_provider` com atestação (instância, geração), visões mescladas (reconcile/resolve/promote/`call_open`), handles `rb-N`, admissão remota com peer no ticket/journal, validação terminal dividida, `remotes` + `dep.remote_peer` no inventário | `matrix-core/src/kernel.rs`, `calls.rs`, `tests/m7_remote_kernel.rs` (9) |
| Serviço | `provision_remote`/`install_remote_definition`, `renew_seq` idempotente, `lease_snapshot_by_token`, `lease_status_by_token`, `live_provider_fences`, `invoke_with_budget`, ação `lease-status`, accept loop + watcher com `revoke.notice` | `matrix-runtime/src/service.rs`, `remote_session_server.rs`, `remote.rs` |
| Executor | Ledger-first admit (replay/conflito/unknown), revalidação de autoridade por perna, cancel cooperativo, downgrade de unknown antes de persistir, `op.query` do ledger escopado, renew via `renew_seq`, inventário com ativações atestadas, subscribe/emit com perda contada, streams com contabilidade limitada + crédito | `matrix-runtime/src/route_executor.rs` |
| Controlador | Worker por peer (connect → activate unary + reuso de lease → ready → register → reconcile → subscribe → renew), backoff, teardown (unregister + release + falha de in-flight), `RemoteTransport` (open/cancel/topology/streams), op-query, inspect | `matrix-runtime/src/route_controller.rs` |
| Host | Gate + admissão locais, worker remoto espelho (quotas → accept → resposta → close, observação de revoke/deadline/liveness, cancel encaminhado por operation), ids SHA, `deliver_remote_event` + `event_tap`, `topology_changed`, `HostPolicy.domain`, bridge de streams (bind/credit/deliver/end, lookup global) | `matrix-host/src/lib.rs`, `tests/remote_dispatch.rs` (6) |
| SDKs | `on_stream` em despachante dedicado (Rust/Python, paridade), `pending_stream_count`, `stream_send` nos fixtures (+ `--stream-log`, `--stream-slow-ms`), flood/descarte/contagem nos dois idiomas | `matrix-component`, `matrix_component.py`, `dep_node(.rs/.py)`, `sdk_units` (6+6) |
| Gerenciado | `remotes:` (authority/domain/peers/routes com capabilities, session listener), `provision_remote` no serve, `RouteManager::sync` no SIGHUP, ready com `session` | `matrix-runtime/src/bin/matrix-managed.rs`, `scripts/demo-remote.py` |

## Autoridade ponta a ponta (validação dividida)

Nenhum lado presume o outro. Controlador valida cadeia e binding
(pai, consumidor, grant da aresta, revisão, quotas, liveness do registro
no instante atestado); executor revalida lease/fence e efeitos locais
(token → principal/lógico/fence, grant cobre cap, definição provê cap,
owner atual) antes de admitir efeito mediado e de novo antes de
persistir. Revogação entre open e persist rebaixa para unknown (nunca ok
falso); cancel cooperativo vence pré-persist e nunca revive terminal.
Recursos remotos ficam pendentes até evidência de liberação; incerteza
nunca vira Disposed.

## Identidade de operação (R08)

Identidade composta inequívoca do controlador —
`domínio:época:consumidor:instância:geração:pai:binding:request:len(sha256(...))`
(determinística no boot; nomes longos caem no fallback `op-<sha256>`
sobre a mesma pré-imagem, teto 128 do fio). A época (entropia por boot)
separa reinícios: pais, bindings e contadores de request podem repetir,
mas a época não — chamada nova nunca replays entrada antiga do ledger
persistente. O `request` é a identidade da invocação no boot: duas
chamadas intencionais iguais sob um pai têm requests distintos (duas
operações, ambas executam); retransmissão no fio reusa o request (mesma
operação, replay pelo dedup do portão, nunca segundo ticket). Conteúdo
divergente sob um id é `operation-id-conflict` (segunda barreira no
executor, nunca reuso silencioso). Política de colisão explícita: ids só
colidem sob colisão SHA-256, e o executor ainda recusa conteúdo
divergente (nega, nunca cruza chamadas).

## Chamadas, cancel, consulta

`call.open` carrega cadeia completa + lease (só token; fence viaja dentro
dele no servidor) + `grant_rev` + `budget_ms` (duração restante; cada host
aplica seu prazo local). `call.accepted.persisted` distingue admissão
durável de volátil; terminal revalidado no controlador. `call.cancel`
indexado por `operation_id` estável (não `request_id` de transporte).
Consulta nunca reexecuta; ausente após retenção é unknown, não prova de
não-execução; principal alheio não vaza resultado (ledger escopado).

## Streams (R02)

Sessão: `stream.data` (base64 + crédito consultivo) via fila bulk,
`stream.credit`/`complete`/`cancel` via controle; receptor concede,
consome em bytes, libera ao soltar o buffer; sem crédito/seq inválida/
excesso rejeita; sem acumular chunks (executor conta, não retém);
tabela limitada (128), teto 1 MiB/stream, tombstones (tardias ignoradas,
nunca recriadas). Host: ids remotos em namespace disjunto (`rstreams`),
bind explícito com crédito inicial, relay com quota de egresso,
entrega ao componente com janela local, crédito do executor alarga a
janela, terminal vira tombstone. SDK: `on_stream` no mesmo despachante
dos eventos (limitado 64, descarta mais antigos e conta); lentidão
estrangula o emissor via crédito em vez de crescer memória; controle
nunca fica atrás de dados (filas separadas + orçamento reservado).

## Eventos (R11)

Inscrições são recursos do contexto consumidor (não sobrevivem a nova
geração). Controlador declara interesse (`event.subscribe`); executor
confirma o conjunto vigente (`event.subscribed`) e emite só o inscrito
via tap local (`on_local_emit`, best-effort com contadores
delivered/dropped). Roteamento verifica inscrição e autoridade atuais;
tópico no payload não concede acesso. Entrega no controlador reusa o
fan-out local (match no bind, quota de egresso por inscrito, carimbo da
ativação vigente; gerações obsoletas filtram no componente). Best-effort
com perda observável; sem replay via ledger.

## Renovação, reconexão, reconciliação (R05/R06/R07)

Renovação corre no canal de controle, independente de pernas longas, com
`seq` idempotente (repetido replays sem girar; mais antigo é stale;
unário intacto). Atrasada nunca revive ativação retirada; resposta
perdida = reconciliação idempotente ou nova ativação. Reconexão = nova
`session_id` + `inventory.reconcile` antes de publicar bindings (nunca
herda autoridade). Streams não retomam; operação segue consultável.
Admitida sem resultado durável é unknown sem evidência adicional.
Fencing só exclui no destino que fiscaliza.

## Desvios e decisões internas, justificados

1. Rota A (controlador admite local, executa remoto; sem tickets sombra):
   um caminho de admissão para pernas locais e remotas (`DepChild.remote_peer`).
2. `remote: true` nunca ativa local; só registro atestado explícito
   satisfaz; provedor local nunca é registrável (sem hijack).
3. Handles remotos `rb-N` (sequência própria); `bind-N` local nunca cruza o fio.
4. `call.open` do controlador leva só token (fence dentro dele no servidor).
5. `operation_id` cunhado no host após admissão (SHA composto acima,
   com identidade da invocação);
   journal carrega peer; correlação vive em `remote_ops` + ledger do executor.
   Nomes longos têm fallback determinístico `op-<sha256>` (teto 128 do fio).
6. Terminais do executor levam só output/erro de negócio (o wrapper de
   durabilidade fica no ledger); replay desembrulha igual (paridade local/remoto).
7. Multi-resposta (`accepted`+`result` no mesmo `request_id`) via
   assinaturas na engine (não `request()` único); tardias após unsubscribe
   são stray (contam, nunca resolvem espera nova).
8. `stream_end` carrega `status` (`ok`→`complete`, demais→`cancel`);
   `stream_data` reporta crédito restante como dica (autoritativo é o host).
9. Lookup de streams por id global no host (perfil mantém ids únicos por
   owner+operação+direção); `bind` recusa duplicata (fail closed).
10. `deliver_remote_event` retorna resultados por sessão (diagnóstico, sem payloads).
11. Renovação por `renew_seq`; `renew` unário intacto.
12. Ponte de streams por associação à chamada (sem API nova no
    componente): id sob `remote/` não vinculado, com exatamente uma perna
    remota em voo na sessão, auto-vincula a ela (zero ou ambíguo →
    sumidouro local, fail closed; fora de `remote/` nunca auto-vincula,
    então telemetria local jamais cruza hosts por acidente).
    Chunks precoces estacionam limitados (32/sessão, TTL 2 s, dreno em
    ordem no mapeamento — dos dois lados da corrida) e a contabilidade
    local os vê na mesma (namespaces disjuntos, sem classe de colisão).
    `stream.open` anunciado mas não transacionado: pernas nascem no
    primeiro chunk. Direção reversa via tap do host executor (observa
    após contabilizar; melhor esforço); ids são por direção (sequência e
    janela próprias cada lado). `bind_remote_stream` explícito segue para
    testes/diagnóstico (qualquer id).
13. Payloads de stream para SDK são UTF-8 (binário degrada lossy, documentado).
14. Reconciliação antes de publicar: attach reconcilia (com ids reais de
    operação; revogadas cancelam fail-fast) antes de registrar; refresh só
    atualiza registros existentes; remoção de peer na config retira o
    registro nomeado no link (mais poda de provedores órfãos), nunca vaza
    binding sem rota.
15. `resources: []` vazio por construção na v0.1 (sem protocolo de handle
    remoto; handles morrem com a sessão); o retido entre hosts são as
    pernas, reconciliadas como operações; lease via sonda pré-registro +
    renew. Mapeamento operação→provedor sobrevive ao terminal (pernas
    sobrevivem a chamadas; despejo oldest-first no teto).
16. Primeira chunk define a base de sequência (tolerância ao prefixo
    estacionado); contiguidade vale da base em diante; duplicata/reordem
    tardias ignoradas, nunca reexecutadas.
17. Terminação de streams guarda ownership explícito: o mapa
    operação→(lógico, instância, geração, fence) revalida dono atual +
    lease viva a cada entrega, e a referência validada viaja até a
    seleção da sessão destino sem nova resolução por nome (seleção
    atômica sob os dois locks; gerações imutáveis, sids únicos):
    substituição no intervalo recusa ou serve a sessão antiga
    (linearização definida), nunca redireciona à nova. Provedor
    substituído ou lease morta param de receber; contabilidade segue
    limitando. Pernas canceladas/revogadas/expiradas/perdidas encerram
    seus streams vinculados nos dois lados (tombstones verificáveis);
    sucesso mantém (pernas sobrevivem a chamadas).
18. Streams `remote/` sobreviventes têm reconciliação verificável:
    pernas de operações revogadas são tombstonadas no executor; o
    controlador encerra as vinculadas ao cancelar; `inspect` expõe
    pernas/recebidos/descartados. Junto ao item 15, é o que permite a
    `resources: []` representar ausência real de pendência.

## Garantias: local × remoto

| Propriedade | Local (M6) | Remoto (M7) |
|---|---|---|
| Admissão | pai+binding+grant+quotas, revalidada | igual + peer/registro vivo no instante atestado |
| Terminal | `dep_chain_check` + `dependency_accept` único | igual no controlador + autoridade revalidada no executor antes de persistir |
| Cancel | revoga local imediato, avisa provedor | igual + `call.cancel` por operação (cooperativo, sem prova física) |
| Streams | crédito do host, excesso encerra, sessão sobrevive | igual nas duas pontas + crédito entre hosts; sem resume implícito |
| Eventos | fan-out com quota, fila 64 com descarte contado | igual via subscribe/tap; perda observável nos dois lados |
| Renovação | lease local com fence | `seq` idempotente sobre sessão, independente de pernas |
| Consulta | n/a | ledger escopado, nunca reexecuta, divergente conflita |
| Diagnóstico | inventário + journal com cadeia | igual + hosts/sessões/leases/streams/recursos pendentes, sem credenciais/payloads |

## Evidência (R01–R13)

| ID | Cenário | Prova |
|---|---|---|
| R01 | Cadeia entre hosts Rust/Python | `route_host_e2e::{e2e_remote_chain_happy_path, _python_provider}` (dep_node real nos dois lados, mesmo negócio, vínculo rastreável; rota só no kernel) + `demo-remote.py` (Python→Rust gerenciado) |
| R02 | Streams bidirecionais + lento | `route_host_e2e::{e2e_bidi_streams_sdk_to_sdk, _python_consumer}` (SDK→host→executor→SDK nos dois sentidos, Rust+Python, auto-bind `remote/` + park/drain + tap + inject com ownership) + `route_streams::replaced_provider_rejects_stale_operation_chunks` (substituição/revoke param entrega) + `remote_dispatch::inject_pins_exact_activation` (ref travada até a seleção: obsoleta recusa, retirada recusa) + `route_streams` (5: +reconcile por id) + `remote_dispatch` + `sdk_units` Rust/Python (2+2) |
| R03 | Saturação | `route_streams::saturation_*` (chamadas/dados/eventos/controle abusivo limitados; controle no orçamento) + `remote_session::control_progresses_under_data_flood` + quotas `resource-exhausted` + orçamento reservado de controle |
| R04 | Retirada durante chamada/stream | `route_host_e2e::{e2e_remote_cancel_no_phantom_ok, e2e_route_loss_fails_closed}` + `route_integration::r04` + tombstones de stream (sessão some → perna cancela, sem ok fantasma) |
| R05 | Partição e retorno | `remote_session` (proxy com atraso/perda, Suspect/Detached) + `route_authority::reconnect_uses_new_session_and_reconciles` + `route_host_e2e::reconnect_with_pending_leg_settles_unknown_and_reconciles` (perna pendente vira unknown, sem ok fantasma; ledger sem resultado falso; reconcile com ids reais antes de republicar) |
| R06 | Renovação atrasada/perdida | `route_authority::renew_seq_idempotent_stale_never_revives` (replay idempotente, stale nunca revive) + renew no controle independente de pernas longas |
| R07 | Queda antes/depois de persistir | `route_integration::r07` (admit→unknown, finish→replay, conflito divergente, principal alheio isolado) + perda de rota vira unknown, nunca replay de desconhecido |
| R08 | Duplicata/conteúdo divergente | Id SHA composta com época do boot + ativação + invocação (request) + fallback + `operation-id-conflict` + `r07` + `remote_dispatch::{operation_id_stable_per_logical_call, intentional_equal_calls_under_one_parent_both_execute, operation_ids_differ_across_controller_boots}` + `route_authority::ledger_does_not_suppress_distinct_boot_operations` (reinício com contadores repetidos nunca suprime) |
| R09 | Geração/fence antigos, recurso alheio | `route_integration::r09` (registro superado rejeita terminal) + revalidação de owner/fence por perna + ownership de handles na fronteira |
| R10 | Credenciais inválidas/revogadas/giradas | `route_authority::revoked_principal_fails_closed_at_admission` (snapshot+grant recusam; liveness pré-persist nega) + isolamento por cert (`rotated`/`stranger`) + ALPN estranho recusado |
| R11 | Evento remoto, descarte, reintro | `route_events::subscribed_events_forward_unsubscribed_do_not` (gate por inscrição, ack do conjunto, retirada para, perda observável) + fan-out com quota + SDK flood/descarte nos dois idiomas |
| R12 | Cliente/perfil antigo × novo | `route_authority::old_executor_without_calls_feature_fails_explicitly` (sem downgrade silencioso) + `unsupported-feature` sem negociação (M6) + ALPN `matrix.remote/0.1` negociado, unary intacto como compat |
| R13 | Parcial/lento/TLS/EOF | `remote_session` (veneno em parcial, flood com progresso, partição) + `write_frame_deadline` (poll sem SNDTIMEO, poison, timeout tardio conta) + truncado nunca reutilizado |

Verificação: `make test` (218 passed / 0 failed no Cargo com
`--test-threads=1` + 6 passed no Python `sdk-python/test_units.py`),
`make compat` PASS, `scripts/smoke-managed.py` PASS,
`scripts/demo-composition.py` (M6 intacto) PASS, `scripts/demo-remote.py`
(M7) PASS; `git diff --check` limpo, zero warnings no build.

## Limites e backlog explícito

Fora da epic (não implementado, não prometido): apps de produto,
sincronizador de workspace, executor de builds, armazenamento de
artefatos de domínio, marketplace, federação, migração arbitrária de
ownership, eleição de líder, exactly-once externo (stream genérico não
obriga serviço de arquivos). Também fora: retomada de stream por offset
(só como feature adicional futura com fonte/sequência/retenção/autorização).
Futuro honesto: auto-bind de streams por ticket (ponte atual é bind
explícito), payloads binários no SDK, `stream.open` componente↔host,
reconciliação de recursos remotos pendentes além de revokes, fuzz de
parser (C12), M6.4 formal. Nada disso é silencioso: ausente falha explicitamente.

## Runbook (operador)

- **Partição / executor perdido**: pernas viram `outcome-unknown` (nunca ok
  falso); independentes seguem. Reanexe (nova sessão + reconcile) antes de
  publicar bindings; consulte operações pelo ledger (`op.query`) em vez de
  reemitir efeito desconhecido.
- **Recuperação de resultado**: `op.query` por (principal, operation_id);
  `completed` replays o terminal; `unknown`/`admitted` = reemitir só com
  novo operation (novo pai) ou aguardar evidência.
- **Recurso pendente**: handles são opacos por owner/host; contar proxy
  como removido não prova liberação remota — reconcilie inventário e
  aguarde `revoke.notice`/expiração de lease; perda de autoridade bloqueia
  novos efeitos e inicia cleanup.
- **Rotação de credenciais**: gire CA/certs, `revoke` o principal antigo
  (admissão e liveness recusam), SIGHUP sincroniza grants/remotes; sessões
  antigas viram Suspect/Detached e nunca reativam por mensagem velha.
- **Diagnóstico**: `inspect` (rotas, sessões, leases, pernas, streams,
  eventos entregues/descartados) + journal (`dependency.*`, `evt.emit`,
  `lease.*`); sem credenciais nem payloads por padrão.
