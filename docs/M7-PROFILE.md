# Perfil remoto M7 — `matrix.remote/0.1`

Status: contrato de fio e ownership + transporte multiplexado com
deadlines absolutos + chamadas/streams/eventos remotos, reconciliação e
SDKs (entregue; ver [M7-COMPOSITION](M7-COMPOSITION.md) para evidência
R01–R13). Referência normativa da epic:
[M7-EPIC](M7-EPIC.md). Implementação: `matrix-proto/src/remote.rs`
(validação executável + vetores em `mod tests`) +
`matrix-runtime/src/session.rs` (sessões TLS multiplexadas) +
`matrix-runtime/tests/remote_session.rs` (6 testes) e
`tests/common/proxy.rs` (fault-injection em loopback).

O perfil unary `matrix.managed/0.1` permanece como compatibilidade, sem
semântica nova. Incapacidade remota (streams, filhas) retorna erro
explícito, sem downgrade silencioso.

## Identidades (todas obrigatórias no fio, sem reutilização ambígua)

| Campo | Formato | Escopo |
|---|---|---|
| `domain` | string `1..64`, `[A-Za-z0-9_-]` | domínio de composição (uma autoridade) |
| `authority` | string (fingerprint do principal controlador) | quem decide o grafo |
| `controller_epoch` | string decimal u64 | época do controlador (boot) |
| `session_id` | string `1..128` | sessão de transporte (reconexão = nova) |
| `activation` | `{logical, instance, generation}` decimais p/ números | ativação emissora |
| `binding_id` | `rb-<n>` opaco, emitido por ativação | binding autorizado da ativação |
| `operation_id` | string `1..128` | operação estável no escopo principal/controlador |
| `stream_id` | string `1..128` | stream único (owner + operação + direção) |
| `lease` | `{token, fence, seq}` | ativação (token) vs autorização de chamada (fence+seq) |

`bind-N` local nunca atravessa o fio sozinho: no perfil remoto o
binding é `rb-<n>` qualificado por `domain` + ativação completa.
Lease de ativação (token) e autorização de chamada (fence + seq de
renovação) são objetos diferentes.

## Tipos e famílias

Envelope: `protocol="matrix.remote"`, `version="0.1"`, `type`,
`message_id`, campos de identidade por família, `body`.

| Tipo | Identidade no topo | Corpo |
|---|---|---|
| `session.hello` | — | `{versions, features, domain, authority, controller_epoch}` |
| `session.welcome` | `session_id` | `{version, features, executor_epoch, limits}` |
| `session.close` | `session_id` | `{reason}` |
| `heartbeat` | `session_id` | `{}` (observação de conectividade, não prova) |
| `lease.renew` | `session_id` | `{token, fence, seq, ttl_ms}` |
| `lease.renewed` | `session_id` | `{status, token, seq}` (`status ∈ ok,stale,retired`) |
| `call.open` | sessão+ativação+`request_id` | `{parent, binding_id, cap, input, timeout_ms, lease, grant_rev, budget_ms, operation_id}` |
| `call.accepted` | sessão+`request_id` | `{status, persisted}` (`persisted` = admissão durável antes do ack) |
| `call.result` | sessão+`request_id` | `{status, output?, error?, terminal}` |
| `call.cancel` | sessão+`request_id` | `{reason, operation_id}` (perna indexada pela operação estável) |
| `stream.open` | sessão+ativação+`request_id` | `{stream_id, operation_id, direction, max_bytes, credit}` |
| `stream.data` | `session_id` | `{stream_id, seq, bytes, credit}` (bytes = base64) |
| `stream.credit` | `session_id` | `{stream_id, credit}` (receptor concede) |
| `stream.complete` | `session_id` | `{stream_id, status}` |
| `stream.cancel` | `session_id` | `{stream_id, reason}` |
| `event.deliver` | sessão+ativação | `{topic, payload, seq}` |
| `event.subscribe` | sessão+`request_id` | `{topics[]}` (controlador declara interesse) |
| `event.subscribed` | sessão+`request_id` | `{topics[]}` (executor confirma o conjunto vigente) |
| `op.query` | `session_id` | `{principal, operation_id}` |
| `op.result` | `session_id` | `{state, result?}` (`admitted,completed,unknown`) |
| `inventory.reconcile` | `session_id` | `{activations[], leases[], operations[], resources[]}` |
| `inventory.result` | `session_id` | `{revoked[], unknown[]}` |
| `revoke.notice` | `session_id` | `{target, fence}` |

Regras de validação (executável em `remote.rs`):

- `timeout_ms`, `ttl_ms`, `budget_ms` inteiros positivos ≤ 35 s de
  transporte; `credit`/`max_bytes`/`seq` inteiros ≥ 0 com teto 1 MiB
  por frame e limites negociados em `session.welcome.limits`.
- `stream.data.bytes` base64 válida; soma por stream nunca excede
  `max_bytes`; dados sem crédito ou com `seq` inválido são rejeitados.
  Emenda v0.1: `stream.data` carrega `operation_id` extra (ignorado por
  pares antigos) para terminar o chunk na sessão provedora dona da perna;
  sem ele, o chunk só contabiliza. Pernas são por direção (um id, uma
  direção, com sequência e janela próprias cada lado). O `stream.open`
  da tabela acima é anunciado mas não transacionado na v0.1: pernas
  nascem no primeiro chunk (janela inicial), como nos streams locais —
  handshake explícito fica para feature futura com fonte/offset.
- `call.open.budget_ms` é duração restante (nunca relógio absoluto);
  cada host aplica seu próprio prazo local.
- `operation_id` + conteúdo: duplicata com conteúdo divergente é erro
  (`operation-id-conflict`); consulta nunca reexecuta. Identidade
  composta inequívoca do controlador —
  `domínio:época:consumidor:instância:geração:pai:binding:request:len(sha256(...))`
  (nomes longos: `op-<sha256>` determinístico sobre a mesma pré-imagem).
  A época é o boot do controlador (entropia por boot): pais, bindings e
  contadores de request podem repetir após reinício, mas a época não —
  chamada nova nunca replays entrada antiga do ledger persistente.
  Ativação completa do consumidor acompanha por auditabilidade.
  — com política explícita de colisão: ids só colidem sob colisão SHA-256,
  e o executor ainda recusa conteúdo divergente sob id reutilizado
  (nega, nunca cruza duas chamadas distintas). O `request` é a identidade
  da invocação: duas chamadas intencionais iguais sob um pai têm requests
  distintos (duas operações, ambas executam); retransmissão no fio reusa o
  request (mesma operação, replay pelo dedup do portão, nunca segundo ticket).
- Resposta de geração/época antiga resolve auditoria, nunca atualiza
  ativação nova (validação no receptor, não no transporte).

## Ownership e autoridade no fio

- O consumidor escolhe só seu `binding_id` autorizado; rota e
  localização são resolvidas pelo controlador. Solicitação recebida do
  plugin não fabrica ancestrais ou grants: `call.open` carrega a cadeia
  completa (`parent` com domínio/ativação/ticket) e o executor revalida
  contra sua autoridade delegada (grant da aresta + revisão).
- O executor aceita chamadas só do controlador autorizado (mTLS +
  grant) e registra quem pode cancelar/consultar cada operação. O mapa
  operação→provedor guarda a referência completa (lógico, instância,
  geração, fence) e cada entrega revalida dono atual + lease viva:
  substituir o provedor nunca redireciona chunks antigos à nova ativação:
  a referência validada viaja até a seleção da sessão destino sem nova
  resolução por nome (gerações imutáveis, ids de sessão únicos) — retirada
  no intervalo recusa ou serve a sessão antiga (lineariza antes dela),
  nunca a nova;
  o mapa sobrevive ao terminal (pernas sobrevivem a chamadas) sob teto
  com despejo oldest-first.
- `call.accepted.persisted` distingue admissão durável de volátil; o
  aceite terminal no controlador revalida a cadeia local; o executor
  valida sua autoridade antes de aceitar efeito mediado.
- Cancel/retirada revoga participação local de imediato; avisar o
  executor não prova interrupção física. Pernas remotas em voo
  (admitidas, sem terminal) ficam pendentes até evidência de liquidação
  (terminal, cancel ou reconcile que as revogue); perda/expiração de
  autoridade bloqueia novos efeitos e inicia cleanup. Incerteza nunca
  vira Disposed.
- Handles remotos: a v0.1 não tem protocolo de aquisição remota (todo
  handle é local ao host que o emitiu e morre com sua sessão), de modo
  que o conjunto remoto de `inventory` é vazio por construção — o que se
  reconcilia como recurso retido são as próprias pernas (operações).
  A lease do executor é reconciliada pela sonda `lease-status` +
  `status` antes do registro e pelo loop de renew depois.
- Estados de rota: `Connected → Suspect → Detached`. Novas admissões
  falham fechado sem autoridade demonstrável. Falha de rota afeta seus
  consumidores; independentes seguem utilizáveis.
- Renovação (`lease.renew` com `seq`) progride independente de chamadas
  longas (canal de controle separado); renovação atrasada nunca revive
  ativação retirada; resposta perdida = reconciliação idempotente ou
  nova ativação, nunca rotação silenciosa com dois donos.
- Reconexão = nova `session_id` + `inventory.reconcile` antes de
  publicar bindings; não herda autoridade da sessão anterior.
- Streams não retomam implicitly: desconexão encerra como interrompido;
  operação pode seguir consultável pelo ledger.
- Diagnóstico correlaciona cadeia/hosts/sessões/leases/bindings/
  streams/recursos pendentes, sem credenciais nem payloads por padrão.

## Limites e progresso

Limites finitos por stream/operação/sessão/host/domínio (bytes
recebidos/retidos/enviados, operações simultâneas, ids de
deduplicação, callbacks, filas de controle) + tolerância máxima de
buffers de frame/TLS. Controle tem orçamento reservado e limitado.
Leitura/escrita com deadlines absolutos (lock + framing + TLS +
flush); frame/TLS parcial e falho invalida o canal, sem anexar
mensagens a sequência truncada. Garantias sujeitas a escalonamento.
