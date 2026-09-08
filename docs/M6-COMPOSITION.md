# M6 — Composição local entre componentes externos: entrega

Data: 2026-09-06. Referência normativa: [M6.1-SPEC](M6.1-SPEC.md) (proposta
inalterada; desvios pontuais justificados abaixo). Demonstração:
`scripts/demo-composition.py` (perfil gerenciado, cadeia Rust → Python,
retirada e reintrodução). Fixtures genéricas: `dep_node` (Rust),
`dep_node.py` (Python); nenhuma aplicação de produto.

## O que foi entregue, por camada

| Camada | Entrega | Código |
|---|---|---|
| Schema | 5 tipos `dependency.*`, validação estrutural + semântica, vetores | `matrix-proto/src/dependency.rs`, `envelope.rs`, `tests/vectors.rs` |
| Negociação | `features` em hello/welcome, interseção, anúncio por política | `handshake.rs`, host `serve_conn`, SDKs anunciam |
| Política | `outbound: {request[], limits{7}}` no manifest; solicitado ≠ grant | `kernel.rs parse_outbound`, `outbound_policy_of` |
| Grants | `grant_outbound`/`revoke_outbound` com revisão; sync no serviço | `kernel.rs`, `service.rs sync_outbound_grants`, config `outbound_grants` |
| Bindings | handles `bind-N` emitidos na ativação, podados na retirada | `issue_dep_bindings`, `dependency_bindings_of`, activate |
| Admissão | `dependency_admit`: pai+binding+ativações+grant+quotas, reserva e revalidação atômicas sob mutex dedicado | `kernel.rs` |
| Tickets | metadados `DepChild`, revogação por pai/consumidor/grant/expiração | `calls.rs`, ganchos em close/cancel/settle/dispose/expire |
| Fronteiras | `dep_chain_check` no commit; `dependency_accept` (aceite único) e `dependency_timeout` no resultado | `kernel.rs` |
| Despacho | worker por abertura (limitado por quotas), `call.open` ao provedor, terminais traduzidos, cancel no fio | host `run_dep_open` |
| Cancel | mapeado/revoked, precoce/pendente, desconhecido/`unknown-request`; morte de sessão revoga e assenta | host `on_dep_cancel`, `drop_session` |
| SDKs | `dependencies()`, `invoke_dependency()` com herança de cancel e prazo local; `DepError`/`DepBinding`; recursos (`acquire/release_resource`) e eventos (`on_event` em despachante dedicado, guarda de thread leitora, contador de descarte) | `matrix-component`, `matrix_component.py` |
| Ativação | `dependency_bindings` no `lifecycle.activate` (só negociado) | host `serve_conn` |
| Diagnóstico | `dep.*` em chamadas e `dep_bindings` em instâncias no inventário; journal `dependency.*` | `kernel.rs inventory` |
| Gerenciado | anúncio habilitado, `outbound_grants` na config + SIGHUP, teste e demo | `service.rs`, `matrix-managed.rs` |

## APIs efetivas

Manifest: `outbound: {"request": ["cap@N", …], "limits": {max_depth,
max_children_per_parent, max_calls_per_session, max_calls_global,
max_seen_requests, max_queued_bytes, max_deadline_ms}}` (todos > 0).
Kernel: `dependency_bindings_of`, `dependency_admit(DepAdmit)`,
`dependency_accept`, `dependency_timeout`, `grant_outbound`,
`revoke_outbound`, `outbound_policy_of`, `call_reap` (M5.1, reutilizado).
Fio: `dependency.open/accepted/result/cancel/cancel.result` + `features`.
SDK Rust: `CallCtx::{ticket, dependencies, invoke_dependency}`,
`DepBinding{id, capability}`, `DepError{code, message}`. SDK Python:
mesmos nomes (`invoke_dependency(binding, input, timeout_s)`,
`DepError` com `.code`/`.message`).
Config gerenciada: `"outbound_grants": {"cons": ["prov.api@1"]}`.

## Desvios da spec, justificados

1. `parent_ticket` aceita eco `"tkt-N"` além do decimal `"17"`: o SDK ecoa
   o ticket recebido; parse leniente, validação estrita.
2. Cancel desconhecido → `dependency.result` erro `unknown-request`
   (não `state` em `cancel.result`): a tabela da spec restringe state a
   `revoked|terminal`; o código preserva a informação sem violar o schema.
3. Erro de negócio carrega `error.origin` (lógico do provedor): campo
   extra permitido; atribuição sem reinterpretar controle.
4. Prazo local do SDK (pedido + 10 s) com cancel no fio e
   `outcome-unknown`: robustez local; a spec silencia e o host impõe o prazo.
5. Sem pré-cheque de `in_flight` do consumidor no portão: pais
   encaminhados saturavam os slots e matavam as filhas por inanição
   (reproduzido: 5/16); quotas da admissão são autoritativas.
6. Re-grant gira revisão e revoga filhas superadas (semântica de rotação,
   como leases M5); revoke remove e invalida tudo sob a chave.
7. `acquire_external` exige a ativação exata (instância + geração) da
   sessão chamadora: pedido de sessão obsoleta falha fechado, e a
   publicação precede a revalidação final (espelho de `acquire`/B3), de
   modo que retirada concorrente revoga em vez de vazar.
8. `resource.release` valida ownership contra a sessão (instância, lógico
   e geração): handle alheio ou obsoleto nega com `permission-denied`.
9. Respostas com payload remoto (outputs, erros de negócio, eventos)
   passam pela quota de egresso da sessão destino; estouro rebaixa para
   erro local (`resource-exhausted`) ou descarta o evento (best-effort).
   Erros locais pequenos nunca entram na quota (orçamento de controle).
10. `on_event` roda em thread despachante dedicada com fila limitada
    (64, descarta mais antigos e conta); chamadas bloqueantes feitas na
    thread leitora recusam de imediato em vez de travar a leitura.
11. Prazo absoluto de envio (`write_frame_deadline`): espera limitada pelo
    mutex + sends não bloqueantes com `poll` limitado ao saldo restante;
    conclusão após o deadline conta como `Timeout` (com envenenamento);
    mutex envenenado falha em vez de escrever em stream rasgado.

## Limites e backlog explícito

- Filhas remotas, federação, WASM e otimizações: fora (epic).
- M6.4 formal (conformidade temporal completa): futuro; cascata,
  substituição e reintrodução estão cobertas por testes (`dep_flow`).
- Snapshot do CLI ainda equivale a abertura de recuperação (R5, backlog — resolvido na M8: snapshot virou somente-leitura, ver `M8-COMPOSITION.md` P09).
- Fuzz de parser, duplicata entre sessões (C12, C14): backlog da auditoria
  M5.1. Consumidor-lento adversarial (C13) coberto por
  `slow_consumer_never_blocks_control`.
- Ação remota `emit` não existe no serviço gerenciado: eventos a
  componentes externos fluem de `emit` local/CLI; entrega remota de
  eventos é trabalho futuro (não prometido).

## Evidência

- `make test`: 164 passed / 0 failed no Cargo + 4 passed no Python
  (`python3 sdk-python/test_units.py`, incluído no alvo): núcleo
  `m61_admission` 14, proto `frame` (deadline/poison) + `handshake`, host
  `dep_flow` 18 + `dependency` 10 + `sdk_units` 4,
  gerenciado `dep_managed` 1, vetores proto, regressão M1–M5 intacta.
- `make compat` PASS; `scripts/smoke-managed.py` PASS; zero warnings.
- `scripts/demo-composition.py` PASS: cadeia Rust → Python, retirada com
  `cancelled` sem sucesso falso, independente responsivo, reintrodução
  com nova geração (lease antigo rejeitado com `stale-generation`),
  recursos adquire/libera/nega.

## Fechamento M6 (epic de revisão)

Achados da revisão integrada corrigidos após a entrega inicial:

- **Progresso do transporte**: nenhuma escrita retém o lock global de
  sessões (writer `Arc` por sessão clonado sob lock breve); prazo absoluto
  de envio (`write_frame_deadline`: espera de mutex + `send(MSG_DONTWAIT)`
  não bloqueante com `poll(POLLOUT)` limitado ao saldo restante — nem
  SNDTIMEO isolado (o Linux o rearma a cada progresso parcial; um `write`
  de 4 MB levou 49 s com SNDTIMEO de 500 ms) nem fatias bloqueantes
  bastam, pois uma única chamada bloqueante não tem limite temporal;
  conclusão tardia conta como `Timeout`, não `Ok`); frame com falha
  envenena a conexão (`shutdown`), nunca anexa outro frame a um parcial;
  controle e outras sessões nunca congelam por consumidor lento (repros
  `slow_consumer_never_blocks_control`, `saturation_control_progresses`,
  `deadline_holds_across_partial_writes` (orçamento 500 ms + tol. 400 ms),
  `failed_frame_poisons_connection`, `zero_budget_fails_closed_without_hanging`).
- **Quotas fiscalizadas nos dois sentidos**: `max_queued_bytes` limita
  bytes destacados por sessão na ida (recusa `resource-exhausted`,
  guarda RAII) e na volta (rebaixamento determinístico; eventos
  descartados). Controle nunca conta: orçamento reservado finito e
  documentado (repros `send_quota_refuses_and_releases`,
  `egress_quota_downgrades_big_output`).
- **Protocolo completo**: o host emite `dependency.accepted` após admitir,
  antes do terminal (teste de ordem `accepted_before_terminal_in_order`).
- **M6.3 entregue**: recursos externos (`resource.acquire/release/result`
  com ownership validado na fronteira, geração exata, teto por contexto
  e liberação na queda da sessão) e eventos (`event.deliver` via
  `EventSink` do kernel para tópicos assinados), nos SDKs Rust/Python com
  paridade (`dep_node`/`dep_node.py`, `on_event` em despachante dedicado,
  `acquire_resource`/`release_resource`, guarda de thread leitora;
  demo passo 4).

Desvios adicionais da spec, justificados:

12. `parent_ticket` numérico inválido no fio é descartado em silêncio
   (regra M2.3 para lixo); o diagnóstico do descarte segue best-effort.
   (Achado lateral: testes crus devem serializar `generation`/`instance_id`
   como strings decimais — a spec já exige; o descarte estrito funcionou
   como projetado.)
13. Eventos a componentes externos usam o tipo `event.deliver`
   (`message_id`, `session_id`, `instance_id`, `generation`,
   `body{topic, payload}`), fora da M6.1-SPEC e documentado aqui.
