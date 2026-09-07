# Contrato de plugins e SDKs

Status: proposta v0.1. Para escrever um manifest aceito hoje, ver [PLUGIN](../PLUGIN.md).

## Manifest conceitual

```json
{
  "manifest_version": "0.1",
  "id": "consumer",
  "component_version": "0.1.0",
  "execution": {"kind": "process", "entrypoint": "consumer"},
  "requires": [
    {"interface": "data.read", "major": 1, "provider": "provider"}
  ],
  "provides": [{"interface": "consumer.run", "major": 1}],
  "permissions_requested": ["data.read"],
  "restart": "on-failure"
}
```

Exemplo não executável no scaffold. Configuração de implantação resolve entrypoint, versões, hosts, quotas e grants; um manifest recebido pela rede não autoriza executar um caminho arbitrário.

## API conceitual

```text
prepare(context, configuration, bindings) -> prepared
activate(prepared) -> ready
handle(context, operation, input, cancellation) -> result/stream
quiesce(context, reason, budget) -> acknowledgement
dispose(context) -> cleanup_report
```

Context oferece `provide`, `subscribe`, `spawn_task`, `acquire_resource` e `invoke`. Cada aquisição retorna handle opaco com owner e inversa gerenciada. SDK pode liberar ergonomicamente via RAII/with/context manager, mas o kernel/host mantém accounting e cleanup se o callback não rodar.

Não cruzar fronteiras de processo com ponteiros, closures Rust ou referências de memória. Interfaces possuem schemas e versões. SDKs traduzem mensagens, streaming, erros e cancelamento preservando semântica.

## Chamadas a dependências (M6)

Componentes externos invocam dependências vinculadas através do kernel
(`dependency-calls/1`, negociada em hello/welcome e anunciada por política
do host). O manifest declara intenção e limites:

```json
"outbound": {"request": ["prov.api@1"], "limits": {
  "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
  "max_calls_global": 64, "max_seen_requests": 64,
  "max_queued_bytes": 65536, "max_deadline_ms": 12000}}
```

Solicitado não é autorizado: o operador concede por
(consumidor, capability) — no perfil gerenciado, `"outbound_grants"` na
configuração. O host entrega handles opacos no `lifecycle.activate`;
o handler chama pelo handle do contexto corrente:

```rust
let out = ctx.invoke_dependency(&ctx.dependencies()[0].id, input, timeout)?;
```

```python
out = ctx.invoke_dependency(ctx.dependencies()[0]["id"], input, timeout_s)
```

Erros preservam código/mensagem do provedor (`DepError`); pai alheio,
handle de outra ativação e grant ausente negam sem despachar; retirada,
expiração e revogação invalidam a cadeia (resultado tardio descartado).
Demonstração: `scripts/demo-composition.py`. Entrega: [M6-COMPOSITION](M6-COMPOSITION.md).

## Recursos e eventos da ativação (M6.3)

O componente adquire recursos do próprio contexto pelo host (mesmas
regras e propriedade do núcleo, com teto por contexto; retirada libera
tudo, inclusive na queda da sessão):

```rust
let timer: u64 = ctx.acquire_resource("timer", "t1", Some(50))?;
ctx.acquire_resource("sub", "sys.tick", None)?;
ctx.release_resource(timer)?;
```

```python
timer = ctx.acquire_resource("timer", "t1", 50)
ctx.acquire_resource("sub", "sys.tick")
ctx.release_resource(timer)
```

`kind` é `cap`/`sub`/`timer`/`task` (`timer` exige `interval_ms` positivo;
`fail-*` é recusado no fio). Eventos de tópicos assinados no manifest
chegam via `on_event` (Rust: `Handler::on_event`; Python:
`Handler.on_event`) num despachante dedicado (fila 64, descarta mais
antigos e conta em `event_dropped_count()`): observar rápido, nunca
bloquear. Entrega best-effort após o despacho in-proc; retirada revoga inscrições.

## Streams e eventos remotos (M7)

Chunks endereçados à ativação chegam via `on_stream` no mesmo
despachante dos eventos (Rust: `Handler::on_stream(stream_id, seq,
payload)`; Python: `Handler.on_stream`): observar rápido — o host só
concede mais crédito à medida que a fila drena, de modo que lentidão
estrangula o emissor em vez de crescer memória. `pending_stream_count()`
expõe a pressão; descarte conta no mesmo contador de eventos.

```rust
ctx.send_stream("s-1", 0, "payload")?;
```

```python
ctx.send_stream("s-1", 0, "payload")
```

Remoto atravessa sem API nova no componente: o mesmo `invoke_dependency`
/ `send_stream` / `on_event` / `on_stream` compõe local e remoto; rota,
autoridade e reconciliação ficam no host/serviço. Paridade Rust/Python
verificada (`sdk_units` 6+6, `dep_node`/`dep_node.py` com `stream_send`,
`--stream-log`, `--stream-slow-ms`). Entrega: [M7-COMPOSITION](M7-COMPOSITION.md).

## Estratégia de linguagens

M1 usa componentes Rust compilados e confiáveis para provar a semântica. M2 introduz hosts/processos e SDKs Rust e Python com a mesma suíte. M3 reforça isolamento. WASM Component Model/WIT é opção posterior com o mesmo contrato de recursos; suporte de cada linguagem deve ser verificado antes de anunciar compatibilidade.

Não usar ABI nativa Rust de bibliotecas dinâmicas como contrato universal. Cada perfil deve informar suporte real a limites, cancelamento, cleanup e migração.

## Integração em aplicações externas

Aplicações vivem em repositórios separados e definem suas interfaces de domínio. O SDK oferece contratos genéricos de composição, recursos e lifecycle.

Conformidade usa fixtures mínimas de provedor, consumidor e componente independente, verificando chamadas, retirada, limpeza e reintrodução entre linguagens. Nenhuma aplicação de produto é requisito de aceitação do SDK.
