# Plugins do Matrix

## Scaffold atual

Hoje um manifest escolhe um reducer já compilado em `matrix-core`; não carrega código Rust/Python externo.

```json
{
  "id": "greeter",
  "version": "1.0.0",
  "capabilities": ["greet.hello@1"],
  "subscriptions": [],
  "reducer": "echo",
  "init_state": {},
  "tier": "inproc",
  "trust": "trusted",
  "restart": "permanent"
}
```

Salve em `plugins/greeter.json` e use o daemon local:

```sh
./target/release/matrix-rt reload --json
./target/release/matrix-rt invoke 'greet.hello@1' '{"ping":true}' --json
```

Reducers disponíveis no dispatch examinado: echo, counter, clock, ancient, model e noop; crasher é um caminho especial de injeção de falhas. `tier`, `trust` e `restart` não fornecem isolamento ou supervisão completos. O reload atual pode manter registros antigos; não usá-lo como prova de cleanup.

## Dependências (`requires`, M1.2)

Um manifest pode declarar requisitos obrigatórios. Sem provedor válido, a
instância fica em `Waiting` (cap não publicada, chamadas rejeitadas com
`dependency-unavailable`); com provedor, ativa e segue o ciclo de vida.
Remover o provedor retira automaticamente os consumidores (novas gerações
em `Waiting`) e reintroduzi-lo os reativa com bindings novos.

```json
{
  "id": "search",
  "version": "1.0.0",
  "capabilities": ["search.query@1"],
  "requires": [{"interface": "workspace.fs@1", "provider": "workspace"}],
  "subscriptions": [],
  "reducer": "noop",
  "init_state": {},
  "tier": "inproc",
  "trust": "trusted",
  "restart": "permanent"
}
```

- Cada item de `requires` é `"iface@major"`, `"iface"` (qualquer major) ou
  objeto `{"interface": ..., "major": N, "provider": "id-logico"}`.
- Sem `provider`, o resolvedor exige provedor único; dois ou mais provedores
  da mesma interface deixam o consumidor em `Waiting` (`ambiguous-provider`).
- Versão incompatível, provedor ausente/inativo e ciclos (`a -> b -> a`)
  mantêm `Waiting` com razão visível em `status`/`inventory`.
- `provides: [{"interface": "x.y", "major": 1}]` é aceito como alternativa a
  `capabilities`.
- Remoção administrativa: `./target/release/matrix-rt remove <id>`.

## Chamadas em voo e políticas (`calls`, M1.3)

`call_open` emite um ticket vinculado a instância, geração, contexto e
autorização. A retirada (`Quiescing`) revoga admissão na hora; o ticket
segue a política da operação, declarada por capability no manifest:

```json
"calls": {
  "search.query@1": {"on_withdraw": "drain", "drain_ms": 200}
}
```

- `on_withdraw: "cancel"` (padrão, `drain_ms: 0`): revoga de imediato e
  sinaliza o worker; `commit_effect` tardio é rejeitado (`cancelled`).
- `on_withdraw: "drain"`: a chamada pode concluir e commitar dentro de
  `drain_ms` mesmo em `Quiescing`; vencido o prazo expira
  (`deadline-exceeded`) e a instância segura `CleanupPending` até aquietar.
- `commit_effect` revalida ticket, geração e estado a cada commit; a
  tentativa tardia é rejeitada E jornalizada (`call.rejected`).
- `call_close` encerra o acompanhamento e finaliza o dono pendente
  (`Disposed`, nunca reativa a descartada — só o reconciliador cria nova
  instância).
- `pins: [handles]` na abertura impedem `release` prematuro
  (`resource-pinned`); limites finitos: 256 chamadas em voo, anel de 1024
  efeitos (`resource-exhausted` além disso).

## Contrato futuro

[SDK e manifest propostos](docs/SDK.md) · [Protocolo](docs/PROTOCOL.md) · [Ciclo de vida](docs/LIFECYCLE.md)

O contrato futuro exige propriedade, dependências e descarte verificáveis. Seus exemplos usam outro formato e não são aceitos pelo scaffold atual. [Estado detalhado](docs/STATUS.md).
