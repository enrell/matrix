# ML1 composition record (multilanguage adoption delivery)

Status: ML1 implemented. Normative epic: `MULTILANGUAGE-EPIC.md`;
language/profile matrix: `ML1-MATRIX.md`; node contract: `ML1-NODE.md`.
All code/comments in English; this record in Portuguese (docs rule).

## Result

Um desenvolvedor instala o pacote da sua linguagem, inicia ou conecta
a um Matrix local, implementa componentes e compõe dependências sem
escrever framing, gerenciar tickets manualmente ou estudar o código
Rust. A aplicação permanece em outro repositório. Identidade,
autoridade, recursos, lifecycle e recuperação continuam sob
fiscalização do kernel.

Entrega: Python (consolidado), JS/TS, Go, Crystal, Elixir, C# e C/C++
(um pacote, duas entradas), mantendo Rust como referência. Cada SDK
suporta aplicação hospedeira e implementação de componentes, com
instalação, bootstrap, scaffolding e diagnóstico próprios.

## Acceptance (L01–L12)

| ID | Resultado | Evidência |
|---|---|---|
| L01 | Todos os SDKs distribuíveis | `scripts/harness-ml1.sh` fase L01: venv+wheel `--no-index`, `npm install --offline` de tarball, `go build/vet/test` com `GOPROXY=off`, `crystal spec`, `mix test`, `dotnet run` SelfTest, `cmake+ctest` C/C++; `sha256sum -c` sobre `ml1/` do MANIFEST |
| L02 | Bootstrap e attach corretos | Suites operador ao vivo por SDK (start/attach/close contra daemon real) + `L02 no-orphans` (nenhum `matrix-managed` restante) |
| L03 | Primeira composição por contrato público | 8 cadeias same-language (`same-<lang>`) a partir de projetos gerados: handler + dependência, sem internals |
| L04 | Tipos e protocolo equivalentes | `matrix-conform vectors` + `local` verdes; suítes por SDK: u64 máximo, frame malformado, geração obsoleta, gate de feature |
| L05 | Autoridade preservada | Por serviço e por SDK: lease morto, capability não concedida e geração obsoleta negados com códigos estáveis |
| L06 | Lifecycle sob falha | `lifecycle` (py, c): retirada sustentada invalida a cadeia, indep isolado responde, restart supervisionado (nova geração) serve, refs antigas morrem; `indep-isolated` nas demais linguagens |
| L07 | Cancelamento e saturação | Flood `stream_send` 32×4KiB com controle concorrente progredindo (js, ex, c); kill mid-sleep → terminal não-ok + mesma operação nunca repete com sucesso fantasma |
| L08 | Streams e eventos | `stream_send` limitado por SDK; bidi local (`chained` + `stream_sent`) por SDK; bidi `chain_with_streams` sobre perna remota com associação observada no log do provedor (`remote/s1`) |
| L09 | Integração entre linguagens | 8 pares direcionados (py↔js, go↔cs, cr↔ex, c↔cpp) + Rust nos dois papéis + cadeia de três (js→go→rs) |
| L10 | Local/remoto | Mesmo código de negócio (nós gerados) sob rotas; partição → unknown, sem replay; reconexão (restart do par com homes frescas) → nova geração serve, refs antigas mortas |
| L11 | Compatibilidade | Gate sem feature por SDK; Rust (geração anterior) compõe nos dois papéis; `VERSIONS.md` publica `0.1.0-experimental` + regra de refusal |
| L12 | Uso e diagnóstico | 8 `scaffold.sh` executados de ponta a ponta; 8 `doctor` verdes contra binário staged; `no-secrets` no log do harness |

Evidência bruta: `scripts/harness-ml1.sh` (**139 passed, 0 failed**,
`harness-ml1: 139 passed, 0 failed`, `HARNESS_EXIT=0`), `make test`
(cargo + Python + `scripts/test-ml1.sh`), `make compat`,
`matrix-conform local`, demos M6/M7, `check-harness-bounds.sh`,
harness M8 (`harness-external.sh`) como regressão.

## Decisions (autonomous, integrated review at delivery)

- Operador via binário staged: nenhuma SDK reimplementa mTLS por
  linguagem; `start`/`serve` + `request` pelo `matrix-managed`
  empacotado. Autoridade, leases, fences e gerações funcionam
  exatamente como o contrato CLI define.
- PKI do operador sempre explícita: identidade do servidor nunca
  implica autoridade do chamador (o SDK Python recusava
  silenciosamente o contrário num rascunho — corrigido antes de
  provar; viraria violação de L05).
- Fila limitada no leitor (Elixir): a primeira versão enfileirava na
  mailbox do dispatcher (ilimitada, drops nunca disparavam) e o pump
  lia antes do `serve` (condição de corrida descartava `call.open`).
  Ambos viraram testes de regressão no `mix test`.
- Crystal `Process#wait` fecha os pipes: drenagem concorrente antes
  do reap; `run_capture` com deadline absoluto mata passado dele.
- Elixir `:json.encode` devolve iodata: achatado antes de framing/argv
  (quatro lugares).
- Dispatcher Elixir agrega 50ms após idle: um take é um round trip de
  microssegundos (ao contrário dos pops lentos dos outros SDKs), de
  modo que sem agregação uma rajada podia ser drenada antes de
  acumular e o overflow nunca disparava (flake `dropped=0` capturado
  uma vez no harness). Após idle, o primeiro take espera a rajada
  acumular; backlog drena em velocidade plena. Atraso de 50ms só no
  primeiro lote após idle; eventos continuam best-effort.
- C++ é RAII sobre o transporte C (mesmo pacote, `matrix.hpp`
  header-only): `MX_ERR_UNSUPPORTED` e outros status locais mapeiam
  para códigos explícitos, nunca `internal` genérico.
- `chain_with_streams` implementado em todos os nós (não só
  referência): a prova bidi roda sobre perna remota.
- Escript Elixir precisa do `escript` (OTP) no PATH: o scaffold gera
  `run-node.sh` com PATHs absolutos staged (validação same-machine,
  sem downloads silenciosos).
- Limite de 100 restarts do `RestartPolicy` respeitado pelo harness
  (5 restarts, janela 30s, backoff 200ms).
- Bug real capturado pelo harness no SDK C (e C++, mesmo
  transporte): `waiter_wait` construía abstime com `CLOCK_MONOTONIC`
  para uma condvar no relógio default (`CLOCK_REALTIME`) — cada slice
  expirava na hora e a "espera" degradava para um spin de ~5ms que só
  vencia hosts em-processo por sorte. Medido no fio: `st=6
  elapsed_ms=5` num orçamento de 18s. Correção: condvar em
  `CLOCK_MONOTONIC` (imune a saltos de relógio de parede) + códigos
  explícitos nos caminhos locais de timeout/cancel (`outcome-unknown`
  / `cancelled`, nunca NULL). Prova pós-correção: bidi com inner de
  1.5s responde em ~1.7s com `chained` + `stream_sent`.

## Deviations from the epic (recorded, none load-bearing)

- Eigentlich "um pacote por ecossistema": C e C++ compartilham um
  pacote (mesmo ecossistema CMake/pkg-config), com duas entradas
  documentadas na matriz. A epic permite ("C++ pode oferecer uma
  camada RAII sobre esse cliente C").
- Eventos ao vivo via serviço gerenciado: sem RPC de broadcast no
  perfil `matrix.managed/0.1`, a entrega a assinantes é provada em
  nível de unidade (flood `event.deliver` com drops contados) e no
  `matrix-conform local` (facade `broadcast`); o harness gerenciado
  prova streams ponta a ponta. Limitação honesta, não lacuna de
  cobertura.
- `matrix-conform` e vetores reutilizados sem alteração (o fio não
  mudou); o contrato comum dos SDKs é provado pelo comportamento
  idêntico dos nós, não por vetores novos.
- Reload (SIGHUP) não é atômico em relação a leases em voo: nos
  primeiros segundos após aplicar nova configuração, invocações com
  leases válidos podem responder `stale-generation` transitório até
  os novos bindings assentarem; a forma assentada é `ok:false`
  (grant revogado) ou `ok:true`. O harness espera a forma assentada
  em vez de afirmar um único tiro. Validate-before-mutate da
  configuração continua valendo; estabilidade de lease através do
  reload não é prometida pelo contrato M8.
- Operation ids fixam o terminal (deduplicação): sondar MUDANÇA DE
  ESTADO com o mesmo id repete para sempre a primeira resposta
  gravada (o harness chegou a "provar" que revoke não funcionava com
  um único id repetido 30 vezes). Sondas de mudança usam um id
  inédito por tentativa; reemissão idêntica é o teste de
  não-replay, não de sondagem.
- Chaves do relatório doctor são idiomáticas por linguagem (JS
  `cliShapeOk`, demais `cli_shape_ok`); o harness aceita ambas.
- Retirada é provada por morte sustentada do provedor (kill em loop
  durante a janela de sondagem: o supervisor reinicia em ~200ms, um
  único tiro disputaria com o respawn), não por SIGHUP — ver
  follow-up kernel abaixo.

## Kernel follow-up (out of ML1 scope, do not fix here)

- Reload sob concorrência (achado, fora do escopo ML1): sob a
  concorrência deste harness (~19 daemons + sondas ativas), o SIGHUP
  deixa de produzir qualquer efeito observável — sem linha no log do
  daemon (`reload revoked/partial/rejected` ausentes), sem revogação,
  com HUP entregue ao PID correto (cmdline verificada) e requests
  intactos. Em daemons ociosos o mesmo fluxo revoga em ~2s (M8 P07 e
  sondas controladas desta entrega). Hipótese principal: contenção ou
  deadlock no caminho do reload da fachada sob carga, a investigar no
  núcleo (M8/M9). O harness ML1 não depende dele: retirada é provada
  por morte sustentada do provedor, e reconexão remota por restart do
  par com homes frescas (reutilizar o home de um daemon morto carrega
  leases/sessões obsoletas que emperram o resume). Nenhum código do
  kernel foi alterado nesta epic.
- Streams ligam-se à perna da operação em voo: chunks sem perna
  ativa afundam localmente (regra M7 single-leg, verificada ao vivo:
  perna echo-rápida completa antes do primeiro chunk e nada entrega;
  perna lenta entrega tudo). Registrado em `ML1-MATRIX.md`.

## Runbooks

```sh
# regressão completa (repo): cargo + python + todos os SDKs
make test
# só SDKs (hermético; partes ao vivo contra build local)
./scripts/test-ml1.sh
# empacotar (bins + SDKs + MANIFEST)
./scripts/package.sh
# adoção externa (fora do checkout; ~6 min)
HARNESS_DIR=/tmp/mxml1 ./scripts/harness-ml1.sh
# fronteira contratual
./scripts/check-harness-bounds.sh
```

Pré-requisitos do harness: Linux x86_64, toolchains (rust, python3
com venv, node+npm, go, crystal, elixir+mix+OTP>=27, dotnet, cmake,
cc/c++, openssl, sha256sum, pgrep/pkill). Sem rede (tudo `--offline`,
`GOPROXY=off`, sem restores).

## Backlog (explicit, not acceptance)

- `mix.exs` do pacote Elixir carrega `package: [licenses: []]`
  vazio: sem licença no repo, sem publicação — quando houver decisão
  de licença, preencher aqui, no `pyproject.toml` e nos demais.
- `shard.yml` sem campo `license` pelo mesmo motivo.
- Browser/Deno/Bun, Windows/macOS, WASM: não reivindicados.
- Payload binário segue recusado explicitamente (feature opcional
  futura, nunca corrupção silenciosa).
- Demo Elixir `latin1` warning sob `env -i`: cosmético (PATH de
  locale ausente no sandbox do teste); `run-node.sh` exporta
  `ELIXIR_ERL_OPTIONS=+fnu`.
