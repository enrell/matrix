# Matrix

Kernel em Rust para composição espaço-temporal de componentes, com plugins independentes de linguagem e execução local ou remota.

**Estado: runtime experimental.** M1 e M2.5 têm implementação local; M3–M5 estão disponíveis no perfil gerenciado `matrix-managed`, com isolamento Linux, ledger durável e hosts por TLS. Consulte as garantias e limitações do [perfil gerenciado](docs/MANAGED-RUNTIME.md).

Matrix é somente o kernel genérico. Aplicações e componentes de negócio vivem em repositórios separados. O núcleo fornece contexto, propriedade, dependências, autorização e ciclo de vida para qualquer domínio que precise de composição espacial e temporal, dentro dos perfis suportados.

## Comece aqui

- [Mapa da documentação](docs/README.md)
- [Estado real do código](docs/STATUS.md)
- [Arquitetura proposta](docs/ARCHITECTURE.md)
- [Contrato e invariantes](docs/CONTRACT.md)
- [Primeiro marco e roadmap](docs/ROADMAP.md)
- [Protocolo v0.1 em elaboração](docs/PROTOCOL.md)

**Próximas fases: consolidação e composição genérica entre processos.** Consulte o [plano do kernel](docs/NEXT-PHASES.md) e a [operação dos perfis atuais](docs/MANAGED-RUNTIME.md). Abstrações de agente do scaffold legado (`Model`, `Tool`, `EchoTool`, `CannedModel`, `Agent`) e o crate `matrix-tui` (incluindo `--agent`) foram removidos em 2026-09-06 conforme o [plano de remoção](docs/KERNEL-CLEANUP.md); não fazem parte da API do kernel.

## Executar o scaffold existente

Requer Rust/Cargo e ambiente Unix. Estes comandos são a interface atual, não implementações do protocolo futuro:

```sh
cargo build --release
export MATRIX_RT_HOME="$PWD"
./target/release/matrix-rt run --json
```

Em outro terminal, na mesma pasta, com a mesma variável:

```sh
./target/release/matrix-rt status --json
./target/release/matrix-rt invoke 'echo.msg@1' '{"ping":true}' --json
./target/release/matrix-rt emit sys.tick '{}' --json
./target/release/matrix-rt invoke 'count.state@1' '{}' --json
./target/release/matrix-rt quit --json
```

Validação canônica: `make test` (release, exemplos atualizados, `--test-threads=1`). O [smoke gerenciado](scripts/smoke-managed.py) verifica a CLI, TLS, deduplicação, efeito com fencing e snapshot. Desempenho WAN e equivalência formal não foram demonstrados.

## Layout atual

| Crate | Hoje |
|---|---|
| `matrix-core` | Contextos, recursos, dependências, tickets e lifecycle local |
| `matrix-rt` | Daemon e CLI por socket Unix, perfil confiável |
| `matrix-host` / `matrix-component` | Host e SDK de processos locais |
| `matrix-guard` | Sandbox Linux e orçamento de supervisão |
| `matrix-runtime` | Serviço gerenciado, SQLite e transporte TLS |
| `matrix-sdk` | Cliente genérico do daemon (socket Unix): `rpc`, `invoke`, `emit`, `status` |

[Direção do projeto](CHARTER.md) · [Plugins](PLUGIN.md) · [Referências](docs/REFERENCES.md)

> **Quebra de API (2026-09-06, [plano de remoção](docs/KERNEL-CLEANUP.md)):** `matrix-tui`, a flag `--agent` e os símbolos `Model`, `Tool`, `EchoTool`, `CannedModel` e `Agent` deixam de existir, sem shim. Consumidores externos desses símbolos precisam manter seu próprio código de aplicação; compatibilidade do protocolo do daemon e do contrato de plugins continua sendo requisito.

O scaffold deriva conceitualmente do master3/agentlab. Seu score `0.812` não é um resultado do Matrix. Documentos originais foram preservados em [history](docs/history/README.md).
