# Matrix

Kernel em Rust para composição espaço-temporal de componentes, com plugins independentes de linguagem e execução local ou remota.

**Estado: runtime experimental.** M1 e M2.5 têm implementação local; M3–M5 estão disponíveis no perfil gerenciado `matrix-managed`, com isolamento Linux, ledger durável e hosts por TLS. Consulte as garantias e limitações do [perfil gerenciado](docs/MANAGED-RUNTIME.md).

Um code agent será uma aplicação montada sobre o Matrix: modelo, ferramentas, loop de execução, memória e interface serão componentes. O núcleo mantém contexto, propriedade, dependências, autorização e ciclo de vida.

## Comece aqui

- [Mapa da documentação](docs/README.md)
- [Estado real do código](docs/STATUS.md)
- [Arquitetura proposta](docs/ARCHITECTURE.md)
- [Contrato e invariantes](docs/CONTRACT.md)
- [Primeiro marco e roadmap](docs/ROADMAP.md)
- [Protocolo v0.1 em elaboração](docs/PROTOCOL.md)

**Próxima aplicação: M6 — agente composto**, usando os perfis já validados. M3–M5 não tornam o loop simulado do SDK um agente real. [Operação, testes e configuração](docs/MANAGED-RUNTIME.md).

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
| `matrix-sdk` | Cliente, interfaces Model/Tool e sequência fixa de agente |
| `matrix-tui` | REPL e demonstração com modelo simulado |

[Direção do projeto](CHARTER.md) · [Plugins](PLUGIN.md) · [Referências](docs/REFERENCES.md)

O scaffold deriva conceitualmente do master3/agentlab. Seu score `0.812` não é um resultado do Matrix. Documentos originais foram preservados em [history](docs/history/README.md).
