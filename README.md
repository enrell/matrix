# Matrix

Kernel em Rust para composição espaço-temporal de componentes, com plugins independentes de linguagem e execução local ou remota.

**Estado: scaffold experimental.** O kernel descrito na documentação é o objetivo de implementação; a composição espaço-temporal completa ainda não está disponível. Esta revisão altera documentação, não o runtime.

Um code agent será uma aplicação montada sobre o Matrix: modelo, ferramentas, loop de execução, memória e interface serão componentes. O núcleo mantém contexto, propriedade, dependências, autorização e ciclo de vida.

## Comece aqui

- [Mapa da documentação](docs/README.md)
- [Estado real do código](docs/STATUS.md)
- [Arquitetura proposta](docs/ARCHITECTURE.md)
- [Contrato e invariantes](docs/CONTRACT.md)
- [Primeiro marco e roadmap](docs/ROADMAP.md)
- [Protocolo v0.1 em elaboração](docs/PROTOCOL.md)

**Primeiro trabalho: M1 — contextos, recursos e dependências locais.** A prova é remover um provedor durante uma chamada, desativar consumidores, limpar recursos gerenciados e preservar um componente independente. Rede e agente real vêm depois dessa base.

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

Validação existente: `cargo test -- --test-threads=1` e `make compat`. Não equivalem à conformidade proposta. Nenhum resultado de teste ou desempenho novo é reivindicado nesta revisão documental.

## Layout atual

| Crate | Hoje |
|---|---|
| `matrix-core` | Registry, dispatch compilado, estado, bus e journal básicos |
| `matrix-rt` | Daemon e CLI por socket Unix |
| `matrix-sdk` | Cliente, interfaces Model/Tool e sequência fixa de agente |
| `matrix-tui` | REPL e demonstração com modelo simulado |

[Direção do projeto](CHARTER.md) · [Plugins](PLUGIN.md) · [Referências](docs/REFERENCES.md)

O scaffold deriva conceitualmente do master3/agentlab. Seu score `0.812` não é um resultado do Matrix. Documentos originais foram preservados em [history](docs/history/README.md).
