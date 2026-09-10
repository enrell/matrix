# Matrix

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](LICENSE-MIT)

Kernel em Rust para composição espaço-temporal de componentes, com plugins
independentes de linguagem e execução local ou remota. Aplicações e
componentes de negócio vivem em repositórios separados — aqui mora o núcleo:
contexto, propriedade, dependências, autorização e ciclo de vida.

**Estado: runtime experimental (`0.1.0`).** Sem promessa de estabilidade `1.0`.
O que cada superfície garante está em [docs/VERSIONS.md](docs/VERSIONS.md);
limitações conhecidas em [docs/M8-COMPOSITION.md](docs/M8-COMPOSITION.md).

## Instalação

SDKs em 9 linguagens, dual-licenciados MIT OR Apache-2.0. Publicação nos
registries está em andamento; hoje, instale dos artefatos locais:

```sh
./scripts/package.sh
```

Depois siga o guia por ecossistema em [docs/INSTALL.md](docs/INSTALL.md)
(venv + wheel offline, `npm install --offline` do tarball, `GOPROXY=off`,
`shards`/`mix`/`dotnet`/`cmake` sem rede). Cada SDK tem um `scaffold.sh`
que gera um projeto funcional a partir de template.

## Uso em 5 minutos

```sh
./scripts/dev-pki.py /tmp/pki --server-name localhost
# edite um config.json (exemplo em sdk-python/templates/config.json)
./target/release/matrix-managed serve /tmp/config.json
```

Ou via SDK Python:

```python
from matrix_operator import start
kernel = start("/path/to/matrix-managed", config_dict, operator_pki)
act = kernel.client.activate("prov", 30000)
v = kernel.client.invoke(act["lease"], act["fence"], "op-1",
                         "prov.echo@1", {"ping": 1})
kernel.close()
```

O demo fim-a-fim (`python3 scripts/demo-composition.py`) mostra chain
Rust→Python com withdraw e reintrodução em ~1 minuto.

## Documentação

- [Mapa da documentação](docs/README.md) · [Estado real do código](docs/STATUS.md)
- [Contrato e invariantes](docs/CONTRACT.md) · [Protocolo](docs/PROTOCOL.md)
- [Perfil gerenciado (operação)](docs/MANAGED-RUNTIME.md) · [Plugins](PLUGIN.md)
- [Conformidade dos SDKs](CONFORMANCE.md) · [Verificação do kernel](KERNEL_VERIFICATION.md)

## Layout

| Crate | Papel |
|---|---|
| `matrix-core` | Contextos, recursos, dependências, tickets e lifecycle local |
| `matrix-rt` | Daemon e CLI por socket Unix, perfil confiável (legado) |
| `matrix-host` / `matrix-component` | Host e SDK de processos locais |
| `matrix-guard` | Sandbox Linux e orçamento de supervisão |
| `matrix-runtime` | Serviço gerenciado: SQLite, TLS mútuo, leases (`api` é a fachada pública) |
| `matrix-sdk` | Cliente legado do daemon (compatibilidade congelada) |

Validação canônica: `make test`. Compatibilidade: `make compat`.

## Licença

MIT OR Apache-2.0 — veja [LICENSE-MIT](LICENSE-MIT) e
[LICENSE-APACHE-2.0](LICENSE-APACHE-2.0). Vale para todos os SDKs e crates;
escolha a que preferir, sem copyleft: uso em projeto fechado é permitido,
basta preservar os avisos.
