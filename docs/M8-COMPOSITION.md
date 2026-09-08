# M8 — Contrato público e adoção independente: entrega

Data: 2026-09-08. Referência normativa: [M8-EPIC](M8-EPIC.md) (aceitação
P01–P12). Base: [M6](M6-COMPOSITION.md), [M7](M7-COMPOSITION.md),
[contrato semântico](CONTRACT.md). Demonstração:
`scripts/harness-external.sh` (projeto fora do checkout, só artefatos
`dist/` + documentação pública).

## Resultado

Uma aplicação externa instala Matrix de artefatos locais, integra por
fachada pública (biblioteca) ou CLI administrada (serviço), compõe
Rust/Python nos perfis local e remoto, e opera (configura, atualiza,
diagnostica, backup/restaura, revoga) sem ler implementação, sem
tabelas internas e sem `PYTHONPATH` para o checkout. Nada de produto,
marketplace, federação ou reescrita arquitetural.

## O que foi entregue, por área

| Área | Entrega | Código |
|---|---|---|
| Fachada | `matrix_runtime::api`: `Config` + `validate`, `Runtime` (start/invoke/inspect/shutdown), `ErrorCode` estáveis, `ReloadReport`, `ShutdownReport`, `InspectOpts`, `broadcast`, `wait_session`, `provision`/`remove`, `snapshot_backup`, `backup_offline`/`restore_backup` | `matrix-runtime/src/api.rs`, `tests/api_facade.rs` |
| Catálogo/Versões | Suportada/experimental/interna/legada por item; política 0.x sem 1.0; `publish = false` (sem licença no repo, sem registry) | `docs/API-CATALOG.md`, `docs/VERSIONS.md` |
| Python | `pyproject.toml` + README + wheel local instalável sem PYTHONPATH | `sdk-python/`, `dist/py` |
| R5/backup | Snapshot somente-leitura da origem (hash idêntico), restore validado (nunca sobrescreve), autoridade antiga morta, corrompido recusado, snapshot online pelo dono | `store.rs` (`OfflineBackup`), CLI `snapshot`/`restore`, `tests/backup_restore.rs` |
| Config | `validate` antes de mutar (boot e reload), semântica documentada (dinâmico × restart), falha parcial explícita | `api.rs`, `matrix-managed.rs`, `docs/MANAGED-RUNTIME.md` |
| Conformidade | `matrix-conform` (vetores + subset comportamental local com componente implementado à mão, sem SDK) + vetores publicados | `matrix-runtime/src/bin/matrix-conform.rs`, `dist/schemas/vectors.json` |
| Distribuição | `scripts/package.sh`: bins, wheel, fixtures, schemas, crates extraídos, MANIFEST com hashes/proveniência | `dist/` (ignorado no git), `docs/INSTALL.md` |
| Harness | Projeto externo em `/tmp`: P01–P12 com Rust+Python, biblioteca+serviço, local+remoto | `scripts/harness-external.sh`, `scripts/check-harness-bounds.sh` |

## Decisões internas, justificadas

1. Fachada como módulo (`matrix_runtime::api`), não rewrite: o serviço
   gerenciado migrou para ela (serve + reload), então a fronteira é
   exercida em produção interna, não só em testes. Internos seguem
   acessíveis sem promessa (catálogo explícito).
2. Sem licença inventada: `publish = false` + wheel local + MANIFEST
   dizendo "sem licença transmitida". Publicar seria declarar direitos
   que o projeto não concedeu.
3. Snapshot online pelo dono + offline somente-leitura com trava de
   exclusividade (dono vivo recusa; parado libera no drop do handle).
   R5 eliminado do caminho público: a abertura de recuperação continua
   existindo, mas chama-se boot — nunca "backup".
4. Restore nunca sobrescreve (remoção explícita primeiro) e nunca
   revive leases (só memória) nem revogações (tabela persiste);
   resultado durável faz replay sob lease nova (prova de ledger, não de
   autoridade).
5. Componente hand-rolled fala o fio com `serde_json` + prefixo de
   tamanho apenas: `matrix-proto` no binário serve aos vetores, não ao
   componente (código separado, sem imports do SDK).
6. Reload em duas fases com rotação dona da fachada: o `Runtime`
   guarda a config ativa, difere na recarga (revoga removidos, aplica o
   novo, vira nova baseline só sem erros). Campos de restart (home, TLS,
   listeners, componentes, authority/domain) diagnosticam em vez de
   aplicar silenciosamente; o daemon virou chamada única com relatório.
   Shutdown verificado relata antes/depois (sessões, leases, pendências)
   após espera limitada de coleta — nunca sucesso fictício.
10. Validação de manifests usa o mesmo parser do provisionamento
    (capabilities, requires, execution, outbound e limites), com
    referências resolvidas fora de ordem (cons antes de prov não é erro)
    e provedores roteados satisfazendo `requires`. O `OfflineBackup`
    segura o lock compartilhado durante toda a cópia (a trava anterior
    caía antes da abertura — janela real). `Runtime::start` com backup
    aberto falha fechado; a prova usa o mesmo home duas vezes.
9. Instalação isolada de verdade: venv + `pip install --no-index` do
   wheel (sem PYTHONPATH, com prova de não-vazamento no interpretador
   do sistema), `cargo vendor` + build `--offline` contra cópias
   extraídas, binários copiados para dentro do projeto externo.
7. Validação estática é só forma (ex.: `seq` de stream aceita qualquer
   string no schema; o runtime descarta o inválido). Vetores documentam
   a camada, não prometem semântica.
8. Launch token (`MATRIX_LAUNCH_TOKEN`) é autenticação de runtime, não
   parte do schema: hello sem token válido não ganha sessão. SDKs o
   enviam; implementações independentes devem ler o env (documentado no
   relatório de conformidade do harness).

## Evidência (P01–P12)

| ID | Prova |
|---|---|
| P01 | Harness instala wheel sem PYTHONPATH, bins executáveis, hashes do MANIFEST conferem, `conform vectors` verde |
| P02 | App Rust externa (`api` somente — grep de fronteira verde): build `--offline`, compõe/inspeciona/encerra com relatório |
| P03 | CLI administra com autoridade e nega sem ela (código estável); `matrix-rt.sock` nunca vinculado pelo gerenciado |
| P04 | Cadeia Rust→Python, acquire, eventos (via conform), retirada no harness + paridade M6/M7 |
| P05 | `matrix-conform local` 23/23 (12 vetores + 11 comportamentais com componente à mão) |
| P06 | Vetores + schemas publicados; negociação recusada explicitamente; matriz em `VERSIONS.md` |
| P07 | Serve recusa config inválida sem mutar; reload válido aplica e revoga (rotação dona da fachada, baseline = conjunto aplicado); restart-exigido e manifests diagnosticam antes de mutar; falha parcial recupera pela baseline rastreada |
| P08 | Remove→provision→activate: nova geração serve, refs antigas mortas, independente sobrevive (fachada + harness) |
| P09 | Origem com hash idêntico, restore validado, corrompido recusado, autoridade antiga morta (`backup_restore.rs` + CLI) |
| P10 | Inspect versionado/limitado sem credenciais (token vivo sob grep); status sem segredos |
| P11 | Revogação nega por código; processo morto vira unknown sem replay automático do mesmo op |
| P12 | Metadados + `INSTALL.md` + referência (`API-CATALOG.md`) + protocolos vigentes + migração + runbooks + limites |

Verificação: `make test` (224 passed / 0 failed no Cargo com
`--test-threads=1` + 6 passed no Python `sdk-python/test_units.py`),
`make compat` PASS, `scripts/smoke-managed.py` PASS, demos M6/M7 PASS,
`matrix-conform` local 23/23 + vectors 13/13, harness externo 44/44
(P01–P12, biblioteca+serviço, local+remoto, Rust+Python, venv isolado,
vendor offline), zero warnings, `git diff --check` limpo. Contagens
reapuradas no fechamento; qualquer divergência entre este relatório e
a saída dos comandos é bug do relatório, não das suítes.

## Limites e backlog explícito

- Sem publicação em registries, releases públicos, Windows/macOS, WASM,
  nem números de desempenho (M9).
- Conformidade remota ao vivo (TLS) não roda no harness (perfis
  estáticos validados; M7 cobre o transporte com mTLS real).
- Fuzz de parser segue parcial (C12); prova formal fora de escopo.
- Snapshot online de stores WAL sob escrita concorrente usa a API de
  backup do SQLite (páginas consistentes); backup de processo vivo por
  arquivo é recusado, não copiado.
- Harness exige Linux + toolchain Rust + Python 3.10 + OpenSSL CLI
  (declarado no relatório, não detectado em runtime além de `need`).

## Runbooks (operador externo)

- **Instalar**: `docs/INSTALL.md` (verificar MANIFEST, binários, wheel, crates).
- **Config inválida**: serve recusa com `campo: motivo` e não cria estado;
  corrigir e repetir (P07).
- **Reload com falha**: inválido preserva tudo; válido parcial relata por
  área no stderr — reinspecionar e recarregar corrigido (P07).
- **Atualização com falha**: refs antigas seguem mortas; re-provisionar e
  reativar gera nova geração; independentes não caem (P08).
- **Credencial revogada**: `revoke` nega admissões novas com
  `permission-denied`; pernas vivas rebaixam, nunca viram ok falso (P11).
- **Processo morto**: chamada vira unknown; reconsultar (`op.query` via
  SDK/ledger) em vez de repetir efeito; mesma operação não reexecuta
  sozinha (P11).
- **Partição**: ver M7 runbook; reutilização exige nova sessão +
  reconcile antes de publicar.
- **Recurso pendente**: handles morrem com a sessão; `CleanupPending`
  aparece no `remove` em vez de travar (P08/P10).
- **Recuperação de operação**: `unknown` pede consulta, nunca repetição
  automática; backup válido restaura ledger, corrompido é recusado (P09).
