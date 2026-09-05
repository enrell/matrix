# Validação do perfil gerenciado M3–M5

Data: 2026-09-05. Base: estado final de M2.5 fornecido pelo outro agente, preservado sem modificar seu trabalho durante a implementação. Desenvolvimento isolado em `implement/m3-m5`; integração verificada contra hashes da base para impedir sobrescrita de mudanças concorrentes.

Validação repetida após integração em `/home/lain/projects/matrix`: `make test` com 100 testes, zero warnings, smoke gerenciado e compatibilidade legada aprovados.

## Resultados

- `./scripts/test-managed.sh`: **100 testes passaram**, incluindo unitários, integração e doctest; zero warnings. O script atualiza o exemplo externo e executa `cargo test --release -- --test-threads=1`.
- `./scripts/smoke-managed.py`: **PASS** para a CLI real, PKI temporária, TLS mútuo, invoke, deduplicação, efeito com fence, release e snapshot offline.
- `make compat`: manifest ancient e chamada ao daemon legado passaram.
- `git diff --check`: sem erros de whitespace.

## Evidência por fronteira

| Fronteira | Exercício realizado |
|---|---|
| Isolamento | Python real tenta ler caminho não montado, escrever `/usr`, abrir conexão de rede, fazer fork e alocar acima do limite; loop excede CPU |
| Host seguro | Plugins Rust e Python respondem dentro da sandbox; conexão sem token de lançamento é rejeitada |
| Supervisão | Processo `/usr/bin/false` reinicia em novas instâncias e para ao esgotar o orçamento |
| Durabilidade | Subprocesso é morto por SIGKILL após admissão e após commit; recuperação distingue unknown/completed |
| Falha de disco | SQLITE_FULL impede confirmação de admissão; banco inválido falha fechado |
| Snapshot e retenção | Snapshot preserva operação desconhecida, desejado e fences depois de ultrapassar a retenção de auditoria |
| Autorização | Principal/capacidade indevidos são negados; revogação sobrevive a reinício até regrant explícito |
| TLS | Certificado de cliente obrigatório, nome incorreto do servidor rejeitado, cliente válido mas não autorizado negado, rotação para novo fingerprint |
| Fencing | Geração antiga tenta uma escrita no KV de destino; transação rejeita e valor atual permanece intacto |
| Remoto | Kernel controlador invoca proxy por TLS; retirada local libera remoto; desaparecimento do listener retira consumidores e preserva independente |
| Reconciliação | Reinício preserva desejado/ledger, mas exige nova lease; resposta concluída é consultável sem reexecutar |
| Regressão | M1.1–M1.4, M2.1–M2.5 e SDKs continuam no gate |

## Limites da evidência

Linux x86_64, rede em loopback e SQLite local. Não demonstra desempenho WAN, segurança contra administrador do host, federação ou prova formal da composição. O perfil de sandbox permite threads e nega novos processos; não é ainda um executor de builds que cria descendentes. Durabilidade cobre o serviço/ledger e o KV gerenciado, não qualquer efeito externo do plugin. Remoto usa controle unary; streams de artefatos não foram portados.

[Operação e configuração](MANAGED-RUNTIME.md)
