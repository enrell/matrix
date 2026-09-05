# Documentação do kernel Matrix

Revisão: 2026-09-05. Idioma: português. **Baseline de projeto v0.1, não protocolo estável nem certificado de implementação.** “Deve” expressa requisito de aceitação futuro.

## Ordem de leitura

| Documento | Pergunta respondida |
|---|---|
| [Estado atual](STATUS.md) | O que existe de fato? |
| [Arquitetura](ARCHITECTURE.md) | Onde vivem as responsabilidades? |
| [Contrato](CONTRACT.md) | Quais propriedades precisamos preservar? |
| [Ciclo de vida](LIFECYCLE.md) | Como ativar, retirar e substituir componentes? |
| [Protocolo](PROTOCOL.md) | Como hosts, kernel e componentes conversam? |
| [Segurança](SECURITY.md) | O que é confiável e como limitar autoridade? |
| [Remoto](REMOTE.md) | O que muda entre máquinas e durante falhas? |
| [Persistência](DURABILITY.md) | O que sobrevive a uma queda? |
| [Plugins e SDK](SDK.md) | Como implementar um componente? |
| [Validação e operação](VALIDATION.md) | Como provar funcionamento e diagnosticar falhas? |
| [Roadmap](ROADMAP.md) | Com o que começar e quando avançar? |
| [Decisões](DECISIONS.md) | O que foi escolhido e o que está aberto? |
| [Referências](REFERENCES.md) | De onde vêm os conceitos? |

## Cobertura das áreas

| Área | Contrato principal | Verificação |
|---|---|---|
| Invariantes, identidade e contextos | CONTRACT | C01–C03 |
| Dependências, ciclo de vida e reload | LIFECYCLE | C04–C07 |
| Recursos e efeitos | CONTRACT, SECURITY | C02, C03, C08 |
| Concorrência e cancelamento | LIFECYCLE, PROTOCOL | C06, C09 |
| Autorização e isolamento | SECURITY | C10, C11 |
| Protocolo, filas e streams | PROTOCOL | C12–C14 |
| Supervisão | LIFECYCLE | C15 |
| Persistência | DURABILITY | C16–C18 |
| Observabilidade | VALIDATION | C19 |
| Remoto e efeitos distribuídos | REMOTE | C20–C23 |
| SDKs e independência de linguagem | SDK | C24 |
| Desempenho | VALIDATION | B01–B04 |

[Contrato](CONTRACT.md) prevalece sobre exemplos. Divergências devem virar correções explícitas, não interpretações silenciosas. Os exemplos são propostas e não devem ser enviados ao scaffold como se fossem sua API atual.
