# Documentação do kernel Matrix

Revisão: 2026-09-05. Idioma: português. **Baseline de projeto v0.1, não protocolo estável nem certificado de implementação.** “Deve” expressa requisito de aceitação futuro.

O [perfil gerenciado M3–M5](MANAGED-RUNTIME.md) documenta a implementação executável e seus limites; os contratos originais continuam como referência.

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
| [Próximas fases](NEXT-PHASES.md) | Quais os incrementos e gates de M5.1 a M9? |
| [Especificação M6.1](M6.1-SPEC.md) | Como componentes externos chamam dependências com autoridade e vínculo pai/filho? |
| [Epic M7](M7-EPIC.md) | Qual a entrega completa de composição remota e como aceitá-la? |
| [Epic M8](M8-EPIC.md) | Como tornar o kernel adotável por repositórios externos através de contratos públicos? |
| [Epic ML1 — SDKs multilíngues](MULTILANGUAGE-EPIC.md) | Como usar Matrix de forma idiomática em Python, JS/TS, Go, Crystal, Elixir, C#, C++ e C? |
| [Catálogo de APIs](API-CATALOG.md) | Qual superfície é suportada, experimental, interna ou legada? |
| [Instalação](INSTALL.md) | Como instalar de artefatos locais e operar o primeiro serviço? |
| [Composição M8](M8-COMPOSITION.md) | O que foi entregue para adoção externa, evidência P01–P12 e runbooks? |
| [Versões](VERSIONS.md) | Como evoluem API, SDKs, protocolos e formato persistente? |
| [Perfil remoto M7](M7-PROFILE.md) | Qual o contrato de fio `matrix.remote/0.1`, ownership e limites? |
| [Composição M6](M6-COMPOSITION.md) | O que foi entregue ponta a ponta, APIs efetivas e desvios da spec? |
| [Composição M7](M7-COMPOSITION.md) | O que foi entregue entre hosts, garantias local×remoto, evidência R01–R13 e runbook? |
| [Remoção de código de aplicação](KERNEL-CLEANUP.md) | O que retirar do scaffold para manter somente o kernel? |
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
