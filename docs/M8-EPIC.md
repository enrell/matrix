# Epic M8 — Contrato público e adoção independente

Status: plano de entrega proposto em 2026-09-07; não implementado. Base: [roadmap](NEXT-PHASES.md), [M6](M6-COMPOSITION.md), [M7](M7-COMPOSITION.md) e [contrato semântico](CONTRACT.md).

## Resultado

Um desenvolvedor em outro repositório consegue integrar Matrix, executar componentes Rust/Python, configurar autoridade e acompanhar lifecycle usando somente APIs e protocolos publicados. Não precisa editar o kernel, acessar suas tabelas internas ou construir transporte, supervisão e reconciliação próprios.

M8 transforma os perfis funcionais existentes em uma base distribuível, documentada e verificável por terceiros. Não acrescenta aplicações de produto. A entrega é uma epic integrada; M8.1–M8.4 são áreas de trabalho, sem aprovação intermediária por slice. Organização interna, nomes concretos de crates e sequência de implementação ficam com o implementador.

## Estado de partida

`matrix-core` exporta módulos e tabelas internas, além de operações de alto nível. `matrix-runtime` expõe serviço, armazenamento, sessões e controladores diretamente. `matrix-sdk` é cliente do daemon legado; `matrix-component` e o SDK Python atendem componentes. Existência de um item `pub` não significa que ele já constitua contrato estável.

M6/M7 têm perfis e limites documentados. M8 deve integrar esses perfis, sem anunciar portabilidade, confiabilidade de plugins ou garantias distribuídas além do que foi validado. Divergências entre textos normativos e relatórios de desvios precisam convergir para uma referência vigente por perfil.

## Superfícies públicas

| Consumidor | Superfície obrigatória | Fronteira |
|---|---|---|
| Aplicação Rust embutindo Matrix | Construção/configuração, start/shutdown, instalação/retirada, chamadas e inspeção | Nenhum acesso direto a locks, registries, tabelas de tickets ou ledger |
| Aplicação operando Matrix como serviço | Cliente/API administrativa documentada e autenticada conforme o perfil | Não usar protocolo legado como bypass de autoridade gerenciada |
| Componente externo | SDK Rust/Python e protocolo implementável por outras linguagens | Identidade emitida pelo host; grants vêm do operador |
| Operador | Configuração, validação, diagnóstico, backup/restauração e atualização | Ações privilegiadas explicitamente separadas das capabilities de plugins |
| Implementador de integração/host | Extensões deliberadamente suportadas | Base confiável e responsabilidades declaradas; nenhuma estabilidade implícita de módulos internos |

Escolher uma fachada coesa para biblioteca e serviço, sem exigir reescrita total das camadas. Retornar erros tipados/códigos estáveis e snapshots de inspeção, sem expor estruturas mutáveis de fiscalização. Isolar helpers de teste e APIs internas; quando visibilidade entre crates for necessária, documentar explicitamente que não integra o contrato suportado.

O modo biblioteca continua sujeito à confiança do processo que o incorpora. O isolamento gerenciado protege contra componentes nos perfis declarados, não contra a aplicação hospedeira alterando a própria memória. Shutdown tem resultado verificável: concluído ou pendências identificadas, sem sucesso fictício.

## Entregas obrigatórias

### API e experiência de integração

- Inventário de APIs: suportada, experimental, interna ou legada; versão e política aplicável a cada superfície.
- Fachada utilizável para configurar, iniciar, inspecionar e encerrar os modos suportados, com inicialização e falhas parciais tratadas.
- Identidades/handles opacos, tipos de configuração e erros de domínio; nenhuma string de diagnóstico usada como única forma de controle.
- SDKs com documentação de threads, callbacks, cancelamento, streams, eventos, backpressure e liberação de recursos. Exemplos utilizam APIs efetivas, não pseudocódigo apresentado como executável.
- Matriz de integração local/remota: mesma fixture funciona nos perfis suportados; diferenças exigidas por M7 permanecem visíveis.

### Distribuição e versões

Produzir artefatos instaláveis em ambiente limpo: pacotes Rust, pacote Python e binários/configurações necessários aos perfis oferecidos. Resolver como SDK Python será versionado e instalado; não depender de PYTHONPATH apontando para o checkout do kernel. Metadados de licença, dependências e requisitos de plataforma precisam estar coerentes com o que se distribui; não inventar licença ausente.

Fixar política de evolução para API Rust, SDK Python, protocolo de componentes, perfil remoto, manifest/config e formato persistente. Números de versão não precisam coincidir, mas sua relação deve estar publicada. Não declarar versão 1.0 automaticamente: M8 pode entregar um contrato público experimental com limites de compatibilidade explícitos.

Alterações incompatíveis devem ser detectáveis antes de operar parcialmente. Quando quebrar uma superfície legada, fornecer migração e rejeição clara; não manter indefinidamente aliases que reintroduzam semântica de aplicação no kernel. Atualização do kernel e atualização de componentes têm contratos diferentes.

Não publicar em registries, criar releases públicos ou enviar código a repositórios remotos como parte implícita desta epic. Pacotes locais e um repositório externo temporário bastam para validar distribuição.

### Protocolo e conformidade independente

Publicar schemas/vetores e um comando de conformidade executável fora do workspace. Cobrir mensagens válidas/inválidas, features ausentes, erros, frames parciais, cancelamento, revogação e geração obsoleta. Definir claramente quais testes exigem Linux, TLS, ferramentas de isolamento ou privilégios específicos.

O conjunto deve verificar comportamento, não só parsing: um pequeno componente implementado diretamente a partir do protocolo, sem SDK Matrix, precisa passar o subconjunto declarado. Pode ser em Rust/Python; não é obrigatório introduzir uma terceira linguagem. SDKs oficiais continuam passando a suíte equivalente.

Separar conformidade de perfil e prova formal. A ausência de fuzz completo não vira alegação de robustez universal, e o harness independente não prova ausência de todo bug de implementação.

### Configuração, carregamento e atualização

Configuração pública cobre manifests, limites, grants solicitados/concedidos, destinos, credenciais e perfis. Validar antes de alterar runtime: erros de schema, combinações não suportadas e referências inexistentes têm diagnóstico acionável.

Definir semântica de reload: campos dinâmicos, campos que exigem restart, ordem de retirada e política em falha parcial. Configuração inválida não pode aplicar silenciosamente metade dos grants. Se uma atualização válida falhar durante execução, estado resultante é explícito e reconciliável; não prometer transação universal entre processos.

Atualização/retorno de versão de componente cria nova ativação. Handles antigos permanecem inválidos; componentes independentes sobrevivem à cascata apropriada. Só o operador autoriza executáveis, mounts e acesso remoto. Pacote instalado não recebe grants automaticamente.

### Operação e recuperação

Inspeção pública correlaciona contexto, recurso, ticket, binding, lease, peer, stream e operação, incluindo origem de Waiting, Failed e CleanupPending. Definir schema versionado, limites/paginação e redação de segredos. Comandos administrativos não devem exigir consulta manual à base SQLite ou estruturas privadas.

Backup/restauração devem ter procedimento executável para os modos realmente suportados. O comportamento R5 — snapshot que abre a origem em modo recuperação e altera epoch/estado — precisa ser eliminado no caminho público de backup, ou substituído por fluxo explicitamente nomeado como manutenção mutante. Não apresentar uma operação mutante como backup somente leitura. Verificar integridade e política de exclusividade.

Restauração não ressuscita leases, sessões ou autoridade antiga: reconciliar antes de republicar. Testar um backup válido e um corrompido; documentar formato/versão e recusa de versão incompatível. Retenção de operações e esgotamento do ledger têm comportamento operacional explícito, preservando unknown.

Runbooks cobrem instalação, início, configuração inválida, retirada, atualização com falha, credencial revogada, processo morto, partição, recurso pendente e recuperação de operação. Diagnóstico não recomenda repetir efeito unknown sem contrato específico.

## Prova de adoção fora do repositório

Criar harness mínimo em diretório/repositório temporário fora do workspace Matrix, com manifests e dependências próprios. Ele é material de validação, não nova aplicação mantida pelo kernel. A receita para recriá-lo pode ficar neste repositório.

O harness deve consumir pacotes/artefatos preparados da entrega. Não usar `include!` sobre fonte interna, paths para submódulos privados, symlinks para testes do workspace ou patches no kernel. Se transporte de distribuição local exigir path, apontar para artefato extraído e completo, não para código interno do checkout. Registrar hashes/versões e a proveniência exata dos artefatos.

Exercitar separadamente biblioteca e serviço. Compor provedor, consumidor e independente; usar Rust e Python; fazer chamada, evento, aquisição/liberação, stream e retirada/reintrodução. Repetir o percurso suportado com um host remoto local em processo separado. Não exigir WAN nem aplicação de negócio.

Usar somente a documentação pública durante a integração. Descobertas que exigirem leitura de implementação viram lacunas de documentação/API, corrigidas na epic antes de fechar. Não afirmar avaliação por terceiro se o mesmo implementador executou o harness; integração independente significa independência técnica do workspace, não avaliador externo fictício.

## Critérios de aceitação

| ID | Cenário | Evidência exigida |
|---|---|---|
| P01 | Instalação a partir de artefatos em ambiente limpo | Rust/Python e binários instaláveis, versões/requisitos explícitos, sem dependência oculta do checkout |
| P02 | Biblioteca externa | Harness inicia, compõe, inspeciona e encerra por fachada pública; nenhuma tabela interna acessada |
| P03 | Serviço externo | Cliente executa administração autorizada e rejeita operação sem autoridade; legado não contorna política |
| P04 | Paridade local/remota | Fixtures reais exercitam chamadas, recursos/eventos e streams suportados, incluindo retirada |
| P05 | Implementação sem SDK | Componente independente passa conformidade do perfil declarado com relatório reproduzível |
| P06 | Compatibilidade e feature ausente | Matriz de versões anterior/candidata, negociação e erros sem downgrade silencioso |
| P07 | Configuração inválida e reload | Diagnóstico antes de mutação; estado preservado ou falha parcial explicitamente reconciliável |
| P08 | Atualizar e retornar componente | Nova geração, refs antigas inválidas, independente operacional, cleanup verificável |
| P09 | Backup/restauração | Origem preservada no backup público, restauração validada, corrupção rejeitada, autoridade antiga não revive |
| P10 | Diagnóstico operacional | Waiting/Failed/CleanupPending explicáveis sem internals; inspeção limitada e sem credenciais |
| P11 | Revogação e falha de host | Aplicação observa estado correto pela API pública; resultado unknown não vira replay automático |
| P12 | Distribuição/documentação completas | Metadados, guia inicial, referência de API, protocolos vigentes, migração, runbooks e limitações consistentes |

Os cenários devem ser reproduzíveis por comandos entregues. Reexecutar regressão M1–M7, compatibilidade, testes Python e demos. Se a reorganização de API exigir adaptar testes internos, preservar as propriedades testadas e registrar mudanças de cobertura. Build verde ou contagem agregada não substituem P01–P12.

## Autonomia, decisões e revisão

O implementador decide fachada, nomes, empacotamento, APIs concretas e ordem de trabalho. Consolidar contratos antes de depender deles é responsabilidade interna da epic, sem aprovação a cada alteração. Mudanças de garantias ou retirada de escopo obrigatório precisam aparecer explicitamente como proposta de mudança, não apenas como desvio no relatório final.

Não inclui marketplace, plugins de negócio, novas plataformas, federação, migração de ownership, otimização sem evidência ou reescrita arquitetural integral. Bugs bloqueando a integração pública fazem parte do fechamento; novos recursos sem relação com a adoção ficam fora.

Entrega final: código e artefatos, catálogo de APIs, documentação vigente, receita do harness externo, suíte de conformidade, matriz de compatibilidade e relatório P01–P12 com limitações. M8 fecha quando uma aplicação externa usa Matrix pelos contratos publicados e opera os perfis declarados sem depender de detalhes internos.

Ao fechar M8, o kernel pode ser considerado funcionalmente pronto para adoção nos perfis validados. M9 mede e otimiza; não é requisito para anunciar desempenho que ainda não foi medido nem motivo para adiar uma versão funcional.
