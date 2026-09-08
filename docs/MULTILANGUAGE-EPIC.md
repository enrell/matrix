# Epic ML1 — Adoção multilíngue do kernel

Status: **entregue** (ver [ML1-COMPOSITION.md](ML1-COMPOSITION.md), [ML1-MATRIX.md](ML1-MATRIX.md), [ML1-NODE.md](ML1-NODE.md)); este documento permanece normativo. Depende dos contratos públicos entregues em [M8](M8-COMPOSITION.md). Complementa [catálogo de APIs](API-CATALOG.md), [versões](VERSIONS.md) e [SDK](SDK.md). ML1 é uma frente de adoção; não renumera M9 nem depende de otimizações de desempenho.

## Resultado

Um desenvolvedor instala o pacote da sua linguagem, inicia ou conecta a um Matrix local, implementa componentes e compõe dependências sem escrever framing, gerenciar tickets manualmente ou estudar o código Rust. A aplicação permanece em outro repositório. Identidade, autoridade, recursos, lifecycle e recuperação continuam sob fiscalização do kernel.

A entrega contempla Python, JavaScript/TypeScript, Go, Crystal, Elixir, C#, C++ e C, mantendo Rust como referência. Não basta publicar tipos gerados ou clientes que só chamam `invoke`: cada SDK deve suportar aplicação hospedeira e implementação de componentes no perfil declarado.

Uma epic integrada, decisões técnicas autônomas e revisão final. Ordem interna sugerida: consolidar Python/Rust e JS/TS, estabilizar o conjunto de conformidade, estender Go/C#, depois Crystal/Elixir/C/C++. Essa ordem não torna as últimas linguagens opcionais nem exige aprovação entre etapas.

## Arquitetura de integração

| Modo | Contrato | Escopo ML1 |
|---|---|---|
| Serviço local gerenciado | Aplicação conecta ou inicia um processo Matrix e usa API pública | Padrão para todas as linguagens |
| Componente hospedado | Kernel lança componente; SDK negocia sessão autenticada e atende handlers | Obrigatório em todas as linguagens |
| Biblioteca Rust | API pública embutida já entregue em M8 | Preservar e validar paridade |
| ABI C para embedding | Kernel Rust dentro de outro processo via FFI | Fora de ML1; epic própria se necessária |

O SDK C desta epic é um cliente/SDK de componentes por IPC; não exige expor o kernel inteiro por ABI C. C++ pode oferecer uma camada RAII sobre esse cliente C, desde que preserve os contratos e distribuição independente.

Usar os protocolos Matrix existentes como fonte de verdade. Tipos, codecs e validação estrutural podem ser gerados a partir de schemas. Lifecycle, agendamento, cancelamento, filas e ergonomia permanecem adaptadores pequenos por runtime e precisam de testes comportamentais.

Não adicionar um protocolo paralelo por linguagem, nem usar o socket administrativo legado para contornar o perfil gerenciado. Quando uma operação pública de M8 só existir em Rust, acrescentar um caminho administrativo genérico autenticado ou documentar que exige bootstrap do operador; não inventar acesso a internals no SDK.

## Duas superfícies claras por SDK

**Aplicação/operador:** iniciar kernel próprio ou conectar ao existente; validar/carregar configuração; readiness; provisionar, ativar, retirar, chamar, inspecionar e encerrar segundo autoridade concedida. Operações administrativas, incluindo grants, backup e reload, ficam em objeto/superfície separados dos handlers de plugin.

**Componente:** registrar capabilities/requisitos, atender chamadas, acessar bindings pelo contexto, adquirir/liberar recursos, receber eventos e abrir/enviar/receber/encerrar streams do perfil suportado. O SDK traduz identidades opacas sem permitir que a aplicação fabrique principal, época ou geração.

Uma API declarativa com decorators, atributos, interfaces ou macros pode gerar manifests, mas continua declarando permissões solicitadas. Instalar pacote ou registrar handler não concede grant. O tutorial inclui configuração do operador, mesmo que use uma fixture mínima pronta.

Decidir e documentar nomes de pacotes, classes/funções e layout antes de gerar exemplos dependentes. Os exemplos deste plano descrevem capacidades desejadas, não APIs já disponíveis.

## Bootstrap e distribuição

- Um pacote instalável por ecossistema, com versão e requisitos explícitos. Produzir artefatos locais para pip, npm, Go modules, Shards, Mix/Hex, NuGet e C/C++ via CMake/pkg-config ou alternativa documentada. Confirmar disponibilidade de nomes antes de eventual publicação, que não faz parte desta autorização.
- Resolver binário por caminho explícito, instalação configurada ou PATH, em ordem documentada. Não baixar/executar binários silenciosamente durante import ou instalação. Instalador explícito pode verificar versão, origem e integridade de artefatos selecionados.
- `start` pertence à aplicação que criou o processo; `connect` apenas anexa cliente. Fechar cliente anexado nunca desliga kernel compartilhado. Falha de bootstrap recolhe somente recursos que criou e apresenta diagnóstico.
- Usar diretório/socket privado, credencial local efêmera ou mecanismo equivalente imposto pelo runtime, sem expor token em argv, logs ou payloads de usuário. Não afrouxar mTLS dos perfis remotos para facilitar demos.
- Negociar versão/features e limites antes de oferecer operações. Erros de binário ausente, versão incompatível, permissão negada e feature ausente precisam ser distintos e acionáveis.
- Declarar matriz inicial de OS/arquitetura e runtime de linguagem realmente testados. Node é o alvo inicial de JS/TS; navegador, Deno e Bun não são suporte implícito. Linux é a base existente; não anunciar novas plataformas por compilar apenas o cliente.

Publicação em registries e criação de releases externos exigem autorização separada. Esta epic entrega pacotes verificáveis e receitas de instalação fora do checkout.

## Adaptação idiomática

| Linguagem | Experiência esperada | Pontos de atenção |
|---|---|---|
| Python | async/await, context managers e iteradores assíncronos | Compatibilidade da API existente, cancelamento de tarefas e integração sem bloquear event loop |
| JS/TS | Um runtime compartilhado, tipos TS, Promise, AsyncIterable, AbortSignal | Nenhuma dependência de compilador TS para usar JS; perda de precisão de inteiros evitada |
| Go | context.Context, interfaces e Close explícito | Goroutines/filas limitadas e propagação de deadline |
| Crystal | Fibers, channels e blocos de escopo | Escalonamento e interrupção cooperativa sem travar leitor |
| Elixir | Processos, supervisão e API por mensagens | Morte do processo consumidor encerra seu contexto; supervisão do SDK não ressuscita geração antiga |
| C# | Task, CancellationToken, IAsyncEnumerable e IAsyncDisposable | Cancelamento distinguível de unknown; callbacks fora do leitor |
| C | Handles opacos, códigos de erro, poll/callback ou loop explícito | Ownership de buffers, comprimento explícito, liberação e afinidade de thread documentados |
| C++ | RAII e APIs compatíveis com o modelo assíncrono escolhido | Destrutores não lançam nem bloqueiam indefinidamente; fechamento verificável explícito |

Não impor uma biblioteca de concorrência específica a todos. A implementação escolhe versões mínimas e integrações suportadas, registra a decisão e as valida em ambiente limpo. Interfaces síncronas opcionais não podem executar espera bloqueante na thread leitora.

## Semântica comum obrigatória

1. **Autoridade:** chamador vem da sessão; dependência vem do binding da ativação; solicitação não equivale a grant. Nenhum fallback para outro provedor ou transporte privilegiado.
2. **Lifecycle:** scopes possuem recursos; encerramento explícito é idempotente e informa pendências. GC, finalizers e destructors são conveniência, não garantia única de liberação.
3. **Cancelamento:** herda prazo do contexto, revoga novas ações e diferencia interrupção da espera, execução e possibilidade de efeito anterior. Timeout não autoriza retry implícito.
4. **Concorrência:** leitor e controle independentes de handlers; filas/threads/tarefas limitadas; deadlines incluem espera de transporte. Preservar identidade exata até a entrega, sem resolver nome lógico depois de validar geração.
5. **Tipos:** ids/u64 sem perda de precisão; nulo, booleanos, números e erros interoperáveis; limites e representação de valores fora da faixa publicados. Não converter silenciosamente bytes arbitrários em UTF-8 lossy.
6. **Streams:** owner e operação explícitos, crédito/backpressure, terminais e cancelamento. Não tratar chunks como eventos descartáveis. Eventos best-effort têm perdas contabilizadas e política documentada.
7. **Recuperação:** reconexão negocia e reconcilia; geração nova não adota referências antigas; operação unknown permanece unknown; consulta não executa novamente.
8. **Diagnóstico:** erros estruturados com código, fase e correlação segura; detalhes internos acessíveis quando úteis, sem exigir compreensão de tickets no primeiro uso.

Limitações existentes de streams M7 — payload textual e associação por única perna — devem aparecer na matriz. A camada ergonômica não pode apresentar streaming genérico concorrente/binário que o kernel não oferece. Para ML1, oferecer associação explícita ao contexto/operação para streams concorrentes; se isso exigir extensão genérica negociada do protocolo, ela integra esta epic. Payload binário pode permanecer feature opcional, mas deve ser recusado explicitamente quando não suportado, nunca corrompido.

## Ferramentas e documentação

Entregar comando/receita de scaffolding para aplicação hospedeira e componente em cada linguagem. Exemplos mínimos: provedor, consumidor e independente; mesmos contratos e resultados, sem aplicação de produto. Cada projeto gerado tem build/install, configuração de grants, execução, teste e cleanup documentados.

Guia inicial deve permitir instalar, executar primeira composição, retirar provedor e entender Waiting em um fluxo contínuo. Referência aprofunda lifecycle, erros, recuperação e permissões. Distinguir claramente conectar a kernel existente de iniciar um novo.

Disponibilizar diagnóstico de ambiente: binário encontrado, versões/features, permissões de socket e requisitos de isolamento. Não esconder erros com mocks ou iniciar kernel sem isolamento quando o perfil solicitado não estiver disponível.

## Conformidade e prova externa

Reutilizar matrix-conform e vetores M8, ampliando o contrato comum dos SDKs. Exigir provas para cada linguagem nos dois papéis: consumidor da API e provedor de capability. Fixtures de referência controladas complementam testes de pares cruzados; não exigir todas as combinações quadráticas para afirmar conformidade.

Pares obrigatórios incluem Python↔JS/TS, Go↔C#, Crystal↔Elixir e C↔C++, além de referência Rust. JS sem TypeScript e TS com tipagem são entradas distintas sobre o mesmo SDK. Pelo menos uma composição mistura três linguagens, e os perfis remotos são exercitados com SDKs sem implementação própria de rede remota em cada plugin.

Cada harness vive fora do checkout, instala os pacotes em ambiente isolado e executa binários staged. Não usar PYTHONPATH, symlinks ou includes para o código interno; cache de dependências só com proveniência e modo offline documentados. Um teste de fronteira não substitui a execução real.

## Aceitação da epic

| ID | Resultado | Evidência |
|---|---|---|
| L01 | Todos os SDKs distribuíveis | Instalação limpa dos oito grupos de linguagem, versões e artefatos identificados |
| L02 | Bootstrap e attach corretos | Iniciar/encerrar próprio kernel; anexar/desanexar sem desligar compartilhado; falha parcial sem órfãos |
| L03 | Primeira composição por contrato público | Projeto gerado em cada linguagem executa handler e dependência sem internals |
| L04 | Tipos e protocolo equivalentes | Vetores comuns, ids grandes, erros e rejeição de mensagens/features inválidas |
| L05 | Autoridade preservada | Grant ausente, handle alheio e geração obsoleta rejeitados em cada SDK |
| L06 | Lifecycle sob falha | Retirada/reintrodução e morte do componente recolhem recursos ou expõem pendências; independente responde |
| L07 | Cancelamento e saturação | Controle progride sob carga definida; nenhum bloqueio do leitor ou crescimento ilimitado |
| L08 | Streams e eventos | Associação inequívoca de streams concorrentes, crédito, terminal, cancel; perdas de eventos contadas; binário suportado ou erro explícito |
| L09 | Integração entre linguagens | Pares definidos e cadeia de três linguagens, com componentes reais |
| L10 | Local/remoto | Mesmo código de negócio usa rotas configuradas; perda/reconexão preserva unknown e identidade |
| L11 | Compatibilidade | SDK anterior/candidato e perfil sem feature; nenhuma quebra silenciosa; migração publicada |
| L12 | Uso e diagnóstico | Guias/scaffolds executados do início ao fim; erros acionáveis e nenhum segredo exposto |

Entrega inclui código, pacotes, APIs concretas, schemas/vetores, matriz por linguagem/perfil, recipes dos harnesses, comandos/resultados e limitações. Rodar regressão M1–M8, SDKs existentes, conformidade e demos. Descrever onde houve avaliação pelo próprio implementador; não inventar teste de usabilidade por terceiros.

ML1 fecha quando todas as linguagens listadas cumprem o subconjunto obrigatório, com diferenças idiomáticas documentadas. SDK parcial deve ser identificado como parcial, sem encerrar a epic por haver pacote em todos os ecossistemas. Revisão ocorre sobre a entrega integrada; decisões internas não exigem aprovação por etapa.

## Fora de escopo

ABI C para embedding do kernel, navegador, novas plataformas, plugins de negócio, integrações específicas de modelos, marketplace e promessas de desempenho. Otimizações medidas podem seguir M9; não são pré-requisito para ergonomia. Rust continua implementando o núcleo, e aplicações continuam em repositórios separados.
