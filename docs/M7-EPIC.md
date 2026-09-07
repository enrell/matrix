# Epic M7 — Composição remota do kernel

Status: entregue 2026-09-07 como um todo integrado (ver
[M7-COMPOSITION](M7-COMPOSITION.md): evidência R01–R13, garantias
local×remoto, runbook). Este documento segue como referência normativa
da aceitação.
Base: [M6 local](M6-COMPOSITION.md), [perfil gerenciado
atual](MANAGED-RUNTIME.md), [contrato](CONTRACT.md) e [princípios
remotos](REMOTE.md).

## Resultado da epic

Uma aplicação externa compõe componentes locais e remotos pelo mesmo modelo de dependências, sem implementar transporte, propagação de autoridade, reconciliação ou cleanup distribuído por conta própria. O kernel mantém identidade e ownership ao atravessar hosts; mudanças de localização não exigem reescrever a lógica dos componentes.

A entrega fecha M7 como um todo: chamadas filhas atravessando hosts, streams com controle de fluxo, eventos remotos, leases/revogação, consulta de operações, reconexão e diagnóstico. M7.1–M7.4 do roadmap são áreas de trabalho, não checkpoints de aprovação. A implementação pode ser reorganizada autonomamente; revisão integrada ocorre na entrega.

## Base existente e lacunas

M5 oferece mTLS, grants, leases, fences, ledger e proxies. O transporte atual atende uma requisição unary por conexão; a renovação e as invocações do proxy compartilham serialização. M6 oferece chamadas filhas locais, bindings, recursos/eventos, quotas e fronteiras de revogação. Não presumir que combinar essas duas implementações já preserve a cadeia de autoridade através da rede.

M7 precisa transportar a relação causal e a autoridade das chamadas, manter controle operacional durante streams/chamadas longas e reconciliar operações sem repetir efeitos desconhecidos. O transporte M5 permanece um perfil de compatibilidade; não recebe silenciosamente semântica nova.

## Fronteira arquitetural

Uma autoridade de composição controla o grafo de cada domínio. Hosts executores remotos fiscalizam recursos e autoridade delegada no seu domínio de execução; não elegem outra autoridade nem recompõem unilateralmente o grafo global durante partição.

Os serviços remotos existentes podem continuar usando um kernel local internamente. Ele não se torna outro decisor do mesmo binding global: configuração explicita quem decide composição, quem executa e qual autoridade pode renovar/revogar cada ativação.

A topologia inicial permite uma autoridade e múltiplos hosts. Chamadas entre componentes em hosts distintos passam por roteamento autorizado, sem descoberta ou conexão arbitrária entre plugins. TLS autentica o host; identidade de componente vem do mapeamento de sessão/ativação que o host confiável atesta.

Usar identificadores completos e distintos para domínio/autoridade, época do kernel, sessão de transporte, ativação, binding, operação e stream. Não reutilizar `bind-N` ou ticket local isolado como identidade global. Lease de ativação e autorização de chamada são objetos diferentes.

## Escopo integrado

| Área | Entrega obrigatória |
|---|---|
| Transporte | Perfil versionado e negociado para sessões persistentes ou canais equivalentes, multiplexação, encerramento e limites explícitos |
| Chamadas | `dependency-calls` atravessa hosts, preservando pai/filho, grants, binding, prazo e validação terminal |
| Streams | Abertura, dados, crédito, conclusão e cancelamento, nos dois sentidos quando negociados |
| Eventos | Encaminhamento a inscrições autorizadas, com quotas, política de perda e retirada por contexto |
| Controle | Renovação, revogação e diagnóstico progridem durante saturação ou chamada longa |
| Recuperação | Consulta autorizada de operações, reconciliação de inventário e nenhuma repetição implícita de efeito desconhecido |
| Integração | SDKs Rust/Python com semântica equivalente, configuração gerenciada e inspeção correlacionada |

Não inclui aplicação de produto, sincronizador de workspace, executor de builds, armazenamento de artefatos de domínio, marketplace, federação, migração arbitrária de ownership, eleição de líder ou exactly-once externo. Transferência genérica em streams não obriga criar serviço de arquivos.

## Contrato de chamadas e autoridade

O consumidor continua escolhendo apenas seu binding autorizado. Localização e rota são resolvidas pelo kernel. Cada travessia verifica a referência completa do consumidor/provedor, vínculo com o pai, grant da aresta, revisão de autoridade, lease e orçamento restante.

Preservar a decisão M6: componentes usam autoridade de serviço limitada pelo operador, sem impersonar usuário final. Uma solicitação recebida do plugin não pode fabricar ancestrais ou grants. O host executor aceita chamadas somente do controlador autorizado e registra quem pode cancelá-las e consultá-las.

Admission, accepted e resultado têm significados separados. A confirmação de admissão remota precisa explicitar se foi persistida antes do ack. O aceite terminal no controlador revalida a cadeia local; o executor também valida sua autoridade antes de aceitar efeito mediado. Resposta antiga pode resolver auditoria de uma operação antiga, nunca atualizar uma ativação nova.

Cancelamento ou retirada revogam participação local imediatamente. Avisar o executor e encerrar streams não prova interrupção física. Recursos remotos ficam pendentes até evidência de liberação; no executor, perda/expiração de autoridade bloqueia novos efeitos mediados e inicia cleanup. Não promover estado incerto a Disposed.

## Tempo, leases e partições

Relógios monotônicos não são comparados entre máquinas. Cada host aplica seu próprio prazo local; budget transmitido é duração restante com política explícita para atraso de transporte. O controlador pode deixar de aceitar resultados antes de o executor observar expiração. Não prometer simultaneidade de revogação ou limite rígido end-to-end sem declarar hipóteses de atraso e escalonamento.

Definir estados Connected, Suspect e Detached, seus gatilhos e efeito na admissão. M7 deve falhar fechado para novas chamadas quando a autoridade não puder ser demonstrada. A interrupção de uma rota afeta seus consumidores; componentes independentes permanecem utilizáveis.

Renovação precisa progredir independentemente de chamadas longas. Sequência/revisão e identidade da lease tornam renovação atrasada inofensiva: ela não revive uma ativação retirada. Resposta de renovação perdida tem recuperação explícita e idempotente, ou força nova ativação; nunca rotação silenciosa que deixe dois proprietários válidos.

## Transporte, fluxo e memória

O implementador escolhe multiplexação e canais, preservando separação efetiva de controle e dados. A escolha deve impedir que saturação de dados ou bloqueio de consumidor retenha locks globais ou esgote a capacidade necessária a renovar, cancelar e retirar.

Limites finitos por stream, operação, sessão, host e domínio incluem bytes recebidos/retidos/enviados, operações simultâneas, ids de deduplicação, callbacks e filas de controle. Definir contabilização e tolerância máxima para buffers de frame/TLS; medir o agregado, não apenas payload de input. Controle tem orçamento reservado e limitado, não uma exceção ilimitada às quotas.

Crédito é concedido pelo receptor, consumido em bytes segundo unidade documentada e liberado somente quando o buffer correspondente deixa de estar retido. Dados sem crédito, sequências inválidas e frames excedentes são rejeitados. Não acumular chunks ilimitados enquanto o consumidor não lê.

Um stream tem owner, operação, direção, sequência e terminal únicos. Fim de stream não significa sucesso da operação. Cancel, retirada e erro liberam crédito/reservas e impedem novos dados aceitos. SDKs processam controle fora de callbacks bloqueantes; filas de eventos têm política de descarte e contagem observável.

Leitura e escrita têm deadlines absolutos, incluindo espera por lock, framing, TLS e flush. Reutilizar o aprendizado M6 sem presumir que timeout TCP limita um frame TLS. Frame parcialmente transmitido e falho invalida o canal afetado; não anexar mensagens a uma sequência truncada. Garantias são sujeitas ao escalonamento, não tempo real rígido.

## Reconexão e durabilidade

Reconectar autentica novamente e reconcilia ativações, leases, recursos pendentes e operações antes de publicar bindings. Uma conexão nova não herda autoridade da anterior apenas porque usa o mesmo certificado.

Operações têm identidade estável no escopo do principal/controlador e conteúdo associado. Duplicata com conteúdo divergente é erro. Consultar resultado não reexecuta. Retenção e limites são explícitos; entrada ausente após retenção não prova que a operação nunca executou.

M7 não retoma streams implicitamente. Desconexão encerra a sessão de stream como interrompida; operação pode permanecer consultável pelo ledger. Retomar de offset só pode ser oferecido como feature adicional com fonte reproduzível, sequência, retenção e autorização definidas. Não confundir reconectar transporte com continuar execução arbitrária.

Após queda do controlador ou executor, reconciliar antes de reativar. Operação admitida sem resultado durável permanece unknown quando não há evidência adicional. Fencing só garante exclusão no destino que o fiscaliza. Demonstrar pelo menos um efeito gerenciado com fence no destino; demais efeitos mantêm sua classificação explícita.

## Eventos e recursos

Inscrições são recursos do contexto consumidor e não sobrevivem a uma nova geração. Roteamento remoto verifica a inscrição e autoridade atuais; tópico no payload não concede acesso. Callbacks enfileirados de uma ativação retirada não ganham autoridade nova.

Eventos são best-effort no perfil inicial, ordenados apenas no escopo definido pela sessão/stream, com perdas e descarte observáveis. Não inferir persistência ou replay de eventos a partir do ledger de operações.

Handles continuam opacos e vinculados ao owner no host que executa a liberação. Controlador mantém a referência ao recurso remoto e seu estado de reconciliação. Requisição alheia/obsoleta falha fechado; contar um proxy como removido não significa que o recurso remoto foi liberado.

## Compatibilidade e operação

Publicar schemas e vetores do perfil novo, versão/ALPN e features negociadas. Perfil M5 unary permanece utilizável com seus limites; incapacidade de streams/filhas remotas retorna erro explícito, sem downgrade silencioso. Nome concreto do novo perfil é decisão da implementação e deve constar na documentação final.

Configuração do operador define controladores confiáveis, hosts, rotas, grants, leases e limites. Rede não aceita manifest ou caminho executável arbitrário como autorização de lançamento. Rotação/revogação de credenciais invalida a autoridade correspondente e aparece no diagnóstico.

Inspect/journal correlacionam cadeia de chamadas, hosts, sessões, leases, bindings, streams e recursos pendentes, distinguindo resultado do negócio, transporte e cleanup. Não registrar credenciais; payloads não entram por padrão. Runbook cobre partição, executor perdido, recuperação de resultado e recurso pendente.

## Aceitação da epic

| ID | Cenário integrado | Evidência necessária |
|---|---|---|
| R01 | Cadeia local → host A → host B com Rust/Python | Mesmo comportamento de negócio e vínculo pai/filho rastreável; nenhuma rota escolhida pelo plugin |
| R02 | Streams bidirecionais e consumidor lento | Bytes e filas limitados; controle progride dentro de orçamento previamente definido |
| R03 | Saturação por chamadas, dados, eventos e controle abusivo | Nenhum crescimento ilimitado; justiça e política de recusa documentadas |
| R04 | Retirada/revogação durante chamada e stream | Resultado/commit tardio rejeitados; independente responde; cleanup tem evidência |
| R05 | Partição assimétrica, perda de canal e retorno | Estados coerentes; sem reativação por mensagem velha; inventário reconciliado |
| R06 | Renovação atrasada ou resposta perdida | Nenhuma ressurreição de lease; chamada longa não impede renovação |
| R07 | Queda antes/depois de admissão e resultado persistidos | Consulta e unknown corretos; ausência de replay de efeito desconhecido |
| R08 | Duplicata entre sessões e principal alheio | Nenhuma execução duplicada; conteúdo divergente rejeitado; consulta não vaza resultado |
| R09 | Geração/fence antigos e recurso de outro owner | Rejeição no destino; nova geração continua funcionando |
| R10 | Credenciais inválidas, revogadas ou rotacionadas | Acesso e cleanup seguem política; nenhum fallback sem autenticação |
| R11 | Evento remoto, descarte e reintrodução | Inscrição revogada, perda contabilizada, callback antigo sem autoridade nova |
| R12 | Cliente/perfil antigo e host novo, e inverso | Compatibilidade declarada preservada; feature ausente falha explicitamente |
| R13 | Escrita/leitura parcial com progresso lento, TLS e EOF | Deadline total respeitado dentro da tolerância definida; canal truncado não reutilizado |

Usar fixtures genéricas e hosts em processos separados. Interleavings críticos precisam de barreiras e falhas controladas; stress complementa. Cenários de rede devem incluir atraso/perda/interrupção reproduzíveis, não apenas encerrar listener em loopback. O mecanismo de injeção é decisão de implementação e não deve alterar a rede global da máquina.

No relatório, separar o que foi exercitado em loopback com falhas injetadas de qualquer validação entre máquinas. Não reivindicar benchmark WAN ou prova formal a partir dessa suíte. Comparar as garantias locais/remotas numa matriz única.

## Entrega e revisão

Entregar código integrado, SDKs, configuração, documentação do perfil efetivo, vetores, suíte de falhas e demo genérica reproduzível. Reexecutar regressão M1–M6, compatibilidade M5 e smoke/demo existentes. Registrar comandos, resultados, limites, desvios e suas justificativas.

Decisões de estruturas internas, sincronização, canais, organização de crates e sequência de trabalho pertencem ao implementador. Escrever os contratos de fio e ownership antes dos mecanismos dependentes; isso é trabalho interno da epic, sem aprovação por slice.

M7 fecha quando R01–R13 e a regressão relevante têm evidência e não há bloqueadores conhecidos de autoridade, ownership, progresso ou recuperação. Uma limitação pode delimitar o perfil, mas não adiar uma entrega obrigatória desta epic sem mudança explícita de escopo.

O marco seguinte é M8: API pública e adoção independente. Aplicações continuam em outros repositórios.
