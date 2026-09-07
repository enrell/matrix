# Contrato semântico e invariantes

Status: requisitos propostos v0.1. Garantias restritas aos efeitos mediados pelo runtime e aos recursos sob sua autoridade.

## Vocabulário

- **Plugin:** definição de código, manifest e interfaces.
- **Instância:** uma ativação concreta de um plugin.
- **Contexto:** escopo de autoridade e propriedade de uma instância ou operação.
- **Geração:** época monotônica de substituição dentro de uma identidade lógica.
- **Capacidade:** interface fornecida; sua existência não concede autorização de uso.
- **Grant:** autorização concedida pelo kernel para operações específicas.
- **Recurso:** handle concreto com proprietário e ação de liberação administrada.
- **Efeito:** alteração no ambiente do componente; pode ou não ser reversível.
- **Host:** executor que aplica lifecycle e isolamento no domínio onde possui autoridade.

Identidades são emitidas pelo kernel. A referência completa inclui época do kernel, id da instância e geração. Reutilizar um id lógico nunca revalida uma referência antiga. Handles são opacos e associados a principal/contexto; conhecer um identificador não concede acesso.

## Invariantes normativos

| Id | Requisito |
|---|---|
| I01 | Todo recurso gerenciado tem exatamente um proprietário vivo ou um registro explícito de cleanup pendente. |
| I02 | Nenhuma chamada é admitida em contexto que saiu de Active. |
| I03 | Operações de geração obsoleta são rejeitadas na fronteira que controla o efeito. |
| I04 | Uma instância Active possui bindings válidos para todas as dependências obrigatórias. |
| I05 | Aquisição publica um recurso somente depois de registrar sua propriedade; falha parcial dispara cleanup. |
| I06 | Descarte repetido é idempotente e não remove recursos de outra instância. |
| I07 | Disposed significa ausência de recursos gerenciados pendentes; incerteza aparece como CleanupPending. |
| I08 | Limpeza não depende exclusivamente da cooperação do plugin. Garantias do host são declaradas por tier. |
| I09 | Filas, mensagens, streams, recursos e chamadas possuem limites finitos configurados. |
| I10 | Resultado desconhecido nunca é reclassificado como “não executado” sem evidência. |
| I11 | Reversão de um componente preserva alterações independentes de outros componentes. |
| I12 | Eventos de lifecycle e erros podem ser associados à instância, geração e operação correspondentes. |

## Classes de efeitos

| Classe | Exemplo | Contrato |
|---|---|---|
| Reversível no contexto | Registrar handler, capacidade ou timer | Kernel/host mantém inversa e id de registro |
| Recurso cancelável | Task, processo gerenciado, stream | Interromper novas ações e confirmar liberação; cancelamento não desfaz efeitos anteriores |
| Alteração preparada | Edição em workspace privado | Descartar ou aplicar explicitamente com verificação de revisão |
| Ação externa | Envio, deploy, escrita em serviço independente | Resultado e idempotência explícitos; compensação depende do serviço |

Inversas devem usar propriedade/identidade de registro. Restaurar cegamente um snapshot global pode apagar trabalho de outro componente e viola I11. Cleanup usa ordem reversa de aquisição dentro do contexto e respeita a ordem de dependências; apenas LIFO não prova composição global.

v0.1 não oferece transferência arbitrária de recursos entre proprietários. Recursos compartilhados exigem um serviço proprietário e handles revogáveis por consumidor. Não expor raw FDs ou memória mutável revogável apenas “no papel”: cópias já entregues podem sobreviver à revogação.

## Fronteira da reivindicação

Spatiotemporal composability neste projeto exige I01–I12 nos perfis suportados. A implementação e a suíte de testes são evidência prática; não constituem prova formal do cálculo de referência. Sem isolamento, acessos ao SO fora do contexto escapam às garantias.
