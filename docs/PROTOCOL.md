# Matrix Component Protocol — rascunho v0.1

> Implementação disponível: [perfil gerenciado M3–M5](MANAGED-RUNTIME.md). Este documento preserva o contrato de projeto; consulte o perfil para saber quais mecanismos e transportes estão implementados.

Nome de trabalho; não é API estável. O JSON por linha do scaffold é um protocolo legado separado. O novo listener deve negociar explicitamente este protocolo e nunca inferir compatibilidade por ambos possuírem um campo `v`.

## Semântica e perfis

Um mesmo modelo de mensagens admite processo local e host remoto. Perfil inicial proposto: mensagens JSON UTF-8 com prefixo de comprimento u32 big-endian. Limite inicial de frame: 1 MiB; limites menores podem ser negociados. Inteiros de sequência/geração são strings decimais para interoperabilidade com runtimes sem inteiro de 64 bits exato.

Não é necessário um codec próprio. Este perfil favorece inspecionabilidade e SDKs iniciais; uma codificação binária poderá ser negociada após medição. Transferência de artefatos grandes usa streams limitados ou referências autorizadas, não frames ilimitados.

Transporte local: socket Unix em Linux. Remoto: stream confiável com TLS e identidade mútua; biblioteca e mecanismo de provisionamento ainda pendentes. Não inventar criptografia. Nenhum perfil remoto entra em conformidade antes de definir autenticação e autorização operacionalmente.

## Envelope ilustrativo

```json
{
  "protocol": "matrix.component",
  "version": "0.1",
  "type": "call.open",
  "message_id": "msg-unique",
  "session_id": "session-issued-by-kernel",
  "instance_id": "instance-issued-by-kernel",
  "generation": "3",
  "request_id": "request-unique",
  "body": {
    "binding_id": "binding-issued-by-kernel",
    "method": "search",
    "timeout_ms": 5000,
    "input": {"query": "symbol"}
  }
}
```

Identidades no envelope são alegações verificadas contra a sessão autenticada. O plugin não escolhe grants. O schema final por mensagem, inclusive campos obrigatórios, será produzido no M2; este envelope não é um schema executável.

## Famílias de mensagens

| Família | Direção predominante | Semântica |
|---|---|---|
| hello / welcome / reject | Host ↔ kernel | Negocia versão, perfil e limites; autenticação precede autorização |
| component.register / registered | Host ↔ kernel | Propõe manifest e recebe identidade; registro não ativa |
| lifecycle.prepare / activate / quiesce / dispose | Kernel → host | Comandos com operation_id, geração e prazo |
| lifecycle.result | Host → kernel | Confirma operação e informa recursos pendentes; ack de recepção não é conclusão |
| capability.changed | Kernel → host | Binding adicionado, retirado ou indisponível, com revisão do grafo |
| resource.acquire / release / result | Host ↔ kernel | Recursos mediados; restritos ao contexto e às permissões |
| call.open / accepted / result / error | Bidirecional mediada | Invocação e conclusão terminal correlacionadas |
| call.cancel / cancel.result | Bidirecional | Pedido e situação do cancelamento; não promete undo |
| stream.data / credit / end | Bidirecional | Sequências e crédito por stream |
| session.heartbeat / renew / close | Host ↔ kernel | Saúde e autorização temporária; heartbeat não prova execução correta |
| inspect.request / result | Cliente autorizado ↔ kernel | Estado, dependências e recursos; separado das permissões de plugin |

## Entrega e repetição

Conexão preserva ordem de frames; não há ordem global entre hosts. Cada request possui um resultado terminal aceito no kernel, mas isso não promete execução exatamente uma vez. `accepted` significa admitido, não concluído nem necessariamente durável.

IDs de lifecycle permitem repetição idempotente dentro da retenção negociada. Duplicatas com mesmo id e conteúdo divergente são erro. Invocações com efeitos não são repetidas automaticamente. Desconexão após admissão pode produzir `outcome-unknown`. Deduplicação persistente de efeitos só existe quando o destino fornece esse contrato.

Timeout é duração restante propagada e reduzida pelos intermediários; não comparar relógios de parede de máquinas diferentes como se fossem sincronizados. Cancelamento é best effort para código externo; revogação de autoridade é aplicada nas fronteiras de recursos.

## Fluxo e limites

Máximos por sessão: chamadas simultâneas, bytes em fila, frames, recursos, inscrições e streams. Configuração exata é parte do perfil de implantação e deve ser finita. Receptor concede créditos em bytes para dados de stream; dados sem crédito são rejeitados. Controle possui orçamento reservado e limitado para que cancel/dispose não dependam de uma fila de dados saturada.

Ao exceder limite, retornar `resource-exhausted` ou encerrar uma sessão abusiva segundo política. Antes de alocar payload, validar tamanho do frame. Rejeitar JSON inválido, ids duplicados de campos e estruturas acima dos limites de profundidade configurados.

## Erros

Códigos estáveis propostos: `invalid-message`, `unsupported-version`, `unauthenticated`, `permission-denied`, `dependency-unavailable`, `ambiguous-provider`, `stale-generation`, `context-not-active`, `resource-exhausted`, `deadline-exceeded`, `cancelled`, `cleanup-pending`, `outcome-unknown`, `internal`.

Erro inclui request_id, fase, código e detalhes seguros. Não fornece um booleano genérico “retryable” que autorize repetir efeitos; pode fornecer uma razão de indisponibilidade e contrato de idempotência da operação.

## Compatibilidade

Mudança de major de interface exige binding novo. Minor só adiciona capacidades compatíveis, negociadas. Tipo de mensagem desconhecido recebe erro; extensões opcionais desconhecidas só são ignoradas se o schema permitir. Sessões não negociadas não registram componentes.

M2 entregará schemas, vetores de mensagens válidas/inválidas e SDKs Rust/Python. O protocolo só será declarado implementado após C12–C14 e C24.
