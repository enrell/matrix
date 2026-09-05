# Persistência, recuperação e efeitos externos

> Implementação disponível: [perfil gerenciado M3–M5](MANAGED-RUNTIME.md). Este documento preserva o contrato de projeto; consulte o perfil para saber quais mecanismos e transportes estão implementados.

Status: proposta. O journal atual não é uma implementação deste contrato.

## Dados separados

1. Estado desejado: manifests, bindings e políticas autorizadas.
2. Estado observado: instâncias, fases e recursos realmente presentes.
3. Ledger de operações: intenção, admissão, resultado, incerteza e reconciliação.
4. Estado da aplicação: snapshots versionados, sob contrato próprio do plugin.

Recuperação reconcilia desejado e observado; não simplesmente reexecuta todos os registros.

## Durabilidade

Perfis propostos: efêmero, buffered e durable. Respostas declaram se persistência é garantida; um ack de admissão nunca implica durabilidade por omissão. No perfil durable, falha de append/sync impede confirmação durável e aparece como erro.

Escolha entre banco transacional ou journal segmentado fica aberta até M4. Ambos devem definir registros completos, corrupção, ordenação, checksum quando aplicável, sync, snapshot e retenção. Não descartar silenciosamente corrupção no meio do histórico. Cauda parcial pode ser recuperada apenas por regra documentada.

Snapshot e prune precisam preservar a informação necessária para deduplicação, gerações e operações desconhecidas. Não podar efeitos pendentes para reduzir disco sem torná-los explícitos.

## Execução de efeitos

Intenção durável antes de chamar destino ajuda a recuperar, mas não resolve atomicidade entre kernel e serviço externo. A janela “destino executou, confirmação não foi persistida” sempre exige contrato do destino.

- Efeito interno transacional: commit de estado e ledger juntos quando suportado.
- Efeito externo idempotente: chave de operação e retenção negociada no destino.
- Efeito externo consultável: consultar resultado e reconciliar.
- Sem nenhum dos anteriores: marcar resultado desconhecido; não repetir automaticamente.

Compensação é uma ação adicional que também pode falhar. Nunca apresentar compensação como inversa perfeita universal.

## Recuperação

Boot valida armazenamento, identifica época, carrega desejado sem publicar instâncias como Active e reconcilia recursos/hosts. Handles antigos não voltam a valer apenas porque existiam no log. Retomar o loop do agente requer checkpoint e política para cada ferramenta.

Workspace privado pode ser descartado ou aplicado com verificação de base. Reconstruir estado interno não autoriza reenviar mensagens, executar deploy ou reaplicar comandos shell.
