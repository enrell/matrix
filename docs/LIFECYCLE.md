# Ciclo de vida, composição e supervisão

Status: proposta normativa v0.1; substituirá a FSM superficial do scaffold.

## Estados

| Estado | Significado | Admite chamadas? |
|---|---|---|
| Registered | Manifest validado, sem ativação |
| Waiting | Dependência obrigatória indisponível |
| Preparing | Recursos provisórios e bindings preparados |
| Active | Publicado e autorizado | Sim |
| Quiescing | Novas chamadas bloqueadas; drenagem/cancelamento |
| CleanupPending | Recursos locais ou remotos ainda não confirmados como liberados |
| Disposed | Instância terminou e recursos gerenciados foram liberados |
| Failed | Falha registrada após cleanup confirmado; política pode criar nova instância |

Células vazias na coluna de admissão significam não. Fluxo normal: Registered → Waiting ou Preparing → Active → Quiescing → CleanupPending → Disposed. Cleanup sem pendências pode concluir imediatamente. Falha durante preparação retira recursos provisórios antes de Failed. Se houver incerteza, permanece CleanupPending com causa registrada.

Uma definição registrada pode criar nova instância após descarte. Disposed nunca volta a Active; reativação usa nova identidade/geração.

## Dependências

Manifest declara interfaces fornecidas e requisitos obrigatórios. v0.1 usa bindings explícitos ou provedor único; ambiguidade é erro, não “último registro vence”. Incompatibilidade de versão mantém Waiting com razão visível. Requisitos opcionais e seleção automática com fallback ficam adiados.

O grafo obrigatório deve ser acíclico no v0.1. Ciclos são rejeitados com caminho explicativo. Ativação segue ordem topológica. Retirada bloqueia admissão no conjunto de consumidores afetados antes da limpeza, que segue consumidores antes de provedores.

## Ativação

1. Validar manifest, autorização, versões, limites e grafo.
2. Reservar identidade e bindings para uma geração específica.
3. Criar contexto provisório e executar preparação com prazo.
4. Revalidar bindings; mudanças durante a preparação abortam a tentativa.
5. Publicar capacidades e marcar Active em um commit de metadados.

Falha no passo 5 remove registros provisórios. Preparação não deve executar efeitos externos irreversíveis.

## Retirada sob concorrência

O ponto de linearização é o commit que marca Quiescing e revoga admissão. Tickets já admitidos seguem política por operação: drenagem limitada ou cancelamento. Serviços de efeitos revalidam grant/geração antes de commit. Um efeito já concluído antes da revogação é registrado, não magicamente desfeito.

Callbacks são cancelados e joins têm prazo. Falha ao liberar não é ignorada: mantém CleanupPending e diagnóstico. Um host de processo pode escalar para término do grupo gerenciado; em código in-process não confiável não há essa garantia.

## Reload v0.1

A primeira versão usa substituição coordenada com possível pausa de disponibilidade. Não promete zero downtime.

1. Validar candidato sem publicá-lo.
2. Retirar geração anterior e dependentes afetados.
3. Confirmar cleanup dos recursos incompatíveis com sobreposição.
4. Criar candidato e publicar uma nova geração após preparação válida.
5. Reativar consumidores com bindings novos.

Se o candidato falhar, manter indisponibilidade explícita. Recriar a versão anterior usa nova instância; não ressuscitar handles antigos. Migração de estado é opt-in, versionada, com entrada imutável e saída validada. Troca sem interrupção e rollback de migração ficam para uma extensão posterior.

## Supervisão

Política por componente: nunca reiniciar, reiniciar em falha, ou manter ativo enquanto desejado. Reiniciar sempre cria nova instância após tratamento do cleanup. Cada política tem orçamento em janela móvel, backoff, jitter e estado terminal observável. Retirada administrativa/dependência ausente não deve causar restart storm. Quotas globais do host limitam cascatas entre componentes.
