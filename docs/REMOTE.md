# Plugins remotos e falhas distribuídas

> Implementação disponível: [perfil gerenciado M3–M5](MANAGED-RUNTIME.md). Este documento preserva o contrato de projeto; consulte o perfil para saber quais mecanismos e transportes estão implementados.

Status: extensão proposta, posterior à conformidade local. Uma autoridade de composição por domínio no v0.1; sem eleição automática de líder ou federação.

## Responsabilidades

Kernel: grafo, admissão, grants, gerações e decisões de lifecycle. Host remoto: processos, recursos locais, isolamento, prazos e cleanup. Plugin: lógica e chamadas por interfaces autorizadas.

O host remoto é base confiável para os recursos sob seu controle. Um plugin remoto arbitrário sem host verificável pode expor RPC, mas não recebe automaticamente garantia de cleanup forte.

## Estados de conexão

Connected → Suspect → Detached. Suspect retira admissão conforme política; expiração da autorização desativa execução autorizada no host. Reconexão exige negociação e reconciliação. O kernel pode revogar participação local imediatamente, mantendo recursos remotos como CleanupPending até obter evidência.

Heartbeat é observação de conectividade, não prova de morte ou liberação. A distinção entre partição e queda real é preservada.

## Autorizações temporárias e fencing

Sessão possui época, sequência de renovação e tempo máximo de validade. Host usa relógio monotônico e expira autorização sem renovação. Mensagens de renovação antigas não prolongam indefinidamente uma época retirada; handshake/nonce e validade precisam ser definidos no perfil remoto, incluindo premissas de atraso e relógio. Até isso ser validado, não reivindicar limite rígido de revogação remota.

Fencing usa época/generation monotônica e é verificado pelo serviço que controla o efeito. Após aceitar uma época nova, o destino rejeita a velha. Se o destino não suporta fencing, classificar ações como externas e não prometer exclusão de escritas antigas.

Encerrar a conexão no kernel ou rejeitar uma resposta não impede por si só um processo remoto de escrever em outro serviço.

## Matriz de falhas

| Situação | Comportamento requerido |
|---|---|
| Desconexão antes de envio confirmado | Registrar estado da entrega; só reenviar automaticamente se não houve admissão comprovada ou operação for idempotente |
| Admitiu, perdeu resposta | `outcome-unknown`; consultar destino por operation_id quando suportado |
| Host cai | Dependências indisponíveis; recursos remotos pendentes até recuperação/reconciliação |
| Plugin cai, host continua | Host limpa recursos e reporta falha com evidência |
| Resposta de geração anterior | Não atualiza estado atual; resultado vai para reconciliação/auditoria |
| Kernel reinicia | Nova época ou recuperação durável da anterior; reautenticar e reconciliar antes de reativar |
| Rede volta | Inventariar instâncias, recursos e operações; revogar órfãos antes de publicar capacidades |

## Dados e latência

Transferir revisão imutável, hash/identidade do conteúdo e política de acesso; resultados apontam para a revisão executada. Um teste verde em outra revisão não valida a edição atual. Artefatos têm limites e autorização próprios.

Preferir operações suficientemente grandes para amortizar RPC: um build remoto, não uma chamada de rede por linha lida. Chamadas locais continuam locais. Otimização de codec ou canal direto depende de benchmark.

## Critério de entrada

M5 exige autenticação operante, rotação de identidade, C20–C23, cenários de partição/reconexão e declaração das garantias por destino. “Remoto conectado” sozinho não satisfaz composição distribuída.
