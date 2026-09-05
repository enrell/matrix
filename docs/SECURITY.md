# Segurança e fronteiras de autoridade

> Implementação disponível: [perfil gerenciado M3–M5](MANAGED-RUNTIME.md). Este documento preserva o contrato de projeto; consulte o perfil para saber quais mecanismos e transportes estão implementados.

Status: requisitos propostos; o scaffold atual não oferece este modelo de segurança.

## Modelo de ameaça

Considerar plugins com bugs ou maliciosos, peer remoto comprometido, payloads malformados, exaustão de recursos e respostas antigas após substituição. O núcleo e os hosts que aplicam isolamento pertencem à base confiável. Comprometimento administrativo do host escapa às garantias locais.

Rust reduz classes de erros de memória no código seguro; não impede falhas lógicas, deadlocks, abuso de permissões, unsafe incorreto ou ações de um processo externo. Plugins in-process são explicitamente confiáveis.

## Autoridade

Autenticação identifica o peer. Autorização limita cada ação por principal, contexto, recurso, interface e operação. Manifest solicita permissões; configuração confiável concede ou nega. Nome de capacidade não é credencial. Não executar comandos ou ampliar permissões porque um plugin solicitou em texto.

Grants são emitidos pelo kernel, associados à sessão/instância/geração, limitados e revogáveis. Chamadas transitivas não ampliam autoridade implicitamente; serviços validam o chamador efetivo para evitar uso de suas permissões em nome de um plugin sem acesso.

## Perfis

| Perfil | Proteção exigida | Limite |
|---|---|---|
| Confiável in-process | Contextos e accounting | Plugin pode violar o processo; sem contenção forte |
| Processo isolado | Usuário/permissões, limites, grupo de processos, mounts e rede restritos | Spawn sozinho não é sandbox |
| WASM | Imports concedidos, memória limitada e interrupção | Capacidades do host ainda precisam de autorização |
| Remoto | Canal autenticado, grants limitados, host confiável e fencing | Kernel não pode limpar diretamente um SO remoto particionado |

Ferramentas de isolamento Linux concretas serão escolhidas no M3. O perfil só pode ser anunciado quando demonstrar contenção de descendentes, acesso a arquivos, rede, CPU e memória.

## Workspace e segredos

Executor recebe snapshot/revisão identificada e somente dados necessários. Escritas ficam em workspace privado. Aplicação ao workspace do usuário é uma operação explícita com validação da revisão-base e detecção de conflito; descarte é separado.

Segredos são concedidos apenas ao componente que os precisa, com escopo e tempo definidos. Logs e inspeção omitem payloads sensíveis por padrão. Dados já enviados não podem ser revogados retroativamente. TLS não impede o receptor autorizado de ler o conteúdo.

## Revogação

Revogar capability bloqueia novas chamadas; revogar um handle impede operações futuras mediadas. Não alegar revogação de bytes/FDs já copiados. Operações em andamento e escritas externas seguem os contratos de [LIFECYCLE](LIFECYCLE.md) e [REMOTE](REMOTE.md).

Listener de rede é opt-in. Instalação local deve funcionar sem portas externas abertas. Provisionamento, rotação e revogação de identidade remota precisam de procedimento operacional antes de M5.
