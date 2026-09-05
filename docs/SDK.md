# Contrato de plugins e SDKs

Status: proposta v0.1. Para escrever um manifest aceito hoje, ver [PLUGIN](../PLUGIN.md).

## Manifest conceitual

```json
{
  "manifest_version": "0.1",
  "id": "agent-loop",
  "component_version": "0.1.0",
  "execution": {"kind": "process", "entrypoint": "agent-loop"},
  "requires": [
    {"interface": "model.complete", "major": 1, "provider": "model-local"},
    {"interface": "tools.execute", "major": 1, "provider": "workspace-tools"}
  ],
  "provides": [{"interface": "agent.run", "major": 1}],
  "permissions_requested": ["model.complete", "tools.execute"],
  "restart": "on-failure"
}
```

Exemplo não executável no scaffold. Configuração de implantação resolve entrypoint, versões, hosts, quotas e grants; um manifest recebido pela rede não autoriza executar um caminho arbitrário.

## API conceitual

```text
prepare(context, configuration, bindings) -> prepared
activate(prepared) -> ready
handle(context, operation, input, cancellation) -> result/stream
quiesce(context, reason, budget) -> acknowledgement
dispose(context) -> cleanup_report
```

Context oferece `provide`, `subscribe`, `spawn_task`, `acquire_resource` e `invoke`. Cada aquisição retorna handle opaco com owner e inversa gerenciada. SDK pode liberar ergonomicamente via RAII/with/context manager, mas o kernel/host mantém accounting e cleanup se o callback não rodar.

Não cruzar fronteiras de processo com ponteiros, closures Rust ou referências de memória. Interfaces possuem schemas e versões. SDKs traduzem mensagens, streaming, erros e cancelamento preservando semântica.

## Estratégia de linguagens

M1 usa componentes Rust compilados e confiáveis para provar a semântica. M2 introduz hosts/processos e SDKs Rust e Python com a mesma suíte. M3 reforça isolamento. WASM Component Model/WIT é opção posterior com o mesmo contrato de recursos; suporte de cada linguagem deve ser verificado antes de anunciar compatibilidade.

Não usar ABI nativa Rust de bibliotecas dinâmicas como contrato universal. Cada perfil deve informar suporte real a limites, cancelamento, cleanup e migração.

## Aplicação de agente

Componentes propostos: provedor de modelo, workspace, catálogo de ferramentas, loop, histórico/memória, interface e executor de testes. Kernel não incorpora prompts, chain-of-thought ou lógica de planejamento.

A demonstração real deve editar numa área privada, executar teste na revisão correta e devolver alteração revisável. Testar pipeline com modelo simulado é útil para conformance, mas não prova um code agent funcional.
