# Direção do Matrix

Status: direção proposta a partir da discussão de 2026-09-05; mecanismos ainda sujeitos à implementação e validação.

## Objetivo

Construir um kernel pequeno em Rust que administre a composição dinâmica de componentes: suas dependências, efeitos gerenciados, recursos e ciclos de vida. Plugins podem usar diferentes linguagens; execução remota é uma extensão do contrato local.

## Fronteira

O kernel possui as regras de propriedade, autorização, composição, comunicação e descarte. Hosts executam componentes sob essas regras. Modelo, ferramentas, memória, planejamento, interface e loop de agente ficam acima dele.

“Tudo é plugin” aplica-se às funcionalidades da aplicação. O mecanismo que aplica as garantias não pode depender de um plugin não confiável obedecer voluntariamente.

## Compromissos

1. Cada garantia tem um invariante, um teste de aceitação e um limite de aplicabilidade.
2. Estado atual, proposta e resultado medido são identificados separadamente.
3. Contratos de componentes independem do transporte e da linguagem.
4. O kernel administra efeitos no contexto; não promete reversão universal de ações externas.
5. Desconexão não prova morte; timeout não prova que uma operação não executou.
6. Plugins sem isolamento são explicitamente confiáveis. Rust não fornece isolamento de plugins por si só.
7. O sistema funciona localmente sem infraestrutura remota obrigatória.
8. Desempenho será medido sob contratos equivalentes; score de projeto ancestral não é evidência do Matrix.
9. A fundação e os contratos permanecem abertos; serviços proprietários podem ser adaptadores opcionais.

## Sucesso

O marco local demonstra dependências reativas e limpeza de efeitos gerenciados sob interleavings. O marco de processo demonstra o mesmo contrato em Rust e Python. O marco remoto demonstra comportamento definido sob partições, reconexão e resultados desconhecidos. Só então a aplicação de agente demonstra edição e validação de uma revisão real.

A documentação não reivindica equivalência formal ao cálculo do Cordis. Essa equivalência exigiria modelagem e demonstração adicionais.

[Plano e critérios](docs/ROADMAP.md)
