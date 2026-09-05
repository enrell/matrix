# Referências e atribuição

Referências consultadas na discussão de projeto de 2026-09-05. A arquitetura Matrix é uma proposta própria de implementação; não reivindica inventar composição espaço-temporal ou equivalência formal ao Cordis.

- [A Programming Paradigm for Spatiotemporal Composability — autores/Cordis](https://github.com/cordiverse/paper): efeitos reversíveis, dependências reativas e contexto que medeia a composição. [Preprint](https://arxiv.org/abs/2608.25512).
- [WebAssembly Component Model](https://component-model.bytecodealliance.org/introduction.html): interfaces entre componentes de diferentes linguagens; referência para um perfil futuro, não dependência instalada do Matrix.
- [Unix domain sockets — Linux manual](https://man7.org/linux/man-pages/man7/unix.7.html): IPC local, credenciais e transferência de descritores. Transferir um descritor não implica poder revogar suas cópias depois.
- [gRPC deadlines](https://grpc.io/docs/guides/deadlines/) e [cancellation](https://grpc.io/docs/guides/cancellation/): referência para distinguir prazo/cancelamento de desfazer efeitos. Não obriga usar gRPC.

Proveniência local: scaffold derivado conceitualmente do master3 em agentlab. Resultados de desempenho/compatibilidade desse projeto permanecem atribuídos a ele. A revisão documental não reexecutou seus benchmarks.

[Documentos históricos](history/README.md) preservam os textos anteriores, inclusive afirmações mais amplas que o código atual. A fonte para estado implementado é [STATUS](STATUS.md), com links para o código inspecionado.
