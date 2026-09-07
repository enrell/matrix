# Perfil gerenciado M3–M5

Implementação experimental de `matrix-guard` e `matrix-runtime`, integrada ao core e host M2.5. Entrada operacional: `matrix-managed`. O daemon legado `matrix-rt` continua disponível como perfil confiável; não adquire automaticamente as garantias deste perfil.

## O que foi implementado

| Marco | Caminho executável | Evidência |
|---|---|---|
| M3 | HostPolicy autorizado pelo operador, token de lançamento por instância, grants, revogação, Bubblewrap + seccomp/rlimits, supervisor | `matrix-guard/tests/isolation.rs`, `matrix-runtime/tests/managed_host.rs`, C10/C23 em `security_remote.rs` |
| M4 | SQLite WAL + synchronous FULL, dono exclusivo, ledger de operações, fencing persistente, snapshot completo e retenção de auditoria | `tests/durability.rs` e teste SQLITE_FULL em `store.rs` |
| M5 | TLS mútuo, pin de certificado cliente, lease expirada no destino, adaptador de proxy, revogação em cascata e reconciliação explícita | `tests/security_remote.rs` |

Os testes usam processos reais (Rust/Python), namespaces Linux, chamadas TLS em loopback, SIGKILL e transações SQLite. Rede em loopback testa o protocolo, não desempenho WAN. A suíte completa inclui M1 e M2.5.

## M3: perfil de processo

A configuração confiável concede execução por id; o manifest não escolhe suas próprias permissões. O host gera um token aleatório por lançamento. SDKs Rust/Python o enviam no hello, e o host verifica seu vínculo à instância antes de registrar a sessão. Uma conexão com o nome correto, mas sem token, não pode assumir uma capacidade.

Sandbox suportada: Linux x86_64 com `/usr/bin/bwrap`, namespaces de usuário/PID/mount/rede, `no_new_privs`, capabilities removidas, ambiente filtrado e seccomp. Somente `/workspace` e `/tmp` privado são graváveis; `/usr`, executável, socket específico e mounts escolhidos pelo operador são expostos para leitura. Memória virtual por processo, CPU acumulada, tamanho de arquivo, descritores e core dumps têm limites.

O perfil permite threads e **nega criação de novos processos**. É adequado a provedores e ferramentas que não precisam executar subprocessos. Builds que exigem fork/exec precisam de outro perfil com accounting de descendentes (por exemplo cgroups); não desabilitar o filtro e continuar chamando-o de mesma sandbox. O limite de memória não mede todo overhead do kernel Linux.

No perfil `trusted`, processos seguem autenticados, mas não possuem isolamento de arquivos/rede. A opção exige escolha explícita. O host encerra o grupo gerenciado e faz wait; gerações retiradas não podem ser ressuscitadas por eventos de ativação enfileirados.

O supervisor do serviço usa orçamento em janela, backoff crescente e jitter determinístico. Cada tentativa cria nova instância. Falha terminal aparece no inventário de leases; a definição não entra num loop ilimitado. A política é opt-in por componente. Credenciais revogadas são persistidas e não voltam a valer num restart; regrant é operação administrativa explícita.

## M4: persistência e garantias

`state/runtime.sqlite` é exclusivo a um runtime, protegido por flock e permissões privadas. Epoch de inicialização e fences são monotônicas no banco. Referências do core recebem identidade aleatória por boot. Estado desejado é armazenado, mas não é automaticamente reexecutado ao recuperar.

O serviço registra admissão antes de invocar. Um resultado só declara `durability: durable` após commit FULL do ledger. Se houver queda após admissão, a recuperação marca `unknown`; não executa novamente. Repetir o mesmo operation_id/principal/payload devolve resultado já concluído. Divergência de payload é erro. Não há exactly-once para comandos externos.

`effect.commit` é um destino demonstrável de efeitos: KV por componente, com validação de fence, mutação e deduplicação na mesma transação. A escrita obsoleta é rejeitada **no destino**, não apenas na resposta do kernel. Isso não torna escritas arbitrárias de plugins transacionais.

Limites: frame/body até 1 MiB, ledger até 100.000 operações e auditoria retém os últimos 10.000 eventos. Tombstones e operações desconhecidas não são podados automaticamente. Ao esgotar o ledger, novas admissões falham; arquivamento/rotação exige decisão operacional. Snapshot inclui estado desejado, ledger, revogações, fences e KV; recusa sobrescrever um destino existente. Erro de disco/corrupção não produz confirmação durável.

O journal JSONL legado é diagnóstico. Persistência durável de toda mutação interna do core e restauração de memória arbitrária de plugins não estão prometidas. Restaurar snapshot exige parar o dono atual e reconciliar o estado externo; não reutilizar leases antigas. O novo processo não publica componentes até ativação autenticada.

## M5: transporte e autoridade

O perfil de controle de hosts é `matrix.managed/0.1` (também ALPN), distinto de `matrix.component/0.1` usado entre host e plugin. É JSON enquadrado por u32 big-endian sobre TLS mútuo. Reutiliza o scanner estrito de JSON. Até 32 conexões, prazo absoluto de 35s por transporte e frames de 1 MiB; nenhum listener de rede existe sem configuração explícita.

A CA valida certificados; o fingerprint SHA-256 do certificado cliente identifica o principal e deve existir nos grants. O cliente valida também o nome DNS do servidor. Não há modo sem autenticação nem opção de ignorar validação de certificado.

Ações: `activate`, `status`, `renew`, `release`, `invoke`, `operation`, `effect.commit`. Somente componentes previamente provisionados pelo operador podem ativar. RPC não aceita comandos de instalação, manifests executáveis ou grants arbitrários. O gateway é unary; não transporta ainda streams de artefatos do perfil de componente. Esses streams permanecem locais no M2.

Lease: 100–30.000 ms, medida no relógio monotônico do host que controla o recurso. Renewal gira o token; token antigo não renova novamente. Não se pressupõe sincronização de relógios entre máquinas. Resposta de renewal perdida exige reconciliação/expiração, não retry cego.

`RemoteProxy::attach` conecta uma instância externa do kernel controlador a um componente provisionado no serviço remoto. Preserva hooks/forwarders locais, renova a lease e retira a definição local quando perde autoridade; dependentes entram em Waiting. Retirada local agenda release remoto. Se a rede estiver indisponível, o host retira o componente quando a lease expira. Deadline/cancelamento interrompe a espera de transporte; não desfaz uma operação externa já executada.

Este perfil é um único controlador e hosts subordinados, não federação/múltiplos líderes. Fencing forte cobre o KV gerenciado; outros destinos precisam aplicar seu próprio fence/idempotência. A indisponibilidade remota é observada com atraso de rede/renovação, não atomicamente em todas as máquinas. A reconexão cria uma nova lease; resultados desconhecidos são consultados explicitamente por operation_id.

## Executar e validar

```sh
./scripts/test-managed.sh
cargo build --release -p matrix-runtime
./target/release/matrix-managed serve /caminho/absoluto/config.json
```

Requisitos dos testes: Linux x86_64, Rust, Bubblewrap com user namespaces, Python 3 e OpenSSL. Os testes de sandbox não são ignorados silenciosamente quando o host não oferece o perfil.

Exemplo de configuração (substitua caminhos e fingerprint):

```json
{
  "home": "/caminho/privado/matrix-state",
  "components": [{
    "manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"},
    "trusted": true,
    "sandbox": null,
    "restart": null
  }],
  "grants": {
    "FINGERPRINT_SHA256_DO_CLIENTE": {
      "components": ["echo"],
      "capabilities": ["echo.msg@1", "matrix.effect.write"]
    }
  },
  "tls": {
    "listen": "127.0.0.1:7443",
    "ca": "/caminho/pki/ca.der",
    "cert": "/caminho/pki/server.der",
    "key": "/caminho/pki/server-key.der"
  }
}
```

Para um processo isolado, use manifest `execution.kind=process`, selecione o executável e args do M2, `trusted=false` e configure `sandbox`:

```json
{
  "workspace": "/caminho/privado/workspace",
  "read_only": [],
  "memory_bytes": 268435456,
  "cpu_seconds": 5,
  "file_bytes": 1048576,
  "open_files": 64
}
```

O workspace já deve existir. O processo vê seu conteúdo em `/workspace`. Para Python, conceda a pasta do SDK/script em `read_only`. Não conceda toda a home.

PKI de desenvolvimento, com diretório novo e chaves privadas:

```sh
./scripts/dev-pki.py /caminho/novo/pki --server-name localhost
./target/release/matrix-managed fingerprint /caminho/novo/pki/client.der
```

Use o fingerprint nos grants. CA de desenvolvimento não substitui provisionamento de produção. Proteja as chaves e distribua somente CA pública, certificado e chave do respectivo peer.

Chamadas ilustrativas:

```sh
./target/release/matrix-managed request /pki/ca.der /pki/client.der /pki/client-key.der 127.0.0.1:7443 localhost '{"action":"activate","component":"echo","ttl_ms":10000}'
```

A resposta contém `lease` e `fence`; substitua nos próximos pedidos. Lease é credencial temporária: não a grave em logs compartilhados.

```json
{"action":"invoke","lease":"TOKEN","fence":"1","operation":"op-unico","cap":"echo.msg@1","input":{"ping":true}}
```

```json
{"action":"release","lease":"TOKEN","fence":"1"}
```

## Chamadas a dependências (M6)

O perfil gerenciado anuncia `dependency-calls/1` e autoriza via grants
outbound do operador, independentes do solicitado no manifest:

```json
{
  "outbound_grants": {"cons": ["prov.api@1"]}
}
```

Sem entrada, a admissão nega com `permission-denied`; remover recarregando
(SIGHUP) revoga e invalida filhas admitidas. O manifest do consumidor
declara `requires` + `outbound.request` com os 7 limites finitos; o host
entrega os handles no `lifecycle.activate`. Demonstração ponta a ponta
(cadeia Rust → Python, retirada e reintrodução):

```sh
python3 scripts/demo-composition.py
```

Detalhes e desvios da spec: [M6-COMPOSITION](M6-COMPOSITION.md).

## Composição remota (M7)

O mesmo modelo de dependências atravessa hosts via `matrix.remote/0.1`
(mTLS, sessão persistente multiplexada, controle com orçamento reservado).
O consumidor escolhe só seu binding `rb-N`; rota e localização resolve o
kernel. Demonstração (Python → Rust entre hosts gerenciados):

```sh
python3 scripts/demo-remote.py
```

Configuração (`serve <config.json>`):

```json
{
  "remotes": {
    "authority": "<fingerprint do controlador>",
    "domain": "demo",
    "peers": [{
      "name": "exec-A", "address": "127.0.0.1:40001",
      "server_name": "localhost",
      "ca": "ca.der", "cert": "client.der", "key": "client-key.der",
      "mgmt_address": "127.0.0.1:40000",
      "domain": "demo", "lease_ttl_ms": 8000
    }],
    "routes": [{"consumer": "cons", "provider": "prov", "peer": "exec-A",
                "capabilities": ["prov.api@1"]}],
    "session_listen": "127.0.0.1:40001",
    "session_ca": "ca.der", "session_cert": "server.der", "session_key": "server-key.der"
  }
}
```

- Manifests com `"remote": true` são snapshots de capacidade (nunca
  materializam local); instale via `components` e o route manager registra
  após atestação (instância/geração) + reconcile + subscribe.
- `session_listen` ausente = só controlador (sem sessões inbound).
  Mudança de listener exige restart; peers/routes sincronizam no SIGHUP
  (remove = unregister + release; adiciona = attach com backoff).
- Streams/eventos/renovação seguem o perfil; detalhes, garantias
  local×remoto, evidência R01–R13 e runbook: [M7-COMPOSITION](M7-COMPOSITION.md).

## Operação e rotação

SIGTERM/SIGINT retira leases e encerra hosts. SIGHUP relê somente grants do config: retira as leases atuais antes de instalar os grants novos. Para trocar certificado/chave do servidor ou política de sandbox, faça restart controlado; clientes reconectam com lease nova. Não manter identidade antiga na lista se a intenção é revogá-la.

Snapshot offline (falha se existir dono ativo):

```sh
./target/release/matrix-managed snapshot /caminho/privado/matrix-state /backup/novo.sqlite
```

A API Rust `Store::snapshot` também suporta snapshot online pelo próprio dono. Inspeção de `Service::inspect` mostra owner lógico, geração, pendências, falha/restart e tempo restante; não revela tokens. Uma resposta `outcome-unknown` pede consulta/reconciliação, nunca repetição automática da ação.

## Integração com M2.5

M2.5 foi incorporado sem substituir sua CLI ou SDK. Alterações compartilhadas: token opcional no hello dos SDKs, HostPolicy/launcher no host, `invoke_for_ref` e época de boot no core. `Host::attach` preserva o perfil confiável legado; o serviço gerenciado exige `Host::attach_with_policy(secure=true)`.
