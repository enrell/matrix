//! Conformidade M2.5: composição integrada através de IPC.
//!
//! Provedor Python (`workspace.fs@1`) + consumidor em-processo (`search`,
//! com `requires`) + independente (`echo`): binding através do socket,
//! retirada/reintrodução do provedor preservando as garantias (M1+M2):
//! consumidor cai para `Waiting` e volta em nova geração, ticket antigo
//! inválido, independente intacto, sem órfãos nem mistura de gerações.

use matrix_core::{Journal, Kernel};
use matrix_host::Host;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-m25-{}-{}-{}",
        std::process::id(),
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    for d in ["plugins", "run", "host"] {
        let _ = std::fs::create_dir_all(dir.join(d));
    }
    dir
}

fn write_manifest(home: &PathBuf, id: &str, body: Value) -> PathBuf {
    let p = home.join("plugins").join(format!("{}.json", id));
    std::fs::write(&p, serde_json::to_string_pretty(&body).unwrap()).unwrap();
    p
}

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timeout aguardando: {}", msg);
}

fn code_of(v: &Value) -> String {
    v.get("code").and_then(|c| c.as_str()).unwrap_or("").to_string()
}

#[test]
fn python_provider_rust_consumer_lifecycle() {
    let home = fresh_home("py-provider");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sdk-python/ext_echo.py");
    assert!(script.exists(), "sdk-python/ext_echo.py ausente");
    let python = std::env::var("PYTHON3").unwrap_or_else(|_| "python3".to_string());

    // Provedor Python: workspace.fs@1 via IPC.
    let m_ws = write_manifest(
        &home,
        "workspace",
        json!({
            "id": "workspace", "version": "1.0.0",
            "capabilities": ["workspace.fs@1"], "subscriptions": [],
            "reducer": "external", "init_state": {}, "tier": "process",
            "trust": "trusted", "restart": "permanent",
            "execution": {"kind": "process", "entrypoint": python,
                          "args": [script.to_string_lossy().to_string(),
                                   "--matrix-sock", "{sock}", "--id", "{id}"],
                          "timeout_ms": 8000},
        }),
    );
    // Consumidor em-processo com dependência declarada.
    let m_search = write_manifest(
        &home,
        "search",
        json!({
            "id": "search", "version": "1.0.0",
            "capabilities": ["search.query@1"],
            "requires": [{"interface": "workspace.fs@1", "provider": "workspace"}],
            "subscriptions": [], "reducer": "noop", "init_state": {},
            "tier": "inproc", "trust": "trusted", "restart": "permanent",
        }),
    );
    let m_echo = write_manifest(
        &home,
        "echo",
        json!({
            "id": "echo", "version": "1.0.0", "capabilities": ["echo.msg@1"],
            "subscriptions": [], "reducer": "echo", "init_state": {},
            "tier": "inproc", "trust": "trusted", "restart": "permanent",
        }),
    );

    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    kernel.load_manifest(&m_ws).unwrap();
    kernel.load_manifest(&m_search).unwrap();
    kernel.load_manifest(&m_echo).unwrap();
    let host = Host::attach(kernel.clone(), &home.join("host")).expect("attach");

    // search só ativa com o provedor Python; binding aponta a instância IPC.
    let ws1 = kernel.instance_ref_of("workspace").expect("ws gen1");
    wait_for("sessão python gen1", Duration::from_secs(15), || {
        host.has_session("workspace", ws1.instance)
    });
    wait_for("search ativa", Duration::from_secs(10), || {
        kernel.context_state_of("search").as_deref() == Some("Active")
    });
    let bs = kernel.bindings_of("search");
    assert_eq!(bs.len(), 1);
    assert_eq!(bs[0].provider_logical, "workspace");
    assert_eq!(bs[0].provider_instance, ws1.instance);

    // Chamadas através do IPC: provedor ecoa, consumidor responde.
    let (v, ok) = kernel.invoke("workspace.fs@1", &json!({"path": "a.txt"}));
    assert!(ok, "provedor python: {}", v);
    assert_eq!(v.get("echo").unwrap().get("path").unwrap(), &json!("a.txt"));
    assert!(kernel.invoke("search.query@1", &json!({})).1);
    assert!(kernel.invoke("echo.msg@1", &json!({"ping": 1})).1);

    // Ticket admitido na geração vigente...
    let search1 = kernel.instance_ref_of("search").expect("search gen1");
    let open = kernel.call_open("search.query@1", &json!({}), None, &[]).expect("abre");

    // ...retira o provedor Python: cascata automática, sem toque manual.
    kernel.dispose_plugin("workspace");
    assert!(host.wait_no_child("workspace", Duration::from_secs(10)), "filho python colhido");
    wait_for("search em Waiting", Duration::from_secs(10), || {
        kernel.context_state_of("search").as_deref() == Some("Waiting")
    });
    assert!(kernel.caps.resolve("workspace.fs@1").is_none());
    assert!(kernel.caps.resolve("search.query@1").is_none());
    // Ticket da geração retirada: inválido (cancelado na retirada).
    let err = kernel.commit_effect(open.ticket, "w", &json!({})).unwrap_err();
    assert!(["cancelled", "stale-generation"].contains(&code_of(&err).as_str()),
        "ticket antigo rejeitado: {}", err);
    assert!(kernel.call_close(open.ticket));
    // Independente intacto durante tudo.
    assert!(kernel.invoke("echo.msg@1", &json!({"ping": 2})).1);

    // Reintroduz o provedor: novo processo, nova sessão, search reativa sozinha.
    kernel.load_manifest(&m_ws).unwrap();
    let ws2 = kernel.instance_ref_of("workspace").expect("ws gen2");
    assert!(ws2.generation > ws1.generation);
    assert_ne!(ws2.instance, ws1.instance);
    wait_for("sessão python gen2", Duration::from_secs(15), || {
        host.has_session("workspace", ws2.instance)
    });
    assert!(!host.has_session("workspace", ws1.instance), "sessão antiga fora");
    wait_for("search reativa", Duration::from_secs(10), || {
        kernel.context_state_of("search").as_deref() == Some("Active")
    });
    let bs = kernel.bindings_of("search");
    assert_eq!(bs[0].provider_instance, ws2.instance, "binding na geração nova");
    let search2 = kernel.instance_ref_of("search").expect("search gen2");
    assert_ne!(search2.instance, search1.instance, "consumidor em nova instância");

    // Nova geração funciona; ticket da antiga segue inválido.
    let (v, ok) = kernel.invoke("workspace.fs@1", &json!({"path": "b.txt"}));
    assert!(ok, "provedor novo: {}", v);
    assert!(kernel.invoke("search.query@1", &json!({})).1);
    assert!(kernel.invoke("echo.msg@1", &json!({"ping": 3})).1);
    assert_eq!(code_of(&kernel.commit_effect(open.ticket, "w", &json!({})).unwrap_err()), "unknown-ticket");

    // Sem órfãos nem mistura: registry só com donos correntes e ativos.
    for (cap, owner) in kernel.caps.snapshot() {
        let r = kernel.caps.resolve_ref(&cap).expect("resolve");
        let cur = kernel.contexts.current(&owner).expect("corrente");
        assert_eq!(r.instance, cur.instance.0, "cap {} sem mistura", cap);
        assert_eq!(cur.state.as_str(), "Active", "dono de {} ativo", cap);
    }
    host.shutdown();
}
