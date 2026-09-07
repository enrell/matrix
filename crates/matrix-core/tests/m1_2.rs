//! M1.2 conformance: C04–C05 + exit demo.
//!
//! - C04: consumer before/after provider → Waiting/Active.
//! - C05: remover provedor compartilhado → consumidores retirados
//!   automaticamente; independente segue ativo.
//! - Cycles and ambiguity rejected with a visible reason (never "last wins").
//! - Incompatible versions stay Waiting.
//! - M1.2 demo (exit gate): workspace → search + echo, withdrawing
//!   manual de search. Remover workspace limpa search automaticamente; echo
//!   workspace auto-withdraws search; reintroducing reactivates the consumer

use matrix_core::{Journal, Kernel, ResourceKind};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-m12-{}-{}-{}",
        std::process::id(),
        tag,
        nanos()
    ));
    let _ = std::fs::create_dir_all(dir.join("plugins"));
    let _ = std::fs::create_dir_all(dir.join("run"));
    dir
}

fn fresh_kernel(tag: &str) -> Kernel {
    let home = fresh_home(tag);
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    Kernel::new(&home, journal, false)
}

fn write_manifest(home: &PathBuf, id: &str, body: Value) -> PathBuf {
    let p = home.join("plugins").join(format!("{}.json", id));
    std::fs::write(&p, serde_json::to_string_pretty(&body).unwrap()).unwrap();
    p
}

fn basic_manifest(id: &str, caps: &[&str], reducer: &str) -> Value {
    json!({
        "id": id,
        "version": "1.0.0",
        "capabilities": caps,
        "subscriptions": [],
        "reducer": reducer,
        "init_state": {},
        "tier": "inproc",
        "trust": "trusted",
        "restart": "permanent",
    })
}

fn with_requires(mut v: Value, requires: Value) -> Value {
    v["requires"] = requires;
    v
}

fn home_of(k: &Kernel) -> PathBuf {
    k.plugins_dir.parent().unwrap().to_path_buf()
}

fn state_of(k: &Kernel, logical: &str) -> String {
    k.context_state_of(logical).unwrap_or_else(|| "<none>".to_string())
}

// ---- C04 ----

#[test]
fn c04_consumer_before_provider_waits_then_activates() {
    let k = fresh_kernel("c04-before");
    let home = home_of(&k);

    // Consumer first: Waiting with a visible reason, publishing no cap.
    let m_search = write_manifest(
        &home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
        ),
    );
    k.load_manifest(&m_search).unwrap();
    assert_eq!(state_of(&k, "search"), "Waiting");
    let reason = k.waiting_reason_of("search").expect("visible reason");
    assert!(reason.contains("workspace"), "reason cites the dependency: {}", reason);
    assert!(k.caps.resolve("search.query@1").is_none(), "Waiting publishes nothing");
    let (v, ok) = k.invoke("search.query@1", &json!({}));
    assert!(!ok, "no admission while Waiting: {}", v);
    assert_eq!(v.get("code").unwrap(), &json!("dependency-unavailable"));

    // Provedor chega: consumidor ativa automaticamente, em ordem.
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    k.load_manifest(&m_ws).unwrap();
    assert_eq!(state_of(&k, "workspace"), "Active");
    assert_eq!(state_of(&k, "search"), "Active");
    assert_eq!(k.caps.resolve("search.query@1").as_deref(), Some("search"));
    // I04: valid bindings for every required dependency.
    let bs = k.bindings_of("search");
    assert_eq!(bs.len(), 1);
    assert_eq!(bs[0].provider_logical, "workspace");
    assert_eq!(bs[0].interface, "workspace.fs@1");
    let (v, ok) = k.invoke("search.query@1", &json!({}));
    assert!(ok, "consumidor ativo responde: {}", v);
}

#[test]
fn c04_consumer_after_provider_active_immediately() {
    let k = fresh_kernel("c04-after");
    let home = home_of(&k);
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    let m_search = write_manifest(
        &home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!(["workspace.fs@1"]),
        ),
    );
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    assert_eq!(state_of(&k, "search"), "Active");
    assert!(k.invoke("search.query@1", &json!({})).1);
}

#[test]
fn c04_chain_activates_in_dependency_order() {
    let k = fresh_kernel("c04-chain");
    let home = home_of(&k);
    // Loads out of order on purpose: c, b, a.
    let m_c = write_manifest(
        &home,
        "c",
        with_requires(basic_manifest("c", &["c.cap@1"], "noop"), json!([{"interface": "b.cap@1", "provider": "b"}])),
    );
    let m_b = write_manifest(
        &home,
        "b",
        with_requires(basic_manifest("b", &["b.cap@1"], "noop"), json!([{"interface": "a.cap@1", "provider": "a"}])),
    );
    let m_a = write_manifest(&home, "a", basic_manifest("a", &["a.cap@1"], "noop"));
    k.load_manifest(&m_c).unwrap();
    assert_eq!(state_of(&k, "c"), "Waiting");
    k.load_manifest(&m_b).unwrap();
    assert_eq!(state_of(&k, "b"), "Waiting");
    assert_eq!(state_of(&k, "c"), "Waiting");
    k.load_manifest(&m_a).unwrap();
    assert_eq!(state_of(&k, "a"), "Active");
    assert_eq!(state_of(&k, "b"), "Active");
    assert_eq!(state_of(&k, "c"), "Active");
    assert!(k.invoke("c.cap@1", &json!({})).1);
    // Chained bindings point at the current instances.
    assert_eq!(k.bindings_of("b")[0].provider_logical, "a");
    assert_eq!(k.bindings_of("c")[0].provider_logical, "b");
}

// ---- C05 ----

#[test]
fn c05_shared_provider_removal_withdraws_consumers() {
    let k = fresh_kernel("c05-shared");
    let home = home_of(&k);
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    let m_s1 = write_manifest(
        &home,
        "search",
        with_requires(basic_manifest("search", &["search.query@1"], "noop"), json!([{"interface": "workspace.fs@1", "provider": "workspace"}])),
    );
    let m_s2 = write_manifest(
        &home,
        "index",
        with_requires(basic_manifest("index", &["index.query@1"], "noop"), json!([{"interface": "workspace.fs@1", "provider": "workspace"}])),
    );
    let m_echo = write_manifest(&home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    for m in [&m_ws, &m_s1, &m_s2, &m_echo] {
        k.load_manifest(m).unwrap();
    }
    assert!(k.invoke("search.query@1", &json!({})).1);
    assert!(k.invoke("index.query@1", &json!({})).1);

    // Removes the shared provider: both consumers leave, no manual touch.
    k.dispose_plugin("workspace");
    assert!(k.caps.resolve("workspace.fs@1").is_none());
    assert!(k.caps.resolve("search.query@1").is_none(), "search retirada");
    assert!(k.caps.resolve("index.query@1").is_none(), "index retirada");
    assert_eq!(state_of(&k, "search"), "Waiting");
    assert_eq!(state_of(&k, "index"), "Waiting");
    // Independente segue ativo.
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": true}));
    assert!(ok);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(true));

    // Reintroduces: new instances and bindings, no consumer intervention.
    k.load_manifest(&m_ws).unwrap();
    assert_eq!(state_of(&k, "search"), "Active");
    assert_eq!(state_of(&k, "index"), "Active");
    assert!(k.invoke("search.query@1", &json!({})).1);
    assert!(k.invoke("index.query@1", &json!({})).1);
}

// ---- rejections ----

#[test]
fn rejects_cycle_with_path() {
    let k = fresh_kernel("cycle");
    let home = home_of(&k);
    let m_a = write_manifest(
        &home,
        "a",
        with_requires(basic_manifest("a", &["a.cap@1"], "noop"), json!([{"interface": "b.cap@1", "provider": "b"}])),
    );
    let m_b = write_manifest(
        &home,
        "b",
        with_requires(basic_manifest("b", &["b.cap@1"], "noop"), json!([{"interface": "a.cap@1", "provider": "a"}])),
    );
    k.load_manifest(&m_a).unwrap();
    k.load_manifest(&m_b).unwrap();
    assert_eq!(state_of(&k, "a"), "Waiting");
    assert_eq!(state_of(&k, "b"), "Waiting");
    for id in ["a", "b"] {
        let r = k.waiting_reason_of(id).unwrap_or_default();
        assert!(r.contains("cycle"), "ciclo explicativo em {}: {}", id, r);
        assert!(r.contains("a") && r.contains("b"), "caminho em {}: {}", id, r);
    }
    assert!(k.caps.resolve("a.cap@1").is_none());
    assert!(k.caps.resolve("b.cap@1").is_none());
    let inv = k.inventory();
    let cyc = inv.get("cycle").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    assert!(cyc.len() >= 3, "inventory shows the cycle: {:?}", cyc);
}

#[test]
fn rejects_ambiguity_without_explicit_binding() {
    let k = fresh_kernel("ambiguous");
    let home = home_of(&k);
    let m_p1 = write_manifest(&home, "p1", basic_manifest("p1", &["x.y@1"], "noop"));
    let m_p2 = write_manifest(&home, "p2", basic_manifest("p2", &["x.y@1"], "noop"));
    let m_c = write_manifest(
        &home,
        "c",
        with_requires(basic_manifest("c", &["c.cap@1"], "noop"), json!(["x.y@1"])),
    );
    k.load_manifest(&m_p1).unwrap();
    k.load_manifest(&m_p2).unwrap();
    k.load_manifest(&m_c).unwrap();
    // Ambiguity is an error, not "last wins".
    assert_eq!(state_of(&k, "c"), "Waiting");
    let r = k.waiting_reason_of("c").unwrap_or_default();
    assert!(r.contains("ambiguous-provider"), "reason: {}", r);
    assert!(r.contains("p1") && r.contains("p2"), "lista provedores: {}", r);
    assert!(k.caps.resolve("c.cap@1").is_none());
}

#[test]
fn explicit_binding_resolves_ambiguity() {
    let k = fresh_kernel("explicit");
    let home = home_of(&k);
    let m_p1 = write_manifest(&home, "p1", basic_manifest("p1", &["x.y@1"], "noop"));
    let m_p2 = write_manifest(&home, "p2", basic_manifest("p2", &["x.y@1"], "noop"));
    let m_c = write_manifest(
        &home,
        "c",
        with_requires(
            basic_manifest("c", &["c.cap@1"], "noop"),
            json!([{"interface": "x.y@1", "provider": "p2"}]),
        ),
    );
    k.load_manifest(&m_p1).unwrap();
    k.load_manifest(&m_p2).unwrap();
    k.load_manifest(&m_c).unwrap();
    assert_eq!(state_of(&k, "c"), "Active");
    assert_eq!(k.bindings_of("c")[0].provider_logical, "p2");
    assert!(k.invoke("c.cap@1", &json!({})).1);
    // Withdrawing p2 drops c even with p1 live (binding was explicit).
    k.dispose_plugin("p2");
    assert_eq!(state_of(&k, "c"), "Waiting");
    assert!(k.caps.resolve("c.cap@1").is_none());
    let r = k.waiting_reason_of("c").unwrap_or_default();
    assert!(r.contains("p2"), "reason cites the bound provider: {}", r);
}

#[test]
fn version_mismatch_stays_waiting() {
    let k = fresh_kernel("version");
    let home = home_of(&k);
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@2"], "noop"));
    let m_search = write_manifest(
        &home,
        "search",
        with_requires(basic_manifest("search", &["search.query@1"], "noop"), json!(["workspace.fs@1"])),
    );
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    assert_eq!(state_of(&k, "search"), "Waiting");
    let r = k.waiting_reason_of("search").unwrap_or_default();
    assert!(r.contains("dependency-unavailable"), "reason: {}", r);
    assert!(k.caps.resolve("search.query@1").is_none());
}

// ---- M1.2 exit demo ----

#[test]
fn demo_workspace_search_echo_reactive() {
    let k = fresh_kernel("demo-m12");
    let home = home_of(&k);
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    let m_search = write_manifest(
        &home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
        ),
    );
    let m_echo = write_manifest(&home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    k.load_manifest(&m_echo).unwrap();
    assert_eq!(state_of(&k, "search"), "Active");

    // Active search acquires real subscription/timer/task.
    let h_sub = k.acquire("search", ResourceKind::Sub { topic: "workspace.changed".into() }).unwrap();
    let h_timer = k.acquire("search", ResourceKind::Timer { label: "debounce".into(), interval_ms: 5 }).unwrap();
    let h_task = k.acquire("search", ResourceKind::Task { label: "index".into() }).unwrap();
    let search_before = k.instance_ref_of("search").unwrap();
    let ws_before = k.instance_ref_of("workspace").unwrap();

    assert!(k.invoke("workspace.fs@1", &json!({})).1);
    assert!(k.invoke("search.query@1", &json!({})).1);
    assert!(k.invoke("echo.msg@1", &json!({"ping": 1})).1);

    // Remove o provedor: NENHUMA retirada manual de search.
    // New admissions blocked, search withdrawn, resources cleaned, echo continues.
    let out = k.dispose_plugin("workspace");
    assert_eq!(out.as_str(), "Disposed");
    assert!(k.caps.resolve("workspace.fs@1").is_none());
    let (v, ok) = k.invoke("workspace.fs@1", &json!({}));
    assert!(!ok, "cap retirada rejeita: {}", v);
    // Automatic consumer withdraw.
    assert!(k.caps.resolve("search.query@1").is_none(), "search auto-retirada");
    assert_eq!(state_of(&k, "search"), "Waiting");
    assert!(!k.resources.is_live(h_timer), "timer cancelado");
    assert!(!k.resources.is_live(h_task), "task cancelada");
    assert!(k.validate_handle(h_sub, "search").is_err());
    assert!(k.validate_handle(h_timer, "search").is_err());
    assert!(k.validate_handle(h_task, "search").is_err());
    let (v, ok) = k.invoke("search.query@1", &json!({}));
    assert!(!ok, "search fora do ar: {}", v);
    assert_eq!(v.get("code").unwrap(), &json!("dependency-unavailable"));
    // Independente segue respondendo.
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": true}));
    assert!(ok);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(true));

    // Inventory without withdrawn generations' live resources.
    let inv = k.inventory();
    let leaked = inv.get("resources").and_then(|v| v.as_array()).map(|arr| {
        arr.iter().filter(|r| {
            let owner = r.get("owner").and_then(|x| x.as_str()).unwrap_or("");
            let state = r.get("state").and_then(|x| x.as_str()).unwrap_or("");
            (owner == "workspace" || owner == "search") && state == "Active"
        }).count()
    }).unwrap_or(999);
    assert_eq!(leaked, 0, "no live resources from withdrawn generations");

    // Reintroduces the provider: search reactivates alone on a NEW instance/binding.
    k.load_manifest(&m_ws).unwrap();
    let search_after = k.instance_ref_of("search").unwrap();
    let ws_after = k.instance_ref_of("workspace").unwrap();
    assert!(search_after.generation > search_before.generation, "consumer new generation");
    assert_ne!(search_after.instance, search_before.instance, "consumer new instance");
    assert!(ws_after.generation > ws_before.generation, "provider new generation");
    assert_eq!(state_of(&k, "search"), "Active");
    let bs = k.bindings_of("search");
    assert_eq!(bs.len(), 1);
    assert_eq!(bs[0].provider_logical, "workspace");
    assert_eq!(bs[0].provider_instance, ws_after.instance, "binding points at the new instance");
    assert!(k.invoke("workspace.fs@1", &json!({})).1);
    assert!(k.invoke("search.query@1", &json!({})).1);
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": 2}));
    assert!(ok, "echo sobrevive ao ciclo: {}", v);
}

#[test]
fn provider_reload_rebinds_consumers_to_new_generation() {
    let k = fresh_kernel("reload-rebind");
    let home = home_of(&k);
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    let m_search = write_manifest(
        &home,
        "search",
        with_requires(basic_manifest("search", &["search.query@1"], "noop"), json!([{"interface": "workspace.fs@1", "provider": "workspace"}])),
    );
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let ws1 = k.instance_ref_of("workspace").unwrap();
    let s1 = k.instance_ref_of("search").unwrap();
    assert_eq!(k.bindings_of("search")[0].provider_instance, ws1.instance);

    // Provider reload (same file): consumer follows on a new generation.
    k.load_manifest(&m_ws).unwrap();
    let ws2 = k.instance_ref_of("workspace").unwrap();
    let s2 = k.instance_ref_of("search").unwrap();
    assert!(ws2.generation > ws1.generation);
    assert!(s2.generation > s1.generation, "consumer rebound on a new generation");
    assert_eq!(state_of(&k, "search"), "Active");
    assert_eq!(k.bindings_of("search")[0].provider_instance, ws2.instance);
    assert!(k.invoke("search.query@1", &json!({})).1);
}
