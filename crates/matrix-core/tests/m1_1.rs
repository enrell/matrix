//! M1.1 conformance: C01–C03 + withdraw demo (ROADMAP/VALIDATION).
//!
//! - C01: discard a generation and reuse the logical id → old handles rejected.
//! - C02: acquire/revoke cap, sub, timer, task → inventory returns to baseline.
//! - C03: dispose twice + fail mid-acquisition → no double
//!   release, orphans, or corruption.
//! - Demo M1.1: remover provedor, limpar consumidores, manter independente.
//!   Automatic reactive cascade (Waiting/bindings) is M1.2; here consumer
//!   withdraw is explicit and documented as such.

use matrix_core::{Journal, Kernel, ResourceKind};
use serde_json::json;
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
        "matrix-m11-{}-{}-{}",
        std::process::id(),
        tag,
        nanos()
    ));
    let _ = std::fs::create_dir_all(dir.join("plugins"));
    let _ = std::fs::create_dir_all(dir.join("run"));
    dir
}

fn write_manifest(home: &PathBuf, id: &str, caps: &[&str], subs: &[&str], reducer: &str) -> PathBuf {
    let p = home.join("plugins").join(format!("{}.json", id));
    let v = json!({
        "id": id,
        "version": "1.0.0",
        "capabilities": caps,
        "subscriptions": subs,
        "reducer": reducer,
        "init_state": {},
        "tier": "inproc",
        "trust": "trusted",
        "restart": "permanent",
    });
    std::fs::write(&p, serde_json::to_string_pretty(&v).unwrap()).unwrap();
    p
}

fn fresh_kernel(tag: &str) -> (Kernel, PathBuf) {
    let home = fresh_home(tag);
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let k = Kernel::new(&home, journal, false);
    (k, home)
}

// ---- C01 ----

#[test]
fn c01_stale_handles_rejected_after_reuse() {
    let (k, _home) = fresh_kernel("c01");
    // The plugins dir must point at the isolated home.
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let m = write_manifest(&home, "echo", &["echo.msg@1"], &[], "echo");
    k.load_manifest(&m).unwrap();
    let ref1 = k.instance_ref_of("echo").expect("ref gen1");
    assert_eq!(ref1.generation, 1);

    let h = k
        .acquire(
            "echo",
            ResourceKind::Timer {
                label: "c01-t".into(),
                interval_ms: 5,
            },
        )
        .expect("acquire timer gen1");
    assert!(k.validate_handle(h, "echo").is_ok());

    let out = k.dispose_plugin("echo");
    assert_eq!(out.as_str(), "Disposed");

    // Reuses the logical id: new generation, new instance.
    k.load_manifest(&m).unwrap();
    let ref2 = k.instance_ref_of("echo").expect("ref gen2");
    assert!(ref2.generation > ref1.generation);
    assert_ne!(ref2.instance, ref1.instance);

    // Handle antigo nunca revalida (C01).
    assert!(k.validate_handle(h, "echo").is_err());
    assert!(k.release(h).is_err());

    // Disposing the old instance never touches the new one (I06).
    let old = matrix_core::InstanceId(ref1.instance);
    let out_old = k.dispose_instance(old);
    assert_eq!(out_old.as_str(), "AlreadyDisposed");
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": true}));
    assert!(ok, "new generation stays active: {}", v);
}

// ---- C02 ----

#[test]
fn c02_inventory_returns_to_baseline_after_cleanup() {
    let (k, _h) = fresh_kernel("c02");
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let mw = write_manifest(&home, "worker", &[], &[], "noop");
    k.load_manifest(&mw).unwrap();
    let baseline = k.inventory();
    let base_active = baseline
        .get("active_resources")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let h_cap = k
        .acquire(
            "worker",
            ResourceKind::Cap {
                name: "test.cap@1".into(),
            },
        )
        .unwrap();
    let h_sub = k
        .acquire(
            "worker",
            ResourceKind::Sub {
                topic: "test.topic".into(),
            },
        )
        .unwrap();
    let h_timer = k
        .acquire(
            "worker",
            ResourceKind::Timer {
                label: "t".into(),
                interval_ms: 5,
            },
        )
        .unwrap();
    let h_task = k
        .acquire(
            "worker",
            ResourceKind::Task { label: "j".into() },
        )
        .unwrap();

    // Real publication: cap resolves, sub receives, threads live.
    assert!(k.caps.resolve("test.cap@1").is_some());
    assert!(k.bus.subscribers("test.topic").contains(&"worker".to_string()));
    assert!(k.resources.is_live(h_timer));
    assert!(k.resources.is_live(h_task));
    // Timer real dispara.
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(k.resources.timer_fires(h_timer).unwrap_or(0) > 0);
    assert!(k.resources.task_beats(h_task).unwrap_or(0) > 0);

    let mid = k.inventory();
    let mid_active = mid.get("active_resources").and_then(|v| v.as_u64()).unwrap();
    assert_eq!(mid_active, base_active + 4);

    // Cleanup reverso e completo.
    for h in [h_task, h_timer, h_sub, h_cap] {
        k.release(h).expect("release ok");
    }
    // Threads realmente terminaram (join no release).
    assert!(!k.resources.is_live(h_timer));
    assert!(!k.resources.is_live(h_task));
    assert!(k.caps.resolve("test.cap@1").is_none());
    assert!(!k.bus.subscribers("test.topic").contains(&"worker".to_string()));

    let end = k.inventory();
    let end_active = end.get("active_resources").and_then(|v| v.as_u64()).unwrap();
    assert_eq!(end_active, base_active, "inventory returns to baseline");

    // Final dispose leaves no pending work.
    let out = k.dispose_plugin("worker");
    assert_eq!(out.as_str(), "Disposed");
    assert_eq!(k.resources.active_for(
        k.contexts.current("worker").map(|c| c.context).unwrap_or(matrix_core::ContextId(0))
    ), 0);
}

// ---- C03 ----

#[test]
fn c03_double_dispose_and_partial_failure_no_orphans() {
    let (k, _h) = fresh_kernel("c03");
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let m = write_manifest(&home, "w", &[], &[], "noop");
    k.load_manifest(&m).unwrap();

    let rel_before = k.resources.total_releases();

    // Idempotent dispose: no double release.
    let a = k.dispose_plugin("w");
    assert_eq!(a.as_str(), "Disposed");
    let rel_after_first = k.resources.total_releases();
    assert!(rel_after_first >= rel_before);
    let b = k.dispose_plugin("w");
    assert_eq!(b.as_str(), "AlreadyDisposed");
    assert_eq!(k.resources.total_releases(), rel_after_first);

    // Reactivates for the partial-failure test.
    k.load_manifest(&m).unwrap();
    let active_before = k.resources.active_count();

    // Mid-way failure: the first item must be undone (I05).
    let res = k.acquire_batch(
        "w",
        vec![
            ResourceKind::Timer {
                label: "keep".into(),
                interval_ms: 5,
            },
            ResourceKind::FailAcquire {
                label: "boom".into(),
            },
            ResourceKind::Task { label: "never".into() },
        ],
    );
    assert!(res.is_err());
    assert_eq!(
        k.resources.active_count(),
        active_before,
        "no orphans after partial failure"
    );

    // Double-releasing one handle never duplicates cleanup.
    let h = k
        .acquire(
            "w",
            ResourceKind::Timer {
                label: "once".into(),
                interval_ms: 5,
            },
        )
        .unwrap();
    let rel0 = k.resources.total_releases();
    assert!(k.release(h).is_ok());
    assert_eq!(k.resources.total_releases(), rel0 + 1);
    assert!(k.release(h).is_err());
    assert_eq!(k.resources.total_releases(), rel0 + 1);
    assert!(k.resources.double_release_attempts() >= 1);
}

#[test]
fn c03_cleanup_pending_never_false_disposed() {
    let (k, _h) = fresh_kernel("c03pending");
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let m = write_manifest(&home, "sticky", &[], &[], "noop");
    k.load_manifest(&m).unwrap();
    let _h = k
        .acquire(
            "sticky",
            ResourceKind::FailRelease {
                label: "stuck".into(),
            },
        )
        .unwrap();
    let out = k.dispose_plugin("sticky");
    match out {
        matrix_core::DisposeOutcome::CleanupPending { .. } => {}
        other => panic!("esperava CleanupPending, obteve {:?}", other),
    }
    assert_eq!(k.context_state_of("sticky").as_deref(), Some("CleanupPending"));
    // A second attempt never fakes Disposed.
    let out2 = k.dispose_plugin("sticky");
    assert_eq!(out2.as_str(), "CleanupPending");
}

// ---- M1.1 demo: provider/consumer/independent ----

#[test]
fn demo_provider_removal_keeps_independent() {
    let (k, _h) = fresh_kernel("demo");
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let m_ws = write_manifest(&home, "workspace", &["workspace.fs@1"], &[], "noop");
    let m_search = write_manifest(&home, "search", &["search.query@1"], &[], "noop");
    let m_echo = write_manifest(&home, "echo", &["echo.msg@1"], &[], "echo");
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    k.load_manifest(&m_echo).unwrap();

    // Consumer acquires real subscription/timer/task.
    let h_sub = k
        .acquire(
            "search",
            ResourceKind::Sub {
                topic: "workspace.changed".into(),
            },
        )
        .unwrap();
    let h_timer = k
        .acquire(
            "search",
            ResourceKind::Timer {
                label: "debounce".into(),
                interval_ms: 5,
            },
        )
        .unwrap();
    let h_task = k
        .acquire(
            "search",
            ResourceKind::Task { label: "index".into() },
        )
        .unwrap();
    let search_ref_before = k.instance_ref_of("search").unwrap();

    // Sanity: everyone answers before withdraw.
    assert!(k.invoke("workspace.fs@1", &json!({})).1);
    assert!(k.invoke("search.query@1", &json!({})).1);
    assert!(k.invoke("echo.msg@1", &json!({"ping": 1})).1);

    // Withdraws the provider mid-operation: blocks new admissions,
    // limpa seus recursos e rejeita uso posterior.
    let out_ws = k.dispose_plugin("workspace");
    assert_eq!(out_ws.as_str(), "Disposed");
    assert!(k.caps.resolve("workspace.fs@1").is_none());
    let (v, ok) = k.invoke("workspace.fs@1", &json!({}));
    assert!(!ok, "cap retirada deve rejeitar: {}", v);

    // Consumer withdraw (M1.1: explicit; M1.2 makes it reactive).
    // Cleans subscription/timer/task and rejects late commit as stale.
    let out_search = k.dispose_plugin("search");
    assert_eq!(out_search.as_str(), "Disposed");
    assert!(!k.resources.is_live(h_timer));
    assert!(!k.resources.is_live(h_task));
    assert!(k.caps.resolve("search.query@1").is_none());
    assert!(k.validate_handle(h_sub, "search").is_err());
    assert!(k.validate_handle(h_timer, "search").is_err());
    assert!(k.validate_handle(h_task, "search").is_err());

    // Independente segue funcionando.
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": true}));
    assert!(ok);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(true));

    // Inventory without withdrawn generations' resources.
    let inv = k.inventory();
    let leaked = inv
        .get("resources")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter(|r| {
                    let owner = r.get("owner").and_then(|x| x.as_str()).unwrap_or("");
                    let state = r.get("state").and_then(|x| x.as_str()).unwrap_or("");
                    (owner == "workspace" || owner == "search") && state == "Active"
                })
                .count()
        })
        .unwrap_or(999);
    assert_eq!(leaked, 0, "no live resources from withdrawn generations");

    // Reintroduces provider + consumer: new instances/bindings.
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let search_ref_after = k.instance_ref_of("search").unwrap();
    assert!(search_ref_after.generation > search_ref_before.generation);
    assert!(k.invoke("workspace.fs@1", &json!({})).1);
    assert!(k.invoke("search.query@1", &json!({})).1);
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": 2}));
    assert!(ok, "echo sobrevive ao ciclo: {}", v);
}
