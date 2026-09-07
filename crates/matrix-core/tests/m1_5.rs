//! M5.1 conformance (closure): B1–B4 as permanent tests.
//!
//! - B1: concurrent dispose performs ONE effective transition and ONE `plugin.unloaded`.
//! - B2: ticket conclusion (any path) re-evaluates `CleanupPending`; diagnostics
//!   cites only existing tickets. Controlled `invoke` × `dispose`
//!   interleaving via a blocking forwarder (no sleeps).
//! - B3: `acquire` × `dispose` never publishes/resolves a discarded instance.
//! - B4: close contract with a dead worker + reaping by the owner.

use matrix_core::{
    CallForwarder, CallPolicy, ForwardOutcome, ForwardRequest, InstanceId, Journal, Kernel,
    ResourceKind, TicketId,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-m15-{}-{}-{}",
        std::process::id(),
        tag,
        nanos()
    ));
    let _ = std::fs::create_dir_all(dir.join("plugins"));
    let _ = std::fs::create_dir_all(dir.join("run"));
    dir
}

fn write_manifest(home: &PathBuf, body: &Value) -> PathBuf {
    let id = body["id"].as_str().unwrap();
    let p = home.join("plugins").join(format!("{}.json", id));
    std::fs::write(&p, serde_json::to_string_pretty(body).unwrap()).unwrap();
    p
}

fn echo_body() -> Value {
    json!({
        "id": "echo", "version": "1.0.0",
        "capabilities": ["echo.msg@1"], "subscriptions": [],
        "reducer": "echo", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
    })
}

fn ext_body() -> Value {
    json!({
        "id": "ext", "version": "1.0.0",
        "capabilities": ["ext.call@1"], "subscriptions": [],
        "reducer": "external", "init_state": {},
        "tier": "process", "trust": "trusted", "restart": "permanent",
        "execution": {"kind": "process", "entrypoint": "/bin/true",
                      "args": [], "timeout_ms": 5000},
    })
}

fn fresh_kernel(tag: &str, body: &Value) -> Kernel {
    let home = fresh_home(tag);
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let k = Kernel::new(&home, journal, false);
    let h = k.plugins_dir.parent().unwrap().to_path_buf();
    let m = write_manifest(&h, body);
    k.load_manifest(&m).unwrap();
    k
}

fn journal_count(k: &Kernel, kind: &str) -> usize {
    k.journal.flush_sync();
    k.journal.tail(100_000).iter().filter(|e| e.kind == kind).count()
}

// ---- B1 ----

#[test]
fn t1_concurrent_dispose_single_transition() {
    let k = Arc::new(fresh_kernel("b1", &echo_body()));
    let inst = InstanceId(k.instance_ref_of("echo").unwrap().instance);
    let bar = Arc::new(Barrier::new(8));
    let mut ths = vec![];
    for _ in 0..8 {
        let (kk, bb) = (k.clone(), bar.clone());
        ths.push(std::thread::spawn(move || {
            bb.wait();
            kk.dispose_instance(inst).as_str().to_string()
        }));
    }
    let outs: Vec<String> = ths.into_iter().map(|t| t.join().unwrap()).collect();
    // Exactly ONE winner; the rest observe idempotence or an ongoing withdraw.
    assert_eq!(
        outs.iter().filter(|o| *o == "Disposed").count(),
        1,
        "duplicated transition: {:?}",
        outs
    );
    assert!(
        outs.iter().all(|o| o == "Disposed" || o == "AlreadyDisposed" || o == "CleanupPending"),
        "desfecho inesperado: {:?}",
        outs
    );
    assert_eq!(journal_count(&k, "plugin.unloaded"), 1, "journal duplicado");
    // No leak from the discarded generation; the live substitute owns its Cap.
    let inv = k.inventory();
    for r in inv["resources"].as_array().unwrap() {
        if r["instance"] == inst.0 {
            assert_eq!(r["state"], "Released", "vazou recurso da gen1: {}", r);
        }
    }
}

// ---- B2 ----

/// Test forwarder: signals entry and blocks until released — a deterministic
/// `invoke` × `dispose` interleaving, no sleeps.
struct BlockingFwd {
    entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl CallForwarder for BlockingFwd {
    fn forward(&self, _req: &ForwardRequest) -> ForwardOutcome {
        if let Some(tx) = self.entered.lock().unwrap().take() {
            let _ = tx.send(());
        }
        let _ = self.release.lock().unwrap().recv();
        ForwardOutcome::Ok(json!({"late": true}))
    }
}

#[test]
fn t2_ephemeral_invoke_settles_after_withdraw() {
    let k = Arc::new(fresh_kernel("b2", &ext_body()));
    let (tx_in, rx_in) = std::sync::mpsc::channel();
    let (tx_rel, rx_rel) = std::sync::mpsc::channel::<()>();
    k.set_forwarder(Arc::new(BlockingFwd {
        entered: Mutex::new(Some(tx_in)),
        release: Mutex::new(rx_rel),
    }));
    let kc = k.clone();
    let inv = std::thread::spawn(move || kc.invoke("ext.call@1", &json!({})));
    // Forwarder entered (ephemeral ticket admitted); dispose linearizes now.
    rx_in.recv().expect("forwarder never entered");
    let inst = InstanceId(k.instance_ref_of("ext").unwrap().instance);
    let gen = k.instance_ref_of("ext").unwrap().generation;
    assert_eq!(k.dispose_instance(inst).as_str(), "CleanupPending");
    tx_rel.send(()).unwrap();
    let (v, ok) = inv.join().unwrap();
    // Late answers never become false successes...
    assert!(!ok, "false success after withdraw: {}", v);
    assert_eq!(v["code"], "cancelled", "code: {}", v);
    // ...and ticket conclusion settles the withdraw (B2), without hanging.
    let cur = k.instance_ref_of("ext").expect("substituta");
    assert!(cur.generation > gen, "withdraw never settled: gen {}", cur.generation);
    assert_eq!(journal_count(&k, "call.rejected"), 1);
}

#[test]
fn t3_cleanup_cause_cites_only_live_tickets() {
    let k = fresh_kernel("b2cause", &echo_body());
    let inst = InstanceId(k.instance_ref_of("echo").unwrap().instance);
    let t1 = k.call_open("echo.msg@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
    let t2 = k.call_open("echo.msg@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
    assert_eq!(k.dispose_instance(inst).as_str(), "CleanupPending");
    assert!(k.call_close(t1.ticket));
    // Ainda pendente, mas a causa cita SÓ o ticket existente.
    assert_eq!(k.context_state_of("echo"), Some("CleanupPending".to_string()));
    let inv = k.inventory();
    let entry = inv["instances"].as_array().unwrap().iter()
        .find(|e| e["state"] == "CleanupPending").unwrap();
    let cause = entry["cause"].as_str().unwrap();
    assert!(cause.contains(&t2.ticket.0.to_string()), "causa perdeu t2: {}", cause);
    assert!(!cause.contains(&t1.ticket.0.to_string()), "causa cita ticket morto: {}", cause);
    assert!(k.call_close(t2.ticket));
    assert!(k.instance_ref_of("echo").unwrap().generation > 1);
}

// ---- B4 (contract: a dead worker never waives the ticket owner's close) ----

#[test]
fn t4_dead_worker_opener_still_closes() {
    let k = fresh_kernel("b4", &echo_body());
    let inst = InstanceId(k.instance_ref_of("echo").unwrap().instance);
    // Owner opens; "worker" observes the flag and dies without closing.
    let open = k.call_open("echo.msg@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
    let flag = open.cancel.clone();
    let flag_w = flag.clone();
    let w = std::thread::spawn(move || {
        while !flag_w.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::yield_now();
        }
        // Dies without closing: simulates executor crash with an admitted ticket.
    });
    assert_eq!(k.dispose_instance(inst).as_str(), "CleanupPending");
    w.join().unwrap();
    assert!(flag.load(std::sync::atomic::Ordering::SeqCst), "cancel never signaled");
    // Owner closes even with a dead executor: settles.
    assert!(k.call_close(open.ticket));
    assert!(k.instance_ref_of("echo").unwrap().generation > 1);
}

// ---- B4 (reap: the owner resolves a dead owner's pending work) ----

#[test]
fn t6_owner_reaps_revoked_ticket_of_dead_owner() {
    let k = fresh_kernel("b4reap", &echo_body());
    let inst = InstanceId(k.instance_ref_of("echo").unwrap().instance);
    // `Admitted` still holds authority: reap refused (cancel first).
    let live = k.call_open("echo.msg@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
    assert!(!k.call_reap(live.ticket, "x"));
    assert!(!k.call_reap(TicketId(999_999), "x"));
    // Owner abandons the ticket (dies without closing); dispose parks.
    let open = k.call_open("echo.msg@1", &json!({}), Some(CallPolicy::drain(0)), &[]).unwrap();
    assert_eq!(k.dispose_instance(inst).as_str(), "CleanupPending");
    assert!(matches!(k.calls.get(open.ticket).map(|t| t.state), Some(matrix_core::TicketState::Expired) | Some(matrix_core::TicketState::Cancelled)));
    // The instance owner reaps already-revoked authority: resolves without
    // assuming execution ended (late commit would still be rejected).
    assert!(k.call_reap(open.ticket, "owner-cleanup"));
    assert_eq!(journal_count(&k, "call.reaped"), 1);
    assert!(!k.call_reap(open.ticket, "again"));
    // Only the live one remains: the cause cites just it; closing settles.
    let inv = k.inventory();
    let entry = inv["instances"].as_array().unwrap().iter()
        .find(|e| e["state"] == "CleanupPending").unwrap();
    assert!(entry["cause"].as_str().unwrap().contains(&live.ticket.0.to_string()));
    assert!(k.call_close(live.ticket));
    assert!(k.instance_ref_of("echo").unwrap().generation > 1);
}

// ---- B3 (stress: invariant must hold on every interleaving) ----

#[test]
fn t5_acquire_withdraw_never_publishes_dead_instance() {
    for i in 0..400 {
        let k = Arc::new(fresh_kernel(&format!("b3-{}", i), &echo_body()));
        let home = k.plugins_dir.parent().unwrap().to_path_buf();
        let body = echo_body();
        let bar = Arc::new(Barrier::new(2));
        let (k2, b2) = (k.clone(), bar.clone());
        let acq = std::thread::spawn(move || {
            b2.wait();
            for _ in 0..30 {
                let _ = k2.acquire("echo", ResourceKind::Cap { name: "echo.msg@1".into() });
            }
        });
        bar.wait();
        for _ in 0..30 {
            if let Some(r) = k.instance_ref_of("echo") {
                let _ = k.dispose_instance(InstanceId(r.instance));
            }
            let m = write_manifest(&home, &body);
            let _ = k.load_manifest(&m);
        }
        let _ = acq.join().unwrap();
        // Invariant 1: the registry never points at a non-Active instance.
        if let Some(id) = k.caps.resolve_instance("echo.msg@1") {
            let st = k.contexts.get_by_instance(id).map(|r| r.state.as_str().to_string());
            assert_eq!(st.as_deref(), Some("Active"), "cap envenenada -> {:?} (iter {})", st, i);
        }
        // Invariant 2: no Active handle owned by a Disposed instance.
        let inv = k.inventory();
        for r in inv["resources"].as_array().unwrap() {
            if r["state"] == "Active" {
                let owner = r["instance"].as_u64().unwrap();
                let st = k.contexts.get_by_instance(InstanceId(owner))
                    .map(|c| c.state.as_str().to_string()).unwrap_or_default();
                assert_ne!(st, "Disposed", "Active handle of Disposed instance: {} (iter {})", r, i);
            }
        }
    }
}
