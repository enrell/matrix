//! M1.3 conformance: C06, C08–C09 + exit demo.
//!
//! - Tickets bound to instance/generation/context/epoch/authorization.
//! - Linearized withdraw: `Quiescing` blocks admission, including in the
//!   admission × dispose race (post-registration revalidation).
//! - Drenagem (`Drain` com prazo) ou cancelamento (`Cancel`); commit tardio
//!   rejected AND journaled — dropping the response is not enough.
//! - Live work pins `CleanupPending`; resources never leave early
//!   (pins + retention until settled); verifiable finalization on close.
//! - Exit demo: search call blocked before the effect, workspace
//!   withdraw, late release rejected, echo alive, new generation ok,
//!   old ticket invalid.

use matrix_core::{CallPolicy, Journal, Kernel, ResourceKind, TicketState, WithdrawPolicy};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-m13-{}-{}-{}",
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

fn std_manifests(home: &PathBuf) -> (PathBuf, PathBuf, PathBuf) {
    let m_ws = write_manifest(home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    let m_search = write_manifest(
        home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
        ),
    );
    let m_echo = write_manifest(home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    (m_ws, m_search, m_echo)
}

fn code_of(v: &Value) -> String {
    v.get("code").and_then(|c| c.as_str()).unwrap_or("").to_string()
}

/// Test lock shareable across threads (mpsc::Receiver is not Sync).
#[derive(Clone)]
struct Gate {
    inner: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl Gate {
    fn new() -> Self {
        Self { inner: Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())) }
    }

    fn release(&self) {
        *self.inner.0.lock().unwrap() = true;
        self.inner.1.notify_all();
    }

    fn is_released(&self) -> bool {
        *self.inner.0.lock().unwrap()
    }
}

// ---- M1.3 exit demo ----

#[test]
fn demo_blocked_call_late_commit_rejected() {
    let k = fresh_kernel("demo-m13");
    let home = home_of(&k);
    let (m_ws, m_search, m_echo) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    k.load_manifest(&m_echo).unwrap();
    assert_eq!(state_of(&k, "search"), "Active");

    // search uses a managed resource pinned to the call.
    let h_task = k.acquire("search", ResourceKind::Task { label: "index".into() }).unwrap();
    let search_gen1 = k.instance_ref_of("search").unwrap();

    // Worker: opens, signals it stopped before the effect, and only leaves on gate —
    // even if cancellation arrives first (so the late commit exists).
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let gate = Gate::new();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(|| {
            let open = k
                .call_open("search.query@1", &json!({"q": "x"}), Some(CallPolicy::cancel()), &[h_task])
                .expect("admission with search Active");
            let saw_cancel = open.cancel.clone();
            arrived_tx.send(open.ticket).unwrap();
            // Cooperative park: leaves on gate; records whether cancel arrived first.
            let mut cancel_before_release = false;
            let park0 = Instant::now();
            loop {
                if saw_cancel.load(Ordering::SeqCst) {
                    cancel_before_release = true;
                }
                if gate.is_released() || park0.elapsed() > Duration::from_secs(15) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            // Late commit (after withdraw): must be rejected.
            let commit = k.commit_effect(open.ticket, "index.write", &json!({"q": "x"}));
            let closed = k.call_close(open.ticket);
            done_tx.send((commit.map_err(|v| code_of(&v)), cancel_before_release, closed)).unwrap();
        });

        let ticket = arrived_rx.recv_timeout(Duration::from_secs(5)).expect("worker parou no gate");
        // Call visible as Admitted in inventory (C19).
        let inv = k.inventory();
        let shown = inv.get("calls").and_then(|v| v.as_array()).map(|a| {
            a.iter().any(|c| {
                c.get("ticket").and_then(|t| t.as_u64()) == Some(ticket.0)
                    && c.get("state").and_then(|s| s.as_str()) == Some("Admitted")
            })
        }).unwrap_or(false);
        assert!(shown, "in-flight ticket visible");

        // Withdraws the provider DURING the call: no manual touch on search.
        k.dispose_plugin("workspace");
        // search retirada automaticamente; trabalho ativo segura CleanupPending.
        assert!(k.caps.resolve("search.query@1").is_none());
        assert_eq!(state_of(&k, "search"), "Waiting"); // definition persists → new generation waits
        // The old generation stays CleanupPending (live work, I07).
        let old_rec = k.contexts.get_by_instance(matrix_core::InstanceId(search_gen1.instance)).unwrap();
        assert_eq!(old_rec.state.as_str(), "CleanupPending", "trabalho ativo segura cleanup");
        // New admissions blocked.
        let denied = k.call_open("search.query@1", &json!({}), None, &[]);
        assert!(denied.is_err(), "admission blocked without provider");
        assert_eq!(code_of(&denied.unwrap_err()), "dependency-unavailable");
        // Pinned resources never leave early.
        assert!(k.resources.is_live(h_task), "task retida com chamada ativa");
        // Independente segue.
        assert!(k.invoke("echo.msg@1", &json!({"ping": 1})).1);

        // Releases the call: the late effect is rejected (ignoring is not enough).
        gate.release();
        let (commit, cancel_seen, closed) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(cancel_seen, "worker observou o cancelamento");
        assert!(closed, "close executado");
        assert_eq!(commit.unwrap_err(), "cancelled", "commit tardio rejeitado");
        assert!(k.committed_effects(Some("search")).is_empty(), "late effect never applied");
        // Verifiable finalization: old generation reaches Disposed, no orphans.
        assert_eq!(state_of(&k, "search"), "Waiting");
        let old_rec = k.contexts.get_by_instance(matrix_core::InstanceId(search_gen1.instance)).unwrap();
        assert_eq!(old_rec.state.as_str(), "Disposed");
        assert!(!k.resources.is_live(h_task), "task liberada ao aquietar");
        let leaked = k.inventory().get("resources").and_then(|v| v.as_array()).map(|a| {
            a.iter().filter(|r| {
                r.get("owner").and_then(|x| x.as_str()) == Some("search")
                    && r.get("instance").and_then(|x| x.as_u64()) == Some(search_gen1.instance)
                    && r.get("state").and_then(|x| x.as_str()) == Some("Active")
            }).count()
        }).unwrap_or(999);
        assert_eq!(leaked, 0, "no live resources from the withdrawn generation");
        // Journaled rejection (audit, I12).
        let entries = matrix_core::Journal::read_all(&k.journal_path);
        assert!(entries.iter().any(|e| e.kind == "call.rejected"
            && e.args.get("ticket").and_then(|t| t.as_u64()) == Some(ticket.0)),
            "call.rejected jornalizado");

        // Reintroduces: new generation works; old ticket stays invalid.
        k.load_manifest(&m_ws).unwrap();
        assert_eq!(state_of(&k, "search"), "Active");
        let search_gen2 = k.instance_ref_of("search").unwrap();
        assert_ne!(search_gen2.instance, search_gen1.instance);
        assert!(search_gen2.generation > search_gen1.generation);
        let late = k.commit_effect(ticket, "index.write", &json!({}));
        assert!(late.is_err(), "old ticket invalid");
        assert_eq!(code_of(&late.unwrap_err()), "unknown-ticket");
        let open2 = k.call_open("search.query@1", &json!({}), None, &[]).expect("new generation admits");
        let seq = k.commit_effect(open2.ticket, "index.write", &json!({"q": "y"})).expect("commit on the new generation");
        assert!(k.call_close(open2.ticket));
        assert_eq!(k.committed_effects(Some("search")).len(), 1);
        assert_eq!(k.committed_effects(Some("search"))[0].seq, seq);
        assert!(k.invoke("echo.msg@1", &json!({"ping": 2})).1);
        let _ = (m_echo, m_search);
    });
}

// ---- C06 dirigido ----

#[test]
fn c06_commit_after_withdraw_rejected() {
    let k = fresh_kernel("c06");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let open = k.call_open("search.query@1", &json!({}), None, &[]).unwrap();
    k.dispose_plugin("workspace");
    let err = k.commit_effect(open.ticket, "w", &json!({})).unwrap_err();
    assert_eq!(code_of(&err), "cancelled");
    assert!(k.committed_effects(Some("search")).is_empty());
    assert!(k.call_close(open.ticket));
    // Closed ticket: invalid forever.
    assert_eq!(code_of(&k.commit_effect(open.ticket, "w", &json!({})).unwrap_err()), "unknown-ticket");
    let _ = m_search;
    let _ = m_ws;
}

// ---- admission × dispose race: invariant under contention ----

#[test]
fn admission_dispose_race_no_unauthorized_commit() {
    let k = fresh_kernel("race");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();

    let committed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let rejected = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let opened_ok = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    std::thread::scope(|s| {
        for _ in 0..4 {
            let (committed_c, rejected_c, opened_c, st) = (&committed, &rejected, &opened_ok, &stop);
            s.spawn(|| {
                let mut n = 0;
                while !st.load(Ordering::Relaxed) && n < 60 {
                    n += 1;
                    let opened = match k.call_open("search.query@1", &json!({"n": n}), None, &[]) {
                        Ok(o) => { opened_c.fetch_add(1, Ordering::Relaxed); o }
                        Err(_) => continue,
                    };
                    match k.commit_effect(opened.ticket, "race.write", &json!({"n": n})) {
                        Ok(_) => { committed_c.fetch_add(1, Ordering::Relaxed); }
                        Err(_) => { rejected_c.fetch_add(1, Ordering::Relaxed); }
                    }
                    k.call_close(opened.ticket);
                }
            });
        }
        // Withdraws and reintroduces the provider mid-fire.
        for _ in 0..6 {
            std::thread::sleep(Duration::from_millis(5));
            k.dispose_plugin("workspace");
            let _ = k.load_manifest(&m_ws);
        }
        stop.store(true, Ordering::Relaxed);
    });
    // Sem deadlock/panic; todo commit tem destino consistente.
    let total = committed.load(Ordering::Relaxed) + rejected.load(Ordering::Relaxed);
    assert!(opened_ok.load(Ordering::Relaxed) >= total - 4, "todo commit veio de ticket admitido");
    assert!(total > 0, "real contention happened");
    assert_eq!(k.calls.pending_count(k.instance_of("search").map(|i| i).unwrap_or(matrix_core::InstanceId(0))), 0);
    // Estado final consistente: provedor presente → consumidor ativo.
    assert_eq!(state_of(&k, "search"), "Active");
    assert!(k.invoke("search.query@1", &json!({})).1);
    // Effects only from generations that existed; no seq duplicates.
    let effs = k.committed_effects(Some("search"));
    let mut seqs: Vec<u64> = effs.iter().map(|e| e.seq).collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len(), effs.len(), "sem efeito duplicado");
}

// ---- drenagem dentro do prazo ----

#[test]
fn drain_policy_commits_during_quiescing() {
    let k = fresh_kernel("drain-ok");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();

    let (arrived_tx, arrived_rx) = mpsc::channel();
    let gate = Gate::new();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(|| {
            let open = k.call_open("search.query@1", &json!({}), Some(CallPolicy::drain(2000)), &[]).unwrap();
            arrived_tx.send(open.ticket).unwrap();
            let park0 = Instant::now();
            while !gate.is_released() && park0.elapsed() < Duration::from_secs(15) {
                std::thread::sleep(Duration::from_millis(1));
            }
            // Dentro do prazo, com Quiescing + Drain: commit aceito.
            let c = k.commit_effect(open.ticket, "index.write", &json!({}));
            let closed = k.call_close(open.ticket);
            done_tx.send((c.map(|s| s).map_err(|v| code_of(&v)), closed)).unwrap();
        });
        let _ticket = arrived_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let t0 = Instant::now();
        // Dispose blocks until drained (no arbitrary sleeps for ordering).
        let out = std::thread::scope(|s2| {
            let h = s2.spawn(|| k.dispose_plugin("workspace"));
            std::thread::sleep(Duration::from_millis(50));
            gate.release();
            h.join().unwrap()
        });
        let (commit, closed) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(commit.is_ok(), "commit na drenagem aceito: {:?}", commit);
        assert!(closed);
        assert!(t0.elapsed() < Duration::from_secs(2), "dentro do prazo");
        // Effects finished before final revocation stay recorded.
        assert_eq!(k.committed_effects(Some("search")).len(), 1);
        let _ = out;
    });
}

// ---- C09: deadline + visible pending + later settlement ----

#[test]
fn c09_drain_expiry_keeps_pending_then_settles() {
    let k = fresh_kernel("c09");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let search_gen1 = k.instance_ref_of("search").unwrap();

    let (arrived_tx, arrived_rx) = mpsc::channel();
    let gate = Gate::new();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(|| {
            let open = k.call_open("search.query@1", &json!({}), Some(CallPolicy::drain(120)), &[]).unwrap();
            arrived_tx.send(open.ticket).unwrap();
            // Stubborn: ignores cancel and gate until the test releases much later.
            while !gate.is_released() {
                std::thread::sleep(Duration::from_millis(1));
            }
            let c = k.commit_effect(open.ticket, "w", &json!({}));
            let closed = k.call_close(open.ticket);
            done_tx.send((c.map_err(|v| code_of(&v)), closed)).unwrap();
        });
        let ticket = arrived_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let t0 = Instant::now();
        k.dispose_plugin("workspace");
        let elapsed = t0.elapsed();
        // Deadline honored even with a blocked callback.
        assert!(elapsed >= Duration::from_millis(120), "waited out the budget: {:?}", elapsed);
        assert!(elapsed < Duration::from_secs(5), "never hung: {:?}", elapsed);
        // Visible pending, never fake Disposed.
        let old = k.contexts.get_by_instance(matrix_core::InstanceId(search_gen1.instance)).unwrap();
        assert_eq!(old.state.as_str(), "CleanupPending");
        let inv = k.inventory();
        let pend = inv.get("calls").and_then(|v| v.as_array()).map(|a| {
            a.iter().filter(|c| {
                c.get("ticket").and_then(|t| t.as_u64()) == Some(ticket.0)
            }).cloned().collect::<Vec<_>>()
        }).unwrap_or_default();
        assert_eq!(pend.len(), 1, "visible pending call");
        assert_eq!(pend[0].get("state").and_then(|s| s.as_str()), Some("Expired"));
        // Libera tarde: commit rejeitado por prazo, close assenta.
        gate.release();
        let (commit, closed) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(commit.unwrap_err(), "deadline-exceeded");
        assert!(closed);
        let old = k.contexts.get_by_instance(matrix_core::InstanceId(search_gen1.instance)).unwrap();
        assert_eq!(old.state.as_str(), "Disposed", "assentou ao fechar");
        assert!(k.committed_effects(Some("search")).is_empty());
        let _ = (m_ws, m_search);
    });
}

// ---- fast cooperative cancellation ----

#[test]
fn cancel_policy_cooperative_worker_closes() {
    let k = fresh_kernel("cancel-coop");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();

    let (arrived_tx, arrived_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(|| {
            let open = k.call_open("search.query@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
            arrived_tx.send(()).unwrap();
            // Cooperativo: fecha assim que o cancel chega.
            let t0 = Instant::now();
            while !open.cancel.load(Ordering::SeqCst) {
                if t0.elapsed() > Duration::from_secs(5) {
                    done_tx.send(false).unwrap();
                    return;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let closed = k.call_close(open.ticket);
            done_tx.send(closed).unwrap();
        });
        arrived_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        k.dispose_plugin("workspace");
        assert!(done_rx.recv_timeout(Duration::from_secs(5)).unwrap(), "worker fechou no cancel");
        assert!(k.committed_effects(Some("search")).is_empty());
        assert_eq!(state_of(&k, "search"), "Waiting");
        let _ = (m_ws, m_search);
    });
}

// ---- C08: interleaved effects, removal preserves the independent ----

#[test]
fn c08_interleaved_effects_survive_provider_removal() {
    let k = fresh_kernel("c08");
    let home = home_of(&k);
    let m_a = write_manifest(&home, "alpha", basic_manifest("alpha", &["alpha.cap@1"], "echo"));
    let m_b = write_manifest(&home, "beta", basic_manifest("beta", &["beta.cap@1"], "echo"));
    k.load_manifest(&m_a).unwrap();
    k.load_manifest(&m_b).unwrap();

    let oa = k.call_open("alpha.cap@1", &json!({"i": 1}), None, &[]).unwrap();
    let ob = k.call_open("beta.cap@1", &json!({"i": 2}), None, &[]).unwrap();
    k.commit_effect(oa.ticket, "write", &json!({"by": "alpha"})).unwrap();
    k.commit_effect(ob.ticket, "write", &json!({"by": "beta"})).unwrap();
    k.call_close(oa.ticket);
    k.call_close(ob.ticket);

    k.dispose_plugin("alpha");
    // B preservado: efeito, cap e invoke intactos.
    let be = k.committed_effects(Some("beta"));
    assert_eq!(be.len(), 1);
    assert_eq!(be[0].payload.get("by").unwrap(), &json!("beta"));
    assert!(k.invoke("beta.cap@1", &json!({})).1);
    assert!(k.committed_effects(Some("alpha")).len() == 1, "A ledger never wiped");
    // A's closed ticket: invalid; reopening A works on a new generation.
    assert_eq!(code_of(&k.commit_effect(oa.ticket, "write", &json!({})).unwrap_err()), "unknown-ticket");
    k.load_manifest(&m_a).unwrap();
    assert!(k.invoke("alpha.cap@1", &json!({})).1);
}

// ---- ticket state machine ----

#[test]
fn ticket_state_machine_no_double_commit() {
    let k = fresh_kernel("tkt-sm");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();

    let o = k.call_open("search.query@1", &json!({}), None, &[]).unwrap();
    assert!(k.commit_effect(o.ticket, "w", &json!({})).is_ok());
    assert_eq!(code_of(&k.commit_effect(o.ticket, "w", &json!({})).unwrap_err()), "already-committed");
    assert!(k.call_close(o.ticket));
    assert!(!k.call_close(o.ticket), "idempotent close reports absence");
    assert_eq!(code_of(&k.commit_effect(o.ticket, "w", &json!({})).unwrap_err()), "unknown-ticket");

    let o2 = k.call_open("search.query@1", &json!({}), None, &[]).unwrap();
    assert!(k.call_cancel(o2.ticket, "teste"));
    assert!(!k.call_cancel(o2.ticket, "teste"), "cancel duplo informa");
    assert_eq!(code_of(&k.commit_effect(o2.ticket, "w", &json!({})).unwrap_err()), "cancelled");
    assert!(k.call_close(o2.ticket));
    assert_eq!(code_of(&k.commit_effect(matrix_core::TicketId(999999), "w", &json!({})).unwrap_err()), "unknown-ticket");
    let _ = (m_ws, m_search);
}

// ---- pins ----

#[test]
fn pinned_resource_rejects_early_release() {
    let k = fresh_kernel("pins");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let h = k.acquire("search", ResourceKind::Timer { label: "p".into(), interval_ms: 5 }).unwrap();

    let o = k.call_open("search.query@1", &json!({}), None, &[h]).unwrap();
    assert_eq!(k.release(h).unwrap_err(), "resource-pinned");
    assert!(k.call_close(o.ticket));
    assert!(k.release(h).is_ok(), "releases after close");

    // Invalid pin: wrong generation / foreign handle.
    let h2 = k.acquire("search", ResourceKind::Timer { label: "q".into(), interval_ms: 5 }).unwrap();
    let o2 = k.call_open("search.query@1", &json!({}), None, &[matrix_core::ResourceHandle(424242)]).unwrap_err();
    assert_eq!(code_of(&o2), "invalid-pin");
    // Another instance's pin (echo) on a search call.
    let m_echo = write_manifest(&home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    k.load_manifest(&m_echo).unwrap();
    let he = k.acquire("echo", ResourceKind::Timer { label: "e".into(), interval_ms: 5 }).unwrap();
    let o3 = k.call_open("search.query@1", &json!({}), None, &[he]).unwrap_err();
    assert_eq!(code_of(&o3), "invalid-pin");
    let _ = (h2, m_ws, m_search);
}

// ---- limite finito de chamadas (I09) ----

#[test]
fn inflight_calls_bounded() {
    use matrix_core::MAX_INFLIGHT_CALLS;
    let k = fresh_kernel("bounds");
    let home = home_of(&k);
    let m_echo = write_manifest(&home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    k.load_manifest(&m_echo).unwrap();
    let mut ids = vec![];
    for _ in 0..MAX_INFLIGHT_CALLS {
        ids.push(k.call_open("echo.msg@1", &json!({}), None, &[]).unwrap().ticket);
    }
    let err = k.call_open("echo.msg@1", &json!({}), None, &[]).unwrap_err();
    assert_eq!(code_of(&err), "resource-exhausted");
    for id in &ids {
        assert!(k.call_close(*id));
    }
    // After closing, admits again.
    let o = k.call_open("echo.msg@1", &json!({}), None, &[]).unwrap();
    assert!(k.call_close(o.ticket));
}

// ---- contract precision: discarded never reactivates ----

#[test]
fn disposed_instance_never_reactivated() {
    let k = fresh_kernel("no-resurrect");
    let home = home_of(&k);
    let (m_ws, m_search, _) = std_manifests(&home);
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let gen1 = k.instance_ref_of("search").unwrap();

    // Cascading withdraw: gen1 Disposed, definition persists, gen2 Waiting.
    k.dispose_plugin("workspace");
    let r1 = k.contexts.get_by_instance(matrix_core::InstanceId(gen1.instance)).unwrap();
    assert_eq!(r1.state.as_str(), "Disposed");
    let waiting = k.instance_ref_of("search").unwrap();
    assert_ne!(waiting.instance, gen1.instance);

    // Reintroduz: a Waiting (nunca descartada) promove; gen1 segue Disposed.
    k.load_manifest(&m_ws).unwrap();
    let gen2 = k.instance_ref_of("search").unwrap();
    assert_eq!(gen2.instance, waiting.instance);
    let r1 = k.contexts.get_by_instance(matrix_core::InstanceId(gen1.instance)).unwrap();
    assert_eq!(r1.state.as_str(), "Disposed", "descartada nunca reativa");

    // Segundo ciclo: gen2 Disposed, gen3 nova; ambas as antigas intocadas.
    k.dispose_plugin("workspace");
    let r2 = k.contexts.get_by_instance(matrix_core::InstanceId(gen2.instance)).unwrap();
    assert_eq!(r2.state.as_str(), "Disposed");
    let r1 = k.contexts.get_by_instance(matrix_core::InstanceId(gen1.instance)).unwrap();
    assert_eq!(r1.state.as_str(), "Disposed");
    k.load_manifest(&m_ws).unwrap();
    let gen3 = k.instance_ref_of("search").unwrap();
    assert_ne!(gen3.instance, gen1.instance);
    assert_ne!(gen3.instance, gen2.instance);
    assert_eq!(state_of(&k, "search"), "Active");
    let _ = m_search;
}

// ---- manifest policy ----

#[test]
fn manifest_call_policy_applies() {
    let k = fresh_kernel("policy-manifest");
    let home = home_of(&k);
    let m_search = write_manifest(
        &home,
        "search",
        {
            let mut s = with_requires(
                basic_manifest("search", &["search.query@1"], "noop"),
                json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
            );
            s["calls"] = json!({"search.query@1": {"on_withdraw": "drain", "drain_ms": 500}});
            s
        },
    );
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    k.load_manifest(&m_ws).unwrap();
    k.load_manifest(&m_search).unwrap();
    let pol = k.policy_for("search", "search.query@1");
    assert_eq!(pol.on_withdraw, WithdrawPolicy::Drain);
    assert_eq!(pol.drain_ms, 500);
    // Override na abertura vence o manifest.
    let o = k.call_open("search.query@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
    assert_eq!(o.policy.on_withdraw, WithdrawPolicy::Cancel);
    assert!(k.call_close(o.ticket));
    // Invalid calls manifests are rejected at load.
    let bad = write_manifest(&home, "bad", {
        let mut b = basic_manifest("bad", &["bad.cap@1"], "noop");
        b["calls"] = json!({"bad.cap@1": {"on_withdraw": "explode"}});
        b
    });
    assert!(k.load_manifest(&bad).is_err());
    assert!(k.context_state_of("bad").is_none());
    let _ = m_search;
    let t: Option<TicketState> = None;
    let _ = t;
}
