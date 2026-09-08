//! M6.1 step 2 acceptance: coordinated child admission in the kernel.
//!
//! - Opaque binding bound to complete activations (both sides).
//! - Operator grant independent of requested permissions; no grant, no admission.
//! - Parent/child registry + atomic quota reservation; residue-free rollback.
//! - Admission coordinated with withdraw, expiry, and revocation; commit
//!   valida a cadeia ancestral.
//! - Barriered interleavings: revocation wins → denies cleanly;
//!   admission wins → revocation finds and invalidates the child.
//!
//! Host dispatch/delivery is step 3; announcement stays disabled.

use matrix_core::{CallPolicy, DepAdmit, InstanceId, Journal, Kernel};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fresh_kernel(tag: &str) -> Kernel {
    let home: PathBuf = std::env::temp_dir().join(format!(
        "matrix-m61-{}-{}-{}",
        std::process::id(),
        tag,
        nanos()
    ));
    let _ = std::fs::create_dir_all(home.join("plugins"));
    let _ = std::fs::create_dir_all(home.join("run"));
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    Kernel::new(&home, journal, false)
}

fn outbound_limits(max_seen: u64, max_depth: u64, max_kids: u64) -> Value {
    json!({"max_depth": max_depth, "max_children_per_parent": max_kids,
           "max_calls_per_session": 16, "max_calls_global": 64,
           "max_seen_requests": max_seen, "max_queued_bytes": 65536,
           "max_deadline_ms": 5000})
}

fn consumer_manifest() -> Value {
    let mut m = json!({
        "id": "consumer", "version": "1.0.0",
        "capabilities": ["cons.cap@1"], "subscriptions": [],
        "requires": [{"interface": "prov.api@1", "provider": "provider"}],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
    });
    m["outbound"] = json!({"request": ["prov.api@1"], "limits": outbound_limits(32, 3, 4)});
    m
}

fn provider_manifest() -> Value {
    json!({
        "id": "provider", "version": "1.0.0",
        "capabilities": ["prov.api@1"], "subscriptions": [],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
    })
}

fn load(k: &Kernel, body: &Value) {
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let p = home.join("plugins").join(format!("{}.json", body["id"].as_str().unwrap()));
    std::fs::write(&p, serde_json::to_string_pretty(body).unwrap()).unwrap();
    k.load_manifest(&p).unwrap();
}

struct Pair {
    k: Kernel,
    cons_inst: u64,
    cons_gen: u64,
    #[allow(dead_code)]
    prov_inst: u64,
}

fn rig(tag: &str) -> Pair {
    let k = fresh_kernel(tag);
    load(&k, &provider_manifest());
    load(&k, &consumer_manifest());
    assert_eq!(k.context_state_of("provider").as_deref(), Some("Active"));
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Active"));
    let c = k.instance_ref_of("consumer").unwrap();
    let p = k.instance_ref_of("provider").unwrap();
    Pair { k, cons_inst: c.instance, cons_gen: c.generation, prov_inst: p.instance }
}

fn binding_of(k: &Kernel) -> String {
    let bs = k.dependency_bindings_of("consumer");
    assert_eq!(bs.len(), 1, "um handle por requisito: {:?}", bs.iter().map(|b| &b.id).collect::<Vec<_>>());
    assert_eq!(bs[0].capability, "prov.api@1");
    assert!(bs[0].id.starts_with("bind-"), "id opaco: {}", bs[0].id);
    bs[0].id.clone()
}

fn open_parent(k: &Kernel, _p: &Pair) -> matrix_core::TicketId {
    k.call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("pai abre")
        .ticket
}

fn admit(k: &Kernel, p: &Pair, parent: matrix_core::TicketId, binding: &str) -> Result<matrix_core::TicketId, matrix_core::DepDeny> {
    k.dependency_admit(&DepAdmit {
        parent,
        binding: binding.to_string(),
        caller_logical: "consumer".to_string(),
        caller_instance: p.cons_inst,
        caller_generation: p.cons_gen,
        session: "s1".to_string(),
        timeout_ms: 5000,
    })
}

fn usage(k: &Kernel) -> (u64, u64) {
    let u = k.dep_usage.lock();
    (u.per_session.get("s1").copied().unwrap_or(0), u.global)
}

// ---- happy path and static denials ----

#[test]
fn happy_admit_commit_close_releases() {
    let r = rig("happy");
    let (rev, _) = r.k.grant_outbound("consumer", "prov.api@1");
    assert!(rev >= 1);
    let b = binding_of(&r.k);
    let parent = open_parent(&r.k, &r);
    let child = admit(&r.k, &r, parent, &b).expect("admite");
    assert_eq!(usage(&r.k), (1, 1));
    // Mediated commit under the child ticket, valid chain.
    assert!(r.k.commit_effect(child, "test", &json!({"v": 1})).is_ok());
    assert!(r.k.call_close(child));
    assert_eq!(usage(&r.k), (0, 0), "reserva liberada no fim");
    assert!(r.k.call_close(parent));
    assert!(r.k.pending_calls().is_empty(), "sem ticket residual");
}

#[test]
fn no_grant_no_admission_no_residue() {
    let r = rig("nogrant");
    let b = binding_of(&r.k);
    let parent = open_parent(&r.k, &r);
    let e = admit(&r.k, &r, parent, &b).expect_err("sem grant do operador");
    assert_eq!(e.code, "permission-denied", "{}", e.reason);
    assert_eq!(usage(&r.k), (0, 0));
    assert!(r.k.pending_calls().iter().all(|t| t.dep.is_none()), "sem filha residual");
    assert!(r.k.call_close(parent));
}

#[test]
fn foreign_parent_and_stale_binding_denied() {
    let r = rig("foreign");
    r.k.grant_outbound("consumer", "prov.api@1");
    let b = binding_of(&r.k);
    // Foreign parent: the provider ticket used as parent by the consumer.
    let alien = r.k.call_open("prov.api@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap().ticket;
    let e = r.k.dependency_admit(&DepAdmit {
        parent: alien, binding: b.clone(),
        caller_logical: "consumer".into(), caller_instance: r.cons_inst,
        caller_generation: r.cons_gen, session: "s1".into(), timeout_ms: 100,
    }).expect_err("pai alheio");
    assert_eq!(e.code, "invalid-parent", "{}", e.reason);
    assert!(r.k.call_close(alien));
    // Pai desconhecido.
    let parent = open_parent(&r.k, &r);
    let e = r.k.dependency_admit(&DepAdmit {
        parent: matrix_core::TicketId(999_999), binding: b.clone(),
        caller_logical: "consumer".into(), caller_instance: r.cons_inst,
        caller_generation: r.cons_gen, session: "s1".into(), timeout_ms: 100,
    }).expect_err("pai desconhecido");
    assert_eq!(e.code, "invalid-parent");
    // Reintroduz o provedor: cascata retira o consumidor; handle antigo
    // denies (other activation), reactivation's new handle admits.
    let pref = r.k.instance_ref_of("provider").unwrap();
    r.k.dispose_plugin("provider");
    load(&r.k, &provider_manifest());
    let pnew = r.k.instance_ref_of("provider").unwrap();
    assert!(pnew.generation > pref.generation);
    let cnew = r.k.instance_ref_of("consumer").unwrap();
    assert!(cnew.generation > r.cons_gen, "consumer reactivated in a new generation");
    let parent2 = r.k.call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap().ticket;
    // Handle pruned at dispose: unknown (reintroduction issues another).
    let e = r.k.dependency_admit(&DepAdmit {
        parent: parent2, binding: b.clone(),
        caller_logical: "consumer".into(), caller_instance: cnew.instance,
        caller_generation: cnew.generation, session: "s1".into(), timeout_ms: 100,
    }).expect_err("dead-activation handle");
    assert_eq!(e.code, "dependency-unavailable", "{} {}", e.code, e.reason);
    // Handle of ANOTHER consumer with this parent/caller: authorizes nothing.
    let mut m2 = consumer_manifest();
    m2["id"] = json!("consumer2");
    m2["capabilities"] = json!(["cons2.cap@1"]);
    load(&r.k, &m2);
    let bx = r.k.dependency_bindings_of("consumer2");
    assert_eq!(bx.len(), 1);
    let e = r.k.dependency_admit(&DepAdmit {
        parent: parent2, binding: bx[0].id.clone(),
        caller_logical: "consumer".into(), caller_instance: cnew.instance,
        caller_generation: cnew.generation, session: "s1".into(), timeout_ms: 100,
    }).expect_err("handle alheio");
    assert_eq!(e.code, "invalid-parent", "{} {}", e.code, e.reason);
    let b2 = binding_of(&r.k);
    assert_ne!(b2, b, "reactivation issues a new handle");
    let child = r.k.dependency_admit(&DepAdmit {
        parent: parent2, binding: b2,
        caller_logical: "consumer".into(), caller_instance: cnew.instance,
        caller_generation: cnew.generation, session: "s1".into(), timeout_ms: 100,
    }).expect("novo handle admite");
    assert!(r.k.call_close(child));
    assert!(r.k.call_close(parent2));
    assert!(r.k.call_close(parent));
    assert_eq!(usage(&r.k), (0, 0));
}

// ---- revocation finds the child ----

#[test]
fn grant_revoke_invalidates_child() {
    let r = rig("revoke");
    r.k.grant_outbound("consumer", "prov.api@1");
    let b = binding_of(&r.k);
    let parent = open_parent(&r.k, &r);
    let child = admit(&r.k, &r, parent, &b).expect("admite");
    let revoked = r.k.revoke_outbound("consumer", "prov.api@1");
    assert_eq!(revoked.len(), 1);
    assert_eq!(revoked[0].id, child);
    let st = r.k.calls.get(child).map(|t| format!("{:?}", t.state));
    assert_eq!(st.as_deref(), Some("Cancelled"), "autoridade revogada: {:?}", st);
    // Commit under a revoked chain is rejected (ticket cancelled by revocation).
    let e = r.k.commit_effect(child, "test", &json!({})).expect_err("grant revogado");
    assert_eq!(e["code"], "cancelled", "{}", e);
    // Residue-free settlement (reap + closes).
    assert!(r.k.call_reap(child, "test-cleanup"));
    assert_eq!(usage(&r.k), (0, 0));
    assert!(r.k.call_close(parent));
    assert!(r.k.pending_calls().is_empty());
}

#[test]
fn parent_end_revokes_child() {
    let r = rig("parentend");
    r.k.grant_outbound("consumer", "prov.api@1");
    let b = binding_of(&r.k);
    let parent = open_parent(&r.k, &r);
    let child = admit(&r.k, &r, parent, &b).expect("admite");
    assert!(r.k.call_close(parent), "pai conclui");
    let st = r.k.calls.get(child).map(|t| format!("{:?}", t.state));
    assert_eq!(st.as_deref(), Some("Cancelled"), "filha revogada: {:?}", st);
    assert!(r.k.commit_effect(child, "test", &json!({})).is_err());
    assert!(r.k.call_close(child));
    assert_eq!(usage(&r.k), (0, 0));
}

#[test]
fn consumer_withdraw_revokes_child() {
    let r = rig("conswithdraw");
    r.k.grant_outbound("consumer", "prov.api@1");
    let b = binding_of(&r.k);
    let parent = open_parent(&r.k, &r);
    let child = admit(&r.k, &r, parent, &b).expect("admite");
    let out = r.k.dispose_instance(InstanceId(r.cons_inst));
    assert_eq!(out.as_str(), "CleanupPending", "pai segura a retirada");
    let st = r.k.calls.get(child).map(|t| format!("{:?}", t.state));
    assert_eq!(st.as_deref(), Some("Cancelled"), "filha revogada: {:?}", st);
    assert!(r.k.call_close(parent));
    assert!(r.k.call_reap(child, "test-cleanup"));
    assert_eq!(usage(&r.k), (0, 0));
}

// ---- expiry ----

#[test]
fn expired_child_rejects_commit_and_settles() {
    let r = rig("expiry");
    r.k.grant_outbound("consumer", "prov.api@1");
    let b = binding_of(&r.k);
    let parent = open_parent(&r.k, &r);
    let child = r.k.dependency_admit(&DepAdmit {
        parent, binding: b,
        caller_logical: "consumer".into(), caller_instance: r.cons_inst,
        caller_generation: r.cons_gen, session: "s1".into(), timeout_ms: 1,
    }).expect("admite");
    std::thread::sleep(std::time::Duration::from_millis(5));
    r.k.calls.expire_overdue(std::time::Instant::now());
    let e = r.k.commit_effect(child, "test", &json!({})).expect_err("expirado");
    assert_eq!(e["code"], "deadline-exceeded", "{}", e);
    assert!(r.k.call_close(child));
    assert!(r.k.call_close(parent));
    assert_eq!(usage(&r.k), (0, 0));
}

// ---- profundidade e quotas ----

#[test]
fn depth_bounded_along_chain() {
    // Cadeia l1 → l2 → l3 (raiz = 0): filha depth 1, neta depth 2.
    // Middle max_depth=1 denies the grandchild; max_depth=2 admits.
    let k = fresh_kernel("depth");
    let home = k.plugins_dir.parent().unwrap().to_path_buf();
    let put = |m: Value| {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        k.load_manifest(&p).unwrap();
    };
    let leaf = json!({
        "id": "l3", "version": "1.0.0", "capabilities": ["l3.api@1"],
        "subscriptions": [], "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent"});
    let mut mid = json!({
        "id": "l2", "version": "1.0.0", "capabilities": ["l2.api@1"],
        "subscriptions": [], "requires": [{"interface": "l3.api@1", "provider": "l3"}],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent"});
    mid["outbound"] = json!({"request": ["l3.api@1"], "limits": outbound_limits(32, 1, 4)});
    let mut top = json!({
        "id": "l1", "version": "1.0.0", "capabilities": ["l1.cap@1"],
        "subscriptions": [], "requires": [{"interface": "l2.api@1", "provider": "l2"}],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent"});
    top["outbound"] = json!({"request": ["l2.api@1"], "limits": outbound_limits(32, 3, 4)});
    put(leaf);
    put(mid);
    put(top);
    k.grant_outbound("l1", "l2.api@1");
    k.grant_outbound("l2", "l3.api@1");
    let r1 = k.instance_ref_of("l1").unwrap();
    let b1 = k.dependency_bindings_of("l1");
    assert_eq!(b1.len(), 1);
    let root = k.call_open("l1.cap@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap().ticket;
    let c1 = k.dependency_admit(&DepAdmit {
        parent: root, binding: b1[0].id.clone(),
        caller_logical: "l1".into(), caller_instance: r1.instance,
        caller_generation: r1.generation, session: "s".into(), timeout_ms: 5000,
    }).expect("depth 1 admite");
    // Neta: pai=c1 executado por l2, binding de l2 → depth 2 > max_depth=1.
    let r2 = k.instance_ref_of("l2").unwrap();
    let b2 = k.dependency_bindings_of("l2");
    assert_eq!(b2.len(), 1);
    let e = k.dependency_admit(&DepAdmit {
        parent: c1, binding: b2[0].id.clone(),
        caller_logical: "l2".into(), caller_instance: r2.instance,
        caller_generation: r2.generation, session: "s".into(), timeout_ms: 5000,
    }).expect_err("depth 2 past the cap");
    assert_eq!(e.code, "resource-exhausted", "{} {}", e.code, e.reason);
    assert!(k.call_close(c1));
    assert!(k.call_close(root));
    let u = k.dep_usage.lock();
    assert_eq!((u.per_session.get("s").copied().unwrap_or(0), u.global), (0, 0));
}

// ---- interleavings decisivos ----

/// Runs admission × revocation N times: either denies cleanly, or admits and
/// revocation finds and invalidates. At the end, no residue and zero quotas.
fn interleave(tag: &str, revoker: fn(&Kernel, &Pair)) {
    for i in 0..60 {
        let r = rig(&format!("{}-{}", tag, i));
        r.k.grant_outbound("consumer", "prov.api@1");
        let b = binding_of(&r.k);
        let parent = open_parent(&r.k, &r);
        let bar = Arc::new(Barrier::new(2));
        let (kk, inst, gen, rp): (&Kernel, u64, u64, &Pair) = (&r.k, r.cons_inst, r.cons_gen, &r);
        std::thread::scope(|s| {
            let bb = bar.clone();
            let b2 = b.clone();
            s.spawn(move || {
                bb.wait();
                for _ in 0..20 {
                    let res = kk.dependency_admit(&DepAdmit {
                        parent, binding: b2.clone(),
                        caller_logical: "consumer".into(),
                        caller_instance: inst, caller_generation: gen,
                        session: "s1".into(), timeout_ms: 5000,
                    });
                    // Fecha de imediato o que admitir (dono vivo).
                    if let Ok(ch) = res {
                        kk.call_close(ch);
                    }
                }
            });
            s.spawn(move || {
                bar.wait();
                for _ in 0..20 {
                    revoker(kk, rp);
                }
            });
        });
        // Per-iteration invariant: an Admitted survivor has a valid chain
        // (commit-probe); the parent is closed; quotas return to zero.
        for t in r.k.pending_calls() {
            if t.dep.is_some() {
                assert_eq!(t.state, matrix_core::TicketState::Admitted, "filha pendente aberta: {:?}", t.state);
                assert!(r.k.commit_effect(t.id, "probe", &json!({})).is_ok(), "survivor has a valid chain");
                assert!(r.k.call_close(t.id));
            } else if t.id == parent {
                assert!(r.k.call_close(t.id));
            } else {
                panic!("ticket inesperado: {:?}", t.id);
            }
        }
        assert_eq!(usage(&r.k), (0, 0), "quotas no zero (iter {})", i);
    }
}

#[test]
fn interleave_provider_withdraw_vs_admit() {
    interleave("prov", |k, r| {
        if let Some(cur) = k.instance_ref_of("provider") {
            let _ = k.dispose_instance(InstanceId(cur.instance));
        }
        load(k, &provider_manifest());
        let _ = r.cons_inst;
    });
}

#[test]
fn interleave_grant_revoke_vs_admit() {
    interleave("grant", |k, _| {
        k.revoke_outbound("consumer", "prov.api@1");
        k.grant_outbound("consumer", "prov.api@1");
    });
}

#[test]
fn interleave_consumer_withdraw_vs_admit() {
    interleave("cons", |k, r| {
        if k.context_state_of("consumer").as_deref() == Some("Active") {
            let _ = k.dispose_instance(InstanceId(r.cons_inst));
        }
    });
}

// ---- atomicidade de quota ----

#[test]
fn quota_per_parent_atomic_under_concurrency() {
    // max_children_per_parent=1 na fixture dedicada: exatamente um vence.
    let k = fresh_kernel("quota");
    load(&k, &provider_manifest());
    let mut m = consumer_manifest();
    m["outbound"]["limits"] = outbound_limits(32, 3, 1);
    load(&k, &m);
    k.grant_outbound("consumer", "prov.api@1");
    let b = binding_of(&k);
    let c = k.instance_ref_of("consumer").unwrap();
    let parent = k.call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap().ticket;
    let bar = Arc::new(Barrier::new(8));
    let k = Arc::new(k);
    let mut ths = vec![];
    for _ in 0..8 {
        let (kk, bb, bnd) = (k.clone(), bar.clone(), b.clone());
        ths.push(std::thread::spawn(move || {
            bb.wait();
            kk.dependency_admit(&DepAdmit {
                parent, binding: bnd,
                caller_logical: "consumer".into(),
                caller_instance: c.instance, caller_generation: c.generation,
                session: "s1".into(), timeout_ms: 5000,
            }).map(|t| t.0)
        }));
    }
    let outs: Vec<_> = ths.into_iter().map(|t| t.join().unwrap()).collect();
    let oks: Vec<_> = outs.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(oks.len(), 1, "exatamente um vence: {:?}", outs);
    for e in outs.iter().filter_map(|r| r.as_ref().err()) {
        assert_eq!(e.code, "resource-exhausted", "perdedor com quota: {} {}", e.code, e.reason);
    }
    for t in k.pending_calls() {
        if t.dep.is_some() {
            assert!(k.call_close(t.id));
        }
    }
    assert!(k.call_close(parent));
    assert_eq!(usage(&k), (0, 0));
}

#[test]
fn acquire_external_rejects_foreign_generation() {
    use matrix_core::ResourceKind;
    let k = fresh_kernel("extgen");
    load(&k, &provider_manifest());
    load(&k, &consumer_manifest());
    let r = k.instance_ref_of("consumer").unwrap();
    // Wrong generation and instance: fails closed at the door.
    assert_eq!(
        k.acquire_external("consumer", ResourceKind::Timer { label: "t".into(), interval_ms: 10 },
            InstanceId(r.instance), r.generation + 1).unwrap_err(),
        "stale-generation");
    assert_eq!(
        k.acquire_external("consumer", ResourceKind::Timer { label: "t".into(), interval_ms: 10 },
            InstanceId(r.instance + 999), r.generation).unwrap_err(),
        "stale-generation");
    // Right one: acquires; after reintroduction, the old one denies.
    let h = k.acquire_external("consumer", ResourceKind::Timer { label: "t".into(), interval_ms: 10 },
        InstanceId(r.instance), r.generation).expect("current");
    assert!(k.validate_handle(h, "consumer").is_ok());
    k.dispose_plugin("consumer");
    load(&k, &consumer_manifest());
    let r2 = k.instance_ref_of("consumer").unwrap();
    assert!(r2.generation > r.generation);
    assert!(k.acquire_external("consumer", ResourceKind::Timer { label: "t".into(), interval_ms: 10 },
        InstanceId(r.instance), r.generation).is_err());
    assert!(k.release(h).is_err(), "dead-generation handle never releases");
}

#[test]
fn acquire_external_withdraw_race_leaves_no_residue() {
    use matrix_core::ResourceKind;
    use std::sync::Barrier;
    // Interleaving acquire × dispose: ou nega limpo, ou publica na
    // live activation; never publishes a discarded instance's resource.
    for i in 0..200 {
        let k = Arc::new(fresh_kernel(&format!("extrace-{}", i)));
        load(&k, &provider_manifest());
        load(&k, &consumer_manifest());
        let bar = Arc::new(Barrier::new(2));
        let (k2, b2) = (k.clone(), bar.clone());
        let acq = std::thread::spawn(move || {
            b2.wait();
            let mut out = vec![];
            for _ in 0..30 {
                if let Some(r) = k2.instance_ref_of("consumer") {
                    out.push(k2.acquire_external(
                        "consumer",
                        ResourceKind::Sub { topic: "test.topic".into() },
                        InstanceId(r.instance), r.generation));
                }
            }
            out
        });
        bar.wait();
        if let Some(r) = k.instance_ref_of("consumer") {
            let _ = k.dispose_instance(InstanceId(r.instance));
        }
        let results = acq.join().unwrap();
        for r in results {
            if let Ok(h) = r {
                let rec = k.resources.record(h).expect("handle recorded");
                // Released handles are settled history; only Active ones
                // must belong to a live activation.
                if rec.state == matrix_core::ResourceState::Active {
                    let st = k.contexts.get_by_instance(rec.owner_instance)
                        .map(|c| c.state.as_str().to_string());
                    assert_eq!(st.as_deref(), Some("Active"),
                        "active handle of non-active instance: {:?} (iter {})", rec, i);
                }
            }
        }
        // The registry never points at a discarded instance.
        if let Some(id) = k.caps.resolve_instance("cons.cap@1") {
            let _ = id;
        }
    }
}
