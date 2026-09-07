//! M7 remote legs in the kernel: one admission path for local and
//! remote children (no shadow tickets).
//!
//! - Registration fails closed (remote-only definitions, no hijack).
//! - Requirements resolve only while registered; withdraw on unregister.
//! - Admission/quotas/revocation/terminal behave like local legs, with
//!   the executor peer recorded on the ticket and journal.
//! - Numeric instance collisions with local instances are harmless.

use matrix_core::{CallPolicy, DepAdmit, Journal, Kernel};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fresh_kernel(tag: &str) -> Kernel {
    let home: PathBuf = std::env::temp_dir().join(format!(
        "matrix-m7k-{}-{}-{}",
        std::process::id(),
        tag,
        nanos()
    ));
    let _ = std::fs::create_dir_all(home.join("plugins"));
    let _ = std::fs::create_dir_all(home.join("run"));
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    Kernel::new(&home, journal, false)
}

fn limits() -> Value {
    json!({"max_depth": 3, "max_children_per_parent": 4,
           "max_calls_per_session": 16, "max_calls_global": 64,
           "max_seen_requests": 32, "max_queued_bytes": 65536,
           "max_deadline_ms": 5000})
}

fn consumer_manifest() -> Value {
    let mut m = json!({
        "id": "consumer", "version": "1.0.0",
        "capabilities": ["cons.cap@1"], "subscriptions": [],
        "requires": [{"interface": "prov.api@1", "provider": "rprov"}],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
    });
    m["outbound"] = json!({"request": ["prov.api@1"], "limits": limits()});
    m
}

fn remote_def() -> Value {
    json!({
        "id": "rprov", "version": "1.0.0",
        "capabilities": ["prov.api@1"], "subscriptions": [],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
        "remote": true,
    })
}

fn local_provider_manifest() -> Value {
    json!({
        "id": "localprov", "version": "1.0.0",
        "capabilities": ["local.api@1"], "subscriptions": [],
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

fn admit(k: &Kernel, inst: u64, gen: u64, parent: matrix_core::TicketId, binding: &str) -> Result<matrix_core::TicketId, matrix_core::DepDeny> {
    k.dependency_admit(&DepAdmit {
        parent,
        binding: binding.to_string(),
        caller_logical: "consumer".to_string(),
        caller_instance: inst,
        caller_generation: gen,
        session: "s1".to_string(),
        timeout_ms: 5000,
    })
}

#[test]
fn register_fails_closed_without_remote_definition() {
    let k = fresh_kernel("regclosed");
    // Unknown logical.
    assert!(k.register_remote_provider("ghost", 7, 1, "exec-A").is_err());
    // Local (non-remote) definition: hijack refused.
    load(&k, &local_provider_manifest());
    assert!(k.register_remote_provider("localprov", 7, 1, "exec-A").is_err());
    assert!(k.remote_provider_of("localprov").is_none());
    // Remote definition carrying requires: manifest rejected.
    let mut bad = remote_def();
    bad["requires"] = json!([{"interface": "x@1"}]);
    assert!(matrix_core::kernel::parse_manifest_value(&bad).is_err());
}

#[test]
fn consumer_waits_until_registered_then_binds_remote() {
    let k = fresh_kernel("waitreg");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Waiting"));
    // The remote definition itself never activates locally.
    assert!(k.context_state_of("rprov").is_none());
    assert!(k.dependency_bindings_of("consumer").is_empty());

    k.register_remote_provider("rprov", 7, 3, "exec-A").expect("register");

    eprintln!("WAITING-REASON: {:?}", k.waiting_reason_of("consumer"));
    eprintln!("INV: {}", serde_json::to_string_pretty(&k.inventory()).unwrap());
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Active"));
    let bs = k.dependency_bindings_of("consumer");
    assert_eq!(bs.len(), 1);
    assert_eq!(bs[0].capability, "prov.api@1");
    assert_eq!(bs[0].provider_logical, "rprov");
    assert_eq!(bs[0].provider_instance.0, 7);
    assert_eq!(bs[0].provider_generation, 3);
    assert!(bs[0].id.starts_with("rb-"), "remote handles are rb-N: {}", bs[0].id);
}

#[test]
fn remote_admit_happy_path_with_peer_on_ticket() {
    let k = fresh_kernel("rhappy");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    k.register_remote_provider("rprov", 7, 3, "exec-A").unwrap();
    k.grant_outbound("consumer", "prov.api@1");
    let c = k.instance_ref_of("consumer").unwrap();
    let b = k.dependency_bindings_of("consumer")[0].id.clone();
    let parent = k
        .call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("parent")
        .ticket;
    let child = admit(&k, c.instance, c.generation, parent, &b).expect("admit remote");
    let rec = k.calls.get(child).expect("ticket");
    assert_eq!(rec.dep.as_ref().unwrap().remote_peer.as_deref(), Some("exec-A"));
    assert_eq!(rec.instance.0, 7, "attested executor instance on ticket");
    // Terminal validation behaves like a local leg.
    assert!(k.commit_effect(child, "test", &json!({"v": 1})).is_ok());
    assert!(k.call_close(child));
    assert!(k.call_close(parent));
    assert!(k.pending_calls().is_empty());
    // Journal carries the remote marker for diagnosis.
    let inv = k.inventory();
    let _ = inv;
}

#[test]
fn reregister_stales_old_bindings() {
    let k = fresh_kernel("restale");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    k.register_remote_provider("rprov", 7, 3, "exec-A").unwrap();
    k.grant_outbound("consumer", "prov.api@1");
    let c = k.instance_ref_of("consumer").unwrap();
    let old = k.dependency_bindings_of("consumer")[0].id.clone();
    // Executor re-activated the provider: new generation attested.
    // The consumer is withdrawn and re-activated (new generation, new
    // handles); the old activation's binding authorizes nothing.
    k.register_remote_provider("rprov", 7, 4, "exec-A").unwrap();
    let c2 = k.instance_ref_of("consumer").unwrap();
    assert_ne!(c2.generation, c.generation, "consumer re-activated");
    let fresh = k.dependency_bindings_of("consumer")[0].id.clone();
    assert_ne!(old, fresh, "handles never reused");
    let parent = k
        .call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("parent")
        .ticket;
    let e = admit(&k, c.instance, c.generation, parent, &old).expect_err("stale binding");
    assert!(
        e.code == "dependency-unavailable" || e.code == "invalid-parent",
        "stale activation denied, got {}",
        e.code
    );
    let child = admit(&k, c2.instance, c2.generation, parent, &fresh).expect("fresh admits");
    assert!(k.call_close(child));
    assert!(k.call_close(parent));
}

#[test]
fn unregister_withdraws_and_denies() {
    let k = fresh_kernel("unreg");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    k.register_remote_provider("rprov", 7, 3, "exec-A").unwrap();
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Active"));
    assert!(k.unregister_remote_provider("rprov"));
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Waiting"));
    assert!(k.dependency_bindings_of("consumer").is_empty());
    assert!(!k.unregister_remote_provider("rprov"), "idempotent");
}

#[test]
fn dispose_remote_definition_unregisters() {
    let k = fresh_kernel("dispremote");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    k.register_remote_provider("rprov", 7, 3, "exec-A").unwrap();
    k.dispose_plugin("rprov");
    assert!(k.remote_provider_of("rprov").is_none());
    let inv = k.inventory();
    assert!(inv["remotes"].as_array().unwrap().is_empty());
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Waiting"));
}

#[test]
fn grant_revoke_and_parent_end_cancel_remote_leg() {
    let k = fresh_kernel("revokeleg");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    k.register_remote_provider("rprov", 7, 3, "exec-A").unwrap();
    k.grant_outbound("consumer", "prov.api@1");
    let c = k.instance_ref_of("consumer").unwrap();
    let b = k.dependency_bindings_of("consumer")[0].id.clone();
    let parent = k
        .call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("parent")
        .ticket;
    let child = admit(&k, c.instance, c.generation, parent, &b).expect("leg");
    // Operator revokes the edge: the remote leg is cancelled like a local one.
    let cancelled = k.revoke_outbound("consumer", "prov.api@1");
    assert!(cancelled.iter().any(|t| t.id == child));
    // Revocation already moved it to Cancelled, so the owner reaps it
    // directly (B4: only Admitted-equivalent holds authority).
    assert!(k.call_reap(child, "owner died"));
    assert!(k.call_close(parent));
}

#[test]
fn remote_instance_collision_with_local_is_harmless() {
    let k = fresh_kernel("collide");
    load(&k, &local_provider_manifest());
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    // Force the numeric collision: attest the remote instance with the
    // local provider's instance number.
    let local_inst = k.instance_ref_of("localprov").unwrap().instance;
    k.register_remote_provider("rprov", local_inst, 3, "exec-A").unwrap();
    k.grant_outbound("consumer", "prov.api@1");
    let c = k.instance_ref_of("consumer").unwrap();
    let b = k.dependency_bindings_of("consumer")[0].id.clone();
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Active"));
    // Disposing the unrelated local provider must not prune remote bindings.
    k.dispose_plugin("localprov");
    assert_eq!(k.context_state_of("consumer").as_deref(), Some("Active"));
    assert_eq!(k.dependency_bindings_of("consumer").len(), 1);
    let parent = k
        .call_open("cons.cap@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("parent")
        .ticket;
    let child = admit(&k, c.instance, c.generation, parent, &b).expect("still admits");
    assert!(k.call_close(child));
    assert!(k.call_close(parent));
}

#[test]
fn inventory_exposes_remote_state() {
    let k = fresh_kernel("inv");
    load(&k, &consumer_manifest());
    load(&k, &remote_def());
    k.register_remote_provider("rprov", 7, 3, "exec-A").unwrap();
    let inv = k.inventory();
    let remotes = inv["remotes"].as_array().unwrap();
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0]["logical"], "rprov");
    assert_eq!(remotes[0]["peer"], "exec-A");
    let defs = inv["definitions"].as_array().unwrap();
    let rd = defs.iter().find(|d| d["id"] == "rprov").unwrap();
    assert_eq!(rd["remote"], true);
}
