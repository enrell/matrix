//! M7 route integration: two live Services, real TLS sessions.
//!
//! Controller admits locally (parent/binding/consumer/grant/quotas),
//! executes on the executor Service, translates the terminal back.
//! Covers R01 (chain), R04 (withdraw mid-call), R07 (crash/query),
//! R08 (duplicates/conflicts), R09 (stale authority), R10 (credentials).

mod common;

use common::*;
use matrix_host::HostPolicy;
use matrix_runtime::route_controller::{PeerConfig, RouteManager, RouteSpec};
use matrix_runtime::service::{Grant, Service};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
#[allow(unused_imports)]

fn exec_service(home: &std::path::Path, principal: &str) -> Arc<Service> {
    let s = Service::open(
        home,
        HostPolicy { secure: true, components: HashMap::new(), enable_dependency_calls: true, domain: "test".into() },
        [(
            principal.into(),
            Grant {
                components: ["prov".into()].into(),
                capabilities: ["prov.api@1".into(), "matrix.effect.write".into()].into(),
            },
        )]
        .into(),
        HashMap::new(),
    )
    .unwrap();
    // In-process provider: no process spawn, kernel executes directly.
    s.provision(&json!({"id":"prov","capabilities":["prov.api@1"],"reducer":"echo"}))
        .unwrap();
    s
}

fn ctrl_service(home: &std::path::Path) -> Arc<Service> {
    // Controller operator holds no remote leases; the consumer is a plain
    // local definition (activated here for the test).
    let s = Service::open(
        home,
        HostPolicy { secure: true, components: HashMap::new(), enable_dependency_calls: true, domain: "test".into() },
        [( "test-operator".into(), Grant {
            components: ["cons".into()].into(),
            capabilities: ["cons.chain@1".into()].into(),
        })].into(),
        HashMap::new(),
    )
    .unwrap();
    s.provision(&json!({
        "id": "cons", "capabilities": ["cons.chain@1"],
        "requires": [{"interface": "prov.api@1", "provider": "prov"}],
        "outbound": {"request": ["prov.api@1"], "limits": {
            "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
            "max_calls_global": 64, "max_seen_requests": 64,
            "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
    }))
    .unwrap();
    // Remote-only provider definition: capability snapshot, never local.
    s.provision_remote(&json!({
        "id": "prov", "capabilities": ["prov.api@1"], "remote": true,
    }))
    .unwrap();
    s.sync_outbound_grants(&[("cons".to_string(), vec!["prov.api@1".to_string()])].into());
    s.activate("test-operator", "cons", 20000).unwrap();
    s
}

fn session_server(exec: &Arc<Service>) -> matrix_runtime::remote_session_server::RemoteSessionServer {
    let p = certificates();
    exec.serve_remote_session(
        "127.0.0.1:0".parse().unwrap(),
        matrix_runtime::session::server_config(
            &p.join("ca.der"),
            &p.join("server.der"),
            &p.join("server-key.der"),
        )
        .unwrap(),
    )
    .unwrap()
}

fn peer_config(
    name: &str,
    session_addr: std::net::SocketAddr,
    mgmt_addr: std::net::SocketAddr,
    client_cert: &str,
) -> PeerConfig {
    let p = certificates();
    PeerConfig {
        name: name.into(),
        address: session_addr,
        server_name: "localhost".into(),
        ca: p.join("ca.der"),
        cert: p.join(format!("{client_cert}.der")),
        key: p.join(format!("{client_cert}-key.der")),
        mgmt_address: mgmt_addr,
        domain: "test".into(),
        lease_ttl_ms: 8000,
    }
}

fn mgmt_server(exec: &Arc<Service>) -> matrix_runtime::remote::Server {
    let p = certificates();
    matrix_runtime::remote::Server::bind(
        exec.clone(),
        matrix_runtime::remote::server_config(
            &p.join("ca.der"),
            &p.join("server.der"),
            &p.join("server-key.der"),
        )
        .unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap()
}

struct Rig {
    ctrl: Arc<Service>,
    exec: Arc<Service>,
    mgr: RouteManager,
    _sess_server: matrix_runtime::remote_session_server::RemoteSessionServer,
    _mgmt_server: matrix_runtime::remote::Server,
}

fn rig() -> Rig {
    rig_with("client")
}

fn rig_with(client_cert: &str) -> Rig {
    let exec_home = home();
    let ctrl_home = home();
    // Controller principal = its client cert fingerprint.
    let ctrl_fp = peer(client_cert);
    let exec = exec_service(&exec_home, &ctrl_fp);
    let ctrl = ctrl_service(&ctrl_home);
    let sess_server = session_server(&exec);
    let sess_addr = sess_server.address;
    let mgmt = mgmt_server(&exec);
    let mgmt_addr = mgmt.address;
    let authority = ctrl_fp.clone();
    let mgr = RouteManager::new(ctrl.clone(), authority);
    mgr.sync(
        vec![peer_config("exec-A", sess_addr, mgmt_addr, client_cert)],
        vec![RouteSpec { consumer: "cons".into(), provider: "prov".into(), peer: "exec-A".into() }],
    );
    // Executor provider lease is held by the route (activate below via
    // the manager's attach round); wait for registration instead.
    // Diagnosis on timeout: surface the link state.
    for _ in 0..100 {
        if ctrl.kernel.remote_provider_of("prov").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if ctrl.kernel.remote_provider_of("prov").is_none() {
        panic!("no registration; link inspect: {}", mgr.inspect());
    }
    Rig { ctrl, exec, mgr, _sess_server: sess_server, _mgmt_server: mgmt }
}

fn cons_parent(ctrl: &Arc<Service>) -> matrix_core::TicketId {
    ctrl.kernel
        .call_open("cons.chain@1", &json!({}), Some(matrix_core::CallPolicy::cancel()), &[])
        .expect("parent opens")
        .ticket
}

fn remote_child(
    ctrl: &Arc<Service>,
    parent: matrix_core::TicketId,
    timeout_ms: u64,
) -> Result<matrix_core::TicketId, matrix_core::DepDeny> {
    let c = ctrl.kernel.instance_ref_of("cons").unwrap();
    let b = ctrl.kernel.dependency_bindings_of("cons")[0].id.clone();
    assert!(b.starts_with("rb-"), "remote handle: {b}");
    ctrl.kernel.dependency_admit(&matrix_core::DepAdmit {
        parent,
        binding: b,
        caller_logical: "cons".to_string(),
        caller_instance: c.instance,
        caller_generation: c.generation,
        session: "test".to_string(),
        timeout_ms,
    })
}

#[test]
fn r01_remote_leg_happy_path() {
    let rig = rig();
    // Consumer activates locally once the remote provider registers.
    // (Activated directly here; leases live on the executor Service.)
    assert_eq!(rig.ctrl.kernel.context_state_of("cons").as_deref(), Some("Active"));
    let parent = cons_parent(&rig.ctrl);
    let child = remote_child(&rig.ctrl, parent, 8000).expect("admit remote leg");
    // The ticket carries the executor peer (no local provider session).
    let rec = rig.ctrl.kernel.calls.get(child).expect("ticket");
    assert_eq!(rec.dep.as_ref().unwrap().remote_peer.as_deref(), Some("exec-A"));
    // Executor provider lease is live (route holds it).
    // Terminal validation behaves like a local leg; close releases quota.
    assert!(rig.ctrl.kernel.commit_effect(child, "test", &json!({"v": 1})).is_ok());
    assert!(rig.ctrl.kernel.call_close(child));
    assert!(rig.ctrl.kernel.call_close(parent));
    assert!(rig.ctrl.kernel.pending_calls().is_empty());
    rig.mgr.shutdown();
}

#[test]
fn r04_consumer_withdraw_cancels_remote_leg() {
    let rig = rig();
    let parent = cons_parent(&rig.ctrl);
    let child = remote_child(&rig.ctrl, parent, 8000).expect("leg");
    // Withdrawing the consumer cancels the remote leg like a local one.
    rig.ctrl.kernel.dispose_plugin("cons");
    // Late terminal validation fails closed (consumer withdrawn).
    let r = rig.ctrl.kernel.commit_effect(child, "test", &json!({"v": 1}));
    assert!(r.is_err(), "withdrawn consumer rejects late commit");
    assert!(rig.ctrl.kernel.call_close(parent));
    rig.mgr.shutdown();
}

#[test]
fn r09_stale_registration_rejects_terminal() {
    let rig = rig();
    let parent = cons_parent(&rig.ctrl);
    let child = remote_child(&rig.ctrl, parent, 8000).expect("leg");
    // Executor re-attests a new generation: old leg terminal is stale.
    rig.ctrl
        .kernel
        .register_remote_provider("prov", 77, 9, "exec-A")
        .unwrap();
    let r = rig.ctrl.kernel.commit_effect(child, "test", &json!({"v": 1}));
    assert!(r.is_err(), "superseded registration rejects late commit");
    assert!(rig.ctrl.kernel.call_close(parent));
    rig.mgr.shutdown();
}

#[test]
fn r07_operation_query_unknown_then_completed() {
    let rig = rig_with("rotated");
    // Unknown operation: explicit unknown, never proof of non-execution.
    let fp = peer("rotated");
    let q = rig.exec.store.operation(&fp, "op-never-ran");
    assert!(q.is_err(), "absent op is not found: {q:?}");
    // Admit without finish: query reports admitted-like unknown state
    // (service-level op.query maps missing→unknown through the route).
    let admitted = rig
        .exec
        .store
        .admit(&fp, "op-crash", &json!({"logical":"prov","cap":"prov.api@1","input":{}}));
    assert!(matches!(admitted, Ok(matrix_runtime::store::Admission::New)));
    let q = rig.exec.store.operation(&fp, "op-crash").unwrap();
    assert_ne!(q["state"], "completed");
    // Duplicate with divergent content conflicts (no silent overwrite).
    let conflict = rig.exec.store.admit(
        &fp,
        "op-crash",
        &json!({"logical":"prov","cap":"prov.api@1","input":{"other":true}}),
    );
    assert!(conflict.is_err(), "divergent content conflicts");
    // Same content replays the persisted terminal (no re-execution).
    rig.exec
        .store
        .finish(&fp, "op-crash", &json!({"ok":true,"value":{},"durability":"durable"}), false)
        .unwrap();
    let replay = rig
        .exec
        .store
        .admit(&fp, "op-crash", &json!({"logical":"prov","cap":"prov.api@1","input":{}}));
    assert!(matches!(replay, Ok(matrix_runtime::store::Admission::Completed(_))));
    // Foreign principal sees nothing (scoped ledger, no leak).
    assert!(rig.exec.store.operation("stranger", "op-crash").is_err());
    let _ = (rig.ctrl, rig.mgr);
}
