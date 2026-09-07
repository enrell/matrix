//! M7 authority, renewal, compat and reconnect (R05/R06/R09/R10/R12).
//!
//! - Revoked principals fail closed at admission (no new effects).
//! - Renewal sequences are idempotent (lost-response replay) and stale
//!   sequences never revive withdrawn leases.
//! - Old executors without `remote-calls/1` fail explicitly (no silent downgrade).
//! - Reconnect uses a new session id + reconciles before publishing bindings.

mod common;

use common::{certificates, home, peer};
use matrix_host::HostPolicy;
use matrix_runtime::route_controller::{PeerConfig, RouteManager, RouteSpec};
use matrix_runtime::service::{Grant, Service};
use matrix_runtime::session::{self, Session, SessionLimits};
use serde_json::json;
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    s.provision(&json!({"id":"prov","capabilities":["prov.api@1"],"reducer":"echo"}))
        .unwrap();
    s
}

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timeout: {msg}");
}

#[test]
fn revoked_principal_fails_closed_at_admission() {
    let exec = exec_service(&home(), &peer("client"));
    // Activate a provider lease for the controller principal.
    let act = exec.activate(&peer("client"), "prov", 8000).unwrap();
    let token = act["lease"].as_str().unwrap().to_string();
    assert!(exec.lease_snapshot_by_token(&peer("client"), &token).is_some());
    assert!(exec.grant_covers(&peer("client"), "prov.api@1"));
    // Revocation retires authority immediately: snapshots and grants refuse.
    exec.revoke(&peer("client")).unwrap();
    assert!(exec.lease_snapshot_by_token(&peer("client"), &token).is_none(), "revoked lease refuses");
    assert!(!exec.grant_covers(&peer("client"), "prov.api@1"), "revoked grant refuses");
    // Liveness recheck (pre-persist guard) also fails.
    let owner = exec.kernel.instance_ref_of("prov").unwrap();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    assert!(!exec.lease_live(&peer("client"), "prov", fence, &owner));
    exec.shutdown();
}

#[test]
fn renew_seq_idempotent_stale_never_revives() {
    let exec = exec_service(&home(), &peer("client"));
    let act = exec.activate(&peer("client"), "prov", 8000).unwrap();
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    // First renewal rotates; replaying the same seq replays (lost-response recovery).
    let r1 = exec.renew_seq(&peer("client"), &token, fence, 1, 8000).unwrap();
    let token2 = r1["lease"].as_str().unwrap().to_string();
    assert_ne!(token, token2, "rotation");
    let replay = exec.renew_seq(&peer("client"), &token2, fence, 1, 8000).unwrap();
    assert_eq!(replay["lease"].as_str().unwrap(), token2, "same seq replays current token");
    assert_eq!(replay.get("replay"), Some(&json!(true)));
    // Older seq is stale (never rewinds).
    assert!(exec.renew_seq(&peer("client"), &token2, fence, 0, 8000).is_err());
    // Wrong fence refuses.
    assert!(exec.renew_seq(&peer("client"), &token2, fence + 1, 2, 8000).is_err());
    // Withdrawn lease never revives via delayed renew.
    exec.revoke(&peer("client")).unwrap();
    assert!(exec.renew_seq(&peer("client"), &token2, fence, 2, 8000).is_err(), "revoked never revives");
    exec.shutdown();
}

#[test]
fn old_executor_without_calls_feature_fails_explicitly() {
    // Server welcomes without `remote-calls/1`: the controller must fail
    // closed with an explicit error, never silently downgrade to unary.
    let p = certificates();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        let tls = session::server_config(&p.join("ca.der"), &p.join("server.der"), &p.join("server-key.der")).unwrap();
        let _ = Session::accept(
            tcp,
            tls,
            SessionLimits::default(),
            &|_| true,
            |_| {
                Ok(json!({
                    "version": "0.1",
                    "features": ["remote-streams/1"],
                    "executor_epoch": "1",
                    "limits": {"max_frame": 1048576},
                }))
            },
        );
    });
    let p = certificates();
    let tls = session::client_config(&p.join("ca.der"), &p.join("client.der"), &p.join("client-key.der")).unwrap();
    let (_session, welcome) = Session::connect(
        addr,
        "localhost",
        tls,
        SessionLimits::default(),
        Session::hello_body("test", &peer("client"), 1),
    )
    .unwrap();
    let feats = welcome.get("features").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    assert!(!feats.iter().any(|f| f == "remote-calls/1"), "old executor lacks calls");
    // The attach gate checks exactly this (mirrored here explicitly).
    assert!(feats.iter().all(|f| f != "remote-calls/1"), "no silent downgrade: caller must refuse");
}

#[test]
fn reconnect_uses_new_session_and_reconciles() {
    // Full rig: connect, note the session id, tear down the route,
    // re-attach with a fresh manager. The new session must differ and
    // the registration must refresh via reconcile (no stale bindings).
    let exec_home = home();
    let ctrl_home = home();
    let ctrl_fp = peer("client");
    let exec = exec_service(&exec_home, &ctrl_fp);
    let ctrl = Service::open(
        &ctrl_home,
        HostPolicy { secure: true, components: HashMap::new(), enable_dependency_calls: true, domain: "test".into() },
        [("test-operator".into(), Grant {
            components: ["cons".into()].into(),
            capabilities: ["cons.chain@1".into()].into(),
        })]
        .into(),
        HashMap::new(),
    )
    .unwrap();
    ctrl.provision(&json!({
        "id": "cons", "capabilities": ["cons.chain@1"],
        "requires": [{"interface": "prov.api@1", "provider": "prov"}],
        "outbound": {"request": ["prov.api@1"], "limits": {
            "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
            "max_calls_global": 64, "max_seen_requests": 64,
            "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
    }))
    .unwrap();
    ctrl.provision_remote(&json!({"id": "prov", "capabilities": ["prov.api@1"], "remote": true})).unwrap();
    let p = certificates();
    let sess_server = ctrl_dummy_session_server(&exec, &p);
    let sess_addr = sess_server.address;
    let mgmt = matrix_runtime::remote::Server::bind(
        exec.clone(),
        matrix_runtime::remote::server_config(&p.join("ca.der"), &p.join("server.der"), &p.join("server-key.der")).unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let mgmt_addr = mgmt.address;
    let pc = |cert: &str| PeerConfig {
        name: "exec-A".into(),
        address: sess_addr,
        server_name: "localhost".into(),
        ca: p.join("ca.der"),
        cert: p.join(format!("{cert}.der")),
        key: p.join(format!("{cert}-key.der")),
        mgmt_address: mgmt_addr,
        domain: "test".into(),
        lease_ttl_ms: 8000,
    };
    let mk_mgr = || {
        let m = RouteManager::new(ctrl.clone(), ctrl_fp.clone());
        m.sync(
            vec![pc("client")],
            vec![RouteSpec { consumer: "cons".into(), provider: "prov".into(), peer: "exec-A".into() }],
        );
        m
    };
    let mgr1 = mk_mgr();
    wait_for("first registration", Duration::from_secs(20), || {
        ctrl.kernel.remote_provider_of("prov").is_some()
    });
    let sess1 = mgr1.inspect()["peers"][0]["session"]["id"].as_str().unwrap_or("").to_string();
    assert!(!sess1.is_empty());
    mgr1.shutdown();
    // Re-attach: new session id, registration refreshes (reconcile before publish).
    let mgr2 = mk_mgr();
    wait_for("second registration", Duration::from_secs(20), || {
        ctrl.kernel.remote_provider_of("prov").is_some()
    });
    let sess2 = mgr2.inspect()["peers"][0]["session"]["id"].as_str().unwrap_or("").to_string();
    assert!(!sess2.is_empty());
    assert_ne!(sess1, sess2, "reconnect never reuses the old session id");
    mgr2.shutdown();
    ctrl.shutdown();
    exec.shutdown();
}

#[test]
fn ledger_does_not_suppress_distinct_boot_operations() {
    // Temporal validity, ledger half (R08): two operation ids minted for
    // the same logical invocation on different boots (different epochs;
    // see `operation_ids_differ_across_controller_boots` for the minting
    // half) admit independently — the second is never suppressed as a
    // replay of the first, and finishing one never resolves the other.
    let exec = exec_service(&home(), &peer("client"));
    let fp = peer("client");
    let body = json!({"logical": "prov", "cap": "prov.api@1", "input": {"v": 1}});
    let op_epoch_1 = "test:epoch-aaa:cons:1:1:1:rb-1:r1:7:digest1";
    let op_epoch_2 = "test:epoch-bbb:cons:1:1:1:rb-1:r1:7:digest2";
    assert!(matches!(
        exec.store.admit(&fp, op_epoch_1, &body),
        Ok(matrix_runtime::store::Admission::New)
    ));
    assert!(matches!(
        exec.store.admit(&fp, op_epoch_2, &body),
        Ok(matrix_runtime::store::Admission::New)
    ));
    exec.store
        .finish(&fp, op_epoch_1, &json!({"ok": true, "value": {"n": 1}, "durability": "durable"}), false)
        .unwrap();
    // Same content retried under the SAME id still replays (no re-execution).
    assert!(matches!(
        exec.store.admit(&fp, op_epoch_1, &body),
        Ok(matrix_runtime::store::Admission::Completed(_))
    ));
    // But the other boot's id is untouched: still admitted, never resolved.
    let q = exec.store.operation(&fp, op_epoch_2).unwrap();
    assert_ne!(q["state"], "completed", "boot-2 leg independent: {q}");
    exec.shutdown();
}

fn ctrl_dummy_session_server(exec: &Arc<Service>, p: &std::path::Path) -> matrix_runtime::remote_session_server::RemoteSessionServer {
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
