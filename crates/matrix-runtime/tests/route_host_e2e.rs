//! End-to-end remote leg through the real host worker (M7 R01).
//!
//! The controller host dispatches `dependency.open` over a real TLS
//! session (RouteManager transport) to the executor Service, which runs
//! a real `dep_node` provider process. Covers:
//! - happy path: consumer chain → remote provider → terminal back;
//! - cancel: slow provider, consumer withdraw cancels the leg (no phantom ok);
//! - session loss: route torn down mid-call fails as unknown, never false success.
//!
//! Uses generic `dep_node` fixtures in separate processes; no product app.

mod common;

use common::*;
use matrix_host::HostPolicy;
use matrix_runtime::route_controller::{PeerConfig, RouteManager, RouteSpec};
use matrix_runtime::service::{Grant, Service};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn dep_bin() -> String {
    let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/examples/dep_node");
    assert!(p.exists(), "build matrix-host --examples first: {:?}", p);
    p.to_string_lossy().to_string()
}

fn exec_service_with(home: &std::path::Path, principal: &str, manifest: &Value) -> Arc<Service> {
    let s = Service::open(
        home,
        HostPolicy {
            secure: true,
            components: [("prov".into(), None)].into(),
            enable_dependency_calls: true,
            domain: "test".into(),
        },
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
    s.provision(manifest).unwrap();
    s
}

fn python_prov_manifest() -> Value {
    let script = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../sdk-python/dep_node.py");
    assert!(script.exists(), "sdk-python/dep_node.py missing: {:?}", script);
    let python = std::env::var("PYTHON3").unwrap_or_else(|_| "python3".to_string());
    json!({
        "id": "prov", "capabilities": ["prov.api@1"],
        "execution": {"kind": "process", "entrypoint": python,
            "args": [script.to_string_lossy().to_string(), "--matrix-sock", "{sock}", "--id", "{id}"]},
    })
}

fn python_cons_manifest(extra_args: &[String]) -> Value {
    let script = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../sdk-python/dep_node.py");
    assert!(script.exists(), "sdk-python/dep_node.py missing: {:?}", script);
    let python = std::env::var("PYTHON3").unwrap_or_else(|_| "python3".to_string());
    let mut args = vec![script.to_string_lossy().to_string(), "--matrix-sock".to_string(), "{sock}".to_string(), "--id".to_string(), "{id}".to_string()];
    args.extend(extra_args.iter().cloned());
    json!({
        "id": "cons", "capabilities": ["cons.chain@1"],
        "requires": [{"interface": "prov.api@1", "provider": "prov"}],
        "outbound": {"request": ["prov.api@1"], "limits": {
            "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
            "max_calls_global": 64, "max_seen_requests": 64,
            "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
        "execution": {"kind": "process", "entrypoint": python, "args": args},
    })
}

fn rust_prov_manifest(entry: &str, extra_args: &[String]) -> Value {
    let mut args = vec!["--matrix-sock".to_string(), "{sock}".to_string(), "--id".to_string(), "{id}".to_string()];
    args.extend(extra_args.iter().cloned());
    json!({
        "id": "prov", "capabilities": ["prov.api@1"],
        "execution": {"kind": "process", "entrypoint": entry, "args": args},
    })
}

fn cons_manifest(entry: &str, extra_args: &[String]) -> Value {
    let mut args = vec!["--matrix-sock".to_string(), "{sock}".to_string(), "--id".to_string(), "{id}".to_string()];
    args.extend(extra_args.iter().cloned());
    json!({
        "id": "cons", "capabilities": ["cons.chain@1"],
        "requires": [{"interface": "prov.api@1", "provider": "prov"}],
        "outbound": {"request": ["prov.api@1"], "limits": {
            "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
            "max_calls_global": 64, "max_seen_requests": 64,
            "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
        "execution": {"kind": "process", "entrypoint": entry, "args": args},
    })
}

fn ctrl_service_with(home: &std::path::Path, manifest: &Value) -> Arc<Service> {
    let s = Service::open(
        home,
        HostPolicy {
            secure: true,
            components: [("cons".into(), None)].into(),
            enable_dependency_calls: true,
            domain: "test".into(),
        },
        [(
            "test-operator".into(),
            Grant {
                components: ["cons".into()].into(),
                capabilities: ["cons.chain@1".into()].into(),
            },
        )]
        .into(),
        HashMap::new(),
    )
    .unwrap();
    s.provision(manifest).unwrap();
    s.provision_remote(&json!({
        "id": "prov", "capabilities": ["prov.api@1"], "remote": true,
    }))
    .unwrap();
    s.sync_outbound_grants(&[("cons".to_string(), vec!["prov.api@1".to_string()])].into());
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

struct Rig {
    ctrl: Arc<Service>,
    exec: Arc<Service>,
    mgr: RouteManager,
    _sess: matrix_runtime::remote_session_server::RemoteSessionServer,
    _mgmt: matrix_runtime::remote::Server,
    ctok: String,
    cfence: u64,
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

fn rig() -> Rig {
    let entry = dep_bin();
    let exec_home = home();
    let ctrl_home = home();
    let ctrl_fp = peer("client");
    rig_with(
        &exec_home,
        &ctrl_home,
        &ctrl_fp,
        &rust_prov_manifest(&entry, &[]),
        &cons_manifest(&entry, &[]),
    )
}

fn rig_with(
    exec_home: &std::path::Path,
    ctrl_home: &std::path::Path,
    ctrl_fp: &str,
    prov_manifest: &Value,
    cons_manifest: &Value,
) -> Rig {
    let exec = exec_service_with(exec_home, ctrl_fp, prov_manifest);
    let ctrl = ctrl_service_with(ctrl_home, cons_manifest);
    let sess = session_server(&exec);
    let sess_addr = sess.address;
    let mgmt = mgmt_server(&exec);
    let mgmt_addr = mgmt.address;
    let mgr = RouteManager::new(ctrl.clone(), ctrl_fp.to_string());
    mgr.sync(
        vec![peer_config("exec-A", sess_addr, mgmt_addr, "client")],
        vec![RouteSpec { consumer: "cons".into(), provider: "prov".into(), peer: "exec-A".into() }],
    );
    wait_for("registration", Duration::from_secs(20), || {
        ctrl.kernel.remote_provider_of("prov").is_some()
    });
    // Executor provider process comes up via the route's unary activate.
    wait_for("exec prov session", Duration::from_secs(20), || {
        exec.kernel
            .instance_of("prov")
            .is_some_and(|r| exec.host.has_session("prov", r.0))
    });
    // Consumer activates AFTER registration so the activate carries rb- bindings.
    let a = ctrl.activate("test-operator", "cons", 20000).unwrap();
    let ctok = a["lease"].as_str().unwrap().to_string();
    let cfence: u64 = a["fence"].as_str().unwrap().parse().unwrap();
    let r = ctrl.kernel.instance_ref_of("cons").unwrap();
    wait_for("ctrl cons session", Duration::from_secs(20), || {
        ctrl.host.has_session("cons", r.instance)
    });
    // Bindings arrived with the post-registration activate.
    assert!(
        !ctrl.kernel.dependency_bindings_of("cons").is_empty(),
        "cons has remote bindings"
    );
    // Wire the real transport into the controller host dispatch.
    use matrix_host::RemoteTransport;
    ctrl.host.set_remote_transport(Some(
        Arc::new(mgr.clone()) as Arc<dyn RemoteTransport>
    ));
    Rig { ctrl, exec, mgr, _sess: sess, _mgmt: mgmt, ctok, cfence }
}

fn invoke_cons(rig: &Rig, op: &str, input: Value) -> Value {
    rig.ctrl
        .invoke("test-operator", &rig.ctok, rig.cfence, op, "cons.chain@1", &input)
        .unwrap_or_else(|e| json!({"ok": false, "invoke_error": e}))
}

#[test]
fn e2e_remote_chain_happy_path() {
    let rig = rig();
    let v = invoke_cons(&rig, "op-e2e-ok", json!({"chain": true, "input": {"value": 42}}));
    assert_eq!(v["ok"], true, "chain admits and dispatches: {v}");
    assert_eq!(v["value"]["chained"]["echo"]["value"], 42, "business payload round-trips: {v}");
    assert_eq!(v["value"]["chained"]["via"], "prov");
    assert_eq!(v["value"]["via"], "cons");
    // Parent/child correlation survives the hop (inspect carries the leg).
    wait_for("no residue", Duration::from_secs(10), || {
        rig.ctrl.kernel.pending_calls().is_empty()
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

#[test]
fn e2e_remote_chain_python_provider() {
    // Same business behavior across languages (C24 parity): Rust consumer
    // chains to a Python provider over the same route.
    let exec_home = home();
    let ctrl_home = home();
    let ctrl_fp = peer("client");
    let rig = rig_with(&exec_home, &ctrl_home, &ctrl_fp, &python_prov_manifest(), &cons_manifest(&dep_bin(), &[]));
    let v = invoke_cons(&rig, "op-e2e-py", json!({"chain": true, "input": {"value": 7}}));
    assert_eq!(v["ok"], true, "cross-language chain: {v}");
    assert_eq!(v["value"]["chained"]["echo"]["value"], 7);
    assert_eq!(v["value"]["chained"]["via"], "prov");
    assert_eq!(v["value"]["via"], "cons");
    wait_for("no residue", Duration::from_secs(10), || {
        rig.ctrl.kernel.pending_calls().is_empty()
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

#[test]
fn e2e_remote_cancel_no_phantom_ok() {
    let rig = rig();
    let rig_ref = &rig;
    // Slow provider (5s); the operator thread withdraws mid-call.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(move || {
            let v = invoke_cons(
                rig_ref,
                "op-e2e-cancel",
                json!({"chain": true, "input": {"sleep_ms": 5000}, "timeout_ms": 15000}),
            );
            let _ = tx.send(v);
        });
        std::thread::sleep(Duration::from_millis(600));
        // Withdrawing the consumer revokes participation locally and
        // forwards cancel over the route; the terminal must not be ok.
        rig.ctrl.kernel.dispose_plugin("cons");
        let v = rx.recv_timeout(Duration::from_secs(20)).expect("terminal");
        assert_eq!(v["ok"], false, "cancelled, never false ok: {v}");
        let code = v["value"]["code"].as_str().unwrap_or("");
        assert!(
            matches!(code, "cancelled" | "outcome-unknown" | "deadline-exceeded"),
            "terminal is cancel-like, got: {v}"
        );
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

#[test]
fn e2e_route_loss_fails_closed() {
    let rig = rig();
    let rig_ref = &rig;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(move || {
            let v = invoke_cons(
                rig_ref,
                "op-e2e-loss",
                json!({"chain": true, "input": {"sleep_ms": 5000}, "timeout_ms": 15000}),
            );
            let _ = tx.send(v);
        });
        std::thread::sleep(Duration::from_millis(600));
        // Tearing down the route mid-call: in-flight leg fails closed.
        rig.mgr.shutdown();
        let v = rx.recv_timeout(Duration::from_secs(20)).expect("terminal");
        assert_eq!(v["ok"], false, "route loss never reports false ok: {v}");
    });
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

fn stream_log_lines(path: &std::path::Path, stream_id: &str) -> Vec<(u64, usize)> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let mut out = vec![];
    for line in text.lines() {
        let mut parts = line.split('\t');
        let (Some(id), Some(seq), Some(len)) = (parts.next(), parts.next(), parts.next()) else { continue };
        if id != stream_id {
            continue;
        }
        if let (Ok(seq), Ok(len)) = (seq.parse(), len.parse()) {
            out.push((seq, len));
        }
    }
    out.sort();
    out
}

#[test]
fn e2e_bidi_streams_sdk_to_sdk() {
    // Integrated stream legs (R02): the consumer SDK streams up while its
    // remote child is in flight (host auto-binds the id to that leg), the
    // executor terminates the chunks into the provider SDK, and the
    // provider streams down the same id (tap → controller → consumer SDK).
    // Neither component implements transport, routing or recovery.
    let entry = dep_bin();
    let exec_home = home();
    let ctrl_home = home();
    let ctrl_fp = peer("client");
    let prov_log = exec_home.join("prov-streams.log");
    let cons_log = ctrl_home.join("cons-streams.log");
    let prov_log_s = prov_log.to_string_lossy().to_string();
    let cons_log_s = cons_log.to_string_lossy().to_string();
    let rig = rig_with(
        &exec_home,
        &ctrl_home,
        &ctrl_fp,
        &rust_prov_manifest(&entry, &["--stream-log".to_string(), prov_log_s]),
        &cons_manifest(&entry, &["--stream-log".to_string(), cons_log_s]),
    );
    let v = invoke_cons(
        &rig,
        "op-e2e-bidi",
        json!({"chain_with_streams": {
            "stream_id": "remote/bidi", "chunks": 6, "chunk_bytes": 12,
            "interval_ms": 20, "prime_ms": 50,
            "input": {"sleep_ms": 2000, "stream_send": {"stream_id": "remote/bidi", "chunks": 5, "chunk_bytes": 10}},
            "timeout_ms": 15000,
        }}),
    );
    assert_eq!(v["ok"], true, "chain with concurrent streams: {v}");
    assert_eq!(v["value"]["stream_sent"], 6);
    assert_eq!(v["value"]["chained"]["via"], "prov");
    // Up: consumer SDK → controller host → executor → provider SDK log.
    wait_for("provider received up chunks", Duration::from_secs(15), || {
        stream_log_lines(&prov_log, "remote/bidi").len() >= 6
    });
    let up = stream_log_lines(&prov_log, "remote/bidi");
    assert_eq!(up.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5], "{up:?}");
    assert!(up.iter().all(|(_, n)| *n == 12), "{up:?}");
    // Down: provider SDK → executor tap → controller → consumer SDK log.
    wait_for("consumer received down chunks", Duration::from_secs(15), || {
        stream_log_lines(&cons_log, "remote/bidi").len() >= 5
    });
    let down = stream_log_lines(&cons_log, "remote/bidi");
    assert_eq!(down.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4], "{down:?}");
    assert!(down.iter().all(|(_, n)| *n == 10), "{down:?}");
    wait_for("no residue", Duration::from_secs(10), || {
        rig.ctrl.kernel.pending_calls().is_empty()
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

#[test]
fn e2e_bidi_streams_python_consumer() {
    // Same integrated legs from the Python SDK (C24 parity): Python cons
    // streams up while chaining to the Rust prov, which streams down.
    let entry = dep_bin();
    let exec_home = home();
    let ctrl_home = home();
    let ctrl_fp = peer("client");
    let prov_log = exec_home.join("prov-streams.log");
    let cons_log = ctrl_home.join("cons-streams.log");
    let prov_log_s = prov_log.to_string_lossy().to_string();
    let cons_log_s = cons_log.to_string_lossy().to_string();
    let rig = rig_with(
        &exec_home,
        &ctrl_home,
        &ctrl_fp,
        &rust_prov_manifest(&entry, &["--stream-log".to_string(), prov_log_s]),
        &python_cons_manifest(&["--stream-log".to_string(), cons_log_s]),
    );
    let v = invoke_cons(
        &rig,
        "op-e2e-bidi-py",
        json!({"chain_with_streams": {
            "stream_id": "remote/bidi-py", "chunks": 4, "chunk_bytes": 8,
            "interval_ms": 20, "prime_ms": 50,
            "input": {"sleep_ms": 2000, "stream_send": {"stream_id": "remote/bidi-py", "chunks": 3, "chunk_bytes": 6}},
            "timeout_ms": 15000,
        }}),
    );
    assert_eq!(v["ok"], true, "python chain with concurrent streams: {v}");
    assert_eq!(v["value"]["stream_sent"], 4);
    wait_for("provider received up chunks", Duration::from_secs(15), || {
        stream_log_lines(&prov_log, "remote/bidi-py").len() >= 4
    });
    assert_eq!(
        stream_log_lines(&prov_log, "remote/bidi-py").iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    wait_for("consumer received down chunks", Duration::from_secs(15), || {
        stream_log_lines(&cons_log, "remote/bidi-py").len() >= 3
    });
    assert_eq!(
        stream_log_lines(&cons_log, "remote/bidi-py").iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    wait_for("no residue", Duration::from_secs(10), || {
        rig.ctrl.kernel.pending_calls().is_empty()
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

#[test]
fn reconcile_revoked_leg_cancels_fail_fast() {
    // A leg the executor reports revoked settles without waiting for its
    // deadline: the mapping (ticket ↔ operation) drives a normal cancel
    // (forwarded, no phantom terminal).
    let rig = rig();
    let rig_ref = &rig;
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(move || {
            let v = invoke_cons(
                rig_ref,
                "op-e2e-revoked",
                json!({"chain": true, "input": {"sleep_ms": 10000}, "timeout_ms": 20000}),
            );
            let _ = tx.send(v);
        });
        // Wait until the leg is dispatched (operation correlated host-side).
        let op = wait_for_op(&rig, Duration::from_secs(15));
        // Bind a stream leg to the doomed operation: revoking must end it
        // too (tombstone, verifiable), not just the call ticket.
        let cinst = rig.ctrl.kernel.instance_ref_of("cons").unwrap().instance;
        let csid = rig.ctrl.host.session_id_of("cons", cinst).expect("cons session");
        rig.ctrl
            .host
            .bind_remote_stream(&csid, "remote/doomed", "exec-A", &op, 65536)
            .expect("bind doomed leg");
        assert!(rig.ctrl.host.remote_streams_for("cons", cinst).iter().any(|(id, _, _)| id == "remote/doomed"));
        // The executor never saw this operation: cancelling by its id must
        // settle exactly this leg.
        assert_eq!(rig.mgr.cancel_revoked_operations("exec-A", &["no-such-op".to_string()]), 0);
        assert!(rig.ctrl.host.remote_streams_for("cons", cinst).iter().any(|(id, _, _)| id == "remote/doomed"), "unknown ids end nothing");
        assert_eq!(rig.mgr.cancel_revoked_operations("exec-A", &[op]), 1);
        assert!(
            !rig.ctrl.host.remote_streams_for("cons", cinst).iter().any(|(id, _, _)| id == "remote/doomed"),
            "revoked operation ends its stream legs"
        );
        let v = rx.recv_timeout(Duration::from_secs(20)).expect("terminal");
        assert_eq!(v["ok"], false, "revoked leg never reports ok: {v}");
        let code = v["value"]["code"].as_str().unwrap_or("");
        assert!(
            matches!(code, "cancelled" | "outcome-unknown" | "deadline-exceeded"),
            "terminal is cancel-like, got: {v}"
        );
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}

/// Finds the single in-flight remote leg's stable operation id.
fn wait_for_op(rig: &Rig, timeout: Duration) -> String {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        let kids: Vec<_> = rig
            .ctrl
            .kernel
            .pending_calls()
            .into_iter()
            .filter(|t| t.dep.as_ref().is_some_and(|d| d.remote_peer.as_deref() == Some("exec-A")))
            .collect();
        if kids.len() == 1 {
            if let Some((_, op)) = rig.ctrl.host.remote_op_of(kids[0].id) {
                return op;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timeout waiting for the remote leg");
}

#[test]
fn reconnect_with_pending_leg_settles_unknown_and_reconciles() {
    // Pending cross-host resource across reconnect (R05/R07): a slow leg
    // is in flight while the route drops and re-attaches. The wait settles
    // `unknown` (never a phantom ok), the new session reconciles before
    // publishing (new session id, registration refreshed), and the ledger
    // keeps no false durable result for the orphaned operation.
    let rig = rig();
    let rig_ref = &rig;
    let sess_before = rig.mgr.inspect()["peers"][0]["session"]["id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    assert!(!sess_before.is_empty());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(move || {
            let v = invoke_cons(
                rig_ref,
                "op-e2e-reconnect",
                json!({"chain": true, "input": {"sleep_ms": 8000}, "timeout_ms": 12000}),
            );
            let _ = tx.send(v);
        });
        let op = wait_for_op(&rig, Duration::from_secs(15));
        // Wait for executor-side admission too: tearing down before the
        // open frame flushes would leave no ledger entry at all (a
        // different, vacuous scenario). The pending resource under test
        // is the admitted-but-unresolved ledger entry.
        wait_for("executor admitted", Duration::from_secs(15), || {
            rig.exec.store.operation(&peer("client"), &op).is_ok()
        });
        // Drop the route (sessions, registration, lease) and re-attach
        // with the same operator config: a new session reconciles first.
        rig.mgr.sync(vec![], vec![]);
        wait_for("registration withdrawn", Duration::from_secs(15), || {
            rig.ctrl.kernel.remote_provider_of("prov").is_none()
        });
        let p = certificates();
        let sess_addr = rig._sess.address;
        let mgmt_addr = rig._mgmt.address;
        rig.mgr.sync(
            vec![PeerConfig {
                name: "exec-A".into(),
                address: sess_addr,
                server_name: "localhost".into(),
                ca: p.join("ca.der"),
                cert: p.join("client.der"),
                key: p.join("client-key.der"),
                mgmt_address: mgmt_addr,
                domain: "test".into(),
                lease_ttl_ms: 8000,
            }],
            vec![RouteSpec { consumer: "cons".into(), provider: "prov".into(), peer: "exec-A".into() }],
        );
        wait_for("registration refreshed", Duration::from_secs(25), || {
            rig.ctrl.kernel.remote_provider_of("prov").is_some()
        });
        let sess_after = rig.mgr.inspect()["peers"][0]["session"]["id"]
            .as_str()
            .unwrap_or("")
            .to_string();
        assert_ne!(sess_before, sess_after, "reconnect never reuses the session id");
        // The orphaned wait settles unknown (its answers died with the old
        // session); the ledger holds no false durable result.
        let v = rx.recv_timeout(Duration::from_secs(25)).expect("terminal");
        assert_eq!(v["ok"], false, "orphaned leg never reports ok: {v}");
        // The executor side cannot have persisted success either: its
        // lease died with the teardown, so the run downgrades to unknown.
        wait_for("ledger settles unknown", Duration::from_secs(20), || {
            rig.exec
                .store
                .operation(&peer("client"), &op)
                .is_ok_and(|q| q["state"] == "unknown")
        });
        let q = rig.exec.store.operation(&peer("client"), &op).unwrap();
        assert_ne!(q["state"], "completed", "no phantom durable result: {q}");
    });
    rig.mgr.shutdown();
    rig.ctrl.shutdown();
    rig.exec.shutdown();
}
