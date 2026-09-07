//! Remote-leg dispatch through the real host worker (M7 route A).
//!
//! A stub [`RemoteTransport`] isolates host-side failures (admission,
//! quotas, terminal translation, cancel forwarding, fail-closed routing)
//! from the network. The full path over real sessions is covered by
//! `matrix-runtime/tests/route_integration.rs`; this file proves the
//! host half against a scripted route.

use matrix_core::{CallPolicy, Journal, Kernel};
use matrix_host::{Host, HostPolicy, RemoteCallOpen, RemoteCallTerminal, RemoteTransport};
use matrix_proto::{encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME};
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-remotedispatch-{}-{}-{}",
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

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = std::time::Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timeout waiting: {}", msg);
}

/// Scripted route: records opens/cancels/streams, answers terminals on demand.
struct StubRoute {
    opens: Mutex<Vec<RemoteCallOpen>>,
    cancels: Mutex<Vec<(String, String)>>,
    streams: Mutex<Vec<(String, String, u64, usize)>>,
    ends: Mutex<Vec<(String, String, String)>>,
    /// Terminal to return from `call_open` (checked per call).
    terminal: Mutex<RemoteCallTerminal>,
    /// Block opens until released (to interleave cancels/races).
    gate: Mutex<bool>,
}

impl StubRoute {
    fn new(terminal: RemoteCallTerminal) -> Arc<Self> {
        Arc::new(Self {
            opens: Mutex::new(vec![]),
            cancels: Mutex::new(vec![]),
            streams: Mutex::new(vec![]),
            ends: Mutex::new(vec![]),
            terminal: Mutex::new(terminal),
            gate: Mutex::new(false),
        })
    }

    fn release(&self) {
        *self.gate.lock().unwrap() = true;
    }
}

impl RemoteTransport for StubRoute {
    fn call_open(&self, open: RemoteCallOpen, cancel: &AtomicBool) -> RemoteCallTerminal {
        self.opens.lock().unwrap().push(open);
        while !*self.gate.lock().unwrap() {
            if cancel.load(Ordering::SeqCst) {
                return RemoteCallTerminal::Failed {
                    code: "cancelled".into(),
                    message: "cancelled".into(),
                };
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        self.terminal.lock().unwrap().clone()
    }

    fn call_cancel(&self, peer: &str, operation_id: &str) {
        self.cancels
            .lock()
            .unwrap()
            .push((peer.to_string(), operation_id.to_string()));
    }

    fn topology_changed(&self) {}

    fn stream_data(&self, peer: &str, _operation: &str, stream_id: &str, seq: u64, payload: &str) {
        self.streams.lock().unwrap().push((
            peer.to_string(),
            stream_id.to_string(),
            seq,
            payload.len(),
        ));
    }

    fn stream_end(&self, peer: &str, stream_id: &str, status: &str) {
        self.ends.lock().unwrap().push((
            peer.to_string(),
            stream_id.to_string(),
            status.to_string(),
        ));
    }
}

fn noop_manifest(id: &str, caps: &[&str]) -> Value {
    json!({
        "id": id, "version": "1.0.0", "capabilities": caps, "subscriptions": [],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
    })
}

fn consumer_manifest() -> Value {
    let mut m = json!({
        "id": "cons", "version": "1.0.0", "capabilities": ["cons.chain@1"], "subscriptions": [],
        "requires": [{"interface": "prov.api@1", "provider": "rprov"}],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
    });
    m["outbound"] = json!({"request": ["prov.api@1"], "limits": {
        "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
        "max_calls_global": 64, "max_seen_requests": 64,
        "max_queued_bytes": 65536, "max_deadline_ms": 12000}});
    m
}

fn remote_def() -> Value {
    json!({
        "id": "rprov", "version": "1.0.0", "capabilities": ["prov.api@1"], "subscriptions": [],
        "reducer": "noop", "init_state": {},
        "tier": "inproc", "trust": "trusted", "restart": "permanent",
        "remote": true,
    })
}

/// Kernel + host with a registered remote provider and a stub route.
/// Consumer is raw (no process) so the test drives `dependency.open`.
fn rig(route: Option<Arc<StubRoute>>) -> (Arc<Kernel>, Arc<Host>) {
    let (kernel, host, _) = rig_home(route);
    (kernel, host)
}

/// Same rig, keeping the home dir (tests that reload manifests).
fn rig_home(route: Option<Arc<StubRoute>>) -> (Arc<Kernel>, Arc<Host>, PathBuf) {
    let home = fresh_home("rig");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [noop_manifest("cons", &["cons.chain@1"]), remote_def()] {
        let id = m["id"].as_str().unwrap();
        let mut full = m.clone();
        if id == "cons" {
            full = consumer_manifest();
        }
        let p = home.join("plugins").join(format!("{id}.json"));
        std::fs::write(&p, serde_json::to_string_pretty(&full).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    // Local consumer activation needs a lease-free path: activate via kernel reconcile.
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    policy.domain = "test".to_string();
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    kernel
        .register_remote_provider("rprov", 7, 3, "exec-A")
        .expect("register");
    // Consumer has no process; activate the definition directly is done
    // by reconcile on registration (remote satisfies requires).
    wait_for("cons active", Duration::from_secs(10), || {
        kernel.context_state_of("cons").as_deref() == Some("Active")
    });
    if let Some(r) = route {
        host.set_remote_transport(Some(r as Arc<dyn RemoteTransport>));
    }
    (kernel, host, home)
}

struct RawCons {
    stream: UnixStream,
    sid: String,
    instance: String,
    generation: String,
    binding: String,
}

fn raw_consumer(host: &Host) -> RawCons {
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    send(
        &mut stream,
        &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "hello",
            "message_id": "h1",
            "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw",
                     "features": ["dependency-calls/1"]},
        }),
    );
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let sid = parse_frame_payload(&raw).unwrap().session_id.unwrap();
    send(
        &mut stream,
        &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "component.register",
            "message_id": "reg1", "session_id": sid,
            "body": {"manifest": {"id": "cons"}},
        }),
    );
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let reg = parse_frame_payload(&raw).unwrap();
    assert_eq!(reg.ty, "registered");
    let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let act = parse_frame_payload(&raw).unwrap();
    assert_eq!(act.ty, "lifecycle.activate");
    let binding = act.body["dependency_bindings"][0]["binding_id"].as_str().unwrap().to_string();
    assert!(binding.starts_with("rb-"), "remote handle: {binding}");
    send(
        &mut stream,
        &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
            "message_id": "lc1", "session_id": act.session_id,
            "instance_id": act.instance_id, "generation": act.generation,
            "request_id": act.request_id,
            "body": {"operation_id": "op", "status": "ok", "pending": []},
        }),
    );
    RawCons { stream, sid, instance, generation, binding }
}

fn dep_open(c: &mut RawCons, mid: &str, rid: &str, parent: &str, input: Value) {
    let f = encode(
        &serde_json::to_vec(&json!({
            "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
            "message_id": mid, "session_id": c.sid,
            "instance_id": c.instance, "generation": c.generation,
            "request_id": rid,
            "body": {"parent_ticket": parent, "binding_id": c.binding,
                     "timeout_ms": 8000, "input": input},
        }))
        .unwrap(),
        DEFAULT_MAX_FRAME,
    )
    .unwrap();
    c.stream.write_all(&f).unwrap();
    c.stream.flush().unwrap();
}

fn recv_body(c: &mut RawCons) -> (String, Value) {
    let raw = read_frame(&mut c.stream, DEFAULT_MAX_FRAME)
        .unwrap_or_else(|e| panic!("frame: {e}"))
        .expect("eof");
    let e = parse_frame_payload(&raw).unwrap();
    (e.ty.clone(), serde_json::to_value(e.body).unwrap())
}

fn open_parent(kernel: &Kernel) -> matrix_core::TicketId {
    kernel
        .call_open("cons.chain@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("parent")
        .ticket
}

#[test]
fn remote_ok_terminal_maps() {
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({"echo": 1})));
    route.release();
    let (kernel, host) = rig(Some(route.clone()));
    let mut c = raw_consumer(&host);
    let parent = open_parent(&kernel);
    dep_open(&mut c, "m1", "r1", &parent.0.to_string(), json!({"v": 1}));
    // Normative sequence preserved for remote legs.
    let (ty, _) = recv_body(&mut c);
    assert_eq!(ty, "dependency.accepted");
    let (ty, b) = recv_body(&mut c);
    assert_eq!(ty, "dependency.result", "{b:?}");
    assert_eq!(b["status"], "ok");
    assert_eq!(b["output"]["echo"], 1);
    // The route saw a well-formed open (authority from admission, not wire).
    let opens = route.opens.lock().unwrap();
    assert_eq!(opens.len(), 1);
    let o = &opens[0];
    assert_eq!(o.peer, "exec-A");
    assert!(o.binding_id.starts_with("rb-"));
    assert_eq!(o.cap, "prov.api@1");
    assert_eq!(o.parent_ticket, parent.0);
    assert_eq!(o.consumer_logical, "cons");
    assert!(!o.operation_id.is_empty() && o.operation_id.len() <= 128, "{}", o.operation_id);
    assert_eq!(o.grant_rev, kernel.outbound_grants.lock().get(&("cons".to_string(), "prov.api@1".to_string())).copied().unwrap());
    // Only the test-held parent remains; the child settled in the worker
    // (poll: the worker's close may lag the result frame under load).
    wait_for("child settled", Duration::from_secs(10), || {
        kernel.pending_calls().iter().all(|t| t.id == parent)
    });
    assert!(kernel.call_close(parent));
    wait_for("all settled", Duration::from_secs(10), || kernel.pending_calls().is_empty());
    host.shutdown();
}

#[test]
fn remote_business_error_preserves_code() {
    let route = StubRoute::new(RemoteCallTerminal::Err {
        code: "teapot".into(),
        message: "remote teapot".into(),
    });
    route.release();
    let (kernel, host) = rig(Some(route));
    let mut c = raw_consumer(&host);
    let parent = open_parent(&kernel);
    dep_open(&mut c, "m1", "r1", &parent.0.to_string(), json!({}));
    assert_eq!(recv_body(&mut c).0, "dependency.accepted");
    let (ty, b) = recv_body(&mut c);
    assert_eq!(ty, "dependency.result");
    assert_eq!(b["status"], "error");
    assert_eq!(b["error"]["code"], "teapot");
    assert_eq!(b["error"]["origin"], "exec-A");
    assert!(kernel.call_close(parent));
    host.shutdown();
}

#[test]
fn no_route_fails_closed() {
    let (_kernel, host) = rig(None);
    let mut c = raw_consumer(&host);
    let parent = open_parent(&_kernel);
    dep_open(&mut c, "m1", "r1", &parent.0.to_string(), json!({}));
    assert_eq!(recv_body(&mut c).0, "dependency.accepted");
    let (ty, b) = recv_body(&mut c);
    assert_eq!(ty, "dependency.result");
    assert_eq!(b["status"], "error");
    assert_eq!(b["error"]["code"], "outcome-unknown");
    // Never silently local: no local provider exists and none ran.
    // Only the test-held parent may remain; the child settled (poll: the
    // worker's close may lag the result frame under parallel load).
    wait_for("child settled", Duration::from_secs(10), || {
        _kernel.pending_calls().iter().all(|t| t.id == parent)
    });
    assert!(_kernel.call_close(parent));
    assert!(_kernel.pending_calls().is_empty());
    host.shutdown();
}

#[test]
fn remote_cancel_forwards_operation() {
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({})));
    // Gate held: the cancel lands while the worker is dispatched.
    let (kernel, host) = rig(Some(route.clone()));
    let mut c = raw_consumer(&host);
    let parent = open_parent(&kernel);
    dep_open(&mut c, "m1", "r1", &parent.0.to_string(), json!({}));
    assert_eq!(recv_body(&mut c).0, "dependency.accepted");
    // Wait until the route saw the open, then cancel on the wire.
    wait_for("route saw open", Duration::from_secs(10), || !route.opens.lock().unwrap().is_empty());
    let op = route.opens.lock().unwrap()[0].operation_id.clone();
    let f = encode(
        &serde_json::to_vec(&json!({
            "protocol": "matrix.component", "version": "0.1", "type": "dependency.cancel",
            "message_id": "c1", "session_id": c.sid,
            "instance_id": c.instance, "generation": c.generation,
            "request_id": "c1", "body": {"target_request_id": "r1"},
        }))
        .unwrap(),
        DEFAULT_MAX_FRAME,
    )
    .unwrap();
    c.stream.write_all(&f).unwrap();
    c.stream.flush().unwrap();
    // Terminal answers cancelled; route got the operation-indexed cancel.
    // Two frames max: cancel.result (revoked) + open result (cancelled),
    // either order.
    let mut seen_cancel = false;
    let mut seen_open = false;
    for _ in 0..2 {
        let (ty, b) = recv_body(&mut c);
        if ty == "dependency.cancel.result" {
            assert_eq!(b["state"], "revoked", "{b:?}");
            seen_cancel = true;
        } else if ty == "dependency.result" {
            assert_eq!(b["error"]["code"], "cancelled", "{b:?}");
            seen_open = true;
        }
    }
    assert!(seen_cancel && seen_open, "both answers");
    wait_for("cancel forwarded", Duration::from_secs(10), || {
        route.cancels.lock().unwrap().iter().any(|(p, o)| p == "exec-A" && o == &op)
    });
    route.release();
    assert!(kernel.call_close(parent));
    host.shutdown();
}

#[test]
fn operation_id_stable_per_logical_call() {
    // Identity is deterministic per invocation (domain, consumer, parent,
    // binding, request, input): distinct parents/requests give distinct
    // ids; lengths stay within the 128-char wire cap.
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({})));
    route.release();
    let (kernel, host) = rig(Some(route.clone()));
    let mut c = raw_consumer(&host);
    for (rid, mid) in [("r1", "m1"), ("r2", "m2")] {
        let parent = open_parent(&kernel);
        dep_open(&mut c, mid, rid, &parent.0.to_string(), json!({"v": 1}));
        assert_eq!(recv_body(&mut c).0, "dependency.accepted");
        assert_eq!(recv_body(&mut c).0, "dependency.result");
        assert!(kernel.call_close(parent));
    }
    let opens = route.opens.lock().unwrap();
    assert_eq!(opens.len(), 2);
    assert_ne!(opens[0].operation_id, opens[1].operation_id);
    assert!(opens.iter().all(|o| !o.operation_id.is_empty() && o.operation_id.len() <= 128));
    host.shutdown();
}

#[test]
fn operation_ids_differ_across_controller_boots() {
    // Epoch separation (R08 temporal validity): parent tickets, bindings
    // and component request counters may all repeat after a restart, but
    // the boot epoch never does — so a new call can never replay an old
    // persistent-ledger entry. Two fresh kernels (fresh epochs) minting
    // the identical logical invocation must mint different ids.
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({})));
    route.release();
    let mut ids = vec![];
    for _ in 0..2 {
        let (kernel, host) = rig(Some(route.clone()));
        let mut c = raw_consumer(&host);
        let parent = open_parent(&kernel);
        dep_open(&mut c, "m1", "r1", &parent.0.to_string(), json!({"v": 1}));
        assert_eq!(recv_body(&mut c).0, "dependency.accepted");
        assert_eq!(recv_body(&mut c).0, "dependency.result");
        assert!(kernel.call_close(parent));
        host.shutdown();
    }
    for o in route.opens.lock().unwrap().iter() {
        assert!(o.operation_id.len() <= 128, "wire cap: {}", o.operation_id);
        ids.push(o.operation_id.clone());
    }
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1], "boot epochs discriminate restarts");
}

#[test]
fn inject_pins_exact_activation() {
    // Ownership barrier (R02/R09 temporal validity): delivery resolves
    // the validated InstanceRef in one atomic selection — never the bare
    // logical name. Whatever replaces the provider past that selection
    // cannot steal the chunk: a stale ref refuses once the new activation
    // registers, and a withdrawn session refuses (or still served the old
    // session, linearizing before the withdraw — never the new one).
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({})));
    route.release();
    let (kernel, host, home) = rig_home(Some(route));
    let mut c = raw_consumer(&host);
    let first = kernel.instance_ref_of("cons").expect("active");
    assert!(host.inject_provider_chunk_owned(&first, "s-p", 0, "one"));
    let (ty, b) = recv_body(&mut c);
    assert_eq!(ty, "stream.data", "{b:?}");
    assert_eq!(b["payload"], "one");
    // Replace the activation: withdraw, reload, re-register.
    kernel.dispose_plugin("cons");
    kernel.load_manifest(&home.join("plugins/cons.json")).expect("reload");
    let mut c2 = raw_consumer(&host);
    let second = kernel.instance_ref_of("cons").expect("reactive");
    assert_ne!(
        (first.instance, first.generation),
        (second.instance, second.generation),
        "replacement moved the activation"
    );
    // The stale ref refuses (this is where name resolution would have
    // redirected into the new generation).
    assert!(!host.inject_provider_chunk_owned(&first, "s-p", 1, "stale"));
    // The fresh ref delivers to the new session.
    assert!(host.inject_provider_chunk_owned(&second, "s-p", 1, "fresh"));
    let (ty, b) = recv_body(&mut c2);
    assert_eq!(ty, "stream.data", "{b:?}");
    assert_eq!(b["payload"], "fresh");
    // Withdrawn entirely: dropping both sockets reaps the sessions, and
    // even the fresh ref refuses afterwards.
    drop(c);
    drop(c2);
    wait_for("sessions reaped", Duration::from_secs(10), || host.session_count() == 0);
    assert!(!host.inject_provider_chunk_owned(&second, "s-p", 2, "gone"));
    host.shutdown();
}

#[test]
fn intentional_equal_calls_under_one_parent_both_execute() {
    // Two intentional equal calls (same parent, binding, input) carry
    // different invocation request ids, so the ledger must not suppress
    // the second as a replay: two route opens, two terminals.
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({"n": 1})));
    route.release();
    let (kernel, host) = rig(Some(route.clone()));
    let mut c = raw_consumer(&host);
    let parent = open_parent(&kernel);
    dep_open(&mut c, "m1", "r1", &parent.0.to_string(), json!({"v": 1}));
    dep_open(&mut c, "m2", "r2", &parent.0.to_string(), json!({"v": 1}));
    // Workers run concurrently: collect the four terminals as a set.
    let mut accepted = 0;
    let mut results = 0;
    for _ in 0..4 {
        match recv_body(&mut c).0.as_str() {
            "dependency.accepted" => accepted += 1,
            "dependency.result" => results += 1,
            ty => panic!("unexpected frame: {ty}"),
        }
    }
    assert_eq!((accepted, results), (2, 2));
    let opens = route.opens.lock().unwrap();
    assert_eq!(opens.len(), 2, "both invocations dispatched");
    assert_ne!(opens[0].operation_id, opens[1].operation_id, "invocation identity differs");
    drop(opens);
    assert!(kernel.call_close(parent));
    wait_for("no residue", Duration::from_secs(10), || kernel.pending_calls().is_empty());
    host.shutdown();
}

fn stream_send(c: &mut RawCons, mid: &str, stream_id: &str, seq: u64, payload: &str) {
    let f = encode(
        &serde_json::to_vec(&json!({
            "protocol": "matrix.component", "version": "0.1", "type": "stream.data",
            "message_id": mid, "session_id": c.sid,
            "instance_id": c.instance, "generation": c.generation,
            "body": {"stream_id": stream_id, "seq": seq.to_string(), "payload": payload},
        }))
        .unwrap(),
        DEFAULT_MAX_FRAME,
    )
    .unwrap();
    c.stream.write_all(&f).unwrap();
    c.stream.flush().unwrap();
}

#[test]
fn remote_stream_relay_credit_and_terminal() {
    let route = StubRoute::new(RemoteCallTerminal::Ok(json!({})));
    route.release();
    let (_kernel, host) = rig(Some(route.clone()));
    let mut c = raw_consumer(&host);
    // Upstream leg (component → executor): explicit bind with a 100-byte window.
    host.bind_remote_stream(&c.sid, "s-up", "exec-A", "op-up", 100).unwrap();
    // Duplicate bind refuses (fail closed, no hijack of the leg).
    assert!(host.bind_remote_stream(&c.sid, "s-up", "exec-A", "op-up", 100).is_err());
    stream_send(&mut c, "m-s0", "s-up", 0, &"x".repeat(40));
    wait_for("relayed seq0", Duration::from_secs(5), || {
        route.streams.lock().unwrap().iter().any(|(_, id, seq, _)| id == "s-up" && *seq == 0)
    });
    // Duplicate seq ignored (never re-relayed).
    stream_send(&mut c, "m-s0d", "s-up", 0, &"x".repeat(40));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(route.streams.lock().unwrap().iter().filter(|(_, id, _, _)| id == "s-up").count(), 1);
    // Over-grant (40 + 80 > 100) ends the leg locally with an error and
    // closes the remote side; late frames never resurrect.
    stream_send(&mut c, "m-s1", "s-up", 1, &"x".repeat(80));
    wait_for("remote end on over-credit", Duration::from_secs(5), || {
        route.ends.lock().unwrap().iter().any(|(_, id, st)| id == "s-up" && st == "error")
    });
    stream_send(&mut c, "m-s2", "s-up", 2, "x");
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(route.streams.lock().unwrap().iter().filter(|(_, id, _, _)| id == "s-up").count(), 1);
    // Drain the local over-credit terminal for s-up (session survives).
    let (ty, b) = recv_body(&mut c);
    assert_eq!(ty, "stream.end", "{b:?}");
    assert_eq!(b["stream_id"], "s-up");
    // Credit widens a second leg: 60-byte window, 40 sent, +100 granted, 80 more ok.
    host.bind_remote_stream(&c.sid, "s-up2", "exec-A", "op-up2", 60).unwrap();
    stream_send(&mut c, "m-t0", "s-up2", 0, &"x".repeat(40));
    wait_for("second leg relayed", Duration::from_secs(5), || {
        route.streams.lock().unwrap().iter().any(|(_, id, _, _)| id == "s-up2")
    });
    host.credit_remote_stream_any("s-up2", 100);
    stream_send(&mut c, "m-t1", "s-up2", 1, &"x".repeat(80));
    wait_for("credited send relayed", Duration::from_secs(5), || {
        route.streams.lock().unwrap().iter().filter(|(_, id, _, _)| id == "s-up2").count() == 2
    });
    // Downstream leg (executor → component): bound, then delivered like a
    // local stream (credit window enforced, tombstones retained).
    host.bind_remote_stream(&c.sid, "s-down", "exec-A", "op-down", 50).unwrap();
    assert!(host.deliver_remote_chunk_any("s-down", 0, "hello"));
    let (ty, b) = recv_body(&mut c);
    assert_eq!(ty, "stream.data", "{b:?}");
    assert_eq!(b["stream_id"], "s-down");
    assert_eq!(b["payload"], "hello");
    // Over-credit delivery ends the leg; further chunks drop silently.
    assert!(!host.deliver_remote_chunk_any("s-down", 1, &"y".repeat(100)));
    assert!(!host.deliver_remote_chunk_any("s-down", 2, "late"));
    // Unknown ids never touch the wire nor a session.
    assert!(!host.deliver_remote_chunk_any("s-ghost", 0, "x"));
    host.credit_remote_stream_any("s-ghost", 10);
    host.end_remote_stream_any("s-ghost");
    assert!(route.streams.lock().unwrap().iter().all(|(_, id, _, _)| id != "s-ghost"));
    host.shutdown();
}
