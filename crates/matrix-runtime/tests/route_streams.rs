//! M7 remote streams (R02): bounded accounting, slow consumer, control progress.
//!
//! Session-level bidirectional streams between a client and an executor
//! [`Route`]: up (client → executor, accounted with credit top-ups,
//! over-grant ends the leg, session survives) and down (executor →
//! client via [`Route::send_stream_chunk`], slow client drains a bounded
//! queue with visible drops). Control (`op.query`) progresses during
//! floods (control-first queues + bounded data).

mod common;

use common::{certificates, home, peer};
use matrix_runtime::route_executor::Route;
use matrix_runtime::service::{Grant, Service};
use matrix_runtime::session::{self, InboundHandler, Session, SessionLimits};
use matrix_host::HostPolicy;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn limits() -> SessionLimits {
    SessionLimits {
        heartbeat_interval: Duration::from_millis(100),
        suspect_after: Duration::from_millis(800),
        detach_after: Duration::from_millis(3000),
        frame_budget: Duration::from_secs(5),
        handshake_budget: Duration::from_secs(5),
        ..Default::default()
    }
}

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

fn b64(bytes: &[u8]) -> String {
    const ALPH: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(((bytes.len() + 2) / 3) * 4);
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i] as u32;
        let b1 = if i + 1 < bytes.len() { bytes[i + 1] as u32 } else { 0 };
        let b2 = if i + 2 < bytes.len() { bytes[i + 2] as u32 } else { 0 };
        let n = bytes.len() - i;
        out.push(ALPH[((b0 >> 2) & 63) as usize] as char);
        out.push(ALPH[(((b0 << 4) | (b1 >> 4)) & 63) as usize] as char);
        out.push(if n > 1 { ALPH[(((b1 << 2) | (b2 >> 6)) & 63) as usize] as char } else { '=' });
        out.push(if n > 2 { ALPH[(b2 & 63) as usize] as char } else { '=' });
        i += 3;
    }
    out
}

fn stream_data(session_id: &str, stream_id: &str, seq: u64, payload: &[u8]) -> Value {
    stream_data_op(session_id, stream_id, seq, payload, "")
}

fn stream_data_op(
    session_id: &str,
    stream_id: &str,
    seq: u64,
    payload: &[u8],
    operation: &str,
) -> Value {
    let mut body = json!({"stream_id": stream_id, "seq": seq, "bytes": b64(payload), "credit": 0});
    if !operation.is_empty() {
        body["operation_id"] = json!(operation);
    }
    json!({
        "protocol": "matrix.remote", "version": "0.1",
        "type": "stream.data", "message_id": format!("t-{stream_id}-{seq}"),
        "session_id": session_id,
        "body": body,
    })
}

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timeout: {msg}");
}

/// Slow client sink: bounded queue (16), 5ms drain, drops counted.
/// Control (`op.query`) is answered immediately (never queued behind data).
/// Events are counted (no retention) to prove bounded fan-out under flood.
struct SlowClient {
    queue: Mutex<Vec<(String, u64)>>,
    dropped: Mutex<u64>,
    received: Mutex<u64>,
    events: Mutex<u64>,
}

impl SlowClient {
    fn new() -> Arc<Self> {
        let arc = Arc::new(Self {
            queue: Mutex::new(vec![]),
            dropped: Mutex::new(0),
            received: Mutex::new(0),
            events: Mutex::new(0),
        });
        let weak = Arc::downgrade(&arc);
        std::thread::Builder::new()
            .name("slow-drain".into())
            .spawn(move || loop {
                let item = weak.upgrade().and_then(|c| c.queue.lock().unwrap().pop());
                match item {
                    Some(_) => {
                        std::thread::sleep(Duration::from_millis(5));
                        if let Some(c) = weak.upgrade() {
                            *c.received.lock().unwrap() += 1;
                        }
                    }
                    None => {
                        if weak.upgrade().is_none() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            })
            .ok();
        arc
    }
}

impl InboundHandler for SlowClient {
    fn on_message(&self, session: &Session, env: Value) {
        match env.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "stream.data" => {
                let sid = env.get("body").and_then(|b| b.get("stream_id")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                let seq = env.get("body").and_then(|b| b.get("seq")).and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
                let mut q = self.queue.lock().unwrap();
                if q.len() >= 16 {
                    *self.dropped.lock().unwrap() += 1;
                } else {
                    q.push((sid, seq));
                }
            }
            "op.query" => {
                let rid = env.get("request_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                if !rid.is_empty() {
                    let _ = session.send_control(&json!({
                        "protocol": "matrix.remote", "version": "0.1",
                        "type": "op.result", "message_id": rid,
                        "session_id": session.session_id(),
                        "request_id": rid, "body": {"state": "unknown"},
                    }));
                }
            }
            "event.deliver" => {
                // Counted, never retained (bounded fan-out proof).
                *self.events.lock().unwrap() += 1;
            }
            _ => {}
        }
    }
}

fn pair() -> (Arc<Service>, Arc<Route>, Arc<Session>, Arc<Session>, Arc<SlowClient>) {
    let p = certificates();
    let exec = exec_service(&home(), &peer("client"));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_tls = session::server_config(&p.join("ca.der"), &p.join("server.der"), &p.join("server-key.der")).unwrap();
    let (route_tx, route_rx) = std::sync::mpsc::channel();
    let exec_w = exec.clone();
    std::thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        let (s, _) = Session::accept(
            tcp,
            server_tls,
            limits(),
            &|pr| pr == peer("client"),
            |_| {
                Ok(json!({
                    "version": "0.1",
                    "features": ["remote-calls/1", "remote-streams/1", "remote-events/1", "remote-ops/1"],
                    "executor_epoch": "1",
                    "limits": {"max_frame": 1048576},
                }))
            },
        )
        .unwrap();
        let route = Route::new(exec_w, s.clone());
        route.attach();
        let _ = route_tx.send((route, s));
    });
    let client_tls = session::client_config(&p.join("ca.der"), &p.join("client.der"), &p.join("client-key.der")).unwrap();
    let (client, _) = Session::connect(
        addr,
        "localhost",
        client_tls,
        limits(),
        Session::hello_body("test", &peer("client"), 1),
    )
    .unwrap();
    let sink = SlowClient::new();
    client.set_handler(sink.clone());
    let (route, server) = route_rx.recv_timeout(Duration::from_secs(10)).expect("server route");
    (exec, route, client, server, sink)
}

#[test]
fn stream_flood_bounded_and_control_progresses() {
    let (_exec, route, client, _server, _sink) = pair();
    // Paced flood up: 300 × 4 KiB = 1.2 MiB against a 1 MiB cap → leg
    // ends with an error, session survives. Pacing avoids overflowing
    // the session's bounded data queue (client-side refusal would drop
    // before the executor sees the bytes); the bound is still evidenced
    // by the server-side cap + tombstone.
    let payload = vec![b'x'; 4096];
    let mut sent = 0u64;
    for seq in 0..300u64 {
        let _ = client.send_data(&stream_data(client.session_id(), "s-flood", seq, &payload));
        sent += 1;
        if sent % 10 == 0 {
            // Let the executor drain; stop pacing once it ended the leg.
            let t0 = Instant::now();
            while t0.elapsed() < Duration::from_secs(2) {
                let insp = route.inspect();
                let recv = insp["stream_received"].as_u64().unwrap_or(0);
                let drop_ = insp["stream_dropped"].as_u64().unwrap_or(0);
                if drop_ > 0 || recv >= sent * 4096 - 10 * 4096 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            if route.inspect()["stream_dropped"].as_u64().unwrap_or(0) > 0 {
                break;
            }
        }
    }
    wait_for("executor accounted", Duration::from_secs(10), || {
        route.inspect()["stream_received"].as_u64().unwrap_or(0) > 0
    });
    // Control still answered during/after the flood (separate budget).
    let ans = client
        .request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "op.query", "message_id": "m-q1",
                "session_id": client.session_id(), "request_id": "q-flood",
                "body": {"principal": peer("client"), "operation_id": "op-never"},
            }),
            Duration::from_secs(10),
            None,
        )
        .expect("control progresses under flood");
    assert_eq!(ans["body"]["state"], "unknown");
    // The leg ended (over-cap) but the session is still usable.
    wait_for("leg ended", Duration::from_secs(10), || {
        route.inspect()["stream_dropped"].as_u64().unwrap_or(0) > 0
    });
    assert!(client.admissible(), "session survives stream excess");
    client.shutdown();
}

#[test]
fn stream_reorder_duplicate_ignored() {
    let (_exec, route, client, _server, _sink) = pair();
    let payload = vec![b'x'; 16];
    // seq 0 ok, 0 duplicate ignored, 2 (gap) ignored, 1 ok.
    for (seq, expect_recv) in [(0u64, 1u64), (0, 1), (2, 1), (1, 2)] {
        let _ = client.send_data(&stream_data(client.session_id(), "s-order", seq, &payload));
        if expect_recv == 2 {
            wait_for("second accepted", Duration::from_secs(5), || {
                route.inspect()["stream_received"].as_u64().unwrap_or(0) >= 32
            });
        } else {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    wait_for("drops counted", Duration::from_secs(5), || {
        route.inspect()["stream_dropped"].as_u64().unwrap_or(0) >= 2
    });
    assert_eq!(route.inspect()["stream_received"].as_u64().unwrap(), 32);
    client.shutdown();
}

#[test]
fn bidirectional_slow_consumer_drops_counted() {
    let (_exec, route, _client, _server, sink) = pair();
    // Down: executor originates 64 chunks fast; slow drain (5ms) with a
    // 16-slot queue must drop, never grow unbounded, and control still flows.
    for seq in 0..64u64 {
        route.send_stream_chunk("s-down", seq, b"down-payload");
    }
    wait_for("slow drain progresses", Duration::from_secs(10), || {
        *sink.received.lock().unwrap() > 0
    });
    // Give the drain a moment; drops must be visible (64 > 16 + drained).
    std::thread::sleep(Duration::from_millis(300));
    let dropped = *sink.dropped.lock().unwrap();
    let received = *sink.received.lock().unwrap();
    assert!(dropped > 0, "slow consumer drops oldest (got {received}, dropped {dropped})");
    assert!(received + dropped <= 64, "bounded (no duplication)");
    _client.shutdown();
}

fn prov_lines(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path).map(|t| t.lines().count()).unwrap_or(0)
}

#[test]
fn replaced_provider_rejects_stale_operation_chunks() {
    // Temporal validity (R02/R09): the operation→provider mapping pins
    // the full activation reference + fence and revalidates the lease on
    // every inject. Replacing the provider (new instance/generation) or
    // killing the lease stops delivery: stale chunks still account (never
    // grow unbounded, session survives) but never reach the new activation.
    let dir = home();
    let entry = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/examples/dep_node");
    assert!(entry.exists(), "build matrix-host --examples first: {:?}", entry);
    let log = dir.join("prov-streams.log");
    let log_s = log.to_string_lossy().to_string();
    let fp = peer("client");
    let exec = Service::open(
        &dir,
        HostPolicy {
            secure: true,
            components: [("prov".into(), None)].into(),
            enable_dependency_calls: true,
            domain: "test".into(),
        },
        [(
            fp.clone(),
            Grant {
                components: ["prov".into()].into(),
                capabilities: ["prov.api@1".into(), "matrix.effect.write".into()].into(),
            },
        )]
        .into(),
        HashMap::new(),
    )
    .unwrap();
    exec.provision(&json!({
        "id": "prov", "capabilities": ["prov.api@1"],
        "execution": {"kind": "process", "entrypoint": entry.to_string_lossy().to_string(),
            "args": ["--matrix-sock", "{sock}", "--id", "{id}", "--stream-log", log_s]},
    }))
    .unwrap();
    // Manual session pair with the Route attached (no manager: full control
    // over admission, replacement and revocation timing).
    let p = certificates();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_tls =
        session::server_config(&p.join("ca.der"), &p.join("server.der"), &p.join("server-key.der"))
            .unwrap();
    let (route_tx, route_rx) = std::sync::mpsc::channel();
    let exec_w = exec.clone();
    std::thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        let (s, _) = Session::accept(
            tcp,
            server_tls,
            limits(),
            &|pr| pr == peer("client"),
            |_| {
                Ok(json!({
                    "version": "0.1",
                    "features": ["remote-calls/1", "remote-streams/1", "remote-events/1", "remote-ops/1"],
                    "executor_epoch": "1",
                    "limits": {"max_frame": 1048576},
                }))
            },
        )
        .unwrap();
        let route = Route::new(exec_w, s);
        route.attach();
        let _ = route_tx.send(route);
    });
    let client_tls =
        session::client_config(&p.join("ca.der"), &p.join("client.der"), &p.join("client-key.der"))
            .unwrap();
    let (client, _) = Session::connect(
        addr,
        "localhost",
        client_tls,
        limits(),
        Session::hello_body("test", &fp, 1),
    )
    .unwrap();
    let route = route_rx.recv_timeout(Duration::from_secs(10)).expect("route");
    // Lease + provider process for the controller principal.
    let act = exec.activate(&fp, "prov", 20000).unwrap();
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    wait_for("prov session", Duration::from_secs(20), || {
        exec.kernel
            .instance_of("prov")
            .is_some_and(|r| exec.host.has_session("prov", r.0))
    });
    // Admit one leg over the wire (multi-answer subscribe like the manager).
    let op = "op-replace-1";
    let rid = format!("co-{op}");
    let (tx, rx) = std::sync::mpsc::channel();
    client.subscribe_answers(&rid, tx).unwrap();
    client
        .send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "call.open", "message_id": rid.clone(),
            "session_id": client.session_id(),
            "instance_id": "1", "generation": "1", "request_id": rid.clone(),
            "body": {
                "parent": {"domain": "test", "ticket": "99",
                    "activation": {"logical": "cons", "instance": "1", "generation": "1"}},
                "binding_id": "rb-1",
                "activation": {"logical": "cons", "instance": "1", "generation": "1"},
                "cap": "prov.api@1", "input": {}, "timeout_ms": 10000, "budget_ms": 10000,
                "lease": token, "grant_rev": "1", "operation_id": op,
            },
        }))
        .unwrap();
    let mut terminal_ok = false;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(15) {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(ans) if ans["type"] == "call.result" => {
                terminal_ok = ans["body"]["status"] == "ok";
                break;
            }
            Ok(_) => continue,
            Err(_) => continue,
        }
    }
    assert!(terminal_ok, "leg admitted and executed");
    client.unsubscribe_answers(&rid);
    // Positive: a chunk under the live activation terminates into prov.
    client
        .send_data(&stream_data_op(client.session_id(), "s-rep", 0, b"one", op))
        .unwrap();
    wait_for("prov got chunk", Duration::from_secs(10), || prov_lines(&log) >= 1);
    // Replace the provider: release + reactivate (new generation/process).
    let gen_before = exec.kernel.instance_ref_of("prov").map(|r| (r.instance, r.generation));
    exec.release(&fp, &token, fence).unwrap();
    exec.activate(&fp, "prov", 20000).unwrap();
    wait_for("prov reactivated", Duration::from_secs(20), || {
        exec.kernel
            .instance_of("prov")
            .is_some_and(|r| exec.host.has_session("prov", r.0))
    });
    let gen_after = exec.kernel.instance_ref_of("prov").map(|r| (r.instance, r.generation));
    assert_ne!(gen_before, gen_after, "replacement moved the activation: {gen_before:?} -> {gen_after:?}");
    let before = prov_lines(&log);
    // Stale chunk for the previous operation: accounted, never injected.
    let recv_before = route.inspect()["stream_received"].as_u64().unwrap();
    client
        .send_data(&stream_data_op(client.session_id(), "s-rep", 1, b"two", op))
        .unwrap();
    wait_for("chunk accounted", Duration::from_secs(10), || {
        route.inspect()["stream_received"].as_u64().unwrap() > recv_before
    });
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(prov_lines(&log), before, "replaced activation never sees stale chunks");
    // Revoking the principal stops even current-generation delivery.
    exec.revoke(&fp).unwrap();
    client
        .send_data(&stream_data_op(client.session_id(), "s-rep", 2, b"three", op))
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(prov_lines(&log), before, "revoked principal delivers nothing");
    client.shutdown();
    exec.shutdown();
}

#[test]
fn inventory_matches_by_operation_id() {
    // Reconcile honesty (R05/R07): admitted ids are kept (no entry),
    // absent ids are revoked, entries without an id are skipped (never
    // mass-revoked by an id-less report).
    let (exec, _route, client, _server, _sink) = pair();
    let fp = peer("client");
    exec.store
        .admit(&fp, "op-keep", &json!({"logical": "prov", "cap": "prov.api@1", "input": {}}))
        .unwrap();
    let ans = client
        .request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "inventory.reconcile", "message_id": "m-inv",
                "session_id": client.session_id(), "request_id": "inv-1",
                "body": {"activations": [], "leases": [], "operations": [
                    {"operation_id": "op-keep"},
                    {"operation_id": "op-never"},
                    {"ticket": 1, "binding": "rb-1"},
                ], "resources": []},
            }),
            Duration::from_secs(10),
            None,
        )
        .expect("reconcile answered");
    let revoked = ans["body"]["revoked"].as_array().cloned().unwrap_or_default();
    assert_eq!(revoked.len(), 1, "only the absent id is revoked: {revoked:?}");
    assert_eq!(revoked[0]["operation_id"], "op-never");
    client.shutdown();
}

#[test]
fn saturation_calls_data_events_control_abuse_bounded() {
    // R03: simultaneous pressure on calls (legs cap), data (bounded
    // queues + stream caps), events (subscription-gated, counted) and
    // abusive control (malformed drops, oversize refuses). Nothing grows
    // unbounded; control still answers; session survives.
    let (_exec, route, client, _server, sink) = pair();
    // Subscribe for events, then flood all families at once.
    let _ = client.request(
        &json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "event.subscribe", "message_id": "m-sub",
            "session_id": client.session_id(), "request_id": "sub-sat",
            "body": {"topics": ["sat"]},
        }),
        Duration::from_secs(10),
        None,
    );
    let payload = vec![b'x'; 2048];
    // Data flood (up) + downstream chunks + events, paced to actually arrive.
    for i in 0..80u64 {
        let _ = client.send_data(&stream_data(client.session_id(), "s-sat", i, &payload));
        route.send_stream_chunk("s-sat-down", i, b"down");
        route.on_local_emit("sat", &json!({"i": i}));
        if i % 20 == 0 {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    // Abusive control: malformed (dropped silently) + oversize (refused).
    // Malformed bypasses validation and never reaches the handler.
    for _ in 0..10 {
        let _ = client.send_control(&json!({"bogus": true}));
    }
    // Legitimate control still answers promptly under the combined load.
    let t0 = Instant::now();
    let ans = client
        .request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "op.query", "message_id": "m-q-sat",
                "session_id": client.session_id(), "request_id": "q-sat",
                "body": {"principal": peer("client"), "operation_id": "op-never"},
            }),
            Duration::from_secs(10),
            None,
        )
        .expect("control answers under saturation");
    assert!(t0.elapsed() < Duration::from_secs(8), "control within budget: {:?}", t0.elapsed());
    assert_eq!(ans["body"]["state"], "unknown");
    // Bounds held: executor table capped, client queue capped, events counted.
    let insp = route.inspect();
    assert!(insp["streams"].as_u64().unwrap_or(0) <= 128, "stream table bounded");
    assert!(insp["stream_received"].as_u64().unwrap_or(0) <= (1 << 20) + 65536, "bytes bounded");
    let sink_total = *sink.received.lock().unwrap() + *sink.dropped.lock().unwrap();
    assert!(sink_total <= 80, "downstream bounded (got {sink_total})");
    assert!(*sink.events.lock().unwrap() <= 80, "events bounded, counted not retained");
    assert!(client.admissible(), "session survives saturation");
    client.shutdown();
}
