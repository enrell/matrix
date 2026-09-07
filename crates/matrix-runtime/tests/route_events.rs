//! M7 remote events (R11): subscription-gated forwarding with loss counting.
//!
//! Executor [`Route`] forwards only subscribed topics (best-effort,
//! per-route delivered/dropped counters); unsubscribed topics never
//! cross the wire. Controller-side fan-out reuses the local
//! `deliver_remote_event` path (topic match at bind, per-subscriber
//! egress quota, current-activation stamping) — covered by M6 fan-out
//! tests + the SDK flood/drop suites in both languages.

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

struct Rec {
    events: Mutex<Vec<(String, Value)>>,
}

impl InboundHandler for Rec {
    fn on_message(&self, _session: &Session, env: Value) {
        if env.get("type").and_then(|v| v.as_str()) == Some("event.deliver") {
            let topic = env["body"]["topic"].as_str().unwrap_or("").to_string();
            let payload = env["body"]["payload"].clone();
            self.events.lock().unwrap().push((topic, payload));
        }
    }
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

fn pair() -> (Arc<Route>, Arc<Session>, Arc<Rec>) {
    let p = certificates();
    let exec = exec_service(&home(), &peer("client"));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_tls = session::server_config(&p.join("ca.der"), &p.join("server.der"), &p.join("server-key.der")).unwrap();
    let (route_tx, route_rx) = std::sync::mpsc::channel();
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
        let route = Route::new(exec, s);
        route.attach();
        let _ = route_tx.send(route);
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
    let rec = Arc::new(Rec { events: Mutex::new(vec![]) });
    client.set_handler(rec.clone());
    let route = route_rx.recv_timeout(Duration::from_secs(10)).expect("route");
    (route, client, rec)
}

fn subscribe(client: &Session, topics: Vec<&str>) -> Value {
    let rid = format!("sub-{}", topics.join(","));
    client
        .request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "event.subscribe", "message_id": rid.clone(),
                "session_id": client.session_id(), "request_id": rid,
                "body": {"topics": topics},
            }),
            Duration::from_secs(10),
            None,
        )
        .expect("subscribe answered")
}

#[test]
fn subscribed_events_forward_unsubscribed_do_not() {
    let (route, client, rec) = pair();
    // No subscription yet: emissions never cross.
    route.on_local_emit("t", &json!({"n": 1}));
    std::thread::sleep(Duration::from_millis(200));
    assert!(rec.events.lock().unwrap().is_empty(), "unsubscribed drops before the wire");
    // Subscribe: ack carries the effective set, delivery starts.
    let ans = subscribe(&client, vec!["t"]);
    assert_eq!(ans["body"]["topics"], json!(["t"]));
    route.on_local_emit("t", &json!({"n": 2}));
    wait_for("delivered", Duration::from_secs(5), || !rec.events.lock().unwrap().is_empty());
    assert_eq!(rec.events.lock().unwrap()[0].1["n"], 2);
    // Other topics still gated.
    route.on_local_emit("other", &json!({"n": 3}));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(rec.events.lock().unwrap().len(), 1);
    // Unsubscribe: delivery stops, loss is observable in inspect.
    let ans = subscribe(&client, vec![]);
    assert_eq!(ans["body"]["topics"], json!([]));
    let delivered_before = route.inspect()["delivered"].as_u64().unwrap();
    route.on_local_emit("t", &json!({"n": 4}));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(rec.events.lock().unwrap().len(), 1, "withdrawn subscription gets nothing new");
    assert_eq!(route.inspect()["delivered"].as_u64().unwrap(), delivered_before);
    client.shutdown();
}
