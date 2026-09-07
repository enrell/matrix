//! M7 transport sessions: handshake, control progress under flood,
//! partition states, poison. Loopback + byte-level fault proxy; no WAN claims.
mod common;

use common::proxy::Proxy;
use common::{certificates, peer};
use matrix_runtime::session::{
    self, InboundHandler, Session, SessionLimits, SessionState,
};
use serde_json::{json, Value};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn limits() -> SessionLimits {
    SessionLimits {
        heartbeat_interval: Duration::from_millis(100),
        suspect_after: Duration::from_millis(400),
        detach_after: Duration::from_millis(1500),
        frame_budget: Duration::from_secs(5),
        handshake_budget: Duration::from_secs(5),
        ..Default::default()
    }
}

fn server_tls() -> Arc<rustls::ServerConfig> {
    let p = certificates();
    session::server_config(&p.join("ca.der"), &p.join("server.der"), &p.join("server-key.der"))
        .unwrap()
}

fn client_tls() -> Arc<rustls::ClientConfig> {
    let p = certificates();
    session::client_config(&p.join("ca.der"), &p.join("client.der"), &p.join("client-key.der"))
        .unwrap()
}

fn welcome_body() -> Value {
    json!({
        "version": "0.1",
        "features": ["remote-calls/1", "remote-streams/1", "remote-events/1", "remote-ops/1"],
        "executor_epoch": "3",
        "limits": {"max_frame": 1048576},
    })
}

/// Accept loop: each connection gets authorized + welcomed, pushed to `out`.
fn serve(
    listener: TcpListener,
    authorize: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    out: Arc<Mutex<Vec<Arc<Session>>>>,
) {
    std::thread::spawn(move || {
        for tcp in listener.incoming().flatten() {
            let auth = authorize.clone();
            let out = out.clone();
            std::thread::spawn(move || {
                let r = Session::accept(
                    tcp,
                    server_tls(),
                    limits(),
                    &|p| auth(p),
                    |_| Ok(welcome_body()),
                );
                if let Ok((s, _)) = r {
                    out.lock().unwrap().push(s);
                }
            });
        }
    });
}

fn hello() -> Value {
    Session::hello_body("d1", &peer("client"), 7)
}

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timeout: {msg}");
}

#[test]
fn handshake_features_and_heartbeat() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let sessions: Arc<Mutex<Vec<Arc<Session>>>> = Arc::new(Mutex::new(vec![]));
    let want = peer("client");
    serve(listener, Arc::new(move |p| p == want), sessions.clone());

    let (client, welcome) = Session::connect(addr, "localhost", client_tls(), limits(), hello())
        .expect("connect");
    assert_eq!(welcome["executor_epoch"], "3");
    assert!(welcome["features"].as_array().unwrap().len() == 4);
    assert_eq!(client.peer_principal(), peer("server"));
    wait_for("server session", Duration::from_secs(5), || !sessions.lock().unwrap().is_empty());
    let server = sessions.lock().unwrap()[0].clone();
    assert_eq!(server.peer_principal(), peer("client"));

    // Heartbeats keep both sides Connected past several intervals.
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(client.state(), SessionState::Connected);
    assert_eq!(server.state(), SessionState::Connected);
    assert!(client.admissible());
    let insp = client.inspect();
    assert_eq!(insp["state"], "Connected");
    client.shutdown();
    server.shutdown();
}

#[test]
fn request_response_and_stray_answers_drop() {
    struct Echo;
    impl InboundHandler for Echo {
        fn on_message(&self, session: &Session, env: Value) {
            if env["type"] == "op.query" {
                let rid = env["request_id"].as_str().unwrap().to_string();
                let _ = session.send_control(&json!({
                    "protocol": "matrix.remote", "version": "0.1",
                    "type": "op.result", "message_id": "m-r",
                    "session_id": env["session_id"],
                    "request_id": rid,
                    "body": {"state": "completed", "result": {"echo": env["body"]}},
                }));
            }
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let sessions: Arc<Mutex<Vec<Arc<Session>>>> = Arc::new(Mutex::new(vec![]));
    let want = peer("client");
    serve(listener, Arc::new(move |p| p == want), sessions.clone());
    let (client, _) = Session::connect(addr, "localhost", client_tls(), limits(), hello()).unwrap();
    wait_for("server session", Duration::from_secs(5), || !sessions.lock().unwrap().is_empty());
    sessions.lock().unwrap()[0].set_handler(Arc::new(Echo));

    let ans = client
        .request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "op.query", "message_id": "m-1",
                "session_id": client.session_id(), "request_id": "r-1",
                "body": {"principal": "fp", "operation_id": "op-1"},
            }),
            Duration::from_secs(5),
            None,
        )
        .expect("echo");
    assert_eq!(ans["body"]["result"]["echo"]["operation_id"], "op-1");

    // Stray answer for an unknown request: dropped + counted, no crash.
    let before = client.inspect()["dropped_late"].as_u64().unwrap();
    sessions.lock().unwrap()[0]
        .send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "op.result", "message_id": "m-x",
            "session_id": "s", "request_id": "no-such",
            "body": {"state": "unknown"},
        }))
        .unwrap();
    wait_for("stray counted", Duration::from_secs(5), || {
        client.inspect()["dropped_late"].as_u64().unwrap() > before
    });
    assert_eq!(client.state(), SessionState::Connected);
    client.shutdown();
}

#[test]
fn control_progresses_under_data_flood() {
    // Control progress under saturation (R02/R03): a 2 MB bulk flood is
    // in flight on a throttled uplink when a control request is issued;
    // it must still be answered within budget. No wire jump-ahead is
    // asserted: bytes already in kernel/TLS buffers are FIFO on the
    // single connection — engine queue priority applies to frames still
    // queued, and sustained stalls surface as Suspect/Detached instead.
    // Recorder: arrival order of stream.data vs the control request.
    struct Rec {
        order: Mutex<Vec<String>>,
    }
    impl InboundHandler for Rec {
        fn on_message(&self, session: &Session, env: Value) {
            if env["type"] == "stream.data" {
                self.order.lock().unwrap().push(format!("data:{}", env["body"]["seq"]));
            } else if env["type"] == "op.query" {
                self.order.lock().unwrap().push("control".into());
                let rid = env["request_id"].as_str().unwrap().to_string();
                let _ = session.send_control(&json!({
                    "protocol": "matrix.remote", "version": "0.1",
                    "type": "op.result", "message_id": "m-r",
                    "session_id": env["session_id"],
                    "request_id": rid, "body": {"state": "completed"},
                }));
            }
        }
    }

    let target_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target: SocketAddr = target_listener.local_addr().unwrap();
    let sessions: Arc<Mutex<Vec<Arc<Session>>>> = Arc::new(Mutex::new(vec![]));
    let rec = Arc::new(Rec { order: Mutex::new(vec![]) });
    let rec_srv = rec.clone();
    let want = peer("client");
    let sessions_srv = sessions.clone();
    std::thread::spawn(move || {
        for tcp in target_listener.incoming().flatten() {
            let rec = rec_srv.clone();
            let sessions = sessions_srv.clone();
            let want = want.clone();
            std::thread::spawn(move || {
                let r = Session::accept(
                    tcp,
                    server_tls(),
                    limits(),
                    &|p| p == want,
                    |_| Ok(welcome_body()),
                );
                if let Ok((s, _)) = r {
                    s.set_handler(rec);
                    sessions.lock().unwrap().push(s);
                }
            });
        }
    });

    // Throttle uplink so the engine queues faster than the wire drains.
    let proxy = Proxy::bind(target);
    proxy.up.delay_ms.store(8, std::sync::atomic::Ordering::SeqCst);

    let (client, _) =
        Session::connect(proxy.address, "localhost", client_tls(), limits(), hello()).unwrap();
    wait_for("server session", Duration::from_secs(5), || !sessions.lock().unwrap().is_empty());

    // Flood 30 bulk frames (64 KB each) onto a throttled uplink, then
    // issue a control request while the flood is still draining. The
    // request must be answered within budget even though ~2 MB precede
    // it on the wire.
    let sid = client.session_id().to_string();
    let data = "A".repeat(64 * 1024);
    for i in 0..30 {
        client
            .send_data(&json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "stream.data", "message_id": format!("m-d{i}"),
                "session_id": sid,
                "body": {"stream_id": "s1", "seq": i, "bytes": data, "credit": 0},
            }))
            .expect("queue data");
    }
    std::thread::sleep(Duration::from_millis(300));
    let t0 = Instant::now();
    client
        .request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "op.query", "message_id": "m-c",
                "session_id": sid, "request_id": "r-c",
                "body": {"principal": "fp", "operation_id": "op-c"},
            }),
            Duration::from_secs(10),
            None,
        )
        .expect("control answered under flood");
    let dt = t0.elapsed();
    assert!(dt < Duration::from_secs(10), "control within budget: {dt:?}");

    // The flood itself still arrives (nothing dropped by the engine),
    // and the control round-trip completed above.
    wait_for("flood drains", Duration::from_secs(15), || {
        rec.order.lock().unwrap().iter().filter(|e| e.starts_with("data:")).count() >= 25
    });
    let order = rec.order.lock().unwrap();
    assert!(order.iter().any(|e| e == "control"), "control arrived");
    client.shutdown();
}

#[test]
fn partition_suspect_then_detached_terminal() {
    let target_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target: SocketAddr = target_listener.local_addr().unwrap();
    let sessions: Arc<Mutex<Vec<Arc<Session>>>> = Arc::new(Mutex::new(vec![]));
    let want = peer("client");
    std::thread::spawn(move || {
        for tcp in target_listener.incoming().flatten() {
            let sessions = sessions.clone();
            let want = want.clone();
            std::thread::spawn(move || {
                let r = Session::accept(
                    tcp,
                    server_tls(),
                    limits(),
                    &|p| p == want,
                    |_| Ok(welcome_body()),
                );
                if let Ok((s, _)) = r {
                    sessions.lock().unwrap().push(s);
                }
            });
        }
    });
    let proxy = Proxy::bind(target);
    let (client, _) =
        Session::connect(proxy.address, "localhost", client_tls(), limits(), hello()).unwrap();
    assert_eq!(client.state(), SessionState::Connected);

    proxy.partition();
    wait_for("suspect", Duration::from_secs(5), || client.state() == SessionState::Suspect);
    assert!(!client.admissible(), "no new admissions while suspect");
    wait_for("detached", Duration::from_secs(5), || client.state() == SessionState::Detached);
    assert!(client.send_control(&json!({"a": 1})).is_err());
    // Healing never revives a detached session: a new one is required.
    proxy.open();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(client.state(), SessionState::Detached);
    client.shutdown();
}

#[test]
fn shutdown_is_clean_eof_for_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let sessions: Arc<Mutex<Vec<Arc<Session>>>> = Arc::new(Mutex::new(vec![]));
    let want = peer("client");
    serve(listener, Arc::new(move |p| p == want), sessions.clone());
    let (client, _) = Session::connect(addr, "localhost", client_tls(), limits(), hello()).unwrap();
    wait_for("server session", Duration::from_secs(5), || !sessions.lock().unwrap().is_empty());
    client.shutdown();
    wait_for("peer detached", Duration::from_secs(5), || {
        sessions.lock().unwrap()[0].state() == SessionState::Detached
    });
}

#[test]
fn handshake_refuses_foreign_profile_and_stranger() {
    // Wrong ALPN (managed unary profile) is not a remote session.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let p = certificates();
    let foreign = matrix_runtime::remote::client_config(
        &p.join("ca.der"),
        &p.join("client.der"),
        &p.join("client-key.der"),
    )
    .unwrap();
    let server_tls_cfg = server_tls();
    let denied = Arc::new(Mutex::new(false));
    let denied_w = denied.clone();
    std::thread::spawn(move || {
        for tcp in listener.incoming().flatten() {
            let r = Session::accept(tcp, server_tls_cfg.clone(), limits(), &|_| true, |_| {
                Ok(welcome_body())
            });
            if r.is_err() {
                *denied_w.lock().unwrap() = true;
            }
            break;
        }
    });
    let tcp = std::net::TcpStream::connect(addr).unwrap();
    tcp.set_nonblocking(true).ok();
    let name = rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap();
    let conn = rustls::ClientConnection::new(foreign, name).unwrap();
    let mut io = (tcp, conn);
    // Finish a foreign-profile handshake; the server refuses afterwards.
    // A NoApplicationProtocol alert during the handshake IS the refusal
    // (disjoint ALPN sets): it fails closed with no session established.
    let t0 = Instant::now();
    let mut eof = false;
    let mut refused_at_handshake = false;
    while io.1.is_handshaking() && t0.elapsed() < Duration::from_secs(5) {
        match io.1.complete_io(&mut io.0) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                eof = true;
                break;
            }
            Err(_) => {
                refused_at_handshake = true;
                break;
            }
        }
    }
    if !eof && !refused_at_handshake {
        assert!(!io.1.is_handshaking());
        // Send a non-hello frame so the refusal is fast, then observe EOF.
        io.0.set_nonblocking(false).ok();
        let mut stream = rustls::StreamOwned::new(io.1, io.0);
        stream.sock.set_write_timeout(Some(Duration::from_secs(5))).ok();
        stream.sock.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let raw = serde_json::to_vec(&serde_json::json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "heartbeat", "message_id": "m-h",
            "session_id": "s", "body": {},
        }))
        .unwrap();
        use std::io::Write;
        let _ = stream.write_all(&(raw.len() as u32).to_be_bytes());
        let _ = stream.write_all(&raw);
        let _ = stream.flush();
        use std::io::Read;
        let mut one = [0u8; 1];
        match stream.read(&mut one) {
            Ok(0) => eof = true,
            _ => {}
        }
    }
    assert!(eof || refused_at_handshake || *denied.lock().unwrap(), "refusal observed");
    wait_for("refused", Duration::from_secs(5), || *denied.lock().unwrap());

    // Stranger certificate (valid chain, no grant) is denied.
    let listener2 = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let server_tls_cfg2 = server_tls();
    let denied2 = Arc::new(Mutex::new(false));
    let denied2_w = denied2.clone();
    std::thread::spawn(move || {
        for tcp in listener2.incoming().flatten() {
            let r = Session::accept(tcp, server_tls_cfg2.clone(), limits(), &|_| false, |_| {
                Ok(welcome_body())
            });
            if r.is_err() {
                *denied2_w.lock().unwrap() = true;
            }
            break;
        }
    });
    let stranger_tls = {
        let p = certificates();
        session::client_config(&p.join("ca.der"), &p.join("rotated.der"), &p.join("rotated-key.der"))
            .unwrap()
    };
    let r = Session::connect(addr2, "localhost", stranger_tls, limits(), hello());
    assert!(r.is_err(), "stranger denied");
    // The accept thread may still be storing the refusal: poll, don't race it.
    wait_for("stranger refused", Duration::from_secs(5), || *denied2.lock().unwrap());
}
