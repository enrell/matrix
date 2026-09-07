//! SDK unit tests over a loopback fake host (M6 closing).
//!
//! - Events never run on the reader: a slow `on_event` does not delay
//!   calls, and floods drop oldest with a visible counter.
//! - Blocking SDK calls made on the reader thread itself fail fast
//!   instead of deadlocking the read that would answer them.
//! - `invoke_dependency` end-to-end against scripted accepted/result.

use matrix_component::{CallCtx, CallOutcome, Component, Handler};
use serde_json::{json, Value};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

static SEQ: AtomicU64 = AtomicU64::new(1);

fn write_frame(s: &mut UnixStream, v: &Value) {
    use matrix_proto::{encode, DEFAULT_MAX_FRAME};
    use std::io::Write;
    let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
    s.write_all(&f).unwrap();
    s.flush().unwrap();
}

fn read_frame(s: &mut UnixStream) -> Value {
    use matrix_proto::{parse_frame_payload, read_frame, DEFAULT_MAX_FRAME};
    let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
    let e = parse_frame_payload(&raw).unwrap();
    json!({"type": e.ty, "body": e.body, "request_id": e.request_id})
}

fn dep_env(ty: &str, body: Value) -> Value {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    json!({
        "protocol": "matrix.component", "version": "0.1", "type": ty,
        "message_id": format!("m{}", n), "session_id": "s1",
        "instance_id": "1", "generation": "1",
        "request_id": format!("r{}", n), "body": body,
    })
}

fn call_open(ticket: &str, input: Value) -> Value {
    json!({
        "protocol": "matrix.component", "version": "0.1", "type": "call.open",
        "message_id": format!("m-{}", ticket), "session_id": "s1",
        "instance_id": "1", "generation": "1",
        "request_id": format!("r-{}", ticket),
        "body": {"ticket": ticket, "capability": "c@1", "input": input},
    })
}

fn dispose() -> Value {
    json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.dispose",
        "message_id": "d1", "session_id": "s1", "instance_id": "1", "generation": "1",
        "request_id": "rd1",
        "body": {"operation_id": "op", "deadline_ms": 100},
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
    panic!("timeout: {}", msg);
}

#[derive(Clone)]
struct Probe {
    events: Arc<Mutex<Vec<(String, Value)>>>,
    streams: Arc<Mutex<Vec<(String, u64, usize)>>>,
    slow_ms: Arc<AtomicU64>,
    stream_slow_ms: Arc<AtomicU64>,
    stashed: Arc<Mutex<Option<CallCtx>>>,
    guard_hits: Arc<Mutex<Vec<String>>>,
}

impl Handler for Probe {
    fn on_call(
        &self,
        ctx: &CallCtx,
        _ticket: &str,
        _cap: &str,
        input: &Value,
        _cancel: &AtomicBool,
    ) -> CallOutcome {
        *self.stashed.lock().unwrap() = Some(ctx.clone());
        if input.get("report_drops").and_then(|v| v.as_bool()).unwrap_or(false) {
            return CallOutcome::Ok(json!({"dropped": ctx.event_dropped_count()}));
        }
        if input.get("chain_it").and_then(|v| v.as_bool()).unwrap_or(false) {
            let b = ctx.dependencies().first().map(|b| b.id.clone()).unwrap_or_default();
            match ctx.invoke_dependency(&b, json!({"v": 1}), Duration::from_secs(5)) {
                Ok(out) => return CallOutcome::Ok(json!({"got": out})),
                Err(e) => {
                    return CallOutcome::Err { code: e.code, message: e.message };
                }
            }
        }
        CallOutcome::Ok(json!({"echo": input}))
    }

    fn on_cancel(&self, _ticket: &str) {
        // Reader-thread context: blocking SDK calls must refuse fast.
        if let Some(ctx) = self.stashed.lock().unwrap().clone() {
            let r = ctx.invoke_dependency("bind-x", json!({}), Duration::from_secs(2));
            self.guard_hits.lock().unwrap().push(format!("dep:{:?}", r.map(|_| ())));
            let r = ctx.acquire_resource("timer", "t", Some(10));
            self.guard_hits.lock().unwrap().push(format!("res:{:?}", r.map(|_| ())));
        }
    }

    fn on_event(&self, topic: &str, payload: &Value) {
        let ms = self.slow_ms.load(Ordering::SeqCst);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
        self.events.lock().unwrap().push((topic.to_string(), payload.clone()));
    }

    fn on_stream(&self, stream_id: &str, seq: u64, payload: &str) {
        let ms = self.stream_slow_ms.load(Ordering::SeqCst);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
        self.streams.lock().unwrap().push((stream_id.to_string(), seq, payload.len()));
    }
}

fn probe() -> (Probe, Arc<Mutex<Vec<(String, Value)>>>, Arc<Mutex<Vec<String>>>) {
    let events = Arc::new(Mutex::new(vec![]));
    let guard_hits = Arc::new(Mutex::new(vec![]));
    (
        Probe {
            events: events.clone(),
            streams: Arc::new(Mutex::new(vec![])),
            slow_ms: Arc::new(AtomicU64::new(0)),
            stream_slow_ms: Arc::new(AtomicU64::new(0)),
            stashed: Arc::new(Mutex::new(None)),
            guard_hits: guard_hits.clone(),
        },
        events,
        guard_hits,
    )
}

fn sock_dir(tag: &str) -> (std::path::PathBuf, UnixListener) {
    let dir = std::env::temp_dir().join(format!(
        "sdkunits-{}-{}-{}",
        tag,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::create_dir_all(&dir);
    let sp = dir.join("t.sock");
    let _ = std::fs::remove_file(&sp);
    let listener = UnixListener::bind(&sp).unwrap();
    (sp, listener)
}

fn handshake(sock: &mut UnixStream, bindings: Value) {
    let _ = read_frame(sock);
    write_frame(
        sock,
        &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "welcome",
            "message_id": "h1", "session_id": "s1",
            "body": {"version": "0.1", "max_frame": 1048576, "limits": {},
                     "features": ["dependency-calls/1"]},
        }),
    );
    let _ = read_frame(sock);
    write_frame(
        sock,
        &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "registered",
            "message_id": "r", "session_id": "s1", "instance_id": "1", "generation": "1",
            "body": {"logical": "t"},
        }),
    );
    let mut act = dep_env(
        "lifecycle.activate",
        json!({"operation_id": "op", "manifest": {}, "bindings": [],
               "dependency_bindings": bindings}),
    );
    act["request_id"] = json!("q");
    write_frame(sock, &act);
    let _ = read_frame(sock);
    sock.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
}

#[test]
fn events_do_not_block_reader() {
    let (sp, listener) = sock_dir("ev");
    let (handler, events, _) = probe();
    handler.slow_ms.store(300, Ordering::SeqCst);
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        handshake(&mut sock, json!([]));
        write_frame(&mut sock, &dep_env("event.deliver", json!({"topic": "t", "payload": {"n": 1}})));
        write_frame(&mut sock, &dep_env("event.deliver", json!({"topic": "t", "payload": {"n": 2}})));
        write_frame(&mut sock, &call_open("tkt-9", json!({})));
        // Prompt answer despite the busy dispatcher.
        let t0 = Instant::now();
        let ans = read_frame(&mut sock);
        assert!(t0.elapsed() < Duration::from_secs(3), "reader independent: {:?}", t0.elapsed());
        assert_eq!(ans["type"], "call.result", "{:?}", ans["type"]);
        wait_for("events drain", Duration::from_secs(10), || events.lock().unwrap().len() == 2);
        let evs = events.lock().unwrap();
        assert_eq!(evs[0].1["n"], 1);
        assert_eq!(evs[1].1["n"], 2);
        write_frame(&mut sock, &dispose());
        let _ = read_frame(&mut sock);
    });
    let comp = Component::connect(&sp, "t").unwrap();
    assert!(comp.negotiated_features().iter().any(|f| f == "dependency-calls/1"));
    let out = comp.serve(handler);
    assert!(out.is_ok(), "dispose ends serve: {:?}", out);
    server.join().unwrap();
}

#[test]
fn event_flood_drops_oldest_and_counts() {
    let (sp, listener) = sock_dir("fl");
    let (handler, _, _) = probe();
    handler.slow_ms.store(30, Ordering::SeqCst);
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        handshake(&mut sock, json!([]));
        // Flood far beyond the 64-slot queue while the dispatcher crawls:
        // overflow is structural, not timing luck.
        for i in 0..120 {
            write_frame(&mut sock, &dep_env("event.deliver", json!({"topic": "t", "payload": {"n": i}})));
        }
        // A call answered while the dispatcher drains proves independence.
        write_frame(&mut sock, &call_open("tkt-9", json!({})));
        let t0 = Instant::now();
        let ans = read_frame(&mut sock);
        assert!(t0.elapsed() < Duration::from_secs(5), "call answered under flood");
        assert_eq!(ans["type"], "call.result");
        // Ask for the drop counter through a call (dispatcher drained by now).
        write_frame(&mut sock, &call_open("tkt-10", json!({"report_drops": true})));
        let ans = read_frame(&mut sock);
        let dropped = ans["body"]["output"]["dropped"].as_u64().unwrap_or(0);
        assert!(dropped >= 1, "overflow counted, got {}", dropped);
        write_frame(&mut sock, &dispose());
        let _ = read_frame(&mut sock);
    });
    let comp = Component::connect(&sp, "t").unwrap();
    let out = comp.serve(handler);
    assert!(out.is_ok(), "dispose ends serve: {:?}", out);
    server.join().unwrap();
}

fn stream_chunk(id: &str, seq: u64, payload: &str) -> Value {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    json!({
        "protocol": "matrix.component", "version": "0.1", "type": "stream.data",
        "message_id": format!("s{n}"), "session_id": "s1",
        "instance_id": "1", "generation": "1",
        "body": {"stream_id": id, "seq": seq.to_string(), "payload": payload},
    })
}

#[test]
fn streams_do_not_block_reader() {
    let (sp, listener) = sock_dir("st");
    let (handler, _, _) = probe();
    handler.stream_slow_ms.store(200, Ordering::SeqCst);
    let streams = handler.streams.clone();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        handshake(&mut sock, json!([]));
        write_frame(&mut sock, &stream_chunk("s1", 0, "hello"));
        write_frame(&mut sock, &stream_chunk("s1", 1, "world"));
        write_frame(&mut sock, &call_open("tkt-9", json!({})));
        // Prompt answer despite the slow stream dispatcher.
        let t0 = Instant::now();
        let ans = read_frame(&mut sock);
        assert!(t0.elapsed() < Duration::from_secs(3), "reader independent: {:?}", t0.elapsed());
        assert_eq!(ans["type"], "call.result", "{:?}", ans["type"]);
        wait_for("streams drain", Duration::from_secs(10), || streams.lock().unwrap().len() == 2);
        let got = streams.lock().unwrap();
        assert_eq!(got[0].1, 0);
        assert_eq!(got[1].1, 1);
        write_frame(&mut sock, &dispose());
        let _ = read_frame(&mut sock);
    });
    let comp = Component::connect(&sp, "t").unwrap();
    let out = comp.serve(handler);
    assert!(out.is_ok(), "dispose ends serve: {:?}", out);
    server.join().unwrap();
}

#[test]
fn stream_flood_drops_oldest_and_counts() {
    let (sp, listener) = sock_dir("sf");
    let (handler, _, _) = probe();
    handler.stream_slow_ms.store(20, Ordering::SeqCst);
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        handshake(&mut sock, json!([]));
        // Flood far beyond the 64-slot shared queue while slow.
        for i in 0..120 {
            write_frame(&mut sock, &stream_chunk("s9", i, "x"));
        }
        write_frame(&mut sock, &call_open("tkt-9", json!({})));
        let t0 = Instant::now();
        let ans = read_frame(&mut sock);
        assert!(t0.elapsed() < Duration::from_secs(5), "call answered under stream flood");
        assert_eq!(ans["type"], "call.result");
        // Drops share the dispatcher counter with events.
        write_frame(&mut sock, &call_open("tkt-10", json!({"report_drops": true})));
        let ans = read_frame(&mut sock);
        let dropped = ans["body"]["output"]["dropped"].as_u64().unwrap_or(0);
        assert!(dropped >= 1, "stream overflow counted, got {}", dropped);
        write_frame(&mut sock, &dispose());
        let _ = read_frame(&mut sock);
    });
    let comp = Component::connect(&sp, "t").unwrap();
    let out = comp.serve(handler);
    assert!(out.is_ok(), "dispose ends serve: {:?}", out);
    server.join().unwrap();
}

#[test]
fn reader_thread_guard_refuses_fast() {
    let (sp, listener) = sock_dir("gd");
    let (handler, _, guard_hits) = probe();
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        handshake(&mut sock, json!([]));
        // Open a call (stashes ctx), then cancel it: on_cancel runs on the
        // reader and must refuse blocking calls immediately.
        write_frame(&mut sock, &call_open("tkt-9", json!({})));
        let _ = read_frame(&mut sock);
        // Wait until the handler stashed the ctx (worker answered already).
        write_frame(
            &mut sock,
            &json!({
                "protocol": "matrix.component", "version": "0.1", "type": "call.cancel",
                "message_id": "c2", "session_id": "s1", "instance_id": "1", "generation": "1",
                "request_id": "rc2",
                "body": {"ticket": "tkt-9", "reason": "test"},
            }),
        );
        write_frame(&mut sock, &dispose());
        let _ = read_frame(&mut sock);
    });
    let comp = Component::connect(&sp, "t").unwrap();
    let t0 = Instant::now();
    let out = comp.serve(handler);
    assert!(out.is_ok(), "dispose ends serve: {:?}", out);
    server.join().unwrap();
    assert!(t0.elapsed() < Duration::from_secs(15));
    let hits = guard_hits.lock().unwrap();
    assert_eq!(hits.len(), 2, "dep + res guard hits: {:?}", *hits);
    assert!(hits.iter().all(|h| h.contains("internal")), "{:?}", *hits);
}

#[test]
fn invoke_dependency_end_to_end() {
    let (sp, listener) = sock_dir("iv");
    let bindings = json!([{"binding_id": "bind-1", "capability": "c@1"}]);
    let server = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
        handshake(&mut sock, bindings);
        // Trigger a chained call.
        let mut open_call = call_open("tkt-9", json!({"chain_it": true}));
        open_call["request_id"] = json!("rc1");
        write_frame(&mut sock, &open_call);
        // Expect dependency.open; answer accepted + ok.
        let open = read_frame(&mut sock);
        assert_eq!(open["type"], "dependency.open", "{:?}", open["type"]);
        assert_eq!(open["body"]["binding_id"], "bind-1");
        assert_eq!(open["body"]["parent_ticket"], "tkt-9");
        let open_rid = open["request_id"].as_str().unwrap().to_string();
        write_frame(&mut sock, &dep_env("dependency.accepted", json!({"child_ticket": "7"})));
        write_frame(
            &mut sock,
            &json!({
                "protocol": "matrix.component", "version": "0.1", "type": "dependency.result",
                "message_id": "mres", "session_id": "s1", "instance_id": "1", "generation": "1",
                "request_id": open_rid,
                "body": {"status": "ok", "output": {"deep": 1}},
            }),
        );
        // Component answers the parent call with the chained output.
        let ans = read_frame(&mut sock);
        assert_eq!(ans["type"], "call.result", "{:?}", ans["type"]);
        assert_eq!(ans["body"]["output"]["got"]["deep"], 1);
        write_frame(&mut sock, &dispose());
        let _ = read_frame(&mut sock);
    });
    let (handler, _, _) = probe();
    let comp = Component::connect(&sp, "t").unwrap();
    let out = comp.serve(handler);
    assert!(out.is_ok(), "dispose ends serve: {:?}", out);
    server.join().unwrap();
}
