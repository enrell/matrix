//! M2.3 conformance: flow and communication failures.
//!
//! - Finite bounds (I09): oversize frames drop the session without
//!   dropping the host; simultaneous per-session calls are capped
//!   (`resource-exhausted`, no unbounded queue).
//! - Credit streams: data past the grant ends the stream with
//!   an error; the session survives.
//! - Cancellation reaches the plugin (observable) and late work never commits.
//! - Disconnect: `outcome-unknown`, exactly one opening (no retry).

use matrix_core::{Journal, Kernel};
use matrix_host::Host;
use matrix_proto::{
    encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn ext_bin() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for profile in [if cfg!(debug_assertions) { "debug" } else { "release" }] {
        let p = manifest.join("../../target").join(profile).join("examples/ext_echo");
        if p.exists() {
            return p;
        }
    }
    panic!("ext_echo not compiled");
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-flow-{}-{}-{}",
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

fn write_manifest(home: &PathBuf, id: &str, body: Value) -> PathBuf {
    let p = home.join("plugins").join(format!("{}.json", id));
    std::fs::write(&p, serde_json::to_string_pretty(&body).unwrap()).unwrap();
    p
}

fn noop_manifest(id: &str, caps: &[&str]) -> Value {
    json!({
        "id": id, "version": "1.0.0", "capabilities": caps, "subscriptions": [],
        "reducer": "noop", "init_state": {}, "tier": "inproc",
        "trust": "trusted", "restart": "permanent",
    })
}

fn ext_manifest(bin: &std::path::Path, id: &str, cap: &str) -> Value {
    json!({
        "id": id, "version": "1.0.0", "capabilities": [cap], "subscriptions": [],
        "reducer": "external", "init_state": {}, "tier": "process",
        "trust": "trusted", "restart": "permanent",
        "execution": {"kind": "process",
            "entrypoint": bin.to_string_lossy().to_string(),
            "args": ["--matrix-sock", "{sock}", "--id", "{id}"],
            "timeout_ms": 8000},
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
    panic!("timeout aguardando: {}", msg);
}

fn code_of(v: &Value) -> String {
    v.get("code").and_then(|c| c.as_str()).unwrap_or("").to_string()
}

struct Rig {
    kernel: Arc<Kernel>,
    host: Arc<Host>,
    home: PathBuf,
}

fn rig_ext(tag: &str, id: &str, cap: &str) -> Rig {
    let bin = ext_bin();
    let home = fresh_home(tag);
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    let m = write_manifest(&home, id, ext_manifest(&bin, id, cap));
    kernel.load_manifest(&m).unwrap();
    let host = Host::attach(kernel.clone(), &home.join("host")).expect("attach");
    Rig { kernel, host, home }
}

// ---- driver cru: fala o protocolo sem o SDK ----

struct RawSession {
    stream: UnixStream,
    session_id: String,
    max_frame: usize,
}

fn raw_register(sock: &PathBuf, logical: &str) -> RawSession {
    let mut stream = UnixStream::connect(sock).expect("connect host");
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let raw = serde_json::to_vec(v).unwrap();
        let f = encode(&raw, DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw"},
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let welcome = parse_frame_payload(&raw).unwrap();
    assert_eq!(welcome.ty, "welcome");
    let session_id = welcome.session_id.clone().unwrap();
    let max_frame = welcome.body.get("max_frame").and_then(|v| v.as_u64()).unwrap_or(65536) as usize;
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": session_id,
        "body": {"manifest": {"id": logical}},
    }));
    let raw = read_frame(&mut stream, max_frame).unwrap().unwrap();
    let reg = parse_frame_payload(&raw).unwrap();
    assert_eq!(reg.ty, "registered", "registro cru: {:?}", reg.body);
    // Responde o activate do host (com prazo).
    let raw = read_frame(&mut stream, max_frame).unwrap().unwrap();
    let act = parse_frame_payload(&raw).unwrap();
    assert_eq!(act.ty, "lifecycle.activate");
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": act.instance_id, "generation": act.generation,
        "request_id": act.request_id,
        "body": {"operation_id": act.body.get("operation_id").cloned().unwrap_or(json!("op")),
                 "status": "ok", "pending": []},
    }));
    RawSession { stream, session_id, max_frame }
}

fn raw_heartbeat(rs: &mut RawSession) -> Value {
    let raw = serde_json::to_vec(&json!({
        "protocol": "matrix.component", "version": "0.1", "type": "session.heartbeat",
        "message_id": format!("hb-{}", Instant::now().elapsed().as_nanos()),
        "session_id": rs.session_id, "body": {},
    }))
    .unwrap();
    let f = encode(&raw, rs.max_frame).unwrap();
    rs.stream.write_all(&f).unwrap();
    rs.stream.flush().unwrap();
    let raw = read_frame(&mut rs.stream, rs.max_frame).unwrap().expect("resposta heartbeat");
    parse_frame_payload(&raw).unwrap().body
}

// ---- limites ----

#[test]
fn oversize_frame_drops_session_host_survives() {
    let bin = ext_bin();
    let home = fresh_home("oversize");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    // In-process logical just to bind the raw session.
    let m = write_manifest(&home, "dummy", noop_manifest("dummy", &["dummy.cap@1"]));
    kernel.load_manifest(&m).unwrap();
    let host = Host::attach(kernel.clone(), &home.join("host")).expect("attach");
    let _ = bin;

    let sock = host.sock_path();
    let mut rs = raw_register(&sock, "dummy");
    // Declares 2 MiB: rejected without allocating, session drops.
    let mut evil = ((2 * 1024 * 1024) as u32).to_be_bytes().to_vec();
    evil.extend_from_slice(b"{}");
    rs.stream.write_all(&evil).unwrap();
    rs.stream.flush().unwrap();
    wait_for("dropped session", Duration::from_secs(5), || host.session_count() == 0);
    // Host keeps operating: a new session registers and answers.
    let mut rs2 = raw_register(&sock, "dummy");
    let _ = raw_heartbeat(&mut rs2);
    assert_eq!(host.session_count(), 1);
    host.shutdown();
}

#[test]
fn unknown_request_id_dropped_session_alive() {
    let home = fresh_home("stale-req");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    let m = write_manifest(&home, "dummy", noop_manifest("dummy", &["dummy.cap@1"]));
    kernel.load_manifest(&m).unwrap();
    let host = Host::attach(kernel.clone(), &home.join("host")).expect("attach");
    let sock = host.sock_path();
    let mut rs = raw_register(&sock, "dummy");
    // Unknown-request_id result: dropped, no answer, no crash.
    let raw = serde_json::to_vec(&json!({
        "protocol": "matrix.component", "version": "0.1", "type": "call.result",
        "message_id": "bogus-1", "session_id": rs.session_id,
        "instance_id": "1", "generation": "1", "request_id": "req-never-issued",
        "body": {"ticket": "tkt-1", "status": "ok", "output": {}},
    }))
    .unwrap();
    let f = encode(&raw, rs.max_frame).unwrap();
    rs.stream.write_all(&f).unwrap();
    rs.stream.flush().unwrap();
    rs.stream.set_read_timeout(Some(Duration::from_millis(300))).ok();
    let mut one = [0u8; 1];
    assert!(rs.stream.read(&mut one).is_err(), "nada responde ao tardio");
    // Live session: heartbeat answers.
    rs.stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let _ = raw_heartbeat(&mut rs);
    host.shutdown();
}

#[test]
fn per_session_call_cap_rejects_fast() {
    use matrix_host::MAX_CALLS_PER_SESSION;
    let r = rig_ext("callcap", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("inst");
    wait_for("session", Duration::from_secs(10), || r.host.has_session("ext", inst.0));
    // Preenche o teto com chamadas dormindo.
    std::thread::scope(|s| {
        let mut hs = vec![];
        for _ in 0..MAX_CALLS_PER_SESSION {
            let k = &r.kernel;
            hs.push(s.spawn(move || {
                let _ = k.invoke("echo.ext@1", &json!({"sleep_ms": 8000}));
            }));
        }
        wait_for("teto atingido", Duration::from_secs(10), || {
            r.host.in_flight_for("ext", inst.0) == MAX_CALLS_PER_SESSION
        });
        // The next one fails fast, no unbounded queue.
        let t0 = Instant::now();
        let (v, ok) = r.kernel.invoke("echo.ext@1", &json!({}));
        assert!(!ok);
        assert_eq!(code_of(&v), "resource-exhausted", "teto: {}", v);
        assert!(t0.elapsed() < Duration::from_secs(2), "fast rejection: {:?}", t0.elapsed());
        // Withdraw releases everything (cooperative cancellation).
        r.kernel.dispose_plugin("ext");
        for h in hs {
            h.join().unwrap();
        }
    });
    assert_eq!(r.host.in_flight_for("ext", inst.0), 0);
    r.host.shutdown();
}

// ---- streams ----

#[test]
fn stream_flood_terminates_stream_session_survives() {
    use matrix_host::STREAM_INITIAL_GRANT;
    let r = rig_ext("flood", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("inst");
    wait_for("session", Duration::from_secs(10), || r.host.has_session("ext", inst.0));
    // 20 × 64 KiB = 1.3 MiB on a 64 KiB-credit stream.
    let (v, ok) = r.kernel.invoke(
        "echo.ext@1",
        &json!({"flood_stream": {"id": "s1", "chunk": 65536, "count": 20}}),
    );
    assert!(ok, "chamada responde: {}", v);
    // Stream ended for excess; session alive and serving.
    wait_for("stream encerrado", Duration::from_secs(5), || {
        r.host.stream_count_for("ext", inst.0) == 0
    });
    assert_eq!(r.host.ended_stream_count_for("ext", inst.0), 1, "excesso registrado");
    assert!(r.host.has_session("ext", inst.0));
    let (v, ok) = r.kernel.invoke("echo.ext@1", &json!({"ping": 1}));
    assert!(ok, "session survives: {}", v);
    assert!(STREAM_INITIAL_GRANT == 65536);
    r.host.shutdown();
}

// ---- cancellation and disconnect ----

#[test]
fn cancel_reaches_plugin_no_late_commit() {
    let r = rig_ext("cancel-prop", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("inst");
    wait_for("session", Duration::from_secs(10), || r.host.has_session("ext", inst.0));
    let mark = r.home.join("cancelled.log");
    let mark_s = mark.to_string_lossy().to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        let k = &r.kernel;
        let ms = mark_s.clone();
        s.spawn(move || {
            // invoke forwards on the wire; mid-flight withdraw cancels.
            let out = k.invoke("echo.ext@1", &json!({"sleep_ms": 30000, "mark_cancel": ms}));
            tx.send(out).unwrap();
        });
        std::thread::sleep(Duration::from_millis(300));
        r.kernel.dispose_plugin("ext");
        let (v, ok) = rx.recv_timeout(Duration::from_secs(10)).expect("fim");
        assert!(!ok, "cancelado, sem sucesso: {}", v);
        assert_eq!(code_of(&v), "cancelled", "code: {}", v);
    });
    // O plugin observou o cancel no fio.
    wait_for("marca de cancel", Duration::from_secs(5), || {
        std::fs::read_to_string(&mark).map(|s| !s.trim().is_empty()).unwrap_or(false)
    });
    r.host.shutdown();
}

#[test]
fn disconnect_single_open_no_retry() {
    let r = rig_ext("no-retry", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("inst");
    wait_for("session", Duration::from_secs(10), || r.host.has_session("ext", inst.0));
    let count = r.home.join("opens.log");
    let count_s = count.to_string_lossy().to_string();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        let k = &r.kernel;
        let cs = count_s.clone();
        s.spawn(move || {
            let out = k.invoke("echo.ext@1", &json!({"sleep_ms": 30000, "count_file": cs}));
            tx.send(out).unwrap();
        });
        std::thread::sleep(Duration::from_millis(300));
        assert!(r.host.kill_child("ext"), "mata o filho");
        let (v, ok) = rx.recv_timeout(Duration::from_secs(10)).expect("resposta");
        assert!(!ok);
        assert_eq!(code_of(&v), "outcome-unknown");
    });
    // Exactly one opening: no automatic retry.
    wait_for("stable count", Duration::from_millis(600), || {
        std::fs::read_to_string(&count).is_ok()
    });
    std::thread::sleep(Duration::from_millis(300));
    let n = std::fs::read_to_string(&count)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(99);
    assert_eq!(n, 1, "uma abertura, sem retry");
    r.kernel.dispose_plugin("ext");
    r.host.shutdown();
}
