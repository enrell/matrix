//! End-to-end local composition (M6 step 3+): Rust/Python chains,
//! withdraw during execution, reintroduction, cancellation, and saturation.
//!
//! Uses generic `dep_node` (Rust) and `dep_node.py` (Python) fixtures:
//! no product application. Critical interleavings use barriers
//! and blocking fixture modes; stress complements, never replaces.

use matrix_core::{CallPolicy, Journal, Kernel};
use matrix_host::{Host, HostPolicy};
use matrix_proto::{encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME};
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn examples() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target").join(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    })
}

fn rust_node() -> (String, Vec<String>) {
    let bin = examples().join("examples/dep_node");
    assert!(bin.exists(), "rust dep_node not compiled: {:?}", bin);
    (
        bin.to_string_lossy().to_string(),
        vec!["--matrix-sock".into(), "{sock}".into(), "--id".into(), "{id}".into()],
    )
}

fn python_node() -> (String, Vec<String>) {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sdk-python/dep_node.py");
    assert!(script.exists(), "sdk-python/dep_node.py ausente: {:?}", script);
    let python = std::env::var("PYTHON3").unwrap_or_else(|_| "python3".to_string());
    (
        python,
        vec![
            script.to_string_lossy().to_string(),
            "--matrix-sock".into(),
            "{sock}".into(),
            "--id".into(),
            "{id}".into(),
        ],
    )
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-depflow-{}-{}-{}",
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

fn ext_manifest(id: &str, caps: &[&str], entry: &(String, Vec<String>)) -> Value {
    json!({
        "id": id, "version": "1.0.0", "capabilities": caps, "subscriptions": [],
        "reducer": "external", "init_state": {}, "tier": "process",
        "trust": "trusted", "restart": "permanent",
        "execution": {"kind": "process", "entrypoint": entry.0,
            "args": entry.1, "timeout_ms": 15000},
    })
}

fn consumer_manifest(entry: &(String, Vec<String>)) -> Value {
    let mut m = ext_manifest("cons", &["cons.chain@1"], entry);
    m["requires"] = json!([{"interface": "prov.api@1", "provider": "prov"}]);
    m["outbound"] = json!({"request": ["prov.api@1"], "limits": {
        "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
        "max_calls_global": 64, "max_seen_requests": 64,
        "max_queued_bytes": 65536, "max_deadline_ms": 12000}});
    m
}

struct Rig {
    kernel: Arc<Kernel>,
    host: Arc<Host>,
}

fn rig(cons_entry: &(String, Vec<String>), prov_entry: &(String, Vec<String>)) -> Rig {
    let home = fresh_home("rig");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [
        ext_manifest("prov", &["prov.api@1"], prov_entry),
        consumer_manifest(cons_entry),
        ext_manifest("indep", &["indep.echo@1"], &rust_node()),
    ] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    wait_for("3 sessions", Duration::from_secs(20), || host.session_count() == 3);
    Rig { kernel, host }
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

fn invoke(kernel: &Kernel, cap: &str, input: Value) -> (Value, bool) {
    kernel.invoke(cap, &input)
}

// ---- D01: cadeia nos dois backends ----

#[test]
fn chain_rust_rust() {
    let r = rig(&rust_node(), &rust_node());
    let (v, ok) = invoke(&r.kernel, "cons.chain@1", json!({"chain": true, "input": {"value": 42}}));
    assert!(ok, "cadeia: {}", v);
    assert_eq!(v["chained"]["echo"]["value"], 42, "ecoou pelo provedor: {}", v);
    assert_eq!(v["chained"]["via"], "prov");
    assert_eq!(v["via"], "cons");
    // Independente intacto.
    let (w, ok) = invoke(&r.kernel, "indep.echo@1", json!({"ping": 1}));
    assert!(ok && w["via"] == "indep", "{}", w);
    r.host.shutdown();
}

#[test]
fn chain_rust_consumer_python_provider() {
    let r = rig(&rust_node(), &python_node());
    let (v, ok) = invoke(&r.kernel, "cons.chain@1", json!({"chain": true, "input": {"value": 7}}));
    assert!(ok, "cadeia cruzada: {}", v);
    assert_eq!(v["chained"]["echo"]["value"], 7);
    assert_eq!(v["chained"]["via"], "prov");
    assert_eq!(v["via"], "cons");
    r.host.shutdown();
}

#[test]
fn chain_python_consumer_rust_provider() {
    let r = rig(&python_node(), &rust_node());
    let (v, ok) = invoke(&r.kernel, "cons.chain@1", json!({"chain": true, "input": {"value": 9}}));
    assert!(ok, "cadeia cruzada inversa: {}", v);
    assert_eq!(v["chained"]["echo"]["value"], 9);
    assert_eq!(v["chained"]["via"], "prov");
    assert_eq!(v["via"], "cons");
    r.host.shutdown();
}

#[test]
fn business_error_preserved_with_origin() {
    let r = rig(&rust_node(), &python_node());
    let (v, ok) = invoke(&r.kernel, "cons.chain@1", json!({"chain": true, "input": {"fail": "teapot"}}));
    assert!(!ok, "business errors never become success: {}", v);
    assert_eq!(v["code"], "teapot", "code preserved: {}", v);
    r.host.shutdown();
}

// ---- D04/D06: withdraw during execution and reintroduction ----

#[test]
fn withdraw_during_execution_and_reintroduce() {
    let r = rig(&rust_node(), &rust_node());
    let k = r.kernel.clone();
    let h = std::thread::spawn(move || {
        invoke(&k, "cons.chain@1", json!({"chain": true, "input": {"sleep_ms": 30000}}))
    });
    // Provider busy on the child; withdraws midway: parks (in-flight
    // work pins it, I07) and the child is revoked with no false success.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(r.kernel.dispose_plugin("prov").as_str(), "CleanupPending");
    let (v, ok) = h.join().unwrap();
    assert!(!ok, "no false success after withdraw: {}", v);
    assert!(
        v["code"] == "cancelled" || v["code"] == "outcome-unknown",
        "revocation terminal: {}",
        v
    );
    // Consumer parks/settles without hanging; independent continues.
    let (w, ok) = invoke(&r.kernel, "indep.echo@1", json!({"ping": 2}));
    assert!(ok, "independent stays responsive: {}", w);
    // Reintroduces under a new identity: chain back, old authority dead.
    let home = r.kernel.plugins_dir.parent().unwrap().to_path_buf();
    let m = home.join("plugins").join("prov.json");
    r.kernel.load_manifest(&m).unwrap();
    wait_for("provedor reativado", Duration::from_secs(20), || {
        r.kernel.context_state_of("prov").as_deref() == Some("Active") && r.host.session_count() == 3
    });
    let (v, ok) = invoke(&r.kernel, "cons.chain@1", json!({"chain": true, "input": {"value": 1}}));
    assert!(ok, "chain after reintroduction: {}", v);
    assert_eq!(v["chained"]["via"], "prov");
    r.host.shutdown();
}

// ---- on-wire cancel with a raw consumer ----

#[test]
fn raw_cancel_revokes_inflight_child() {
    use matrix_proto::{encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME};
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    let (provider, _) = (ext_manifest("prov", &["prov.api@1"], &rust_node()), {
        let m = consumer_manifest(&rust_node());
        m
    });
    // Raw consumer: no process (spawn fails) for the raw socket to bind.
    let consumer = consumer_manifest(&("/bin/false".to_string(), vec![]));
    let home = fresh_home("rawcancel");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [provider, consumer] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    wait_for("prov session", Duration::from_secs(20), || host.session_count() == 1);
    // Consumidor cru: registra e abre filha bloqueante (sleep longo).
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    let recv = |s: &mut UnixStream| -> Value {
        let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
        let e = parse_frame_payload(&raw).unwrap();
        serde_json::to_value(e.body).unwrap()
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw",
                 "features": ["dependency-calls/1"]},
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let sid = parse_frame_payload(&raw).unwrap().session_id.unwrap();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": sid,
        "body": {"manifest": {"id": "cons"}},
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let reg = parse_frame_payload(&raw).unwrap();
    assert_eq!(reg.ty, "registered");
    let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let act = parse_frame_payload(&raw).unwrap();
    assert_eq!(act.ty, "lifecycle.activate");
    assert!(act.body.get("dependency_bindings").and_then(|v| v.as_array()).is_some_and(|a| !a.is_empty()), "bindings no activate: {:?}", act.body);
    let binding = act.body["dependency_bindings"][0]["binding_id"].as_str().unwrap().to_string();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": act.instance_id, "generation": act.generation,
        "request_id": act.request_id,
        "body": {"operation_id": "op", "status": "ok", "pending": []},
    }));
    // Durable kernel-side parent + blocking open.
    let parent = kernel
        .call_open("cons.chain@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("pai")
        .ticket;
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
        "message_id": "o1", "session_id": sid,
        "instance_id": instance, "generation": generation,
        "request_id": "r1",
        "body": {"parent_ticket": parent.0.to_string(), "binding_id": binding,
                 "timeout_ms": 12000, "input": {"sleep_ms": 10000}},
    }));
    // Normative sequence: `accepted` before the terminal.
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().expect("accepted");
    let e = parse_frame_payload(&raw).unwrap();
    assert_eq!(e.ty, "dependency.accepted", "{:?}", e.body);
    assert_eq!(e.request_id.as_deref(), Some("r1"));
    assert!(!e.body["child_ticket"].as_str().unwrap_or("").is_empty());
    std::thread::sleep(Duration::from_millis(500));
    // Cancela no fio: cancel.result `revoked` + open responde `cancelled`.
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "dependency.cancel",
        "message_id": "c1", "session_id": sid,
        "instance_id": instance, "generation": generation,
        "request_id": "c1", "body": {"target_request_id": "r1"},
    }));
    let b = recv(&mut stream);
    // Either order of cancel.result vs open result is valid.
    let mut seen_cancel = false;
    let mut seen_open = false;
    let mut b = b;
    for _ in 0..2 {
        // Tells apart by body: cancel.result carries target_request_id.
        if b.get("target_request_id").is_some() {
            assert_eq!(b["state"], "revoked", "{:?}", b);
            seen_cancel = true;
        } else {
            assert_eq!(b["status"], "error", "{:?}", b);
            assert_eq!(b["error"]["code"], "cancelled", "{:?}", b);
            seen_open = true;
        }
        if seen_cancel && seen_open {
            break;
        }
        b = recv(&mut stream);
    }
    assert!(seen_cancel && seen_open, "both answers");
    assert!(kernel.call_close(parent));
    host.shutdown();
}

// ---- Accepted precedes terminal, in order ----

#[test]
fn accepted_before_terminal_in_order() {
    // Sessionless provider: fast dispatch; accepted → terminal order.
    let (provider, _) = (ext_manifest("prov", &["prov.api@1"], &rust_node()), {
        let m = consumer_manifest(&rust_node());
        m
    });
    let consumer = consumer_manifest(&("/bin/false".to_string(), vec![]));
    let home = fresh_home("accord");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [provider, consumer] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    wait_for("prov up", Duration::from_secs(20), || host.session_count() == 1);
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    let recv_ty = |s: &mut UnixStream, tag: &str| {
        let raw = match read_frame(s, DEFAULT_MAX_FRAME) {
            Ok(Some(r)) => r,
            Ok(None) => panic!("{}: eof", tag),
            Err(e) => panic!("{}: {:?}", tag, e),
        };
        parse_frame_payload(&raw).unwrap()
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw",
                 "features": ["dependency-calls/1"]},
    }));
    let e = recv_ty(&mut stream, "acc-welcome");
    assert_eq!(e.ty, "welcome");
    let sid = e.session_id.unwrap();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": sid,
        "body": {"manifest": {"id": "cons"}},
    }));
    let reg = recv_ty(&mut stream, "acc-registered");
    assert_eq!(reg.ty, "registered");
    let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
    let act = recv_ty(&mut stream, "acc-activate");
    assert_eq!(act.ty, "lifecycle.activate");
    let binding = act.body["dependency_bindings"][0]["binding_id"].as_str().unwrap().to_string();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": instance, "generation": generation,
        "request_id": act.request_id,
        "body": {"operation_id": "op", "status": "ok", "pending": []},
    }));
    let parent = kernel
        .call_open("cons.chain@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("pai")
        .ticket;
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
        "message_id": "o1", "session_id": sid,
        "instance_id": instance, "generation": generation,
        "request_id": "r1",
        "body": {"parent_ticket": parent.0.to_string(), "binding_id": binding,
                 "timeout_ms": 5000, "input": {}},
    }));
    let first = recv_ty(&mut stream, "acc-openresp");
    assert_eq!(first.ty, "dependency.accepted", "accepted primeiro: {:?}", first.body);
    assert_eq!(first.request_id.as_deref(), Some("r1"));
    let second = recv_ty(&mut stream, "acc-terminal");
    assert_eq!(second.ty, "dependency.result", "terminal depois: {:?}", second.body);
    assert_eq!(second.request_id.as_deref(), Some("r1"));
    // Real provider with session: real dispatch, real echo.
    assert_eq!(second.body["status"], "ok", "{:?}", second.body);
    assert_eq!(second.body["output"]["echo"], json!({}));
    assert!(kernel.call_close(parent));
    host.shutdown();
}

#[test]
fn slow_consumer_never_blocks_control() {
    use matrix_proto::{encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME};
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    // 512 KB of output to a never-reading consumer: fills the socket
    // buffer. Without write deadlines + global lock, control used to freeze.
    let (provider, _) = (ext_manifest("prov", &["prov.api@1"], &rust_node()), {
        let m = consumer_manifest(&rust_node());
        m
    });
    // Loose quota (2 MB): the bottleneck here is the socket buffer, not the quota.
    let mut consumer = consumer_manifest(&("/bin/false".to_string(), vec![]));
    consumer["outbound"]["limits"]["max_queued_bytes"] = json!(2 * 1024 * 1024);
    let home = fresh_home("slowcons");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [provider, consumer, ext_manifest("probe", &["probe.cap@1"], &rust_node())] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    // Provider + probe with sessions (consumer is /bin/false: no process).
    wait_for("sessions", Duration::from_secs(20), || host.session_count() == 2);
    // Raw consumer that never reads past activate.
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(20))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw-noread",
                 "features": ["dependency-calls/1"]},
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let e = parse_frame_payload(&raw).unwrap();
    assert_eq!(e.ty, "welcome");
    let sid = e.session_id.unwrap();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": sid,
        "body": {"manifest": {"id": "cons"}},
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let reg = parse_frame_payload(&raw).unwrap();
    assert_eq!(reg.ty, "registered");
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let act = parse_frame_payload(&raw).unwrap();
    assert_eq!(act.ty, "lifecycle.activate");
    let binding = act.body["dependency_bindings"][0]["binding_id"].as_str().unwrap().to_string();
    let (instance, generation) = (act.instance_id.clone().unwrap(), act.generation.unwrap().to_string());
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": instance, "generation": generation,
        "request_id": act.request_id,
        "body": {"operation_id": "op", "status": "ok", "pending": []},
    }));
    // Drains `accepted` (reads ONLY up to it; never reads again).
    let parent = kernel
        .call_open("cons.chain@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("pai")
        .ticket;
    // 512 KB: cabe no frame (1 MB), estoura o buffer do socket (~212 KB).
    let big = "y".repeat(512 * 1024);
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
        "message_id": "o1", "session_id": sid,
        "instance_id": instance, "generation": generation,
        "request_id": "r1",
        "body": {"parent_ticket": parent.0.to_string(), "binding_id": binding,
                 "timeout_ms": 30000, "input": {"blob": big}},
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().expect("accepted");
    assert_eq!(parse_frame_payload(&raw).unwrap().ty, "dependency.accepted");
    drop(big);
    // NEVER reads this socket again. Control on another session must progress
    // while the 512 KB send stalls/expires on the full buffer.
    let k2 = kernel.clone();
    let big_call = std::thread::spawn(move || {
        // Aguarda o assentamento da filha (fecha por timeout de escrita).
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(20) {
            if k2.pending_calls().iter().all(|t| t.dep.is_none()) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        false
    });
    let t0 = Instant::now();
    let (w, ok) = invoke(&kernel, "probe.cap@1", json!({"ping": 1}));
    assert!(ok, "controle progride com consumidor lento: {}", w);
    assert!(t0.elapsed() < Duration::from_secs(10), "fast control");
    assert!(big_call.join().unwrap(), "filha assentou (sem travar)");
    assert!(kernel.call_close(parent));
    host.shutdown();
}

#[test]
fn send_quota_refuses_and_releases() {
    // Tiny quota: 1 KB input exceeds, small passes; after the end,
    // the reservation returns (no residual refusal on resend).
    let (provider, _) = (ext_manifest("prov", &["prov.api@1"], &rust_node()), {
        let m = consumer_manifest(&rust_node());
        m
    });
    let mut consumer = consumer_manifest(&("/bin/false".to_string(), vec![]));
    consumer["outbound"]["limits"]["max_queued_bytes"] = json!(128);
    let home = fresh_home("quota");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [provider, consumer] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    wait_for("prov session", Duration::from_secs(20), || host.session_count() == 1);
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    let recv = |s: &mut UnixStream| {
        let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
        parse_frame_payload(&raw).unwrap()
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw",
                 "features": ["dependency-calls/1"]},
    }));
    let sid = recv(&mut stream).session_id.unwrap();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": sid,
        "body": {"manifest": {"id": "cons"}},
    }));
    let reg = recv(&mut stream);
    assert_eq!(reg.ty, "registered");
    let act = recv(&mut stream);
    assert_eq!(act.ty, "lifecycle.activate");
    let binding = act.body["dependency_bindings"][0]["binding_id"].as_str().unwrap().to_string();
    let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": instance, "generation": generation,
        "request_id": act.request_id,
        "body": {"operation_id": "op", "status": "ok", "pending": []},
    }));
    let parent = kernel
        .call_open("cons.chain@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("pai")
        .ticket;
    let open = |s: &mut UnixStream, mid: &str, rid: &str, input: Value| {
        send(s, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
            "message_id": mid, "session_id": sid,
            "instance_id": instance, "generation": generation,
            "request_id": rid,
            "body": {"parent_ticket": parent.0.to_string(), "binding_id": binding,
                     "timeout_ms": 5000, "input": input},
        }));
        let t = recv(s);
        assert_eq!(t.ty, "dependency.result", "{:?}", t.body);
        t.body
    };
    let open_accepted = |s: &mut UnixStream, mid: &str, rid: &str, input: Value| {
        send(s, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
            "message_id": mid, "session_id": sid,
            "instance_id": instance, "generation": generation,
            "request_id": rid,
            "body": {"parent_ticket": parent.0.to_string(), "binding_id": binding,
                     "timeout_ms": 5000, "input": input},
        }));
        // accepted ALWAYS precedes on admission; refusals have no accepted.
        let a = recv(s);
        assert_eq!(a.ty, "dependency.accepted", "{:?}", a.body);
        let t = recv(s);
        assert_eq!(t.ty, "dependency.result", "{:?}", t.body);
        t.body
    };
    // 1 KB >> 128 quota: deterministic refusal, never admitted (no accepted).
    let big = "z".repeat(1024);
    let b = open(&mut stream, "m1", "r1", json!({"blob": big}));
    assert_eq!(b["error"]["code"], "resource-exhausted", "{:?}", b);
    // Small passes (real sessioned provider serves) and releases.
    let b = open_accepted(&mut stream, "m2", "r2", json!({"ping": 1}));
    assert_eq!(b["status"], "ok", "{:?}", b);
    // And again: no reservation residue.
    let b = open_accepted(&mut stream, "m3", "r3", json!({"ping": 2}));
    assert_eq!(b["status"], "ok", "{:?}", b);
    assert!(kernel.call_close(parent));
    host.shutdown();
}

// ---- D08: control progresses under saturation ----

#[test]
fn saturation_control_progresses() {
    let r = rig(&rust_node(), &rust_node());
    // Saturates the provider session with blocking children (cap 16).
    // Long deadline (12 s policy cap) so they coexist.
    let mut ths = vec![];
    for _ in 0..16 {
        let k = r.kernel.clone();
        ths.push(std::thread::spawn(move || {
            invoke(&k, "cons.chain@1", json!({"chain": true, "timeout_ms": 60000, "input": {"sleep_ms": 60000}}))
        }));
    }
    wait_for("saturado", Duration::from_secs(20), || {
        r.kernel.calls.snapshot().iter().filter(|t| t.dep.is_some()).count() >= 12
    });
    // Control: new session registers + heartbeat; independent answers.
    let t0 = Instant::now();
    let (w, ok) = invoke(&r.kernel, "indep.echo@1", json!({"ping": 3}));
    assert!(ok, "independent under saturation: {}", w);
    assert!(t0.elapsed() < Duration::from_secs(10), "controle progride");
    // Revocation needs no handler cooperation: withdraws and all settles.
    let t0 = Instant::now();
    let _ = r.kernel.dispose_plugin("prov");
    let mut downs = 0;
    for t in ths {
        let (v, ok) = t.join().unwrap();
        assert!(!ok, "no false success under withdraw: {}", v);
        downs += 1;
    }
    assert_eq!(downs, 16);
    assert!(t0.elapsed() < Duration::from_secs(15), "withdraw settles under saturation");
    r.host.shutdown();
}

// ---- M6.3: recursos e eventos externos ----

fn sub_manifest(entry: &(String, Vec<String>), event_log: Option<String>) -> Value {
    let (bin, mut args) = (entry.0.clone(), entry.1.clone());
    if let Some(path) = event_log {
        args.push("--event-log".into());
        args.push(path);
    }
    let mut m = json!({
        "id": "sub", "version": "1.0.0",
        "capabilities": ["sub.chain@1"], "subscriptions": ["test.topic"],
        "requires": [{"interface": "prov.api@1", "provider": "prov"}],
        "reducer": "external", "init_state": {}, "tier": "process",
        "trust": "trusted", "restart": "permanent",
        "execution": {"kind": "process", "entrypoint": bin,
            "args": args, "timeout_ms": 15000},
    });
    m["outbound"] = json!({"request": ["prov.api@1"], "limits": {
        "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
        "max_calls_global": 64, "max_seen_requests": 64,
        "max_queued_bytes": 65536, "max_deadline_ms": 12000}});
    m
}

fn rig_sub(cons_entry: &(String, Vec<String>), event_log: Option<String>) -> (Rig, PathBuf) {
    let home = fresh_home("sub");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    let prov = ext_manifest("prov", &["prov.api@1"], &rust_node());
    let cons = sub_manifest(cons_entry, event_log);
    for m in [prov, cons] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("sub", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    wait_for("2 sessions", Duration::from_secs(20), || host.session_count() == 2);
    (Rig { kernel, host }, home)
}

fn event_lines(path: &PathBuf) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| l.to_string())
        .collect()
}

fn wait_lines(path: &PathBuf, n: usize) {
    wait_for("eventos", Duration::from_secs(10), || event_lines(path).len() >= n);
}

#[test]
fn resources_acquire_release_rust() {
    let (r, _home) = rig_sub(&rust_node(), None);
    // Timer via chain: acquire, release, double-release denies.
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"acquire": {"kind": "timer", "label": "t1", "interval_ms": 50}}));
    assert!(ok, "adquire: {}", v);
    let h = v["acquired"]["handle"].as_str().expect("handle").to_string();
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"release": h.parse::<u64>().unwrap()}));
    assert!(ok, "libera: {}", v);
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"release": h.parse::<u64>().unwrap()}));
    assert!(!ok, "double release denies: {}", v);
    // Sub via chain works too.
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"acquire": {"kind": "sub", "label": "test.topic"}}));
    assert!(ok, "sub: {}", v);
    r.host.shutdown();
}

#[test]
fn resources_acquire_release_python() {
    let (r, _home) = rig_sub(&python_node(), None);
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"acquire": {"kind": "timer", "label": "t9", "interval_ms": 50}}));
    assert!(ok, "adquire (py): {}", v);
    let h: u64 = v["acquired"]["handle"].as_str().unwrap().parse().unwrap();
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"release": h}));
    assert!(ok, "libera (py): {}", v);
    // Unknown kinds deny without admitting anything.
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"acquire": {"kind": "wormhole", "label": "x"}}));
    assert!(!ok, "kind desconhecido: {}", v);
    r.host.shutdown();
}

#[test]
fn events_delivered_and_revoked_on_withdraw() {
    let log = std::env::temp_dir().join(format!("matrix-evpy-{}-{}.log", std::process::id(), Instant::now().elapsed().as_nanos()));
    let (r, _home) = rig_sub(&python_node(), Some(log.to_string_lossy().to_string()));
    r.kernel.emit("test.topic", &json!({"n": 1}));
    wait_lines(&log, 1);
    let lines = event_lines(&log);
    assert!(lines[0].starts_with("test.topic\t"), "{:?}", lines);
    assert!(lines[0].contains("\"n\":1") || lines[0].contains("\"n\": 1"), "{:?}", lines);
    // Unsubscribed topics never deliver.
    r.kernel.emit("other.topic", &json!({"n": 2}));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(event_lines(&log).len(), 1);
    // Withdraw revokes the subscription: waits for the session drop (async
    // hook) then emits — no delivery, no residue.
    assert_eq!(r.kernel.dispose_plugin("sub").as_str(), "Disposed");
    wait_for("dropped session", Duration::from_secs(10), || r.host.session_count() == 1);
    r.kernel.emit("test.topic", &json!({"n": 3}));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(event_lines(&log).len(), 1, "nothing after withdraw");
    let _ = std::fs::remove_file(&log);
    r.host.shutdown();
}

#[test]
fn events_delivered_rust() {
    let log = std::env::temp_dir().join(format!("matrix-evrs-{}-{}.log", std::process::id(), Instant::now().elapsed().as_nanos()));
    let (r, _home) = rig_sub(&rust_node(), Some(log.to_string_lossy().to_string()));
    r.kernel.emit("test.topic", &json!({"tick": true}));
    wait_lines(&log, 1);
    assert!(event_lines(&log)[0].starts_with("test.topic\t"));
    let _ = std::fs::remove_file(&log);
    r.host.shutdown();
}

#[test]
fn stale_handle_rejected_after_withdraw() {
    let (r, _home) = rig_sub(&rust_node(), None);
    let (v, ok) = invoke(&r.kernel, "sub.chain@1", json!({"acquire": {"kind": "timer", "label": "t2", "interval_ms": 50}}));
    assert!(ok, "{}", v);
    let h: u64 = v["acquired"]["handle"].as_str().unwrap().parse().unwrap();
    assert_eq!(r.kernel.dispose_plugin("sub").as_str(), "Disposed");
    // Dead-generation handles never release (nor pretend to).
    let (v, ok) = invoke(&r.kernel, "prov.api@1", json!({"echo": 1}));
    assert!(ok, "prov segue: {}", v);
    assert!(r.kernel.release(matrix_core::ResourceHandle(h)).is_err(), "handle obsoleto rejeitado");
    r.host.shutdown();
}

fn noop_manifest(id: &str, caps: &[&str]) -> Value {
    json!({
        "id": id, "version": "1.0.0", "capabilities": caps, "subscriptions": [],
        "reducer": "noop", "init_state": {}, "tier": "inproc",
        "trust": "trusted", "restart": "permanent",
    })
}

#[test]
fn session_drop_releases_external_handles() {
    // Second processless consumer: raw socket acquires, closes without
    // release it; the session drop releases the handles (verifiable fate).
    let home = fresh_home("sessdrop");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    let m = noop_manifest("raw", &["raw.cap@1"]);
    let p = home.join("plugins").join("raw.json");
    std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
    kernel.load_manifest(&p).unwrap();
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    let recv = |s: &mut UnixStream| {
        let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
        parse_frame_payload(&raw).unwrap()
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw-res",
                 "features": ["dependency-calls/1"]},
    }));
    let sid = recv(&mut stream).session_id.unwrap();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": sid,
        "body": {"manifest": {"id": "raw"}},
    }));
    let reg = recv(&mut stream);
    assert_eq!(reg.ty, "registered");
    let act = recv(&mut stream);
    assert_eq!(act.ty, "lifecycle.activate");
    let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": instance, "generation": generation,
        "request_id": act.request_id,
        "body": {"operation_id": "op", "status": "ok", "pending": []},
    }));
    let acquire = |s: &mut UnixStream, mid: &str, rid: &str, kind: &str, label: &str| {
        send(s, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "resource.acquire",
            "message_id": mid, "session_id": sid,
            "instance_id": instance, "generation": generation,
            "request_id": rid,
            "body": {"operation_id": format!("op-{}", rid), "kind": kind, "label": label,
                     "interval_ms": 50},
        }));
        let e = recv(s);
        assert_eq!(e.ty, "resource.result", "{:?}", e.body);
        assert_eq!(e.request_id.as_deref(), Some(rid));
        e.body
    };
    let b = acquire(&mut stream, "m1", "r1", "timer", "t-raw");
    assert_eq!(b["status"], "ok", "{:?}", b);
    let h: u64 = b["handle"].as_str().unwrap().parse().unwrap();
    // Closes without releasing: the drop releases; later release denies.
    drop(stream);
    wait_for("handles liberados", Duration::from_secs(10), || {
        kernel.release(matrix_core::ResourceHandle(h)).is_err()
    });
    host.shutdown();
}

#[test]
fn foreign_release_denied_owner_release_ok() {
    // Two logicals, two sessions: releasing a foreign handle denies with
    // `permission-denied`; o dono solta normalmente.
    let home = fresh_home("ownrel");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for id in ["raw1", "raw2"] {
        let p = home.join("plugins").join(format!("{}.json", id));
        std::fs::write(&p, serde_json::to_string_pretty(&noop_manifest(id, &["c@1"])).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    let mut sessions = vec![];
    for logical in ["raw1", "raw2"] {
        let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
        let send = |s: &mut UnixStream, v: &Value| {
            let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
            s.write_all(&f).unwrap();
            s.flush().unwrap();
        };
        let recv = |s: &mut UnixStream| {
            let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
            parse_frame_payload(&raw).unwrap()
        };
        send(&mut stream, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "hello",
            "message_id": "h1",
            "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw",
                     "features": ["dependency-calls/1"]},
        }));
        let sid = recv(&mut stream).session_id.unwrap();
        send(&mut stream, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "component.register",
            "message_id": "reg1", "session_id": sid,
            "body": {"manifest": {"id": logical}},
        }));
        let reg = recv(&mut stream);
        assert_eq!(reg.ty, "registered");
        let act = recv(&mut stream);
        assert_eq!(act.ty, "lifecycle.activate");
        let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
        send(&mut stream, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
            "message_id": "lc1", "session_id": act.session_id,
            "instance_id": instance, "generation": generation,
            "request_id": act.request_id,
            "body": {"operation_id": "op", "status": "ok", "pending": []},
        }));
        sessions.push((stream, sid, instance, generation));
    }
    let res_op = |s: &mut UnixStream, sid: &str, instance: &str, generation: &str, mid: &str, rid: &str,
                  ty: &str, body: Value| {
        let send = |s: &mut UnixStream, v: &Value| {
            let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
            s.write_all(&f).unwrap();
            s.flush().unwrap();
        };
        send(s, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": ty,
            "message_id": mid, "session_id": sid,
            "instance_id": instance, "generation": generation,
            "request_id": rid, "body": body,
        }));
        let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
        let e = parse_frame_payload(&raw).unwrap();
        assert_eq!(e.ty, "resource.result", "{:?}", e.body);
        e.body
    };
    let (mut s1, sid1, inst1, gen1) = sessions.remove(0);
    let (mut s2, sid2, inst2, gen2) = sessions.remove(0);
    // raw1 adquire timer.
    let b = res_op(&mut s1, &sid1, &inst1, &gen1, "m1", "r1", "resource.acquire",
        json!({"operation_id": "op-1", "kind": "timer", "label": "t1", "interval_ms": 50}));
    assert_eq!(b["status"], "ok", "{:?}", b);
    let handle = b["handle"].as_str().unwrap().to_string();
    // raw2 tenta soltar o alheio: negado.
    let b = res_op(&mut s2, &sid2, &inst2, &gen2, "m2", "r2", "resource.release",
        json!({"operation_id": "op-2", "handle": handle}));
    assert_eq!(b["status"], "error", "{:?}", b);
    assert_eq!(b["code"], "permission-denied", "{:?}", b);
    // Owner releases: ok; again: error (already released, no double effect).
    let b = res_op(&mut s1, &sid1, &inst1, &gen1, "m3", "r3", "resource.release",
        json!({"operation_id": "op-3", "handle": handle}));
    assert_eq!(b["status"], "ok", "{:?}", b);
    let b = res_op(&mut s1, &sid1, &inst1, &gen1, "m4", "r4", "resource.release",
        json!({"operation_id": "op-4", "handle": handle}));
    assert_eq!(b["status"], "error", "{:?}", b);
    host.shutdown();
}

#[test]
fn egress_quota_downgrades_big_output() {
    // 128 quota on the consumer + 4 KB output for a small input:
    // deterministic downgrade to `resource-exhausted`, leaking no reservation.
    let (provider, _) = (ext_manifest("prov", &["prov.api@1"], &rust_node()), {
        let m = consumer_manifest(&rust_node());
        m
    });
    let mut consumer = consumer_manifest(&("/bin/false".to_string(), vec![]));
    consumer["outbound"]["limits"]["max_queued_bytes"] = json!(128);
    let home = fresh_home("egress");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in [provider, consumer] {
        let p = home.join("plugins").join(format!("{}.json", m["id"].as_str().unwrap()));
        std::fs::write(&p, serde_json::to_string_pretty(&m).unwrap()).unwrap();
        kernel.load_manifest(&p).unwrap();
    }
    kernel.grant_outbound("cons", "prov.api@1");
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    wait_for("prov session", Duration::from_secs(20), || host.session_count() == 1);
    let mut stream = UnixStream::connect(host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(15))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let f = encode(&serde_json::to_vec(v).unwrap(), DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    let recv = |s: &mut UnixStream| {
        let raw = read_frame(s, DEFAULT_MAX_FRAME).unwrap().expect("frame");
        parse_frame_payload(&raw).unwrap()
    };
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw",
                 "features": ["dependency-calls/1"]},
    }));
    let sid = recv(&mut stream).session_id.unwrap();
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "component.register",
        "message_id": "reg1", "session_id": sid,
        "body": {"manifest": {"id": "cons"}},
    }));
    let reg = recv(&mut stream);
    assert_eq!(reg.ty, "registered");
    let act = recv(&mut stream);
    assert_eq!(act.ty, "lifecycle.activate");
    let binding = act.body["dependency_bindings"][0]["binding_id"].as_str().unwrap().to_string();
    let (instance, generation) = (reg.instance_id.unwrap(), reg.generation.unwrap().to_string());
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": instance, "generation": generation,
        "request_id": act.request_id,
        "body": {"operation_id": "op", "status": "ok", "pending": []},
    }));
    let parent = kernel
        .call_open("cons.chain@1", &json!({}), Some(CallPolicy::cancel()), &[])
        .expect("pai")
        .ticket;
    let open = |s: &mut UnixStream, mid: &str, rid: &str, input: Value| {
        send(s, &json!({
            "protocol": "matrix.component", "version": "0.1", "type": "dependency.open",
            "message_id": mid, "session_id": sid,
            "instance_id": instance, "generation": generation,
            "request_id": rid,
            "body": {"parent_ticket": parent.0.to_string(), "binding_id": binding,
                     "timeout_ms": 8000, "input": input},
        }));
        let a = recv(s);
        assert_eq!(a.ty, "dependency.accepted", "{:?}", a.body);
        let t = recv(s);
        assert_eq!(t.ty, "dependency.result", "{:?}", t.body);
        t.body
    };
    // ~100 B input, 4 KB output: downgrade, no leaked payload.
    let b = open(&mut stream, "m1", "r1", json!({"amplify": 4096}));
    assert_eq!(b["error"]["code"], "resource-exhausted", "{:?}", b);
    assert!(b.get("output").is_none(), "sem payload: {:?}", b);
    // Reserva liberada: troca pequena passa em seguida.
    let b = open(&mut stream, "m2", "r2", json!({"ping": 1}));
    assert_eq!(b["status"], "ok", "{:?}", b);
    assert!(kernel.call_close(parent));
    host.shutdown();
}
