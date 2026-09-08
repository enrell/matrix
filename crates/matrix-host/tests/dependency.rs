//! M6.1 step 1 acceptance: extension schema, negotiation, and gate.
//!
//! - Negotiated sessions get the feature; legacy sessions work without it.
//! - `dependency.open` without negotiation → `unsupported-feature`, no work.
//! - No outbound policy → `permission-denied` (deny by default).
//! - Mensagem malformada → `invalid-message`; duplicata → `duplicate-request`;
//!   full seen-table → `resource-exhausted` (no silent eviction).
//! - Wrong direction/garbage never drops the session.
//! - Valid/invalid `outbound` manifests at the kernel.
//!
//! Admission, dispatch, and effects are step 2: a valid+authorized open gets
//! `internal` documented as the placeholder in this step.

use matrix_core::{Journal, Kernel};
use matrix_host::{Host, HostPolicy};
use matrix_proto::{encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME};
use serde_json::{json, Value};
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-dep1-{}-{}-{}",
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

fn outbound_manifest(id: &str, caps: &[&str], max_seen: u64) -> Value {
    let mut m = noop_manifest(id, caps);
    m["outbound"] = json!({"request": ["ext.call@1"], "limits": {
        "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
        "max_calls_global": 64, "max_seen_requests": max_seen,
        "max_queued_bytes": 65536, "max_deadline_ms": 5000}});
    m
}

struct Rig {
    kernel: Arc<Kernel>,
    host: Arc<Host>,
    #[allow(dead_code)]
    home: PathBuf,
}

fn rig(manifests: &[Value]) -> Rig {
    let home = fresh_home("rig");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    for m in manifests {
        let p = write_manifest(&home, m["id"].as_str().unwrap(), m.clone());
        kernel.load_manifest(&p).unwrap();
    }
    // Negotiation exercised via explicit internal config; production
    // uses `Host::attach` (announcement disabled) — see the dedicated test.
    let mut policy = HostPolicy::default();
    policy.enable_dependency_calls = true;
    let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy).expect("attach");
    Rig { kernel, host, home }
}

// ---- driver cru ----

struct RawSession {
    stream: UnixStream,
    session_id: String,
    max_frame: usize,
    instance: String,
    generation: String,
    features: Vec<String>,
    next: u64,
}

fn raw_register(sock: &PathBuf, logical: &str, features: Option<Vec<&str>>) -> RawSession {
    let mut stream = UnixStream::connect(sock).expect("connect host");
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let send = |s: &mut UnixStream, v: &Value| {
        let raw = serde_json::to_vec(v).unwrap();
        let f = encode(&raw, DEFAULT_MAX_FRAME).unwrap();
        s.write_all(&f).unwrap();
        s.flush().unwrap();
    };
    let mut hello_body = json!({"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw-dep"});
    if let Some(fs) = features {
        hello_body["features"] = json!(fs);
    }
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1", "body": hello_body,
    }));
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let welcome = parse_frame_payload(&raw).unwrap();
    assert_eq!(welcome.ty, "welcome");
    let features = welcome.body.get("features").and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();
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
    let raw = read_frame(&mut stream, max_frame).unwrap().unwrap();
    let act = parse_frame_payload(&raw).unwrap();
    assert_eq!(act.ty, "lifecycle.activate");
    let (instance, generation) = (act.instance_id.clone().unwrap(), act.generation.unwrap().to_string());
    send(&mut stream, &json!({
        "protocol": "matrix.component", "version": "0.1", "type": "lifecycle.result",
        "message_id": "lc1", "session_id": act.session_id,
        "instance_id": act.instance_id, "generation": act.generation,
        "request_id": act.request_id,
        "body": {"operation_id": act.body.get("operation_id").cloned().unwrap_or(json!("op")),
                 "status": "ok", "pending": []},
    }));
    RawSession { stream, session_id, max_frame, instance, generation, features, next: 1 }
}

impl RawSession {
    fn dep(&mut self, ty: &str, body: Value) -> Value {
        self.next += 1;
        let mid = format!("d{}", self.next);
        let rid = format!("r{}", self.next);
        self.dep_with_ids(ty, &mid, &rid, body)
    }

    fn dep_with_ids(&mut self, ty: &str, mid: &str, rid: &str, body: Value) -> Value {
        let raw = serde_json::to_vec(&json!({
            "protocol": "matrix.component", "version": "0.1", "type": ty,
            "message_id": mid, "session_id": self.session_id,
            "instance_id": self.instance, "generation": self.generation,
            "request_id": rid, "body": body,
        }))
        .unwrap();
        let f = encode(&raw, self.max_frame).unwrap();
        self.stream.write_all(&f).unwrap();
        self.stream.flush().unwrap();
        // Normative sequence: `accepted` precedes the terminal (drained here).
        loop {
            let raw = read_frame(&mut self.stream, self.max_frame).unwrap().expect("extension answer");
            let e = parse_frame_payload(&raw).unwrap();
            if e.ty == "dependency.accepted" {
                assert_eq!(e.request_id.as_deref(), Some(rid), "request_id correlation");
                continue;
            }
            assert_eq!(e.ty, "dependency.result", "answer: {:?}", e.body);
            assert_eq!(e.request_id.as_deref(), Some(rid), "request_id correlation");
            return e.body;
        }
    }

    fn heartbeat_ok(&mut self) {
        let raw = serde_json::to_vec(&json!({
            "protocol": "matrix.component", "version": "0.1", "type": "session.heartbeat",
            "message_id": format!("hb-{}", self.next), "session_id": self.session_id, "body": {},
        }))
        .unwrap();
        let f = encode(&raw, self.max_frame).unwrap();
        self.stream.write_all(&f).unwrap();
        self.stream.flush().unwrap();
        let raw = read_frame(&mut self.stream, self.max_frame).unwrap().expect("heartbeat");
        let e = parse_frame_payload(&raw).unwrap();
        assert_eq!(e.ty, "session.heartbeat");
    }
}

fn err_code(body: &Value) -> String {
    body.get("error").and_then(|e| e.get("code")).and_then(|c| c.as_str()).unwrap_or("").to_string()
}

// ---- negotiation and compatibility ----

#[test]
fn negotiated_session_gets_feature_and_old_session_works() {
    let r = rig(&[noop_manifest("new", &["new.cap@1"]), noop_manifest("old", &["old.cap@1"])]);
    let mut new = raw_register(&r.host.sock_path(), "new", Some(vec!["dependency-calls/1"]));
    assert!(new.features.iter().any(|f| f == "dependency-calls/1"), "{:?}", new.features);
    let mut old = raw_register(&r.host.sock_path(), "old", None);
    assert!(!old.features.iter().any(|f| f == "dependency-calls/1"));
    new.heartbeat_ok();
    old.heartbeat_ok();
    assert_eq!(r.host.session_count(), 2);
    r.host.shutdown();
}

#[test]
fn bad_features_in_hello_rejected() {
    let r = rig(&[noop_manifest("dummy", &["dummy.cap@1"])]);
    let mut stream = UnixStream::connect(r.host.sock_path()).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let raw = serde_json::to_vec(&json!({
        "protocol": "matrix.component", "version": "0.1", "type": "hello",
        "message_id": "h1",
        "body": {"versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "raw", "features": "dependency-calls/1"},
    }))
    .unwrap();
    let f = encode(&raw, DEFAULT_MAX_FRAME).unwrap();
    stream.write_all(&f).unwrap();
    stream.flush().unwrap();
    let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let e = parse_frame_payload(&raw).unwrap();
    assert_eq!(e.ty, "reject", "features malformado: {:?}", e.body);
    r.host.shutdown();
}

#[test]
fn feature_disabled_by_default_in_production() {
    // `Host::attach` = production: even when offered, nothing negotiates and
    // opens land on `unsupported-feature` (spec: announce only when
    // implementada e habilitada).
    let home = fresh_home("prod");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    let p = write_manifest(&home, "pol", outbound_manifest("pol", &["pol.cap@1"], 8));
    kernel.load_manifest(&p).unwrap();
    let host = Host::attach(kernel.clone(), &home.join("host")).expect("attach");
    let mut rs = raw_register(&host.sock_path(), "pol", Some(vec!["dependency-calls/1"]));
    assert!(!rs.features.iter().any(|f| f == "dependency-calls/1"), "{:?}", rs.features);
    let b = rs.dep("dependency.open", open_body());
    assert_eq!(b["status"], "error");
    assert_eq!(err_code(&b), "unsupported-feature", "{:?}", b);
    rs.heartbeat_ok();
    host.shutdown();
}

#[test]
fn requested_permissions_are_not_grants() {
    // Declared intent is NOT authorization: negotiated session, real binding,
    // valid message, but no operator grant → `permission-denied`
    // at admission, admitting and dispatching nothing.
    let (r, binding, parent) = rig_chained(8, false);
    assert!(r.kernel.outbound_policy_of("cons").is_some(), "declared intent");
    let mut rs = raw_register(&r.host.sock_path(), "cons", Some(vec!["dependency-calls/1"]));
    let b = open_as(&r, &mut rs, "m1", "r1", &binding, parent);
    assert_eq!(b["status"], "error");
    assert_eq!(err_code(&b), "permission-denied", "{:?}", b);
    assert!(r.kernel.pending_calls().iter().all(|t| t.dep.is_none()), "nada admitido sem grant");
    rs.heartbeat_ok();
    assert!(r.kernel.call_close(parent));
    r.host.shutdown();
}

// ---- gate: negotiation and authorization ----

fn open_body() -> Value {
    json!({"parent_ticket": "17", "binding_id": "b1", "timeout_ms": 1500, "input": {"value": 42}})
}

fn chained_manifests(max_seen: u64) -> (Value, Value) {
    let provider = noop_manifest("prov", &["prov.api@1"]);
    let mut consumer = noop_manifest("cons", &["cons.cap@1"]);
    consumer["requires"] = json!([{"interface": "prov.api@1", "provider": "prov"}]);
    consumer["outbound"] = json!({"request": ["prov.api@1"], "limits": {
        "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
        "max_calls_global": 64, "max_seen_requests": max_seen,
        "max_queued_bytes": 65536, "max_deadline_ms": 5000}});
    (provider, consumer)
}

fn rig_chained(max_seen: u64, grant: bool) -> (Rig, String, matrix_core::TicketId) {
    let (provider, consumer) = chained_manifests(max_seen);
    let r = rig(&[provider, consumer]);
    if grant {
        r.kernel.grant_outbound("cons", "prov.api@1");
    }
    let bs = r.kernel.dependency_bindings_of("cons");
    assert_eq!(bs.len(), 1);
    let parent = r.kernel.call_open("cons.cap@1", &json!({}), Some(matrix_core::CallPolicy::cancel()), &[])
        .expect("pai")
        .ticket;
    (r, bs[0].id.clone(), parent)
}

fn open_as(_r: &Rig, rs: &mut RawSession, mid: &str, rid: &str, binding: &str, parent: matrix_core::TicketId) -> Value {
    rs.dep_with_ids("dependency.open", mid, rid, json!({
        "parent_ticket": parent.0.to_string(),
        "binding_id": binding, "timeout_ms": 1500, "input": {"value": 42},
    }))
}

#[test]
fn open_without_negotiation_rejected_without_work() {
    let r = rig(&[outbound_manifest("pol", &["pol.cap@1"], 8)]);
    let mut rs = raw_register(&r.host.sock_path(), "pol", None);
    let b = rs.dep("dependency.open", open_body());
    assert_eq!(b["status"], "error");
    assert_eq!(err_code(&b), "unsupported-feature", "{:?}", b);
    // Session stays alive; nothing admitted (empty call inventory).
    rs.heartbeat_ok();
    assert!(r.kernel.pending_calls().is_empty());
    r.host.shutdown();
}

#[test]
fn open_without_policy_rejected() {
    let r = rig(&[noop_manifest("bare", &["bare.cap@1"])]);
    let mut rs = raw_register(&r.host.sock_path(), "bare", Some(vec!["dependency-calls/1"]));
    let b = rs.dep("dependency.open", open_body());
    assert_eq!(b["status"], "error");
    assert_eq!(err_code(&b), "permission-denied", "{:?}", b);
    rs.heartbeat_ok();
    r.host.shutdown();
}

// ---- semantics and dedup ----

#[test]
fn open_invalid_rejected() {
    let r = rig(&[outbound_manifest("pol", &["pol.cap@1"], 8)]);
    let mut rs = raw_register(&r.host.sock_path(), "pol", Some(vec!["dependency-calls/1"]));
    // binding vazio
    let mut b = open_body();
    b["binding_id"] = json!("");
    assert_eq!(err_code(&rs.dep("dependency.open", b)), "invalid-message");
    // timeout zero
    let mut b = open_body();
    b["timeout_ms"] = json!(0);
    assert_eq!(err_code(&rs.dep("dependency.open", b)), "invalid-message");
    // campo de autoridade no payload
    let mut b = open_body();
    b["provider"] = json!("evil");
    assert_eq!(err_code(&rs.dep("dependency.open", b)), "invalid-message");
    // absent parent (structural parse failure → no answer; session lives)
    rs.heartbeat_ok();
    r.host.shutdown();
}

#[test]
fn open_duplicate_and_full_seen_table() {
    // Sessionless provider: fast dispatch becomes `outcome-unknown`; the
    // seen-table semantics never change (duplicate and cap).
    let (r, binding, parent) = rig_chained(2, true);
    let mut rs = raw_register(&r.host.sock_path(), "cons", Some(vec!["dependency-calls/1"]));
    let b = open_as(&r, &mut rs, "m1", "r1", &binding, parent);
    assert_eq!(err_code(&b), "outcome-unknown", "{:?}", b);
    // Same request, new message → explicit duplicate, no re-execution.
    let b = open_as(&r, &mut rs, "m2", "r1", &binding, parent);
    assert_eq!(err_code(&b), "duplicate-request", "{:?}", b);
    // Identical retransmit (same message_id) → duplicate too.
    let b = open_as(&r, &mut rs, "m2", "r1", &binding, parent);
    assert_eq!(err_code(&b), "duplicate-request", "{:?}", b);
    // A second distinct request takes the last slot.
    let b = open_as(&r, &mut rs, "m3", "r2", &binding, parent);
    assert_eq!(err_code(&b), "outcome-unknown", "{:?}", b);
    // Full table: refuses without evicting (r1/r2 stay duplicates).
    let b = open_as(&r, &mut rs, "m4", "r3", &binding, parent);
    assert_eq!(err_code(&b), "resource-exhausted", "{:?}", b);
    let b = open_as(&r, &mut rs, "m5", "r1", &binding, parent);
    assert_eq!(err_code(&b), "duplicate-request", "{:?}", b);
    rs.heartbeat_ok();
    assert!(r.kernel.call_close(parent));
    r.host.shutdown();
}

#[test]
fn wrong_direction_and_garbage_do_not_kill_session() {
    let r = rig(&[outbound_manifest("pol", &["pol.cap@1"], 8)]);
    let mut rs = raw_register(&r.host.sock_path(), "pol", Some(vec!["dependency-calls/1"]));
    // `accepted` runs host→component: validated and dropped.
    let raw = serde_json::to_vec(&json!({
        "protocol": "matrix.component", "version": "0.1", "type": "dependency.accepted",
        "message_id": "w1", "session_id": rs.session_id,
        "instance_id": rs.instance, "generation": rs.generation,
        "request_id": "wr1", "body": {"child_ticket": "3"},
    }))
    .unwrap();
    let f = encode(&raw, rs.max_frame).unwrap();
    rs.stream.write_all(&f).unwrap();
    rs.stream.flush().unwrap();
    // Unknown extension type: parse rejects, silence, session lives.
    let raw = serde_json::to_vec(&json!({
        "protocol": "matrix.component", "version": "0.1", "type": "dependency.frobnicate",
        "message_id": "w2", "session_id": rs.session_id,
        "instance_id": rs.instance, "generation": rs.generation,
        "request_id": "wr2", "body": {},
    }))
    .unwrap();
    let f = encode(&raw, rs.max_frame).unwrap();
    rs.stream.write_all(&f).unwrap();
    rs.stream.flush().unwrap();
    // Unknown-target cancel: `unknown-request` error, session lives.
    let b = rs.dep_with_ids("dependency.cancel", "w3", "wr3", json!({"target_request_id": "r1"}));
    assert_eq!(err_code(&b), "unknown-request", "{:?}", b);
    rs.heartbeat_ok();
    r.host.shutdown();
}

// ---- manifest outbound no kernel ----

#[test]
fn outbound_manifest_validated_at_load() {
    let home = fresh_home("outbound");
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let k = Kernel::new(&home, journal, false);
    // Valid: readable policy.
    let p = write_manifest(&home, "ok", outbound_manifest("ok", &["ok.cap@1"], 4));
    k.load_manifest(&p).unwrap();
    let pol = k.outbound_policy_of("ok").expect("policy");
    assert_eq!(pol.requested, vec!["ext.call@1".to_string()]);
    assert_eq!(pol.limits.max_seen_requests, 4);
    assert_eq!(pol.limits.max_deadline_ms, 5000);
    // Ausente = desabilitado.
    let p = write_manifest(&home, "bare", noop_manifest("bare", &["bare.cap@1"]));
    k.load_manifest(&p).unwrap();
    assert!(k.outbound_policy_of("bare").is_none());
    // Limites ausentes/zerados e allow malformado rejeitam a carga.
    for (id, mut m) in [("nolim", outbound_manifest("nolim", &["a@1"], 4)),
                        ("zero", outbound_manifest("zero", &["a@1"], 4)),
                        ("badcap", outbound_manifest("badcap", &["a@1"], 4))] {
        match id {
            "nolim" => { m.as_object_mut().unwrap()["outbound"].as_object_mut().unwrap().remove("limits"); }
            "zero" => { m["outbound"]["limits"]["max_depth"] = json!(0); }
            _ => { m["outbound"]["request"] = json!(["sem-versao"]); }
        }
        let p = write_manifest(&home, id, m);
        assert!(k.load_manifest(&p).is_err(), "{}", id);
    }
}
