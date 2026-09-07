//! Protocol conformance vectors (M2.1, milestone output).
//!
//! Valid ones check the contract per family; invalid ones check bounded
//! rejection (C12: no crash, no runaway allocation). The new protocol
//! is distinguishable from legacy in both directions.

use matrix_proto::*;
use serde_json::json;

fn frame_of(v: &serde_json::Value) -> Vec<u8> {
    let raw = serde_json::to_vec(v).unwrap();
    encode(&raw, DEFAULT_MAX_FRAME).unwrap()
}

fn valid_envelope(ty: &str, top: serde_json::Value, body: serde_json::Value) -> serde_json::Value {
    let mut o = serde_json::Map::new();
    o.insert("protocol".into(), json!(PROTOCOL_ID));
    o.insert("version".into(), json!(PROTOCOL_VERSION));
    o.insert("type".into(), json!(ty));
    if let serde_json::Value::Object(m) = top {
        for (k, v) in m {
            o.insert(k, v);
        }
    }
    o.insert("body".into(), body);
    serde_json::Value::Object(o)
}

// ---- valid: one per family ----

#[test]
fn vectors_valid_cover_families() {
    let ids = json!({"message_id": "m1", "session_id": "s1"});
    let full = json!({"message_id": "m1", "session_id": "s1", "instance_id": "4", "generation": "2", "request_id": "r1"});
    let cases: &[(&str, serde_json::Value, serde_json::Value)] = &[
        ("hello", json!({"message_id": "m1"}), json!({"versions": ["0.1"], "max_frame": 65536, "client": "t"})),
        ("welcome", ids.clone(), json!({"version": "0.1", "max_frame": 65536, "limits": {}})),
        ("reject", json!({"message_id": "m1"}), json!({"code": "unsupported-version", "reason": "x"})),
        ("component.register", ids.clone(), json!({"manifest": {"id": "p"}})),
        ("registered", full.clone(), json!({"logical": "p"})),
        ("lifecycle.prepare", full.clone(), json!({"operation_id": "op1", "manifest": {}, "bindings": []})),
        ("lifecycle.activate", full.clone(), json!({"operation_id": "op1", "manifest": {}, "bindings": []})),
        ("lifecycle.quiesce", full.clone(), json!({"operation_id": "op1", "deadline_ms": 100})),
        ("lifecycle.dispose", full.clone(), json!({"operation_id": "op1", "deadline_ms": 100})),
        ("lifecycle.result", full.clone(), json!({"operation_id": "op1", "status": "ok"})),
        ("capability.changed", ids.clone(), json!({"revision": "7", "change": "added"})),
        ("resource.acquire", full.clone(), json!({"operation_id": "op1", "kind": "timer", "label": "t"})),
        ("resource.release", full.clone(), json!({"operation_id": "op1", "handle": "9"})),
        ("resource.result", full.clone(), json!({"operation_id": "op1", "status": "released"})),
        ("call.open", full.clone(), json!({"ticket": "tkt-1", "capability": "a.b@1", "input": {}})),
        ("call.accepted", full.clone(), json!({"ticket": "tkt-1"})),
        ("call.result", full.clone(), json!({"ticket": "tkt-1", "status": "ok"})),
        ("call.error", full.clone(), json!({"ticket": "tkt-1", "error": {}})),
        ("call.cancel", full.clone(), json!({"ticket": "tkt-1", "reason": "x"})),
        ("call.cancel.result", full.clone(), json!({"ticket": "tkt-1", "status": "cancelled"})),
        ("stream.data", json!({"message_id": "m1", "session_id": "s1", "instance_id": "4", "generation": "2"}), json!({"stream_id": "st1", "seq": "0", "payload": "e30="})),
        ("stream.credit", json!({"message_id": "m1", "session_id": "s1", "instance_id": "4", "generation": "2"}), json!({"stream_id": "st1", "bytes": 4096})),
        ("stream.end", json!({"message_id": "m1", "session_id": "s1", "instance_id": "4", "generation": "2"}), json!({"stream_id": "st1", "status": "ok"})),
        ("session.heartbeat", ids.clone(), json!({})),
        ("session.renew", ids.clone(), json!({})),
        ("session.close", ids.clone(), json!({})),
        ("inspect.request", json!({"message_id": "m1"}), json!({"query": "instances"})),
        ("inspect.result", json!({"message_id": "m1"}), json!({"result": []})),
        ("dependency.open", full.clone(), json!({"parent_ticket": "17", "binding_id": "b1", "timeout_ms": 1500, "input": {"value": 42}})),
        ("dependency.accepted", full.clone(), json!({"child_ticket": "3"})),
        ("dependency.result", full.clone(), json!({"status": "ok", "output": {"v": 1}})),
        ("dependency.cancel", full.clone(), json!({"target_request_id": "r9"})),
        ("dependency.cancel.result", full.clone(), json!({"target_request_id": "r9", "state": "revoked"})),
        ("event.deliver", full.clone(), json!({"topic": "sys.tick", "payload": {}})),
    ];
    assert_eq!(cases.len(), TYPES.len(), "every family covered");
    for (ty, top, body) in cases {
        let v = valid_envelope(ty, top.clone(), body.clone());
        // Via framing real: codifica, fatia, valida.
        let f = frame_of(&v);
        let (payload, rest) = split_frame(&f, DEFAULT_MAX_FRAME).unwrap().unwrap();
        assert!(rest.is_empty());
        let e = parse_frame_payload(payload).unwrap_or_else(|e| panic!("{}: {:?}", ty, e));
        assert_eq!(&e.ty, ty);
    }
}

// ---- invalid: bounded rejection ----

#[test]
fn vectors_invalid_rejected() {
    // Giant declared frame: rejected without allocating the payload.
    let mut huge = (DEFAULT_MAX_FRAME as u32 + 1).to_be_bytes().to_vec();
    huge.extend_from_slice(b"{}");
    assert!(split_frame(&huge, DEFAULT_MAX_FRAME).is_err());

    // Truncated mid-payload.
    let f = frame_of(&json!({"protocol": PROTOCOL_ID}));
    let cut = &f[..f.len() - 2];
    assert!(split_frame(cut, DEFAULT_MAX_FRAME).unwrap().is_none());

    // Non-JSON garbage, shallow-invalid JSON, duplicates, depth.
    for raw in [
        &b"\xff\xfe\x00"[..],
        b"{oops",
        br#"{"a":1,"a":2}"#,
        br#"{"protocol":"matrix.component","version":"0.1","type":"hello","message_id":"m","body":{"versions":["0.1"],"max_frame":1,"client":"c"},"body":{}}"#,
    ] {
        assert!(scan_raw(raw).is_err(), "{:?}", &raw[..raw.len().min(16)]);
    }
    let mut deep = String::from("[");
    for _ in 0..(MAX_DEPTH + 2) {
        deep.push('[');
    }
    assert!(scan_raw(deep.as_bytes()).is_err());

    // Envelope: wrong protocol, incompatible version, unknown type,
    // non-decimal generation, incomplete body, empty message_id.
    let base = |ty: &str| valid_envelope(ty, json!({"message_id": "m1", "session_id": "s1", "instance_id": "1", "generation": "1", "request_id": "r1"}), json!({"ticket": "t", "capability": "c", "input": {}}));
    let mut v = base("call.open");
    v["protocol"] = json!("other");
    assert!(parse_frame_payload(&serde_json::to_vec(&v).unwrap()).is_err());
    let mut v = base("call.open");
    v["version"] = json!("9.9");
    let err = parse_frame_payload(&serde_json::to_vec(&v).unwrap()).unwrap_err();
    assert_eq!(err.code, "unsupported-version");
    let v = valid_envelope("teleport", json!({"message_id": "m"}), json!({}));
    assert!(parse_frame_payload(&serde_json::to_vec(&v).unwrap()).is_err());
    let mut v = base("call.open");
    v["generation"] = json!("abc");
    assert!(parse_frame_payload(&serde_json::to_vec(&v).unwrap()).is_err());
    let v = valid_envelope("call.open",
        json!({"message_id": "m1", "session_id": "s1", "instance_id": "1", "generation": "1", "request_id": "r1"}),
        json!({"ticket": "t"}));
    assert!(parse_frame_payload(&serde_json::to_vec(&v).unwrap()).is_err());
    let mut v = base("call.open");
    v["message_id"] = json!("");
    assert!(parse_frame_payload(&serde_json::to_vec(&v).unwrap()).is_err());
}

// ---- handshake ponta a ponta sobre bytes ----

#[test]
fn handshake_roundtrip_over_framing() {
    // Cliente oferece; servidor negocia e responde welcome em frames.
    let hello = valid_envelope("hello", json!({"message_id": "h1"}),
        json!({"versions": ["9.9", "0.1"], "max_frame": 65536, "client": " Ausland"}));
    let f = frame_of(&hello);
    let (payload, _) = split_frame(&f, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let env = parse_frame_payload(payload).unwrap();
    let (versions, want, feats) = hello_offer(&env).unwrap();
    assert!(feats.is_empty(), "hello without features = legacy session");
    let w = negotiate(&versions, &feats, want, "sess-1".into()).unwrap();
    assert_eq!(w.version, "0.1");
    let welcome = valid_envelope("welcome",
        json!({"message_id": "h1", "session_id": w.session_id}),
        json!({"version": w.version, "max_frame": w.max_frame, "limits": {}}));
    let f2 = frame_of(&welcome);
    let (p2, _) = split_frame(&f2, DEFAULT_MAX_FRAME).unwrap().unwrap();
    assert_eq!(parse_frame_payload(p2).unwrap().ty, "welcome");

    // No overlap: reject with a stable code.
    let hello2 = valid_envelope("hello", json!({"message_id": "h2"}),
        json!({"versions": ["9.9"], "max_frame": 100, "client": "c"}));
    let f3 = frame_of(&hello2);
    let (p3, _) = split_frame(&f3, DEFAULT_MAX_FRAME).unwrap().unwrap();
    let env3 = parse_frame_payload(p3).unwrap();
    let (v3, w3, f3) = hello_offer(&env3).unwrap();
    let err = negotiate(&v3, &f3, w3, "s".into()).unwrap_err();
    assert_eq!(err.code, "unsupported-version");
}

// ---- socket real: UDS com frames ida e volta ----

#[test]
fn unix_socket_frame_exchange() {
    use std::io::Write;
    use std::os::unix::net::{UnixListener, UnixStream};
    let dir = std::env::temp_dir().join(format!("matrix-proto-{}-{}", std::process::id(), line!()));
    let _ = std::fs::create_dir_all(&dir);
    let sp = dir.join("t.sock");
    let _ = std::fs::remove_file(&sp);
    let listener = UnixListener::bind(&sp).unwrap();
    let payload = serde_json::to_vec(&valid_envelope("session.heartbeat",
        json!({"message_id": "hb1", "session_id": "s"}), json!({}))).unwrap();
    let frame = encode(&payload, DEFAULT_MAX_FRAME).unwrap();
    let writer = std::thread::spawn({
        let sp = sp.clone();
        let frame = frame.clone();
        move || {
            let mut s = UnixStream::connect(sp).unwrap();
            s.write_all(&frame).unwrap();
            // Reads the echo.
            read_frame(&mut s, DEFAULT_MAX_FRAME).unwrap().unwrap()
        }
    });
    let (mut conn, _) = listener.accept().unwrap();
    let got = read_frame(&mut conn, DEFAULT_MAX_FRAME).unwrap().unwrap();
    assert_eq!(got, payload);
    write_frame(&mut conn, &got).unwrap();
    let echo = writer.join().unwrap();
    assert_eq!(echo, payload);
    assert_eq!(parse_frame_payload(&echo).unwrap().ty, "session.heartbeat");
    let _ = std::fs::remove_file(&sp);
}
