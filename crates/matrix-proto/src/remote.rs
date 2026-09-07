//! `matrix.remote/0.1` profile (M7, wire contracts and ownership).
//!
//! Remote composition across hosts under one composition authority.
//! Layers, mirroring the local profile:
//! - structural (here): known types + required top-level identity fields;
//!   garbage keeps being dropped without killing the session;
//! - semantic (here): bounded ids, decimal-string integers, positive
//!   durations, closed `status` sets, base64 stream bytes, and no
//!   authority fabrication in payloads (grants/ancestors come from the
//!   authenticated session + delegation, never the wire).
//!
//! Normative field catalog: `docs/M7-PROFILE.md`.

use crate::error::{WireError, INVALID_MESSAGE};
use serde_json::Value;

/// Profile id (also the TLS ALPN).
pub const REMOTE_PROFILE: &str = "matrix.remote/0.1";
/// Envelope protocol/version.
pub const REMOTE_PROTOCOL_ID: &str = "matrix.remote";
pub const REMOTE_PROTOCOL_VERSION: &str = "0.1";

/// Negotiable features.
pub const REMOTE_CALLS_1: &str = "remote-calls/1";
pub const REMOTE_STREAMS_1: &str = "remote-streams/1";
pub const REMOTE_EVENTS_1: &str = "remote-events/1";
pub const REMOTE_OPS_1: &str = "remote-ops/1";
pub const REMOTE_FEATURES: &[&str] =
    &[REMOTE_CALLS_1, REMOTE_STREAMS_1, REMOTE_EVENTS_1, REMOTE_OPS_1];

/// Profile message types.
pub const TYPES: &[&str] = &[
    "session.hello",
    "session.welcome",
    "session.close",
    "heartbeat",
    "lease.renew",
    "lease.renewed",
    "call.open",
    "call.accepted",
    "call.result",
    "call.cancel",
    "stream.open",
    "stream.data",
    "stream.credit",
    "stream.complete",
    "stream.cancel",
    "event.deliver",
    "event.subscribe",
    "event.subscribed",
    "op.query",
    "op.result",
    "inventory.reconcile",
    "inventory.result",
    "revoke.notice",
];

/// Absolute transport budget per request/session handshake (ms).
pub const MAX_TRANSPORT_MS: u64 = 35_000;
/// Single-frame cap, matching the local profile.
pub const MAX_FRAME: usize = 1024 * 1024;
/// Id length cap.
pub const MAX_ID_LEN: usize = 128;

/// Payload must never fabricate authority: the caller picks only its
/// authorized binding; route, ancestors and grants are resolved and
/// revalidated by the hosts.
const FORBIDDEN_CALL_FIELDS: &[&str] = &[
    "provider",
    "grant",
    "grants",
    "principal",
    "executable",
    "entrypoint",
    "path",
    "ancestor",
    "ancestors",
];

fn bad(reason: &str) -> WireError {
    WireError::new(INVALID_MESSAGE, "remote").with_details(serde_json::json!({"reason": reason}))
}

fn bad_field(field: &str, reason: &str) -> WireError {
    WireError::new(INVALID_MESSAGE, "remote")
        .with_details(serde_json::json!({"field": field, "reason": reason}))
}

fn check_id(body: &Value, field: &str) -> Result<String, WireError> {
    match body.get(field).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= MAX_ID_LEN => Ok(s.to_string()),
        Some(_) => Err(bad_field(field, "empty or oversize id")),
        None => Err(bad_field(field, "missing id")),
    }
}

/// Decimal-string u64 (epochs, instances, generations, fences).
fn check_decimal(body: &Value, field: &str) -> Result<u64, WireError> {
    let s = body
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| bad_field(field, "decimal string required"))?;
    if s.is_empty() || s.len() > 20 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad_field(field, "decimal string required"));
    }
    s.parse::<u64>().map_err(|_| bad_field(field, "out of range"))
}

/// Positive bounded duration in ms (a remaining budget, never a clock).
fn check_budget_ms(body: &Value, field: &str) -> Result<u64, WireError> {
    match body.get(field).and_then(|v| v.as_u64()) {
        Some(n) if n > 0 && n <= MAX_TRANSPORT_MS => Ok(n),
        _ => Err(bad_field(field, "positive integer within transport budget required")),
    }
}

fn check_domain(s: &str) -> Result<(), WireError> {
    if s.is_empty()
        || s.len() > 64
        || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(bad_field("domain", "1..64 of [A-Za-z0-9_-] required"));
    }
    Ok(())
}

fn check_activation(v: &Value) -> Result<(), WireError> {
    let o = v.as_object().ok_or_else(|| bad_field("activation", "object required"))?;
    for f in ["logical", "instance", "generation"] {
        let s = o
            .get(f)
            .and_then(|x| x.as_str())
            .ok_or_else(|| bad_field("activation", "logical/instance/generation strings required"))?;
        if s.is_empty() || s.len() > MAX_ID_LEN {
            return Err(bad_field("activation", "empty or oversize member"));
        }
        if f != "logical" {
            if !s.bytes().all(|b| b.is_ascii_digit()) || s.parse::<u64>().is_err() {
                return Err(bad_field("activation", "instance/generation must be decimal"));
            }
        }
    }
    Ok(())
}

/// Required top-level fields per type (beyond protocol/version/type).
/// `body` is always required on this profile.
pub fn required_top(ty: &str) -> Option<&'static [&'static str]> {
    Some(match ty {
        "session.hello" => &["message_id", "body"],
        "session.welcome" => &["message_id", "session_id", "body"],
        "session.close" => &["message_id", "session_id", "body"],
        "heartbeat" => &["message_id", "session_id", "body"],
        "lease.renew" | "lease.renewed" => &["message_id", "session_id", "body"],
        "call.open" => &[
            "message_id",
            "session_id",
            "instance_id",
            "generation",
            "request_id",
            "body",
        ],
        "call.accepted" | "call.result" | "call.cancel" => {
            &["message_id", "session_id", "request_id", "body"]
        }
        "stream.open" => &[
            "message_id",
            "session_id",
            "instance_id",
            "generation",
            "request_id",
            "body",
        ],
        "stream.data" | "stream.credit" | "stream.complete" | "stream.cancel" => {
            &["message_id", "session_id", "body"]
        }
        "event.deliver" => &["message_id", "session_id", "instance_id", "generation", "body"],
        "event.subscribe" => &["message_id", "session_id", "request_id", "body"],
        "event.subscribed" => &["message_id", "session_id", "request_id", "body"],
        "op.query" | "op.result" => &["message_id", "session_id", "body"],
        "inventory.reconcile" | "inventory.result" => &["message_id", "session_id", "body"],
        "revoke.notice" => &["message_id", "session_id", "body"],
        _ => return None,
    })
}

fn check_base64(s: &str) -> bool {
    if s.len() > 4 * ((MAX_FRAME / 3) + 1) {
        return false;
    }
    let ok = s.bytes().all(|b| {
        b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'=' || b == b'\n' || b == b'\r'
    });
    ok && s.len() % 4 == 0
}

/// Semantic body validation per type.
pub fn validate_body(ty: &str, body: &Value) -> Result<(), WireError> {
    match ty {
        "session.hello" => {
            let versions = body.get("versions").and_then(|v| v.as_array()).ok_or_else(|| {
                bad_field("versions", "array of version strings required")
            })?;
            if versions.is_empty() || versions.len() > 8 {
                return Err(bad_field("versions", "1..8 entries required"));
            }
            for v in versions {
                match v.as_str() {
                    Some(s) if !s.is_empty() && s.len() <= 32 => {}
                    _ => return Err(bad_field("versions", "entries must be non-empty strings")),
                }
            }
            check_domain(body.get("domain").and_then(|v| v.as_str()).ok_or_else(|| {
                bad_field("domain", "missing domain")
            })?)?;
            check_id(body, "authority")?;
            check_decimal(body, "controller_epoch")?;
            Ok(())
        }
        "session.welcome" => {
            check_id(body, "version")?;
            check_decimal(body, "executor_epoch")?;
            if let Some(feats) = body.get("features") {
                let arr = feats
                    .as_array()
                    .ok_or_else(|| bad_field("features", "array of strings required"))?;
                if arr.len() > 16 {
                    return Err(bad_field("features", "too many entries"));
                }
                for f in arr {
                    match f.as_str() {
                        Some(s) if !s.is_empty() && s.len() <= 64 => {}
                        _ => return Err(bad_field("features", "entries must be non-empty strings")),
                    }
                }
            }
            Ok(())
        }
        "session.close" => {
            check_id(body, "reason")?;
            Ok(())
        }
        "heartbeat" => Ok(()),
        "lease.renew" => {
            check_id(body, "token")?;
            check_decimal(body, "fence")?;
            match body.get("seq").and_then(|v| v.as_u64()) {
                Some(_) => {}
                None => return Err(bad_field("seq", "monotonic sequence required")),
            }
            check_budget_ms(body, "ttl_ms")?;
            Ok(())
        }
        "lease.renewed" => {
            match body.get("status").and_then(|v| v.as_str()) {
                Some("ok") | Some("stale") | Some("retired") => Ok(()),
                _ => Err(bad_field("status", "must be ok, stale or retired")),
            }?;
            check_id(body, "token")?;
            match body.get("seq").and_then(|v| v.as_u64()) {
                Some(_) => Ok(()),
                None => Err(bad_field("seq", "monotonic sequence required")),
            }
        }
        "call.open" => {
            let parent = body.get("parent").ok_or_else(|| bad_field("parent", "object required"))?;
            check_activation(parent.get("activation").ok_or_else(|| {
                bad_field("parent", "parent.activation required")
            })?)?;
            check_id(parent, "ticket")?;
            check_domain(parent.get("domain").and_then(|v| v.as_str()).ok_or_else(|| {
                bad_field("parent", "parent.domain required")
            })?)?;
            let binding = check_id(body, "binding_id")?;
            if !binding.starts_with("rb-") {
                return Err(bad_field("binding_id", "remote bindings are rb-<n>"));
            }
            check_activation(body.get("activation").ok_or_else(|| {
                bad_field("activation", "consumer activation required")
            })?)?;
            check_id(body, "cap")?;
            if body.get("input").is_none() {
                return Err(bad_field("input", "missing input"));
            }
            check_budget_ms(body, "timeout_ms")?;
            check_budget_ms(body, "budget_ms")?;
            check_id(body, "lease")?;
            check_decimal(body, "grant_rev")?;
            check_id(body, "operation_id")?;
            for f in FORBIDDEN_CALL_FIELDS {
                if body.get(*f).is_some() {
                    return Err(bad_field(f, "authority must not come from payload"));
                }
            }
            Ok(())
        }
        "call.accepted" => {
            match body.get("status").and_then(|v| v.as_str()) {
                Some("admitted") => Ok(()),
                _ => Err(bad_field("status", "must be admitted")),
            }?;
            match body.get("persisted").and_then(|v| v.as_bool()) {
                Some(_) => Ok(()),
                None => Err(bad_field("persisted", "boolean required")),
            }
        }
        "call.result" => {
            match body.get("status").and_then(|v| v.as_str()) {
                Some("ok") => {
                    if body.get("output").is_none() {
                        return Err(bad_field("output", "ok requires output"));
                    }
                    Ok(())
                }
                Some("error") => {
                    let err =
                        body.get("error").and_then(|v| v.as_object()).ok_or_else(|| {
                            bad_field("error", "error requires {code, message}")
                        })?;
                    match (
                        err.get("code").and_then(|v| v.as_str()),
                        err.get("message").and_then(|v| v.as_str()),
                    ) {
                        (Some(c), Some(_)) if !c.is_empty() => Ok(()),
                        _ => Err(bad_field("error", "non-empty code and message required")),
                    }
                }
                _ => Err(bad_field("status", "must be ok or error")),
            }?;
            match body.get("terminal").and_then(|v| v.as_bool()) {
                Some(_) => Ok(()),
                None => Err(bad_field("terminal", "boolean required")),
            }
        }
        "call.cancel" => {
            check_id(body, "reason")?;
            // Stable operation the cancel targets (executor legs are
            // indexed by operation_id, not by transport request_id).
            check_id(body, "operation_id")?;
            Ok(())
        }
        "stream.open" => {
            check_id(body, "stream_id")?;
            check_id(body, "operation_id")?;
            match body.get("direction").and_then(|v| v.as_str()) {
                Some("up") | Some("down") | Some("bidi") => {}
                _ => return Err(bad_field("direction", "must be up, down or bidi")),
            }
            for f in ["max_bytes", "credit"] {
                match body.get(f).and_then(|v| v.as_u64()) {
                    Some(_) => {}
                    None => return Err(bad_field(f, "non-negative integer required")),
                }
            }
            check_activation(body.get("activation").ok_or_else(|| {
                bad_field("activation", "owner activation required")
            })?)?;
            Ok(())
        }
        "stream.data" => {
            check_id(body, "stream_id")?;
            match body.get("seq").and_then(|v| v.as_u64()) {
                Some(_) => {}
                None => return Err(bad_field("seq", "non-negative integer required")),
            }
            let bytes = body
                .get("bytes")
                .and_then(|v| v.as_str())
                .ok_or_else(|| bad_field("bytes", "base64 string required"))?;
            if !check_base64(bytes) {
                return Err(bad_field("bytes", "invalid base64 payload"));
            }
            match body.get("credit").and_then(|v| v.as_u64()) {
                Some(_) => Ok(()),
                None => Err(bad_field("credit", "remaining-credit report required")),
            }
        }
        "stream.credit" => {
            check_id(body, "stream_id")?;
            match body.get("credit").and_then(|v| v.as_u64()) {
                Some(_) => Ok(()),
                None => Err(bad_field("credit", "non-negative integer required")),
            }
        }
        "stream.complete" | "stream.cancel" => {
            check_id(body, "stream_id")?;
            match body.get("status").and_then(|v| v.as_str()) {
                Some("ok") | Some("error") | Some("cancelled") => Ok(()),
                _ => Err(bad_field("status", "must be ok, error or cancelled")),
            }
        }
        "event.deliver" => {
            check_id(body, "topic")?;
            if body.get("payload").is_none() {
                return Err(bad_field("payload", "missing payload"));
            }
            match body.get("seq").and_then(|v| v.as_u64()) {
                Some(_) => Ok(()),
                None => Err(bad_field("seq", "non-negative integer required")),
            }
        }
        "event.subscribe" | "event.subscribed" => {
            let topics = body.get("topics").and_then(|v| v.as_array()).ok_or_else(|| {
                bad_field("topics", "array of topic strings required")
            })?;
            if topics.len() > 128 {
                return Err(bad_field("topics", "at most 128 entries"));
            }
            for t in topics {
                match t.as_str() {
                    Some(s) if !s.is_empty() && s.len() <= MAX_ID_LEN => {}
                    _ => return Err(bad_field("topics", "entries must be non-empty strings")),
                }
            }
            Ok(())
        }
        "op.query" => {
            check_id(body, "principal")?;
            check_id(body, "operation_id")?;
            Ok(())
        }
        "op.result" => {
            match body.get("state").and_then(|v| v.as_str()) {
                Some("admitted") | Some("completed") | Some("unknown") => Ok(()),
                _ => Err(bad_field("state", "must be admitted, completed or unknown")),
            }?;
            Ok(())
        }
        "inventory.reconcile" => {
            for f in ["activations", "leases", "operations", "resources"] {
                if !body.get(f).and_then(|v| v.as_array()).is_some() {
                    return Err(bad_field(f, "array required"));
                }
            }
            Ok(())
        }
        "inventory.result" => {
            for f in ["revoked", "unknown"] {
                if !body.get(f).and_then(|v| v.as_array()).is_some() {
                    return Err(bad_field(f, "array required"));
                }
            }
            // Executor attested activations (registration refresh on the
            // controller). Optional so old peers still validate.
            if let Some(acts) = body.get("activations") {
                let arr = acts.as_array().ok_or_else(|| bad_field("activations", "array required"))?;
                if arr.len() > 256 {
                    return Err(bad_field("activations", "at most 256 entries"));
                }
                for a in arr {
                    check_id(a, "logical")?;
                    check_decimal(a, "instance")?;
                    check_decimal(a, "generation")?;
                }
            }
            Ok(())
        }
        "revoke.notice" => {
            check_id(body, "target")?;
            check_decimal(body, "fence")?;
            Ok(())
        }
        _ => Err(bad("unknown remote message")),
    }
}

/// `true` for answer types: they carry the `request_id` of the request
/// they answer and never reach the service handler on a miss. Any other
/// known type carrying a `request_id` is an inbound request for the
/// handler (which answers with the same `request_id`).
pub fn is_answer_type(ty: &str) -> bool {
    matches!(
        ty,
        "call.accepted"
            | "call.result"
            | "op.result"
            | "inventory.result"
            | "lease.renewed"
            | "event.subscribed"
    )
}

/// Full envelope validation: protocol/version/type, required top-level
/// identity fields (all strings, bounded), then the semantic body.
pub fn validate_envelope(v: &Value) -> Result<(), WireError> {
    let o = v.as_object().ok_or_else(|| bad("envelope must be an object"))?;
    match o.get("protocol").and_then(|x| x.as_str()) {
        Some(REMOTE_PROTOCOL_ID) => {}
        _ => return Err(bad("protocol must be matrix.remote")),
    }
    match o.get("version").and_then(|x| x.as_str()) {
        Some(REMOTE_PROTOCOL_VERSION) => {}
        _ => return Err(bad("version must be 0.1")),
    }
    let ty = o
        .get("type")
        .and_then(|x| x.as_str())
        .ok_or_else(|| bad("missing type"))?;
    let fields = required_top(ty).ok_or_else(|| bad("unknown remote message"))?;
    for f in fields {
        match o.get(*f) {
            Some(Value::String(s)) if !s.is_empty() && s.len() <= MAX_ID_LEN => {}
            Some(Value::Object(_)) if *f == "body" => {}
            _ => return Err(bad_field(f, "missing or oversize field")),
        }
    }
    // Numeric identity travels as decimal strings (local-profile rule).
    for f in ["instance_id", "generation"] {
        if let Some(x) = o.get(f) {
            match x.as_str() {
                Some(s)
                    if !s.is_empty()
                        && s.bytes().all(|b| b.is_ascii_digit())
                        && s.parse::<u64>().is_ok() => {}
                _ => return Err(bad_field(f, "decimal string required")),
            }
        }
    }
    let body = o.get("body").ok_or_else(|| bad("missing body"))?;
    validate_body(ty, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env(ty: &str, top: serde_json::Value, body: serde_json::Value) -> Value {
        let mut o = json!({
            "protocol": "matrix.remote",
            "version": "0.1",
            "type": ty,
            "message_id": "m1",
            "body": body,
        });
        for (k, v) in top.as_object().unwrap() {
            o[k] = v.clone();
        }
        o
    }

    #[test]
    fn hello_vectors() {
        let body = json!({"versions":["0.1"],"features":["remote-calls/1"],"domain":"d1","authority":"fp","controller_epoch":"7"});
        assert!(validate_body("session.hello", &body).is_ok());
        assert!(validate_envelope(&env("session.hello", json!({}), body)).is_ok());
        // Bad domain characters rejected.
        let bad = json!({"versions":["0.1"],"domain":"d 1!","authority":"fp","controller_epoch":"7"});
        assert!(validate_body("session.hello", &bad).is_err());
    }

    #[test]
    fn call_open_vector_and_forbidden_fields() {
        let good = json!({
            "parent": {"domain":"d1","ticket":"41","activation":{"logical":"cons","instance":"3","generation":"1"}},
            "binding_id": "rb-0",
            "activation": {"logical":"cons","instance":"3","generation":"1"},
            "cap": "prov.api@1",
            "input": {"v": 1},
            "timeout_ms": 5000,
            "budget_ms": 8000,
            "lease": "tok",
            "grant_rev": "9",
            "operation_id": "op-1",
        });
        assert!(validate_body("call.open", &good).is_ok());
        // Local-style binding never crosses the wire.
        let mut local = good.clone();
        local["binding_id"] = json!("bind-0");
        assert!(validate_body("call.open", &local).is_err());
        // Authority fabrication rejected.
        for f in ["provider", "grant", "principal", "ancestors"] {
            let mut b = good.clone();
            b[f] = json!("x");
            assert!(validate_body("call.open", &b).is_err(), "{}", f);
        }
        // Budgets are bounded durations.
        let mut over = good.clone();
        over["budget_ms"] = json!(60_000);
        assert!(validate_body("call.open", &over).is_err());
    }

    #[test]
    fn stream_vectors() {
        assert!(validate_body(
            "stream.open",
            &json!({"stream_id":"s1","operation_id":"op-1","direction":"bidi","max_bytes":65536,"credit":4096,
                    "activation":{"logical":"c","instance":"1","generation":"1"}})
        ).is_ok());
        assert!(validate_body(
            "stream.data",
            &json!({"stream_id":"s1","seq":0,"bytes":"aGk=","credit":4090})
        ).is_ok());
        assert!(validate_body("stream.data", &json!({"stream_id":"s1","seq":0,"bytes":"!!!","credit":1})).is_err());
        assert!(validate_body("stream.credit", &json!({"stream_id":"s1","credit":8192})).is_ok());
        assert!(validate_body("stream.complete", &json!({"stream_id":"s1","status":"ok"})).is_ok());
        assert!(validate_body("stream.cancel", &json!({"stream_id":"s1","status":"bogus"})).is_err());
    }

    #[test]
    fn lease_and_ops_vectors() {
        assert!(validate_body(
            "lease.renew",
            &json!({"token":"t","fence":"4","seq":12,"ttl_ms":5000})
        ).is_ok());
        assert!(validate_body(
            "lease.renewed",
            &json!({"status":"stale","token":"t","seq":12})
        ).is_ok());
        assert!(validate_body("op.query", &json!({"principal":"fp","operation_id":"op-9"})).is_ok());
        assert!(validate_body("op.result", &json!({"state":"unknown"})).is_ok());
        assert!(validate_body(
            "inventory.reconcile",
            &json!({"activations":[],"leases":[],"operations":[],"resources":[]})
        ).is_ok());
        assert!(validate_body("revoke.notice", &json!({"target":"prov","fence":"5"})).is_ok());
        // call.cancel targets the stable operation, not the transport id.
        assert!(validate_body("call.cancel", &json!({"reason":"x","operation_id":"op-1"})).is_ok());
        assert!(validate_body("call.cancel", &json!({"reason":"x"})).is_err());
        // Event subscriptions are explicit string arrays.
        assert!(validate_body("event.subscribe", &json!({"topics":["a.b","c"]})).is_ok());
        assert!(validate_body("event.subscribed", &json!({"topics":[]})).is_ok());
        assert!(validate_body("event.subscribe", &json!({"topics":[""]})).is_err());
        assert!(is_answer_type("event.subscribed"));
        assert!(!is_answer_type("event.subscribe"));
        // inventory.result activations are optional but shaped when present.
        assert!(validate_body("inventory.result", &json!({"revoked":[],"unknown":[]})).is_ok());
        assert!(validate_body(
            "inventory.result",
            &json!({"revoked":[],"unknown":[],"activations":[{"logical":"p","instance":"7","generation":"3"}]})
        ).is_ok());
        assert!(validate_body(
            "inventory.result",
            &json!({"revoked":[],"unknown":[],"activations":[{"logical":"p","instance":7,"generation":"3"}]})
        ).is_err());
        assert!(validate_envelope(&env(
            "call.result",
            json!({"session_id":"s","request_id":"r"}),
            json!({"status":"ok","output":{},"terminal":true})
        )).is_ok());
        // Numeric identity must be decimal strings, never numbers.
        assert!(validate_envelope(&env(
            "call.open",
            json!({"session_id":"s","instance_id":3,"generation":"1","request_id":"r"}),
            json!({})
        )).is_err());
    }
}
