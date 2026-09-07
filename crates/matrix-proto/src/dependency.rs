//! `dependency-calls/1` extension (M6.1, step 1: schema and validation).
//!
//! Calls to dependencies across external components. Layers:
//! - structural (`envelope.rs`): known types + required fields; garbage
//!   keeps being dropped without killing the session;
//! - semantic (here): non-empty ids, positive integer `timeout_ms`,
//!   closed `status`/`state`, and no authority fields in the payload
//!   (`provider`, `grant`, `principal`, executable). The host answers
//!   semantic violations with an error `dependency.result`; admission,
//!   quotas, and dispatch are later steps.

use crate::error::{WireError, INVALID_MESSAGE};
use serde_json::Value;

/// Name negotiated in `hello.body.features` / `welcome.body.features`.
pub const DEPENDENCY_CALLS_1: &str = "dependency-calls/1";

/// Extension message types (direction in parentheses).
pub const TYPES: &[&str] = &[
    "dependency.open",          // component → host
    "dependency.accepted",      // host → component
    "dependency.result",        // host → component
    "dependency.cancel",        // component → host
    "dependency.cancel.result", // host → component
];

/// Extension-specific error codes (beyond the core vocabulary).
pub const UNSUPPORTED_FEATURE: &str = "unsupported-feature";
pub const INVALID_PARENT: &str = "invalid-parent";
pub const DUPLICATE_REQUEST: &str = "duplicate-request";
pub const UNKNOWN_REQUEST: &str = "unknown-request";

/// Id length cap (mirrors the `stream_id` rule).
pub const MAX_ID_LEN: usize = 256;

/// Fields the payload must never carry: authority comes from the
/// authenticated session + binding + operator grant, never the wire
/// (M6.1 Authority section).
const FORBIDDEN_OPEN_FIELDS: &[&str] = &[
    "provider",
    "grant",
    "grants",
    "principal",
    "executable",
    "entrypoint",
    "path",
];

fn bad(reason: &str) -> WireError {
    WireError::new(INVALID_MESSAGE, "dependency").with_details(serde_json::json!({"reason": reason}))
}

fn bad_field(field: &str, reason: &str) -> WireError {
    WireError::new(INVALID_MESSAGE, "dependency")
        .with_details(serde_json::json!({"field": field, "reason": reason}))
}

/// Non-empty, bounded id (ticket decimal strings follow this rule).
fn check_id(body: &Value, field: &str) -> Result<String, WireError> {
    match body.get(field).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= MAX_ID_LEN => Ok(s.to_string()),
        Some(_) => Err(bad_field(field, "empty or oversize id")),
        None => Err(bad_field(field, "missing id")),
    }
}

/// Semantic body validation per type. Called by the host after parsing;
/// `validate_envelope` enforces structural field presence.
pub fn validate_body(ty: &str, body: &Value) -> Result<(), WireError> {
    match ty {
        "dependency.open" => {
            check_id(body, "parent_ticket")?;
            check_id(body, "binding_id")?;
            match body.get("timeout_ms").and_then(|v| v.as_u64()) {
                Some(n) if n > 0 => {}
                _ => return Err(bad_field("timeout_ms", "positive integer required")),
            }
            if !body.get("input").is_some() {
                return Err(bad_field("input", "missing input"));
            }
            for f in FORBIDDEN_OPEN_FIELDS {
                if body.get(*f).is_some() {
                    return Err(bad_field(f, "authority must not come from payload"));
                }
            }
            Ok(())
        }
        "dependency.accepted" => {
            check_id(body, "child_ticket")?;
            Ok(())
        }
        "dependency.result" => {
            match body.get("status").and_then(|v| v.as_str()) {
                Some("ok") => {
                    if body.get("output").is_none() {
                        return Err(bad_field("output", "ok requires output"));
                    }
                    Ok(())
                }
                Some("error") => {
                    let err = body.get("error").and_then(|v| v.as_object()).ok_or_else(|| {
                        bad_field("error", "error requires {code, message}")
                    })?;
                    match (err.get("code").and_then(|v| v.as_str()), err.get("message").and_then(|v| v.as_str())) {
                        (Some(c), Some(_)) if !c.is_empty() => Ok(()),
                        _ => Err(bad_field("error", "error requires non-empty code and message")),
                    }
                }
                _ => Err(bad_field("status", "must be ok or error")),
            }
        }
        "dependency.cancel" => {
            check_id(body, "target_request_id")?;
            Ok(())
        }
        "dependency.cancel.result" => {
            check_id(body, "target_request_id")?;
            match body.get("state").and_then(|v| v.as_str()) {
                Some("revoked") | Some("terminal") => Ok(()),
                _ => Err(bad_field("state", "must be revoked or terminal")),
            }
        }
        _ => Err(bad("unknown dependency message")),
    }
}

/// Extracts optional `features` from the hello body. Absent = legacy
/// session (no extensions). Present requires an array of strings.
pub fn hello_features(body: &Value) -> Result<Vec<String>, WireError> {
    let Some(v) = body.get("features") else { return Ok(vec![]) };
    let arr = v.as_array().ok_or_else(|| bad_field("features", "array of strings required"))?;
    if arr.len() > 64 {
        return Err(bad_field("features", "too many entries"));
    }
    arr.iter()
        .map(|e| match e.as_str() {
            Some(s) if !s.is_empty() && s.len() <= 128 => Ok(s.to_string()),
            _ => Err(bad_field("features", "entries must be non-empty strings")),
        })
        .collect()
}

/// Intersects negotiated with supported (supported order).
pub fn intersect(supported: &[&str], offered: &[String]) -> Vec<String> {
    supported
        .iter()
        .filter(|s| offered.iter().any(|o| o == **s))
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn valid_open_vector() {
        // Canonical vector from the spec (M6.1 Messages section).
        let body = json!({"parent_ticket":"17","binding_id":"binding-issued-by-kernel","timeout_ms":1500,"input":{"value":42}});
        assert!(validate_body("dependency.open", &body).is_ok());
    }

    #[test]
    fn valid_result_vectors() {
        assert!(validate_body("dependency.accepted", &json!({"child_ticket":"3"})).is_ok());
        assert!(validate_body("dependency.result", &json!({"status":"ok","output":{"v":1}})).is_ok());
        assert!(validate_body(
            "dependency.result",
            &json!({"status":"error","error":{"code":"cancelled","message":"revoked"}})
        ).is_ok());
        assert!(validate_body(
            "dependency.cancel.result",
            &json!({"target_request_id":"r1","state":"revoked"})
        ).is_ok());
        assert!(validate_body("dependency.cancel", &json!({"target_request_id":"r1"})).is_ok());
    }

    #[test]
    fn rejects_empty_ids_and_bad_timeout() {
        let base = json!({"parent_ticket":"17","binding_id":"b","timeout_ms":1500,"input":{}});
        for (field, val) in [("parent_ticket", json!("")), ("binding_id", json!(""))] {
            let mut b = base.clone();
            b[field] = val;
            assert!(validate_body("dependency.open", &b).is_err(), "{}", field);
        }
        for timeout in [json!(0), json!(-5), json!(1.5), json!("1500"), json!(null)] {
            let mut b = base.clone();
            b["timeout_ms"] = timeout.clone();
            assert!(validate_body("dependency.open", &b).is_err(), "{}", timeout);
        }
        let mut missing = base.clone();
        missing.as_object_mut().unwrap().remove("input");
        assert!(validate_body("dependency.open", &missing).is_err());
    }

    #[test]
    fn rejects_authority_fields_in_payload() {
        for f in ["provider", "grant", "grants", "principal", "executable", "entrypoint", "path"] {
            let mut b = json!({"parent_ticket":"17","binding_id":"b","timeout_ms":10,"input":{}});
            b[f] = json!("x");
            assert!(validate_body("dependency.open", &b).is_err(), "{}", f);
        }
    }

    #[test]
    fn rejects_bad_status_and_state() {
        assert!(validate_body("dependency.result", &json!({"status":"ok"})).is_err());
        assert!(validate_body("dependency.result", &json!({"status":"error"})).is_err());
        assert!(validate_body(
            "dependency.result",
            &json!({"status":"error","error":{"code":"","message":"m"}})
        ).is_err());
        assert!(validate_body("dependency.result", &json!({"status":"maybe"})).is_err());
        assert!(validate_body(
            "dependency.cancel.result",
            &json!({"target_request_id":"r","state":"pending"})
        ).is_err());
        assert!(validate_body("dependency.accepted", &json!({})).is_err());
        assert!(validate_body("dependency.cancel", &json!({})).is_err());
    }

    #[test]
    fn features_absent_means_legacy() {
        assert_eq!(hello_features(&json!({})).unwrap(), Vec::<String>::new());
        assert_eq!(
            hello_features(&json!({"features":["dependency-calls/1"]})).unwrap(),
            vec!["dependency-calls/1".to_string()]
        );
        assert!(hello_features(&json!({"features":"dependency-calls/1"})).is_err());
        assert!(hello_features(&json!({"features":[42]})).is_err());
        assert_eq!(
            intersect(&["dependency-calls/1"], &["other".into(), "dependency-calls/1".into()]),
            vec!["dependency-calls/1".to_string()]
        );
        assert!(intersect(&["dependency-calls/1"], &["other".into()]).is_empty());
    }
}
