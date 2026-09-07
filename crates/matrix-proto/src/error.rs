//! Stable wire error codes (M2.1, cf. `docs/PROTOCOL.md` §errors).
//!
//! Errors include request_id, phase, code, and safe details. No generic
//! "retryable" boolean: effect retries only under the destination's contract.

/// Document codes. The kernel uses these same names where applicable;
/// internal kernel codes are mapped (detail preserved).
pub const INVALID_MESSAGE: &str = "invalid-message";
pub const UNSUPPORTED_VERSION: &str = "unsupported-version";
pub const UNAUTHENTICATED: &str = "unauthenticated";
pub const PERMISSION_DENIED: &str = "permission-denied";
pub const DEPENDENCY_UNAVAILABLE: &str = "dependency-unavailable";
pub const AMBIGUOUS_PROVIDER: &str = "ambiguous-provider";
pub const STALE_GENERATION: &str = "stale-generation";
pub const CONTEXT_NOT_ACTIVE: &str = "context-not-active";
pub const RESOURCE_EXHAUSTED: &str = "resource-exhausted";
pub const DEADLINE_EXCEEDED: &str = "deadline-exceeded";
pub const CANCELLED: &str = "cancelled";
pub const CLEANUP_PENDING: &str = "cleanup-pending";
pub const OUTCOME_UNKNOWN: &str = "outcome-unknown";
pub const INTERNAL: &str = "internal";

/// Maps internal kernel codes to the stable wire set.
/// The original code goes in `details.kernel_code`.
pub fn kernel_code_to_wire(code: &str) -> &'static str {
    match code {
        "invalid-message" | "invalid-pin" => INVALID_MESSAGE,
        "unsupported-version" => UNSUPPORTED_VERSION,
        "unauthenticated" => UNAUTHENTICATED,
        "permission-denied" => PERMISSION_DENIED,
        "dependency-unavailable" => DEPENDENCY_UNAVAILABLE,
        "ambiguous-provider" => AMBIGUOUS_PROVIDER,
        "stale-generation" | "stale-handle" => STALE_GENERATION,
        "context-not-active" | "plugin-not-active" | "plugin-not-loaded" | "no-such-capability" => {
            // Missing capability vs inactive context differ in the detail.
            if code == "no-such-capability" {
                INVALID_MESSAGE
            } else {
                CONTEXT_NOT_ACTIVE
            }
        }
        "resource-exhausted" => RESOURCE_EXHAUSTED,
        "deadline-exceeded" => DEADLINE_EXCEEDED,
        "cancelled" => CANCELLED,
        "cleanup-pending" => CLEANUP_PENDING,
        "outcome-unknown" => OUTCOME_UNKNOWN,
        _ => INTERNAL,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireError {
    pub code: String,
    pub request_id: Option<String>,
    pub phase: String,
    pub details: serde_json::Value,
}

impl WireError {
    pub fn new(code: &str, phase: &str) -> Self {
        Self {
            code: code.to_string(),
            request_id: None,
            phase: phase.to_string(),
            details: serde_json::Value::Null,
        }
    }

    pub fn with_request(mut self, id: &str) -> Self {
        self.request_id = Some(id.to_string());
        self
    }

    pub fn with_details(mut self, d: serde_json::Value) -> Self {
        self.details = d;
        self
    }

    pub fn to_value(&self) -> serde_json::Value {
        serde_json::json!({
            "code": self.code,
            "request_id": self.request_id,
            "phase": self.phase,
            "details": self.details,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_codes_map_to_stable_set() {
        let stable = [
            INVALID_MESSAGE, UNSUPPORTED_VERSION, UNAUTHENTICATED, PERMISSION_DENIED,
            DEPENDENCY_UNAVAILABLE, AMBIGUOUS_PROVIDER, STALE_GENERATION, CONTEXT_NOT_ACTIVE,
            RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED, CANCELLED, CLEANUP_PENDING, OUTCOME_UNKNOWN,
            INTERNAL,
        ];
        for k in [
            "no-such-capability", "plugin-not-active", "plugin-not-loaded", "unknown-ticket",
            "already-committed", "invalid-pin", "unknown-handle", "stale-handle",
            "already-released", "resource-pinned", "stale-generation", "context-not-active",
            "dependency-unavailable", "resource-exhausted", "deadline-exceeded", "cancelled",
            "plugin-panicked", "reducer-missing", "anything-else",
        ] {
            assert!(stable.contains(&kernel_code_to_wire(k)), "{}", k);
        }
        assert_eq!(kernel_code_to_wire("stale-generation"), STALE_GENERATION);
        assert_eq!(kernel_code_to_wire("cancelled"), CANCELLED);
    }
}
