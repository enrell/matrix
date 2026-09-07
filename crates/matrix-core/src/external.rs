//! External execution boundary (M2.2).
//!
//! The kernel decides composition, admission, and lifecycle; hosts execute. For
//! that the kernel exposes two extension points:
//! - [`CallForwarder`]: delivers calls to external plugins (blocking,
//!   with deadline and cooperative cancellation). Without a forwarder, an external
//!   capability is an explicit error — never silent dispatch.
//! - [`LifecycleHook`]: non-blocking events (the kernel never waits for the
//!   host while holding locks); the host queues and reconciles at its own pace.
//!
//! `ExecutionKind` comes from the manifest (`execution`): `inprocess` (default) or
//! `process` (local binary with `entrypoint`, `args`, and `timeout_ms`).

use crate::identity::InstanceId;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::AtomicBool,
    Arc,
};

use crate::calls::TicketId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionKind {
    InProcess,
    External { entrypoint: String, args: Vec<String>, timeout_ms: u64 },
}

impl ExecutionKind {
    pub fn is_external(&self) -> bool {
        matches!(self, ExecutionKind::External { .. })
    }

    pub fn timeout_ms(&self) -> u64 {
        match self {
            ExecutionKind::InProcess => 0,
            ExecutionKind::External { timeout_ms, .. } => *timeout_ms,
        }
    }
}

/// Manifest `execution`. Missing = `inprocess`.
pub fn parse_execution(v: Option<&Value>) -> Result<ExecutionKind, String> {
    let Some(v) = v else { return Ok(ExecutionKind::InProcess) };
    if let Some(s) = v.as_str() {
        return match s {
            "inprocess" | "in-process" => Ok(ExecutionKind::InProcess),
            "process" => Err("'execution: process' exige objeto com entrypoint".to_string()),
            _ => Err(format!("execution desconhecido: {:?}", s)),
        };
    }
    let obj = v.as_object().ok_or_else(|| "'execution' deve ser string ou objeto".to_string())?;
    let kind = obj.get("kind").and_then(|x| x.as_str()).unwrap_or("inprocess");
    match kind {
        "inprocess" | "in-process" => Ok(ExecutionKind::InProcess),
        "process" => {
            let entrypoint = obj
                .get("entrypoint")
                .and_then(|x| x.as_str())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "execution.process exige 'entrypoint'".to_string())?;
            let args = obj
                .get("args")
                .and_then(|x| x.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let timeout_ms = obj.get("timeout_ms").and_then(|x| x.as_u64()).unwrap_or(5000);
            if timeout_ms == 0 || timeout_ms > 300_000 {
                return Err("timeout_ms fora de [1, 300000]".to_string());
            }
            Ok(ExecutionKind::External { entrypoint, args, timeout_ms })
        }
        _ => Err(format!("execution.kind desconhecido: {:?}", kind)),
    }
}

/// Delivery request to an external plugin.
#[derive(Debug, Clone)]
pub struct ForwardRequest {
    pub ticket: TicketId,
    pub cap: String,
    pub input: Value,
    pub logical: String,
    pub instance: InstanceId,
    pub generation: u64,
    pub timeout_ms: u64,
    pub cancel: Arc<AtomicBool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardError {
    Cancelled,
    Timeout,
    /// Session/process vanished mid-flight: no false success, no retry.
    Gone(String),
    Protocol(String),
}

impl ForwardError {
    pub fn code(&self) -> &'static str {
        match self {
            ForwardError::Cancelled => "cancelled",
            ForwardError::Timeout => "deadline-exceeded",
            ForwardError::Gone(_) => "outcome-unknown",
            ForwardError::Protocol(_) => "internal",
        }
    }
}

/// Transported result: success, remote business error (code
/// preserved), or delivery failure.
#[derive(Debug, Clone)]
pub enum ForwardOutcome {
    Ok(Value),
    Err { code: String, message: String },
    Failed(ForwardError),
}

/// Delivers external calls. Implemented by the host (`matrix-host`).
pub trait CallForwarder: Send + Sync {
    fn forward(&self, req: &ForwardRequest) -> ForwardOutcome;
}

/// Lifecycle events for the host (non-blocking).
#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    Activated {
        logical: String,
        instance: u64,
        generation: u64,
        entrypoint: String,
        args: Vec<String>,
        timeout_ms: u64,
    },
    Withdrawn { logical: String, instance: u64, generation: u64 },
    Removed { logical: String },
}

pub trait LifecycleHook: Send + Sync {
    fn on_lifecycle(&self, ev: LifecycleEvent);
}

/// `{sock}`/`{id}` substitution in the entrypoint args.
pub fn expand_args(args: &[String], sock: &str, id: &str) -> Vec<String> {
    args.iter()
        .map(|a| a.replace("{sock}", sock).replace("{id}", id))
        .collect()
}

/// Per-capability call policies from the manifest (re-exported here
/// so the host exposes consistent limits; parsing lives in the kernel).
pub type CallPolicies = HashMap<String, crate::calls::CallPolicy>;
