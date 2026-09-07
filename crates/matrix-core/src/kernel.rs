//! M1.2 kernel: local composition with reactive dependencies.
//!
//! - Per-instance identity (`InstanceId`/`ContextId`/generation/epoch) (M1.1).
//! - Table of real resources with idempotent disposal (M1.1).
//! - Reactive requirements/bindings (M1.2): `Waiting` while dependencies
//!   dependencies, topological-order activation, cycle rejection and
//!   ambiguity, automatic consumer withdraw and reactivation in
//!   new instances when the provider returns.
//!
//! Legacy adapters (`provide`, `leases`, old error codes)
//! kept for the existing CLI; they carry no new semantics.
//! Tickets/cancellation (M1.3) and coordinated substitution (M1.4) live elsewhere.

use crate::bus::Bus;
use crate::calls::{
    CallPolicy, CallsTable, CommittedEffect, DepChild, TicketId, TicketRecord, TicketState,
    WithdrawPolicy, MAX_INFLIGHT_CALLS,
};
use crate::context::{ContextError, ContextRecord, ContextTable, DisposeClaim};
use crate::external::{
    CallForwarder, ExecutionKind, ForwardRequest, LifecycleEvent, LifecycleHook,
};
use crate::deps::{
    cap_satisfies, find_cycle, graph_edges, parse_provides, parse_requires, provider_candidates,
    split_cap, topo_order, transitive_consumers, Binding, Requirement, ResolveError,
};
use crate::fsm::Fsm;
use crate::identity::{ContextId, InstanceId, InstanceRef, ResourceHandle};
use crate::journal::Journal;
use crate::leases::Lease;
use crate::registry::Registry;
use crate::resources::{ResourceKind, ResourceTable};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Instant;

/// Registered definition (desired). Survives cascading withdraw to allow
/// reactivation; explicit removal deletes the definition.
#[derive(Debug, Clone)]
pub struct StoredManifest {
    pub id: String,
    pub caps: Vec<String>,
    pub requires: Vec<Requirement>,
    pub subs: Vec<String>,
    /// Per-operation policy (manifest `calls`; M1.3).
    pub calls: HashMap<String, CallPolicy>,
    pub reducer: String,
    pub init_state: Value,
    pub tier: String,
    pub trust: String,
    pub restart: String,
    /// Source file (for reload to reconcile removals; M1.4).
    pub source: Option<PathBuf>,
    /// How to execute: in-process or external process (M2.2).
    pub execution: ExecutionKind,
    /// Remote-only definition (M7): capability snapshot of a provider that
    /// executes on another host. Never activates locally; satisfied only
    /// by an explicit `register_remote_provider` (fail closed).
    pub remote: bool,
    /// Dependency-call policy (M6.1 step 1): declared intent
    /// plus finite limits. Absent = extension disabled
    /// (deny by default); the manifest grants itself no permission —
    /// the operator grant is verified at admission (step 2).
    pub outbound: Option<OutboundPolicy>,
}

/// Finite outbound-policy limits (M6.1): all required and
/// positive while `outbound` is present; missing limits while enabling
/// rejects the manifest.
#[derive(Debug, Clone)]
pub struct OutboundLimits {
    /// Maximum chain depth (root = 0).
    pub max_depth: u64,
    /// Simultaneous children per parent ticket.
    pub max_children_per_parent: u64,
    /// Child calls per session.
    pub max_calls_per_session: u64,
    /// Global child calls (core).
    pub max_calls_global: u64,
    /// Seen requests retained per session (no silent eviction).
    pub max_seen_requests: u64,
    /// Queued bytes per session for the extension.
    pub max_queued_bytes: u64,
    /// Maximum child deadline (ms, local monotonic).
    pub max_deadline_ms: u64,
}

/// Outbound policy declared in the manifest (M6.1 step 1).
///
/// REQUESTED permissions + finite limits. Declaring authorizes nothing:
/// effective authorization comes from the operator grant, verified at
/// admission (step 2). `outbound_policy_of` exposes only this declaration.
#[derive(Debug, Clone)]
pub struct OutboundPolicy {
    /// Requested versioned capabilities (`name@major`).
    pub requested: Vec<String>,
    pub limits: OutboundLimits,
}

/// Validates `outbound: {request, limits}`. Absent `None` = disabled.
fn parse_outbound(v: Option<&Value>) -> Result<Option<OutboundPolicy>, String> {
    let Some(v) = v else { return Ok(None) };
    let obj = v.as_object().ok_or("'outbound' deve ser objeto")?;
    let requested = obj
        .get("request")
        .and_then(|a| a.as_array())
        .ok_or("'outbound.request' deve ser array de capabilities versionadas solicitadas")?
        .iter()
        .map(|c| {
            c.as_str()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "'outbound.request' requires non-empty strings".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    for c in &requested {
        let mut parts = c.split('@');
        let (name, ver) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
        if name.is_empty()
            || ver.is_empty()
            || parts.next().is_some()
            || !ver.chars().all(|ch| ch.is_ascii_digit())
        {
            return Err(format!("invalid versioned capability in outbound.request: {:?}", c));
        }
    }
    let lim = obj
        .get("limits")
        .and_then(|l| l.as_object())
        .ok_or("'outbound.limits' is required with all finite limits")?;
    let get = |k: &str| -> Result<u64, String> {
        match lim.get(k).and_then(|x| x.as_u64()) {
            Some(n) if n > 0 => Ok(n),
            _ => Err(format!("'outbound.limits.{}' must be a positive integer", k)),
        }
    };
    Ok(Some(OutboundPolicy {
        requested,
        limits: OutboundLimits {
            max_depth: get("max_depth")?,
            max_children_per_parent: get("max_children_per_parent")?,
            max_calls_per_session: get("max_calls_per_session")?,
            max_calls_global: get("max_calls_global")?,
            max_seen_requests: get("max_seen_requests")?,
            max_queued_bytes: get("max_queued_bytes")?,
            max_deadline_ms: get("max_deadline_ms")?,
        },
    }))
}

/// Event sink for external subscribers (M6.3).
///
/// The host implements and registers it; `emit` delivers after in-process dispatch.
/// Best-effort with no kernel locks held: authority and lifecycle stay
/// decided in the kernel, never in the sink.
pub trait EventSink: Send + Sync {
    fn on_event(&self, topic: &str, payload: &Value);
}

/// External-resource cap per context (M6.3, includes activation ones).
pub const MAX_EXTERNAL_RESOURCES_PER_CONTEXT: usize = 64;

/// Prefix of the `CleanupPending` cause for in-flight work (B2).
const INFLIGHT_CAUSE_PREFIX: &str = "in-flight calls pending: ";

/// Opaque binding handle issued at activation (M6.1 step 2).
///
/// Binds the consumer's COMPLETE activation (instance + generation +
/// context) to the resolved requirement and the provider's complete
/// activation, with the exact capability. The id (`bind-N` local,
/// `rb-N` remote) carries no semantics: knowing the id authorizes
/// nothing; admission revalidates everything. Reintroducing either side
/// invalidates the handle (a new one is issued).
#[derive(Debug, Clone)]
pub struct DepBinding {
    pub id: String,
    pub consumer_logical: String,
    pub consumer_instance: InstanceId,
    pub consumer_generation: u64,
    pub consumer_context: ContextId,
    pub interface: String,
    pub capability: String,
    pub provider_logical: String,
    pub provider_instance: InstanceId,
    pub provider_generation: u64,
}

/// Child-quota reservations (M6.1 step 2). Per-parent derives from the
/// ticket index; only counter-backed quotas live here (session/global).
#[derive(Debug, Clone, Default)]
pub struct DepUsage {
    pub per_session: HashMap<String, u64>,
    pub global: u64,
}

/// Child admission request (M6.1 step 2). The caller's session and
/// activation arrive authenticated by the host; the kernel authorizes via
/// (executor == caller), current binding, and operator grant.
pub struct DepAdmit {
    pub parent: TicketId,
    pub binding: String,
    pub caller_logical: String,
    pub caller_instance: u64,
    pub caller_generation: u64,
    pub session: String,
    pub timeout_ms: u64,
}

/// Admission denial, with an extension-vocabulary code.
#[derive(Debug, Clone)]
pub struct DepDeny {
    pub code: &'static str,
    pub reason: String,
}

/// Remote provider registration (M7): attested executor-side activation
/// for a remote-only definition. The ONLY way a `remote: true` definition
/// satisfies requirements; a local definition can never be registered
/// (no hijacking local providers). Instance/generation are the
/// executor's attested values, refreshed on every attach-reconcile.
#[derive(Debug, Clone)]
pub struct RemoteProvider {
    pub logical: String,
    pub instance: u64,
    pub generation: u64,
    /// Route key: executor peer name (operator-configured both sides).
    pub peer: String,
}

/// Admitted call in progress (worker/test handle).
#[derive(Debug, Clone)]
pub struct OpenCall {
    pub ticket: TicketId,
    pub cap: String,
    pub logical: String,
    pub instance: InstanceId,
    pub generation: u64,
    pub context: ContextId,
    pub policy: CallPolicy,
    /// Mirror of the worker-observed cancel signal.
    pub cancel: Arc<AtomicBool>,
}

pub fn parse_manifest_value(v: &Value) -> Result<StoredManifest, String> {
    let id = v.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
    if id.trim().is_empty() {
        return Err("manifest without id".into());
    }
    // provides: legacy `capabilities` + new `provides` (union, no duplicates).
    let mut caps: Vec<String> = v
        .get("capabilities")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if v.get("capabilities").is_some()
        && v.get("capabilities").and_then(|x| x.as_array()).is_none()
    {
        return Err("'capabilities' deve ser array de strings".to_string());
    }
    let extra = parse_provides(v.get("provides"))?;
    for c in &extra {
        if !caps.contains(c) {
            caps.push(c.clone());
        }
    }
    if v.get("provides").is_some() && caps.is_empty() && extra.is_empty() {
        // Explicit `provides: []` = no caps; ok.
    }
    let requires = parse_requires(v.get("requires"))?;
    let subs: Vec<String> = v
        .get("subscriptions")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if v.get("subscriptions").is_some()
        && v.get("subscriptions").and_then(|x| x.as_array()).is_none()
    {
        return Err("'subscriptions' deve ser array de strings".to_string());
    }
    // Empty caps/subs after trim mean a malformed manifest.
    if let Some(arr) = v.get("capabilities").and_then(|x| x.as_array()) {
        if arr.iter().any(|x| x.as_str().map(|s| s.trim().is_empty()).unwrap_or(true)) {
            return Err("capability vazia".to_string());
        }
    }
    if let Some(arr) = v.get("subscriptions").and_then(|x| x.as_array()) {
        if arr.iter().any(|x| x.as_str().map(|s| s.trim().is_empty()).unwrap_or(true)) {
            return Err("subscription vazia".to_string());
        }
    }
    let reducer = v.get("reducer").and_then(|x| x.as_str()).unwrap_or("noop").to_string();
    let init_state = v.get("init_state").cloned().unwrap_or(json!({}));
    let tier = v.get("tier").and_then(|x| x.as_str()).unwrap_or("inproc").to_string();
    let trust = v.get("trust").and_then(|x| x.as_str()).unwrap_or("trusted").to_string();
    let restart = v.get("restart").and_then(|x| x.as_str()).unwrap_or("permanent").to_string();
    let calls = parse_call_policies(v.get("calls"))?;
    let execution = crate::external::parse_execution(v.get("execution"))?;
    let outbound = parse_outbound(v.get("outbound"))?;
    let remote = v.get("remote").and_then(|x| x.as_bool()).unwrap_or(false);
    if remote && v.get("requires").and_then(|x| x.as_array()).is_some_and(|a| !a.is_empty()) {
        return Err("remote definitions provide only; requires must be empty".into());
    }
    Ok(StoredManifest {
        id,
        caps,
        requires,
        subs,
        calls,
        reducer,
        init_state,
        tier,
        trust,
        restart,
        source: None,
        execution,
        outbound,
        remote,
    })
}

/// Per-operation policy from `calls: {cap: {on_withdraw, drain_ms}}`.
fn parse_call_policies(v: Option<&Value>) -> Result<HashMap<String, CallPolicy>, String> {
    let Some(v) = v else { return Ok(HashMap::new()) };
    let obj = v.as_object().ok_or_else(|| "'calls' must be a cap→policy object".to_string())?;
    let mut out = HashMap::new();
    for (cap, pv) in obj {
        if cap.trim().is_empty() {
            return Err("cap vazia em 'calls'".to_string());
        }
        let mut pol = CallPolicy::default();
        if let Some(o) = pv.get("on_withdraw").and_then(|x| x.as_str()) {
            pol.on_withdraw = WithdrawPolicy::parse(o)
                .ok_or_else(|| format!("invalid on_withdraw in '{}': {:?}", cap, o))?;
        }
        if let Some(d) = pv.get("drain_ms").and_then(|x| x.as_u64()) {
            pol.drain_ms = d;
        } else if pv.get("drain_ms").is_some() {
            return Err(format!("invalid drain_ms in '{}'", cap));
        }
        out.insert(cap.clone(), pol);
    }
    Ok(out)
}

fn provides_of(defs: &HashMap<String, StoredManifest>) -> HashMap<String, Vec<String>> {
    defs.iter().map(|(k, d)| (k.clone(), d.caps.clone())).collect()
}

fn requires_of(defs: &HashMap<String, StoredManifest>) -> HashMap<String, Vec<Requirement>> {
    defs.iter().map(|(k, d)| (k.clone(), d.requires.clone())).collect()
}

#[derive(Debug, Clone)]
pub struct Plugin {
    pub id: String,
    pub state: Fsm,
    pub tier: String,
    pub trust: String,
    pub generation: u64,
    pub restart_policy: String,
    pub caps: Vec<String>,
    pub subs: Vec<String>,
    /// Declared requirements (definition copy; I12 diagnostics).
    pub requires: Vec<Requirement>,
    /// Bindings resolved for the current instance (I04).
    pub bindings: Vec<Binding>,
    /// Per-operation policy in force on this instance (M1.3).
    pub calls: HashMap<String, CallPolicy>,
    /// How to execute this instance (M2.2).
    pub execution: ExecutionKind,
    /// Visible reason while `Waiting` (C04/C19).
    pub wait_cause: Option<String>,
    pub reducer: String,
    pub init_state: Value,
    pub json_state: Value,
    /// Legacy: kept for compat; real ownership lives in `resources`.
    pub leases: Vec<Lease>,
    pub panic_count: u32,
    pub restart_times: Vec<Instant>,
    // --- M1.1: instance identity ---
    pub instance_id: u64,
    pub context_id: u64,
    pub epoch: u64,
    /// Handles acquired by the instance (acquisition order).
    pub resources: Vec<ResourceHandle>,
}

impl Plugin {
    pub fn restart_policy_str(&self) -> &str {
        &self.restart_policy
    }

    pub fn instance_ref(&self) -> InstanceRef {
        InstanceRef::new(self.epoch, self.instance_id, self.context_id, &self.id, self.generation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisposeOutcome {
    Disposed,
    AlreadyDisposed,
    CleanupPending { reason: String },
}

impl DisposeOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            DisposeOutcome::Disposed => "Disposed",
            DisposeOutcome::AlreadyDisposed => "AlreadyDisposed",
            DisposeOutcome::CleanupPending { .. } => "CleanupPending",
        }
    }
}

pub struct Kernel {
    pub plugins: Mutex<HashMap<String, Plugin>>,
    pub caps: Registry,
    pub bus: Bus,
    pub journal: Journal,
    pub journal_path: PathBuf,
    pub plugins_dir: PathBuf,
    pub quit: Arc<AtomicBool>,
    pub dry_run: bool,
    pub contexts: ContextTable,
    pub resources: ResourceTable,
    /// Desired definitions (manifests). Cascading withdraw preserves;
    /// explicit removal deletes.
    pub definitions: Mutex<HashMap<String, StoredManifest>>,
    /// In-flight calls, confirmed tickets and effects (M1.3).
    pub calls: CallsTable,
    /// External call delivery (M2.2; absent = explicit error).
    pub forwarder: Mutex<Option<Arc<dyn CallForwarder>>>,
    /// Remote-leg delivery (M7 route manager; absent = explicit error).
    /// Separate from `forwarder` so route teardown never affects local
    /// execution.
    pub remote_forwarder: Mutex<Option<Arc<dyn CallForwarder>>>,
    /// Lifecycle events for the host (M2.2; non-blocking).
    pub hook: Mutex<Option<Arc<dyn LifecycleHook>>>,
    /// Opaque binding handles per activation (M6.1 step 2).
    pub dep_bindings: Mutex<HashMap<String, DepBinding>>,
    /// Opaque id sequence (`bind-N`).
    pub dep_seq: AtomicU64,
    /// Operator grants: (consumer, capability) → revision (step 2).
    /// Independent of `outbound.request` (declared intent).
    pub outbound_grants: Mutex<HashMap<(String, String), u64>>,
    /// Remote provider activations attested by the route manager (M7):
    /// logical → executor-side activation + peer. Only `remote: true`
    /// definitions can be registered.
    pub remote_providers: Mutex<HashMap<String, RemoteProvider>>,
    /// Grant revision sequence.
    pub grant_seq: AtomicU64,
    /// Child-quota reservations (session/global).
    pub dep_usage: Mutex<DepUsage>,
    /// Serializes child admissions (short, no I/O; always first).
    pub dep_admit_lock: Mutex<()>,
    /// Event sink for external sessions (M6.3; no retention).
    pub event_sink: Mutex<Option<Arc<dyn EventSink>>>,
}

impl Kernel {
    pub fn new(home: &PathBuf, journal: Journal, dry_run: bool) -> Self {
        Self {
            plugins: Mutex::new(HashMap::new()),
            caps: Registry::new(),
            bus: Bus::new(),
            journal,
            journal_path: home.join("run/journal.jsonl"),
            plugins_dir: home.join("plugins"),
            quit: Arc::new(AtomicBool::new(false)),
            dry_run,
            contexts: ContextTable::new(),
            resources: ResourceTable::new(),
            definitions: Mutex::new(HashMap::new()),
            calls: CallsTable::new(),
            forwarder: Mutex::new(None),
            remote_forwarder: Mutex::new(None),
            hook: Mutex::new(None),
            dep_bindings: Mutex::new(HashMap::new()),
            dep_seq: AtomicU64::new(1),
            outbound_grants: Mutex::new(HashMap::new()),
            remote_providers: Mutex::new(HashMap::new()),
            grant_seq: AtomicU64::new(1),
            dep_usage: Mutex::new(DepUsage::default()),
            dep_admit_lock: Mutex::new(()),
            event_sink: Mutex::new(None),
        }
    }

    /// Registers the external-call deliverer (local host, M2.2).
    /// A remote-call deliverer (M7 route) is set separately and never
    /// replaces this one.
    pub fn set_forwarder(&self, f: Arc<dyn CallForwarder>) {
        *self.forwarder.lock() = Some(f);
    }

    /// Registers the deliverer for M7 remote legs (route manager only).
    /// Kept apart from the local host forwarder so withdrawing the route
    /// can never strand or hijack local execution.
    pub fn set_remote_forwarder(&self, f: Arc<dyn CallForwarder>) {
        *self.remote_forwarder.lock() = Some(f);
    }

    /// Clears the remote-leg deliverer (route teardown). In-flight remote
    /// legs keep their tickets; terminal validation fails them closed.
    pub fn clear_remote_forwarder(&self) {
        *self.remote_forwarder.lock() = None;
    }

    /// Registers the lifecycle observer (local host, M2.2).
    pub fn set_hook(&self, h: Arc<dyn LifecycleHook>) {
        *self.hook.lock() = Some(h);
    }

    fn emit_hook(&self, ev: LifecycleEvent) {
        // Non-blocking by contract: the impl enqueues; the kernel never
        // waits for the host (and never with retained locks — callers ensure it).
        if let Some(h) = self.hook.lock().clone() {
            h.on_lifecycle(ev);
        }
    }

    fn emit_activated(&self, logical: &str, instance: u64, generation: u64) {
        let def = self.definitions.lock().get(logical).cloned();
        let Some(def) = def else { return };
        if let ExecutionKind::External { entrypoint, args, timeout_ms } = def.execution {
            self.emit_hook(LifecycleEvent::Activated {
                logical: logical.to_string(),
                instance,
                generation,
                entrypoint,
                args,
                timeout_ms,
            });
        }
    }

    pub fn epoch(&self) -> u64 {
        self.contexts.epoch()
    }

    /// Adaptador legado preservado.
    pub fn provide(&self, cap: &str, fiber: &str) {
        self.caps.provide(cap, fiber);
    }

    pub fn instance_ref_of(&self, logical: &str) -> Option<InstanceRef> {
        self.contexts.current(logical).map(|r| r.reference())
    }

    pub fn instance_of(&self, logical: &str) -> Option<InstanceId> {
        self.contexts.current(logical).map(|r| r.instance)
    }

    pub fn waiting_reason_of(&self, logical: &str) -> Option<String> {
        self.contexts.current(logical).and_then(|r| r.cause)
    }

    pub fn bindings_of(&self, logical: &str) -> Vec<Binding> {
        self.plugins.lock().get(logical).map(|p| p.bindings.clone()).unwrap_or_default()
    }

    /// Per-operation policy (manifest `calls` or the M1.3 default).
    pub fn policy_for(&self, logical: &str, cap: &str) -> CallPolicy {
        self.plugins
            .lock()
            .get(logical)
            .and_then(|p| p.calls.get(cap).copied())
            .unwrap_or_default()
    }

    /// Component-declared outbound policy (M6.1 step 1):
    /// REQUESTED permissions + limits. Absent = extension disabled
    /// for it (deny by default). Present is NOT authorization: no
    /// operator grant is verified here (step 2 admission verifies).
    pub fn outbound_policy_of(&self, logical: &str) -> Option<OutboundPolicy> {
        self.definitions
            .lock()
            .get(logical)
            .and_then(|d| d.outbound.clone())
    }

    // ---- external-component resources and events (M6.3) ----
    //
    // External acquisition runs the core's `acquire` (same
    // validations and per-activation ownership), plus a per-context cap and
    // rollback before publish: never publishes above the cap. Events
    // arrive via the host-registered `EventSink` (best-effort delivery;
    // authority stays in the kernel).

    /// Registers the host event sink (delivery to subscribers).
    /// Called with no kernel locks held by the sink.
    pub fn set_event_sink(&self, sink: Option<Arc<dyn EventSink>>) {
        *self.event_sink.lock() = sink;
    }

    /// Manifest-declared topics (activation subscriptions).
    pub fn subscriptions_of(&self, logical: &str) -> Vec<String> {
        self.definitions
            .lock()
            .get(logical)
            .map(|d| d.subs.clone())
            .unwrap_or_default()
    }

    /// Acquires a resource for a component's activation from its bound
    /// session (M6.3). The caller passes the exact (instance, generation)
    /// it was authenticated for: anything else fails closed, so a stale
    /// session's request can never land on a new generation. Same rules as
    /// `acquire` plus a per-context external-resource cap; above the cap,
    /// rolls back before publishing (no residue). Publication precedes the
    /// final revalidation exactly like `acquire`/B3, so withdraw racing
    /// publication is revoked rather than leaked.
    pub fn acquire_external(
        &self,
        logical: &str,
        kind: ResourceKind,
        caller_instance: InstanceId,
        caller_generation: u64,
    ) -> Result<ResourceHandle, String> {
        let cur = self
            .contexts
            .current(logical)
            .ok_or_else(|| "plugin-not-loaded".to_string())?;
        if cur.instance != caller_instance || cur.generation != caller_generation {
            return Err("stale-generation".to_string());
        }
        self.contexts.require_active(cur.instance).map_err(|e| match e {
            ContextError::NotActive { .. } => "context-not-active".to_string(),
            ContextError::StaleGeneration { .. } => "stale-generation".to_string(),
            _ => "plugin-not-loaded".to_string(),
        })?;
        let handle = self
            .resources
            .register(cur.context, cur.instance, &cur.logical, cur.generation, kind.clone())
            .map_err(|e| e.to_string())?;
        if self.resources.active_for(cur.context) > MAX_EXTERNAL_RESOURCES_PER_CONTEXT {
            self.resources.rollback_register(handle);
            return Err("resource-exhausted".to_string());
        }
        match &kind {
            ResourceKind::Cap { name } => {
                let r = cur.reference();
                self.caps.provide_instance(name, &r);
            }
            ResourceKind::Sub { topic } => {
                self.bus.subscribe_instance(&cur.logical, cur.instance, topic);
            }
            ResourceKind::Timer { .. }
            | ResourceKind::Task { .. }
            | ResourceKind::FailRelease { .. } => {}
            ResourceKind::FailAcquire { .. } => {
                unreachable!("register rejects FailAcquire before publishing")
            }
        }
        // Post-publication revalidation (`acquire`/B3 mirror): withdraw
        // racing publication revokes and rolls back instead of leaking.
        let fresh = self.contexts.current(logical);
        let ok = fresh.as_ref().is_some_and(|c| {
            c.instance == cur.instance
                && c.generation == cur.generation
                && c.state.canonical() == Fsm::Active
        });
        if !ok {
            match &kind {
                ResourceKind::Cap { name } => {
                    self.caps.revoke_cap_if_owned(name, cur.instance);
                }
                ResourceKind::Sub { topic } => {
                    self.bus.unsubscribe_topic_if_owned(cur.instance, topic);
                }
                _ => {}
            }
            self.resources.rollback_register(handle);
            self.journal.append(
                logical,
                "resource.rejected",
                json!({
                    "handle": handle.0,
                    "kind": kind.kind_name(),
                    "label": kind.label(),
                    "instance": cur.instance.0,
                    "generation": cur.generation,
                    "reason": "acquire-race",
                }),
                Value::Null,
            );
            return Err("stale-generation".to_string());
        }
        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(logical) {
                if p.instance_id == cur.instance.0 && p.generation == cur.generation {
                    p.resources.push(handle);
                }
            }
        }
        self.journal.append(
            logical,
            "resource.acquired",
            json!({
                "handle": handle.0,
                "kind": kind.kind_name(),
                "label": kind.label(),
                "instance": cur.instance.0,
                "context": cur.context.0,
                "generation": cur.generation,
                "external": true,
            }),
            Value::Null,
        );
        Ok(handle)
    }

    // ---- dependencies across external components (M6.1 step 2) ----
    //
    // Coordinator: validates parent, activations, binding, grant, quotas;
    // reserves and registers the child before any publication; revalidates
    // after registering (withdraw/revoke race). Lock order: the
    // admission mutex (`dep_admit_lock`) is ALWAYS first; inside it only
    // short acquisitions without I/O. No other path takes it.

    /// Opaque handles of the consumer's current activation (for the host
    /// to deliver in `lifecycle.activate` in step 3; tests use it directly).
    pub fn dependency_bindings_of(&self, logical: &str) -> Vec<DepBinding> {
        let cur = self.contexts.current(logical);
        let Some(cur) = cur else { return vec![] };
        self.dep_bindings
            .lock()
            .values()
            .filter(|b| {
                b.consumer_logical == logical
                    && b.consumer_instance == cur.instance
                    && b.consumer_generation == cur.generation
            })
            .cloned()
            .collect()
    }

    /// Registers a remote provider activation (M7, route manager only).
    /// Fails closed unless a `remote: true` definition with matching caps
    /// is present: local providers can never be registered. Refreshing
    /// overwrites (new attach-reconcile wins); dependents re-resolve on
    /// the next `reconcile()`.
    pub fn register_remote_provider(
        &self,
        logical: &str,
        instance: u64,
        generation: u64,
        peer: &str,
    ) -> Result<(), String> {
        let remote = self
            .definitions
            .lock()
            .get(logical)
            .is_some_and(|d| d.remote);
        if !remote {
            return Err("not a remote definition".to_string());
        }
        if peer.is_empty() || peer.len() > 128 {
            return Err("invalid peer".to_string());
        }
        self.remote_providers.lock().insert(
            logical.to_string(),
            RemoteProvider {
                logical: logical.to_string(),
                instance,
                generation,
                peer: peer.to_string(),
            },
        );
        self.journal.append(
            logical,
            "remote.registered",
            json!({"instance": instance, "generation": generation, "peer": peer}),
            Value::Null,
        );
        self.reconcile();
        Ok(())
    }

    /// Removes a remote registration (route teardown). Dependents fall
    /// back to `Waiting` on the next `reconcile()`; in-flight remote
    /// legs are settled by host workers observing the loss (same as
    /// local provider-session loss).
    pub fn unregister_remote_provider(&self, logical: &str) -> bool {
        let removed = self.remote_providers.lock().remove(logical).is_some();
        if removed {
            // Prune by logical (remote instances may collide numerically
            // with local ones; `prune_dep_bindings` cannot tell them apart).
            self.dep_bindings.lock().retain(|_, b| b.provider_logical != logical);
            self.journal.append(logical, "remote.unregistered", json!({}), Value::Null);
            self.reconcile();
        }
        removed
    }

    /// Remote registration snapshot for routing (host).
    pub fn remote_provider_of(&self, logical: &str) -> Option<RemoteProvider> {
        self.remote_providers.lock().get(logical).cloned()
    }

    /// Logicals registered via one route peer (route-table pruning).
    pub fn remote_providers_of_peer(&self, peer: &str) -> Vec<String> {
        self.remote_providers
            .lock()
            .values()
            .filter(|r| r.peer == peer)
            .map(|r| r.logical.clone())
            .collect()
    }

    /// Synthetic `Active` record for a registered remote provider, so
    /// requirement resolution and staleness checks see one activation.
    /// Never inserted into the context table (no local lifecycle).
    fn remote_state(&self, logical: &str) -> Option<ContextRecord> {
        let reg = self.remote_providers.lock().get(logical).cloned()?;
        if !self.definitions.lock().contains_key(logical) {
            return None;
        }
        Some(ContextRecord {
            context: ContextId(u64::MAX),
            instance: InstanceId(reg.instance),
            logical: reg.logical,
            generation: reg.generation,
            epoch: self.epoch(),
            state: Fsm::Active,
            parent: None,
            cause: None,
        })
    }

    /// Issues one opaque handle per resolved requirement of the activation
    /// that just became `Active`. Called on both activation paths.
    fn issue_dep_bindings(
        &self,
        logical: &str,
        inst: InstanceId,
        generation: u64,
        bindings: &[Binding],
    ) {
        let Some(cur) = self.contexts.get_by_instance(inst) else { return };
        if cur.logical != logical || cur.generation != generation {
            return;
        }
        let defs = self.definitions.lock().clone();
        let mut out = self.dep_bindings.lock();
        // Reissuing clears the activation's handles (never reuses ids).
        out.retain(|_, b| b.consumer_instance != inst);
        for b in bindings {
            let cap = defs.get(&b.provider_logical).and_then(|d| {
                let (base, major) = split_cap(&b.interface);
                let req = Requirement {
                    interface: b.interface.clone(),
                    base,
                    major,
                    provider: Some(b.provider_logical.clone()),
                };
                d.caps.iter().find(|c| cap_satisfies(c, &req)).cloned()
            });
            let Some(capability) = cap else { continue };
            let n = self.dep_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Remote providers get `rb-` handles (M7 wire rule); local
            // ones keep `bind-`. One shared sequence: ids never collide.
            let remote = defs.get(&b.provider_logical).is_some_and(|d| d.remote);
            let id = if remote { format!("rb-{n}") } else { format!("bind-{n}") };
            out.insert(
                id.clone(),
                DepBinding {
                    id,
                    consumer_logical: logical.to_string(),
                    consumer_instance: inst,
                    consumer_generation: generation,
                    consumer_context: cur.context,
                    interface: b.interface.clone(),
                    capability,
                    provider_logical: b.provider_logical.clone(),
                    provider_instance: InstanceId(b.provider_instance),
                    provider_generation: b.provider_generation,
                },
            );
        }
    }

    /// Removes handles involving the discarded instance (the dispose
    /// winner calls it; admission revalidates anyway).
    fn prune_dep_bindings(&self, inst: InstanceId) {
        // Remote bindings point at attested executor-side instances that
        // may collide numerically with local ones: never prune by a
        // registered remote provider's instance.
        let remote: HashSet<String> = self.remote_providers.lock().keys().cloned().collect();
        self.dep_bindings.lock().retain(|_, b| {
            b.consumer_instance != inst
                && (remote.contains(&b.provider_logical) || b.provider_instance != inst)
        });
    }

    /// Operator grant (consumer, exact capability). Authority source
    /// independent of `outbound.request`. Re-grant rotates the
    /// revision and revokes superseded-revision children. Returns the
    /// as revoked, for operator visibility.
    pub fn grant_outbound(&self, consumer: &str, cap: &str) -> (u64, Vec<TicketRecord>) {
        let rev = self.grant_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.outbound_grants
            .lock()
            .insert((consumer.to_string(), cap.to_string()), rev);
        let revoked = self
            .calls
            .revoke_grant_children(consumer, cap, Some(rev), "grant-superseded");
        for t in &revoked {
            self.journal.append(
                &t.logical,
                "call.cancelled",
                json!({"ticket": t.id.0, "reason": "grant-superseded"}),
                Value::Null,
            );
        }
        (rev, revoked)
    }

    /// Revokes an operator grant: removes the authorization and invalidates
    /// admitted under it. Returns the invalidated ones.
    pub fn revoke_outbound(&self, consumer: &str, cap: &str) -> Vec<TicketRecord> {
        self.outbound_grants
            .lock()
            .remove(&(consumer.to_string(), cap.to_string()));
        let revoked = self
            .calls
            .revoke_grant_children(consumer, cap, None, "grant-revoked");
        for t in &revoked {
            self.journal.append(
                &t.logical,
                "call.cancelled",
                json!({"ticket": t.id.0, "reason": "grant-revoked"}),
                Value::Null,
            );
        }
        revoked
    }

    /// Releases a finished child's quota reservation (idempotent, saturating).
    fn dep_release(&self, session: &str) {
        let mut u = self.dep_usage.lock();
        if let Some(c) = u.per_session.get_mut(session) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                u.per_session.remove(session);
            }
        }
        u.global = u.global.saturating_sub(1);
    }

    /// Revokes open descendants of a finished ticket + journals.
    /// Called on every ticket end (close/cancel/reap/settle).
    fn revoke_ticket_descendants(&self, parent: TicketId, reason: &str) {
        for t in self.calls.revoke_descendants(parent, reason) {
            self.journal.append(
                &t.logical,
                "call.cancelled",
                json!({"ticket": t.id.0, "reason": reason}),
                Value::Null,
            );
        }
    }

    /// Admits a dependency child (coordinated operation). Validates parent
    /// (Admitted, executor == caller), current opaque binding,
    /// `Active` and current, consumer policy, depth, operator
    /// grant and quotas; reserves and registers; revalidates after registering
    /// (if withdraw/revoke won, rolls back and denies — no residue).
    pub fn dependency_admit(&self, req: &DepAdmit) -> Result<TicketId, DepDeny> {
        let deny = |code: &'static str, reason: String| DepDeny { code, reason };
        if req.timeout_ms == 0 {
            return Err(deny("invalid-message", "timeout must be positive".into()));
        }
        if req.session.is_empty() || req.binding.is_empty() {
            return Err(deny("invalid-message", "session and binding required".into()));
        }
        let caller = InstanceId(req.caller_instance);
        let _guard = self.dep_admit_lock.lock();
        // Parent: exists, is open, and runs as exactly the caller.
        let parent = self.calls.get(req.parent).ok_or_else(|| {
            deny("invalid-parent", format!("unknown parent tkt-{}", req.parent.0))
        })?;
        if parent.state != TicketState::Admitted {
            return Err(deny(
                "invalid-parent",
                format!("parent {} is {}", req.parent.0, parent.state.as_str()),
            ));
        }
        if parent.logical != req.caller_logical
            || parent.instance != caller
            || parent.generation != req.caller_generation
        {
            return Err(deny("invalid-parent", "parent owned by another activation".into()));
        }
        // Opaque binding: exists and belongs to this activation (foreign handles authorize nothing).
        let b = self
            .dep_bindings
            .lock()
            .get(&req.binding)
            .cloned()
            .ok_or_else(|| deny("dependency-unavailable", "unknown binding".into()))?;
        if b.consumer_logical != req.caller_logical
            || b.consumer_instance != caller
            || b.consumer_generation != req.caller_generation
        {
            return Err(deny("invalid-parent", "binding of another activation".into()));
        }
        // Current, Active activations on both sides.
        let ccur = self.contexts.current(&req.caller_logical).ok_or_else(|| {
            deny("plugin-not-loaded", format!("consumer {} gone", req.caller_logical))
        })?;
        if ccur.instance != caller || ccur.state.canonical() != Fsm::Active {
            return Err(deny("context-not-active", "consumer not active".into()));
        }
        if ccur.generation != req.caller_generation {
            return Err(deny("stale-generation", "consumer superseded".into()));
        }
        // Provider side: local Active activation, or — when no local
        // activation exists — an attested remote registration matching
        // the binding (M7). Remote legs account the ticket to the
        // consumer's context (no local provider lifecycle exists; the
        // executor side tracks its own).
        let (ticket_ctx, remote_peer) = match self.contexts.current(&b.provider_logical) {
            Some(pcur) => {
                if pcur.instance != b.provider_instance
                    || pcur.generation != b.provider_generation
                    || pcur.state.canonical() != Fsm::Active
                {
                    return Err(deny(
                        "dependency-unavailable",
                        "provider reintroduced; binding stale".into(),
                    ));
                }
                (pcur.context, None)
            }
            None => {
                let reg = self.remote_providers.lock().get(&b.provider_logical).cloned();
                let Some(reg) = reg else {
                    return Err(deny(
                        "dependency-unavailable",
                        format!("provider {} gone", b.provider_logical),
                    ));
                };
                if reg.instance != b.provider_instance.0
                    || reg.generation != b.provider_generation
                {
                    return Err(deny(
                        "dependency-unavailable",
                        "provider reintroduced; binding stale".into(),
                    ));
                }
                if !self
                    .definitions
                    .lock()
                    .get(&b.provider_logical)
                    .is_some_and(|d| d.remote)
                {
                    return Err(deny(
                        "dependency-unavailable",
                        "remote definition withdrawn".into(),
                    ));
                }
                (ccur.context, Some(reg.peer))
            }
        };
        // Consumer policy (intent + limits) and depth.
        let policy = self
            .definitions
            .lock()
            .get(&req.caller_logical)
            .and_then(|d| d.outbound.clone())
            .ok_or_else(|| deny("permission-denied", "no outbound policy".into()))?;
        let depth = parent.dep.as_ref().map(|d| d.depth + 1).unwrap_or(1);
        if depth as u64 > policy.limits.max_depth {
            return Err(deny("resource-exhausted", "max depth exceeded".into()));
        }
        // Operator grant, with captured revision (revalidated at boundaries).
        let rev = self
            .outbound_grants
            .lock()
            .get(&(req.caller_logical.clone(), b.capability.clone()))
            .copied()
            .ok_or_else(|| {
                deny(
                    "permission-denied",
                    format!("no operator grant for {} on {}", req.caller_logical, b.capability),
                )
            })?;
        // Quotas: parent, session, global.
        if self.calls.open_children_of(req.parent).len() as u64
            >= policy.limits.max_children_per_parent
        {
            return Err(deny("resource-exhausted", "too many children of parent".into()));
        }
        {
            let mut u = self.dep_usage.lock();
            if u.per_session.get(&req.session).copied().unwrap_or(0)
                >= policy.limits.max_calls_per_session
            {
                return Err(deny("resource-exhausted", "too many dependency calls in session".into()));
            }
            if u.global >= policy.limits.max_calls_global {
                return Err(deny("resource-exhausted", "too many dependency calls".into()));
            }
            if self.calls.inflight_count() >= MAX_INFLIGHT_CALLS {
                return Err(deny("resource-exhausted", "too many in-flight calls".into()));
            }
            *u.per_session.entry(req.session.clone()).or_insert(0) += 1;
            u.global += 1;
        }
        // Register: provider ticket + metadata + deadline.
        let release = |this: &Self| this.dep_release(&req.session);
        let now = Instant::now();
        let cap_deadline = now + std::time::Duration::from_millis(policy.limits.max_deadline_ms);
        let req_deadline = now + std::time::Duration::from_millis(req.timeout_ms);
        let deadline = [cap_deadline, req_deadline]
            .into_iter()
            .chain(parent.drain_until)
            .min()
            .unwrap_or(req_deadline);
        let cancel = Arc::new(AtomicBool::new(false));
        let id = self.calls.alloc(
            &b.capability,
            &b.provider_logical,
            b.provider_instance,
            b.provider_generation,
            ticket_ctx,
            self.epoch(),
            CallPolicy::cancel(),
            vec![],
            cancel,
        );
        self.calls.set_drain_until(id, deadline);
        if !self.calls.set_dep_meta(
            id,
            DepChild {
                parent: req.parent,
                binding: b.id.clone(),
                consumer: caller,
                session: req.session.clone(),
                grant_consumer: req.caller_logical.clone(),
                grant_cap: b.capability.clone(),
                grant_rev: rev,
                depth,
                remote_peer: remote_peer.clone(),
            },
        ) {
            self.calls.remove(id);
            release(self);
            return Err(deny("internal", "ticket vanished during admission".into()));
        }
        // Post-registration revalidation: if withdraw/revoke won midway,
        // rolls back everything and denies (no ticket or reservation residue).
        // Remote legs revalidate the registration (not a local context).
        let provider_live = match &remote_peer {
            None => self.contexts.current(&b.provider_logical).is_some_and(|c| {
                c.instance == b.provider_instance && c.state.canonical() == Fsm::Active
            }),
            Some(_) => self
                .remote_providers
                .lock()
                .get(&b.provider_logical)
                .is_some_and(|r| {
                    r.instance == b.provider_instance.0
                        && r.generation == b.provider_generation
                })
                && self
                    .definitions
                    .lock()
                    .get(&b.provider_logical)
                    .is_some_and(|d| d.remote),
        };
        let still_valid = self.calls.get(req.parent).is_some_and(|p| p.state == TicketState::Admitted)
            && self
                .contexts
                .current(&req.caller_logical)
                .is_some_and(|c| c.instance == caller && c.state.canonical() == Fsm::Active)
            && provider_live
            && self
                .outbound_grants
                .lock()
                .get(&(req.caller_logical.clone(), b.capability.clone()))
                .copied()
                == Some(rev);
        if !still_valid {
            self.calls.remove(id);
            release(self);
            self.journal.append(
                &req.caller_logical,
                "dependency.rejected",
                json!({"binding": b.id, "reason": "revoked-during-admission"}),
                Value::Null,
            );
            return Err(deny("dependency-unavailable", "revoked during admission".into()));
        }
        self.journal.append(
            &req.caller_logical,
            "dependency.admitted",
            json!({
                "ticket": id.0, "parent": req.parent.0, "binding": b.id,
                "capability": b.capability, "provider": b.provider_logical,
                "depth": depth, "grant_rev": rev,
                "remote": remote_peer.is_some(),
                "peer": remote_peer.clone().unwrap_or_default(),
            }),
            Value::Null,
        );
        Ok(id)
    }

    // ---- in-flight calls: mediated tickets, drain and commits (M1.3) ----

    /// Admission core shared by `invoke` and `call_open` (I02–I04).
    /// Returns the owning logical + active, current context record.
    fn admission_record(&self, cap: &str) -> Result<(String, ContextRecord), (Value, bool)> {
        let r = self.caps.resolve_ref(cap);
        let Some(r) = r else {
            // Cap from a known but unpublished definition? Report a
            // dependency instead of a generic "missing" (C04/C19).
            let owner: Option<String> = self
                .definitions
                .lock()
                .iter()
                .find(|(_, d)| d.caps.iter().any(|c| c == cap))
                .map(|(k, _)| k.clone());
            if let Some(o) = owner {
                if let Some(st) = self.contexts.current(&o) {
                    match st.state.canonical() {
                        Fsm::Waiting | Fsm::Preparing | Fsm::Registered => {
                            let reason = st.cause.clone().unwrap_or_else(|| "dependency unavailable".to_string());
                            return Err((
                                json!({"error": reason, "code": "dependency-unavailable", "logical": o}),
                                false,
                            ));
                        }
                        Fsm::Quiescing | Fsm::CleanupPending | Fsm::Disposed | Fsm::Failed => {
                            return Err((
                                json!({"error": "plugin not available", "code": "context-not-active", "logical": o}),
                                false,
                            ));
                        }
                        _ => {}
                    }
                }
            }
            return Err((json!({"error": "no such capability", "code": "no-such-capability"}), false));
        };
        // Legacy record without instance: binds to the current instance.
        let inst = if r.instance == 0 {
            let cur = self.contexts.current(&r.logical);
            match cur {
                Some(c) => c.instance,
                None => {
                    return Err((json!({"error": "plugin not available", "code": "plugin-not-loaded"}), false))
                }
            }
        } else {
            InstanceId(r.instance)
        };
        match self.contexts.require_active(inst) {
            Ok(ctx) => {
                // Current generation of the logical id (I03).
                if let Some(cur) = self.contexts.current(&ctx.logical) {
                    if cur.instance != inst {
                        return Err((
                            json!({"error": "stale generation", "code": "stale-generation",
                                   "logical": ctx.logical, "generation": ctx.generation}),
                            false,
                        ));
                    }
                }
                let fiber = ctx.logical.clone();
                // I04: Active instance with valid bindings? Merged view so
                // remote-bound consumers can open parent tickets.
                let fresh = self.current_all_merged();
                let active = self
                    .plugins
                    .lock()
                    .get(&fiber)
                    .map(|p| p.state.canonical() == Fsm::Active)
                    .unwrap_or(false);
                if !active {
                    return Err((
                        json!({"error": "plugin not available", "code": "context-not-active"}),
                        false,
                    ));
                }
                if self.bindings_stale(&fiber, &fresh) {
                    return Err((
                        json!({"error": "dependency-unavailable: binding obsoleto", "code": "dependency-unavailable", "logical": fiber}),
                        false,
                    ));
                }
                Ok((fiber, ctx))
            }
            Err(ContextError::NotActive { logical, state }) => Err((
                json!({"error": "context not active", "code": "context-not-active",
                       "logical": logical, "state": state}),
                false,
            )),
            Err(ContextError::StaleGeneration { logical, expected, got }) => Err((
                json!({"error": "stale generation", "code": "stale-generation",
                       "logical": logical, "expected": expected, "got": got}),
                false,
            )),
            Err(_) => Err((
                json!({"error": "stale generation", "code": "stale-generation"}),
                false,
            )),
        }
    }

    /// Opens a call: issues a ticket bound to instance, generation,
    /// context, epoch, and authorization (I02–I04). Post-registration
    /// revalidation closes the admission × dispose race: either it observes
    /// `Quiescing` and is rejected, or disposal observes the ticket and drains/cancels it.
    pub fn call_open(
        &self,
        cap: &str,
        input: &Value,
        policy_override: Option<CallPolicy>,
        pins: &[ResourceHandle],
    ) -> Result<OpenCall, Value> {
        if self.calls.inflight_count() >= MAX_INFLIGHT_CALLS {
            return Err(json!({"error": "too many in-flight calls", "code": "resource-exhausted"}));
        }
        let (logical, ctx) = self.admission_record(cap).map_err(|(v, _)| v)?;
        // Pins: must exist, be active, and belong to the same activation.
        for h in pins {
            match self.resources.validate(*h, ctx.instance, ctx.generation) {
                Ok(_) => {}
                Err(_) => {
                    return Err(json!({"error": format!("invalid pin {}", h), "code": "invalid-pin"}));
                }
            }
        }
        let policy = policy_override.unwrap_or_else(|| self.policy_for(&logical, cap));
        let cancel = Arc::new(AtomicBool::new(false));
        let id = self.calls.alloc(
            cap,
            &logical,
            ctx.instance,
            ctx.generation,
            ctx.context,
            ctx.epoch,
            policy,
            pins.to_vec(),
            cancel.clone(),
        );
        self.journal.append(
            &logical,
            "call.open",
            json!({
                "ticket": id.0, "cap": cap, "input": input,
                "instance": ctx.instance.0, "generation": ctx.generation,
                "policy": policy.on_withdraw.as_str(), "drain_ms": policy.drain_ms,
            }),
            Value::Null,
        );
        // Revalidation: disposal may have linearized between admit and alloc.
        match self.admission_record(cap) {
            Ok((_, fresh)) if fresh.instance == ctx.instance && fresh.generation == ctx.generation => {
                Ok(OpenCall {
                    ticket: id,
                    cap: cap.to_string(),
                    logical,
                    instance: ctx.instance,
                    generation: ctx.generation,
                    context: ctx.context,
                    policy,
                    cancel,
                })
            }
            Ok(_) => {
                let _ = self.settle_ticket(id);
                self.journal.append(
                    &logical,
                    "call.cancelled",
                    json!({"ticket": id.0, "reason": "admission-race: superseded"}),
                    Value::Null,
                );
                Err(json!({"error": "stale generation", "code": "stale-generation", "logical": logical}))
            }
            Err((v, _)) => {
                let _ = self.settle_ticket(id);
                self.journal.append(
                    &logical,
                    "call.cancelled",
                    json!({"ticket": id.0, "reason": "admission-race"}),
                    Value::Null,
                );
                Err(v)
            }
        }
    }

    /// Boundary that applies the effect: rejects revoked, expired,
    /// of a stale generation or a context outside valid drain. A late
    /// attempt is rejected AND journaled (`call.rejected`).
    /// Child-result terminal accept (M6.1 step 3, dispatcher):
    /// revalidates the chain and marks `Committed` atomically — accepted a
    /// single time. Failure maps into the extension vocabulary.
    pub fn dependency_accept(&self, child: TicketId) -> Result<(), DepDeny> {
        let t = self.calls.get(child).ok_or(DepDeny {
            code: "internal",
            reason: format!("unknown child tkt-{}", child.0),
        })?;
        if let Err(e) = self.dep_chain_check(&t, "result") {
            let code = e
                .get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("internal");
            let reason = e
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("chain rejected")
                .to_string();
            let code = match code {
                "cancelled" => "cancelled",
                "deadline-exceeded" => "deadline-exceeded",
                "permission-denied" => "permission-denied",
                "stale-generation" => "stale-generation",
                "context-not-active" => "context-not-active",
                _ => "internal",
            };
            return Err(DepDeny { code, reason });
        }
        if !self.calls.mark_committed_if_admitted(child) {
            return Err(DepDeny {
                code: "cancelled",
                reason: format!("child tkt-{} revoked during accept", child.0),
            });
        }
        Ok(())
    }

    /// Expires an admitted child whose deadline the dispatcher saw pass.
    /// Marks `Expired`, revokes descendants, and journals. Returns whether it marked.
    pub fn dependency_timeout(&self, child: TicketId) -> bool {
        if !self.calls.expire_ticket(child) {
            return false;
        }
        self.journal.append(
            &self
                .calls
                .get(child)
                .map(|t| t.logical)
                .unwrap_or_else(|| "?".to_string()),
            "call.expired",
            json!({"ticket": child.0, "reason": "deadline-exceeded"}),
            Value::Null,
        );
        self.revoke_ticket_descendants(child, "parent-expired");
        true
    }

    /// Validates a child's ancestral chain for terminal boundaries
    /// (mediated commit and result accept): live deadline, open parent,
    /// current Active consumer, grant at the captured revision. Emits the
    /// same `call.rejected`/`call.expired` as commit; callers map the
    /// error into their own vocabulary.
    fn dep_chain_check(&self, t: &TicketRecord, kind: &str) -> Result<(), Value> {
        let ticket = t.id;
        if let Some(dep) = t.dep.clone() {
            if t.drain_until.is_some_and(|u| Instant::now() >= u) {
                self.calls.set_state(ticket, TicketState::Expired);
                self.journal.append(
                    &t.logical,
                    "call.expired",
                    json!({"ticket": ticket.0, "reason": "deadline-exceeded"}),
                    Value::Null,
                );
                return Err(json!({"error": "dependency deadline exceeded", "code": "deadline-exceeded", "ticket": ticket.0}));
            }
            let parent_open = self
                .calls
                .get(dep.parent)
                .is_some_and(|p| p.state == TicketState::Admitted);
            if !parent_open {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "parent-revoked", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "parent revoked", "code": "cancelled", "ticket": ticket.0}));
            }
            let consumer_ok = self
                .contexts
                .get_by_instance(dep.consumer)
                .filter(|c| c.state.canonical() == Fsm::Active)
                .filter(|c| {
                    self.contexts
                        .current(&c.logical)
                        .is_some_and(|cur| cur.instance == dep.consumer)
                });
            if consumer_ok.is_none() {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "consumer-withdrawn", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "consumer withdrawn", "code": "stale-generation", "ticket": ticket.0}));
            }
            let grant_ok = self
                .outbound_grants
                .lock()
                .get(&(dep.grant_consumer.clone(), dep.grant_cap.clone()))
                .copied()
                == Some(dep.grant_rev);
            if !grant_ok {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "grant-revoked", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "operator grant revoked", "code": "permission-denied", "ticket": ticket.0}));
            }
        }
        Ok(())
    }

    pub fn commit_effect(&self, ticket: TicketId, kind: &str, payload: &Value) -> Result<u64, Value> {
        let t = self.calls.get(ticket).ok_or_else(|| {
            json!({"error": format!("unknown ticket {}", ticket), "code": "unknown-ticket"})
        })?;
        match t.state {
            TicketState::Committed => {
                return Err(json!({"error": "already committed", "code": "already-committed", "ticket": ticket.0}));
            }
            TicketState::Cancelled => {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "cancelled", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "ticket cancelled", "code": "cancelled", "ticket": ticket.0}));
            }
            TicketState::Expired => {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "drain-exceeded", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "drain deadline exceeded", "code": "deadline-exceeded", "ticket": ticket.0}));
            }
            TicketState::Admitted => {}
        }

        // Child ancestral chain (M6.1 step 2): open parent, consumer
        // current and Active, grant at the captured revision, live deadline.
        // A revoked link rejects the commit (journaled); expiry marks
        // `Expired` without assuming execution ended.
        if t.dep.is_some() {
            self.dep_chain_check(&t, kind)?;
        }
        // Provider-side ownership: local legs revalidate the provider
        // context here. Remote legs (M7) skip it — the executor validates
        // its own activation before accepting the mediated effect; the
        // controller only requires the registration to still be live at
        // the attested (instance, generation). Anything else fails closed.
        let remote_live = match t.dep.as_ref().and_then(|d| d.remote_peer.as_ref()) {
            None => None,
            Some(_) => {
                let live = self
                    .remote_providers
                    .lock()
                    .get(&t.logical)
                    .is_some_and(|r| {
                        r.instance == t.instance.0 && r.generation == t.generation
                    })
                    && self
                        .definitions
                        .lock()
                        .get(&t.logical)
                        .is_some_and(|d| d.remote);
                if !live {
                    self.journal.append(
                        &t.logical,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "remote-withdrawn", "kind": kind}),
                        Value::Null,
                    );
                    return Err(json!({"error": "remote provider withdrawn", "code": "stale-generation", "logical": t.logical}));
                }
                Some(())
            }
        };
        if remote_live.is_none() {
            if t.epoch != self.epoch() {
                return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
            }
            let owner = self.contexts.get_by_instance(t.instance);
            let Some(owner) = owner else {
                return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
            };
            if owner.logical != t.logical || owner.generation != t.generation {
                return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
            }
            if let Some(cur) = self.contexts.current(&t.logical) {
                if cur.instance != t.instance {
                    self.journal.append(
                        &t.logical,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "stale-generation", "kind": kind}),
                        Value::Null,
                    );
                    return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
                }
            }
            match owner.state.canonical() {
                Fsm::Active => {}
                Fsm::Quiescing => {
                    // Bounded drain: commits accepted within the deadline (effects
                    // finished before revocation is recorded, not undone).
                    let within = t.policy.on_withdraw == WithdrawPolicy::Drain
                        && t.drain_until.map(|u| std::time::Instant::now() <= u).unwrap_or(false);
                    if !within {
                        self.journal.append(
                            &t.logical,
                            "call.rejected",
                            json!({"ticket": ticket.0, "reason": "revoked", "kind": kind}),
                            Value::Null,
                        );
                        return Err(json!({"error": "ticket revoked", "code": "cancelled", "ticket": ticket.0}));
                    }
                }
                _ => {
                    self.journal.append(
                        &t.logical,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "context-not-active", "kind": kind}),
                        Value::Null,
                    );
                    return Err(json!({"error": "context not active", "code": "context-not-active", "logical": t.logical}));
                }
            }
        }
        let e = self.calls.push_effect(ticket, &t.logical, t.instance, t.generation, kind, payload.clone());
        self.calls.set_state(ticket, TicketState::Committed);
        self.journal.append(
            &t.logical,
            "call.committed",
            json!({
                "ticket": ticket.0, "effect": e.seq, "kind": kind,
                "instance": t.instance.0, "generation": t.generation,
            }),
            Value::Null,
        );
        Ok(e.seq)
    }

    /// Explicit cooperative cancellation. Returns whether a ticket was open.
    /// A parent end revokes still-open descendants (M6.1 step 2).
    pub fn call_cancel(&self, ticket: TicketId, reason: &str) -> bool {
        let rec = self.calls.get(ticket);
        let Some(r) = rec else { return false };
        if self.calls.cancel(ticket, reason) {
            self.journal.append(
                &r.logical,
                "call.cancelled",
                json!({"ticket": ticket.0, "reason": reason}),
                Value::Null,
            );
            self.revoke_ticket_descendants(ticket, "parent-cancelled");
            true
        } else {
            false
        }
    }

    /// Closes tracking (idempotent). Finalizes the owner if withdraw left
    /// it pending and nothing else remains live.
    ///
    /// Close contract (B4): openers close — even if the worker
    /// died (observe `cancel`/outcome, then close). The instance owner
    /// may reap already-revoked authority via `call_reap`.
    pub fn call_close(&self, ticket: TicketId) -> bool {
        let Some(r) = self.calls.remove(ticket) else { return false };
        self.journal.append(
            &r.logical,
            "call.closed",
            json!({"ticket": ticket.0, "state": r.state.as_str()}),
            Value::Null,
        );
        self.revoke_ticket_descendants(ticket, "parent-ended");
        if let Some(dep) = r.dep.as_ref() {
            self.dep_release(&dep.session.clone());
        }
        self.maybe_finalize(r.instance);
        true
    }

    /// Single ticket conclusion (B2): removes from the map and re-evaluates
    /// the pending owner. Every ticket end passes through here — `invoke`'s
    /// ephemeral path, admission-race exits, `ForwardOutcome::Failed`.
    /// (`call_close` does the same with a `call.closed` journal for durable
    /// ones.) Without it, raw `remove` parks `CleanupPending`
    /// with zero pending work and no way out.
    fn settle_ticket(&self, ticket: TicketId) -> Option<TicketRecord> {
        let r = self.calls.remove(ticket)?;
        // A parent end revokes descendants; a child end releases the reservation.
        self.revoke_ticket_descendants(ticket, "parent-ended");
        if let Some(dep) = r.dep.as_ref() {
            self.dep_release(&dep.session.clone());
        }
        self.maybe_finalize(r.instance);
        Some(r)
    }

    /// Reaps a ticket for the instance owner (B4): ends a ticket whose
    /// authority was already revoked (`Cancelled`, `Expired`, or `Committed`)
    /// when the original owner died without closing. Refuses `Admitted` (still
    /// holding authority: cancel first). Journals `call.reaped` and re-evaluates
    /// `CleanupPending`. Assumes nothing about execution having ended —
    /// just releases the accounting pin; late commit stays rejected by
    /// the ticket machine.
    pub fn call_reap(&self, ticket: TicketId, reason: &str) -> bool {
        let rec = self.calls.get(ticket);
        let Some(r) = rec else { return false };
        match r.state {
            TicketState::Cancelled | TicketState::Expired | TicketState::Committed => {}
            TicketState::Admitted => return false,
        }
        let Some(r) = self.settle_ticket(ticket) else { return false };
        self.journal.append(
            &r.logical,
            "call.reaped",
            json!({"ticket": ticket.0, "state": r.state.as_str(), "reason": reason}),
            Value::Null,
        );
        true
    }

    /// In-flight calls (C19): all of them, with state and policy.
    pub fn pending_calls(&self) -> Vec<TicketRecord> {
        self.calls.snapshot()
    }

    /// Confirmed effects, optionally per logical (C08).
    pub fn committed_effects(&self, logical: Option<&str>) -> Vec<CommittedEffect> {
        self.calls.effects_for(logical)
    }

    /// B2: rewrites the `CleanupPending` cause with the current tickets
    /// when it describes pending work (the `in-flight calls pending:` prefix).
    fn refresh_cleanup_cause(&self, inst: InstanceId) {
        let cur = self.contexts.get_by_instance(inst);
        let Some(r) = cur else { return };
        if r.state.canonical() != Fsm::CleanupPending {
            return;
        }
        let is_inflight = r
            .cause
            .as_deref()
            .is_some_and(|c| c.starts_with(INFLIGHT_CAUSE_PREFIX));
        if !is_inflight {
            return;
        }
        let mut pending: Vec<u64> = self.calls.pending_for(inst).iter().map(|t| t.id.0).collect();
        pending.sort_unstable();
        self.contexts
            .mark_cleanup_pending(inst, format!("{}{:?}", INFLIGHT_CAUSE_PREFIX, pending));
    }
    /// Verifiable finalization: a `CleanupPending` owner with no more live
    /// work releases resources and goes `Disposed`. Never reactivates a
    /// discarded instance: only new instances come from the reconciler.
    /// On settling, it reconciles (a persistent definition gains a substitute
    fn maybe_finalize(&self, inst: InstanceId) {
        let rec = match self.contexts.get_by_instance(inst) {
            Some(r) => r,
            None => return,
        };
        if rec.state.canonical() != Fsm::CleanupPending {
            return;
        }
        if self.calls.pending_count(inst) > 0 {
            // B2: the cause cites only EXISTING tickets (never already
            // concluded ids). Resource-failure causes are preserved.
            self.refresh_cleanup_cause(inst);
            return;
        }
        if !self.calls.try_claim_cleanup(inst) {
            return;
        }
        let mut settled = false;
        let failures = self.release_context_resources(&rec);
        if failures.is_empty() {
            self.contexts.mark_disposed(inst);
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::Disposed;
                        p.leases.clear();
                    }
                }
            }
            self.journal.append(
                &rec.logical,
                "plugin.unloaded",
                json!({
                    "id": rec.logical, "instance": inst.0,
                    "context": rec.context.0, "generation": rec.generation,
                    "after": "cleanup-settled",
                }),
                Value::Null,
            );
            settled = true;
        } else {
            self.contexts
                .mark_cleanup_pending(inst, failures.join("; "));
        }
        self.calls.release_cleanup_claim(inst);
        if settled {
            self.reconcile();
        }
    }

    /// Releases context resources in reverse order (holding no locks in
    /// join). Returns failures; idempotent (double attempts are benign).
    fn release_context_resources(&self, rec: &ContextRecord) -> Vec<String> {
        let mut handles = self.resources.handles_for(rec.context);
        handles.reverse();
        let mut failures: Vec<String> = vec![];
        for h in handles {
            let Some(rr) = self.resources.record(h) else { continue };
            if rr.state != crate::resources::ResourceState::Active {
                continue;
            }
            if rr.owner_context != rec.context {
                continue;
            }
            match self.resources.begin_release(h) {
                Ok(claim) => {
                    match &claim.kind {
                        ResourceKind::Cap { name } => {
                            self.caps.revoke_cap_if_owned(name, rec.instance);
                        }
                        ResourceKind::Sub { topic } => {
                            self.bus.unsubscribe_topic_if_owned(rec.instance, topic);
                        }
                        _ => {}
                    }
                    crate::resources::ResourceTable::finish_release(claim);
                }
                Err(crate::resources::ReleaseError::AlreadyReleased(_)) => {}
                Err(crate::resources::ReleaseError::UnknownHandle(_)) => {}
                Err(crate::resources::ReleaseError::CleanupFailed { reason, .. }) => {
                    failures.push(reason);
                }
            }
        }
        failures
    }

    // ---- resource acquisition (C02) ----

    /// Acquires a resource for the logical id's current generation.
    /// Rejects when the context left `Active` (I02). Registers ownership
    /// before publishing the effect; partial failure rolls back (I05).
    pub fn acquire(&self, logical: &str, kind: ResourceKind) -> Result<ResourceHandle, String> {
        let ctx = self
            .contexts
            .current(logical)
            .ok_or_else(|| "plugin-not-loaded".to_string());
        let ctx = ctx?;
        self.contexts
            .require_active(ctx.instance)
            .map_err(|e| match e {
                ContextError::NotActive { .. } => "context-not-active".to_string(),
                ContextError::StaleGeneration { .. } => "stale-generation".to_string(),
                _ => "plugin-not-loaded".to_string(),
            })?;

        let handle = self
            .resources
            .register(ctx.context, ctx.instance, &ctx.logical, ctx.generation, kind.clone())
            .map_err(|e| e.to_string())?;

        // Publishes the effect only after registering (I05).
        let publish_err: Option<String> = match &kind {
            ResourceKind::Cap { name } => {
                let r = ctx.reference();
                self.caps.provide_instance(name, &r);
                None
            }
            ResourceKind::Sub { topic } => {
                self.bus.subscribe_instance(&ctx.logical, ctx.instance, topic);
                None
            }
            ResourceKind::Timer { .. }
            | ResourceKind::Task { .. }
            | ResourceKind::FailRelease { .. } => None,
            ResourceKind::FailAcquire { .. } => {
                unreachable!("register rejeita FailAcquire antes de publicar")
            }
        };
        if let Some(err) = publish_err {
            self.resources.rollback_register(handle);
            return Err(err);
        }

        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(logical) {
                // Avoids recording an old-generation handle on the new entry.
                if p.instance_id == ctx.instance.0 && p.generation == ctx.generation {
                    p.resources.push(handle);
                }
            }
        }
        // Post-publication revalidation (B3): disposal may have linearized
        // between `require_active` and here (`call_open` mirror). If the
        // in between; if the current activation changed or left `Active`,
        // rolls back registration + publication and rejects — never publishes
        let stale: Option<String> = match self.contexts.current(logical) {
            Some(cur)
                if cur.instance == ctx.instance && cur.generation == ctx.generation =>
            {
                if cur.state.canonical() == Fsm::Active {
                    None
                } else {
                    Some("context-not-active".to_string())
                }
            }
            Some(_) => Some("stale-generation".to_string()),
            None => Some("plugin-not-loaded".to_string()),
        };
        if let Some(err) = stale {
            match &kind {
                ResourceKind::Cap { name } => {
                    self.caps.revoke_cap_if_owned(name, ctx.instance);
                }
                ResourceKind::Sub { topic } => {
                    self.bus.unsubscribe_topic_if_owned(ctx.instance, topic);
                }
                _ => {}
            }
            self.resources.rollback_register(handle);
            if let Some(p) = self.plugins.lock().get_mut(logical) {
                p.resources.retain(|h| *h != handle);
            }
            self.journal.append(
                logical,
                "resource.rejected",
                json!({
                    "handle": handle.0,
                    "kind": kind.kind_name(),
                    "label": kind.label(),
                    "instance": ctx.instance.0,
                    "generation": ctx.generation,
                    "reason": "acquire-race",
                }),
                Value::Null,
            );
            return Err(err);
        }
        self.journal.append(
            logical,
            "resource.acquired",
            json!({
                "handle": handle.0,
                "kind": kind.kind_name(),
                "label": kind.label(),
                "instance": ctx.instance.0,
                "context": ctx.context.0,
                "generation": ctx.generation,
            }),
            Value::Null,
        );
        Ok(handle)
    }

    /// Transactional acquisition: any item failing cleans up the earlier
    /// ones in reverse order (I05). Basis for C03 with no external oracles.
    pub fn acquire_batch(
        &self,
        logical: &str,
        kinds: Vec<ResourceKind>,
    ) -> Result<Vec<ResourceHandle>, String> {
        let mut out = Vec::with_capacity(kinds.len());
        for k in kinds {
            match self.acquire(logical, k) {
                Ok(h) => out.push(h),
                Err(e) => {
                    for h in out.iter().rev() {
                        let _ = self.release(*h);
                    }
                    return Err(e);
                }
            }
        }
        Ok(out)
    }

    /// Releases a handle. Never releases twice: the second call
    /// returns an error without rerunning cleanup (C03). Handles from another
    /// activation are untouched here — disposal uses the recorded owner.
    pub fn release(&self, handle: ResourceHandle) -> Result<(), String> {
        let rec = self.resources.record(handle).ok_or_else(|| {
            // The journal records nothing; unknown handles are rejected (C01).
            "unknown-handle".to_string()
        })?;
        // A resource pinned by an in-flight call cannot be released
        // liberado prematuramente (M1.3).
        if rec.state == crate::resources::ResourceState::Active {
            let pinned = self
                .calls
                .pending_for(rec.owner_instance)
                .iter()
                .any(|t| t.pins.contains(&handle));
            if pinned {
                return Err("resource-pinned".to_string());
            }
        }
        let claim = self
            .resources
            .begin_release(handle)
            .map_err(|e| match e {
                crate::resources::ReleaseError::AlreadyReleased(_) => {
                    "already-released".to_string()
                }
                crate::resources::ReleaseError::UnknownHandle(_) => "unknown-handle".to_string(),
                crate::resources::ReleaseError::CleanupFailed { reason, .. } => reason,
            })?;
        // Revokes the publication only if still owned by the owner instance.
        match &claim.kind {
            ResourceKind::Cap { name } => {
                self.caps
                    .revoke_cap_if_owned(name, InstanceId(rec.owner_instance.0));
            }
            ResourceKind::Sub { topic } => {
                self.bus
                    .unsubscribe_topic_if_owned(InstanceId(rec.owner_instance.0), topic);
            }
            _ => {}
        }
        crate::resources::ResourceTable::finish_release(claim);
        self.journal.append(
            &rec.owner_logical,
            "resource.released",
            json!({
                "handle": handle.0,
                "instance": rec.owner_instance.0,
                "generation": rec.generation,
            }),
            Value::Null,
        );
        Ok(())
    }

    /// Validates handle use against the current activation (C01).
    pub fn validate_handle(&self, handle: ResourceHandle, logical: &str) -> Result<(), String> {
        let cur = self
            .contexts
            .current(logical)
            .ok_or_else(|| "plugin-not-loaded".to_string())?;
        self.resources
            .validate(handle, cur.instance, cur.generation)
            .map(|_| ())
            .map_err(|e| match e {
                crate::resources::ReleaseError::AlreadyReleased(_) => {
                    "stale-handle".to_string()
                }
                crate::resources::ReleaseError::UnknownHandle(_) => "unknown-handle".to_string(),
                crate::resources::ReleaseError::CleanupFailed { reason, .. } => reason,
            })
    }

    // ---- reactive dependencies (M1.2) ----

    /// Resolves one logical's bindings against definitions + states.
    /// Current ones. Cycles, ambiguity, versions, and down providers become
    /// `ResolveError` with a visible reason (never "last wins").
    fn resolve_for(
        logical: &str,
        defs: &HashMap<String, StoredManifest>,
        states: &HashMap<String, ContextRecord>,
        cycle_members: &HashSet<String>,
        cycle_path: &[String],
    ) -> Result<Vec<Binding>, ResolveError> {
        if cycle_members.contains(logical) {
            return Err(ResolveError::Cycle { path: cycle_path.to_vec() });
        }
        let def = defs.get(logical).ok_or_else(|| ResolveError::Missing {
            interface: logical.to_string(),
        })?;
        if def.requires.is_empty() {
            return Ok(vec![]);
        }
        let provides = provides_of(defs);
        let mut out = Vec::with_capacity(def.requires.len());
        for req in &def.requires {
            if let Some(want_provider) = &req.provider {
                // Explicit binding: the named provider must exist and provide.
                let pdef = defs.get(want_provider).ok_or_else(|| ResolveError::UnknownProvider {
                    interface: req.interface.clone(),
                    provider: want_provider.clone(),
                })?;
                if !pdef.caps.iter().any(|c| cap_satisfies(c, req)) {
                    return Err(ResolveError::VersionMismatch {
                        interface: req.interface.clone(),
                        provider: want_provider.clone(),
                        detail: format!("{} provides [{}]", want_provider, pdef.caps.join(", ")),
                    });
                }
                let st = states.get(want_provider).ok_or_else(|| ResolveError::ProviderNotActive {
                    interface: req.interface.clone(),
                    provider: want_provider.clone(),
                    state: "unknown".to_string(),
                })?;
                if st.state.canonical() != Fsm::Active {
                    let mut s = st.state.as_str().to_string();
                    if let Some(c) = &st.cause {
                        s = format!("{} ({})", s, c);
                    }
                    return Err(ResolveError::ProviderNotActive {
                        interface: req.interface.clone(),
                        provider: want_provider.clone(),
                        state: s,
                    });
                }
                out.push(Binding {
                    interface: req.interface.clone(),
                    provider_logical: want_provider.clone(),
                    provider_instance: st.instance.0,
                    provider_generation: st.generation,
                });
            } else {
                let mut cands = provider_candidates(req, &provides);
                // Self-dependency counts as a candidate (a cycle if alone).
                if cands.is_empty() {
                    return Err(ResolveError::Missing { interface: req.interface.clone() });
                }
                if cands.len() > 1 {
                    cands.sort();
                    return Err(ResolveError::Ambiguous {
                        interface: req.interface.clone(),
                        providers: cands,
                    });
                }
                let pname = cands.into_iter().next().unwrap();
                let st = states.get(&pname).ok_or_else(|| ResolveError::ProviderNotActive {
                    interface: req.interface.clone(),
                    provider: pname.clone(),
                    state: "unknown".to_string(),
                })?;
                if st.state.canonical() != Fsm::Active {
                    let mut s = st.state.as_str().to_string();
                    if let Some(c) = &st.cause {
                        s = format!("{} ({})", s, c);
                    }
                    return Err(ResolveError::ProviderNotActive {
                        interface: req.interface.clone(),
                        provider: pname,
                        state: s,
                    });
                }
                out.push(Binding {
                    interface: req.interface.clone(),
                    provider_logical: pname.clone(),
                    provider_instance: st.instance.0,
                    provider_generation: st.generation,
                });
            }
        }
        Ok(out)
    }

    /// Partial I04: stored bindings may still point at the instances
    /// current, live providers?
    fn bindings_stale(&self, logical: &str, states: &HashMap<String, ContextRecord>) -> bool {
        let bindings = self.plugins.lock().get(logical).map(|p| p.bindings.clone()).unwrap_or_default();
        if bindings.is_empty() {
            return false;
        }
        for b in &bindings {
            match states.get(&b.provider_logical) {
                Some(st)
                    if st.state.canonical() == Fsm::Active
                        && st.instance.0 == b.provider_instance
                        && st.generation == b.provider_generation =>
                {
                    continue
                }
                _ => return true,
            }
        }
        false
    }

    /// Creates a `Waiting` instance + matching plugin entry.
    /// Publishes no capabilities (I04). Returns the created generation.
    fn create_waiting_instance(&self, def: &StoredManifest, reason: &str) -> ContextRecord {
        let ctx = self.contexts.create_waiting(&def.id, reason.to_string());
        let p = Plugin {
            id: def.id.clone(),
            state: Fsm::Waiting,
            tier: def.tier.clone(),
            trust: def.trust.clone(),
            generation: ctx.generation,
            restart_policy: def.restart.clone(),
            caps: def.caps.clone(),
            subs: def.subs.clone(),
            requires: def.requires.clone(),
            calls: def.calls.clone(),
            execution: def.execution.clone(),
            bindings: vec![],
            wait_cause: Some(reason.to_string()),
            reducer: def.reducer.clone(),
            init_state: def.init_state.clone(),
            json_state: initial_json_state(&def.reducer, &def.init_state),
            leases: vec![],
            panic_count: 0,
            restart_times: vec![],
            instance_id: ctx.instance.0,
            context_id: ctx.context.0,
            epoch: ctx.epoch,
            resources: vec![],
        };
        self.plugins.lock().insert(def.id.clone(), p);
        self.journal.append(
            &def.id,
            "plugin.waiting",
            json!({
                "id": def.id,
                "instance": ctx.instance.0,
                "context": ctx.context.0,
                "generation": ctx.generation,
                "reason": reason,
                "requires": def.requires.iter().map(|r| r.interface.clone()).collect::<Vec<_>>(),
            }),
            Value::Null,
        );
        ctx
    }

    /// Ensures `Waiting` with the given reason. Never moves `Active` → `Waiting`
    /// on the same instance (withdraw uses dispose + new generation). Returns
    /// `true` when something changed.
    fn ensure_waiting(&self, logical: &str, defs: &HashMap<String, StoredManifest>, reason: String) -> bool {
        let Some(def) = defs.get(logical).cloned() else { return false };
        let cur = self.contexts.current(logical);
        match cur {
            None => {
                self.create_waiting_instance(&def, &reason);
                true
            }
            Some(st) => match st.state.canonical() {
                Fsm::Waiting | Fsm::Preparing | Fsm::Registered => {
                    if st.cause.as_deref() == Some(reason.as_str()) {
                        // Syncs the plugin entry on divergence.
                        let mut changed = false;
                        {
                            let mut ps = self.plugins.lock();
                            if let Some(p) = ps.get_mut(logical) {
                                if p.state.canonical() != Fsm::Waiting || p.wait_cause.as_deref() != Some(reason.as_str()) {
                                    p.state = Fsm::Waiting;
                                    p.wait_cause = Some(reason.clone());
                                    changed = true;
                                }
                            } else {
                                changed = true;
                            }
                        }
                        if changed {
                            let p = Plugin {
                                id: def.id.clone(),
                                state: Fsm::Waiting,
                                tier: def.tier.clone(),
                                trust: def.trust.clone(),
                                generation: st.generation,
                                restart_policy: def.restart.clone(),
                                caps: def.caps.clone(),
                                subs: def.subs.clone(),
                                requires: def.requires.clone(),
                                calls: def.calls.clone(),
                                execution: def.execution.clone(),
                                bindings: vec![],
                                wait_cause: Some(reason.clone()),
                                reducer: def.reducer.clone(),
                                init_state: def.init_state.clone(),
                                json_state: initial_json_state(&def.reducer, &def.init_state),
                                leases: vec![],
                                panic_count: 0,
                                restart_times: vec![],
                                instance_id: st.instance.0,
                                context_id: st.context.0,
                                epoch: st.epoch,
                                resources: vec![],
                            };
                            self.plugins.lock().insert(logical.to_string(), p);
                        }
                        changed
                    } else {
                        self.contexts.mark_waiting(st.instance, reason.clone());
                        {
                            let mut ps = self.plugins.lock();
                            if let Some(p) = ps.get_mut(logical) {
                                p.state = Fsm::Waiting;
                                p.wait_cause = Some(reason.clone());
                            }
                        }
                        self.journal.append(
                            logical,
                            "plugin.waiting",
                            json!({"id": logical, "instance": st.instance.0, "generation": st.generation, "reason": reason}),
                            Value::Null,
                        );
                        true
                    }
                }
                Fsm::Disposed => {
                    self.create_waiting_instance(&def, &reason);
                    true
                }
                // Failed/CleanupPending/Quiescing: terminal or uncertain; skips.
                _ => false,
            },
        }
    }

    /// Withdraws an active instance to `Waiting` in a NEW generation:
    /// dispose (limpa recursos, revoga caps) + cria Waiting.
    /// Callers guarantee consumers-before-providers here
    /// (reverse pass from the top). Returns `true` when something changed.
    fn withdraw_to_waiting(&self, logical: &str, defs: &HashMap<String, StoredManifest>, reason: String) -> bool {
        let Some(def) = defs.get(logical).cloned() else { return false };
        let cur = self.contexts.current(logical);
        match cur {
            None => {
                self.create_waiting_instance(&def, &reason);
                true
            }
            Some(st) => match st.state.canonical() {
                Fsm::Active | Fsm::Preparing | Fsm::Registered => {
                    let _ = self.dispose_instance_inner(st.instance, false);
                    self.create_waiting_instance(&def, &reason);
                    true
                }
                Fsm::Waiting => self.ensure_waiting(logical, defs, reason),
                Fsm::Disposed => {
                    self.create_waiting_instance(&def, &reason);
                    true
                }
                _ => false,
            },
        }
    }

    /// Acquires the definition's caps/subs as tracked resources and publishes.
    /// Partial failure rolls back the acquired set (I05). Returns handles in order.
    fn acquire_definition_resources(
        &self,
        ctx: &ContextRecord,
        def: &StoredManifest,
    ) -> Result<Vec<ResourceHandle>, String> {
        let mut acquired: Vec<ResourceHandle> = vec![];
        for c in &def.caps {
            match self.resources.register(
                ctx.context,
                ctx.instance,
                &def.id,
                ctx.generation,
                ResourceKind::Cap { name: c.clone() },
            ) {
                Ok(h) => {
                    let r = ctx.reference();
                    self.caps.provide_instance(c, &r);
                    acquired.push(h);
                }
                Err(e) => {
                    self.rollback_acquired(ctx.instance, &acquired);
                    return Err(e.to_string());
                }
            }
        }
        for t in &def.subs {
            match self.resources.register(
                ctx.context,
                ctx.instance,
                &def.id,
                ctx.generation,
                ResourceKind::Sub { topic: t.clone() },
            ) {
                Ok(h) => {
                    self.bus.subscribe_instance(&def.id, ctx.instance, t);
                    acquired.push(h);
                }
                Err(e) => {
                    self.rollback_acquired(ctx.instance, &acquired);
                    return Err(e.to_string());
                }
            }
        }
        Ok(acquired)
    }

    fn rollback_acquired(&self, inst: InstanceId, acquired: &[ResourceHandle]) {
        for h in acquired.iter().rev() {
            if let Ok(claim) = self.resources.begin_release(*h) {
                match &claim.kind {
                    ResourceKind::Cap { name } => {
                        self.caps.revoke_cap_if_owned(name, inst);
                    }
                    ResourceKind::Sub { topic } => {
                        self.bus.unsubscribe_topic_if_owned(inst, topic);
                    }
                    _ => {}
                }
                crate::resources::ResourceTable::finish_release(claim);
            }
        }
    }

    /// Promotes a `Waiting` instance → `Active` with resolved bindings:
    /// revalidates, acquires/publishes, and commits into metadata (LIFECYCLE §5).
    fn promote_waiting(&self, logical: &str, def: &StoredManifest, bindings: Vec<Binding>) -> bool {
        let cur = self.contexts.current(logical);
        let Some(st) = cur else { return false };
        if st.state.canonical() != Fsm::Waiting
            && st.state.canonical() != Fsm::Preparing
            && st.state.canonical() != Fsm::Registered
        {
            return false;
        }
        // Revalidates bindings against fresh states (a change during
        // preparation aborts the attempt). Merged view: remote
        // registrations satisfy requirements like local activations.
        let fresh = self.current_all_merged();
        let defs = self.definitions.lock().clone();
        let cycle_members: HashSet<String> = HashSet::new();
        match Self::resolve_for(logical, &defs, &fresh, &cycle_members, &[]) {
            Ok(fresh_bindings) => {
                // Uses the fresh bindings (equal to the passed ones in the common case).
                let _ = bindings;
                match self.acquire_definition_resources(&st, def) {
                    Ok(acquired) => {
                        {
                            let mut ps = self.plugins.lock();
                            if let Some(p) = ps.get_mut(logical) {
                                if p.instance_id != st.instance.0 {
                                    // Superseded during acquisition: rolls everything back.
                                    drop(ps);
                                    self.rollback_acquired(st.instance, &acquired);
                                    return false;
                                }
                                p.state = Fsm::Active;
                                p.bindings = fresh_bindings.clone();
                                p.wait_cause = None;
                                p.resources = acquired;
                            } else {
                                drop(ps);
                                self.rollback_acquired(st.instance, &acquired);
                                return false;
                            }
                        }
                        if !self.contexts.mark_active(st.instance) {
                            return false;
                        }
                        self.journal.append(
                            logical,
                            "plugin.active",
                            json!({
                                "id": logical,
                                "instance": st.instance.0,
                                "generation": st.generation,
                                "bindings": fresh_bindings.iter().map(|b| json!({
                                    "interface": b.interface,
                                    "provider": b.provider_logical,
                                    "instance": b.provider_instance,
                                    "generation": b.provider_generation,
                                })).collect::<Vec<_>>(),
                            }),
                            Value::Null,
                        );
                        self.emit_activated(logical, st.instance.0, st.generation);
                        self.issue_dep_bindings(logical, st.instance, st.generation, &fresh_bindings);
                        true
                    }
                    Err(e) => {
                        let reason = format!("acquire failed: {}", e);
                        self.ensure_waiting(logical, &defs, reason);
                        false
                    }
                }
            }
            Err(e) => {
                self.ensure_waiting(logical, &defs, e.to_string());
                false
            }
        }
    }

    /// Creates and activates a new instance (no usable current one).
    fn create_active(&self, def: &StoredManifest, bindings: Vec<Binding>, json_state: Value) -> bool {
        let ctx = self.contexts.create(&def.id);
        match self.acquire_definition_resources(&ctx, def) {
            Ok(acquired) => {
                let p = Plugin {
                    id: def.id.clone(),
                    state: Fsm::Active,
                    tier: def.tier.clone(),
                    trust: def.trust.clone(),
                    generation: ctx.generation,
                    restart_policy: def.restart.clone(),
                    caps: def.caps.clone(),
                    subs: def.subs.clone(),
                    requires: def.requires.clone(),
                    calls: def.calls.clone(),
                    execution: def.execution.clone(),
                    bindings: bindings.clone(),
                    wait_cause: None,
                    reducer: def.reducer.clone(),
                    init_state: def.init_state.clone(),
                    json_state,
                    leases: vec![],
                    panic_count: 0,
                    restart_times: vec![],
                    instance_id: ctx.instance.0,
                    context_id: ctx.context.0,
                    epoch: ctx.epoch,
                    resources: acquired,
                };
                self.plugins.lock().insert(def.id.clone(), p);
                // `plugin.loaded` is journaled by `load_manifest` (one per load);
                // here the activation transition with bindings is recorded (I12).
                self.journal.append(
                    &def.id,
                    "plugin.active",
                    json!({
                        "id": def.id,
                        "instance": ctx.instance.0,
                        "generation": ctx.generation,
                        "bindings": bindings.iter().map(|b| json!({
                            "interface": b.interface,
                            "provider": b.provider_logical,
                            "instance": b.provider_instance,
                            "generation": b.provider_generation,
                        })).collect::<Vec<_>>(),
                    }),
                    Value::Null,
                );
                self.emit_activated(&def.id, ctx.instance.0, ctx.generation);
                self.issue_dep_bindings(&def.id, ctx.instance, ctx.generation, &bindings);
                true
            }
            Err(e) => {
                // Acquisition failure: cleans up provisionals, stays Waiting (I05).
                let _ = self.dispose_instance_inner(ctx.instance, false);
                let defs = self.definitions.lock().clone();
                self.ensure_waiting(&def.id, &defs, format!("acquire failed: {}", e));
                false
            }
        }
    }

    /// Reconciliation engine: withdraws consumers before providers,
    /// activates providers before consumers, until stable.
    pub fn reconcile(&self) {
        let n = self.definitions.lock().len();
        let max = n * 2 + 4;
        for _ in 0..max {
            if !self.reconcile_step() {
                break;
            }
        }
    }

    fn reconcile_step(&self) -> bool {
        let defs: HashMap<String, StoredManifest> = self.definitions.lock().clone();
        if defs.is_empty() {
            return false;
        }
        // Hygiene: registrations without a live `remote: true` definition
        // fail closed (dependents withdraw below).
        let stale: Vec<String> = self
            .remote_providers
            .lock()
            .keys()
            .filter(|l| defs.get(*l).is_none_or(|d| !d.remote))
            .cloned()
            .collect();
        if !stale.is_empty() {
            for l in &stale {
                self.remote_providers.lock().remove(l);
                self.dep_bindings.lock().retain(|_, b| b.provider_logical != *l);
                self.journal.append(l, "remote.unregistered", json!({"reason": "definition-gone"}), Value::Null);
            }
            return true;
        }
        let provides = provides_of(&defs);
        let requires = requires_of(&defs);
        let edges = graph_edges(&requires, &provides);

        // 1. Cycles: members stay Waiting with the path, never Active.
        if let Some(path) = find_cycle(&edges) {
            let members: HashSet<String> = path.iter().cloned().collect();
            let mut changed = false;
            let mut sorted: Vec<&String> = members.iter().collect();
            sorted.sort();
            for m in sorted {
                if !defs.contains_key(m) {
                    continue;
                }
                let reason = ResolveError::Cycle { path: path.clone() }.to_string();
                // Active leaves; Waiting refreshes its reason; Disposed recreates Waiting.
                let cur = self.contexts.current(m);
                match cur.map(|r| r.state.canonical()) {
                    Some(Fsm::Active) | Some(Fsm::Preparing) | Some(Fsm::Registered) | None | Some(Fsm::Disposed) => {
                        if self.withdraw_to_waiting(m, &defs, reason) {
                            changed = true;
                        }
                    }
                    Some(Fsm::Waiting) => {
                        if self.ensure_waiting(m, &defs, reason) {
                            changed = true;
                        }
                    }
                    _ => {}
                }
            }
            return changed;
        }

        let mut order = topo_order(&edges).unwrap_or_default();
        if order.is_empty() {
            order = {
                let mut ks: Vec<String> = defs.keys().cloned().collect();
                ks.sort();
                ks
            };
        }
        let cycle_members: HashSet<String> = HashSet::new();
        let empty_path: Vec<String> = vec![];

        // 2. Withdraw: consumers before providers (reverse order).
        let mut withdrew = false;
        // Snapshot for deciding (holding no locks across mutations).
        // Remote registrations appear as synthetic Active records so
        // requirements resolve; they carry no local lifecycle.
        let states = self.current_all_merged();
        // Transitive consumers first: reverse topo order already ensures it.
        for logical in order.iter().rev() {
            if !defs.contains_key(logical) {
                continue;
            }
            let Some(st) = states.get(logical) else { continue };
            if st.state.canonical() != Fsm::Active {
                continue;
            }
            let stale = self.bindings_stale(logical, &states);
            let resolved = Self::resolve_for(logical, &defs, &states, &cycle_members, &empty_path);
            if resolved.is_err() || stale {
                let reason = match resolved {
                    Err(e) => e.to_string(),
                    Ok(_) => "stale-binding: provider replaced; rebinding".to_string(),
                };
                if self.withdraw_to_waiting(logical, &defs, reason) {
                    withdrew = true;
                }
            }
        }
        if withdrew {
            return true;
        }

        // 3. Activation: providers before consumers.
        // Re-snapshot after withdraws (which would have returned above); nothing changed here.
        let states = self.current_all_merged();
        for logical in &order {
            let Some(def) = defs.get(logical).cloned() else { continue };
            if !defs.contains_key(logical) {
                continue;
            }
            if def.remote {
                // Remote-only definitions never activate locally; their
                // requirements stay satisfied via registration (see above).
                continue;
            }
            let cur = states.get(logical).cloned();
            let cur_state = cur.as_ref().map(|r| r.state.canonical());
            match cur_state {
                Some(Fsm::Active) => {
                    // Standing I04: bindings still valid?
                    if self.bindings_stale(logical, &states) {
                        if self.withdraw_to_waiting(logical, &defs, "stale-binding: provider replaced; rebinding".to_string()) {
                            return true;
                        }
                    }
                    continue;
                }
                Some(Fsm::Failed) | Some(Fsm::CleanupPending) | Some(Fsm::Quiescing) => continue,
                _ => {}
            }
            match Self::resolve_for(logical, &defs, &states, &cycle_members, &empty_path) {
                Ok(bindings) => {
                    let is_waiting = matches!(
                        cur_state,
                        Some(Fsm::Waiting) | Some(Fsm::Preparing) | Some(Fsm::Registered)
                    );
                    if is_waiting {
                        if self.promote_waiting(logical, &def, bindings) {
                            return true;
                        }
                    } else {
                        // No usable current instance: new Active instance.
                        let js = initial_json_state(&def.reducer, &def.init_state);
                        // Preserves counter/clock when history exists.
                        let js = self.preserved_json_state(logical, &def, js);
                        if self.create_active(&def, bindings, js) {
                            return true;
                        }
                    }
                }
                Err(e) => {
                    if self.ensure_waiting(logical, &defs, e.to_string()) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Local contexts plus synthetic `Active` records for registered
    /// remote providers (reconcile/resolve/staleness only).
    fn current_all_merged(&self) -> HashMap<String, ContextRecord> {
        let mut states = self.contexts.current_all();
        let regs: Vec<String> = self.remote_providers.lock().keys().cloned().collect();
        for logical in regs {
            if let Some(rec) = self.remote_state(&logical) {
                states.insert(logical, rec);
            }
        }
        states
    }

    /// Preserved counter/clock state across generations (M1.1 compat).
    fn preserved_json_state(&self, logical: &str, def: &StoredManifest, fresh: Value) -> Value {
        let old = self.plugins.lock().get(logical).map(|p| p.json_state.clone());
        match (def.reducer.as_str(), old) {
            ("counter", Some(s)) if s.get("state").is_some() => s,
            ("clock", Some(s)) if s.get("count").is_some() => s,
            _ => fresh,
        }
    }

    // ---- lifecycle ----

    /// Installs an already-validated remote-only definition (M7 route
    /// manager path; see `Service::provision_remote`). Inserts desired
    /// state and reconciles; never activates or spawns locally.
    pub fn install_remote_definition(&self, def: StoredManifest) {
        debug_assert!(def.remote);
        let id = def.id.clone();
        self.definitions.lock().insert(id.clone(), def);
        // A live registration for a re-provisioned definition is stale
        // until the next attach-reconcile re-attests it.
        if self.remote_providers.lock().remove(&id).is_some() {
            self.dep_bindings.lock().retain(|_, b| b.provider_logical != id);
            self.journal.append(&id, "remote.unregistered", json!({"reason": "definition-reprovisioned"}), Value::Null);
        }
        self.reconcile();
    }

    pub fn load_manifest(&self, path: &PathBuf) -> Result<String, String> {
        let txt = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let v: Value = serde_json::from_str(&txt).map_err(|e| e.to_string())?;
        let mut def = parse_manifest_value(&v)?;
        def.source = Some(path.clone());
        let id = def.id.clone();
        // Preserves the previous generation's counter/clock (M1.1 compat).
        let old_state = self.plugins.lock().get(&id).map(|p| (p.reducer.clone(), p.json_state.clone()));
        // Registers/updates the desired definition.
        self.definitions.lock().insert(id.clone(), def.clone());
        // Withdraws the previous generation before replacing (no overlap).
        if let Some(cur) = self.contexts.current(&id) {
            match cur.state.canonical() {
                Fsm::Disposed | Fsm::Failed | Fsm::CleanupPending => {}
                _ => {
                    let _ = self.dispose_instance_inner(cur.instance, false);
                }
            }
        }
        // If the definition persists and the current one stopped with work
        // (CleanupPending), creates the Waiting substitute now: without
        // in flight, the reconciler skips CleanupPending, and without this
        // the logical would stall for an external event. Never reactivates
        {
            let cur = self.contexts.current(&id);
            if matches!(cur.map(|r| r.state.canonical()), Some(Fsm::CleanupPending)) {
                let defs = self.definitions.lock().clone();
                if let Some(def) = defs.get(&id) {
                    self.create_waiting_instance(def, "reload: awaiting activation");
                }
            }
        }
        // Reconciles: activates in order (or Waiting with a visible reason).
        self.reconcile();
        // Reapplies preserved state if the new instance activated.
        if let Some((old_reducer, old_json)) = old_state {
            if old_reducer == def.reducer {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&id) {
                    match (def.reducer.as_str(), &old_json) {
                        ("counter", s) if s.get("state").is_some() => p.json_state = old_json,
                        ("clock", s) if s.get("count").is_some() => p.json_state = old_json,
                        _ => {}
                    }
                }
            }
        }
        // Journal de carga (compat: um por load bem-sucedido).
        if let Some(cur) = self.contexts.current(&id) {
            self.journal.append(
                &id,
                "plugin.loaded",
                json!({
                    "id": id,
                    "instance": cur.instance.0,
                    "context": cur.context.0,
                    "generation": cur.generation,
                    "epoch": cur.epoch,
                    "state": cur.state.as_str(),
                }),
                Value::Null,
            );
        }
        Ok(id)
    }

    fn dispose_instance_inner(&self, inst: InstanceId, journal_on_idempotent: bool) -> DisposeOutcome {
        let rec = match self.contexts.get_by_instance(inst) {
            Some(r) => r,
            None => return DisposeOutcome::AlreadyDisposed,
        };
        match rec.state.canonical() {
            Fsm::Disposed | Fsm::Failed => return DisposeOutcome::AlreadyDisposed,
            Fsm::CleanupPending => {
                return DisposeOutcome::CleanupPending {
                    reason: rec.cause.unwrap_or_else(|| "cleanup pending".to_string()),
                }
            }
            _ => {}
        }
        // Linearization point with atomic claiming (B1): a single dispose
        // performs the transition; concurrent disposers observe an ongoing
        // withdraw, duplicating neither transition nor journal.
        let rec = match self.contexts.claim_dispose(inst) {
            Ok(DisposeClaim::Fresh(r)) => r,
            Ok(DisposeClaim::InFlight { .. }) => {
                return DisposeOutcome::CleanupPending {
                    reason: "dispose in progress".to_string(),
                }
            }
            Err(ContextError::AlreadyDisposed { .. }) => return DisposeOutcome::AlreadyDisposed,
            Err(_) => return DisposeOutcome::AlreadyDisposed,
        };
        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(&rec.logical) {
                if p.instance_id == inst.0 {
                    p.state = Fsm::Quiescing;
                }
            }
        }
        // Step 2: handles involving the discarded instance go (admission revalidates).
        self.prune_dep_bindings(inst);
        // External execution boundary: notifies the host at linearization.
        self.emit_hook(LifecycleEvent::Withdrawn {
            logical: rec.logical.clone(),
            instance: inst.0,
            generation: rec.generation,
        });
        // Revokes admission immediately (publications), without touching the
        // ownership records: in-flight work holds resources (I07), but nothing
        // ownership records: in-flight work holds resources (I07), but nothing
        for h in self.resources.handles_for(rec.context) {
            let Some(rr) = self.resources.record(h) else { continue };
            if rr.owner_context != rec.context {
                continue;
            }
            match &rr.kind {
                ResourceKind::Cap { name } => {
                    self.caps.revoke_cap_if_owned(name, inst);
                }
                ResourceKind::Sub { topic } => {
                    self.bus.unsubscribe_topic_if_owned(inst, topic);
                }
                _ => {}
            }
        }
        // Snapshot outside the metadata lock; blocking cleanup never
        // new is admitted from here on.
        let now = std::time::Instant::now();
        let mut drain_until: Option<std::time::Instant> = None;
        for t in self.calls.pending_for(inst) {
            match t.policy.on_withdraw {
                WithdrawPolicy::Cancel => {
                    if self.calls.cancel(t.id, "withdraw") {
                        self.journal.append(
                            &rec.logical,
                            "call.cancelled",
                            json!({"ticket": t.id.0, "reason": "withdraw"}),
                            Value::Null,
                        );
                    }
                }
                WithdrawPolicy::Drain => {
                    let until = now + std::time::Duration::from_millis(t.policy.drain_ms);
                    self.calls.set_drain_until(t.id, until);
                    drain_until = Some(drain_until.map(|u: std::time::Instant| u.max(until)).unwrap_or(until));
                    self.journal.append(
                        &rec.logical,
                        "call.draining",
                        json!({"ticket": t.id.0, "drain_ms": t.policy.drain_ms}),
                        Value::Null,
                    );
                }
            }
        }
        // Bounded drain, holding no locks (condvar wakes on close).
        if let Some(until) = drain_until {
            if until > std::time::Instant::now() {
                self.calls.wait_settled(inst, until);
            }
        }
        // Step 2: this consumer's children lose authority (B4 resolves).
        for t in self.calls.revoke_children_of_consumer(inst, "consumer-withdraw") {
            self.journal.append(
                &t.logical,
                "call.cancelled",
                json!({"ticket": t.id.0, "reason": "consumer-withdraw"}),
                Value::Null,
            );
        }
        // Expires overdue drains (stubborn work becomes explicit pending).
        for id in self.calls.expire_overdue(std::time::Instant::now()) {
            self.journal.append(
                &rec.logical,
                "call.expired",
                json!({"ticket": id.0, "reason": "drain-exceeded"}),
                Value::Null,
            );
            // Step 2: expiring the parent revokes still-open descendants.
            self.revoke_ticket_descendants(id, "parent-expired");
        }
        // Still-active work pins CleanupPending; resources are NOT released
        // early (I07). Post-mark rechecking closes the race with a
        // concurrent close.
        if self.calls.pending_count(inst) > 0 {
            let pending: Vec<u64> = self.calls.pending_for(inst).iter().map(|t| t.id.0).collect();
            let reason = format!("{}{:?}", INFLIGHT_CAUSE_PREFIX, pending);
            self.contexts.mark_cleanup_pending(inst, reason.clone());
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::CleanupPending;
                    }
                }
            }
            self.journal.append(
                &rec.logical,
                "plugin.cleanup_pending",
                json!({
                    "id": rec.logical, "instance": inst.0,
                    "generation": rec.generation, "reason": reason,
                }),
                Value::Null,
            );
            if self.calls.pending_count(inst) == 0 {
                // A close landed between check and mark: finalizes now.
                self.maybe_finalize(inst);
            }
            let _ = journal_on_idempotent;
            return match self.contexts.get_by_instance(inst).map(|r| r.state.canonical()) {
                Some(Fsm::Disposed) => DisposeOutcome::Disposed,
                _ => DisposeOutcome::CleanupPending {
                    reason: self.contexts.get_by_instance(inst)
                        .and_then(|r| r.cause)
                        .unwrap_or(reason),
                },
            };
        }
        let failures = self.release_context_resources(&rec);
        if failures.is_empty() {
            self.contexts.mark_disposed(inst);
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::Disposed;
                        p.leases.clear();
                    }
                }
            }
            self.journal.append(
                &rec.logical,
                "plugin.unloaded",
                json!({
                    "id": rec.logical,
                    "instance": inst.0,
                    "context": rec.context.0,
                    "generation": rec.generation,
                }),
                Value::Null,
            );
            DisposeOutcome::Disposed
        } else {
            let reason = failures.join("; ");
            self.contexts.mark_cleanup_pending(inst, reason.clone());
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::CleanupPending;
                    }
                }
            }
            // Uncertainty surfaces as CleanupPending, never fake Disposed (I07).
            self.journal.append(
                &rec.logical,
                "plugin.cleanup_pending",
                json!({
                    "id": rec.logical,
                    "instance": inst.0,
                    "generation": rec.generation,
                    "reason": reason,
                }),
                Value::Null,
            );
            let _ = journal_on_idempotent;
            DisposeOutcome::CleanupPending { reason }
        }
    }

    /// Explicit removal: deletes the definition, discards the instance, and
    /// cascading-withdraws consumers (which keep their definitions in
    /// `Waiting`). Idempotente (I06).
    pub fn dispose_plugin(&self, id: &str) -> DisposeOutcome {
        // Captures affected consumers before removing (for diagnosis if needed).
        let _affected = self.transitive_dependents(id);
        // Coupled teardown: a remote registration dies with its definition
        // (dependents withdraw via reconcile; legs fail at revalidation).
        if self.remote_providers.lock().remove(id).is_some() {
            self.dep_bindings.lock().retain(|_, b| b.provider_logical != id);
            self.journal.append(id, "remote.unregistered", json!({"reason": "definition-disposed"}), Value::Null);
        }
        self.definitions.lock().remove(id);
        self.emit_hook(LifecycleEvent::Removed { logical: id.to_string() });
        let out = match self.contexts.current(id) {
            Some(cur) => self.dispose_instance_inner(cur.instance, false),
            None => DisposeOutcome::AlreadyDisposed,
        };
        // Cascade + refreshed Waiting reasons.
        self.reconcile();
        out
    }

    /// Transitive consumers with a registered definition.
    fn transitive_dependents(&self, logical: &str) -> Vec<String> {
        let defs: HashMap<String, StoredManifest> = self.definitions.lock().clone();
        let edges = graph_edges(&requires_of(&defs), &provides_of(&defs));
        transitive_consumers(logical, &edges)
            .into_iter()
            .filter(|l| defs.contains_key(l))
            .collect()
    }

    /// Instance-addressed disposal: safe against a new generation.
    /// Disposing the old generation never removes the new one's resources (I06/C01).
    /// Deletes no definitions; reconciles affected dependents.
    pub fn dispose_instance(&self, inst: InstanceId) -> DisposeOutcome {
        let out = self.dispose_instance_inner(inst, false);
        self.reconcile();
        out
    }

    /// Legacy compat: the scaffold called without a return. Kept as an
    /// adapter; new code must use the `DisposeOutcome` return.
    pub fn dispose_plugin_legacy(&self, id: &str) {
        let _ = self.dispose_plugin(id);
    }

    /// Inventory for inspection (C02/C19): instances, generations, requires,
    /// bindings, resources per owner, and cleanup/cycle diagnostics.
    pub fn inventory(&self) -> Value {
        let ctxs = self.contexts.snapshot();
        let res = self.resources.inventory();
        let defs: HashMap<String, StoredManifest> = self.definitions.lock().clone();
        let bindings_by_logical: HashMap<String, (u64, Vec<Binding>)> = {
            self.plugins
                .lock()
                .iter()
                .map(|(k, p)| (k.clone(), (p.instance_id, p.bindings.clone())))
                .collect()
        };
        let instances: Vec<Value> = ctxs
            .iter()
            .map(|c| {
                let requires: Vec<String> = defs
                    .get(&c.logical)
                    .map(|d| d.requires.iter().map(|r| {
                        if let Some(p) = &r.provider {
                            format!("{}@{}", r.interface, p)
                        } else {
                            r.interface.clone()
                        }
                    }).collect())
                    .unwrap_or_default();
                let bindings: Vec<Value> = bindings_by_logical
                    .get(&c.logical)
                    .filter(|(inst, _)| *inst == c.instance.0)
                    .map(|(_, bs)| {
                        bs.iter().map(|b| json!({
                            "interface": b.interface,
                            "provider": b.provider_logical,
                            "instance": b.provider_instance,
                            "generation": b.provider_generation,
                        })).collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                json!({
                    "logical": c.logical,
                    "instance": c.instance.0,
                    "context": c.context.0,
                    "generation": c.generation,
                    "epoch": c.epoch,
                    "state": c.state.as_str(),
                    "cause": c.cause,
                    "waiting_reason": c.cause,
                    "requires": requires,
                    "bindings": bindings,
                    "dep_bindings": self
                        .dep_bindings
                        .lock()
                        .values()
                        .filter(|b| b.consumer_instance == c.instance)
                        .map(|b| json!({
                            "id": b.id,
                            "interface": b.interface,
                            "capability": b.capability,
                            "provider": b.provider_logical,
                        }))
                        .collect::<Vec<_>>(),
                    "active_resources": self.resources.active_for(c.context),
                })
            })
            .collect();
        let resources: Vec<Value> = res
            .iter()
            .map(|r| {
                json!({
                    "handle": r.handle.0,
                    "kind": r.kind.kind_name(),
                    "label": r.kind.label(),
                    "owner": r.owner_logical,
                    "instance": r.owner_instance.0,
                    "context": r.owner_context.0,
                    "generation": r.generation,
                    "state": match r.state {
                        crate::resources::ResourceState::Active => "Active",
                        crate::resources::ResourceState::Released => "Released",
                    },
                    "seq": r.seq,
                })
            })
            .collect();
        let edges = graph_edges(&requires_of(&defs), &provides_of(&defs));
        let cycle = find_cycle(&edges).unwrap_or_default();
        let calls: Vec<Value> = self
            .calls
            .snapshot()
            .iter()
            .map(|t| {
                json!({
                    "ticket": t.id.0,
                    "cap": t.cap,
                    "logical": t.logical,
                    "instance": t.instance.0,
                    "generation": t.generation,
                    "state": t.state.as_str(),
                    "policy": t.policy.on_withdraw.as_str(),
                    "drain_ms": t.policy.drain_ms,
                    "elapsed_ms": t.elapsed_ms(),
                    "drain_left_ms": t.drain_left_ms(),
                    "pins": t.pins.iter().map(|h| h.0).collect::<Vec<_>>(),
                    "cancel_reason": t.cancel_reason,
                    "dep": t.dep.as_ref().map(|d| json!({
                        "parent": d.parent.0,
                        "binding": d.binding,
                        "consumer": d.consumer.0,
                        "depth": d.depth,
                        "grant": {"consumer": d.grant_consumer, "cap": d.grant_cap, "rev": d.grant_rev},
                        "remote_peer": d.remote_peer,
                    })),
                })
            })
            .collect();
        let effects: Vec<Value> = self
            .calls
            .effects_for(None)
            .iter()
            .map(|e| {
                json!({
                    "seq": e.seq,
                    "ticket": e.ticket.0,
                    "logical": e.logical,
                    "instance": e.instance.0,
                    "generation": e.generation,
                    "kind": e.kind,
                    "payload": e.payload,
                })
            })
            .collect();
        let definitions: Vec<Value> = {
            let mut ds: Vec<&StoredManifest> = defs.values().collect();
            ds.sort_by(|a, b| a.id.cmp(&b.id));
            ds.iter().map(|d| json!({
                "id": d.id,
                "provides": d.caps,
                "remote": d.remote,
                "requires": d.requires.iter().map(|r| json!({
                    "interface": r.interface,
                    "provider": r.provider,
                })).collect::<Vec<_>>(),
            })).collect()
        };
        let mut remotes: Vec<Value> = self
            .remote_providers
            .lock()
            .values()
            .map(|r| {
                json!({
                    "logical": r.logical,
                    "instance": r.instance,
                    "generation": r.generation,
                    "peer": r.peer,
                })
            })
            .collect();
        remotes.sort_by(|a, b| a["logical"].as_str().cmp(&b["logical"].as_str()));
        json!({
            "epoch": self.epoch(),
            "instances": instances,
            "definitions": definitions,
            "remotes": remotes,
            "cycle": cycle,
            "calls": calls,
            "effects": effects,
            "resources": resources,
            "active_resources": self.resources.active_count(),
            "total_releases": self.resources.total_releases(),
            "double_release_attempts": self.resources.double_release_attempts(),
            "capabilities": self.caps.snapshot(),
        })
    }

    /// Coordinated substitution (M1.4): validates candidates without publishing,
    /// withdraws the previous generation and dependents, reconciles manifests
    /// removed from disk, clears records, and publishes the new generation.
    /// An invalid candidate never touches the live generation (recoverable).
    pub fn reload(&self) -> Result<usize, String> {
        let dir = self.plugins_dir.clone();
        let Ok(rd) = std::fs::read_dir(&dir) else { return Ok(0) };
        let mut files: Vec<PathBuf> = rd
            .flatten()
            .map(|f| f.path())
            .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
            .collect();
        files.sort();
        // Phase 1 — validate candidates without publishing (pure parse).
        let mut candidates: Vec<(PathBuf, StoredManifest)> = vec![];
        let mut failed: Vec<String> = vec![];
        for p in &files {
            let txt = match std::fs::read_to_string(p) {
                Ok(t) => t,
                Err(e) => {
                    failed.push(format!("{}: {}", p.display(), e));
                    continue;
                }
            };
            let v: Value = match serde_json::from_str(&txt) {
                Ok(v) => v,
                Err(e) => {
                    failed.push(format!("{}: {}", p.display(), e));
                    continue;
                }
            };
            match parse_manifest_value(&v) {
                Ok(mut def) => {
                    def.source = Some(p.clone());
                    candidates.push((p.clone(), def));
                }
                Err(e) => failed.push(format!("{}: {}", p.display(), e)),
            }
        }
        // Fase 2 — reconciliar removidos e ids renomeados.
        // `present`: .json files on disk (even ones that failed to parse —
        // an invalid candidate keeps the live generation: recoverable).
        let present: std::collections::HashSet<PathBuf> = files.iter().cloned().collect();
        let mut path_to_id: HashMap<PathBuf, String> = HashMap::new();
        for (p, def) in &candidates {
        // Last file (sorted order) wins on duplicate ids.
            path_to_id.insert(p.clone(), def.id.clone());
        }
        let stale: Vec<String> = {
            let defs = self.definitions.lock();
            let mut out = vec![];
            for (id, def) in defs.iter() {
                match &def.source {
                    Some(src) if src.starts_with(&dir) => {
                        if !present.contains(src) {
                            // File vanished from disk → removes the definition.
                            out.push(id.clone());
                        } else if let Some(parsed) = path_to_id.get(src) {
                            // File exists and was parsed declaring another id.
                            if parsed != id {
                                out.push(id.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            out.sort();
            out
        };
        let mut removed = 0usize;
        for id in stale {
            self.dispose_plugin(&id);
            removed += 1;
        }
        // Phase 3 — publish candidates (each load withdraws the previous generation).
        let mut n = 0;
        for (p, _) in &candidates {
            if self.load_manifest(p).is_ok() {
                n += 1;
            } else {
                failed.push(format!("{}: activation failed", p.display()));
            }
        }
        self.journal.append(
            "sys",
            "sys.reload",
            json!({"reloaded": n, "removed": removed, "failed": failed}),
            Value::Null,
        );
        Ok(n)
    }

    // ---- dispatch ----

    fn fault(&self, fiber: &str, mode: &str) -> Value {
        let mut failed = false;
        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(fiber) {
                let now = Instant::now();
                p.restart_times.retain(|t| now.duration_since(*t).as_secs() < 10);
                p.restart_times.push(now);
                p.panic_count += 1;
                if p.restart_times.len() > 5 {
                    p.state = Fsm::Failed;
                    failed = true;
                }
            }
        }
        // Mirrors the failure into the current instance's context.
        if failed {
            if let Some(cur) = self.contexts.current(fiber) {
                self.contexts
                    .mark_failed(cur.instance, format!("fault budget exceeded ({})", mode));
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(fiber) {
                    p.state = Fsm::Failed;
                }
            }
        }
        self.journal.append(
            fiber,
            "sys.fault",
            json!({"mode": mode, "contained": true, "failed": failed}),
            Value::Null,
        );
        json!({"contained": true, "mode": mode, "failed": failed})
    }

    pub fn invoke(&self, cap: &str, input: &Value) -> (Value, bool) {
        self.invoke_bound(cap, input, None)
    }

    /// Admission constrained to an authenticated instance grant. Never route a
    /// stale caller into a replacement generation merely by capability name.
    pub fn invoke_for_ref(&self, expected: &InstanceRef, cap: &str, input: &Value) -> (Value, bool) {
        self.invoke_bound(cap, input, Some(expected))
    }

    fn invoke_bound(&self, cap: &str, input: &Value, expected: Option<&InstanceRef>) -> (Value, bool) {
        // Fast synchronous path on an ephemeral ticket: post-validation closes
        // race with a concurrent dispose (revoked midway → cancelled).
        let open = match self.call_open_ephemeral(cap, input) {
            Ok(o) => o,
            Err(v) => {
                let ok = false;
                return (v, ok);
            }
        };
        let fiber = open.logical.clone();
        let ticket = open.ticket;
        if expected.is_some_and(|r| r.epoch != self.epoch() || r.instance != open.instance.0 || r.context != open.context.0 || r.generation != open.generation || r.logical != fiber) {
            let _ = self.settle_ticket(ticket);
            return (json!({"code":"stale-generation"}), false);
        }
        let (reducer, execution, state_ok) = {
            let ps = self.plugins.lock();
            match ps.get(&fiber) {
                Some(p) if p.state.canonical() == Fsm::Active && p.instance_id == open.instance.0 && p.generation == open.generation => {
                    (p.reducer.clone(), p.execution.clone(), true)
                }
                Some(_) => (String::new(), ExecutionKind::InProcess, false),
                None => (String::new(), ExecutionKind::InProcess, false),
            }
        };
        if !state_ok {
            let _ = self.settle_ticket(ticket);
            let ps = self.plugins.lock();
            let code = match ps.get(&fiber).map(|p| p.state.canonical()) {
                Some(Fsm::Failed) | Some(Fsm::Disposed) => "plugin-not-active",
                Some(Fsm::Quiescing) | Some(Fsm::CleanupPending) => "context-not-active",
                Some(Fsm::Waiting) | Some(Fsm::Preparing) | Some(Fsm::Registered) => "dependency-unavailable",
                _ => "plugin-not-loaded",
            };
            return (json!({"error": "plugin not available", "code": code}), false);
        }
        // External execution: hands off to the host (deadline + cancellation);
        // a missing forwarder is an explicit error, never silent dispatch.
        if let ExecutionKind::External { timeout_ms, .. } = execution {
            let (instance, generation) = (open.instance, open.generation);
            let fwd = self.forwarder.lock().clone();
            let Some(fwd) = fwd else {
                let _ = self.settle_ticket(ticket);
                return (json!({"error": "external plugin without host", "code": "internal"}), false);
            };
            let req = ForwardRequest {
                ticket,
                cap: cap.to_string(),
                input: input.clone(),
                logical: fiber.clone(),
                instance,
                generation,
                timeout_ms,
                cancel: open.cancel.clone(),
            };
            let out = fwd.forward(&req);
            // Post-validation of the ticket (mid-flight revoke → no false success).
            let admitted = matches!(self.calls.get(ticket), Some(t) if t.state == TicketState::Admitted);
            let _ = self.settle_ticket(ticket);
            match out {
                crate::external::ForwardOutcome::Ok(value) if admitted => {
                    if !self.dry_run {
                        self.journal.append(&fiber, "cap.invoke", json!({"cap": cap, "input": input}), Value::Null);
                    }
                    return (value, true);
                }
                crate::external::ForwardOutcome::Ok(_) => {
                    self.journal.append(
                        &fiber,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "revoked-during-forward", "cap": cap}),
                        Value::Null,
                    );
                    return (json!({"error": "ticket revoked", "code": "cancelled"}), false);
                }
                crate::external::ForwardOutcome::Err { code, message } if admitted => {
                    return (json!({"error": message, "code": code}), false);
                }
                crate::external::ForwardOutcome::Err { code, message } => {
                    let _ = code;
                    self.journal.append(
                        &fiber,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "revoked-during-forward", "cap": cap}),
                        Value::Null,
                    );
                    return (json!({"error": message, "code": "cancelled"}), false);
                }
                crate::external::ForwardOutcome::Failed(e) => {
                    let _ = self.settle_ticket(ticket);
                    return (json!({"error": format!("forward failed: {:?}", e), "code": e.code()}), false);
                }
            }
        }
        if reducer == "crasher" {
            let _ = self.settle_ticket(ticket);
            let mode = input.get("mode").and_then(|x| x.as_str()).unwrap_or("panic");
            if mode == "panic" {
                let r = std::panic::catch_unwind(|| panic!("injected panic"));
                debug_assert!(r.is_err());
            }
            let info = self.fault(&fiber, mode);
            return (json!({"error": "injected fault", "code": "plugin-panicked", "fault": info}), false);
        }
        let entry = Reducers::dispatch(&reducer, &fiber, cap, input, self);
        let Some(entry) = entry else {
            let _ = self.settle_ticket(ticket);
            return (json!({"error": "unknown reducer", "code": "reducer-missing"}), false);
        };
        // Post-validation: disposal linearized mid-dispatch invalidates.
        match self.calls.get(ticket) {
            Some(t) if t.state == TicketState::Admitted => {}
            _ => {
                let _ = self.settle_ticket(ticket);
                self.journal.append(
                    &fiber,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "revoked-during-dispatch", "cap": cap}),
                    Value::Null,
                );
                return (json!({"error": "ticket revoked", "code": "cancelled"}), false);
            }
        }
        let _ = self.settle_ticket(ticket);
        if !self.dry_run {
            self.journal.append(&fiber, "cap.invoke", json!({"cap": cap, "input": input}), Value::Null);
        }
        (entry, true)
    }

    /// Ephemeral `invoke` ticket: no open/close journal (the
    /// `cap.invoke` stays the record), but present in the map so drain
    /// observes and revokes on dispose races.
    fn call_open_ephemeral(&self, cap: &str, _input: &Value) -> Result<OpenCall, Value> {
        if self.calls.inflight_count() >= MAX_INFLIGHT_CALLS {
            return Err(json!({"error": "too many in-flight calls", "code": "resource-exhausted"}));
        }
        let (logical, ctx) = self.admission_record(cap).map_err(|(v, _)| v)?;
        let policy = self.policy_for(&logical, cap);
        let cancel = Arc::new(AtomicBool::new(false));
        let id = self.calls.alloc(
            cap, &logical, ctx.instance, ctx.generation, ctx.context,
            ctx.epoch, policy, vec![], cancel.clone(),
        );
        match self.admission_record(cap) {
            Ok((_, fresh)) if fresh.instance == ctx.instance && fresh.generation == ctx.generation => {
                Ok(OpenCall {
                    ticket: id, cap: cap.to_string(), logical,
                    instance: ctx.instance, generation: ctx.generation,
                    context: ctx.context, policy, cancel,
                })
            }
            Ok(_) => {
                let _ = self.settle_ticket(id);
                Err(json!({"error": "stale generation", "code": "stale-generation", "logical": logical}))
            }
            Err((v, _)) => {
                let _ = self.settle_ticket(id);
                Err(v)
            }
        }
    }

    pub fn deliver_evt(&self, topic: &str, payload: &Value) {
        // Delivers only to active, current instances (I02/I03).
        let targets = self.bus.subscriber_instances(topic);
        let legacy = self.bus.subscribers(topic);
        let mut seen: std::collections::HashSet<u64> = targets.iter().map(|i| i.0).collect();
        for inst in targets {
            if self.contexts.require_active(inst).is_err() {
                continue;
            }
            let logical = self
                .contexts
                .get_by_instance(inst)
                .map(|r| r.logical)
                .unwrap_or_default();
            if logical.is_empty() {
                continue;
            }
            // Only the current generation receives events.
            if let Some(cur) = self.contexts.current(&logical) {
                if cur.instance != inst {
                    continue;
                }
            }
            let mut ps = self.plugins.lock();
            let Some(p) = ps.get_mut(&logical) else { continue };
            if p.state.canonical() != Fsm::Active {
                continue;
            }
            if p.reducer == "counter" && topic == "sys.tick" {
                let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"state": n});
            } else if p.reducer == "clock" && topic == "sys.tick" {
                let n = p.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"count": n});
            }
            let _ = payload;
        }
        // Legacy path (instanceless subscriptions, kept for compat).
        for fiber in legacy {
            let mut ps = self.plugins.lock();
            let Some(p) = ps.get_mut(&fiber) else { continue };
            if p.instance_id != 0 && seen.contains(&p.instance_id) {
                continue;
            }
            if p.instance_id != 0 {
                // New instance handled above; instanceless legacy (0) continues.
                continue;
            }
            if p.state.canonical() != Fsm::Active {
                continue;
            }
            if p.reducer == "counter" && topic == "sys.tick" {
                let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"state": n});
            } else if p.reducer == "clock" && topic == "sys.tick" {
                let n = p.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"count": n});
            }
            let _ = payload;
            let _ = &mut seen;
        }
    }

    pub fn emit(&self, topic: &str, payload: &Value) -> u64 {
        let seq = self.journal.append("cli", "evt.emit", json!({"topic": topic, "payload": payload}), Value::Null);
        self.deliver_evt(topic, payload);
        // External fan-out after in-process dispatch, holding no locks.
        if let Some(sink) = self.event_sink.lock().clone() {
            sink.on_event(topic, payload);
        }
        seq
    }

    /// Boot = journal fold (replay). Restores counters via evt.emit/sys.tick.
    pub fn replay(&self) -> (usize, usize) {
        let entries = crate::journal::Journal::read_all(&self.journal_path);
        let total = entries.len();
        let mut applied = 0;
        for e in &entries {
            if e.kind == "evt.emit" {
                let topic = e.args.get("topic").and_then(|x| x.as_str()).unwrap_or("");
                if topic == "sys.tick" {
                    let mut ps = self.plugins.lock();
                    if let Some(p) = ps.get_mut("counter") {
                        if p.state.canonical() == Fsm::Active {
                            let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                            p.json_state = json!({"state": n});
                            applied += 1;
                        }
                    }
                }
            }
        }
        (applied, total)
    }

    pub fn reset_bench(&self) {
        let _ = self.journal.reset();
        let ids: Vec<String> = self.plugins.lock().keys().cloned().collect();
        for id in &ids {
            if id.starts_with("gen") {
                self.definitions.lock().remove(id);
                if let Some(cur) = self.contexts.current(id) {
                    let _ = self.dispose_instance_inner(cur.instance, false);
                }
            }
        }
        // Clears test-acquired timers/tasks; baseline caps/subs stay.
        let inv = self.resources.inventory();
        for r in inv {
            match r.kind {
                ResourceKind::Timer { .. } | ResourceKind::Task { .. } => {
                    let _ = self.release(r.handle);
                }
                _ => {}
            }
        }
        {
            let mut ps = self.plugins.lock();
            for p in ps.values_mut() {
                // Only normalizes Active instances; Waiting/Disposed/Failed and
                // withdraw states are not resurrected here.
                if p.state.canonical() != Fsm::Active {
                    continue;
                }
                p.panic_count = 0;
                p.restart_times.clear();
                if p.reducer == "counter" {
                    p.json_state = json!({"state": 0});
                } else if p.reducer == "clock" {
                    p.json_state = json!({"count": 0});
                } else if p.reducer != "crasher" {
                    p.json_state = p.init_state.clone();
                }
            }
        }
        // Reactivates matching Active contexts and reconciles the rest.
        for c in self.contexts.snapshot() {
            if c.state.canonical() == Fsm::Active {
                self.contexts.set_state(c.instance, Fsm::Active);
            }
        }
        self.reconcile();
    }

    /// Old voided withdraw (scaffold compat). Delegated to idempotent
    /// disposal; kept for legacy external calls.
    pub fn dispose_plugin_unchecked(&self, id: &str) {
        let _ = self.dispose_plugin(id);
    }

    // Diagnostic APIs for M1.3+ (tickets/cancellation).
    pub fn active_resources_for(&self, logical: &str) -> usize {
        self.contexts
            .current(logical)
            .map(|c| self.resources.active_for(c.context))
            .unwrap_or(0)
    }

    pub fn context_state_of(&self, logical: &str) -> Option<String> {
        self.contexts
            .current(logical)
            .map(|c| c.state.as_str().to_string())
    }
}

// Keeps compat with old `dispose_plugin` calls with no return in
// external code ignoring `DisposeOutcome`: the method above returns the
        // outcome; this block only documents that callers must observe it.
// outcome; observe it in new code.
impl Kernel {
    #[allow(dead_code)]
    fn _note_idempotent_dispose(&self) {
        let _ = ContextId(0);
        let _ = InstanceId(0);
    }
}

fn initial_json_state(reducer: &str, init_state: &Value) -> Value {
    match reducer {
        "counter" => json!({"state": 0}),
        "clock" => json!({"count": 0}),
        _ => init_state.clone(),
    }
}

// ---- pure reducers (describe effects as data) ----

struct Reducers;

impl Reducers {
    fn dispatch(reducer: &str, fiber: &str, cap: &str, input: &Value, k: &Kernel) -> Option<Value> {
        match reducer {
            "echo" => Some(json!({"echo": input})),
            "ancient" => {
                let _ = (fiber, cap);
                Some(json!({"out": 42}))
            }
            "counter" => {
                let ps = k.plugins.lock();
                let n = ps.get(fiber)?.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0);
                Some(json!({"state": n}))
            }
            "clock" => {
                let ps = k.plugins.lock();
                let n = ps.get(fiber)?.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
                Some(json!({"count": n}))
            }
            "model" => Some(json!({"text": "canned-response", "in": input})),
            "noop" => Some(json!({"ok": true})),
            _ => None,
        }
    }
}
