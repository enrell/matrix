//! Supported public facade for embedding and operating Matrix (M8).
//!
//! This module is the supported contract for external applications:
//! configure, start, compose, inspect and shut down the local and remote
//! profiles without touching kernel tables, host locks, the ledger or
//! session engines. Everything else in this crate (`service`, `store`,
//! `session`, `route_controller`, `route_executor`, `remote`,
//! `remote_session_server`) is internal machinery: reachable for
//! operational necessity, with no stability promise (see
//! `docs/API-CATALOG.md`). Internal tests keep using those modules
//! directly; this facade is exercised only through its own items
//! (`tests/api_facade.rs` imports nothing else).
//!
//! Design notes (normative for the facade, not new kernel semantics):
//! - Errors are stable string codes ([`ErrorCode`]); human messages are
//!   diagnostic only and never the sole control channel.
//! - Handles are opaque strings; ids/activations are validated claims.
//! - [`Config::validate`] diagnoses before any mutation; reload applies
//!   nothing when validation fails (failures are explicit, never half
//!   grants).
//! - Inspection snapshots carry a schema tag, bounded lists with
//!   truncation flags, and no credentials (asserted in tests).
//! - Shutdown is verified: [`ShutdownReport`] tells what retired and
//!   what was still open — never fictitious success.

use crate::route_controller::{PeerConfig, RouteManager, RouteSpec};
use crate::service::{Grant, Service};
use matrix_guard::RestartPolicy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Versioned inspect snapshot schema served by [`Runtime::inspect`].
pub const INSPECT_SCHEMA: &str = "matrix.inspect/1";

/// Facade contract version (experimental; see version policy in
/// `docs/VERSIONS.md`). Unrelated to crate numbers.
pub const API_VERSION: &str = "0.1.0-experimental";

/// Stable error codes returned by every facade fallible operation.
/// Wire/db diagnostics map onto these; unknown strings become
/// [`ErrorCode::Internal`] without losing the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode {
    PermissionDenied,
    StaleGeneration,
    ResourceExhausted,
    OutcomeUnknown,
    InvalidMessage,
    InvalidSchema,
    UnknownComponent,
    UnsupportedCombination,
    MissingReference,
    ContextNotActive,
    ComponentAlreadyLeased,
    Internal,
}

impl ErrorCode {
    /// Stable wire spelling (kebab-case, frozen while `API_VERSION`
    /// stays `0.x-experimental` per `docs/VERSIONS.md`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PermissionDenied => "permission-denied",
            Self::StaleGeneration => "stale-generation",
            Self::ResourceExhausted => "resource-exhausted",
            Self::OutcomeUnknown => "outcome-unknown",
            Self::InvalidMessage => "invalid-message",
            Self::InvalidSchema => "invalid-schema",
            Self::UnknownComponent => "unknown-component",
            Self::UnsupportedCombination => "unsupported-combination",
            Self::MissingReference => "missing-reference",
            Self::ContextNotActive => "context-not-active",
            Self::ComponentAlreadyLeased => "component-already-leased",
            Self::Internal => "internal",
        }
    }

    /// Parses a code produced anywhere in the stack (kernel, host,
    /// service, transport). Unrecognized spellings are [`Self::Internal`].
    pub fn parse(s: &str) -> Self {
        match s {
            "permission-denied" => Self::PermissionDenied,
            "stale-generation" => Self::StaleGeneration,
            "resource-exhausted" => Self::ResourceExhausted,
            "outcome-unknown" => Self::OutcomeUnknown,
            "invalid-message" => Self::InvalidMessage,
            "invalid-schema" => Self::InvalidSchema,
            "unknown-component" => Self::UnknownComponent,
            "unsupported-combination" => Self::UnsupportedCombination,
            "missing-reference" => Self::MissingReference,
            "context-not-active" => Self::ContextNotActive,
            "component-already-leased" => Self::ComponentAlreadyLeased,
            _ => Self::Internal,
        }
    }
}

/// Facade error: stable code for control flow, message for humans.
#[derive(Debug, Clone, Serialize)]
pub struct Error {
    pub code: ErrorCode,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

impl From<String> for Error {
    /// Service/kernel `Err(String)` convention: the head token before a
    /// `": "` separator is the code when it parses, else internal.
    /// Messages survive verbatim either way.
    fn from(s: String) -> Self {
        let head = s.split(':').next().unwrap_or("").trim();
        Self { code: ErrorCode::parse(head), message: s }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for Error {}

/// One actionable config diagnostic (validate-before-mutate).
#[derive(Debug, Clone, Serialize)]
pub struct Diagnostic {
    /// Dotted field path (`grants.alice`, `remotes.peers[0].address`).
    pub field: String,
    pub code: ErrorCode,
    pub message: String,
}

/// One component entry: operator-owned manifest plus launch policy.
/// Either sandboxed or explicitly trusted — never neither.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentSpec {
    pub manifest: Value,
    #[serde(default)]
    pub sandbox: Option<matrix_guard::Sandbox>,
    #[serde(default)]
    pub trusted: bool,
    #[serde(default)]
    pub restart: Option<RestartPolicy>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSpec {
    pub listen: std::net::SocketAddr,
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemotePeerSpec {
    pub name: String,
    pub address: std::net::SocketAddr,
    pub server_name: String,
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub mgmt_address: std::net::SocketAddr,
    pub domain: String,
    pub lease_ttl_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteRouteSpec {
    pub consumer: String,
    pub provider: String,
    pub peer: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemotesSpec {
    #[serde(default)]
    pub authority: String,
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub peers: Vec<RemotePeerSpec>,
    #[serde(default)]
    pub routes: Vec<RemoteRouteSpec>,
    #[serde(default)]
    pub session_listen: Option<std::net::SocketAddr>,
    #[serde(default)]
    pub session_ca: Option<PathBuf>,
    #[serde(default)]
    pub session_cert: Option<PathBuf>,
    #[serde(default)]
    pub session_key: Option<PathBuf>,
}

/// Operator configuration (supported surface). Parse with serde, then
/// [`Config::validate`]: only an empty diagnostic list may start or
/// reload a runtime.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub home: PathBuf,
    pub components: Vec<ComponentSpec>,
    pub grants: HashMap<String, Grant>,
    /// Operator outbound grants (consumer → exact capabilities).
    /// Independent of `outbound.request`; missing = denied.
    #[serde(default)]
    pub outbound_grants: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub tls: Option<TlsSpec>,
    #[serde(default)]
    pub remotes: RemotesSpec,
}

impl Config {
    /// Parses JSON without touching disk or runtime state.
    pub fn parse(body: &Value) -> Result<Self, Vec<Diagnostic>> {
        serde_json::from_value(body.clone()).map_err(|e| {
            vec![Diagnostic {
                field: "$".into(),
                code: ErrorCode::InvalidSchema,
                message: e.to_string(),
            }]
        })
    }

    /// Validates everything checkable before mutation: ids, launch
    /// policy, cross-references (grants/outbound/routes/peers), domains,
    /// lease ranges and session-listener completeness. Returns every
    /// problem found (no fail-fast truncation).
    pub fn validate(&self) -> Vec<Diagnostic> {
        let mut out = vec![];
        let mut diag = |field: &str, code: ErrorCode, message: &str| {
            out.push(Diagnostic { field: field.into(), code, message: message.into() });
        };
        if !self.home.is_absolute() {
            diag("home", ErrorCode::InvalidSchema, "home must be absolute");
        }
        let mut ids = HashSet::new();
        // Routed providers also satisfy `requires` (attested at runtime).
        // Component ids are collected first so forward references (cons
        // before prov) resolve: declaration order carries no meaning.
        // (`ids` below stays the duplicate detector for the main loop.)
        let mut satisfiable: HashSet<&str> = HashSet::new();
        for r in &self.remotes.routes {
            satisfiable.insert(r.provider.as_str());
        }
        for c in &self.components {
            if let Some(id) = c.manifest.get("id").and_then(|v| v.as_str()) {
                satisfiable.insert(id);
            }
        }
        for (i, c) in self.components.iter().enumerate() {
            let base = format!("components[{i}]");
            let Some(id) = c.manifest.get("id").and_then(|v| v.as_str()) else {
                diag(&format!("{base}.manifest.id"), ErrorCode::InvalidSchema, "missing component id");
                continue;
            };
            if id.is_empty()
                || id.len() > 128
                || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                diag(&format!("{base}.manifest.id"), ErrorCode::InvalidSchema, "id must be 1..128 of [A-Za-z0-9_-]");
            }
            if !ids.insert(id.to_string()) {
                diag(&format!("{base}.manifest.id"), ErrorCode::InvalidSchema, "duplicate component id");
            }
            if c.sandbox.is_none() && !c.trusted {
                diag(&base, ErrorCode::UnsupportedCombination, "select sandbox or explicitly trusted");
            }
            if c.manifest.get("remote") == Some(&json!(true)) {
                diag(
                    &format!("{base}.manifest"),
                    ErrorCode::UnsupportedCombination,
                    "remote-only definitions belong to remotes.routes[].capabilities, not components[]",
                );
            }
            // Full manifest validation before any mutation: capabilities,
            // requires, subscriptions, execution, outbound and limits are
            // checked by the same parser provisioning uses.
            match matrix_core::kernel::parse_manifest_value(&c.manifest) {
                Ok(parsed) => {
                    for req in &parsed.requires {
                        if let Some(p) = &req.provider {
                            if !satisfiable.contains(p.as_str()) {
                                diag(
                                    &format!("{base}.manifest.requires"),
                                    ErrorCode::MissingReference,
                                    &format!("provider {p} is neither provisioned nor routed"),
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    diag(&format!("{base}.manifest"), ErrorCode::InvalidSchema, &e);
                }
            }
        }
        let known: HashSet<&str> = ids.iter().map(String::as_str).collect();
        for (principal, grant) in &self.grants {
            for c in &grant.components {
                if !known.contains(c.as_str()) {
                    diag(
                        &format!("grants.{principal}"),
                        ErrorCode::MissingReference,
                        &format!("component {c} not provisioned"),
                    );
                }
            }
        }
        for (consumer, caps) in &self.outbound_grants {
            if !known.contains(consumer.as_str()) {
                diag(
                    &format!("outbound_grants.{consumer}"),
                    ErrorCode::MissingReference,
                    "consumer not provisioned",
                );
            }
            if caps.is_empty() {
                diag(
                    &format!("outbound_grants.{consumer}"),
                    ErrorCode::InvalidSchema,
                    "capability list must not be empty",
                );
            }
        }
        let mut peers = HashSet::new();
        for (i, p) in self.remotes.peers.iter().enumerate() {
            let base = format!("remotes.peers[{i}]");
            if p.name.is_empty() || p.name.len() > 128 {
                diag(&format!("{base}.name"), ErrorCode::InvalidSchema, "peer name must be 1..128");
            } else if !peers.insert(p.name.clone()) {
                diag(&format!("{base}.name"), ErrorCode::InvalidSchema, "duplicate peer name");
            }
            if p.domain.is_empty()
                || p.domain.len() > 64
                || !p.domain.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                diag(&format!("{base}.domain"), ErrorCode::InvalidSchema, "domain must be 1..64 of [A-Za-z0-9_-]");
            }
            if !(100..=30_000).contains(&p.lease_ttl_ms) {
                diag(&format!("{base}.lease_ttl_ms"), ErrorCode::InvalidSchema, "lease ttl must be 100..30000 ms");
            }
            if p.server_name.is_empty() {
                diag(&format!("{base}.server_name"), ErrorCode::MissingReference, "server name required for mTLS");
            }
        }
        if !self.remotes.domain.is_empty()
            && (self.remotes.domain.len() > 64
                || !self.remotes.domain.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
        {
            diag("remotes.domain", ErrorCode::InvalidSchema, "domain must be 1..64 of [A-Za-z0-9_-]");
        }
        for (i, r) in self.remotes.routes.iter().enumerate() {
            let base = format!("remotes.routes[{i}]");
            if !peers.contains(&r.peer) {
                diag(&format!("{base}.peer"), ErrorCode::MissingReference, "peer not configured (fail closed)");
            }
            if !known.contains(r.consumer.as_str()) {
                diag(&format!("{base}.consumer"), ErrorCode::MissingReference, "consumer not provisioned");
            }
            if r.capabilities.is_empty() {
                diag(
                    &format!("{base}.capabilities"),
                    ErrorCode::InvalidSchema,
                    "remote provider capabilities must be declared",
                );
            }
        }
        let sess = [
            self.remotes.session_listen.is_some(),
            self.remotes.session_ca.is_some(),
            self.remotes.session_cert.is_some(),
            self.remotes.session_key.is_some(),
        ];
        if sess.iter().any(|b| *b) && sess.iter().any(|b| !b) {
            diag("remotes.session_*", ErrorCode::InvalidSchema, "session listener fields must be all present or all absent");
        }
        out
    }
}

/// What a reload changed. Failed reloads change nothing (validate runs
/// first); partial runtime failures during apply are reported per area
/// with the resulting state left explicit (routes re-sync on the next
/// valid reload; grants already retired stay retired).
#[derive(Debug, Clone, Serialize, Default)]
pub struct ReloadReport {
    pub grants_applied: bool,
    pub outbound_applied: bool,
    pub remotes_applied: bool,
    /// Principals revoked because the new config dropped them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revoked: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

/// Merge applied grants after a reload attempt: the baseline must mirror
/// the LIVE set, or the next rotation would miss partially applied
/// grants. Pure (unit-tested): failed grants keep old caps when they
/// existed, vanish when they never went live; dropped principals vanish
/// unless their revoke failed (still live with old caps).
fn merge_applied_grants(
    old: &HashMap<String, Grant>,
    new: &HashMap<String, Grant>,
    grant_failed: &HashSet<String>,
    revoke_failed: &HashSet<String>,
) -> HashMap<String, Grant> {
    let mut applied = HashMap::new();
    for (principal, grant) in new {
        if grant_failed.contains(principal) {
            if let Some(old_grant) = old.get(principal) {
                applied.insert(principal.clone(), old_grant.clone());
            }
        } else {
            applied.insert(principal.clone(), grant.clone());
        }
    }
    for (principal, grant) in old {
        if !new.contains_key(principal) && revoke_failed.contains(principal) {
            applied.insert(principal.clone(), grant.clone());
        }
    }
    applied
}

/// Fields a reload never applies hot: changing any of these is diagnosed
/// (not silently accepted) and requires an operator restart.
fn restart_diff(active: &Config, new: &Config) -> Vec<Diagnostic> {
    let mut out = vec![];
    let mut diag = |field: &str, message: &str| {
        out.push(Diagnostic {
            field: field.into(),
            code: ErrorCode::UnsupportedCombination,
            message: message.into(),
        });
    };
    let canon = |v: &Value| serde_json::to_string(v).unwrap_or_default();
    if active.home != new.home {
        diag("home", "home changes require restart, not applied");
    }
    if canon(&json!(active.tls)) != canon(&json!(new.tls)) {
        diag("tls", "listener/TLS changes require restart, not applied");
    }
    if active.remotes.session_listen != new.remotes.session_listen
        || active.remotes.session_ca != new.remotes.session_ca
        || active.remotes.session_cert != new.remotes.session_cert
        || active.remotes.session_key != new.remotes.session_key
    {
        diag("remotes.session_*", "session listener changes require restart, not applied");
    }
    if active.remotes.authority != new.remotes.authority || active.remotes.domain != new.remotes.domain {
        diag("remotes.authority/domain", "authority/domain changes require restart, not applied");
    }
    let mut old: Vec<String> = active.components.iter().map(|c| canon(&json!(c))).collect();
    let mut cur: Vec<String> = new.components.iter().map(|c| canon(&json!(c))).collect();
    old.sort();
    cur.sort();
    if old != cur {
        diag("components", "component set or launch policy changes require restart (use provision/remove/activate), not applied");
    }
    out
}

/// Verified shutdown outcome: counts before and after, so a caller can
/// tell a clean stop from leftovers. `sessions_after`,
/// `leases_after` and `pending_calls_after` are expected to be zero; any
/// nonzero is reported with its count (no payloads), never hidden.
/// Never fictitious success.
#[derive(Debug, Clone, Serialize)]
pub struct ShutdownReport {
    pub sessions_before: usize,
    pub sessions_after: usize,
    pub leases_before: usize,
    pub leases_after: usize,
    pub pending_calls_after: usize,
    pub routes_active: usize,
}

/// Bounded inspect options (limits are caps, not offsets: over-limit
/// lists truncate with `truncated: true` plus totals).
#[derive(Debug, Clone, Copy)]
pub struct InspectOpts {
    pub max_calls: usize,
    pub max_leases: usize,
}

impl Default for InspectOpts {
    fn default() -> Self {
        Self { max_calls: 128, max_leases: 128 }
    }
}

/// Offline backup of a stopped service's store (M8 P09): read-only copy,
/// origin preserved bit-for-bit. Fails on live stores, corrupt images,
/// schema mismatch and existing destinations.
pub fn backup_offline(home: &std::path::Path, dest: &std::path::Path) -> Result<(), Error> {
    let src = crate::store::Store::open_read_only(&home.join("state/runtime.sqlite")).map_err(Error::from)?;
    src.copy_to(dest).map_err(Error::from)
}

/// Stages a validated backup into a home directory for the next boot
/// (M8 P09). Refuses corrupt/versioned images and never overwrites
/// existing state: remove it explicitly first. Booting afterwards is a
/// recovery open (epoch advances, `admitted`→`unknown`); leases and
/// sessions from before are gone by construction (memory-only), and
/// persisted revocations stay in force.
pub fn restore_backup(backup: &std::path::Path, home: &std::path::Path) -> Result<(), Error> {
    let src = crate::store::Store::open_read_only(backup).map_err(Error::from)?;
    let dest = home.join("state/runtime.sqlite");
    if dest.exists() {
        return Err(Error::new(ErrorCode::UnsupportedCombination, "destination store exists; remove it explicitly first"));
    }
    if home.join("state/runtime.sqlite-lock").exists() {
        // A lock file next to missing state is stale debris; a live
        // runtime always holds the state file itself.
        let _ = std::fs::remove_file(home.join("state/runtime.sqlite-lock"));
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::new(ErrorCode::Internal, e.to_string()))?;
    }
    src.copy_to(&dest).map_err(Error::from)
}

/// A started runtime: operator facade over one home directory.
/// Sessions, leases, routes and listeners live here; dropping without
/// [`Runtime::shutdown`] still stops supervision (service Drop), but only
/// `shutdown` reports the outcome.
pub struct Runtime {
    service: Arc<Service>,
    routes: Option<RouteManager>,
    servers: std::sync::Mutex<Vec<ServerGuard>>,
    /// Last applied configuration: reload diffs against it (rotation +
    /// restart-required detection), then replaces it on success.
    active: std::sync::Mutex<Config>,
}

enum ServerGuard {
    Unary(crate::remote::Server),
    Session(crate::remote_session_server::RemoteSessionServer),
}

impl Runtime {
    /// Validates, then starts. Validation failures change nothing.
    /// Provision failures shut down what started and name the stage.
    pub fn start(config: &Config) -> Result<Self, Error> {
        let diags = config.validate();
        if !diags.is_empty() {
            let msg = diags.iter().map(|d| format!("{}: {}", d.field, d.message)).collect::<Vec<_>>().join("; ");
            return Err(Error::new(ErrorCode::InvalidSchema, format!("invalid config: {msg}")));
        }
        let mut policy = matrix_host::HostPolicy {
            secure: true,
            components: HashMap::new(),
            enable_dependency_calls: false,
            domain: config.remotes.domain.clone(),
        };
        let mut restart = HashMap::new();
        for c in &config.components {
            let id = c.manifest["id"].as_str().unwrap_or("").to_string();
            if policy.components.insert(id.clone(), c.sandbox.clone()).is_some() {
                return Err(Error::new(ErrorCode::InvalidSchema, format!("duplicate component: {id}")));
            }
            if let Some(p) = &c.restart {
                restart.insert(id, p.clone());
            }
        }
        let service = Service::open(&config.home, policy, config.grants.clone(), restart)
            .map_err(Error::from)?;
        service.sync_outbound_grants(&config.outbound_grants);
        for c in &config.components {
            service.provision(&c.manifest).map_err(|e| {
                service.shutdown();
                Error::new(ErrorCode::parse(&e), format!("provision {}: {e}", c.manifest["id"].as_str().unwrap_or("?")))
            })?;
        }
        let mut servers = vec![];
        if let Some(t) = &config.tls {
            let s = crate::remote::Server::bind(
                service.clone(),
                crate::remote::server_config(&t.ca, &t.cert, &t.key).map_err(Error::from)?,
                t.listen,
            )
            .map_err(|e| {
                service.shutdown();
                Error::from(e)
            })?;
            servers.push(ServerGuard::Unary(s));
        }
        if let (Some(listen), Some(ca), Some(cert), Some(key)) = (
            config.remotes.session_listen,
            config.remotes.session_ca.clone(),
            config.remotes.session_cert.clone(),
            config.remotes.session_key.clone(),
        ) {
            let sc = crate::session::server_config(&ca, &cert, &key).map_err(Error::from)?;
            let s = service.serve_remote_session(listen, sc).map_err(|e| {
                service.shutdown();
                Error::from(e)
            })?;
            servers.push(ServerGuard::Session(s));
        }
        let routes = if config.remotes.peers.is_empty() && config.remotes.routes.is_empty() {
            None
        } else {
            let mgr = RouteManager::new(service.clone(), config.remotes.authority.clone());
            for r in &config.remotes.routes {
                let caps: Vec<Value> = r.capabilities.iter().map(|c| json!(c)).collect();
                let _ = service.provision_remote(&json!({"id": r.provider, "capabilities": caps, "remote": true}));
            }
            mgr.sync(
                config.remotes.peers.iter().map(|p| PeerConfig {
                    name: p.name.clone(),
                    address: p.address,
                    server_name: p.server_name.clone(),
                    ca: p.ca.clone(),
                    cert: p.cert.clone(),
                    key: p.key.clone(),
                    mgmt_address: p.mgmt_address,
                    domain: p.domain.clone(),
                    lease_ttl_ms: p.lease_ttl_ms,
                }).collect(),
                config.remotes.routes.iter().map(|r| RouteSpec {
                    consumer: r.consumer.clone(),
                    provider: r.provider.clone(),
                    peer: r.peer.clone(),
                }).collect(),
            );
            Some(mgr)
        };
        let active = config.clone();
        Ok(Self { service, routes, servers: std::sync::Mutex::new(servers), active: std::sync::Mutex::new(active) })
    }

    /// Operator activate: lease credentials for one component.
    pub fn activate(&self, principal: &str, logical: &str, ttl_ms: u64) -> Result<Value, Error> {
        self.service.activate(principal, logical, ttl_ms).map_err(Error::from)
    }

    /// Operator invoke through a lease (operation ids dedup server-side).
    pub fn invoke(
        &self,
        principal: &str,
        token: &str,
        fence: u64,
        operation: &str,
        cap: &str,
        input: &Value,
    ) -> Result<Value, Error> {
        self.service.invoke(principal, token, fence, operation, cap, input).map_err(Error::from)
    }

    /// Releases a lease (verified cleanup on the executor side too).
    pub fn release(&self, principal: &str, token: &str, fence: u64) -> Result<Value, Error> {
        self.service.release(principal, token, fence).map_err(Error::from)
    }

    /// Replaces the grant set for freshly issued leases (already-issued
    /// leases keep no widened authority; see `revoke` first for rotation).
    pub fn grant(&self, principal: String, grant: Grant) -> Result<(), Error> {
        self.service.grant(principal, grant).map_err(Error::from)
    }

    /// Retires a principal: new admissions refuse, live legs downgrade.
    pub fn revoke(&self, principal: &str) -> Result<(), Error> {
        self.service.revoke(principal).map_err(Error::from)
    }

    /// Syncs operator outbound grants (missing = denied, in-flight legs
    /// of removed edges settle through revocation).
    pub fn sync_outbound_grants(&self, want: &HashMap<String, Vec<String>>) {
        self.service.sync_outbound_grants(want);
    }

    /// Two-phase reload owned by the facade: validates the candidate
    /// first (diagnostics mutate nothing, including restart-required
    /// fields, which are reported, never silently accepted), then rotates
    /// authority (revokes principals the new config dropped, so removed
    /// grants stop admitting immediately) and applies grants → outbound
    /// → remotes, reporting per-area outcome. The applied config becomes
    /// the baseline for the next reload.
    pub fn reload(&self, new: &Config) -> Result<ReloadReport, Vec<Diagnostic>> {
        let mut diags = new.validate();
        {
            let active = self.active.lock().unwrap();
            diags.extend(restart_diff(&active, new));
        }
        if !diags.is_empty() {
            return Err(diags);
        }
        let mut report = ReloadReport::default();
        let mut errors = vec![];
        // Rotation: retire what the new config dropped BEFORE installing
        // anything, so removed authority never survives beside the new set.
        // Revoke/grant failures are tracked per principal (not just as
        // strings): the baseline below must mirror the LIVE set, or the
        // next rotation would miss partially applied grants.
        let dropped: Vec<String> = {
            let active = self.active.lock().unwrap();
            active.grants.keys().filter(|p| !new.grants.contains_key(*p)).cloned().collect()
        };
        let mut revoke_failed = HashSet::new();
        for principal in dropped {
            match self.service.revoke(&principal) {
                Ok(()) => report.revoked.push(principal),
                Err(e) => {
                    revoke_failed.insert(principal.clone());
                    errors.push(format!("revoke {principal}: {e}"));
                }
            }
        }
        // Grants apply per principal; any runtime failure is reported
        // explicitly (validation already excluded invalid configs, so a
        // failure here is environmental and inspectable, never silent).
        let mut grant_failed = HashSet::new();
        for (principal, grant) in new.grants.clone() {
            if let Err(e) = self.service.grant(principal.clone(), grant) {
                grant_failed.insert(principal.clone());
                errors.push(format!("grants.{principal}: {e}"));
            }
        }
        report.grants_applied = grant_failed.is_empty() && revoke_failed.is_empty();
        self.service.sync_outbound_grants(&new.outbound_grants);
        report.outbound_applied = true;
        if let Some(mgr) = &self.routes {
            let routes: Vec<RouteSpec> = new.remotes.routes.iter().map(|r| RouteSpec {
                consumer: r.consumer.clone(),
                provider: r.provider.clone(),
                peer: r.peer.clone(),
            }).collect();
            for r in &new.remotes.routes {
                let caps: Vec<Value> = r.capabilities.iter().map(|c| json!(c)).collect();
                let _ = self.service.provision_remote(&json!({"id": r.provider, "capabilities": caps, "remote": true}));
            }
            mgr.sync(
                new.remotes.peers.iter().map(|p| PeerConfig {
                    name: p.name.clone(),
                    address: p.address,
                    server_name: p.server_name.clone(),
                    ca: p.ca.clone(),
                    cert: p.cert.clone(),
                    key: p.key.clone(),
                    mgmt_address: p.mgmt_address,
                    domain: p.domain.clone(),
                    lease_ttl_ms: p.lease_ttl_ms,
                }).collect(),
                routes,
            );
            report.remotes_applied = true;
        } else if !new.remotes.peers.is_empty() || !new.remotes.routes.is_empty() {
            errors.push("remotes: routes require restart (manager starts at boot only)".into());
            report.remotes_applied = false;
        } else {
            report.remotes_applied = true;
        }
        report.errors = errors;
        // The new baseline mirrors the EFFECTIVELY applied set, not the
        // requested one: partially applied grants stay visible to the next
        // rotation instead of leaking invisibly beside it. Outbound and
        // remotes sync infallibly, so they follow the candidate; restart-
        // gated areas never change hot (diagnosed above).
        {
            let old_grants = self.active.lock().unwrap().grants.clone();
            let mut applied = new.clone();
            applied.grants = merge_applied_grants(&old_grants, &new.grants, &grant_failed, &revoke_failed);
            *self.active.lock().unwrap() = applied;
        }
        Ok(report)
    }

    /// Versioned, bounded, redacted inspection snapshot. Lists truncate at
    /// `opts` caps with `truncated: true`; credentials never appear (a
    /// test holds a live token and greps the output).
    pub fn inspect(&self, opts: InspectOpts) -> Value {
        let raw = self.service.inspect();
        let take = |v: &Value, key: &str, max: usize| -> (Vec<Value>, bool, usize) {
            let arr = v.get(key).and_then(|x| x.as_array()).cloned().unwrap_or_default();
            let total = arr.len();
            let items: Vec<Value> = arr.into_iter().take(max).collect();
            let truncated = total > max;
            (items, truncated, total)
        };
        let kernel = raw.get("kernel").cloned().unwrap_or(Value::Null);
        let (calls, calls_truncated, calls_total) = take(&kernel, "calls", opts.max_calls);
        let (leases, leases_truncated, leases_total) = take(&raw, "leases", opts.max_leases);
        json!({
            "schema": INSPECT_SCHEMA,
            "api": API_VERSION,
            "epoch": raw.get("epoch").cloned().unwrap_or(Value::Null),
            "calls": calls,
            "calls_truncated": calls_truncated,
            "calls_total": calls_total,
            "leases": leases,
            "leases_truncated": leases_truncated,
            "leases_total": leases_total,
            "kernel": {
                "instances": kernel.get("instances").cloned().unwrap_or(Value::Null),
                "remotes": kernel.get("remotes").cloned().unwrap_or(Value::Null),
            },
        })
    }

    /// Unary management listen address, if bound.
    pub fn unary_addr(&self) -> Option<std::net::SocketAddr> {
        self.servers.lock().unwrap().iter().find_map(|s| match s {
            ServerGuard::Unary(srv) => Some(srv.address),
            _ => None,
        })
    }

    /// Remote session listen address, if bound (route B).
    pub fn session_addr(&self) -> Option<std::net::SocketAddr> {
        self.servers.lock().unwrap().iter().find_map(|s| match s {
            ServerGuard::Session(srv) => Some(srv.address),
            _ => None,
        })
    }

    /// Online snapshot through the live handle (works while running;
    /// page-level copy, no epoch advance, no state flips).
    pub fn snapshot_backup(&self, dest: &std::path::Path) -> Result<(), Error> {
        self.service.store.snapshot(dest).map_err(Error::from)
    }

    /// Operator install: stages a validated component manifest (not
    /// activated). Remote-only snapshots go through routes, not here.
    pub fn provision(&self, manifest: &Value) -> Result<(), Error> {
        self.service.provision(manifest).map_err(Error::from)
    }

    /// Operator remove: withdraws the definition, drops sessions, frees
    /// handles. Reports the kernel outcome verbatim (`Disposed`,
    /// `AlreadyDisposed`, or `CleanupPending` with reason) — a pending
    /// cleanup is listed, never hidden.
    pub fn remove(&self, logical: &str) -> Result<String, Error> {
        use matrix_core::DisposeOutcome;
        match self.service.kernel.dispose_plugin(logical) {
            DisposeOutcome::Disposed => Ok("Disposed".into()),
            DisposeOutcome::AlreadyDisposed => Ok("AlreadyDisposed".into()),
            DisposeOutcome::CleanupPending { reason } => Ok(format!("CleanupPending: {reason}")),
        }
    }

    /// Operator broadcast to bus subscribers (best-effort fan-out;
    /// delivery, quotas and loss counting follow the event profile).
    /// Returns the kernel sequence number for correlation.
    pub fn broadcast(&self, topic: &str, payload: &Value) -> u64 {
        self.service.kernel.emit(topic, payload)
    }

    /// Waits for a component session to be serving (process spawned and
    /// registered) after activate. Activation alone only loads the
    /// definition; readiness is explicit and observable here — callers
    /// must not assume an invoke right after activate finds a session.
    pub fn wait_session(&self, logical: &str, timeout: std::time::Duration) -> Result<(), Error> {
        let t0 = std::time::Instant::now();
        loop {
            if let Some(inst) = self.service.kernel.instance_of(logical) {
                if self.service.host.has_session(logical, inst.0) {
                    return Ok(());
                }
            }
            if t0.elapsed() >= timeout {
                return Err(Error::new(ErrorCode::OutcomeUnknown, format!("no session for {logical}")));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Host control socket for out-of-band conformance tooling
    /// (raw-protocol checks). Normal integration uses the SDKs instead.
    pub fn host_sock_path(&self) -> std::path::PathBuf {
        self.service.host.sock_path()
    }

    /// Verified shutdown: retires leases, stops listeners, supervision,
    /// host and routes, then reports the after-state. Idempotent: a
    /// second call reports zeros.
    pub fn shutdown(&self) -> ShutdownReport {
        let sessions_before = self.service.host.session_count();
        let leases_before = self.service.live_lease_count();
        if let Some(mgr) = &self.routes {
            mgr.shutdown();
        }
        for srv in self.servers.lock().unwrap().iter_mut() {
            match srv {
                ServerGuard::Unary(s) => s.shutdown(),
                ServerGuard::Session(s) => s.shutdown(),
            }
        }
        self.service.shutdown();
        // Let reader threads observe EOF and reap (bounded wait): the
        // report reflects the settled state, not a mid-flight snapshot.
        let t0 = std::time::Instant::now();
        while self.service.host.session_count() > 0 && t0.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(10));
        }
        ShutdownReport {
            sessions_before,
            sessions_after: self.service.host.session_count(),
            leases_before,
            leases_after: self.service.live_lease_count(),
            pending_calls_after: self.service.kernel.pending_calls().len(),
            routes_active: self.routes.as_ref().map(|m| m.inspect()["peers"].as_array().map(|p| p.len()).unwrap_or(0)).unwrap_or(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(caps: &[&str]) -> Grant {
        Grant {
            components: ["echo".into()].into_iter().collect(),
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn merge_full_success_applies_everything() {
        let old: HashMap<String, Grant> =
            [("alice".to_string(), grant(&["echo.msg@1"]))].into_iter().collect();
        let new: HashMap<String, Grant> =
            [("alice".to_string(), grant(&["echo.msg@1", "matrix.effect.write"]))].into_iter().collect();
        let got = merge_applied_grants(&old, &new, &HashSet::new(), &HashSet::new());
        assert_eq!(got.keys().collect::<HashSet<_>>(), new.keys().collect::<HashSet<_>>());
        assert!(got["alice"].capabilities.contains("matrix.effect.write"));
    }

    #[test]
    fn merge_total_failure_keeps_old_baseline() {
        let old: HashMap<String, Grant> =
            [("alice".to_string(), grant(&["echo.msg@1"]))].into_iter().collect();
        let new: HashMap<String, Grant> =
            [("alice".to_string(), grant(&["echo.msg@1", "matrix.effect.write"])),
             ("bob".to_string(), grant(&["echo.msg@1"]))].into_iter().collect();
        let failed: HashSet<String> = ["alice".to_string(), "bob".to_string()].into_iter().collect();
        let got = merge_applied_grants(&old, &new, &failed, &HashSet::new());
        assert_eq!(got.keys().collect::<HashSet<_>>(), old.keys().collect::<HashSet<_>>(), "nothing went live: baseline stays exactly old");
        assert!(!got["alice"].capabilities.contains("matrix.effect.write"));
    }

    #[test]
    fn merge_partial_tracks_each_principal() {
        // alice's new caps went live, bob's never did, carol's revoke
        // failed (still live with old caps): the next rotation must see
        // all three, or bob leaks invisibly / carol is forgotten.
        let old: HashMap<String, Grant> = [
            ("alice".to_string(), grant(&["echo.msg@1"])),
            ("carol".to_string(), grant(&["echo.msg@1"])),
        ].into_iter().collect();
        let new: HashMap<String, Grant> = [
            ("alice".to_string(), grant(&["echo.msg@1", "matrix.effect.write"])),
            ("bob".to_string(), grant(&["echo.msg@1"])),
        ].into_iter().collect();
        let grant_failed: HashSet<String> = ["bob".to_string()].into_iter().collect();
        let revoke_failed: HashSet<String> = ["carol".to_string()].into_iter().collect();
        let got = merge_applied_grants(&old, &new, &grant_failed, &revoke_failed);
        assert_eq!(got.len(), 2, "alice live with new caps, carol retained, bob absent");
        assert!(got["alice"].capabilities.contains("matrix.effect.write"), "applied kept");
        assert!(!got.contains_key("bob"), "never-live stays out");
        assert_eq!(got["carol"].capabilities, old["carol"].capabilities, "failed revoke stays tracked");
    }
}
