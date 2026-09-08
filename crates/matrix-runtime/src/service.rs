//! Operator-provisioned runtime boundary. Remote peers cannot provide arbitrary
//! executable manifests or bypass grants. Leases expire at this resource owner.
use crate::store::{Admission, Result, Store};
use matrix_core::{InstanceRef, Journal, Kernel};
use matrix_guard::{random_token, token_eq, RestartBudget, RestartPolicy};
use matrix_host::{Host, HostPolicy};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub components: HashSet<String>,
    pub capabilities: HashSet<String>,
}
struct Lease {
    principal: String,
    logical: String,
    token: String,
    fence: u64,
    deadline: Instant,
    owner: InstanceRef,
    external: bool,
    started: Instant,
    retry: Option<Instant>,
    failed: bool,
    budget: RestartBudget,
    /// Last session-renew sequence served (M7 idempotent renew; 0 = none).
    /// The unary `renew` path never touches it.
    renew_seq: u64,
}
struct State {
    leases: HashMap<String, Lease>,
    grants: HashMap<String, Grant>,
    revoked: HashSet<String>,
    /// Installed outbound grants: (consumer, capability).
    outbound: HashSet<(String, String)>,
}
pub struct Service {
    pub kernel: Arc<Kernel>,
    pub host: Arc<Host>,
    pub store: Store,
    home: PathBuf,
    allowed_external: HashSet<String>,
    state: Mutex<State>,
    restart: HashMap<String, RestartPolicy>,
    stopped: AtomicBool,
    /// Live executor routes (weak; M7 revocation push + diagnosis).
    remote_routes: Mutex<Vec<std::sync::Weak<crate::route_executor::Route>>>,
}
impl Service {
    pub fn open(
        home: &Path,
        policy: HostPolicy,
        grants: HashMap<String, Grant>,
        restart: HashMap<String, RestartPolicy>,
    ) -> Result<Arc<Self>> {
        if !policy.secure {
            return Err("managed runtime requires secure HostPolicy".into());
        }
        // Full dependency circuit (M6): the managed profile
        // advertises `dependency-calls/1`; outbound grants come from the operator
        // config via `sync_outbound_grants`.
        let mut policy = policy;
        policy.enable_dependency_calls = true;
        let store = Store::open(&home.join("state/runtime.sqlite"))?;
        std::fs::create_dir_all(home.join("plugins")).map_err(|e| e.to_string())?;
        let kernel = Arc::new(Kernel::new(
            &home.to_path_buf(),
            Journal::open(&home.join("run/journal.jsonl"), false, false)
                .map_err(|e| e.to_string())?,
            false,
        ));
        let allowed_external = policy.components.keys().cloned().collect();
        let host = Host::attach_with_policy(kernel.clone(), &home.join("host"), policy)
            .map_err(|e| e.to_string())?;
        let revoked = store.revoked()?;
        let s = Arc::new(Self {
            kernel,
            host,
            store,
            home: home.into(),
            allowed_external,
            state: Mutex::new(State {
                leases: HashMap::new(),
                grants,
                revoked,
                outbound: HashSet::new(),
            }),
            restart,
            stopped: AtomicBool::new(false),
            remote_routes: Mutex::new(vec![]),
        });
        // Desired definitions are retained but never automatically replayed as effects.
        // No peer-owned component is published before fresh authenticated activation.
        let weak = Arc::downgrade(&s);
        std::thread::Builder::new()
            .name("matrix-supervisor".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_millis(20));
                let Some(s) = weak.upgrade() else { break };
                if s.stopped.load(Ordering::SeqCst) {
                    break;
                }
                s.tick();
            })
            .map_err(|e| e.to_string())?;
        Ok(s)
    }
    /// Operator API only: validated manifests are stored durably, not activated.
    pub fn provision(&self, body: &Value) -> Result<()> {
        let id = body.get("id").and_then(Value::as_str).ok_or("missing id")?;
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err("invalid component id".into());
        }
        // Parse using the kernel's own candidate validator via public parser.
        let parsed = matrix_core::kernel::parse_manifest_value(body)?;
        if parsed.execution.is_external() && !self.allowed_external.contains(id) {
            return Err("execution not operator-authorized".into());
        }
        if parsed.remote {
            // Remote-only definitions never materialize locally: they carry
            // no executable and must not enter reconciliation as locals.
            // The route manager installs the capability snapshot directly.
            return Err("remote definitions are installed by the route manager".into());
        }
        self.store.set_desired(id, body)
    }
    fn materialize(&self, logical: &str) -> Result<PathBuf> {
        let (_, body) = self
            .store
            .desired()?
            .into_iter()
            .find(|(id, _)| id == logical)
            .ok_or("component not provisioned")?;
        let path = self.home.join("plugins").join(format!("{logical}.json"));
        let tmp = path.with_extension("pending");
        let mut f = File::create(&tmp).map_err(|e| e.to_string())?;
        f.write_all(serde_json::to_string(&body).unwrap().as_bytes())
            .map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
        File::open(path.parent().unwrap())
            .and_then(|d| d.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(path)
    }
    pub fn authorized(&self, principal: &str) -> bool {
        let s = self.state.lock().unwrap();
        s.grants.contains_key(principal) && !s.revoked.contains(principal)
    }
    pub fn activate(&self, principal: &str, logical: &str, ttl_ms: u64) -> Result<Value> {
        if !(100..=30000).contains(&ttl_ms) {
            return Err("invalid lease duration".into());
        }
        let mut s = self.state.lock().unwrap();
        if s.revoked.contains(principal)
            || !s
                .grants
                .get(principal)
                .is_some_and(|g| g.components.contains(logical))
        {
            return Err("permission-denied".into());
        }
        if s.leases.values().any(|l| l.logical == logical) {
            return Err("component-already-leased".into());
        }
        if s.leases.len() >= 128 {
            return Err("resource-exhausted".into());
        }
        let path = self.materialize(logical)?;
        let token = random_token().map_err(|e| e.to_string())?;
        let fence = self.store.next_fence(logical)?;
        self.store.event(
            "lease.created",
            &json!({"logical":logical,"principal":principal,"fence":fence.to_string()}),
        )?;
        if let Err(e) = self.kernel.load_manifest(&path) {
            self.kernel.dispose_plugin(logical);
            return Err(e);
        }
        let owner = self
            .kernel
            .instance_ref_of(logical)
            .ok_or("activation failed")?;
        let external = self
            .kernel
            .definitions
            .lock()
            .get(logical)
            .is_some_and(|d| d.execution.is_external());
        let response = json!({"lease":token,"fence":fence.to_string(),"ttl_ms":ttl_ms,"instance":owner.describe(),"epoch":self.store.epoch.to_string()});
        s.leases.insert(
            token.clone(),
            Lease {
                principal: principal.into(),
                logical: logical.into(),
                token,
                fence,
                deadline: Instant::now() + Duration::from_millis(ttl_ms),
                owner,
                external,
                started: Instant::now(),
                retry: None,
                failed: false,
                budget: RestartBudget::default(),
                renew_seq: 0,
            },
        );
        Ok(response)
    }
    fn validate<'a>(s: &'a State, principal: &str, token: &str, fence: u64) -> Result<&'a Lease> {
        if s.revoked.contains(principal) {
            return Err("permission-denied".into());
        }
        let l = s.leases.get(token).ok_or("stale-generation")?;
        if !token_eq(&l.principal, principal) {
            return Err("permission-denied".into());
        }
        if l.fence != fence || Instant::now() >= l.deadline {
            return Err("stale-generation".into());
        }
        if l.failed || l.retry.is_some() {
            return Err("context-not-active".into());
        }
        Ok(l)
    }
    /// Status of one lease without touching it (route attach fast-path:
    /// reuse a live lease instead of stacking `component-already-leased`
    /// activations on retry).
    pub fn lease_status_by_token(&self, principal: &str, token: &str) -> Result<Value> {
        let s = self.state.lock().unwrap();
        let l = s.leases.get(token).ok_or("stale-generation")?;
        if l.principal != principal {
            return Err("permission-denied".into());
        }
        if Instant::now() >= l.deadline || l.failed || l.retry.is_some() {
            return Err("stale-generation".into());
        }
        let active = self.kernel.context_state_of(&l.logical).as_deref() == Some("Active");
        let ready = active && (!l.external || self.host.has_session(&l.logical, l.owner.instance));
        Ok(
            json!({"ready":ready,"instance":l.owner.describe(),"remaining_ms":l.deadline.saturating_duration_since(Instant::now()).as_millis(),"fence":l.fence.to_string()}),
        )
    }

    pub fn lease_status(&self, principal: &str, token: &str, fence: u64) -> Result<Value> {
        let s = self.state.lock().unwrap();
        let l = Self::validate(&s, principal, token, fence)?;
        let active = self.kernel.context_state_of(&l.logical).as_deref() == Some("Active");
        let ready = active && (!l.external || self.host.has_session(&l.logical, l.owner.instance));
        Ok(
            json!({"ready":ready,"instance":l.owner.describe(),"remaining_ms":l.deadline.saturating_duration_since(Instant::now()).as_millis()}),
        )
    }
    /// Each renewal rotates the token. Delayed/replayed renewals cannot extend
    /// a subsequently issued lease; owner monotonic clock is authoritative.
    pub fn renew(&self, principal: &str, token: &str, fence: u64, ttl_ms: u64) -> Result<Value> {
        if !(100..=30000).contains(&ttl_ms) {
            return Err("invalid lease duration".into());
        }
        let mut s = self.state.lock().unwrap();
        Self::validate(&s, principal, token, fence)?;
        let new = random_token().map_err(|e| e.to_string())?;
        let mut l = s.leases.remove(token).unwrap();
        l.token = new.clone();
        l.deadline = Instant::now() + Duration::from_millis(ttl_ms);
        s.leases.insert(new.clone(), l);
        Ok(json!({"lease":new,"fence":fence.to_string(),"ttl_ms":ttl_ms}))
    }

    /// Session renew with client sequence (M7 `lease.renew`).
    /// Same rotation as [`Self::renew`], plus idempotency: a repeated
    /// `seq` replays the current token without rotating (lost-response
    /// recovery); an older `seq` is stale. A delayed renew never revives
    /// a withdrawn/expired lease (`validate` runs first, same as unary).
    /// The unary path never touches `renew_seq`, so mixed use stays safe:
    /// session retries compare only against session-issued sequences.
    pub fn renew_seq(
        &self,
        principal: &str,
        token: &str,
        fence: u64,
        seq: u64,
        ttl_ms: u64,
    ) -> Result<Value> {
        if !(100..=30000).contains(&ttl_ms) {
            return Err("invalid lease duration".into());
        }
        if seq == 0 {
            return Err("invalid-message".into());
        }
        let mut s = self.state.lock().unwrap();
        // A replayed seq answers from the live lease without revalidating
        // the deadline: the rotation already happened for this seq, and
        // answering with the current token cannot extend anything. A dead
        // lease replays nothing (falls through to the live check below).
        if let Some(l) = s.leases.get(token) {
            if l.principal == principal
                && l.fence == fence
                && l.renew_seq == seq
                && Instant::now() < l.deadline
                && !l.failed
                && l.retry.is_none()
            {
                let cur = l.token.clone();
                return Ok(json!({"lease":cur,"fence":fence.to_string(),"ttl_ms":ttl_ms,"replay":true}));
            }
        }
        Self::validate(&s, principal, token, fence)?;
        let l = s.leases.get(token).unwrap();
        if seq < l.renew_seq {
            return Err("stale-generation".into());
        }
        let new = random_token().map_err(|e| e.to_string())?;
        let mut l = s.leases.remove(token).unwrap();
        l.token = new.clone();
        l.deadline = Instant::now() + Duration::from_millis(ttl_ms);
        l.renew_seq = seq;
        s.leases.insert(new.clone(), l);
        Ok(json!({"lease":new,"fence":fence.to_string(),"ttl_ms":ttl_ms}))
    }

    /// Token-only lease lookup for M7 `call.open` (token uniquely
    /// identifies the lease; fence travels inside it server-side).
    pub fn lease_snapshot_by_token(
        &self,
        principal: &str,
        token: &str,
    ) -> Option<(String, matrix_core::InstanceRef, u64)> {
        let s = self.state.lock().unwrap();
        if s.revoked.contains(principal) {
            return None;
        }
        s.leases.get(token).and_then(|l| {
            if l.principal != principal {
                return None;
            }
            // Same health rule as `validate`, minus the fence parameter
            // (the token already binds it).
            if Instant::now() >= l.deadline || l.failed || l.retry.is_some() {
                return None;
            }
            Some((l.logical.clone(), l.owner.clone(), l.fence))
        })
    }

    /// Live (logical, fence) pairs for a principal (route watcher diff).
    pub fn live_provider_fences(&self, principal: &str) -> Vec<(String, u64)> {
        let s = self.state.lock().unwrap();
        let now = Instant::now();
        let mut out: Vec<(String, u64)> = vec![];
        for l in s.leases.values() {
            if l.principal == principal && now < l.deadline && !l.failed && l.retry.is_none() {
                if !out.iter().any(|(g, _)| g == &l.logical) {
                    out.push((l.logical.clone(), l.fence));
                }
            }
        }
        out.sort();
        out
    }
    pub fn invoke(
        &self,
        principal: &str,
        token: &str,
        fence: u64,
        operation: &str,
        cap: &str,
        input: &Value,
    ) -> Result<Value> {
        let (logical, owner) = {
            let s = self.state.lock().unwrap();
            let l = Self::validate(&s, principal, token, fence)?;
            if !s
                .grants
                .get(principal)
                .is_some_and(|g| g.capabilities.contains(cap))
            {
                return Err("permission-denied".into());
            }
            if !self
                .kernel
                .definitions
                .lock()
                .get(&l.logical)
                .is_some_and(|d| d.caps.iter().any(|c| c == cap))
            {
                return Err("permission-denied".into());
            }
            (l.logical.clone(), l.owner.clone())
        };
        let request = json!({"logical":logical,"cap":cap,"input":input});
        match self.store.admit(principal, operation, &request)? {
            Admission::Completed(v) => return Ok(v),
            Admission::Unknown => return Err("outcome-unknown".into()),
            Admission::New => {}
        }
        // Revocation may race after this check. Kernel tickets and post-check
        // fence results; external effects remain explicitly non-revertible.
        if self.kernel.instance_ref_of(&logical).as_ref() != Some(&owner) {
            return Err("stale-generation".into());
        }
        let (value, ok) = self.kernel.invoke_for_ref(&owner, cap, input);
        let current = {
            let s = self.state.lock().unwrap();
            !s.revoked.contains(principal)
                && s.leases.values().any(|l| {
                    l.principal == principal
                        && l.logical == logical
                        && l.fence == fence
                        && l.owner == owner
                        && Instant::now() < l.deadline
                        && !l.failed
                })
        };
        let code = value.get("code").and_then(Value::as_str).unwrap_or("");
        let unknown =
            !current || matches!(code, "outcome-unknown" | "deadline-exceeded" | "cancelled");
        let result = if !current {
            json!({"ok":false,"value":{"code":"outcome-unknown"},"durability":"durable"})
        } else {
            json!({"ok":ok,"value":value,"durability":"durable"})
        };
        self.store.finish(principal, operation, &result, unknown)?;
        Ok(result)
    }
    /// Route-visible snapshot of one lease: logical + owner reference.
    /// Same checks as `validate` (principal, fence, deadline, health).
    pub fn lease_snapshot(
        &self,
        principal: &str,
        token: &str,
        fence: u64,
    ) -> Option<(String, matrix_core::InstanceRef)> {
        let s = self.state.lock().unwrap();
        Self::validate(&s, principal, token, fence)
            .ok()
            .map(|l| (l.logical.clone(), l.owner.clone()))
    }

    /// Live leases currently held (operator diagnosis; tokens never leave).
    pub fn live_lease_count(&self) -> usize {
        self.state.lock().unwrap().leases.len()
    }

    /// Grant check factored for the route (same rule as `invoke`).
    pub fn grant_covers(&self, principal: &str, cap: &str) -> bool {
        let s = self.state.lock().unwrap();
        if s.revoked.contains(principal) {
            return false;
        }
        s.grants
            .get(principal)
            .is_some_and(|g| g.capabilities.contains(cap))
    }

    /// Liveness recheck before persisting a remote result (same rule as
    /// `invoke`'s post-check: unrevoked, lease live, owner unchanged).
    pub fn lease_live(
        &self,
        principal: &str,
        logical: &str,
        fence: u64,
        owner: &matrix_core::InstanceRef,
    ) -> bool {
        let s = self.state.lock().unwrap();
        !s.revoked.contains(principal)
            && s.leases.values().any(|l| {
                l.principal == principal
                    && l.logical == logical
                    && l.fence == fence
                    && l.owner == *owner
                    && Instant::now() < l.deadline
                    && !l.failed
            })
    }

    /// Kernel execution with a caller-computed budget. The kernel applies
    /// its own admission/deadline/cancel rules; this only bounds the wait
    /// here so a wedged provider cannot pin the route worker past the
    /// remaining budget (the operation stays queryable as unknown).
    pub fn invoke_with_budget(
        &self,
        owner: &matrix_core::InstanceRef,
        cap: &str,
        input: &Value,
        budget: Duration,
    ) -> (Value, bool) {
        // The kernel call itself is synchronous; run it on a worker and
        // bound the wait. A timeout here means unknown, never a replay:
        // the ledger entry stays `admitted` untilDrive a real finish lands.
        let kernel = self.kernel.clone();
        let owner = owner.clone();
        let cap = cap.to_string();
        let input = input.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("matrix-route-invoke".into())
            .spawn(move || {
                let _ = tx.send(kernel.invoke_for_ref(&owner, &cap, &input));
            })
            .ok();
        match rx.recv_timeout(budget.max(Duration::from_millis(1))) {
            Ok(out) => out,
            Err(_) => (json!({"error": "executor timeout", "code": "deadline-exceeded"}), false),
        }
    }

    /// Installs a remote-only provider definition (M7 route manager).
    /// Operator-only: records the executor-attested capability snapshot
    /// without materializing anything locally. Fails closed unless the
    /// definition is well-formed, capability-bearing and require-free.
    pub fn provision_remote(&self, body: &Value) -> Result<()> {
        let id = body.get("id").and_then(Value::as_str).ok_or("missing id")?;
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err("invalid component id".into());
        }
        let parsed = matrix_core::kernel::parse_manifest_value(body)?;
        if !parsed.remote {
            return Err("not a remote definition".into());
        }
        if parsed.caps.is_empty() {
            return Err("remote definition provides nothing".into());
        }
        if !parsed.requires.is_empty() {
            return Err("remote definitions provide only".into());
        }
        self.kernel.install_remote_definition(parsed);
        Ok(())
    }

    pub fn release(&self, principal: &str, token: &str, fence: u64) -> Result<Value> {
        let mut s = self.state.lock().unwrap();
        let l = s.leases.get(token).ok_or("stale-generation")?;
        if l.principal != principal {
            return Err("permission-denied".into());
        }
        if l.fence != fence {
            return Err("stale-generation".into());
        }
        let l = s.leases.remove(token).unwrap();
        // Keep the control lock through retirement so a new lease cannot race
        // an old logical-id removal. Kernel never calls into this lock.
        Ok(self.retire(l))
    }
    fn retire(&self, l: Lease) -> Value {
        let out = self.kernel.dispose_plugin(&l.logical);
        self.host.kill_child(&l.logical);
        let clean = out.as_str() != "CleanupPending"
            && !self.host.child_running(&l.logical)
            && self.kernel.active_resources_for(&l.logical) == 0;
        let audit = self.store.event(
            "lease.retired",
            &json!({"logical":l.logical,"fence":l.fence.to_string(),"cleanup_confirmed":clean}),
        );
        json!({"state":if clean {"Disposed"} else {"CleanupPending"},"kernel":out.as_str(),"audit_durable":audit.is_ok()})
    }
    pub fn commit_effect(
        &self,
        principal: &str,
        token: &str,
        fence: u64,
        operation: &str,
        key: &str,
        value: &Value,
    ) -> Result<Value> {
        let s = self.state.lock().unwrap();
        let l = Self::validate(&s, principal, token, fence)?;
        if !s
            .grants
            .get(principal)
            .is_some_and(|g| g.capabilities.contains("matrix.effect.write"))
        {
            return Err("permission-denied".into());
        }
        // Serialize with lease retirement and validate at the resource owner.
        self.store
            .commit_effect(principal, operation, &l.logical, fence, key, value)
    }
    /// Operator-only provisioning/rotation; never exposed to a remote peer.
    pub fn grant(&self, principal: String, grant: Grant) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        self.store.set_revoked(&principal, false)?;
        s.revoked.remove(&principal);
        s.grants.insert(principal, grant);
        Ok(())
    }
    pub fn revoke(&self, principal: &str) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        s.revoked.insert(principal.into());
        let durable = self.store.set_revoked(principal, true);
        let keys: Vec<_> = s
            .leases
            .iter()
            .filter(|(_, l)| l.principal == principal)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(l) = s.leases.remove(&k) {
                self.retire(l);
            }
        }
        durable
    }
    /// Syncs operator outbound grants (consumer → capabilities).
    /// Independent authority source from `outbound.request`; missing =
    /// denied. Revokes removed ones, installs new ones; kernel revisions rotate
    /// and invalidate superseded children. Operator only.
    pub fn sync_outbound_grants(&self, want: &HashMap<String, Vec<String>>) {
        let want_set: HashSet<(String, String)> = want
            .iter()
            .flat_map(|(c, caps)| caps.iter().map(move |cap| (c.clone(), cap.clone())))
            .collect();
        let mut s = self.state.lock().unwrap();
        for (consumer, cap) in s.outbound.difference(&want_set) {
            self.kernel.revoke_outbound(consumer, cap);
        }
        for (consumer, cap) in want_set.difference(&s.outbound) {
            self.kernel.grant_outbound(consumer, cap);
        }
        s.outbound = want_set;
    }
    fn tick(&self) {
        let mut s = self.state.lock().unwrap();
        let now = Instant::now();
        let expired: Vec<_> = s
            .leases
            .iter()
            .filter(|(_, l)| now >= l.deadline)
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired {
            if let Some(l) = s.leases.remove(&k) {
                self.retire(l);
            }
        }
        for l in s.leases.values_mut() {
            if !l.external || l.failed {
                continue;
            }
            if let Some(at) = l.retry {
                if now < at {
                    continue;
                }
                l.retry = None;
                let result = self
                    .materialize(&l.logical)
                    .and_then(|p| self.kernel.load_manifest(&p));
                if result.is_ok() {
                    if let Some(owner) = self.kernel.instance_ref_of(&l.logical) {
                        l.owner = owner;
                    }
                }
                l.started = now;
                continue;
            }
            if self.host.child_running(&l.logical)
                || now.duration_since(l.started) < Duration::from_millis(250)
            {
                continue;
            }
            self.kernel.dispose_plugin(&l.logical);
            if let Some(p) = self.restart.get(&l.logical) {
                l.retry = l.budget.reserve(now, p);
            }
            if l.retry.is_none() {
                l.failed = true;
            }
            let _ = self.store.event(
                "supervision.failure",
                &json!({"logical":l.logical,"failed":l.failed,"instance":l.owner.describe()}),
            );
        }
    }
    pub fn inspect(&self) -> Value {
        let s = self.state.lock().unwrap();
        json!({"epoch":self.store.epoch.to_string(),"kernel":self.kernel.inventory(),"leases":s.leases.values().map(|l|json!({"principal":l.principal,"logical":l.logical,"fence":l.fence.to_string(),"failed":l.failed,"restart_pending":l.retry.is_some(),"remaining_ms":l.deadline.saturating_duration_since(Instant::now()).as_millis()})).collect::<Vec<_>>()})
    }
    pub fn shutdown(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let mut s = self.state.lock().unwrap();
        for (_, l) in s.leases.drain() {
            self.retire(l);
        }
        self.host.shutdown();
    }

    /// Serves `matrix.remote/0.1` sessions (M7 route B): accept loop over
    /// mTLS, one [`crate::route_executor::Route`] per authorized peer.
    /// Binding is explicit; `Service::open` enables no listener. Shares
    /// the 32-connection cap Neuordnung with the unary profile.
    pub fn serve_remote_session(
        self: &Arc<Self>,
        address: std::net::SocketAddr,
        config: Arc<rustls::ServerConfig>,
    ) -> std::result::Result<crate::remote_session_server::RemoteSessionServer, String> {
        let server = crate::remote_session_server::RemoteSessionServer::bind(self.clone(), config, address)?;
        // Executor event tap: local emissions additionally reach routes
        // with matching subscriptions (best effort, bounded per route).
        let weak = Arc::downgrade(self);
        self.host.set_event_tap(Some(Arc::new(move |topic: &str, payload: &serde_json::Value| {
            let Some(svc) = weak.upgrade() else { return };
            let routes = svc.remote_routes.lock().unwrap().clone();
            for r in routes {
                if let Some(route) = r.upgrade() {
                    route.on_local_emit(topic, payload);
                }
            }
        })));
        // Executor stream tap: provider stream chunks additionally travel
        // over the remote session (best effort, bounded by its queues).
        // The controller delivers them to sessions holding the stream id
        // (consumer-initiated bidi legs); unknown ids drop there.
        let weak_stream = Arc::downgrade(self);
        self.host.set_stream_tap(Some(Arc::new(
            move |_sid: &str, stream_id: &str, seq: u64, payload: &str| {
                let Some(svc) = weak_stream.upgrade() else { return };
                let routes = svc.remote_routes.lock().unwrap().clone();
                for r in routes {
                    if let Some(route) = r.upgrade() {
                        route.send_stream_chunk(stream_id, seq, payload.as_bytes());
                    }
                }
            },
        )));
        Ok(server)
    }

    /// Executor-side revocation push for one provider logical (route
    /// manager/watchers call on local withdraw/revoke): best effort.
    pub fn notify_remote_revoked(&self, target: &str, fence: u64) {
        let routes = self.remote_routes.lock().unwrap().clone();
        for r in routes {
            if let Some(route) = r.upgrade() {
                route.notify_revoked(target, fence);
            }
        }
    }

    pub(crate) fn track_route(&self, route: &Arc<crate::route_executor::Route>) {
        let mut routes = self.remote_routes.lock().unwrap();
        routes.retain(|w| w.upgrade().is_some());
        routes.push(Arc::downgrade(route));
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        self.shutdown();
    }
}
