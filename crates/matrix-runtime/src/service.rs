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
}
struct State {
    leases: HashMap<String, Lease>,
    grants: HashMap<String, Grant>,
    revoked: HashSet<String>,
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
            }),
            restart,
            stopped: AtomicBool::new(false),
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
}
impl Drop for Service {
    fn drop(&mut self) {
        self.shutdown();
    }
}
