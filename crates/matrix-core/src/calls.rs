//! Admission tickets, drain/cancel, and mediated commits (M1.3).
//!
//! Covers C06, C08–C09 and I02/I03/I07 in the trusted in-process profile:
//! - `call_open` issues a ticket bound to instance, generation, context,
//!   epoch, and authorization (resolved capability + valid bindings).
//! - Withdraw's linearization point (`Quiescing`) revokes admission;
//!   `call_open` revalidates after registering the ticket, closing the
//!   admission × dispose race: either admission observes `Quiescing` and is rejected,
//!   or disposal observes the ticket and drains/cancels it.
//! - Explicit per-operation policy: `Drain` (finish within budget) or
//!   `Cancel` (revoke immediately). Bounded deadlines; waits hold no
//!   kernel locks, via condvar.
//! - `commit_effect` is the boundary that applies the effect: revalidates
//!   ticket, generation, and state; late attempts are rejected AND journaled
//!   (`call.rejected`) — dropping the response is not enough.
//! - Still-active work pins `CleanupPending`; resources are not released
//!   early. `call_close` finalizes (`maybe_finalize` in the kernel).
//!
//! Finite bounds (I09): `MAX_INFLIGHT_CALLS` per kernel, `MAX_EFFECTS` in the
//! ledger (ring evicting oldest; the journal persists everything).

use crate::identity::{ContextId, InstanceId, ResourceHandle};
use parking_lot::{Condvar, Mutex};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Maximum in-flight calls per kernel (I09).
pub const MAX_INFLIGHT_CALLS: usize = 256;
/// In-memory confirmed-effect ring size (I09).
pub const MAX_EFFECTS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WithdrawPolicy {
    Drain,
    Cancel,
}

impl WithdrawPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            WithdrawPolicy::Drain => "drain",
            WithdrawPolicy::Cancel => "cancel",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "drain" => Some(WithdrawPolicy::Drain),
            "cancel" => Some(WithdrawPolicy::Cancel),
            _ => None,
        }
    }
}

/// Explicit per-operation policy (manifest `calls`, overridable at open).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallPolicy {
    pub on_withdraw: WithdrawPolicy,
    pub drain_ms: u64,
}

impl Default for CallPolicy {
    fn default() -> Self {
        Self { on_withdraw: WithdrawPolicy::Cancel, drain_ms: 0 }
    }
}

impl CallPolicy {
    pub fn cancel() -> Self {
        Self::default()
    }

    pub fn drain(drain_ms: u64) -> Self {
        Self { on_withdraw: WithdrawPolicy::Drain, drain_ms }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TicketId(pub u64);

impl std::fmt::Display for TicketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tkt-{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TicketState {
    Admitted,
    Committed,
    Cancelled,
    Expired,
}

impl TicketState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TicketState::Admitted => "Admitted",
            TicketState::Committed => "Committed",
            TicketState::Cancelled => "Cancelled",
            TicketState::Expired => "Expired",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TicketRecord {
    pub id: TicketId,
    pub cap: String,
    pub logical: String,
    pub instance: InstanceId,
    pub generation: u64,
    pub context: ContextId,
    pub epoch: u64,
    pub policy: CallPolicy,
    pub state: TicketState,
    /// Cooperative cancellation signal observed by the worker.
    pub cancel: Arc<AtomicBool>,
    /// Handles declared as used by the call (anti-early-release).
    pub pins: Vec<ResourceHandle>,
    pub opened_at: Instant,
    /// Drain deadline fixed at revocation (`Drain` only).
    pub drain_until: Option<Instant>,
    pub cancel_reason: Option<String>,
    /// Dependency child (M6.1 step 2); `None` = root call.
    pub dep: Option<DepChild>,
}

/// Dependency-call parent/child link (M6.1 step 2).
///
/// Child authority derives from: an `Admitted` parent whose executor is the
/// caller, a current opaque binding, the operator grant at the captured
/// revision, and a deadline. Revoking any link cancels the child (without
/// assuming execution ended: late commit stays rejected).
#[derive(Debug, Clone)]
pub struct DepChild {
    pub parent: TicketId,
    pub binding: String,
    pub consumer: InstanceId,
    pub session: String,
    pub grant_consumer: String,
    pub grant_cap: String,
    pub grant_rev: u64,
    pub depth: u32,
    /// Executor peer for M7 remote legs; `None` for local children.
    pub remote_peer: Option<String>,
}

impl TicketRecord {
    pub fn is_open(&self) -> bool {
        self.state == TicketState::Admitted
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.opened_at.elapsed().as_millis()
    }

    pub fn drain_left_ms(&self) -> Option<u128> {
        self.drain_until.map(|u| {
            let now = Instant::now();
            if u > now {
                (u - now).as_millis()
            } else {
                0
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct CommittedEffect {
    pub seq: u64,
    pub ticket: TicketId,
    pub logical: String,
    pub instance: InstanceId,
    pub generation: u64,
    pub kind: String,
    pub payload: Value,
    pub at_ms: u64,
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct CallsInner {
    next_ticket: u64,
    next_effect: u64,
    tickets: HashMap<TicketId, TicketRecord>,
    effects: VecDeque<CommittedEffect>,
    /// Finalizations in progress (avoids noisy double release).
    cleanup_claims: HashSet<InstanceId>,
}

impl CallsInner {
    fn new() -> Self {
        Self {
            next_ticket: 1,
            next_effect: 1,
            tickets: HashMap::new(),
            effects: VecDeque::new(),
            cleanup_claims: HashSet::new(),
        }
    }
}

pub struct CallsTable {
    inner: Mutex<CallsInner>,
    cv: Condvar,
}

impl Default for CallsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl CallsTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CallsInner::new()),
            cv: Condvar::new(),
        }
    }

    /// Allocates a kernel-validated ticket. Returns the issued id.
    pub fn alloc(
        &self,
        cap: &str,
        logical: &str,
        instance: InstanceId,
        generation: u64,
        context: ContextId,
        epoch: u64,
        policy: CallPolicy,
        pins: Vec<ResourceHandle>,
        cancel: Arc<AtomicBool>,
    ) -> TicketId {
        let mut i = self.inner.lock();
        let id = TicketId(i.next_ticket);
        i.next_ticket += 1;
        i.tickets.insert(
            id,
            TicketRecord {
                id,
                cap: cap.to_string(),
                logical: logical.to_string(),
                instance,
                generation,
                context,
                epoch,
                policy,
                state: TicketState::Admitted,
                cancel,
                pins,
                opened_at: Instant::now(),
                drain_until: None,
                cancel_reason: None,
                dep: None,
            },
        );
        id
    }

    /// Attaches child metadata to an admitted ticket (coordinated admission).
    pub fn set_dep_meta(&self, id: TicketId, meta: DepChild) -> bool {
        let mut i = self.inner.lock();
        let Some(t) = i.tickets.get_mut(&id) else { return false };
        t.dep = Some(meta);
        self.cv.notify_all();
        true
    }

    /// Still-open children of a parent (for quotas and revocation).
    pub fn open_children_of(&self, parent: TicketId) -> Vec<TicketRecord> {
        self.inner
            .lock()
            .tickets
            .values()
            .filter(|t| t.dep.as_ref().is_some_and(|d| d.parent == parent) && t.state == TicketState::Admitted)
            .cloned()
            .collect()
    }

    /// Revokes open descendants: marks `Cancelled` + signals, without
    /// assuming execution ended (late commit stays rejected).
    /// Returns the marked ones, for the caller to journal. Idempotent.
    pub fn revoke_descendants(&self, parent: TicketId, reason: &str) -> Vec<TicketRecord> {
        let mut i = self.inner.lock();
        let mut out = vec![];
        for t in i.tickets.values_mut() {
            let mine = t.dep.as_ref().is_some_and(|d| d.parent == parent);
            if mine && t.state == TicketState::Admitted {
                t.state = TicketState::Cancelled;
                t.cancel.store(true, Ordering::SeqCst);
                t.cancel_reason = Some(reason.to_string());
                out.push(t.clone());
            }
        }
        if !out.is_empty() {
            self.cv.notify_all();
        }
        out
    }

    /// Revokes open children of a consumer (consumer withdraw).
    pub fn revoke_children_of_consumer(&self, consumer: InstanceId, reason: &str) -> Vec<TicketRecord> {
        let mut i = self.inner.lock();
        let mut out = vec![];
        for t in i.tickets.values_mut() {
            let mine = t.dep.as_ref().is_some_and(|d| d.consumer == consumer);
            if mine && t.state == TicketState::Admitted {
                t.state = TicketState::Cancelled;
                t.cancel.store(true, Ordering::SeqCst);
                t.cancel_reason = Some(reason.to_string());
                out.push(t.clone());
            }
        }
        if !out.is_empty() {
            self.cv.notify_all();
        }
        out
    }

    /// Revokes open children under a grant. `keep_rev`: only preserves the
    /// indicated revision (`None` = revokes all, i.e. removed grant).
    pub fn revoke_grant_children(
        &self,
        consumer: &str,
        cap: &str,
        keep_rev: Option<u64>,
        reason: &str,
    ) -> Vec<TicketRecord> {
        let mut i = self.inner.lock();
        let mut out = vec![];
        for t in i.tickets.values_mut() {
            let mine = t.dep.as_ref().is_some_and(|d| {
                d.grant_consumer == consumer && d.grant_cap == cap && Some(d.grant_rev) != keep_rev
            });
            if mine && t.state == TicketState::Admitted {
                t.state = TicketState::Cancelled;
                t.cancel.store(true, Ordering::SeqCst);
                t.cancel_reason = Some(reason.to_string());
                out.push(t.clone());
            }
        }
        if !out.is_empty() {
            self.cv.notify_all();
        }
        out
    }

    pub fn get(&self, id: TicketId) -> Option<TicketRecord> {
        self.inner.lock().tickets.get(&id).cloned()
    }

    /// Marks `Committed` only if still `Admitted` (single terminal
    /// accept; M6.1 step 3). Returns whether it marked.
    pub fn mark_committed_if_admitted(&self, id: TicketId) -> bool {
        let mut i = self.inner.lock();
        match i.tickets.get_mut(&id) {
            Some(t) if t.state == TicketState::Admitted => {
                t.state = TicketState::Committed;
                self.cv.notify_all();
                true
            }
            _ => false,
        }
    }

    /// Expires an admitted ticket (deadline observed by the dispatcher).
    /// Returns whether it marked. Descendants are left for the caller to revoke.
    pub fn expire_ticket(&self, id: TicketId) -> bool {
        let mut i = self.inner.lock();
        match i.tickets.get_mut(&id) {
            Some(t) if t.state == TicketState::Admitted => {
                t.state = TicketState::Expired;
                t.cancel.store(true, Ordering::SeqCst);
                t.cancel_reason = Some("deadline-exceeded".to_string());
                self.cv.notify_all();
                true
            }
            _ => false,
        }
    }

    pub fn set_state(&self, id: TicketId, state: TicketState) -> bool {
        let mut i = self.inner.lock();
        let Some(t) = i.tickets.get_mut(&id) else { return false };
        t.state = state;
        self.cv.notify_all();
        true
    }

    pub fn set_drain_until(&self, id: TicketId, until: Instant) -> bool {
        let mut i = self.inner.lock();
        let Some(t) = i.tickets.get_mut(&id) else { return false };
        if t.state == TicketState::Admitted {
            t.drain_until = Some(until);
        }
        self.cv.notify_all();
        true
    }

    /// Revokes by cancellation: marks + signals. Only affects `Admitted`.
    pub fn cancel(&self, id: TicketId, reason: &str) -> bool {
        let mut i = self.inner.lock();
        let Some(t) = i.tickets.get_mut(&id) else { return false };
        if t.state != TicketState::Admitted {
            return false;
        }
        t.state = TicketState::Cancelled;
        t.cancel.store(true, Ordering::SeqCst);
        t.cancel_reason = Some(reason.to_string());
        self.cv.notify_all();
        true
    }

    /// Expires overdue drains. Returns the newly expired ids.
    pub fn expire_overdue(&self, now: Instant) -> Vec<TicketId> {
        let mut i = self.inner.lock();
        let mut out = vec![];
        for t in i.tickets.values_mut() {
            if t.state == TicketState::Admitted {
                if let Some(until) = t.drain_until {
                    if now >= until {
                        t.state = TicketState::Expired;
                        t.cancel.store(true, Ordering::SeqCst);
                        t.cancel_reason = Some("drain-exceeded".to_string());
                        out.push(t.id);
                    }
                }
            }
        }
        if !out.is_empty() {
            self.cv.notify_all();
        }
        out
    }

    pub fn remove(&self, id: TicketId) -> Option<TicketRecord> {
        let mut i = self.inner.lock();
        let r = i.tickets.remove(&id);
        if r.is_some() {
            self.cv.notify_all();
        }
        r
    }

    pub fn pending_for(&self, inst: InstanceId) -> Vec<TicketRecord> {
        let mut v: Vec<TicketRecord> = self
            .inner
            .lock()
            .tickets
            .values()
            .filter(|t| t.instance == inst)
            .cloned()
            .collect();
        v.sort_by_key(|t| t.id.0);
        v
    }

    pub fn pending_count(&self, inst: InstanceId) -> usize {
        self.inner.lock().tickets.values().filter(|t| t.instance == inst).count()
    }

    pub fn inflight_count(&self) -> usize {
        self.inner.lock().tickets.len()
    }

    pub fn snapshot(&self) -> Vec<TicketRecord> {
        let mut v: Vec<TicketRecord> = self.inner.lock().tickets.values().cloned().collect();
        v.sort_by_key(|t| t.id.0);
        v
    }

    /// Waits (holding no caller locks) until the instance has no pending
    /// work or `until` arrives. Returns `true` if settled.
    pub fn wait_settled(&self, inst: InstanceId, until: Instant) -> bool {
        let mut g = self.inner.lock();
        loop {
            if !g.tickets.values().any(|t| t.instance == inst) {
                return true;
            }
            let now = Instant::now();
            if now >= until {
                return false;
            }
            self.cv.wait_for(&mut g, until - now);
        }
    }

    pub fn notify(&self) {
        self.cv.notify_all();
    }

    /// Records a confirmed effect (bounded ring; journal persists all).
    pub fn push_effect(
        &self,
        ticket: TicketId,
        logical: &str,
        instance: InstanceId,
        generation: u64,
        kind: &str,
        payload: Value,
    ) -> CommittedEffect {
        let mut i = self.inner.lock();
        let seq = i.next_effect;
        i.next_effect += 1;
        let e = CommittedEffect {
            seq,
            ticket,
            logical: logical.to_string(),
            instance,
            generation,
            kind: kind.to_string(),
            payload,
            at_ms: wall_ms(),
        };
        i.effects.push_back(e.clone());
        while i.effects.len() > MAX_EFFECTS {
            i.effects.pop_front();
        }
        e
    }

    pub fn effects_for(&self, logical: Option<&str>) -> Vec<CommittedEffect> {
        self
            .inner
            .lock()
            .effects
            .iter()
            .filter(|e| logical.map(|l| e.logical == l).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn effects_len(&self) -> usize {
        self.inner.lock().effects.len()
    }

    pub fn try_claim_cleanup(&self, inst: InstanceId) -> bool {
        self.inner.lock().cleanup_claims.insert(inst)
    }

    pub fn release_cleanup_claim(&self, inst: InstanceId) {
        self.inner.lock().cleanup_claims.remove(&inst);
    }
}
