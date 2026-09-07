//! Contexts and instance identity (M1.1).
//!
//! An instance is one concrete activation of a logical id; a context is
//! that activation's authority/ownership scope (M1.1: 1:1).
//! Generation is monotonic per logical id; reusing the id never revalidates
//! old references (C01/I03).
//!
//! The context tree (who is discarded with whom) is separate from the
//! dependency graph (who may be active) — cf. ARCH. M1.1 builds
//! the tree (root = kernel); the reactive graph arrives in M1.2.

use crate::fsm::Fsm;
use crate::identity::{fresh_epoch, ContextId, InstanceId, InstanceRef};
use parking_lot::Mutex;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ContextRecord {
    pub context: ContextId,
    pub instance: InstanceId,
    pub logical: String,
    pub generation: u64,
    pub epoch: u64,
    pub state: Fsm,
    pub parent: Option<ContextId>,
    /// Cause recorded while CleanupPending/Failed (I12).
    pub cause: Option<String>,
}

impl ContextRecord {
    pub fn reference(&self) -> InstanceRef {
        InstanceRef::new(
            self.epoch,
            self.instance.0,
            self.context.0,
            &self.logical,
            self.generation,
        )
    }
}

#[derive(Debug)]
pub enum ContextError {
    UnknownInstance(u64),
    UnknownContext(u64),
    NotActive { logical: String, state: String },
    StaleGeneration { logical: String, expected: u64, got: u64 },
    AlreadyDisposed { logical: String },
}

/// Result of the atomic dispose claim (B1).
#[derive(Debug, Clone)]
pub enum DisposeClaim {
    /// This caller won: it performed the transition to `Quiescing`.
    Fresh(ContextRecord),
    /// Another dispose already linearized and is in flight.
    InFlight { logical: String },
}

impl std::fmt::Display for ContextError {    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContextError::UnknownInstance(i) => write!(f, "unknown instance inst-{}", i),
            ContextError::UnknownContext(c) => write!(f, "unknown context ctx-{}", c),
            ContextError::NotActive { logical, state } => {
                write!(f, "context-not-active: {} is {}", logical, state)
            }
            ContextError::StaleGeneration { logical, expected, got } => {
                write!(f, "stale-generation: {} expected #{} got #{}", logical, expected, got)
            }
            ContextError::AlreadyDisposed { logical } => {
                write!(f, "already disposed: {}", logical)
            }
        }
    }
}

impl std::error::Error for ContextError {}

struct Inner {
    epoch: u64,
    next_instance: u64,
    next_context: u64,
    by_instance: HashMap<InstanceId, ContextRecord>,
    by_context: HashMap<ContextId, InstanceId>,
    current_by_logical: HashMap<String, InstanceId>,
    generations: HashMap<String, u64>,
}

pub struct ContextTable {
    inner: Mutex<Inner>,
}

impl ContextTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                epoch: fresh_epoch(),
                next_instance: 1,
                next_context: 1,
                by_instance: HashMap::new(),
                by_context: HashMap::new(),
                current_by_logical: HashMap::new(),
                generations: HashMap::new(),
            }),
        }
    }

    pub fn epoch(&self) -> u64 {
        self.inner.lock().epoch
    }

    /// Creates a new activation for a logical id: monotonic generation +1,
    /// ids frescos nunca reutilizados. Estado inicial `Active`
    /// (M1.2: `create_in` allows `Waiting` for missing dependencies).
    pub fn create(&self, logical: &str) -> ContextRecord {
        self.create_in(logical, Fsm::Active, None)
    }

    pub fn create_in(&self, logical: &str, state: Fsm, cause: Option<String>) -> ContextRecord {
        let mut i = self.inner.lock();
        let gen = i.generations.get(logical).copied().unwrap_or(0) + 1;
        i.generations.insert(logical.to_string(), gen);
        let inst = InstanceId(i.next_instance);
        i.next_instance += 1;
        let ctx = ContextId(i.next_context);
        i.next_context += 1;
        let rec = ContextRecord {
            context: ctx,
            instance: inst,
            logical: logical.to_string(),
            generation: gen,
            epoch: i.epoch,
            state,
            parent: None,
            cause,
        };
        i.by_context.insert(ctx, inst);
        i.by_instance.insert(inst, rec.clone());
        i.current_by_logical.insert(logical.to_string(), inst);
        rec
    }

    /// New activation in `Waiting` with a visible reason (C04).
    pub fn create_waiting(&self, logical: &str, cause: String) -> ContextRecord {
        self.create_in(logical, Fsm::Waiting, Some(cause))
    }

    pub fn get_by_instance(&self, inst: InstanceId) -> Option<ContextRecord> {
        self.inner.lock().by_instance.get(&inst).cloned()
    }

    pub fn get_by_context(&self, ctx: ContextId) -> Option<ContextRecord> {
        let i = self.inner.lock();
        let inst = *i.by_context.get(&ctx)?;
        i.by_instance.get(&inst).cloned()
    }

    pub fn current(&self, logical: &str) -> Option<ContextRecord> {
        let i = self.inner.lock();
        let inst = *i.current_by_logical.get(logical)?;
        i.by_instance.get(&inst).cloned()
    }

    /// Logical → current-record map (for binding resolution without
    /// holding locks across calls).
    pub fn current_all(&self) -> HashMap<String, ContextRecord> {
        let i = self.inner.lock();
        i.current_by_logical
            .iter()
            .filter_map(|(k, inst)| i.by_instance.get(inst).cloned().map(|r| (k.clone(), r)))
            .collect()
    }

    pub fn generation_of(&self, logical: &str) -> u64 {
        self.inner.lock().generations.get(logical).copied().unwrap_or(0)
    }

    /// Requires an active context (I02). Returns a typed error to map into
    /// `context-not-active` / `stale-generation` no protocolo futuro.
    pub fn require_active(&self, inst: InstanceId) -> Result<ContextRecord, ContextError> {
        let i = self.inner.lock();
        let rec = i
            .by_instance
            .get(&inst)
            .ok_or(ContextError::UnknownInstance(inst.0))?;
        if rec.state.canonical() != Fsm::Active {
            return Err(ContextError::NotActive {
                logical: rec.logical.clone(),
                state: rec.state.as_str().to_string(),
            });
        }
        // If no longer the logical id's current generation, the reference
        // is stale even if the record still says Active (it should not,
        // but the check is cheap and makes I03 explicit).
        if let Some(cur) = i.current_by_logical.get(&rec.logical) {
            if *cur != inst {
                let cur_gen = i
                    .by_instance
                    .get(cur)
                    .map(|r| r.generation)
                    .unwrap_or(rec.generation);
                return Err(ContextError::StaleGeneration {
                    logical: rec.logical.clone(),
                    expected: cur_gen,
                    got: rec.generation,
                });
            }
        }
        Ok(rec.clone())
    }

    /// Withdraw linearization point: marks `Quiescing` and blocks
    /// new admissions.
    ///
    /// Atomic dispose claim (B1): exactly one caller observes
    /// `Fresh` and performs the transition; concurrent callers observe
    /// `InFlight` (another dispose already linearized and is in flight) or
    /// terminal state. Replaces check-then-mark (the TOCTOU that used to
    /// duplicava `Disposed` + `plugin.unloaded`).
    pub fn claim_dispose(&self, inst: InstanceId) -> Result<DisposeClaim, ContextError> {
        let mut i = self.inner.lock();
        let rec = i
            .by_instance
            .get_mut(&inst)
            .ok_or(ContextError::UnknownInstance(inst.0))?;
        match rec.state.canonical() {
            Fsm::Disposed => {
                return Err(ContextError::AlreadyDisposed {
                    logical: rec.logical.clone(),
                })
            }
            Fsm::Failed => {
                // Failed already went through cleanup; never reopens.
                return Err(ContextError::AlreadyDisposed {
                    logical: rec.logical.clone(),
                });
            }
            Fsm::Quiescing => {
                return Ok(DisposeClaim::InFlight {
                    logical: rec.logical.clone(),
                });
            }
            Fsm::CleanupPending => {
                // Parked by another dispose: it owns the outcome.
                return Ok(DisposeClaim::InFlight {
                    logical: rec.logical.clone(),
                });
            }
            _ => {}
        }
        rec.state = Fsm::Quiescing;
        rec.cause = None;
        Ok(DisposeClaim::Fresh(rec.clone()))
    }

    pub fn mark_disposed(&self, inst: InstanceId) {
        if let Some(rec) = self.inner.lock().by_instance.get_mut(&inst) {
            rec.state = Fsm::Disposed;
            rec.cause = None;
        }
    }

    pub fn mark_cleanup_pending(&self, inst: InstanceId, cause: String) {
        if let Some(rec) = self.inner.lock().by_instance.get_mut(&inst) {
            rec.state = Fsm::CleanupPending;
            rec.cause = Some(cause);
        }
    }

    pub fn mark_failed(&self, inst: InstanceId, cause: String) {
        if let Some(rec) = self.inner.lock().by_instance.get_mut(&inst) {
            rec.state = Fsm::Failed;
            rec.cause = Some(cause);
        }
    }

    /// Promotes `Waiting`/`Preparing` → `Active` (activation, LIFECYCLE §5).
    /// Returns `false` if the instance is no longer waiting (e.g. it was
    /// superseded by a new generation) — the caller must abort.
    pub fn mark_active(&self, inst: InstanceId) -> bool {
        let mut inner = self.inner.lock();
        let Some(rec) = inner.by_instance.get_mut(&inst) else { return false };
        match rec.state.canonical() {
            Fsm::Waiting | Fsm::Preparing | Fsm::Registered => {
                rec.state = Fsm::Active;
                rec.cause = None;
                true
            }
            Fsm::Active => true,
            _ => false,
        }
    }

    /// Updates the `Waiting` reason without swapping instances.
    /// Never moves `Active` → `Waiting` (withdraw uses dispose + new generation).
    pub fn mark_waiting(&self, inst: InstanceId, cause: String) -> bool {
        let mut inner = self.inner.lock();
        let Some(rec) = inner.by_instance.get_mut(&inst) else { return false };
        match rec.state.canonical() {
            Fsm::Waiting | Fsm::Preparing | Fsm::Registered => {
                rec.state = Fsm::Waiting;
                rec.cause = Some(cause);
                true
            }
            _ => false,
        }
    }

    pub fn set_state(&self, inst: InstanceId, state: Fsm) {
        if let Some(rec) = self.inner.lock().by_instance.get_mut(&inst) {
            rec.state = state;
        }
    }

    /// Inventory for inspection (partial C19 in M1.1).
    pub fn snapshot(&self) -> Vec<ContextRecord> {
        let mut v: Vec<ContextRecord> =
            self.inner.lock().by_instance.values().cloned().collect();
        v.sort_by_key(|r| (r.logical.clone(), r.generation));
        v
    }
}

impl Default for ContextTable {
    fn default() -> Self {
        Self::new()
    }
}
