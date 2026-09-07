//! Resource table with ownership and real cleanup (M1.1).
//!
//! Covers C02/C03 and I01/I05/I06/I07 in the trusted in-process profile:
//! - Every resource has an owner (`ContextId` + `InstanceId` + generation).
//! - Ownership is recorded before the effect is published (I05).
//! - Repeated disposal is idempotent; it never removes another
//!   instance's resource (I06).
//! - `Disposed` only after zero pending items; failure becomes `CleanupPending` (I07).
//!
//! Real resources at this stage: `Cap` (registration), `Sub` (subscription),
//! `Timer` (cancellable thread with observable counter) and `Task`
//! (cancellable thread with heartbeat). `FailAcquire`/`FailRelease`
//! exist only to exercise partial failure and pending cleanup
//! without faking external resources.
//!
//! Lock ordering (to avoid deadlock, cf. ARCH): never hold two
//! kernel locks at the same time. Methods here only touch the table's
//! internal lock; publication into `Registry`/`Bus` and joins happen outside
//! the lock, from the `ReleaseClaim` returned by `begin_release`.

use crate::identity::{ContextId, InstanceId, ResourceHandle};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceKind {
    Cap { name: String },
    Sub { topic: String },
    Timer { label: String, interval_ms: u64 },
    Task { label: String },
    /// Deterministic acquisition failure (for C03 without mocks).
    FailAcquire { label: String },
    /// Resource that refuses release (for I07 without faking external I/O).
    FailRelease { label: String },
}

impl ResourceKind {
    pub fn label(&self) -> &str {
        match self {
            ResourceKind::Cap { name } => name,
            ResourceKind::Sub { topic } => topic,
            ResourceKind::Timer { label, .. } => label,
            ResourceKind::Task { label } => label,
            ResourceKind::FailAcquire { label } => label,
            ResourceKind::FailRelease { label } => label,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            ResourceKind::Cap { .. } => "cap",
            ResourceKind::Sub { .. } => "sub",
            ResourceKind::Timer { .. } => "timer",
            ResourceKind::Task { .. } => "task",
            ResourceKind::FailAcquire { .. } => "fail-acquire",
            ResourceKind::FailRelease { .. } => "fail-release",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceState {
    Active,
    Released,
}

#[derive(Debug, Clone)]
pub struct ResourceRecord {
    pub handle: ResourceHandle,
    pub kind: ResourceKind,
    pub owner_context: ContextId,
    pub owner_instance: InstanceId,
    pub owner_logical: String,
    pub generation: u64,
    pub state: ResourceState,
    /// Global acquisition order (for reverse cleanup).
    pub seq: u64,
}

#[derive(Debug)]
pub enum AcquireError {
    InvalidName(String),
    FailInjected(String),
}

impl std::fmt::Display for AcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcquireError::InvalidName(s) => write!(f, "invalid resource name: {}", s),
            AcquireError::FailInjected(s) => write!(f, "injected acquire failure: {}", s),
        }
    }
}

impl std::error::Error for AcquireError {}

#[derive(Debug)]
pub enum ReleaseError {
    UnknownHandle(u64),
    AlreadyReleased(u64),
    CleanupFailed { handle: u64, reason: String },
}

impl std::fmt::Display for ReleaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReleaseError::UnknownHandle(h) => write!(f, "unknown resource handle res-{}", h),
            ReleaseError::AlreadyReleased(h) => write!(f, "resource res-{} already released", h),
            ReleaseError::CleanupFailed { handle, reason } => {
                write!(f, "cleanup failed for res-{}: {}", handle, reason)
            }
        }
    }
}

impl std::error::Error for ReleaseError {}

/// Live control for a real timer.
struct TimerLive {
    cancel: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    fires: Arc<AtomicUsize>,
    join: Option<JoinHandle<()>>,
}

/// Live control for a real task.
struct TaskLive {
    cancel: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    beats: Arc<AtomicUsize>,
    join: Option<JoinHandle<()>>,
}

/// What `begin_release` hands over for completion outside the lock.
pub struct ReleaseClaim {
    pub handle: ResourceHandle,
    pub kind: ResourceKind,
    pub owner_context: ContextId,
    pub timer: Option<(Arc<AtomicBool>, Arc<AtomicBool>, Option<JoinHandle<()>>)>,
    pub task: Option<(Arc<AtomicBool>, Arc<AtomicBool>, Option<JoinHandle<()>>)>,
}

struct Inner {
    next_handle: u64,
    next_seq: u64,
    records: HashMap<ResourceHandle, ResourceRecord>,
    by_owner: HashMap<ContextId, Vec<ResourceHandle>>,
    timers: HashMap<ResourceHandle, TimerLive>,
    tasks: HashMap<ResourceHandle, TaskLive>,
    /// Diagnostics: how many real cleanups ran (no double counting).
    total_releases: u64,
    double_release_attempts: u64,
}

impl Inner {
    fn new() -> Self {
        Self {
            next_handle: 1,
            next_seq: 1,
            records: HashMap::new(),
            by_owner: HashMap::new(),
            timers: HashMap::new(),
            tasks: HashMap::new(),
            total_releases: 0,
            double_release_attempts: 0,
        }
    }
}

pub struct ResourceTable {
    inner: Mutex<Inner>,
}

impl Default for ResourceTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
        }
    }

    fn validate_kind(kind: &ResourceKind) -> Result<(), AcquireError> {
        match kind {
            ResourceKind::Cap { name } | ResourceKind::Sub { topic: name } => {
                if name.trim().is_empty() {
                    return Err(AcquireError::InvalidName(name.clone()));
                }
                Ok(())
            }
            ResourceKind::Timer { label, interval_ms } => {
                if label.trim().is_empty() {
                    return Err(AcquireError::InvalidName(label.clone()));
                }
                if *interval_ms == 0 || *interval_ms > 60_000 {
                    return Err(AcquireError::InvalidName(format!(
                        "interval_ms out of range: {}",
                        interval_ms
                    )));
                }
                Ok(())
            }
            ResourceKind::Task { label } => {
                if label.trim().is_empty() {
                    return Err(AcquireError::InvalidName(label.clone()));
                }
                Ok(())
            }
            ResourceKind::FailAcquire { label } => Err(AcquireError::FailInjected(label.clone())),
            ResourceKind::FailRelease { label } => {
                if label.trim().is_empty() {
                    return Err(AcquireError::InvalidName(label.clone()));
                }
                Ok(())
            }
        }
    }

    /// Records ownership BEFORE publishing the effect (I05).
    /// For `Timer`/`Task`, the thread is really created here;
    /// if spawning fails, the record is rolled back and the error propagates.
    pub fn register(
        &self,
        owner_context: ContextId,
        owner_instance: InstanceId,
        owner_logical: &str,
        generation: u64,
        kind: ResourceKind,
    ) -> Result<ResourceHandle, AcquireError> {
        Self::validate_kind(&kind)?;

        let (handle, seq) = {
            let mut i = self.inner.lock();
            let h = ResourceHandle(i.next_handle);
            i.next_handle += 1;
            let s = i.next_seq;
            i.next_seq += 1;
            let rec = ResourceRecord {
                handle: h,
                kind: kind.clone(),
                owner_context,
                owner_instance,
                owner_logical: owner_logical.to_string(),
                generation,
                state: ResourceState::Active,
                seq: s,
            };
            i.records.insert(h, rec);
            i.by_owner.entry(owner_context).or_default().push(h);
            (h, s)
        };
        let _ = seq;

        // Real effect publication for Timer/Task (thread).
        // Cap/Sub are published by the Kernel into Registry/Bus after this
        // registration, preserving I05 without nesting locks.
        match &kind {
            ResourceKind::Timer { interval_ms, .. } => {
                let cancel = Arc::new(AtomicBool::new(false));
                let done = Arc::new(AtomicBool::new(false));
                let fires = Arc::new(AtomicUsize::new(0));
                let c2 = cancel.clone();
                let d2 = done.clone();
                let f2 = fires.clone();
                let interval = Duration::from_millis(*interval_ms);
                // Real thread: sleeps in 1ms slices for fast
                // cooperative cancellation; each full interval counts one fire.
                let join = std::thread::Builder::new()
                    .name(format!("matrix-timer-{}", handle.0))
                    .spawn(move || {
                        let mut acc = Duration::from_millis(0);
                        let slice = Duration::from_millis(1);
                        loop {
                            if c2.load(Ordering::SeqCst) {
                                break;
                            }
                            std::thread::sleep(slice);
                            if c2.load(Ordering::SeqCst) {
                                break;
                            }
                            acc += slice;
                            if acc >= interval {
                                acc = Duration::from_millis(0);
                                f2.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        d2.store(true, Ordering::SeqCst);
                    });
                match join {
                    Ok(j) => {
                        self.inner.lock().timers.insert(
                            handle,
                            TimerLive {
                                cancel,
                                done,
                                fires,
                                join: Some(j),
                            },
                        );
                    }
                    Err(e) => {
                        // Rollback: removes the record (clean partial failure, I05).
                        let mut i = self.inner.lock();
                        i.records.remove(&handle);
                        if let Some(v) = i.by_owner.get_mut(&owner_context) {
                            v.retain(|h| *h != handle);
                        }
                        return Err(AcquireError::FailInjected(format!(
                            "timer spawn failed: {}",
                            e
                        )));
                    }
                }
            }
            ResourceKind::Task { .. } => {
                let cancel = Arc::new(AtomicBool::new(false));
                let done = Arc::new(AtomicBool::new(false));
                let beats = Arc::new(AtomicUsize::new(0));
                let c2 = cancel.clone();
                let d2 = done.clone();
                let b2 = beats.clone();
                let join = std::thread::Builder::new()
                    .name(format!("matrix-task-{}", handle.0))
                    .spawn(move || {
                        // Cooperative work: heartbeat until cancellation.
                        // Represents a managed task; M1.3 will add
                        // blocking at a controlled point + join budget.
                        loop {
                            if c2.load(Ordering::SeqCst) {
                                break;
                            }
                            b2.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        d2.store(true, Ordering::SeqCst);
                    });
                match join {
                    Ok(j) => {
                        self.inner.lock().tasks.insert(
                            handle,
                            TaskLive {
                                cancel,
                                done,
                                beats,
                                join: Some(j),
                            },
                        );
                    }
                    Err(e) => {
                        let mut i = self.inner.lock();
                        i.records.remove(&handle);
                        if let Some(v) = i.by_owner.get_mut(&owner_context) {
                            v.retain(|h| *h != handle);
                        }
                        return Err(AcquireError::FailInjected(format!(
                            "task spawn failed: {}",
                            e
                        )));
                    }
                }
            }
            _ => {}
        }
        Ok(handle)
    }

    /// Claims the release: marks `Released` under lock and extracts the
    /// live handle for completion outside the lock (no double release).
    /// Idempotent in the sense of never releasing twice: the second
    /// call returns `AlreadyReleased` without re-running cleanup.
    pub fn begin_release(&self, handle: ResourceHandle) -> Result<ReleaseClaim, ReleaseError> {
        // Upfront read without holding a mutable borrow (avoids E0499).
        let (state, kind_snapshot) = {
            let i = self.inner.lock();
            let rec = i
                .records
                .get(&handle)
                .ok_or(ReleaseError::UnknownHandle(handle.0))?;
            (rec.state, rec.kind.clone())
        };
        if state == ResourceState::Released {
            self.inner.lock().double_release_attempts += 1;
            return Err(ReleaseError::AlreadyReleased(handle.0));
        }
        // Resource simulating a cleanup failure: stays Active to
        // force CleanupPending on the owner (I07). Not counted as released.
        if matches!(kind_snapshot, ResourceKind::FailRelease { .. }) {
            let label = kind_snapshot.label().to_string();
            return Err(ReleaseError::CleanupFailed {
                handle: handle.0,
                reason: format!("resource '{}' refused release", label),
            });
        }
        let mut i = self.inner.lock();
        // Revalidates under lock (race between check and mark).
        let already = i
            .records
            .get(&handle)
            .map(|r| r.state == ResourceState::Released)
            .unwrap_or(false);
        if already {
            i.double_release_attempts += 1;
            return Err(ReleaseError::AlreadyReleased(handle.0));
        }
        let (kind, owner_context) = {
            let rec = i
                .records
                .get_mut(&handle)
                .ok_or(ReleaseError::UnknownHandle(handle.0))?;
            rec.state = ResourceState::Released;
            (rec.kind.clone(), rec.owner_context)
        };
        i.total_releases += 1;
        let timer = i.timers.remove(&handle).map(|t| (t.cancel, t.done, t.join));
        let task = i.tasks.remove(&handle).map(|t| (t.cancel, t.done, t.join));
        Ok(ReleaseClaim {
            handle,
            kind,
            owner_context,
            timer,
            task,
        })
    }

    /// Finishes cleanup outside the lock: signals cancellation and joins.
    /// Cooperative Timer/Task finish within a few ms; blocking join here
    /// never happens while holding the metadata lock (cf. ARCH).
    pub fn finish_release(claim: ReleaseClaim) {
        if let Some((cancel, _done, join)) = claim.timer {
            cancel.store(true, Ordering::SeqCst);
            if let Some(j) = join {
                let _ = j.join();
            }
        }
        if let Some((cancel, _done, join)) = claim.task {
            cancel.store(true, Ordering::SeqCst);
            if let Some(j) = join {
                let _ = j.join();
            }
        }
        // Cap/Sub/FailRelease have no thread; release is the map marking
        // plus (for Cap/Sub) the revocation done by the Kernel.
    }

    /// Full release in one step (claim + finish).
    /// `AlreadyReleased` is an error with no side effects (no double release).
    pub fn release(&self, handle: ResourceHandle) -> Result<(), ReleaseError> {
        let claim = self.begin_release(handle)?;
        Self::finish_release(claim);
        Ok(())
    }

    /// Undoes a record that has not been published yet (I05 rollback).
    /// Only removes while still `Active`; never touches `Released`.
    pub fn rollback_register(&self, handle: ResourceHandle) {
        let mut i = self.inner.lock();
        let should_remove = i
            .records
            .get(&handle)
            .map(|r| r.state == ResourceState::Active)
            .unwrap_or(false);
        if !should_remove {
            return;
        }
        let rec = i.records.remove(&handle);
        if let Some(r) = rec {
            if let Some(v) = i.by_owner.get_mut(&r.owner_context) {
                v.retain(|h| *h != handle);
            }
        }
        // If a thread existed (Timer/Task registered but publish failed in the
        // Kernel), cancel and join outside the lock.
        let timer = i.timers.remove(&handle);
        let task = i.tasks.remove(&handle);
        drop(i);
        if let Some(t) = timer {
            t.cancel.store(true, Ordering::SeqCst);
            if let Some(j) = t.join {
                let _ = j.join();
            }
        }
        if let Some(t) = task {
            t.cancel.store(true, Ordering::SeqCst);
            if let Some(j) = t.join {
                let _ = j.join();
            }
        }
    }

    /// Snapshot of one owner's handles in acquisition order.
    pub fn handles_for(&self, owner: ContextId) -> Vec<ResourceHandle> {
        self.inner
            .lock()
            .by_owner
            .get(&owner)
            .cloned()
            .unwrap_or_default()
    }

    pub fn record(&self, handle: ResourceHandle) -> Option<ResourceRecord> {
        self.inner.lock().records.get(&handle).cloned()
    }

    /// Validates handle use: exists, is active, and belongs to the expected
    /// activation (C01: reusing a logical id never revalidates an old reference).
    pub fn validate(
        &self,
        handle: ResourceHandle,
        expect_instance: InstanceId,
        expect_generation: u64,
    ) -> Result<ResourceRecord, ReleaseError> {
        let i = self.inner.lock();
        let rec = i
            .records
            .get(&handle)
            .ok_or(ReleaseError::UnknownHandle(handle.0))?;
        if rec.state != ResourceState::Active {
            return Err(ReleaseError::AlreadyReleased(handle.0));
        }
        if rec.owner_instance != expect_instance || rec.generation != expect_generation {
            return Err(ReleaseError::CleanupFailed {
                handle: handle.0,
                reason: format!(
                    "stale handle: owned by inst-{}#{}",
                    rec.owner_instance.0, rec.generation
                ),
            });
        }
        Ok(rec.clone())
    }

    pub fn active_count(&self) -> usize {
        self.inner
            .lock()
            .records
            .values()
            .filter(|r| r.state == ResourceState::Active)
            .count()
    }

    pub fn active_for(&self, owner: ContextId) -> usize {
        let i = self.inner.lock();
        i.by_owner
            .get(&owner)
            .map(|v| {
                v.iter()
                    .filter(|h| {
                        i.records
                            .get(h)
                            .map(|r| r.state == ResourceState::Active)
                            .unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn total_releases(&self) -> u64 {
        self.inner.lock().total_releases
    }

    pub fn double_release_attempts(&self) -> u64 {
        self.inner.lock().double_release_attempts
    }

    /// Inventory for inspection (C02/C19): all records, with state.
    pub fn inventory(&self) -> Vec<ResourceRecord> {
        let mut v: Vec<ResourceRecord> = self.inner.lock().records.values().cloned().collect();
        v.sort_by_key(|r| (r.owner_context.0, r.seq));
        v
    }

    /// Observability for real timers/tasks (cancellation proof).
    pub fn timer_fires(&self, handle: ResourceHandle) -> Option<usize> {
        self.inner
            .lock()
            .timers
            .get(&handle)
            .map(|t| t.fires.load(Ordering::SeqCst))
    }

    pub fn task_beats(&self, handle: ResourceHandle) -> Option<usize> {
        self.inner
            .lock()
            .tasks
            .get(&handle)
            .map(|t| t.beats.load(Ordering::SeqCst))
    }

    pub fn is_live(&self, handle: ResourceHandle) -> bool {
        let i = self.inner.lock();
        if let Some(t) = i.timers.get(&handle) {
            return !t.done.load(Ordering::SeqCst);
        }
        if let Some(t) = i.tasks.get(&handle) {
            return !t.done.load(Ordering::SeqCst);
        }
        i.records
            .get(&handle)
            .map(|r| r.state == ResourceState::Active)
            .unwrap_or(false)
    }
}
