//! Capability registry: `ns.name@MAJOR` → owner.
//! M1.1: each registration carries instance + generation to reject
//! stale references (C01/I03) and never revoke the new generation when
//! discarding the old one (I06). Legacy `provide`/`revoke_fiber` methods
//! are adapters kept for the old CLI; new code must use
//! `provide_instance`/`revoke_instance`/`resolve_ref`.

use crate::identity::{InstanceId, InstanceRef};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Entry {
    logical: String,
    instance: u64,
    generation: u64,
    epoch: u64,
}

#[derive(Debug, Default)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, Entry>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Legacy adapter: registers without instance identity.
    /// Kept for compat; does not carry the new generation semantics.
    pub fn provide(&self, cap: &str, fiber: &str) {
        self.inner.lock().insert(
            cap.to_string(),
            Entry {
                logical: fiber.to_string(),
                instance: 0,
                generation: 0,
                epoch: 0,
            },
        );
    }

    /// Registration with full identity (M1.1 path).
    pub fn provide_instance(&self, cap: &str, r: &InstanceRef) {
        self.inner.lock().insert(
            cap.to_string(),
            Entry {
                logical: r.logical.clone(),
                instance: r.instance,
                generation: r.generation,
                epoch: r.epoch,
            },
        );
    }

    pub fn resolve(&self, cap: &str) -> Option<String> {
        self.inner.lock().get(cap).map(|e| e.logical.clone())
    }

    pub fn resolve_ref(&self, cap: &str) -> Option<InstanceRef> {
        self.inner.lock().get(cap).map(|e| InstanceRef {
            epoch: e.epoch,
            instance: e.instance,
            context: 0,
            logical: e.logical.clone(),
            generation: e.generation,
        })
    }

    pub fn resolve_instance(&self, cap: &str) -> Option<InstanceId> {
        self.inner.lock().get(cap).and_then(|e| {
            if e.instance == 0 {
                None
            } else {
                Some(InstanceId(e.instance))
            }
        })
    }

    /// Legacy adapter: removes everything for the logical fiber (may hit the new
    /// generation; the M1.1 Kernel no longer uses this path on disposal).
    pub fn revoke_fiber(&self, fiber: &str) {
        self.inner.lock().retain(|_, v| v.logical != fiber);
    }

    /// Safe revocation: removes only the given instance's caps (I06).
    /// Returns how many entries were removed.
    pub fn revoke_instance(&self, inst: InstanceId) -> usize {
        let before = self.inner.lock().len();
        self.inner
            .lock()
            .retain(|_, v| v.instance != inst.0);
        before - self.inner.lock().len()
    }

    /// Revokes a specific cap only if it still belongs to the instance.
    /// Prevents discarding the old generation from removing the new one (I06).
    pub fn revoke_cap_if_owned(&self, cap: &str, inst: InstanceId) -> bool {
        let mut m = self.inner.lock();
        match m.get(cap) {
            Some(e) if e.instance == inst.0 => {
                m.remove(cap);
                true
            }
            Some(e) if e.instance == 0 => {
                // Legacy ownerless registration: remove by name (compat).
                m.remove(cap);
                true
            }
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn snapshot(&self) -> HashMap<String, String> {
        self.inner
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), v.logical.clone()))
            .collect()
    }

    pub fn snapshot_refs(&self) -> HashMap<String, InstanceRef> {
        self.inner
            .lock()
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    InstanceRef {
                        epoch: v.epoch,
                        instance: v.instance,
                        context: 0,
                        logical: v.logical.clone(),
                        generation: v.generation,
                    },
                )
            })
            .collect()
    }
}
