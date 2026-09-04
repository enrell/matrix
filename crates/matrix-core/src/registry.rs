//! Capability registry: `ns.name@MAJOR` → fiber dona.
//! Envelopes versionados passam por aqui sem quebrar (S10/S15).

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<String, String>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn provide(&self, cap: &str, fiber: &str) {
        self.inner.lock().insert(cap.to_string(), fiber.to_string());
    }

    pub fn resolve(&self, cap: &str) -> Option<String> {
        self.inner.lock().get(cap).cloned()
    }

    pub fn revoke_fiber(&self, fiber: &str) {
        self.inner.lock().retain(|_, v| v != fiber);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn snapshot(&self) -> HashMap<String, String> {
        self.inner.lock().clone()
    }
}
