//! Async pub/sub, topic string (`domain.verb`), at-most-once per subscriber.
//! Aceita blocking (sem poll-sleep) no daemon; aqui o barramento é síncrono
//! e a entrega p/ reducers vive em `kernel.rs`.

use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Bus {
    subs: Arc<Mutex<HashMap<String, Vec<String>>>>,
}

impl Bus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, fiber: &str, topic: &str) {
        let mut subs = self.subs.lock();
        let e = subs.entry(topic.to_string()).or_default();
        if !e.contains(&fiber.to_string()) {
            e.push(fiber.to_string());
        }
    }

    pub fn unsubscribe_fiber(&self, fiber: &str) {
        let mut subs = self.subs.lock();
        for v in subs.values_mut() {
            v.retain(|f| f != fiber);
        }
    }

    pub fn subscribers(&self, topic: &str) -> Vec<String> {
        self.subs.lock().get(topic).cloned().unwrap_or_default()
    }

    pub fn publish(&self, _topic: &str, _payload: &Value) {}
}
