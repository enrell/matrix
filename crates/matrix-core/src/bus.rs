//! Barramento pub/sub por tópico (`domain.verb`).
//! M1.1: inscrições carregam instância para cleanup seletivo (I06) e
//! rejeição de geração obsoleta (C01). `subscribe`/`unsubscribe_fiber`
//! legados são adaptadores; código novo usa `*_instance`.

use crate::identity::InstanceId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sub {
    logical: String,
    instance: u64,
}

#[derive(Debug, Default)]
pub struct Bus {
    subs: Arc<Mutex<HashMap<String, Vec<Sub>>>>,
}

impl Bus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adaptador legado (sem instância).
    pub fn subscribe(&self, fiber: &str, topic: &str) {
        let mut subs = self.subs.lock();
        let e = subs.entry(topic.to_string()).or_default();
        let s = Sub {
            logical: fiber.to_string(),
            instance: 0,
        };
        if !e.contains(&s) {
            e.push(s);
        }
    }

    pub fn subscribe_instance(&self, logical: &str, inst: InstanceId, topic: &str) {
        let mut subs = self.subs.lock();
        let e = subs.entry(topic.to_string()).or_default();
        let s = Sub {
            logical: logical.to_string(),
            instance: inst.0,
        };
        if !e.contains(&s) {
            e.push(s);
        }
    }

    /// Adaptador legado: remove por nome lógico.
    pub fn unsubscribe_fiber(&self, fiber: &str) {
        let mut subs = self.subs.lock();
        for v in subs.values_mut() {
            v.retain(|s| s.logical != fiber);
        }
    }

    /// Remoção segura: só inscrições da instância (I06).
    pub fn unsubscribe_instance(&self, inst: InstanceId) {
        let mut subs = self.subs.lock();
        for v in subs.values_mut() {
            v.retain(|s| s.instance != inst.0);
        }
    }

    pub fn unsubscribe_topic_if_owned(&self, inst: InstanceId, topic: &str) -> bool {
        let mut subs = self.subs.lock();
        if let Some(v) = subs.get_mut(topic) {
            let before = v.len();
            v.retain(|s| !(s.instance == inst.0 || (s.instance == 0)));
            return v.len() != before;
        }
        false
    }

    pub fn subscribers(&self, topic: &str) -> Vec<String> {
        self.subs
            .lock()
            .get(topic)
            .map(|v| v.iter().map(|s| s.logical.clone()).collect())
            .unwrap_or_default()
    }

    pub fn subscriber_instances(&self, topic: &str) -> Vec<InstanceId> {
        self.subs
            .lock()
            .get(topic)
            .map(|v| {
                v.iter()
                    .filter(|s| s.instance != 0)
                    .map(|s| InstanceId(s.instance))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn publish(&self, _topic: &str, _payload: &serde_json::Value) {}
}
