//! Capability registry: `ns.name@MAJOR` → dona.
//! M1.1: cada registro carrega instância + geração para rejeitar
//! referências obsoletas (C01/I03) e nunca revogar a geração nova ao
//! descartar a antiga (I06). Métodos legados `provide`/`revoke_fiber`
//! são adaptadores preservados para a CLI antiga; código novo deve usar
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

    /// Adaptador legado: registra sem identidade de instância.
    /// Preservado para compat; não carrega a nova semântica de geração.
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

    /// Registro com identidade completa (caminho M1.1).
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

    /// Adaptador legado: remove tudo do fiber lógico (pode atingir geração
    /// nova; o Kernel M1.1 não usa mais este caminho no descarte).
    pub fn revoke_fiber(&self, fiber: &str) {
        self.inner.lock().retain(|_, v| v.logical != fiber);
    }

    /// Revogação segura: remove apenas caps da instância indicada (I06).
    /// Retorna quantas entradas foram removidas.
    pub fn revoke_instance(&self, inst: InstanceId) -> usize {
        let before = self.inner.lock().len();
        self.inner
            .lock()
            .retain(|_, v| v.instance != inst.0);
        before - self.inner.lock().len()
    }

    /// Revoga uma cap específica somente se ainda pertencer à instância.
    /// Evita que o descarte da geração antiga remova a nova (I06).
    pub fn revoke_cap_if_owned(&self, cap: &str, inst: InstanceId) -> bool {
        let mut m = self.inner.lock();
        match m.get(cap) {
            Some(e) if e.instance == inst.0 => {
                m.remove(cap);
                true
            }
            Some(e) if e.instance == 0 => {
                // Registro legado sem dono: remove por nome (compat).
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
