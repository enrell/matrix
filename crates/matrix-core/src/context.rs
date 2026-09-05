//! Contextos e identidade de instâncias (M1.1).
//!
//! Uma instância é uma ativação concreta de um id lógico; um contexto é
//! o escopo de autoridade/propriedade dessa ativação (M1.1: 1:1).
//! Geração é monotônica por id lógico; reutilizar o id nunca revalida
//! referências antigas (C01/I03).
//!
//! A árvore de contextos (quem é descartado com quem) é separada do
//! grafo de dependências (quem pode estar ativo) — cf. ARCH. M1.1 monta
//! a árvore (raiz = kernel); o grafo reativo entra em M1.2.

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
    /// Causa registrada quando em CleanupPending/Failed (I12).
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

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    /// Cria nova ativação para um id lógico: geração monotônica +1,
    /// ids frescos nunca reutilizados. Estado inicial `Active`
    /// (M1.2: `create_in` permite `Waiting` para dependências ausentes).
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

    /// Nova ativação em `Waiting` com razão visível (C04).
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

    /// Mapa lógico → registro corrente (para resolução de bindings sem
    /// reter locks através de chamadas).
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

    /// Exige contexto ativo (I02). Retorna erro tipado para mapear em
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
        // Se não é mais a geração corrente do id lógico, a referência é
        // obsoleta mesmo que o registro ainda diga Active (não deveria,
        // mas a checagem é barata e explicita I03).
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

    /// Ponto de linearização da retirada: marca `Quiescing` e bloqueia
    /// novas admissões. Idempotente: se já terminal/em retirada, informa
    /// sem duplicar efeitos (I06).
    pub fn mark_quiescing(&self, inst: InstanceId) -> Result<ContextRecord, ContextError> {
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
            Fsm::Quiescing | Fsm::CleanupPending => {
                return Ok(rec.clone());
            }
            Fsm::Failed => {
                // Failed já passou por cleanup; não reabre.
                return Err(ContextError::AlreadyDisposed {
                    logical: rec.logical.clone(),
                });
            }
            _ => {}
        }
        rec.state = Fsm::Quiescing;
        rec.cause = None;
        Ok(rec.clone())
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

    /// Promove `Waiting`/`Preparing` → `Active` (ativação, LIFECYCLE §5).
    /// Retorna `false` se a instância não está mais aguardando (ex.: foi
    /// substituída por nova geração) — o chamador deve abortar.
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

    /// Atualiza a razão de `Waiting` sem trocar de instância.
    /// Não move `Active` → `Waiting` (retirada usa dispose + nova geração).
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

    /// Inventário para inspeção (C19 parcial em M1.1).
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
