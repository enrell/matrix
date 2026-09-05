//! Tickets de admissão, drenagem/cancelamento e commits mediados (M1.3).
//!
//! Cobre C06, C08–C09 e I02/I03/I07 no perfil confiável in-process:
//! - `call_open` emite um ticket vinculado a instância, geração, contexto,
//!   época e autorização (capability resolvida + bindings válidos).
//! - O ponto de linearização da retirada (`Quiescing`) revoga admissão;
//!   `call_open` revalida após registrar o ticket, fechando a corrida
//!   admissão × descarte: ou a admissão observa o `Quiescing` e é rejeitada,
//!   ou o descarte observa o ticket e o drena/cancela.
//! - Política explícita por operação: `Drain` (conclui dentro do prazo) ou
//!   `Cancel` (revoga de imediato). Prazos limitados; espera sem locks do
//!   kernel retidos, via condvar.
//! - `commit_effect` é a fronteira que aplica o efeito: revalida ticket,
//!   geração e estado; tentativa tardia é rejeitada E jornalizada
//!   (`call.rejected`) — descartar a resposta não basta.
//! - Trabalho ainda ativo segura `CleanupPending`; recursos não são liberados
//!   prematuramente. `call_close` finaliza (`maybe_finalize` no kernel).
//!
//! Limites finitos (I09): `MAX_INFLIGHT_CALLS` por kernel, `MAX_EFFECTS` no
//! ledger (anel com despejo do mais antigo; o journal persiste tudo).

use crate::identity::{ContextId, InstanceId, ResourceHandle};
use parking_lot::{Condvar, Mutex};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Máximo de chamadas em andamento por kernel (I09).
pub const MAX_INFLIGHT_CALLS: usize = 256;
/// Tamanho do anel de efeitos confirmados em memória (I09).
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

/// Política explícita por operação (manifest `calls`, sobrescrevível na abertura).
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
    /// Sinal cooperativo de cancelamento observado pelo worker.
    pub cancel: Arc<AtomicBool>,
    /// Handles declarados como usados pela chamada (anti-liberação precoce).
    pub pins: Vec<ResourceHandle>,
    pub opened_at: Instant,
    /// Prazo de drenagem fixado na revogação (só `Drain`).
    pub drain_until: Option<Instant>,
    pub cancel_reason: Option<String>,
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
    /// Reivindicações de finalização em curso (evita release duplo ruidoso).
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

    /// Aloca ticket já validado pelo kernel. Retorna o id emitido.
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
            },
        );
        id
    }

    pub fn get(&self, id: TicketId) -> Option<TicketRecord> {
        self.inner.lock().tickets.get(&id).cloned()
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

    /// Revoga por cancelamento: marca + sinaliza. Só afeta `Admitted`.
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

    /// Expira drenagens vencidas. Retorna os ids expirados agora.
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

    /// Espera (sem reter locks do chamador) até não haver pendências da
    /// instância ou `until` chegar. Retorna `true` se aquietou.
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

    /// Registra efeito confirmado (anel limitado; journal persiste tudo).
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
