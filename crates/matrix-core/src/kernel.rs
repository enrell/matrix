//! Kernel M1.2: composição local com dependências reativas.
//!
//! - Identidade por instância (`InstanceId`/`ContextId`/geração/época) (M1.1).
//! - Tabela de recursos reais com descarte idempotente (M1.1).
//! - Requisitos/bindings reativos (M1.2): `Waiting` enquanto faltarem
//!   dependências, ativação em ordem topológica, rejeição de ciclos e
//!   ambiguidade, retirada automática de consumidores e reativação em
//!   novas instâncias quando o provedor retorna.
//!
//! Adaptadores legados (`provide`, `leases`, códigos de erro antigos)
//! preservados para a CLI existente; não carregam a nova semântica.
//! Tickets/cancelamento (M1.3) e substituição coordenada (M1.4) seguem fora.

use crate::bus::Bus;
use crate::calls::{
    CallPolicy, CallsTable, CommittedEffect, TicketId, TicketRecord, TicketState, WithdrawPolicy,
    MAX_INFLIGHT_CALLS,
};
use crate::context::{ContextError, ContextRecord, ContextTable};
use crate::external::{
    CallForwarder, ExecutionKind, ForwardRequest, LifecycleEvent, LifecycleHook,
};
use crate::deps::{
    cap_satisfies, find_cycle, graph_edges, parse_provides, parse_requires, provider_candidates,
    topo_order, transitive_consumers, Binding, Requirement, ResolveError,
};
use crate::fsm::Fsm;
use crate::identity::{ContextId, InstanceId, InstanceRef, ResourceHandle};
use crate::journal::Journal;
use crate::leases::Lease;
use crate::registry::Registry;
use crate::resources::{ResourceKind, ResourceTable};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

/// Definição registrada (desejado). Sobrevive à retirada em cascata para
/// permitir reativação; remoção explícita apaga a definição.
#[derive(Debug, Clone)]
pub struct StoredManifest {
    pub id: String,
    pub caps: Vec<String>,
    pub requires: Vec<Requirement>,
    pub subs: Vec<String>,
    /// Política por operação (manifest `calls`; M1.3).
    pub calls: HashMap<String, CallPolicy>,
    pub reducer: String,
    pub init_state: Value,
    pub tier: String,
    pub trust: String,
    pub restart: String,
    /// Arquivo de origem (para o reload reconciliar removidos; M1.4).
    pub source: Option<PathBuf>,
    /// Como executar: em-processo ou processo externo (M2.2).
    pub execution: ExecutionKind,
}

/// Chamada admitida em andamento (handle do worker/teste).
#[derive(Debug, Clone)]
pub struct OpenCall {
    pub ticket: TicketId,
    pub cap: String,
    pub logical: String,
    pub instance: InstanceId,
    pub generation: u64,
    pub context: ContextId,
    pub policy: CallPolicy,
    /// Espelho do sinal de cancelamento (observado pelo worker).
    pub cancel: Arc<AtomicBool>,
}

fn parse_manifest_value(v: &Value) -> Result<StoredManifest, String> {
    let id = v.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
    if id.trim().is_empty() {
        return Err("manifest without id".into());
    }
    // provides: `capabilities` legado + `provides` novo (união, sem duplicar).
    let mut caps: Vec<String> = v
        .get("capabilities")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if v.get("capabilities").is_some()
        && v.get("capabilities").and_then(|x| x.as_array()).is_none()
    {
        return Err("'capabilities' deve ser array de strings".to_string());
    }
    let extra = parse_provides(v.get("provides"))?;
    for c in &extra {
        if !caps.contains(c) {
            caps.push(c.clone());
        }
    }
    if v.get("provides").is_some() && caps.is_empty() && extra.is_empty() {
        // `provides: []` explícito = nenhum cap; ok.
    }
    let requires = parse_requires(v.get("requires"))?;
    let subs: Vec<String> = v
        .get("subscriptions")
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if v.get("subscriptions").is_some()
        && v.get("subscriptions").and_then(|x| x.as_array()).is_none()
    {
        return Err("'subscriptions' deve ser array de strings".to_string());
    }
    // Caps/subs vazios após trim indicam manifest malformado.
    if let Some(arr) = v.get("capabilities").and_then(|x| x.as_array()) {
        if arr.iter().any(|x| x.as_str().map(|s| s.trim().is_empty()).unwrap_or(true)) {
            return Err("capability vazia".to_string());
        }
    }
    if let Some(arr) = v.get("subscriptions").and_then(|x| x.as_array()) {
        if arr.iter().any(|x| x.as_str().map(|s| s.trim().is_empty()).unwrap_or(true)) {
            return Err("subscription vazia".to_string());
        }
    }
    let reducer = v.get("reducer").and_then(|x| x.as_str()).unwrap_or("noop").to_string();
    let init_state = v.get("init_state").cloned().unwrap_or(json!({}));
    let tier = v.get("tier").and_then(|x| x.as_str()).unwrap_or("inproc").to_string();
    let trust = v.get("trust").and_then(|x| x.as_str()).unwrap_or("trusted").to_string();
    let restart = v.get("restart").and_then(|x| x.as_str()).unwrap_or("permanent").to_string();
    let calls = parse_call_policies(v.get("calls"))?;
    let execution = crate::external::parse_execution(v.get("execution"))?;
    Ok(StoredManifest {
        id,
        caps,
        requires,
        subs,
        calls,
        reducer,
        init_state,
        tier,
        trust,
        restart,
        source: None,
        execution,
    })
}

/// Política por operação a partir de `calls: {cap: {on_withdraw, drain_ms}}`.
fn parse_call_policies(v: Option<&Value>) -> Result<HashMap<String, CallPolicy>, String> {
    let Some(v) = v else { return Ok(HashMap::new()) };
    let obj = v.as_object().ok_or_else(|| "'calls' deve ser objeto cap→política".to_string())?;
    let mut out = HashMap::new();
    for (cap, pv) in obj {
        if cap.trim().is_empty() {
            return Err("cap vazia em 'calls'".to_string());
        }
        let mut pol = CallPolicy::default();
        if let Some(o) = pv.get("on_withdraw").and_then(|x| x.as_str()) {
            pol.on_withdraw = WithdrawPolicy::parse(o)
                .ok_or_else(|| format!("on_withdraw inválido em '{}': {:?}", cap, o))?;
        }
        if let Some(d) = pv.get("drain_ms").and_then(|x| x.as_u64()) {
            pol.drain_ms = d;
        } else if pv.get("drain_ms").is_some() {
            return Err(format!("drain_ms inválido em '{}'", cap));
        }
        out.insert(cap.clone(), pol);
    }
    Ok(out)
}

fn provides_of(defs: &HashMap<String, StoredManifest>) -> HashMap<String, Vec<String>> {
    defs.iter().map(|(k, d)| (k.clone(), d.caps.clone())).collect()
}

fn requires_of(defs: &HashMap<String, StoredManifest>) -> HashMap<String, Vec<Requirement>> {
    defs.iter().map(|(k, d)| (k.clone(), d.requires.clone())).collect()
}

#[derive(Debug, Clone)]
pub struct Plugin {
    pub id: String,
    pub state: Fsm,
    pub tier: String,
    pub trust: String,
    pub generation: u64,
    pub restart_policy: String,
    pub caps: Vec<String>,
    pub subs: Vec<String>,
    /// Requisitos declarados (cópia da definição; diagnóstico I12).
    pub requires: Vec<Requirement>,
    /// Bindings resolvidos para a instância corrente (I04).
    pub bindings: Vec<Binding>,
    /// Política por operação vigente nesta instância (M1.3).
    pub calls: HashMap<String, CallPolicy>,
    /// Como executar esta instância (M2.2).
    pub execution: ExecutionKind,
    /// Razão visível quando em `Waiting` (C04/C19).
    pub wait_cause: Option<String>,
    pub reducer: String,
    pub init_state: Value,
    pub json_state: Value,
    /// Legado: mantido para compat; propriedade real vive em `resources`.
    pub leases: Vec<Lease>,
    pub panic_count: u32,
    pub restart_times: Vec<Instant>,
    // --- M1.1: identidade de instância ---
    pub instance_id: u64,
    pub context_id: u64,
    pub epoch: u64,
    /// Handles adquiridos pela instância (ordem de aquisição).
    pub resources: Vec<ResourceHandle>,
}

impl Plugin {
    pub fn restart_policy_str(&self) -> &str {
        &self.restart_policy
    }

    pub fn instance_ref(&self) -> InstanceRef {
        InstanceRef::new(self.epoch, self.instance_id, self.context_id, &self.id, self.generation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisposeOutcome {
    Disposed,
    AlreadyDisposed,
    CleanupPending { reason: String },
}

impl DisposeOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            DisposeOutcome::Disposed => "Disposed",
            DisposeOutcome::AlreadyDisposed => "AlreadyDisposed",
            DisposeOutcome::CleanupPending { .. } => "CleanupPending",
        }
    }
}

pub struct Kernel {
    pub plugins: Mutex<HashMap<String, Plugin>>,
    pub caps: Registry,
    pub bus: Bus,
    pub journal: Journal,
    pub journal_path: PathBuf,
    pub plugins_dir: PathBuf,
    pub quit: Arc<AtomicBool>,
    pub dry_run: bool,
    pub contexts: ContextTable,
    pub resources: ResourceTable,
    /// Definições desejadas (manifests). Retirada em cascata preserva;
    /// remoção explícita apaga.
    pub definitions: Mutex<HashMap<String, StoredManifest>>,
    /// Chamadas em andamento, tickets e efeitos confirmados (M1.3).
    pub calls: CallsTable,
    /// Entrega de chamadas externas (M2.2; ausente = erro explícito).
    pub forwarder: Mutex<Option<Arc<dyn CallForwarder>>>,
    /// Eventos de lifecycle para o host (M2.2; não-bloqueante).
    pub hook: Mutex<Option<Arc<dyn LifecycleHook>>>,
}

impl Kernel {
    pub fn new(home: &PathBuf, journal: Journal, dry_run: bool) -> Self {
        Self {
            plugins: Mutex::new(HashMap::new()),
            caps: Registry::new(),
            bus: Bus::new(),
            journal,
            journal_path: home.join("run/journal.jsonl"),
            plugins_dir: home.join("plugins"),
            quit: Arc::new(AtomicBool::new(false)),
            dry_run,
            contexts: ContextTable::new(),
            resources: ResourceTable::new(),
            definitions: Mutex::new(HashMap::new()),
            calls: CallsTable::new(),
            forwarder: Mutex::new(None),
            hook: Mutex::new(None),
        }
    }

    /// Registra o entregador de chamadas externas (host local, M2.2).
    pub fn set_forwarder(&self, f: Arc<dyn CallForwarder>) {
        *self.forwarder.lock() = Some(f);
    }

    /// Registra o observador de lifecycle (host local, M2.2).
    pub fn set_hook(&self, h: Arc<dyn LifecycleHook>) {
        *self.hook.lock() = Some(h);
    }

    fn emit_hook(&self, ev: LifecycleEvent) {
        // Não-bloqueante por contrato: a impl enfileira; o kernel nunca
        // espera o host (e nunca com locks retidos — chamadores garantem).
        if let Some(h) = self.hook.lock().clone() {
            h.on_lifecycle(ev);
        }
    }

    fn emit_activated(&self, logical: &str, instance: u64, generation: u64) {
        let def = self.definitions.lock().get(logical).cloned();
        let Some(def) = def else { return };
        if let ExecutionKind::External { entrypoint, args, timeout_ms } = def.execution {
            self.emit_hook(LifecycleEvent::Activated {
                logical: logical.to_string(),
                instance,
                generation,
                entrypoint,
                args,
                timeout_ms,
            });
        }
    }

    pub fn epoch(&self) -> u64 {
        self.contexts.epoch()
    }

    /// Adaptador legado preservado.
    pub fn provide(&self, cap: &str, fiber: &str) {
        self.caps.provide(cap, fiber);
    }

    pub fn instance_ref_of(&self, logical: &str) -> Option<InstanceRef> {
        self.contexts.current(logical).map(|r| r.reference())
    }

    pub fn instance_of(&self, logical: &str) -> Option<InstanceId> {
        self.contexts.current(logical).map(|r| r.instance)
    }

    pub fn waiting_reason_of(&self, logical: &str) -> Option<String> {
        self.contexts.current(logical).and_then(|r| r.cause)
    }

    pub fn bindings_of(&self, logical: &str) -> Vec<Binding> {
        self.plugins.lock().get(logical).map(|p| p.bindings.clone()).unwrap_or_default()
    }

    /// Política vigente para uma operação (manifest `calls` ou padrão M1.3).
    pub fn policy_for(&self, logical: &str, cap: &str) -> CallPolicy {
        self.plugins
            .lock()
            .get(logical)
            .and_then(|p| p.calls.get(cap).copied())
            .unwrap_or_default()
    }

    // ---- chamadas em voo: tickets, drenagem e commits mediados (M1.3) ----

    /// Núcleo de admissão compartilhado por `invoke` e `call_open` (I02–I04).
    /// Retorna o lógico dono + registro de contexto ativo e corrente.
    fn admission_record(&self, cap: &str) -> Result<(String, ContextRecord), (Value, bool)> {
        let r = self.caps.resolve_ref(cap);
        let Some(r) = r else {
            // Cap de definição conhecida mas não publicada? Diagnóstico de
            // dependência em vez de "não existe" genérico (C04/C19).
            let owner: Option<String> = self
                .definitions
                .lock()
                .iter()
                .find(|(_, d)| d.caps.iter().any(|c| c == cap))
                .map(|(k, _)| k.clone());
            if let Some(o) = owner {
                if let Some(st) = self.contexts.current(&o) {
                    match st.state.canonical() {
                        Fsm::Waiting | Fsm::Preparing | Fsm::Registered => {
                            let reason = st.cause.clone().unwrap_or_else(|| "dependência indisponível".to_string());
                            return Err((
                                json!({"error": reason, "code": "dependency-unavailable", "logical": o}),
                                false,
                            ));
                        }
                        Fsm::Quiescing | Fsm::CleanupPending | Fsm::Disposed | Fsm::Failed => {
                            return Err((
                                json!({"error": "plugin not available", "code": "context-not-active", "logical": o}),
                                false,
                            ));
                        }
                        _ => {}
                    }
                }
            }
            return Err((json!({"error": "no such capability", "code": "no-such-capability"}), false));
        };
        // Registro legado sem instância: vincula à instância corrente.
        let inst = if r.instance == 0 {
            let cur = self.contexts.current(&r.logical);
            match cur {
                Some(c) => c.instance,
                None => {
                    return Err((json!({"error": "plugin not available", "code": "plugin-not-loaded"}), false))
                }
            }
        } else {
            InstanceId(r.instance)
        };
        match self.contexts.require_active(inst) {
            Ok(ctx) => {
                // Geração corrente do id lógico (I03).
                if let Some(cur) = self.contexts.current(&ctx.logical) {
                    if cur.instance != inst {
                        return Err((
                            json!({"error": "stale generation", "code": "stale-generation",
                                   "logical": ctx.logical, "generation": ctx.generation}),
                            false,
                        ));
                    }
                }
                let fiber = ctx.logical.clone();
                // I04: instância Active com bindings válidos?
                let fresh = self.contexts.current_all();
                let active = self
                    .plugins
                    .lock()
                    .get(&fiber)
                    .map(|p| p.state.canonical() == Fsm::Active)
                    .unwrap_or(false);
                if !active {
                    return Err((
                        json!({"error": "plugin not available", "code": "context-not-active"}),
                        false,
                    ));
                }
                if self.bindings_stale(&fiber, &fresh) {
                    return Err((
                        json!({"error": "dependency-unavailable: binding obsoleto", "code": "dependency-unavailable", "logical": fiber}),
                        false,
                    ));
                }
                Ok((fiber, ctx))
            }
            Err(ContextError::NotActive { logical, state }) => Err((
                json!({"error": "context not active", "code": "context-not-active",
                       "logical": logical, "state": state}),
                false,
            )),
            Err(ContextError::StaleGeneration { logical, expected, got }) => Err((
                json!({"error": "stale generation", "code": "stale-generation",
                       "logical": logical, "expected": expected, "got": got}),
                false,
            )),
            Err(_) => Err((
                json!({"error": "stale generation", "code": "stale-generation"}),
                false,
            )),
        }
    }

    /// Abre uma chamada: emite ticket vinculado a instância, geração,
    /// contexto, época e autorização (I02–I04). A revalidação pós-registro
    /// fecha a corrida admissão × descarte: ou enxerga o `Quiescing` e é
    /// rejeitada, ou o descarte a enxerga e a drena/cancela.
    pub fn call_open(
        &self,
        cap: &str,
        input: &Value,
        policy_override: Option<CallPolicy>,
        pins: &[ResourceHandle],
    ) -> Result<OpenCall, Value> {
        if self.calls.inflight_count() >= MAX_INFLIGHT_CALLS {
            return Err(json!({"error": "too many in-flight calls", "code": "resource-exhausted"}));
        }
        let (logical, ctx) = self.admission_record(cap).map_err(|(v, _)| v)?;
        // Pins: devem existir, ativos e pertencer à mesma ativação.
        for h in pins {
            match self.resources.validate(*h, ctx.instance, ctx.generation) {
                Ok(_) => {}
                Err(_) => {
                    return Err(json!({"error": format!("invalid pin {}", h), "code": "invalid-pin"}));
                }
            }
        }
        let policy = policy_override.unwrap_or_else(|| self.policy_for(&logical, cap));
        let cancel = Arc::new(AtomicBool::new(false));
        let id = self.calls.alloc(
            cap,
            &logical,
            ctx.instance,
            ctx.generation,
            ctx.context,
            ctx.epoch,
            policy,
            pins.to_vec(),
            cancel.clone(),
        );
        self.journal.append(
            &logical,
            "call.open",
            json!({
                "ticket": id.0, "cap": cap, "input": input,
                "instance": ctx.instance.0, "generation": ctx.generation,
                "policy": policy.on_withdraw.as_str(), "drain_ms": policy.drain_ms,
            }),
            Value::Null,
        );
        // Revalidação: o descarte pode ter linearizado entre admitir e alocar.
        match self.admission_record(cap) {
            Ok((_, fresh)) if fresh.instance == ctx.instance && fresh.generation == ctx.generation => {
                Ok(OpenCall {
                    ticket: id,
                    cap: cap.to_string(),
                    logical,
                    instance: ctx.instance,
                    generation: ctx.generation,
                    context: ctx.context,
                    policy,
                    cancel,
                })
            }
            Ok(_) => {
                self.calls.remove(id);
                self.journal.append(
                    &logical,
                    "call.cancelled",
                    json!({"ticket": id.0, "reason": "admission-race: superseded"}),
                    Value::Null,
                );
                Err(json!({"error": "stale generation", "code": "stale-generation", "logical": logical}))
            }
            Err((v, _)) => {
                self.calls.remove(id);
                self.journal.append(
                    &logical,
                    "call.cancelled",
                    json!({"ticket": id.0, "reason": "admission-race"}),
                    Value::Null,
                );
                Err(v)
            }
        }
    }

    /// Fronteira que aplica o efeito: rejeita tickets revogados, expirados,
    /// de geração obsoleta ou de contexto fora de drenagem válida. A
    /// tentativa tardia é rejeitada E jornalizada (`call.rejected`).
    pub fn commit_effect(&self, ticket: TicketId, kind: &str, payload: &Value) -> Result<u64, Value> {
        let t = self.calls.get(ticket).ok_or_else(|| {
            json!({"error": format!("unknown ticket {}", ticket), "code": "unknown-ticket"})
        })?;
        match t.state {
            TicketState::Committed => {
                return Err(json!({"error": "already committed", "code": "already-committed", "ticket": ticket.0}));
            }
            TicketState::Cancelled => {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "cancelled", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "ticket cancelled", "code": "cancelled", "ticket": ticket.0}));
            }
            TicketState::Expired => {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "drain-exceeded", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "drain deadline exceeded", "code": "deadline-exceeded", "ticket": ticket.0}));
            }
            TicketState::Admitted => {}
        }
        if t.epoch != self.epoch() {
            return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
        }
        let owner = self.contexts.get_by_instance(t.instance);
        let Some(owner) = owner else {
            return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
        };
        if owner.logical != t.logical || owner.generation != t.generation {
            return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
        }
        if let Some(cur) = self.contexts.current(&t.logical) {
            if cur.instance != t.instance {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "stale-generation", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "stale generation", "code": "stale-generation", "logical": t.logical}));
            }
        }
        match owner.state.canonical() {
            Fsm::Active => {}
            Fsm::Quiescing => {
                // Drenagem limitada: commit aceito dentro do prazo (efeito
                // concluído antes da revogação é registrado, não desfeito).
                let within = t.policy.on_withdraw == WithdrawPolicy::Drain
                    && t.drain_until.map(|u| std::time::Instant::now() <= u).unwrap_or(false);
                if !within {
                    self.journal.append(
                        &t.logical,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "revoked", "kind": kind}),
                        Value::Null,
                    );
                    return Err(json!({"error": "ticket revoked", "code": "cancelled", "ticket": ticket.0}));
                }
            }
            _ => {
                self.journal.append(
                    &t.logical,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "context-not-active", "kind": kind}),
                    Value::Null,
                );
                return Err(json!({"error": "context not active", "code": "context-not-active", "logical": t.logical}));
            }
        }
        let e = self.calls.push_effect(ticket, &t.logical, t.instance, t.generation, kind, payload.clone());
        self.calls.set_state(ticket, TicketState::Committed);
        self.journal.append(
            &t.logical,
            "call.committed",
            json!({
                "ticket": ticket.0, "effect": e.seq, "kind": kind,
                "instance": t.instance.0, "generation": t.generation,
            }),
            Value::Null,
        );
        Ok(e.seq)
    }

    /// Cancelamento cooperativo explícito. Retorna se havia ticket aberto.
    pub fn call_cancel(&self, ticket: TicketId, reason: &str) -> bool {
        let rec = self.calls.get(ticket);
        let Some(r) = rec else { return false };
        if self.calls.cancel(ticket, reason) {
            self.journal.append(
                &r.logical,
                "call.cancelled",
                json!({"ticket": ticket.0, "reason": reason}),
                Value::Null,
            );
            true
        } else {
            false
        }
    }

    /// Encerra o acompanhamento (idempotente). Finaliza o dono se a retirada
    /// o deixou pendente e nada mais resta ativo.
    pub fn call_close(&self, ticket: TicketId) -> bool {
        let Some(r) = self.calls.remove(ticket) else { return false };
        self.journal.append(
            &r.logical,
            "call.closed",
            json!({"ticket": ticket.0, "state": r.state.as_str()}),
            Value::Null,
        );
        self.maybe_finalize(r.instance);
        true
    }

    /// Chamadas em andamento (C19): todas, com estado e política.
    pub fn pending_calls(&self) -> Vec<TicketRecord> {
        self.calls.snapshot()
    }

    /// Efeitos confirmados, opcionalmente por lógico (C08).
    pub fn committed_effects(&self, logical: Option<&str>) -> Vec<CommittedEffect> {
        self.calls.effects_for(logical)
    }

    /// Finalização verificável: dono em `CleanupPending` sem mais trabalho
    /// ativo libera recursos e vai a `Disposed`. Nunca reativa descartada:
    /// só cria-se nova instância via reconciliador. Ao assentar, reconcilia
    /// (definição persistente ganha substituta sem evento externo).
    fn maybe_finalize(&self, inst: InstanceId) {
        let rec = match self.contexts.get_by_instance(inst) {
            Some(r) => r,
            None => return,
        };
        if rec.state.canonical() != Fsm::CleanupPending {
            return;
        }
        if self.calls.pending_count(inst) > 0 {
            return;
        }
        if !self.calls.try_claim_cleanup(inst) {
            return;
        }
        let mut settled = false;
        let failures = self.release_context_resources(&rec);
        if failures.is_empty() {
            self.contexts.mark_disposed(inst);
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::Disposed;
                        p.leases.clear();
                    }
                }
            }
            self.journal.append(
                &rec.logical,
                "plugin.unloaded",
                json!({
                    "id": rec.logical, "instance": inst.0,
                    "context": rec.context.0, "generation": rec.generation,
                    "after": "cleanup-settled",
                }),
                Value::Null,
            );
            settled = true;
        } else {
            self.contexts
                .mark_cleanup_pending(inst, failures.join("; "));
        }
        self.calls.release_cleanup_claim(inst);
        if settled {
            self.reconcile();
        }
    }

    /// Libera recursos do contexto em ordem reversa (sem locks retidos no
    /// join). Retorna falhas; idempotente (dupla tentativa é benigna).
    fn release_context_resources(&self, rec: &ContextRecord) -> Vec<String> {
        let mut handles = self.resources.handles_for(rec.context);
        handles.reverse();
        let mut failures: Vec<String> = vec![];
        for h in handles {
            let Some(rr) = self.resources.record(h) else { continue };
            if rr.state != crate::resources::ResourceState::Active {
                continue;
            }
            if rr.owner_context != rec.context {
                continue;
            }
            match self.resources.begin_release(h) {
                Ok(claim) => {
                    match &claim.kind {
                        ResourceKind::Cap { name } => {
                            self.caps.revoke_cap_if_owned(name, rec.instance);
                        }
                        ResourceKind::Sub { topic } => {
                            self.bus.unsubscribe_topic_if_owned(rec.instance, topic);
                        }
                        _ => {}
                    }
                    crate::resources::ResourceTable::finish_release(claim);
                }
                Err(crate::resources::ReleaseError::AlreadyReleased(_)) => {}
                Err(crate::resources::ReleaseError::UnknownHandle(_)) => {}
                Err(crate::resources::ReleaseError::CleanupFailed { reason, .. }) => {
                    failures.push(reason);
                }
            }
        }
        failures
    }

    // ---- aquisição de recursos (C02) ----

    /// Adquire um recurso para a geração corrente do id lógico.
    /// Rejeita se o contexto saiu de `Active` (I02). Registra propriedade
    /// antes de publicar o efeito; falha parcial desfaz o registro (I05).
    pub fn acquire(&self, logical: &str, kind: ResourceKind) -> Result<ResourceHandle, String> {
        let ctx = self
            .contexts
            .current(logical)
            .ok_or_else(|| "plugin-not-loaded".to_string());
        let ctx = ctx?;
        self.contexts
            .require_active(ctx.instance)
            .map_err(|e| match e {
                ContextError::NotActive { .. } => "context-not-active".to_string(),
                ContextError::StaleGeneration { .. } => "stale-generation".to_string(),
                _ => "plugin-not-loaded".to_string(),
            })?;

        let handle = self
            .resources
            .register(ctx.context, ctx.instance, &ctx.logical, ctx.generation, kind.clone())
            .map_err(|e| e.to_string())?;

        // Publica o efeito somente após o registro (I05).
        let publish_err: Option<String> = match &kind {
            ResourceKind::Cap { name } => {
                let r = ctx.reference();
                self.caps.provide_instance(name, &r);
                None
            }
            ResourceKind::Sub { topic } => {
                self.bus.subscribe_instance(&ctx.logical, ctx.instance, topic);
                None
            }
            ResourceKind::Timer { .. }
            | ResourceKind::Task { .. }
            | ResourceKind::FailRelease { .. } => None,
            ResourceKind::FailAcquire { .. } => {
                unreachable!("register rejeita FailAcquire antes de publicar")
            }
        };
        if let Some(err) = publish_err {
            self.resources.rollback_register(handle);
            return Err(err);
        }

        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(logical) {
                // Evita registrar handle de geração antiga na nova entrada.
                if p.instance_id == ctx.instance.0 && p.generation == ctx.generation {
                    p.resources.push(handle);
                }
            }
        }
        self.journal.append(
            logical,
            "resource.acquired",
            json!({
                "handle": handle.0,
                "kind": kind.kind_name(),
                "label": kind.label(),
                "instance": ctx.instance.0,
                "context": ctx.context.0,
                "generation": ctx.generation,
            }),
            Value::Null,
        );
        Ok(handle)
    }

    /// Aquisição transacional: falha em qualquer item limpa os anteriores
    /// em ordem reversa (I05). Base para C03 sem oráculos externos.
    pub fn acquire_batch(
        &self,
        logical: &str,
        kinds: Vec<ResourceKind>,
    ) -> Result<Vec<ResourceHandle>, String> {
        let mut out = Vec::with_capacity(kinds.len());
        for k in kinds {
            match self.acquire(logical, k) {
                Ok(h) => out.push(h),
                Err(e) => {
                    for h in out.iter().rev() {
                        let _ = self.release(*h);
                    }
                    return Err(e);
                }
            }
        }
        Ok(out)
    }

    /// Libera um handle. Nunca libera duas vezes: a segunda chamada
    /// retorna erro sem reexecutar cleanup (C03). Handles de outra
    /// ativação não são tocados aqui — descarte usa o dono registrado.
    pub fn release(&self, handle: ResourceHandle) -> Result<(), String> {
        let rec = self.resources.record(handle).ok_or_else(|| {
            // Journal não registra; handle desconhecido é rejeitado (C01).
            "unknown-handle".to_string()
        })?;
        // Recurso afixado (pinned) por chamada em andamento não pode ser
        // liberado prematuramente (M1.3).
        if rec.state == crate::resources::ResourceState::Active {
            let pinned = self
                .calls
                .pending_for(rec.owner_instance)
                .iter()
                .any(|t| t.pins.contains(&handle));
            if pinned {
                return Err("resource-pinned".to_string());
            }
        }
        let claim = self
            .resources
            .begin_release(handle)
            .map_err(|e| match e {
                crate::resources::ReleaseError::AlreadyReleased(_) => {
                    "already-released".to_string()
                }
                crate::resources::ReleaseError::UnknownHandle(_) => "unknown-handle".to_string(),
                crate::resources::ReleaseError::CleanupFailed { reason, .. } => reason,
            })?;
        // Revoga publicação somente se ainda pertencer à instância dona.
        match &claim.kind {
            ResourceKind::Cap { name } => {
                self.caps
                    .revoke_cap_if_owned(name, InstanceId(rec.owner_instance.0));
            }
            ResourceKind::Sub { topic } => {
                self.bus
                    .unsubscribe_topic_if_owned(InstanceId(rec.owner_instance.0), topic);
            }
            _ => {}
        }
        crate::resources::ResourceTable::finish_release(claim);
        self.journal.append(
            &rec.owner_logical,
            "resource.released",
            json!({
                "handle": handle.0,
                "instance": rec.owner_instance.0,
                "generation": rec.generation,
            }),
            Value::Null,
        );
        Ok(())
    }

    /// Valida uso de handle contra a ativação corrente (C01).
    pub fn validate_handle(&self, handle: ResourceHandle, logical: &str) -> Result<(), String> {
        let cur = self
            .contexts
            .current(logical)
            .ok_or_else(|| "plugin-not-loaded".to_string())?;
        self.resources
            .validate(handle, cur.instance, cur.generation)
            .map(|_| ())
            .map_err(|e| match e {
                crate::resources::ReleaseError::AlreadyReleased(_) => {
                    "stale-handle".to_string()
                }
                crate::resources::ReleaseError::UnknownHandle(_) => "unknown-handle".to_string(),
                crate::resources::ReleaseError::CleanupFailed { reason, .. } => reason,
            })
    }

    // ---- dependências reativas (M1.2) ----

    /// Resolve os bindings de um lógico contra definições + estados
    /// correntes. Ciclo, ambiguidade, versão e provedor inativo viram
    /// `ResolveError` com razão visível (nunca "último vence").
    fn resolve_for(
        logical: &str,
        defs: &HashMap<String, StoredManifest>,
        states: &HashMap<String, ContextRecord>,
        cycle_members: &HashSet<String>,
        cycle_path: &[String],
    ) -> Result<Vec<Binding>, ResolveError> {
        if cycle_members.contains(logical) {
            return Err(ResolveError::Cycle { path: cycle_path.to_vec() });
        }
        let def = defs.get(logical).ok_or_else(|| ResolveError::Missing {
            interface: logical.to_string(),
        })?;
        if def.requires.is_empty() {
            return Ok(vec![]);
        }
        let provides = provides_of(defs);
        let mut out = Vec::with_capacity(def.requires.len());
        for req in &def.requires {
            if let Some(want_provider) = &req.provider {
                // Binding explícito: provedor indicado deve existir e prover.
                let pdef = defs.get(want_provider).ok_or_else(|| ResolveError::UnknownProvider {
                    interface: req.interface.clone(),
                    provider: want_provider.clone(),
                })?;
                if !pdef.caps.iter().any(|c| cap_satisfies(c, req)) {
                    return Err(ResolveError::VersionMismatch {
                        interface: req.interface.clone(),
                        provider: want_provider.clone(),
                        detail: format!("{} provê [{}]", want_provider, pdef.caps.join(", ")),
                    });
                }
                let st = states.get(want_provider).ok_or_else(|| ResolveError::ProviderNotActive {
                    interface: req.interface.clone(),
                    provider: want_provider.clone(),
                    state: "unknown".to_string(),
                })?;
                if st.state.canonical() != Fsm::Active {
                    let mut s = st.state.as_str().to_string();
                    if let Some(c) = &st.cause {
                        s = format!("{} ({})", s, c);
                    }
                    return Err(ResolveError::ProviderNotActive {
                        interface: req.interface.clone(),
                        provider: want_provider.clone(),
                        state: s,
                    });
                }
                out.push(Binding {
                    interface: req.interface.clone(),
                    provider_logical: want_provider.clone(),
                    provider_instance: st.instance.0,
                    provider_generation: st.generation,
                });
            } else {
                let mut cands = provider_candidates(req, &provides);
                // Auto-dependência conta como candidata (vira ciclo se única).
                if cands.is_empty() {
                    return Err(ResolveError::Missing { interface: req.interface.clone() });
                }
                if cands.len() > 1 {
                    cands.sort();
                    return Err(ResolveError::Ambiguous {
                        interface: req.interface.clone(),
                        providers: cands,
                    });
                }
                let pname = cands.into_iter().next().unwrap();
                let st = states.get(&pname).ok_or_else(|| ResolveError::ProviderNotActive {
                    interface: req.interface.clone(),
                    provider: pname.clone(),
                    state: "unknown".to_string(),
                })?;
                if st.state.canonical() != Fsm::Active {
                    let mut s = st.state.as_str().to_string();
                    if let Some(c) = &st.cause {
                        s = format!("{} ({})", s, c);
                    }
                    return Err(ResolveError::ProviderNotActive {
                        interface: req.interface.clone(),
                        provider: pname,
                        state: s,
                    });
                }
                out.push(Binding {
                    interface: req.interface.clone(),
                    provider_logical: pname.clone(),
                    provider_instance: st.instance.0,
                    provider_generation: st.generation,
                });
            }
        }
        Ok(out)
    }

    /// I04 parcial: bindings guardados ainda apontam para as instâncias
    /// provedoras correntes e ativas?
    fn bindings_stale(&self, logical: &str, states: &HashMap<String, ContextRecord>) -> bool {
        let bindings = self.plugins.lock().get(logical).map(|p| p.bindings.clone()).unwrap_or_default();
        if bindings.is_empty() {
            return false;
        }
        for b in &bindings {
            match states.get(&b.provider_logical) {
                Some(st)
                    if st.state.canonical() == Fsm::Active
                        && st.instance.0 == b.provider_instance
                        && st.generation == b.provider_generation =>
                {
                    continue
                }
                _ => return true,
            }
        }
        false
    }

    /// Cria instância `Waiting` + entrada de plugin correspondente.
    /// Não publica capacidades (I04). Retorna a geração criada.
    fn create_waiting_instance(&self, def: &StoredManifest, reason: &str) -> ContextRecord {
        let ctx = self.contexts.create_waiting(&def.id, reason.to_string());
        let p = Plugin {
            id: def.id.clone(),
            state: Fsm::Waiting,
            tier: def.tier.clone(),
            trust: def.trust.clone(),
            generation: ctx.generation,
            restart_policy: def.restart.clone(),
            caps: def.caps.clone(),
            subs: def.subs.clone(),
            requires: def.requires.clone(),
            calls: def.calls.clone(),
            execution: def.execution.clone(),
            bindings: vec![],
            wait_cause: Some(reason.to_string()),
            reducer: def.reducer.clone(),
            init_state: def.init_state.clone(),
            json_state: initial_json_state(&def.reducer, &def.init_state),
            leases: vec![],
            panic_count: 0,
            restart_times: vec![],
            instance_id: ctx.instance.0,
            context_id: ctx.context.0,
            epoch: ctx.epoch,
            resources: vec![],
        };
        self.plugins.lock().insert(def.id.clone(), p);
        self.journal.append(
            &def.id,
            "plugin.waiting",
            json!({
                "id": def.id,
                "instance": ctx.instance.0,
                "context": ctx.context.0,
                "generation": ctx.generation,
                "reason": reason,
                "requires": def.requires.iter().map(|r| r.interface.clone()).collect::<Vec<_>>(),
            }),
            Value::Null,
        );
        ctx
    }

    /// Garante `Waiting` com a razão indicada. Não move `Active` → `Waiting`
    /// na mesma instância (retirada usa dispose + nova geração). Retorna
    /// `true` se algo mudou.
    fn ensure_waiting(&self, logical: &str, defs: &HashMap<String, StoredManifest>, reason: String) -> bool {
        let Some(def) = defs.get(logical).cloned() else { return false };
        let cur = self.contexts.current(logical);
        match cur {
            None => {
                self.create_waiting_instance(&def, &reason);
                true
            }
            Some(st) => match st.state.canonical() {
                Fsm::Waiting | Fsm::Preparing | Fsm::Registered => {
                    if st.cause.as_deref() == Some(reason.as_str()) {
                        // Sincroniza entrada de plugin se divergir.
                        let mut changed = false;
                        {
                            let mut ps = self.plugins.lock();
                            if let Some(p) = ps.get_mut(logical) {
                                if p.state.canonical() != Fsm::Waiting || p.wait_cause.as_deref() != Some(reason.as_str()) {
                                    p.state = Fsm::Waiting;
                                    p.wait_cause = Some(reason.clone());
                                    changed = true;
                                }
                            } else {
                                changed = true;
                            }
                        }
                        if changed {
                            let p = Plugin {
                                id: def.id.clone(),
                                state: Fsm::Waiting,
                                tier: def.tier.clone(),
                                trust: def.trust.clone(),
                                generation: st.generation,
                                restart_policy: def.restart.clone(),
                                caps: def.caps.clone(),
                                subs: def.subs.clone(),
                                requires: def.requires.clone(),
                                calls: def.calls.clone(),
                                execution: def.execution.clone(),
                                bindings: vec![],
                                wait_cause: Some(reason.clone()),
                                reducer: def.reducer.clone(),
                                init_state: def.init_state.clone(),
                                json_state: initial_json_state(&def.reducer, &def.init_state),
                                leases: vec![],
                                panic_count: 0,
                                restart_times: vec![],
                                instance_id: st.instance.0,
                                context_id: st.context.0,
                                epoch: st.epoch,
                                resources: vec![],
                            };
                            self.plugins.lock().insert(logical.to_string(), p);
                        }
                        changed
                    } else {
                        self.contexts.mark_waiting(st.instance, reason.clone());
                        {
                            let mut ps = self.plugins.lock();
                            if let Some(p) = ps.get_mut(logical) {
                                p.state = Fsm::Waiting;
                                p.wait_cause = Some(reason.clone());
                            }
                        }
                        self.journal.append(
                            logical,
                            "plugin.waiting",
                            json!({"id": logical, "instance": st.instance.0, "generation": st.generation, "reason": reason}),
                            Value::Null,
                        );
                        true
                    }
                }
                Fsm::Disposed => {
                    self.create_waiting_instance(&def, &reason);
                    true
                }
                // Failed/CleanupPending/Quiescing: terminal ou incerto; não toca.
                _ => false,
            },
        }
    }

    /// Retira instância ativa para `Waiting` em NOVA geração:
    /// dispose (limpa recursos, revoga caps) + cria Waiting.
    /// Consumidores-antes-de-provedores é garantido pelo chamador
    /// (passada reversa ao topo). Retorna `true` se mudou algo.
    fn withdraw_to_waiting(&self, logical: &str, defs: &HashMap<String, StoredManifest>, reason: String) -> bool {
        let Some(def) = defs.get(logical).cloned() else { return false };
        let cur = self.contexts.current(logical);
        match cur {
            None => {
                self.create_waiting_instance(&def, &reason);
                true
            }
            Some(st) => match st.state.canonical() {
                Fsm::Active | Fsm::Preparing | Fsm::Registered => {
                    let _ = self.dispose_instance_inner(st.instance, false);
                    self.create_waiting_instance(&def, &reason);
                    true
                }
                Fsm::Waiting => self.ensure_waiting(logical, defs, reason),
                Fsm::Disposed => {
                    self.create_waiting_instance(&def, &reason);
                    true
                }
                _ => false,
            },
        }
    }

    /// Adquire caps/subs da definição como recursos rastreados e publica.
    /// Falha parcial desfaz o adquirido (I05). Retorna handles em ordem.
    fn acquire_definition_resources(
        &self,
        ctx: &ContextRecord,
        def: &StoredManifest,
    ) -> Result<Vec<ResourceHandle>, String> {
        let mut acquired: Vec<ResourceHandle> = vec![];
        for c in &def.caps {
            match self.resources.register(
                ctx.context,
                ctx.instance,
                &def.id,
                ctx.generation,
                ResourceKind::Cap { name: c.clone() },
            ) {
                Ok(h) => {
                    let r = ctx.reference();
                    self.caps.provide_instance(c, &r);
                    acquired.push(h);
                }
                Err(e) => {
                    self.rollback_acquired(ctx.instance, &acquired);
                    return Err(e.to_string());
                }
            }
        }
        for t in &def.subs {
            match self.resources.register(
                ctx.context,
                ctx.instance,
                &def.id,
                ctx.generation,
                ResourceKind::Sub { topic: t.clone() },
            ) {
                Ok(h) => {
                    self.bus.subscribe_instance(&def.id, ctx.instance, t);
                    acquired.push(h);
                }
                Err(e) => {
                    self.rollback_acquired(ctx.instance, &acquired);
                    return Err(e.to_string());
                }
            }
        }
        Ok(acquired)
    }

    fn rollback_acquired(&self, inst: InstanceId, acquired: &[ResourceHandle]) {
        for h in acquired.iter().rev() {
            if let Ok(claim) = self.resources.begin_release(*h) {
                match &claim.kind {
                    ResourceKind::Cap { name } => {
                        self.caps.revoke_cap_if_owned(name, inst);
                    }
                    ResourceKind::Sub { topic } => {
                        self.bus.unsubscribe_topic_if_owned(inst, topic);
                    }
                    _ => {}
                }
                crate::resources::ResourceTable::finish_release(claim);
            }
        }
    }

    /// Promove instância `Waiting` → `Active` com os bindings resolvidos:
    /// revalida, adquire/publica e commita em metadados (LIFECYCLE §5).
    fn promote_waiting(&self, logical: &str, def: &StoredManifest, bindings: Vec<Binding>) -> bool {
        let cur = self.contexts.current(logical);
        let Some(st) = cur else { return false };
        if st.state.canonical() != Fsm::Waiting
            && st.state.canonical() != Fsm::Preparing
            && st.state.canonical() != Fsm::Registered
        {
            return false;
        }
        // Revalida bindings contra estados frescos (mudança durante a
        // preparação aborta a tentativa).
        let fresh = self.contexts.current_all();
        let defs = self.definitions.lock().clone();
        let cycle_members: HashSet<String> = HashSet::new();
        match Self::resolve_for(logical, &defs, &fresh, &cycle_members, &[]) {
            Ok(fresh_bindings) => {
                // Usa os bindings frescos (iguais aos passados no caso comum).
                let _ = bindings;
                match self.acquire_definition_resources(&st, def) {
                    Ok(acquired) => {
                        {
                            let mut ps = self.plugins.lock();
                            if let Some(p) = ps.get_mut(logical) {
                                if p.instance_id != st.instance.0 {
                                    // Substituída durante aquisição: desfaz tudo.
                                    drop(ps);
                                    self.rollback_acquired(st.instance, &acquired);
                                    return false;
                                }
                                p.state = Fsm::Active;
                                p.bindings = fresh_bindings.clone();
                                p.wait_cause = None;
                                p.resources = acquired;
                            } else {
                                drop(ps);
                                self.rollback_acquired(st.instance, &acquired);
                                return false;
                            }
                        }
                        if !self.contexts.mark_active(st.instance) {
                            return false;
                        }
                        self.journal.append(
                            logical,
                            "plugin.active",
                            json!({
                                "id": logical,
                                "instance": st.instance.0,
                                "generation": st.generation,
                                "bindings": fresh_bindings.iter().map(|b| json!({
                                    "interface": b.interface,
                                    "provider": b.provider_logical,
                                    "instance": b.provider_instance,
                                    "generation": b.provider_generation,
                                })).collect::<Vec<_>>(),
                            }),
                            Value::Null,
                        );
                        self.emit_activated(logical, st.instance.0, st.generation);
                        true
                    }
                    Err(e) => {
                        let reason = format!("acquire failed: {}", e);
                        self.ensure_waiting(logical, &defs, reason);
                        false
                    }
                }
            }
            Err(e) => {
                self.ensure_waiting(logical, &defs, e.to_string());
                false
            }
        }
    }

    /// Cria e ativa nova instância (sem corrente aproveitável).
    fn create_active(&self, def: &StoredManifest, bindings: Vec<Binding>, json_state: Value) -> bool {
        let ctx = self.contexts.create(&def.id);
        match self.acquire_definition_resources(&ctx, def) {
            Ok(acquired) => {
                let p = Plugin {
                    id: def.id.clone(),
                    state: Fsm::Active,
                    tier: def.tier.clone(),
                    trust: def.trust.clone(),
                    generation: ctx.generation,
                    restart_policy: def.restart.clone(),
                    caps: def.caps.clone(),
                    subs: def.subs.clone(),
                    requires: def.requires.clone(),
                    calls: def.calls.clone(),
                    execution: def.execution.clone(),
                    bindings: bindings.clone(),
                    wait_cause: None,
                    reducer: def.reducer.clone(),
                    init_state: def.init_state.clone(),
                    json_state,
                    leases: vec![],
                    panic_count: 0,
                    restart_times: vec![],
                    instance_id: ctx.instance.0,
                    context_id: ctx.context.0,
                    epoch: ctx.epoch,
                    resources: acquired,
                };
                self.plugins.lock().insert(def.id.clone(), p);
                // `plugin.loaded` é jornalizado pelo `load_manifest` (um por carga);
                // aqui registra-se a transição de ativação com bindings (I12).
                self.journal.append(
                    &def.id,
                    "plugin.active",
                    json!({
                        "id": def.id,
                        "instance": ctx.instance.0,
                        "generation": ctx.generation,
                        "bindings": bindings.iter().map(|b| json!({
                            "interface": b.interface,
                            "provider": b.provider_logical,
                            "instance": b.provider_instance,
                            "generation": b.provider_generation,
                        })).collect::<Vec<_>>(),
                    }),
                    Value::Null,
                );
                self.emit_activated(&def.id, ctx.instance.0, ctx.generation);
                true
            }
            Err(e) => {
                // Falha de aquisição: limpa provisórios e fica Waiting (I05).
                let _ = self.dispose_instance_inner(ctx.instance, false);
                let defs = self.definitions.lock().clone();
                self.ensure_waiting(&def.id, &defs, format!("acquire failed: {}", e));
                false
            }
        }
    }

    /// Motor de reconciliação: retira consumidores antes de provedores,
    /// ativa provedores antes de consumidores, até estabilizar.
    pub fn reconcile(&self) {
        let n = self.definitions.lock().len();
        let max = n * 2 + 4;
        for _ in 0..max {
            if !self.reconcile_step() {
                break;
            }
        }
    }

    fn reconcile_step(&self) -> bool {
        let defs: HashMap<String, StoredManifest> = self.definitions.lock().clone();
        if defs.is_empty() {
            return false;
        }
        let provides = provides_of(&defs);
        let requires = requires_of(&defs);
        let edges = graph_edges(&requires, &provides);

        // 1. Ciclos: membros ficam Waiting com o caminho, nunca Active.
        if let Some(path) = find_cycle(&edges) {
            let members: HashSet<String> = path.iter().cloned().collect();
            let mut changed = false;
            let mut sorted: Vec<&String> = members.iter().collect();
            sorted.sort();
            for m in sorted {
                if !defs.contains_key(m) {
                    continue;
                }
                let reason = ResolveError::Cycle { path: path.clone() }.to_string();
                // Active sai; Waiting atualiza razão; Disposed recria Waiting.
                let cur = self.contexts.current(m);
                match cur.map(|r| r.state.canonical()) {
                    Some(Fsm::Active) | Some(Fsm::Preparing) | Some(Fsm::Registered) | None | Some(Fsm::Disposed) => {
                        if self.withdraw_to_waiting(m, &defs, reason) {
                            changed = true;
                        }
                    }
                    Some(Fsm::Waiting) => {
                        if self.ensure_waiting(m, &defs, reason) {
                            changed = true;
                        }
                    }
                    _ => {}
                }
            }
            return changed;
        }

        let mut order = topo_order(&edges).unwrap_or_default();
        if order.is_empty() {
            order = {
                let mut ks: Vec<String> = defs.keys().cloned().collect();
                ks.sort();
                ks
            };
        }
        let cycle_members: HashSet<String> = HashSet::new();
        let empty_path: Vec<String> = vec![];

        // 2. Retirada: consumidores antes de provedores (ordem reversa).
        let mut withdrew = false;
        // Snapshot para decisão (sem reter locks durante mutações).
        let states = self.contexts.current_all();
        // Consumidores transitivos primeiro: reverso do topo já garante.
        for logical in order.iter().rev() {
            if !defs.contains_key(logical) {
                continue;
            }
            let Some(st) = states.get(logical) else { continue };
            if st.state.canonical() != Fsm::Active {
                continue;
            }
            let stale = self.bindings_stale(logical, &states);
            let resolved = Self::resolve_for(logical, &defs, &states, &cycle_members, &empty_path);
            if resolved.is_err() || stale {
                let reason = match resolved {
                    Err(e) => e.to_string(),
                    Ok(_) => "stale-binding: provedor substituído; religando".to_string(),
                };
                if self.withdraw_to_waiting(logical, &defs, reason) {
                    withdrew = true;
                }
            }
        }
        if withdrew {
            return true;
        }

        // 3. Ativação: provedores antes de consumidores.
        // Re-snapshot após retiradas (que retornariam acima); aqui nada mudou.
        let states = self.contexts.current_all();
        for logical in &order {
            let Some(def) = defs.get(logical).cloned() else { continue };
            if !defs.contains_key(logical) {
                continue;
            }
            let cur = states.get(logical).cloned();
            let cur_state = cur.as_ref().map(|r| r.state.canonical());
            match cur_state {
                Some(Fsm::Active) => {
                    // I04 contínua: bindings ainda válidos?
                    if self.bindings_stale(logical, &states) {
                        if self.withdraw_to_waiting(logical, &defs, "stale-binding: provedor substituído; religando".to_string()) {
                            return true;
                        }
                    }
                    continue;
                }
                Some(Fsm::Failed) | Some(Fsm::CleanupPending) | Some(Fsm::Quiescing) => continue,
                _ => {}
            }
            match Self::resolve_for(logical, &defs, &states, &cycle_members, &empty_path) {
                Ok(bindings) => {
                    let is_waiting = matches!(
                        cur_state,
                        Some(Fsm::Waiting) | Some(Fsm::Preparing) | Some(Fsm::Registered)
                    );
                    if is_waiting {
                        if self.promote_waiting(logical, &def, bindings) {
                            return true;
                        }
                    } else {
                        // Sem corrente aproveitável: nova instância Active.
                        let js = initial_json_state(&def.reducer, &def.init_state);
                        // Preserva counter/clock se houver histórico.
                        let js = self.preserved_json_state(logical, &def, js);
                        if self.create_active(&def, bindings, js) {
                            return true;
                        }
                    }
                }
                Err(e) => {
                    if self.ensure_waiting(logical, &defs, e.to_string()) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Estado preservado de counter/clock através de gerações (compat M1.1).
    fn preserved_json_state(&self, logical: &str, def: &StoredManifest, fresh: Value) -> Value {
        let old = self.plugins.lock().get(logical).map(|p| p.json_state.clone());
        match (def.reducer.as_str(), old) {
            ("counter", Some(s)) if s.get("state").is_some() => s,
            ("clock", Some(s)) if s.get("count").is_some() => s,
            _ => fresh,
        }
    }

    // ---- lifecycle ----

    pub fn load_manifest(&self, path: &PathBuf) -> Result<String, String> {
        let txt = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let v: Value = serde_json::from_str(&txt).map_err(|e| e.to_string())?;
        let mut def = parse_manifest_value(&v)?;
        def.source = Some(path.clone());
        let id = def.id.clone();
        // Preserva counter/clock da geração anterior (compat M1.1).
        let old_state = self.plugins.lock().get(&id).map(|p| (p.reducer.clone(), p.json_state.clone()));
        // Registra/atualiza a definição desejada.
        self.definitions.lock().insert(id.clone(), def.clone());
        // Retira a geração anterior antes de substituir (sem sobreposição).
        if let Some(cur) = self.contexts.current(&id) {
            match cur.state.canonical() {
                Fsm::Disposed | Fsm::Failed | Fsm::CleanupPending => {}
                _ => {
                    let _ = self.dispose_instance_inner(cur.instance, false);
                }
            }
        }
        // Se a definição persiste e a corrente parou com trabalho em voo
        // (CleanupPending), cria a substituta em Waiting agora: o
        // reconciliador não toca em CleanupPending, e sem isto o lógico
        // travaria até um evento externo. Nunca reativa a descartada.
        {
            let cur = self.contexts.current(&id);
            if matches!(cur.map(|r| r.state.canonical()), Some(Fsm::CleanupPending)) {
                let defs = self.definitions.lock().clone();
                if let Some(def) = defs.get(&id) {
                    self.create_waiting_instance(def, "reload: aguardando ativação");
                }
            }
        }
        // Reconcilia: ativa em ordem (ou Waiting com razão visível).
        self.reconcile();
        // Reaplica estado preservado se a nova instância ativou.
        if let Some((old_reducer, old_json)) = old_state {
            if old_reducer == def.reducer {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&id) {
                    match (def.reducer.as_str(), &old_json) {
                        ("counter", s) if s.get("state").is_some() => p.json_state = old_json,
                        ("clock", s) if s.get("count").is_some() => p.json_state = old_json,
                        _ => {}
                    }
                }
            }
        }
        // Journal de carga (compat: um por load bem-sucedido).
        if let Some(cur) = self.contexts.current(&id) {
            self.journal.append(
                &id,
                "plugin.loaded",
                json!({
                    "id": id,
                    "instance": cur.instance.0,
                    "context": cur.context.0,
                    "generation": cur.generation,
                    "epoch": cur.epoch,
                    "state": cur.state.as_str(),
                }),
                Value::Null,
            );
        }
        Ok(id)
    }

    fn dispose_instance_inner(&self, inst: InstanceId, journal_on_idempotent: bool) -> DisposeOutcome {
        let rec = match self.contexts.get_by_instance(inst) {
            Some(r) => r,
            None => return DisposeOutcome::AlreadyDisposed,
        };
        match rec.state.canonical() {
            Fsm::Disposed | Fsm::Failed => return DisposeOutcome::AlreadyDisposed,
            Fsm::CleanupPending => {
                return DisposeOutcome::CleanupPending {
                    reason: rec.cause.unwrap_or_else(|| "cleanup pending".to_string()),
                }
            }
            _ => {}
        }
        // Ponto de linearização: bloqueia novas admissões (LIFECYCLE).
        let rec = match self.contexts.mark_quiescing(inst) {
            Ok(r) => r,
            Err(ContextError::AlreadyDisposed { .. }) => return DisposeOutcome::AlreadyDisposed,
            Err(_) => return DisposeOutcome::AlreadyDisposed,
        };
        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(&rec.logical) {
                if p.instance_id == inst.0 {
                    p.state = Fsm::Quiescing;
                }
            }
        }
        // Fronteira de execução externa: avisa o host na linearização.
        self.emit_hook(LifecycleEvent::Withdrawn {
            logical: rec.logical.clone(),
            instance: inst.0,
            generation: rec.generation,
        });
        // Revoga admissão de imediato (publicações), sem tocar nos registros
        // de propriedade: trabalho em voo segura os recursos (I07), mas nada
        // novo é admitido a partir daqui.
        for h in self.resources.handles_for(rec.context) {
            let Some(rr) = self.resources.record(h) else { continue };
            if rr.owner_context != rec.context {
                continue;
            }
            match &rr.kind {
                ResourceKind::Cap { name } => {
                    self.caps.revoke_cap_if_owned(name, inst);
                }
                ResourceKind::Sub { topic } => {
                    self.bus.unsubscribe_topic_if_owned(inst, topic);
                }
                _ => {}
            }
        }
        // Snapshot fora do lock de metadados; cleanup bloqueante nunca
        // retém lock de metadados (ARCH). Primeiro: revogar tickets.
        let now = std::time::Instant::now();
        let mut drain_until: Option<std::time::Instant> = None;
        for t in self.calls.pending_for(inst) {
            match t.policy.on_withdraw {
                WithdrawPolicy::Cancel => {
                    if self.calls.cancel(t.id, "withdraw") {
                        self.journal.append(
                            &rec.logical,
                            "call.cancelled",
                            json!({"ticket": t.id.0, "reason": "withdraw"}),
                            Value::Null,
                        );
                    }
                }
                WithdrawPolicy::Drain => {
                    let until = now + std::time::Duration::from_millis(t.policy.drain_ms);
                    self.calls.set_drain_until(t.id, until);
                    drain_until = Some(drain_until.map(|u: std::time::Instant| u.max(until)).unwrap_or(until));
                    self.journal.append(
                        &rec.logical,
                        "call.draining",
                        json!({"ticket": t.id.0, "drain_ms": t.policy.drain_ms}),
                        Value::Null,
                    );
                }
            }
        }
        // Drenagem limitada, sem reter locks (condvar acorda no close).
        if let Some(until) = drain_until {
            if until > std::time::Instant::now() {
                self.calls.wait_settled(inst, until);
            }
        }
        // Expira drenagens vencidas (trabalho teimoso vira pendência explícita).
        for id in self.calls.expire_overdue(std::time::Instant::now()) {
            self.journal.append(
                &rec.logical,
                "call.expired",
                json!({"ticket": id.0, "reason": "drain-exceeded"}),
                Value::Null,
            );
        }
        // Trabalho ainda ativo segura CleanupPending; recursos NÃO são
        // liberados prematuramente (I07). Re-checagem pós-marcação fecha a
        // corrida com um close concorrente.
        if self.calls.pending_count(inst) > 0 {
            let pending: Vec<u64> = self.calls.pending_for(inst).iter().map(|t| t.id.0).collect();
            let reason = format!("in-flight calls pending: {:?}", pending);
            self.contexts.mark_cleanup_pending(inst, reason.clone());
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::CleanupPending;
                    }
                }
            }
            self.journal.append(
                &rec.logical,
                "plugin.cleanup_pending",
                json!({
                    "id": rec.logical, "instance": inst.0,
                    "generation": rec.generation, "reason": reason,
                }),
                Value::Null,
            );
            if self.calls.pending_count(inst) == 0 {
                // Um close pousou entre checagem e marcação: finaliza agora.
                self.maybe_finalize(inst);
            }
            let _ = journal_on_idempotent;
            return match self.contexts.get_by_instance(inst).map(|r| r.state.canonical()) {
                Some(Fsm::Disposed) => DisposeOutcome::Disposed,
                _ => DisposeOutcome::CleanupPending {
                    reason: self.contexts.get_by_instance(inst)
                        .and_then(|r| r.cause)
                        .unwrap_or(reason),
                },
            };
        }
        let failures = self.release_context_resources(&rec);
        if failures.is_empty() {
            self.contexts.mark_disposed(inst);
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::Disposed;
                        p.leases.clear();
                    }
                }
            }
            self.journal.append(
                &rec.logical,
                "plugin.unloaded",
                json!({
                    "id": rec.logical,
                    "instance": inst.0,
                    "context": rec.context.0,
                    "generation": rec.generation,
                }),
                Value::Null,
            );
            DisposeOutcome::Disposed
        } else {
            let reason = failures.join("; ");
            self.contexts.mark_cleanup_pending(inst, reason.clone());
            {
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(&rec.logical) {
                    if p.instance_id == inst.0 {
                        p.state = Fsm::CleanupPending;
                    }
                }
            }
            // Incerteza aparece como CleanupPending, nunca falso Disposed (I07).
            self.journal.append(
                &rec.logical,
                "plugin.cleanup_pending",
                json!({
                    "id": rec.logical,
                    "instance": inst.0,
                    "generation": rec.generation,
                    "reason": reason,
                }),
                Value::Null,
            );
            let _ = journal_on_idempotent;
            DisposeOutcome::CleanupPending { reason }
        }
    }

    /// Remoção explícita: apaga a definição, descarta a instância e
    /// retira em cascata os consumidores (que mantêm suas definições em
    /// `Waiting`). Idempotente (I06).
    pub fn dispose_plugin(&self, id: &str) -> DisposeOutcome {
        // Captura afetados antes de remover (para diagnóstico, se preciso).
        let _affected = self.transitive_dependents(id);
        self.definitions.lock().remove(id);
        self.emit_hook(LifecycleEvent::Removed { logical: id.to_string() });
        let out = match self.contexts.current(id) {
            Some(cur) => self.dispose_instance_inner(cur.instance, false),
            None => DisposeOutcome::AlreadyDisposed,
        };
        // Cascata + razões de Waiting atualizadas.
        self.reconcile();
        out
    }

    /// Consumidores transitivos com definição registrada.
    fn transitive_dependents(&self, logical: &str) -> Vec<String> {
        let defs: HashMap<String, StoredManifest> = self.definitions.lock().clone();
        let edges = graph_edges(&requires_of(&defs), &provides_of(&defs));
        transitive_consumers(logical, &edges)
            .into_iter()
            .filter(|l| defs.contains_key(l))
            .collect()
    }

    /// Descarte endereçado por instância: seguro contra geração nova.
    /// Descartar a geração antiga nunca remove recursos da nova (I06/C01).
    /// Não apaga definições; reconcilia dependentes afetados.
    pub fn dispose_instance(&self, inst: InstanceId) -> DisposeOutcome {
        let out = self.dispose_instance_inner(inst, false);
        self.reconcile();
        out
    }

    /// Compat legada: o scaffold chamava sem retorno. Preservada como
    /// adaptador; código novo deve usar o retorno `DisposeOutcome`.
    pub fn dispose_plugin_legacy(&self, id: &str) {
        let _ = self.dispose_plugin(id);
    }

    /// Inventário para inspeção (C02/C19): instâncias, gerações,requires,
    /// bindings, recursos por dono e diagnóstico de cleanup/ciclos.
    pub fn inventory(&self) -> Value {
        let ctxs = self.contexts.snapshot();
        let res = self.resources.inventory();
        let defs: HashMap<String, StoredManifest> = self.definitions.lock().clone();
        let bindings_by_logical: HashMap<String, (u64, Vec<Binding>)> = {
            self.plugins
                .lock()
                .iter()
                .map(|(k, p)| (k.clone(), (p.instance_id, p.bindings.clone())))
                .collect()
        };
        let instances: Vec<Value> = ctxs
            .iter()
            .map(|c| {
                let requires: Vec<String> = defs
                    .get(&c.logical)
                    .map(|d| d.requires.iter().map(|r| {
                        if let Some(p) = &r.provider {
                            format!("{}@{}", r.interface, p)
                        } else {
                            r.interface.clone()
                        }
                    }).collect())
                    .unwrap_or_default();
                let bindings: Vec<Value> = bindings_by_logical
                    .get(&c.logical)
                    .filter(|(inst, _)| *inst == c.instance.0)
                    .map(|(_, bs)| {
                        bs.iter().map(|b| json!({
                            "interface": b.interface,
                            "provider": b.provider_logical,
                            "instance": b.provider_instance,
                            "generation": b.provider_generation,
                        })).collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                json!({
                    "logical": c.logical,
                    "instance": c.instance.0,
                    "context": c.context.0,
                    "generation": c.generation,
                    "epoch": c.epoch,
                    "state": c.state.as_str(),
                    "cause": c.cause,
                    "waiting_reason": c.cause,
                    "requires": requires,
                    "bindings": bindings,
                    "active_resources": self.resources.active_for(c.context),
                })
            })
            .collect();
        let resources: Vec<Value> = res
            .iter()
            .map(|r| {
                json!({
                    "handle": r.handle.0,
                    "kind": r.kind.kind_name(),
                    "label": r.kind.label(),
                    "owner": r.owner_logical,
                    "instance": r.owner_instance.0,
                    "context": r.owner_context.0,
                    "generation": r.generation,
                    "state": match r.state {
                        crate::resources::ResourceState::Active => "Active",
                        crate::resources::ResourceState::Released => "Released",
                    },
                    "seq": r.seq,
                })
            })
            .collect();
        let edges = graph_edges(&requires_of(&defs), &provides_of(&defs));
        let cycle = find_cycle(&edges).unwrap_or_default();
        let calls: Vec<Value> = self
            .calls
            .snapshot()
            .iter()
            .map(|t| {
                json!({
                    "ticket": t.id.0,
                    "cap": t.cap,
                    "logical": t.logical,
                    "instance": t.instance.0,
                    "generation": t.generation,
                    "state": t.state.as_str(),
                    "policy": t.policy.on_withdraw.as_str(),
                    "drain_ms": t.policy.drain_ms,
                    "elapsed_ms": t.elapsed_ms(),
                    "drain_left_ms": t.drain_left_ms(),
                    "pins": t.pins.iter().map(|h| h.0).collect::<Vec<_>>(),
                    "cancel_reason": t.cancel_reason,
                })
            })
            .collect();
        let effects: Vec<Value> = self
            .calls
            .effects_for(None)
            .iter()
            .map(|e| {
                json!({
                    "seq": e.seq,
                    "ticket": e.ticket.0,
                    "logical": e.logical,
                    "instance": e.instance.0,
                    "generation": e.generation,
                    "kind": e.kind,
                    "payload": e.payload,
                })
            })
            .collect();
        let definitions: Vec<Value> = {
            let mut ds: Vec<&StoredManifest> = defs.values().collect();
            ds.sort_by(|a, b| a.id.cmp(&b.id));
            ds.iter().map(|d| json!({
                "id": d.id,
                "provides": d.caps,
                "requires": d.requires.iter().map(|r| json!({
                    "interface": r.interface,
                    "provider": r.provider,
                })).collect::<Vec<_>>(),
            })).collect()
        };
        json!({
            "epoch": self.epoch(),
            "instances": instances,
            "definitions": definitions,
            "cycle": cycle,
            "calls": calls,
            "effects": effects,
            "resources": resources,
            "active_resources": self.resources.active_count(),
            "total_releases": self.resources.total_releases(),
            "double_release_attempts": self.resources.double_release_attempts(),
            "capabilities": self.caps.snapshot(),
        })
    }

    /// Substituição coordenada (M1.4): valida candidatos sem publicá-los,
    /// retira a geração anterior e dependentes, reconcilia manifests
    /// removidos do disco, limpa registros e publica a nova geração.
    /// Candidato inválido não toca na geração vigente (recuperável).
    pub fn reload(&self) -> Result<usize, String> {
        let dir = self.plugins_dir.clone();
        let Ok(rd) = std::fs::read_dir(&dir) else { return Ok(0) };
        let mut files: Vec<PathBuf> = rd
            .flatten()
            .map(|f| f.path())
            .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
            .collect();
        files.sort();
        // Fase 1 — validar candidatos sem publicar (parse puro).
        let mut candidates: Vec<(PathBuf, StoredManifest)> = vec![];
        let mut failed: Vec<String> = vec![];
        for p in &files {
            let txt = match std::fs::read_to_string(p) {
                Ok(t) => t,
                Err(e) => {
                    failed.push(format!("{}: {}", p.display(), e));
                    continue;
                }
            };
            let v: Value = match serde_json::from_str(&txt) {
                Ok(v) => v,
                Err(e) => {
                    failed.push(format!("{}: {}", p.display(), e));
                    continue;
                }
            };
            match parse_manifest_value(&v) {
                Ok(mut def) => {
                    def.source = Some(p.clone());
                    candidates.push((p.clone(), def));
                }
                Err(e) => failed.push(format!("{}: {}", p.display(), e)),
            }
        }
        // Fase 2 — reconciliar removidos e ids renomeados.
        // `present`: arquivos .json no disco (mesmo os que falharam no parse —
        // candidato inválido mantém a geração vigente: recuperável).
        let present: std::collections::HashSet<PathBuf> = files.iter().cloned().collect();
        let mut path_to_id: HashMap<PathBuf, String> = HashMap::new();
        for (p, def) in &candidates {
            // Último arquivo (ordem classificada) vence em caso de id duplicado.
            path_to_id.insert(p.clone(), def.id.clone());
        }
        let stale: Vec<String> = {
            let defs = self.definitions.lock();
            let mut out = vec![];
            for (id, def) in defs.iter() {
                match &def.source {
                    Some(src) if src.starts_with(&dir) => {
                        if !present.contains(src) {
                            // Arquivo sumiu do disco → remove a definição.
                            out.push(id.clone());
                        } else if let Some(parsed) = path_to_id.get(src) {
                            // Arquivo existe e foi parseado declarando outro id.
                            if parsed != id {
                                out.push(id.clone());
                            }
                        }
                    }
                    _ => {}
                }
            }
            out.sort();
            out
        };
        let mut removed = 0usize;
        for id in stale {
            self.dispose_plugin(&id);
            removed += 1;
        }
        // Fase 3 — publicar candidatos (cada load retira a geração anterior).
        let mut n = 0;
        for (p, _) in &candidates {
            if self.load_manifest(p).is_ok() {
                n += 1;
            } else {
                failed.push(format!("{}: activation failed", p.display()));
            }
        }
        self.journal.append(
            "sys",
            "sys.reload",
            json!({"reloaded": n, "removed": removed, "failed": failed}),
            Value::Null,
        );
        Ok(n)
    }

    // ---- dispatch ----

    fn fault(&self, fiber: &str, mode: &str) -> Value {
        let mut failed = false;
        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(fiber) {
                let now = Instant::now();
                p.restart_times.retain(|t| now.duration_since(*t).as_secs() < 10);
                p.restart_times.push(now);
                p.panic_count += 1;
                if p.restart_times.len() > 5 {
                    p.state = Fsm::Failed;
                    failed = true;
                }
            }
        }
        // Espelha falha no contexto da instância corrente.
        if failed {
            if let Some(cur) = self.contexts.current(fiber) {
                self.contexts
                    .mark_failed(cur.instance, format!("fault budget exceeded ({})", mode));
                let mut ps = self.plugins.lock();
                if let Some(p) = ps.get_mut(fiber) {
                    p.state = Fsm::Failed;
                }
            }
        }
        self.journal.append(
            fiber,
            "sys.fault",
            json!({"mode": mode, "contained": true, "failed": failed}),
            Value::Null,
        );
        json!({"contained": true, "mode": mode, "failed": failed})
    }

    pub fn invoke(&self, cap: &str, input: &Value) -> (Value, bool) {
        // Via rápida síncrona sobre ticket efêmero: a pós-validação fecha a
        // corrida com um descarte concorrente (revogada no meio → cancelled).
        let open = match self.call_open_ephemeral(cap, input) {
            Ok(o) => o,
            Err(v) => {
                let ok = false;
                return (v, ok);
            }
        };
        let fiber = open.logical.clone();
        let ticket = open.ticket;
        let (reducer, execution, state_ok) = {
            let ps = self.plugins.lock();
            match ps.get(&fiber) {
                Some(p) if p.state.canonical() == Fsm::Active => {
                    (p.reducer.clone(), p.execution.clone(), true)
                }
                Some(_) => (String::new(), ExecutionKind::InProcess, false),
                None => (String::new(), ExecutionKind::InProcess, false),
            }
        };
        if !state_ok {
            self.calls.remove(ticket);
            let ps = self.plugins.lock();
            let code = match ps.get(&fiber).map(|p| p.state.canonical()) {
                Some(Fsm::Failed) | Some(Fsm::Disposed) => "plugin-not-active",
                Some(Fsm::Quiescing) | Some(Fsm::CleanupPending) => "context-not-active",
                Some(Fsm::Waiting) | Some(Fsm::Preparing) | Some(Fsm::Registered) => "dependency-unavailable",
                _ => "plugin-not-loaded",
            };
            return (json!({"error": "plugin not available", "code": code}), false);
        }
        // Execução externa: entrega ao host (prazo + cancelamento); sem
        // forwarder é erro explícito, nunca dispatch silencioso.
        if let ExecutionKind::External { timeout_ms, .. } = execution {
            let cur = self.contexts.current(&fiber);
            let (instance, generation) = cur
                .map(|c| (c.instance, c.generation))
                .unwrap_or((open.instance, open.generation));
            let fwd = self.forwarder.lock().clone();
            let Some(fwd) = fwd else {
                self.calls.remove(ticket);
                return (json!({"error": "external plugin without host", "code": "internal"}), false);
            };
            let req = ForwardRequest {
                ticket,
                cap: cap.to_string(),
                input: input.clone(),
                logical: fiber.clone(),
                instance,
                generation,
                timeout_ms,
                cancel: open.cancel.clone(),
            };
            let out = fwd.forward(&req);
            // Pós-validação do ticket (revogada no meio → sem sucesso falso).
            let admitted = matches!(self.calls.get(ticket), Some(t) if t.state == TicketState::Admitted);
            self.calls.remove(ticket);
            match out {
                crate::external::ForwardOutcome::Ok(value) if admitted => {
                    if !self.dry_run {
                        self.journal.append(&fiber, "cap.invoke", json!({"cap": cap, "input": input}), Value::Null);
                    }
                    return (value, true);
                }
                crate::external::ForwardOutcome::Ok(_) => {
                    self.journal.append(
                        &fiber,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "revoked-during-forward", "cap": cap}),
                        Value::Null,
                    );
                    return (json!({"error": "ticket revoked", "code": "cancelled"}), false);
                }
                crate::external::ForwardOutcome::Err { code, message } if admitted => {
                    return (json!({"error": message, "code": code}), false);
                }
                crate::external::ForwardOutcome::Err { code, message } => {
                    let _ = code;
                    self.journal.append(
                        &fiber,
                        "call.rejected",
                        json!({"ticket": ticket.0, "reason": "revoked-during-forward", "cap": cap}),
                        Value::Null,
                    );
                    return (json!({"error": message, "code": "cancelled"}), false);
                }
                crate::external::ForwardOutcome::Failed(e) => {
                    return (json!({"error": format!("forward failed: {:?}", e), "code": e.code()}), false);
                }
            }
        }
        if reducer == "crasher" {
            self.calls.remove(ticket);
            let mode = input.get("mode").and_then(|x| x.as_str()).unwrap_or("panic");
            if mode == "panic" {
                let r = std::panic::catch_unwind(|| panic!("injected panic"));
                debug_assert!(r.is_err());
            }
            let info = self.fault(&fiber, mode);
            return (json!({"error": "injected fault", "code": "plugin-panicked", "fault": info}), false);
        }
        let entry = Reducers::dispatch(&reducer, &fiber, cap, input, self);
        let Some(entry) = entry else {
            self.calls.remove(ticket);
            return (json!({"error": "unknown reducer", "code": "reducer-missing"}), false);
        };
        // Pós-validação: descarte linearizado no meio do dispatch invalida.
        match self.calls.get(ticket) {
            Some(t) if t.state == TicketState::Admitted => {}
            _ => {
                self.calls.remove(ticket);
                self.journal.append(
                    &fiber,
                    "call.rejected",
                    json!({"ticket": ticket.0, "reason": "revoked-during-dispatch", "cap": cap}),
                    Value::Null,
                );
                return (json!({"error": "ticket revoked", "code": "cancelled"}), false);
            }
        }
        self.calls.remove(ticket);
        if !self.dry_run {
            self.journal.append(&fiber, "cap.invoke", json!({"cap": cap, "input": input}), Value::Null);
        }
        (entry, true)
    }

    /// Ticket efêmero do `invoke`: sem journal de open/close (a linha
    /// `cap.invoke` continua sendo o registro), mas presente no mapa para a
    /// drenagem observar e revogar em caso de corrida com descarte.
    fn call_open_ephemeral(&self, cap: &str, _input: &Value) -> Result<OpenCall, Value> {
        if self.calls.inflight_count() >= MAX_INFLIGHT_CALLS {
            return Err(json!({"error": "too many in-flight calls", "code": "resource-exhausted"}));
        }
        let (logical, ctx) = self.admission_record(cap).map_err(|(v, _)| v)?;
        let policy = self.policy_for(&logical, cap);
        let cancel = Arc::new(AtomicBool::new(false));
        let id = self.calls.alloc(
            cap, &logical, ctx.instance, ctx.generation, ctx.context,
            ctx.epoch, policy, vec![], cancel.clone(),
        );
        match self.admission_record(cap) {
            Ok((_, fresh)) if fresh.instance == ctx.instance && fresh.generation == ctx.generation => {
                Ok(OpenCall {
                    ticket: id, cap: cap.to_string(), logical,
                    instance: ctx.instance, generation: ctx.generation,
                    context: ctx.context, policy, cancel,
                })
            }
            Ok(_) => {
                self.calls.remove(id);
                Err(json!({"error": "stale generation", "code": "stale-generation", "logical": logical}))
            }
            Err((v, _)) => {
                self.calls.remove(id);
                Err(v)
            }
        }
    }

    pub fn deliver_evt(&self, topic: &str, payload: &Value) {
        // Entrega apenas para instâncias ativas e correntes (I02/I03).
        let targets = self.bus.subscriber_instances(topic);
        let legacy = self.bus.subscribers(topic);
        let mut seen: std::collections::HashSet<u64> = targets.iter().map(|i| i.0).collect();
        for inst in targets {
            if self.contexts.require_active(inst).is_err() {
                continue;
            }
            let logical = self
                .contexts
                .get_by_instance(inst)
                .map(|r| r.logical)
                .unwrap_or_default();
            if logical.is_empty() {
                continue;
            }
            // Somente geração corrente recebe eventos.
            if let Some(cur) = self.contexts.current(&logical) {
                if cur.instance != inst {
                    continue;
                }
            }
            let mut ps = self.plugins.lock();
            let Some(p) = ps.get_mut(&logical) else { continue };
            if p.state.canonical() != Fsm::Active {
                continue;
            }
            if p.reducer == "counter" && topic == "sys.tick" {
                let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"state": n});
            } else if p.reducer == "clock" && topic == "sys.tick" {
                let n = p.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"count": n});
            }
            let _ = payload;
        }
        // Caminho legado (inscrições sem instância, preservado para compat).
        for fiber in legacy {
            let mut ps = self.plugins.lock();
            let Some(p) = ps.get_mut(&fiber) else { continue };
            if p.instance_id != 0 && seen.contains(&p.instance_id) {
                continue;
            }
            if p.instance_id != 0 {
                // Instância nova já tratada acima; legado sem instância (0) segue.
                continue;
            }
            if p.state.canonical() != Fsm::Active {
                continue;
            }
            if p.reducer == "counter" && topic == "sys.tick" {
                let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"state": n});
            } else if p.reducer == "clock" && topic == "sys.tick" {
                let n = p.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"count": n});
            }
            let _ = payload;
            let _ = &mut seen;
        }
    }

    pub fn emit(&self, topic: &str, payload: &Value) -> u64 {
        let seq = self.journal.append("cli", "evt.emit", json!({"topic": topic, "payload": payload}), Value::Null);
        self.deliver_evt(topic, payload);
        seq
    }

    /// Boot = fold do journal (replay). Restaura counter via evt.emit/sys.tick.
    pub fn replay(&self) -> (usize, usize) {
        let entries = crate::journal::Journal::read_all(&self.journal_path);
        let total = entries.len();
        let mut applied = 0;
        for e in &entries {
            if e.kind == "evt.emit" {
                let topic = e.args.get("topic").and_then(|x| x.as_str()).unwrap_or("");
                if topic == "sys.tick" {
                    let mut ps = self.plugins.lock();
                    if let Some(p) = ps.get_mut("counter") {
                        if p.state.canonical() == Fsm::Active {
                            let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                            p.json_state = json!({"state": n});
                            applied += 1;
                        }
                    }
                }
            }
        }
        (applied, total)
    }

    pub fn reset_bench(&self) {
        let _ = self.journal.reset();
        let ids: Vec<String> = self.plugins.lock().keys().cloned().collect();
        for id in &ids {
            if id.starts_with("gen") {
                self.definitions.lock().remove(id);
                if let Some(cur) = self.contexts.current(id) {
                    let _ = self.dispose_instance_inner(cur.instance, false);
                }
            }
        }
        // Limpa timers/tasks adquiridos por testes; caps/subs do baseline ficam.
        let inv = self.resources.inventory();
        for r in inv {
            match r.kind {
                ResourceKind::Timer { .. } | ResourceKind::Task { .. } => {
                    let _ = self.release(r.handle);
                }
                _ => {}
            }
        }
        {
            let mut ps = self.plugins.lock();
            for p in ps.values_mut() {
                // Só normaliza instâncias Active; Waiting/Disposed/Failed e
                // estados de retirada não são ressuscitados aqui.
                if p.state.canonical() != Fsm::Active {
                    continue;
                }
                p.panic_count = 0;
                p.restart_times.clear();
                if p.reducer == "counter" {
                    p.json_state = json!({"state": 0});
                } else if p.reducer == "clock" {
                    p.json_state = json!({"count": 0});
                } else if p.reducer != "crasher" {
                    p.json_state = p.init_state.clone();
                }
            }
        }
        // Reativa contextos Active correspondentes e reconcilia o resto.
        for c in self.contexts.snapshot() {
            if c.state.canonical() == Fsm::Active {
                self.contexts.set_state(c.instance, Fsm::Active);
            }
        }
        self.reconcile();
    }

    /// Retirada antiga sem retorno (compat com scaffold). Delegada ao
    /// descarte idempotente; mantida para chamadas externas legadas.
    pub fn dispose_plugin_unchecked(&self, id: &str) {
        let _ = self.dispose_plugin(id);
    }

    // APIs de diagnóstico para M1.3+ (tickets/cancelamento).
    pub fn active_resources_for(&self, logical: &str) -> usize {
        self.contexts
            .current(logical)
            .map(|c| self.resources.active_for(c.context))
            .unwrap_or(0)
    }

    pub fn context_state_of(&self, logical: &str) -> Option<String> {
        self.contexts
            .current(logical)
            .map(|c| c.state.as_str().to_string())
    }
}

// Mantém compat com chamadas antigas `dispose_plugin` sem retorno em
// código externo que ignore `DisposeOutcome`: o método acima retorna o
// outcome; este bloco apenas documenta que o retorno deve ser observado
// em código novo.
impl Kernel {
    #[allow(dead_code)]
    fn _note_idempotent_dispose(&self) {
        let _ = ContextId(0);
        let _ = InstanceId(0);
    }
}

fn initial_json_state(reducer: &str, init_state: &Value) -> Value {
    match reducer {
        "counter" => json!({"state": 0}),
        "clock" => json!({"count": 0}),
        _ => init_state.clone(),
    }
}

// ---- reducers puros (descrevem efeitos como dados) ----

struct Reducers;

impl Reducers {
    fn dispatch(reducer: &str, fiber: &str, cap: &str, input: &Value, k: &Kernel) -> Option<Value> {
        match reducer {
            "echo" => Some(json!({"echo": input})),
            "ancient" => {
                let _ = (fiber, cap);
                Some(json!({"out": 42}))
            }
            "counter" => {
                let ps = k.plugins.lock();
                let n = ps.get(fiber)?.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0);
                Some(json!({"state": n}))
            }
            "clock" => {
                let ps = k.plugins.lock();
                let n = ps.get(fiber)?.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
                Some(json!({"count": n}))
            }
            "model" => Some(json!({"text": "canned-response", "in": input})),
            "noop" => Some(json!({"ok": true})),
            _ => None,
        }
    }
}
