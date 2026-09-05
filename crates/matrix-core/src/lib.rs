//! matrix-core — kernel lib (M1.1: contextos, identidade e recursos reais).
//!
//! Conceitos derivados de agentlab `master3` (cf. `master3/docs/DESIGN.md`):
//! D-core (reducers + journal + replay), B (envelope `v`, rlimits — aqui só
//! o contrato), C (vocabulário OTP), A (Store-per-call — roadmap wasm).
//! Nenhum código copiado; só o mapa de trade-offs.

pub mod bus;
pub mod calls;
pub mod context;
pub mod deps;
pub mod envelope;
pub mod external;
pub mod fsm;
pub mod identity;
pub mod journal;
pub mod kernel;
pub mod leases;
pub mod registry;
pub mod resources;

pub use calls::{
    CallPolicy, CallsTable, CommittedEffect, TicketId, TicketRecord, TicketState, WithdrawPolicy,
    MAX_EFFECTS, MAX_INFLIGHT_CALLS,
};
pub use external::{
    CallForwarder, ExecutionKind, ForwardError, ForwardOutcome, ForwardRequest, LifecycleEvent,
    LifecycleHook, expand_args,
};
pub use fsm::Fsm;

pub use context::{ContextRecord, ContextTable};
pub use deps::{Binding, Requirement, ResolveError};
pub use envelope::PROTO_V;
pub use identity::{ContextId, InstanceId, InstanceRef, ResourceHandle};
pub use journal::Journal;
pub use kernel::{DisposeOutcome, Kernel, StoredManifest};
pub use resources::{ResourceKind, ResourceState};
