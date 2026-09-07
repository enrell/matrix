//! matrix-core — kernel lib (M1.1: contexts, identity, and real resources).
//!
//! Concepts derived from agentlab `master3` (cf. `master3/docs/DESIGN.md`):
//! D-core (reducers + journal + replay), B (`v` envelope, rlimits — contract
//! only here), C (OTP vocabulary), A (store-per-call — wasm roadmap).
//! No copied code; only the trade-off map.

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
    CallPolicy, CallsTable, CommittedEffect, DepChild, TicketId, TicketRecord, TicketState,
    WithdrawPolicy, MAX_EFFECTS, MAX_INFLIGHT_CALLS,
};
pub use external::{
    CallForwarder, ExecutionKind, ForwardError, ForwardOutcome, ForwardRequest, LifecycleEvent,
    LifecycleHook, expand_args,
};
pub use fsm::Fsm;

pub use context::{ContextRecord, ContextTable, DisposeClaim};
pub use deps::{Binding, Requirement, ResolveError};
pub use envelope::PROTO_V;
pub use identity::{ContextId, InstanceId, InstanceRef, ResourceHandle};
pub use journal::Journal;
pub use kernel::{
    DepAdmit, DepBinding, DepDeny, DisposeOutcome, EventSink, Kernel, OutboundLimits,
    OutboundPolicy, StoredManifest, MAX_EXTERNAL_RESOURCES_PER_CONTEXT,
};
pub use resources::{ResourceKind, ResourceState};
