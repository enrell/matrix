//! matrix-core — kernel lib (síntese v3, reescrita).
//!
//! Conceitos derivados de agentlab `master3` (cf. `master3/docs/DESIGN.md`):
//! D-core (reducers + journal + replay), B (envelope `v`, rlimits — aqui só
//! o contrato), C (vocabulário OTP), A (Store-per-call — roadmap wasm).
//! Nenhum código copiado; só o mapa de trade-offs.

pub mod bus;
pub mod envelope;
pub mod fsm;
pub mod journal;
pub mod kernel;
pub mod leases;
pub mod registry;

pub use envelope::PROTO_V;
pub use journal::Journal;
pub use kernel::Kernel;
