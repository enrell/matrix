//! Lifecycle FSM aligned with `docs/LIFECYCLE.md` (M1).
//!
//! Normal flow: Registered → Waiting | Preparing → Active → Quiescing
//!   → CleanupPending → Disposed. Preparation failure cleans up provisional
//!   resources before Failed. Uncertainty pins CleanupPending, never a false Disposed.
//!
//! Legacy `Pending`/`Loading`/`Unloading` variants are kept as
//! aliases for external adapters; new code must use the normative
//! states.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Fsm {
    Registered,
    Waiting,
    Preparing,
    Active,
    Quiescing,
    CleanupPending,
    Disposed,
    Failed,
    /// Legacy: equivalent to `Registered`.
    Pending,
    /// Legacy: equivalent to `Preparing`.
    Loading,
    /// Legacy: equivalent to `Quiescing`.
    Unloading,
}

impl Fsm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Fsm::Registered => "Registered",
            Fsm::Waiting => "Waiting",
            Fsm::Preparing => "Preparing",
            Fsm::Active => "Active",
            Fsm::Quiescing => "Quiescing",
            Fsm::CleanupPending => "CleanupPending",
            Fsm::Disposed => "Disposed",
            Fsm::Failed => "Failed",
            Fsm::Pending => "Pending",
            Fsm::Loading => "Loading",
            Fsm::Unloading => "Unloading",
        }
    }

    /// States that admit new calls (I02). Only `Active` admits.
    pub fn admits_calls(&self) -> bool {
        matches!(self, Fsm::Active)
    }

    /// Normalizes legacy aliases to the corresponding normative state.
    pub fn canonical(&self) -> Fsm {
        match self {
            Fsm::Pending => Fsm::Registered,
            Fsm::Loading => Fsm::Preparing,
            Fsm::Unloading => Fsm::Quiescing,
            s => *s,
        }
    }

    pub fn is_terminal_like(&self) -> bool {
        matches!(
            self.canonical(),
            Fsm::Disposed | Fsm::Failed | Fsm::CleanupPending
        )
    }
}
