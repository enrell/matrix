//! Lifecycle FSM alinhada a `docs/LIFECYCLE.md` (M1).
//!
//! Fluxo normal: Registered → Waiting | Preparing → Active → Quiescing
//!   → CleanupPending → Disposed. Falha na preparação limpa provisórios
//!   antes de Failed. Incerteza fixa CleanupPending, nunca falso Disposed.
//!
//! Variantes legadas `Pending`/`Loading`/`Unloading` são mantidas como
//! aliases para adaptadores externos; código novo deve usar os estados
//! normativos.

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
    /// Legado: equivale a `Registered`.
    Pending,
    /// Legado: equivale a `Preparing`.
    Loading,
    /// Legado: equivale a `Quiescing`.
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

    /// Estados que admitem novas chamadas (I02). Só `Active` admite.
    pub fn admits_calls(&self) -> bool {
        matches!(self, Fsm::Active)
    }

    /// Normaliza aliases legados para o estado normativo correspondente.
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
