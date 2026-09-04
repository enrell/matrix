//! Lifecycle FSM: Pending→Loading→Active→Failed→Unloading→Disposed (+PREPARING).
//! Cf. agentlab SPEC controlada + `master3/src/kernel/fsm.rs`.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fsm {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
    Preparing,
}

impl Fsm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Fsm::Pending => "Pending",
            Fsm::Loading => "Loading",
            Fsm::Active => "Active",
            Fsm::Failed => "Failed",
            Fsm::Unloading => "Unloading",
            Fsm::Disposed => "Disposed",
            Fsm::Preparing => "Preparing",
        }
    }
}
