//! Kernel-owned lease table: FDs, child procs, wasm Stores, timers, subs.
//! Dispose revoga TUDO unilateralmente (SPEC §2.5; S11 leak-free).

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseKind {
    Fd,
    Child,
    Timer,
    Sub,
    Cap,
}

#[derive(Debug, Clone)]
pub struct Lease {
    pub kind: LeaseKind,
    pub label: String,
}

impl Lease {
    pub fn new(kind: LeaseKind, label: impl Into<String>) -> Self {
        Self { kind, label: label.into() }
    }
}
