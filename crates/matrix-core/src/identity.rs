//! Identidades emitidas pelo kernel (M1.1).
//!
//! Contrato (`docs/CONTRACT.md`): identidades são emitidas pelo kernel.
//! A referência completa inclui época do kernel, id da instância e
//! geração. Reutilizar um id lógico nunca revalida uma referência antiga.
//!
//! - `KernelEpoch`: época do kernel (nova a cada boot; futura base de fencing).
//! - `InstanceId`: ativação concreta, única por kernel (nunca reutilizada).
//! - `ContextId`: escopo de autoridade/propriedade (M1.1: 1:1 com instância).
//! - `Generation`: época monotônica de substituição dentro do id lógico.
//! - `ResourceHandle`: handle opaco de recurso (nunca reutilizado).
//! - `InstanceRef`: referência completa verificável na fronteira do efeito.

use std::fmt;
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KernelEpoch(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InstanceId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResourceHandle(pub u64);

impl fmt::Display for KernelEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e{}", self.0)
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "inst-{}", self.0)
    }
}

impl fmt::Display for ContextId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ctx-{}", self.0)
    }
}

impl fmt::Display for ResourceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "res-{}", self.0)
    }
}

/// Referência completa de uma ativação. É o que deve ser validado na
/// fronteira que controla o efeito (I03): geração obsoleta é rejeitada
/// mesmo que o id lógico tenha sido reutilizado.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstanceRef {
    pub epoch: u64,
    pub instance: u64,
    pub context: u64,
    pub logical: String,
    pub generation: u64,
}

impl InstanceRef {
    pub fn new(epoch: u64, instance: u64, context: u64, logical: &str, generation: u64) -> Self {
        Self {
            epoch,
            instance,
            context,
            logical: logical.to_string(),
            generation,
        }
    }

    /// Chave estável para logs/diagnóstico (I12).
    pub fn describe(&self) -> String {
        format!(
            "e{}:{}:{}:{}#{}",
            self.epoch, self.logical, self.instance, self.context, self.generation
        )
    }

    pub fn same_activation(&self, other: &InstanceRef) -> bool {
        self.epoch == other.epoch
            && self.instance == other.instance
            && self.generation == other.generation
    }
}

impl fmt::Display for InstanceRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.describe())
    }
}

/// Random boot identity. Durable ordering belongs to the managed store's
/// monotonic fencing counters; boot identity itself must not collide when two
/// kernels start in the same process/millisecond. Entropy failure fails closed.
pub fn fresh_epoch() -> u64 {
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .expect("kernel boot requires OS entropy");
    u64::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_generation_never_revalidates() {
        let a = InstanceRef::new(1, 7, 7, "workspace", 1);
        let b = InstanceRef::new(1, 8, 8, "workspace", 2);
        assert_ne!(a.logical, "".to_string());
        assert!(!a.same_activation(&b));
        // Mesmo id lógico, gerações distintas: referências diferentes.
        assert_eq!(a.logical, b.logical);
        assert_ne!(a.generation, b.generation);
    }
}
