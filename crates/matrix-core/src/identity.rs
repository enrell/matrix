//! Kernel-issued identities (M1.1).
//!
//! Contract (`docs/CONTRACT.md`): identities are issued by the kernel.
//! The full reference includes the kernel epoch, the instance id, and the
//! generation. Reusing a logical id never revalidates an old reference.
//!
//! - `KernelEpoch`: kernel epoch (fresh on each boot; future fencing base).
//! - `InstanceId`: concrete activation, unique per kernel (never reused).
//! - `ContextId`: authority/ownership scope (M1.1: 1:1 with instance).
//! - `Generation`: monotonic replacement epoch within the logical id.
//! - `ResourceHandle`: opaque resource handle (never reused).
//! - `InstanceRef`: full reference verifiable at the effect boundary.

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

/// Full reference of an activation. This is what must be validated at the
/// effect-controlling boundary (I03): a stale generation is rejected
/// even if the logical id was reused.
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

    /// Stable key for logs/diagnostics (I12).
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
        // Same logical id, distinct generations: different references.
        assert_eq!(a.logical, b.logical);
        assert_ne!(a.generation, b.generation);
    }
}
