#![doc = include_str!("../README.md")]

mod bloom;
mod pool;
mod shared;

#[cfg(test)]
mod tests;

pub use bloom::{
    runtime_filter_layout, runtime_filter_ptrs, AtomicBloomRef, BloomAttachError, BloomParamError,
    BloomParams, RuntimeFilterLayout,
};
pub use pool::{
    RuntimeFilterBuildHandle, RuntimeFilterKeyType, RuntimeFilterPool,
    RuntimeFilterPoolAttachError, RuntimeFilterPoolConfig, RuntimeFilterPoolLayout,
    RuntimeFilterProbeHandle, RuntimeFilterTarget, RUNTIME_FILTER_POOL_VERSION,
};
pub use shared::{
    pack_lifecycle_word, unpack_lifecycle_word, LifecycleError, LifecycleSnapshot, ProbeDecision,
    RuntimeFilterBuilder, RuntimeFilterHeader, RuntimeFilterProbe, RuntimeFilterSlot,
    RuntimeFilterState,
};

/// Hash an integer join key using the current pg_fusion runtime-filter
/// contract.
///
/// Runtime filters store already-hashed keys. Build and probe code must use the
/// same helper for the same logical key type; this function is intentionally
/// simple while runtime filters support only integer keys.
#[inline]
pub fn hash_int_key(value: i64) -> u64 {
    value as u64
}
