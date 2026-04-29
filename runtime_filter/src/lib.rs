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

#[inline]
pub fn hash_int_key(value: i64) -> u64 {
    value as u64
}
