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
    RuntimeFilterBuildHandle, RuntimeFilterExecId, RuntimeFilterKeyType, RuntimeFilterPool,
    RuntimeFilterPoolAttachError, RuntimeFilterPoolConfig, RuntimeFilterPoolLayout,
    RuntimeFilterProbeHandle, RuntimeFilterTarget, RUNTIME_FILTER_POOL_VERSION,
};
pub use shared::{
    pack_lifecycle_word, unpack_lifecycle_word, LifecycleError, LifecycleSnapshot, ProbeDecision,
    RuntimeFilterBuilder, RuntimeFilterHeader, RuntimeFilterProbe, RuntimeFilterSlot,
    RuntimeFilterState,
};

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Hash a boolean join key using the pg_fusion runtime-filter contract.
///
/// Runtime filters store already-hashed keys. Build and probe code must use the
/// same helper for the same logical key type. The Bloom implementation applies
/// its own splitmix step, so these helpers focus on type-stable logical
/// normalization rather than full hash diffusion.
#[inline]
pub fn hash_bool_key(value: bool) -> u64 {
    u64::from(value)
}

/// Hash an integer join key using the pg_fusion runtime-filter contract.
#[inline]
pub fn hash_int_key(value: i64) -> u64 {
    value as u64
}

/// Hash an Arrow Decimal128/PostgreSQL numeric join key using the pg_fusion
/// runtime-filter contract.
///
/// Arrow stores decimals as scaled integers, so equal numeric values can have
/// different raw integers when their scales differ. Runtime filters hash the
/// canonical finite decimal value instead: trailing base-10 zeros are stripped
/// from the scaled integer while scale remains positive, then both the
/// normalized integer and normalized scale enter the hash.
#[inline]
pub fn hash_decimal128_key(mut value: i128, mut scale: i8) -> u64 {
    while scale > 0 && value % 10 == 0 {
        value /= 10;
        scale -= 1;
    }

    let mut bytes = [0_u8; 17];
    bytes[..16].copy_from_slice(&value.to_le_bytes());
    bytes[16] = scale as u8;
    hash_bytes_key(&bytes)
}

/// Hash an Arrow `Interval(MonthDayNano)` join key using the pg_fusion
/// runtime-filter contract.
///
/// This intentionally hashes the exact Arrow triple. PostgreSQL calendar
/// equivalences such as `1 month` versus `30 days` are not normalized here.
#[inline]
pub fn hash_interval_month_day_nano_key(months: i32, days: i32, nanoseconds: i64) -> u64 {
    let mut bytes = [0_u8; 16];
    bytes[..4].copy_from_slice(&months.to_le_bytes());
    bytes[4..8].copy_from_slice(&days.to_le_bytes());
    bytes[8..16].copy_from_slice(&nanoseconds.to_le_bytes());
    hash_bytes_key(&bytes)
}

/// Hash a `float4` join key using the pg_fusion runtime-filter contract.
///
/// `+0.0` and `-0.0` are normalized to the same hash and all NaNs are
/// canonicalized so build and probe agree on logically equivalent values.
#[inline]
pub fn hash_float32_key(value: f32) -> u64 {
    canonical_f32_bits(value) as u64
}

/// Hash a `float8` join key using the pg_fusion runtime-filter contract.
///
/// `+0.0` and `-0.0` are normalized to the same hash and all NaNs are
/// canonicalized so build and probe agree on logically equivalent values.
#[inline]
pub fn hash_float64_key(value: f64) -> u64 {
    canonical_f64_bits(value)
}

/// Hash a UTF-8/text-like join key using the pg_fusion runtime-filter contract.
#[inline]
pub fn hash_bytes_key(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[inline]
fn canonical_f32_bits(value: f32) -> u32 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

#[inline]
fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}
