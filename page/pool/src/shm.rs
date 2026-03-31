use std::sync::atomic::AtomicU64;

pub(crate) const PAGE_ALIGN: usize = 64;
pub(crate) const PAGE_POOL_MAGIC: u64 = 0x5041_4745_504f_4f4c;
pub(crate) const PAGE_POOL_VERSION: u32 = 3;
pub(crate) const STATE_SHIFT: u64 = 62;
pub(crate) const STATE_MASK: u64 = 0b11 << STATE_SHIFT;
pub(crate) const GENERATION_MASK: u64 = !STATE_MASK;
pub(crate) const MAX_GENERATION: u64 = GENERATION_MASK;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum LeaseState {
    Free = 0,
    Attached = 1,
    Detached = 2,
    Exhausted = 3,
}

impl LeaseState {
    pub(crate) fn from_raw(raw_state: u8) -> Option<Self> {
        match raw_state {
            0 => Some(Self::Free),
            1 => Some(Self::Attached),
            2 => Some(Self::Detached),
            3 => Some(Self::Exhausted),
            _ => None,
        }
    }
}

#[repr(C)]
pub(crate) struct PoolHeader {
    pub(crate) magic: u64,
    pub(crate) version: u32,
    pub(crate) _reserved0: u32,
    pub(crate) page_size: u64,
    pub(crate) page_count: u32,
    pub(crate) _reserved1: u32,
    pub(crate) region_size: u64,
    pub(crate) pool_id: u64,
}

#[repr(C)]
pub(crate) struct SharedMetrics {
    pub(crate) acquire_ok: AtomicU64,
    pub(crate) acquire_empty: AtomicU64,
    pub(crate) release_ok: AtomicU64,
    pub(crate) release_exhausted: AtomicU64,
    pub(crate) release_bad_page: AtomicU64,
    pub(crate) release_stale_generation: AtomicU64,
    pub(crate) release_still_attached: AtomicU64,
    pub(crate) release_not_leased: AtomicU64,
    pub(crate) acquire_retry_loops: AtomicU64,
    pub(crate) release_retry_loops: AtomicU64,
    pub(crate) leased_pages: AtomicU64,
    pub(crate) exhausted_pages: AtomicU64,
    pub(crate) high_watermark_pages: AtomicU64,
}

impl SharedMetrics {
    pub(crate) fn zeroed() -> Self {
        Self {
            acquire_ok: AtomicU64::new(0),
            acquire_empty: AtomicU64::new(0),
            release_ok: AtomicU64::new(0),
            release_exhausted: AtomicU64::new(0),
            release_bad_page: AtomicU64::new(0),
            release_stale_generation: AtomicU64::new(0),
            release_still_attached: AtomicU64::new(0),
            release_not_leased: AtomicU64::new(0),
            acquire_retry_loops: AtomicU64::new(0),
            release_retry_loops: AtomicU64::new(0),
            leased_pages: AtomicU64::new(0),
            exhausted_pages: AtomicU64::new(0),
            high_watermark_pages: AtomicU64::new(0),
        }
    }
}

#[inline]
pub(crate) fn pack_state(generation: u64, state: LeaseState) -> u64 {
    debug_assert!(generation <= MAX_GENERATION);
    (generation & GENERATION_MASK) | ((state as u64) << STATE_SHIFT)
}

#[inline]
pub(crate) fn unpack_state(value: u64) -> (u64, u8) {
    (
        value & GENERATION_MASK,
        ((value & STATE_MASK) >> STATE_SHIFT) as u8,
    )
}
