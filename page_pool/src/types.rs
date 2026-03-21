use crate::error::ConfigError;
use crate::layout::compute_layout_from_config;
use std::num::{NonZeroU32, NonZeroUsize};

/// Static configuration for a [`crate::PagePool`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PagePoolConfig {
    /// Fixed size of each page in bytes.
    pub page_size: NonZeroUsize,
    /// Total number of pages managed by the pool.
    pub page_count: NonZeroU32,
}

impl PagePoolConfig {
    /// Validate and construct a page-pool configuration.
    ///
    /// Validation checks the full region layout, including alignment and
    /// overflow constraints.
    pub fn new(page_size: usize, page_count: u32) -> Result<Self, ConfigError> {
        let page_size = NonZeroUsize::new(page_size).ok_or(ConfigError::ZeroPageSize)?;
        let page_count = NonZeroU32::new(page_count).ok_or(ConfigError::ZeroPageCount)?;
        let cfg = Self {
            page_size,
            page_count,
        };
        compute_layout_from_config(cfg)?;
        Ok(cfg)
    }
}

/// Required size and alignment for a page-pool memory region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionLayout {
    /// Total number of bytes required for the region.
    pub size: usize,
    /// Required alignment for the region base address.
    pub align: usize,
}

/// Detached ownership token for a single page.
///
/// A descriptor is the unit that may be passed across process boundaries after
/// [`crate::PageLease::into_descriptor`]. It is not just an identifier: the
/// `(pool_id, page_id, generation)` tuple is validated against the shared page
/// state on every detached access and release.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageDescriptor {
    /// Shared pool instance identity.
    pub pool_id: u64,
    /// Stable page slot index within the pool.
    pub page_id: u32,
    /// Monotonic generation for this page slot.
    pub generation: u64,
}

/// Shared metrics snapshot for a page pool.
///
/// `page_size`, `page_count`, `free_pages`, `leased_pages`, `exhausted_pages`,
/// and `high_watermark_pages` are gauges. The remaining fields are monotonically
/// increasing counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSnapshot {
    /// Configured fixed page size in bytes.
    pub page_size: usize,
    /// Configured page count.
    pub page_count: u32,
    /// Pages currently available for acquisition.
    pub free_pages: u64,
    /// Pages currently owned by an attached lease or detached descriptor.
    pub leased_pages: u64,
    /// Pages permanently retired because their generation space was exhausted.
    pub exhausted_pages: u64,
    /// Maximum observed concurrent leased pages.
    pub high_watermark_pages: u64,
    /// Successful acquires.
    pub acquire_ok: u64,
    /// Failed acquires because the pool was empty.
    pub acquire_empty: u64,
    /// Successful releases back to the freelist.
    pub release_ok: u64,
    /// Releases that retired a page into the exhausted state.
    pub release_exhausted: u64,
    /// Release attempts with an out-of-range page id.
    pub release_bad_page: u64,
    /// Release attempts with a stale generation.
    pub release_stale_generation: u64,
    /// Release attempts against a still-attached live lease.
    pub release_still_attached: u64,
    /// Release attempts for pages that were not currently leased.
    pub release_not_leased: u64,
    /// CAS retry loops spent in acquisition.
    pub acquire_retry_loops: u64,
    /// CAS retry loops spent in release.
    pub release_retry_loops: u64,
}
