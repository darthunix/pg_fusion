use crate::error::{
    AccessError, AcquireError, AttachError, ConfigError, DetachError, InitError, ReleaseError,
};
use crate::layout::{
    compute_layout_from_config, compute_layout_from_values, validate_region, ComputedLayout,
};
use crate::lease::PageLease;
use crate::shm::{
    pack_state, unpack_state, LeaseState, PoolHeader, SharedMetrics, MAX_GENERATION,
    PAGE_POOL_MAGIC, PAGE_POOL_VERSION,
};
use crate::types::{PageDescriptor, PagePoolConfig, PoolSnapshot, RegionLayout};
use lockfree::{StackError, TreiberStack, TreiberStackHeader};
use std::ptr::NonNull;
use std::slice;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// Process-local handle to a shared-memory page pool.
///
/// The handle contains raw pointers into a shared region, but those pointers
/// are not written into shared memory. Multiple processes may attach their own
/// local handles to the same initialized region.
#[derive(Clone, Copy)]
pub struct PagePool {
    metrics: NonNull<SharedMetrics>,
    states: NonNull<AtomicU64>,
    freelist_header: NonNull<TreiberStackHeader>,
    freelist_next: NonNull<AtomicU32>,
    data: NonNull<u8>,
    pool_id: u64,
    page_size: usize,
    page_count: u32,
    region_size: usize,
}

// Safety: the handle only stores pointers to shared memory containing atomics.
unsafe impl Send for PagePool {}
unsafe impl Sync for PagePool {}

impl PagePool {
    /// Compute the required size and alignment for a pool region.
    pub fn layout(cfg: PagePoolConfig) -> Result<RegionLayout, ConfigError> {
        let computed = compute_layout_from_config(cfg)?;
        Ok(RegionLayout {
            size: computed.region.size(),
            align: computed.region.align(),
        })
    }

    /// # Safety
    /// - `base` must point to a writable memory region of at least `len` bytes.
    /// - The region must satisfy [`PagePool::layout`] for `cfg`.
    /// - Initialization must happen exactly once before concurrent access.
    /// - The region must remain valid for the lifetime of the returned handle.
    pub unsafe fn init_in_place(
        base: NonNull<u8>,
        len: usize,
        cfg: PagePoolConfig,
    ) -> Result<Self, InitError> {
        let computed = compute_layout_from_config(cfg).map_err(InitError::InvalidConfig)?;
        validate_region(base, len, computed.region.align(), computed.region.size()).map_err(
            |(expected, actual, aligned)| {
                if aligned {
                    InitError::RegionTooSmall { expected, actual }
                } else {
                    InitError::BadAlignment { expected, actual }
                }
            },
        )?;

        let base_ptr = base.as_ptr();
        let header_ptr = base_ptr.cast::<PoolHeader>();
        let metrics_ptr = base_ptr
            .add(computed.metrics_offset)
            .cast::<SharedMetrics>();
        let states_ptr = base_ptr.add(computed.states_offset).cast::<AtomicU64>();
        let freelist_base = base_ptr.add(computed.freelist_offset);
        let freelist_header_ptr = freelist_base.cast::<TreiberStackHeader>();
        let freelist_next_ptr = base_ptr
            .add(computed.freelist_next_offset)
            .cast::<AtomicU32>();
        let data_ptr = base_ptr.add(computed.data_offset);
        let pool_id = fresh_pool_id(base, len, cfg);

        std::ptr::write(metrics_ptr, SharedMetrics::zeroed());
        for page_id in 0..cfg.page_count.get() as usize {
            std::ptr::write(
                states_ptr.add(page_id),
                AtomicU64::new(pack_state(0, LeaseState::Free)),
            );
        }
        let _ = TreiberStack::init_in_place(
            freelist_header_ptr,
            freelist_next_ptr,
            cfg.page_count.get() as usize,
        );
        std::ptr::write_bytes(data_ptr, 0, computed.data_size);
        std::ptr::write(
            header_ptr,
            PoolHeader {
                magic: PAGE_POOL_MAGIC,
                version: PAGE_POOL_VERSION,
                _reserved0: 0,
                page_size: cfg.page_size.get() as u64,
                page_count: cfg.page_count.get(),
                _reserved1: 0,
                region_size: computed.region.size() as u64,
                pool_id,
            },
        );

        Ok(build_handle(
            base,
            pool_id,
            cfg.page_size.get(),
            cfg.page_count.get(),
            computed,
        ))
    }

    /// Attach a process-local handle to an already initialized region.
    ///
    /// This validates the shared header, version, and derived layout before
    /// constructing the returned handle.
    /// # Safety
    /// - `base` must point to a previously initialized page-pool region.
    /// - The region must remain valid for the lifetime of the returned handle.
    pub unsafe fn attach(base: NonNull<u8>, len: usize) -> Result<Self, AttachError> {
        let header_align = std::mem::align_of::<PoolHeader>();
        if len < std::mem::size_of::<PoolHeader>() {
            return Err(AttachError::RegionTooSmall {
                expected: std::mem::size_of::<PoolHeader>(),
                actual: len,
            });
        }
        if (base.as_ptr() as usize) % header_align != 0 {
            return Err(AttachError::BadAlignment {
                expected: header_align,
                actual: base.as_ptr() as usize,
            });
        }

        let header = &*base.as_ptr().cast::<PoolHeader>();
        if header.magic != PAGE_POOL_MAGIC {
            return Err(AttachError::BadMagic {
                expected: PAGE_POOL_MAGIC,
                actual: header.magic,
            });
        }
        if header.version != PAGE_POOL_VERSION {
            return Err(AttachError::UnsupportedVersion {
                expected: PAGE_POOL_VERSION,
                actual: header.version,
            });
        }

        let page_size = usize::try_from(header.page_size)
            .map_err(|_| AttachError::InvalidConfig(ConfigError::LayoutOverflow))?;
        let page_count = header.page_count;
        let computed = compute_layout_from_values(page_size, page_count)
            .map_err(AttachError::InvalidConfig)?;
        if header.region_size as usize != computed.region.size() {
            return Err(AttachError::LayoutMismatch {
                expected: computed.region.size(),
                actual: header.region_size as usize,
            });
        }
        validate_region(base, len, computed.region.align(), computed.region.size()).map_err(
            |(expected, actual, aligned)| {
                if aligned {
                    AttachError::RegionTooSmall { expected, actual }
                } else {
                    AttachError::BadAlignment { expected, actual }
                }
            },
        )?;

        Ok(build_handle(
            base,
            header.pool_id,
            page_size,
            page_count,
            computed,
        ))
    }

    /// Try to lease one page from the freelist.
    ///
    /// On success this returns an attached [`PageLease`]. Dropping that lease
    /// returns the page to the pool unless it has been detached with
    /// [`PageLease::into_descriptor`].
    pub fn try_acquire(&self) -> Result<PageLease<'_>, AcquireError> {
        let page_id = match self.freelist().allocate() {
            Ok(page_id) => page_id,
            Err(StackError::Empty) => {
                self.metrics_ref()
                    .acquire_empty
                    .fetch_add(1, Ordering::Relaxed);
                return Err(AcquireError::Empty);
            }
            Err(StackError::Full) => unreachable!("TreiberStack::allocate never returns Full"),
        };

        let state = self.state_ref(page_id as usize);
        let mut retries = 0u64;
        loop {
            let current = state.load(Ordering::Acquire);
            let (generation, raw_state) = unpack_state(current);
            if raw_state != LeaseState::Free as u8 {
                if retries > 0 {
                    self.metrics_ref()
                        .acquire_retry_loops
                        .fetch_add(retries, Ordering::Relaxed);
                }
                return Err(AcquireError::CorruptState {
                    page_id,
                    generation,
                    raw_state,
                });
            }

            let Some(next_generation) = generation.checked_add(1) else {
                if retries > 0 {
                    self.metrics_ref()
                        .acquire_retry_loops
                        .fetch_add(retries, Ordering::Relaxed);
                }
                return Err(AcquireError::CorruptState {
                    page_id,
                    generation,
                    raw_state,
                });
            };
            let leased_state = pack_state(next_generation, LeaseState::Attached);
            match state.compare_exchange(current, leased_state, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if retries > 0 {
                        self.metrics_ref()
                            .acquire_retry_loops
                            .fetch_add(retries, Ordering::Relaxed);
                    }
                    self.metrics_ref()
                        .acquire_ok
                        .fetch_add(1, Ordering::Relaxed);
                    let leased_pages = self
                        .metrics_ref()
                        .leased_pages
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    self.observe_high_watermark(leased_pages);
                    return Ok(PageLease {
                        pool: self,
                        descriptor: PageDescriptor {
                            pool_id: self.pool_id,
                            page_id,
                            generation: next_generation,
                        },
                        detached: false,
                    });
                }
                Err(_) => {
                    retries += 1;
                }
            }
        }
    }

    /// Release a detached page descriptor back to the pool.
    ///
    /// This method only accepts descriptors produced by
    /// [`PageLease::into_descriptor`]. Calling it on the result of
    /// [`PageLease::descriptor`] while the lease is still alive returns
    /// [`ReleaseError::LeaseStillAttached`].
    pub fn release(&self, desc: PageDescriptor) -> Result<(), ReleaseError> {
        self.release_with_state(desc, LeaseState::Detached, true)
    }

    /// Read the current shared metrics snapshot.
    pub fn snapshot(&self) -> PoolSnapshot {
        let leased_pages = self.metrics_ref().leased_pages.load(Ordering::Relaxed);
        let exhausted_pages = self.metrics_ref().exhausted_pages.load(Ordering::Relaxed);
        PoolSnapshot {
            page_size: self.page_size,
            page_count: self.page_count,
            free_pages: u64::from(self.page_count)
                .saturating_sub(leased_pages)
                .saturating_sub(exhausted_pages),
            leased_pages,
            exhausted_pages,
            high_watermark_pages: self
                .metrics_ref()
                .high_watermark_pages
                .load(Ordering::Relaxed),
            acquire_ok: self.metrics_ref().acquire_ok.load(Ordering::Relaxed),
            acquire_empty: self.metrics_ref().acquire_empty.load(Ordering::Relaxed),
            release_ok: self.metrics_ref().release_ok.load(Ordering::Relaxed),
            release_exhausted: self.metrics_ref().release_exhausted.load(Ordering::Relaxed),
            release_bad_page: self.metrics_ref().release_bad_page.load(Ordering::Relaxed),
            release_stale_generation: self
                .metrics_ref()
                .release_stale_generation
                .load(Ordering::Relaxed),
            release_still_attached: self
                .metrics_ref()
                .release_still_attached
                .load(Ordering::Relaxed),
            release_not_leased: self
                .metrics_ref()
                .release_not_leased
                .load(Ordering::Relaxed),
            acquire_retry_loops: self
                .metrics_ref()
                .acquire_retry_loops
                .load(Ordering::Relaxed),
            release_retry_loops: self
                .metrics_ref()
                .release_retry_loops
                .load(Ordering::Relaxed),
        }
    }

    /// Return the configured fixed page size in bytes.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Return the configured page count.
    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Return the total shared region size in bytes.
    pub fn region_size(&self) -> usize {
        self.region_size
    }

    /// # Safety
    /// - `desc` must refer to a currently detached page.
    /// - The caller must synchronize descriptor handoff and any concurrent page
    ///   readers outside this crate.
    /// - The returned slice is valid only while the underlying region remains
    ///   mapped and the descriptor still owns the page.
    pub unsafe fn page_bytes(&self, desc: PageDescriptor) -> Result<&[u8], AccessError> {
        self.validate_detached_descriptor(desc)?;
        Ok(self.page_slice_unchecked(desc.page_id as usize))
    }

    /// # Safety
    /// - `desc` must refer to a currently detached page.
    /// - The caller must guarantee exclusive mutable access to the page
    ///   contents.
    /// - Any publish/read protocol for the page contents is outside the scope
    ///   of this crate and must be enforced externally.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn page_bytes_mut(&self, desc: PageDescriptor) -> Result<&mut [u8], AccessError> {
        self.validate_detached_descriptor(desc)?;
        Ok(slice::from_raw_parts_mut(
            self.page_ptr_unchecked(desc.page_id as usize),
            self.page_size,
        ))
    }

    pub(crate) fn release_attached(&self, desc: PageDescriptor) -> Result<(), ReleaseError> {
        self.release_with_state(desc, LeaseState::Attached, false)
    }

    pub(crate) fn detach_descriptor(&self, desc: PageDescriptor) -> Result<(), DetachError> {
        let state = self.state_ref(desc.page_id as usize);
        loop {
            let current = state.load(Ordering::Acquire);
            let (generation, raw_state) = unpack_state(current);
            if generation != desc.generation {
                return Err(DetachError::StaleGeneration {
                    page_id: desc.page_id,
                    expected: generation,
                    actual: desc.generation,
                });
            }

            let actual_state = match LeaseState::from_raw(raw_state) {
                Some(state) => state,
                None => {
                    return Err(DetachError::CorruptState {
                        page_id: desc.page_id,
                        generation,
                        raw_state,
                    });
                }
            };

            if actual_state != LeaseState::Attached {
                return Err(DetachError::NotAttached {
                    page_id: desc.page_id,
                    generation,
                    raw_state,
                });
            }

            match state.compare_exchange(
                current,
                pack_state(generation, LeaseState::Detached),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue,
            }
        }
    }

    pub(crate) fn state_ref(&self, page_index: usize) -> &AtomicU64 {
        unsafe { &*self.states.as_ptr().add(page_index) }
    }

    pub(crate) unsafe fn page_slice_unchecked(&self, page_index: usize) -> &[u8] {
        slice::from_raw_parts(
            self.page_ptr_unchecked(page_index).cast_const(),
            self.page_size,
        )
    }

    pub(crate) unsafe fn page_ptr_unchecked(&self, page_index: usize) -> *mut u8 {
        self.data.as_ptr().add(page_index * self.page_size)
    }

    fn release_with_state(
        &self,
        desc: PageDescriptor,
        expected_state: LeaseState,
        count_errors: bool,
    ) -> Result<(), ReleaseError> {
        if desc.pool_id != self.pool_id {
            return Err(ReleaseError::PoolMismatch {
                expected_pool_id: self.pool_id,
                actual_pool_id: desc.pool_id,
            });
        }

        let page_index = match self.page_index(desc.page_id) {
            Ok(page_index) => page_index,
            Err(err) => {
                if count_errors {
                    self.metrics_ref()
                        .release_bad_page
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(err);
            }
        };

        let state = self.state_ref(page_index);
        let mut retries = 0u64;
        let release_to_exhausted = loop {
            let current = state.load(Ordering::Acquire);
            let (generation, raw_state) = unpack_state(current);
            if generation != desc.generation {
                if count_errors && retries > 0 {
                    self.metrics_ref()
                        .release_retry_loops
                        .fetch_add(retries, Ordering::Relaxed);
                }
                if count_errors {
                    self.metrics_ref()
                        .release_stale_generation
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Err(ReleaseError::StaleGeneration {
                    page_id: desc.page_id,
                    expected: generation,
                    actual: desc.generation,
                });
            }

            let actual_state = match LeaseState::from_raw(raw_state) {
                Some(state) => state,
                None => {
                    if count_errors && retries > 0 {
                        self.metrics_ref()
                            .release_retry_loops
                            .fetch_add(retries, Ordering::Relaxed);
                    }
                    return Err(ReleaseError::CorruptState {
                        page_id: desc.page_id,
                        generation,
                        raw_state,
                    });
                }
            };

            if actual_state != expected_state {
                if count_errors && retries > 0 {
                    self.metrics_ref()
                        .release_retry_loops
                        .fetch_add(retries, Ordering::Relaxed);
                }
                return match (expected_state, actual_state) {
                    (LeaseState::Detached, LeaseState::Attached) => {
                        if count_errors {
                            self.metrics_ref()
                                .release_still_attached
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ReleaseError::LeaseStillAttached {
                            page_id: desc.page_id,
                            generation,
                        })
                    }
                    (_, LeaseState::Exhausted) => Err(ReleaseError::Exhausted {
                        page_id: desc.page_id,
                        generation,
                    }),
                    (LeaseState::Detached, LeaseState::Free)
                    | (LeaseState::Attached, LeaseState::Free) => {
                        if count_errors {
                            self.metrics_ref()
                                .release_not_leased
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(ReleaseError::NotLeased {
                            page_id: desc.page_id,
                            generation,
                        })
                    }
                    (LeaseState::Attached, LeaseState::Detached) => {
                        Err(ReleaseError::CorruptState {
                            page_id: desc.page_id,
                            generation,
                            raw_state,
                        })
                    }
                    _ => unreachable!("expected state mismatch must be handled above"),
                };
            }

            let release_to_exhausted = generation == MAX_GENERATION;
            let released_state = if release_to_exhausted {
                pack_state(generation, LeaseState::Exhausted)
            } else {
                pack_state(generation, LeaseState::Free)
            };
            match state.compare_exchange(
                current,
                released_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break release_to_exhausted,
                Err(_) => {
                    retries += 1;
                }
            }
        };

        if !release_to_exhausted {
            if let Err(StackError::Full) = self.freelist().release(desc.page_id) {
                let mut revert_retries = 0u64;
                loop {
                    let current = state.load(Ordering::Acquire);
                    let (generation, raw_state) = unpack_state(current);
                    if generation != desc.generation || raw_state != LeaseState::Free as u8 {
                        break;
                    }

                    match state.compare_exchange(
                        current,
                        pack_state(generation, expected_state),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(_) => {
                            revert_retries += 1;
                        }
                    }
                }
                if retries + revert_retries > 0 {
                    self.metrics_ref()
                        .release_retry_loops
                        .fetch_add(retries + revert_retries, Ordering::Relaxed);
                }
                return Err(ReleaseError::CorruptFreeList {
                    page_id: desc.page_id,
                });
            }
        }

        if retries > 0 {
            self.metrics_ref()
                .release_retry_loops
                .fetch_add(retries, Ordering::Relaxed);
        }
        self.metrics_ref()
            .release_ok
            .fetch_add(1, Ordering::Relaxed);
        if release_to_exhausted {
            self.metrics_ref()
                .release_exhausted
                .fetch_add(1, Ordering::Relaxed);
            self.metrics_ref()
                .exhausted_pages
                .fetch_add(1, Ordering::Relaxed);
        }
        self.metrics_ref()
            .leased_pages
            .fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    fn validate_detached_descriptor(&self, desc: PageDescriptor) -> Result<usize, AccessError> {
        if desc.pool_id != self.pool_id {
            return Err(AccessError::PoolMismatch {
                expected_pool_id: self.pool_id,
                actual_pool_id: desc.pool_id,
            });
        }

        let page_index = desc.page_id as usize;
        if page_index >= self.page_count as usize {
            return Err(AccessError::BadPageId {
                page_id: desc.page_id,
                page_count: self.page_count,
            });
        }

        let current = self.state_ref(page_index).load(Ordering::Acquire);
        let (generation, raw_state) = unpack_state(current);
        if generation != desc.generation {
            return Err(AccessError::StaleGeneration {
                page_id: desc.page_id,
                expected: generation,
                actual: desc.generation,
            });
        }

        match LeaseState::from_raw(raw_state) {
            Some(LeaseState::Detached) => Ok(page_index),
            Some(LeaseState::Attached) => Err(AccessError::LeaseStillAttached {
                page_id: desc.page_id,
                generation,
            }),
            Some(LeaseState::Exhausted) => Err(AccessError::Exhausted {
                page_id: desc.page_id,
                generation,
            }),
            Some(LeaseState::Free) => Err(AccessError::NotLeased {
                page_id: desc.page_id,
                generation,
            }),
            None => Err(AccessError::CorruptState {
                page_id: desc.page_id,
                generation,
                raw_state,
            }),
        }
    }

    fn observe_high_watermark(&self, leased_pages: u64) {
        let watermark = &self.metrics_ref().high_watermark_pages;
        let mut current = watermark.load(Ordering::Relaxed);
        while leased_pages > current {
            match watermark.compare_exchange(
                current,
                leased_pages,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn metrics_ref(&self) -> &SharedMetrics {
        unsafe { self.metrics.as_ref() }
    }

    fn page_index(&self, page_id: u32) -> Result<usize, ReleaseError> {
        let page_index = page_id as usize;
        if page_index >= self.page_count as usize {
            return Err(ReleaseError::BadPageId {
                page_id,
                page_count: self.page_count,
            });
        }
        Ok(page_index)
    }

    fn freelist(&self) -> TreiberStack {
        unsafe { TreiberStack::attach(self.freelist_header.as_ptr(), self.freelist_next.as_ptr()) }
    }
}

fn build_handle(
    base: NonNull<u8>,
    pool_id: u64,
    page_size: usize,
    page_count: u32,
    computed: ComputedLayout,
) -> PagePool {
    let base_ptr = base.as_ptr();
    let freelist_header_ptr = unsafe { base_ptr.add(computed.freelist_offset) }.cast();
    let freelist_next_ptr = unsafe { base_ptr.add(computed.freelist_next_offset) }.cast();

    PagePool {
        metrics: NonNull::new(unsafe { base_ptr.add(computed.metrics_offset).cast() })
            .expect("page pool metrics ptr is null"),
        states: NonNull::new(unsafe { base_ptr.add(computed.states_offset).cast() })
            .expect("page pool states ptr is null"),
        freelist_header: NonNull::new(freelist_header_ptr).expect("freelist header ptr is null"),
        freelist_next: NonNull::new(freelist_next_ptr).expect("freelist next ptr is null"),
        data: NonNull::new(unsafe { base_ptr.add(computed.data_offset) })
            .expect("page pool data ptr is null"),
        pool_id,
        page_size,
        page_count,
        region_size: computed.region.size(),
    }
}

fn fresh_pool_id(base: NonNull<u8>, len: usize, cfg: PagePoolConfig) -> u64 {
    let time_seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed) as u128;
    let seed = time_seed
        ^ ((base.as_ptr() as usize as u128) << 64)
        ^ ((len as u128) << 32)
        ^ ((cfg.page_size.get() as u128) << 16)
        ^ ((cfg.page_count.get() as u128) << 96)
        ^ ((std::process::id() as u128) << 48)
        ^ counter;

    let id = mix64(seed as u64) ^ mix64((seed >> 64) as u64);
    if id == 0 {
        1
    } else {
        id
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
