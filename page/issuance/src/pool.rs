use crate::error::{AcquireError, AttachError, ConfigError, InitError, ReleaseError};
use lockfree::{
    treiber_stack_layout, treiber_stack_ptrs, StackError, TreiberStack, TreiberStackHeader,
};
use std::alloc::Layout;
use std::num::NonZeroU32;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const ISSUANCE_MAGIC: u64 = 0x5049_5353_5541_4E31;
const ISSUANCE_VERSION: u32 = 1;
const STATE_FREE: u8 = 0;
const STATE_LEASED: u8 = 1;
static NEXT_INSTANCE_SALT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IssuanceConfig {
    permit_count: NonZeroU32,
}

impl IssuanceConfig {
    /// Create a config for a pool with `permit_count` permits.
    pub fn new(permit_count: u32) -> Result<Self, ConfigError> {
        let permit_count = NonZeroU32::new(permit_count).ok_or(ConfigError::ZeroPermitCount)?;
        Ok(Self { permit_count })
    }

    /// Return the number of permits managed by the pool.
    pub fn permit_count(self) -> u32 {
        self.permit_count.get()
    }
}

/// Shared-memory size and alignment required for an issuance region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionLayout {
    pub size: usize,
    pub align: usize,
}

/// Snapshot of pool occupancy and cumulative counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IssuanceSnapshot {
    pub permit_count: u32,
    pub free_permits: u64,
    pub leased_permits: u64,
    pub acquire_ok: u64,
    pub acquire_empty: u64,
    pub release_ok: u64,
    pub release_bad_id: u64,
    pub release_double_free: u64,
}

#[repr(C)]
struct PoolHeader {
    magic: u64,
    version: u32,
    permit_count: u32,
    instance_id: u64,
    region_size: u64,
}

#[repr(C)]
struct SharedMetrics {
    acquire_ok: AtomicU64,
    acquire_empty: AtomicU64,
    release_ok: AtomicU64,
    release_bad_id: AtomicU64,
    release_double_free: AtomicU64,
    leased_permits: AtomicU64,
}

impl SharedMetrics {
    fn zeroed() -> Self {
        Self {
            acquire_ok: AtomicU64::new(0),
            acquire_empty: AtomicU64::new(0),
            release_ok: AtomicU64::new(0),
            release_bad_id: AtomicU64::new(0),
            release_double_free: AtomicU64::new(0),
            leased_permits: AtomicU64::new(0),
        }
    }
}

/// Attached handle to a shared-memory issuance pool.
#[derive(Clone, Copy)]
pub struct IssuancePool {
    metrics: NonNull<SharedMetrics>,
    states: NonNull<AtomicU8>,
    freelist_header: NonNull<TreiberStackHeader>,
    freelist_next: NonNull<std::sync::atomic::AtomicU32>,
    permit_count: u32,
    instance_id: u64,
}

unsafe impl Send for IssuancePool {}
unsafe impl Sync for IssuancePool {}

/// RAII guard for one leased permit.
pub struct PermitLease {
    pool: IssuancePool,
    permit_id: u32,
    detached: bool,
}

struct ComputedLayout {
    region: Layout,
    metrics_offset: usize,
    freelist_offset: usize,
    states_offset: usize,
    stack_layout: lockfree::StackLayout,
}

impl IssuancePool {
    /// Compute the shared-memory layout required for one pool instance.
    pub fn layout(cfg: IssuanceConfig) -> Result<RegionLayout, ConfigError> {
        let computed = compute_layout(cfg)?;
        Ok(RegionLayout {
            size: computed.region.size(),
            align: computed.region.align(),
        })
    }

    /// # Safety
    /// - `base` must point to a writable region of at least `len` bytes.
    /// - The region must satisfy [`IssuancePool::layout`] for `cfg`.
    /// - Initialization must happen exactly once before concurrent access.
    pub unsafe fn init_in_place(
        base: NonNull<u8>,
        len: usize,
        cfg: IssuanceConfig,
    ) -> Result<Self, InitError> {
        let computed = compute_layout(cfg).map_err(InitError::InvalidConfig)?;
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
        let freelist_base = base_ptr.add(computed.freelist_offset);
        let (freelist_header_ptr, freelist_next_ptr) =
            treiber_stack_ptrs(freelist_base, computed.stack_layout);
        let states_ptr = base_ptr.add(computed.states_offset).cast::<AtomicU8>();

        std::ptr::write(
            header_ptr,
            PoolHeader {
                magic: ISSUANCE_MAGIC,
                version: ISSUANCE_VERSION,
                permit_count: cfg.permit_count(),
                instance_id: next_instance_id(base),
                region_size: computed.region.size() as u64,
            },
        );
        std::ptr::write(metrics_ptr, SharedMetrics::zeroed());
        for permit_id in 0..cfg.permit_count() as usize {
            std::ptr::write(states_ptr.add(permit_id), AtomicU8::new(STATE_FREE));
        }
        let _ = TreiberStack::init_in_place(
            freelist_header_ptr,
            freelist_next_ptr,
            cfg.permit_count() as usize,
        );

        Ok(build_handle(base, cfg.permit_count(), computed))
    }

    /// # Safety
    /// `base` must point to a previously initialized issuance region that stays
    /// valid for the lifetime of the returned handle.
    pub unsafe fn attach(base: NonNull<u8>, len: usize) -> Result<Self, AttachError> {
        validate_region(
            base,
            len,
            std::mem::align_of::<PoolHeader>(),
            std::mem::size_of::<PoolHeader>(),
        )
        .map_err(|(expected, actual, aligned)| {
            if aligned {
                AttachError::RegionTooSmall { expected, actual }
            } else {
                AttachError::BadAlignment { expected, actual }
            }
        })?;

        let header = &*base.as_ptr().cast::<PoolHeader>();
        if header.magic != ISSUANCE_MAGIC {
            return Err(AttachError::BadMagic {
                expected: ISSUANCE_MAGIC,
                actual: header.magic,
            });
        }
        if header.version != ISSUANCE_VERSION {
            return Err(AttachError::UnsupportedVersion {
                expected: ISSUANCE_VERSION,
                actual: header.version,
            });
        }
        let cfg = IssuanceConfig::new(header.permit_count).map_err(AttachError::InvalidConfig)?;
        let computed = compute_layout(cfg).map_err(AttachError::InvalidConfig)?;
        validate_region(base, len, computed.region.align(), computed.region.size()).map_err(
            |(expected, actual, aligned)| {
                if aligned {
                    AttachError::LayoutMismatch { expected, actual }
                } else {
                    AttachError::BadAlignment { expected, actual }
                }
            },
        )?;
        if header.region_size as usize != computed.region.size() {
            return Err(AttachError::LayoutMismatch {
                expected: header.region_size as usize,
                actual: computed.region.size(),
            });
        }

        Ok(build_handle(base, cfg.permit_count(), computed))
    }

    pub fn permit_count(&self) -> u32 {
        self.permit_count
    }

    /// Return the shared instance identity for this issuance region.
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Try to lease one permit without blocking.
    pub fn try_acquire(&self) -> Result<PermitLease, AcquireError> {
        let permit_id = match self.freelist().allocate() {
            Ok(id) => id,
            Err(StackError::Empty) => {
                self.metrics().acquire_empty.fetch_add(1, Ordering::Relaxed);
                return Err(AcquireError::Empty);
            }
            Err(StackError::Full) => unreachable!("TreiberStack::allocate never returns Full"),
        };

        let state = self.state(permit_id);
        match state.compare_exchange(
            STATE_FREE,
            STATE_LEASED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.metrics().acquire_ok.fetch_add(1, Ordering::Relaxed);
                self.metrics()
                    .leased_permits
                    .fetch_add(1, Ordering::Relaxed);
                Ok(PermitLease {
                    pool: *self,
                    permit_id,
                    detached: false,
                })
            }
            Err(actual) => {
                let _ = self.freelist().release(permit_id);
                Err(AcquireError::CorruptState {
                    permit_id,
                    state: actual,
                })
            }
        }
    }

    pub fn release(&self, permit_id: u32) -> Result<(), ReleaseError> {
        if permit_id >= self.permit_count {
            self.metrics()
                .release_bad_id
                .fetch_add(1, Ordering::Relaxed);
            return Err(ReleaseError::BadPermitId {
                permit_id,
                permit_count: self.permit_count,
            });
        }

        let state = self.state(permit_id);
        let actual = state
            .compare_exchange(
                STATE_LEASED,
                STATE_FREE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .err();

        if let Some(actual) = actual {
            if actual == STATE_FREE {
                self.metrics()
                    .release_double_free
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ReleaseError::DoubleFree { permit_id });
            }
            return Err(ReleaseError::CorruptState {
                permit_id,
                state: actual,
            });
        }

        if let Err(StackError::Full) = self.freelist().release(permit_id) {
            let _ = state.compare_exchange(
                STATE_FREE,
                STATE_LEASED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            return Err(ReleaseError::CorruptFreeList { permit_id });
        }

        self.metrics().release_ok.fetch_add(1, Ordering::Relaxed);
        self.metrics()
            .leased_permits
            .fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn snapshot(&self) -> IssuanceSnapshot {
        let leased = self.metrics().leased_permits.load(Ordering::Relaxed);
        IssuanceSnapshot {
            permit_count: self.permit_count,
            free_permits: u64::from(self.permit_count).saturating_sub(leased),
            leased_permits: leased,
            acquire_ok: self.metrics().acquire_ok.load(Ordering::Relaxed),
            acquire_empty: self.metrics().acquire_empty.load(Ordering::Relaxed),
            release_ok: self.metrics().release_ok.load(Ordering::Relaxed),
            release_bad_id: self.metrics().release_bad_id.load(Ordering::Relaxed),
            release_double_free: self.metrics().release_double_free.load(Ordering::Relaxed),
        }
    }
    fn metrics(&self) -> &SharedMetrics {
        unsafe { self.metrics.as_ref() }
    }

    fn state(&self, permit_id: u32) -> &AtomicU8 {
        unsafe { &*self.states.as_ptr().add(permit_id as usize) }
    }

    fn freelist(&self) -> TreiberStack {
        unsafe { TreiberStack::attach(self.freelist_header.as_ptr(), self.freelist_next.as_ptr()) }
    }
}

impl PermitLease {
    /// Return the leased permit id.
    pub fn permit_id(&self) -> u32 {
        self.permit_id
    }

    /// Detach the raw permit id from this lease without releasing it.
    ///
    /// Callers must later return the same permit to the pool exactly once.
    pub fn into_id(mut self) -> u32 {
        self.detached = true;
        self.permit_id
    }
}

impl Drop for PermitLease {
    fn drop(&mut self) {
        if !self.detached {
            let _ = self.pool.release(self.permit_id);
        }
    }
}

fn compute_layout(cfg: IssuanceConfig) -> Result<ComputedLayout, ConfigError> {
    let header = Layout::new::<PoolHeader>();
    let metrics = Layout::new::<SharedMetrics>();
    let stack_layout = treiber_stack_layout(cfg.permit_count() as usize)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let states = Layout::array::<AtomicU8>(cfg.permit_count() as usize)
        .map_err(|_| ConfigError::LayoutOverflow)?;

    let (region, metrics_offset) = header
        .extend(metrics)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (region, freelist_offset) = region
        .extend(stack_layout.layout)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (region, states_offset) = region
        .extend(states)
        .map_err(|_| ConfigError::LayoutOverflow)?;

    Ok(ComputedLayout {
        region: region.pad_to_align(),
        metrics_offset,
        freelist_offset,
        states_offset,
        stack_layout,
    })
}

fn validate_region(
    base: NonNull<u8>,
    len: usize,
    expected_align: usize,
    expected_size: usize,
) -> Result<(), (usize, usize, bool)> {
    let actual_align = (base.as_ptr() as usize) & (expected_align - 1);
    if actual_align != 0 {
        return Err((expected_align, base.as_ptr() as usize, false));
    }
    if len < expected_size {
        return Err((expected_size, len, true));
    }
    Ok(())
}

fn build_handle(base: NonNull<u8>, permit_count: u32, computed: ComputedLayout) -> IssuancePool {
    let base_ptr = base.as_ptr();
    let header = unsafe { &*base_ptr.cast::<PoolHeader>() };
    let freelist_base = unsafe { base_ptr.add(computed.freelist_offset) };
    let (freelist_header_ptr, freelist_next_ptr) =
        unsafe { treiber_stack_ptrs(freelist_base, computed.stack_layout) };
    IssuancePool {
        metrics: NonNull::new(
            unsafe { base_ptr.add(computed.metrics_offset) }.cast::<SharedMetrics>(),
        )
        .expect("metrics ptr"),
        states: NonNull::new(unsafe { base_ptr.add(computed.states_offset) }.cast::<AtomicU8>())
            .expect("states ptr"),
        freelist_header: NonNull::new(freelist_header_ptr).expect("freelist header"),
        freelist_next: NonNull::new(freelist_next_ptr).expect("freelist next"),
        permit_count,
        instance_id: header.instance_id,
    }
}

fn next_instance_id(base: NonNull<u8>) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let salt = NEXT_INSTANCE_SALT.fetch_add(1, Ordering::Relaxed);
    let addr = base.as_ptr() as usize as u64;
    let mixed = now.rotate_left(17) ^ addr.rotate_left(7) ^ salt;
    if mixed == 0 {
        1
    } else {
        mixed
    }
}
