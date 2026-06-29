use std::alloc::{Layout, LayoutError};
use std::error::Error;
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use control_transport::BackendLeaseSlot;

use crate::{
    AtomicBloomRef, BloomAttachError, BloomParams, LifecycleError, ProbeDecision,
    RuntimeFilterHeader, RuntimeFilterSlot, RuntimeFilterState,
};

const POOL_MAGIC: u64 = 0x5047_4655_5246_5031;
/// Shared-memory pool format version.
pub const RUNTIME_FILTER_POOL_VERSION: u32 = 3;

const SLOT_FREE: u64 = 0;
const SLOT_INITIALIZING: u64 = 1;
const SLOT_ALLOCATED: u64 = 2;
const SLOT_RETIRING: u64 = 3;

const SLOT_STATE_BITS: u32 = 2;
const SLOT_REF_BITS: u32 = 16;
const SLOT_REF_SHIFT: u32 = SLOT_STATE_BITS;
const SLOT_EPOCH_SHIFT: u32 = SLOT_STATE_BITS + SLOT_REF_BITS;
const SLOT_STATE_MASK: u64 = (1u64 << SLOT_STATE_BITS) - 1;
const SLOT_REF_MASK: u64 = (1u64 << SLOT_REF_BITS) - 1;
const SLOT_EPOCH_MASK: u64 = (1u64 << (64 - SLOT_EPOCH_SHIFT)) - 1;
const SLOT_MAX_REFS: u32 = SLOT_REF_MASK as u32;
const SLOT_MAX_EPOCH: u64 = SLOT_EPOCH_MASK;

/// Runtime-filter key types currently supported by pg_fusion scan probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum RuntimeFilterKeyType {
    /// Signed 16-bit integer key.
    Int16 = 1,
    /// Signed 32-bit integer key.
    Int32 = 2,
    /// Signed 64-bit integer key.
    Int64 = 3,
    /// Boolean key.
    Boolean = 4,
    /// 32-bit floating-point key.
    Float32 = 5,
    /// 64-bit floating-point key.
    Float64 = 6,
    /// UTF-8 byte key.
    Utf8View = 7,
    /// PostgreSQL `uuid` key stored as 16 bytes.
    Uuid = 8,
    /// Binary byte key.
    BinaryView = 9,
    /// Arrow `Date32` key stored as native-endian days.
    Date32 = 10,
    /// Arrow `Time64(Microsecond)` key stored as native-endian microseconds.
    Time64Microsecond = 11,
    /// Arrow `Timestamp(Microsecond, None)` key stored as native-endian microseconds.
    TimestampMicrosecond = 12,
    /// Arrow `Decimal128` key stored as a scaled signed 128-bit integer.
    Decimal128 = 13,
    /// Arrow `Interval(MonthDayNano)` key stored as months/days/nanoseconds.
    IntervalMonthDayNano = 14,
}

impl RuntimeFilterKeyType {
    fn from_raw(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::Int16),
            2 => Some(Self::Int32),
            3 => Some(Self::Int64),
            4 => Some(Self::Boolean),
            5 => Some(Self::Float32),
            6 => Some(Self::Float64),
            7 => Some(Self::Utf8View),
            8 => Some(Self::Uuid),
            9 => Some(Self::BinaryView),
            10 => Some(Self::Date32),
            11 => Some(Self::Time64Microsecond),
            12 => Some(Self::TimestampMicrosecond),
            13 => Some(Self::Decimal128),
            14 => Some(Self::IntervalMonthDayNano),
            _ => None,
        }
    }
}

/// Execution namespace for runtime-filter targets.
///
/// Backend-local session epochs are unique only within one primary backend
/// lease. Runtime filters need the full primary lease identity so concurrent
/// backend executions cannot attach to each other's filters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct RuntimeFilterExecId {
    slot_id: u32,
    generation: u64,
    lease_epoch: u64,
    session_epoch: u64,
}

impl RuntimeFilterExecId {
    pub const fn new(slot_id: u32, generation: u64, lease_epoch: u64, session_epoch: u64) -> Self {
        Self {
            slot_id,
            generation,
            lease_epoch,
            session_epoch,
        }
    }

    pub const fn from_peer(peer: BackendLeaseSlot, session_epoch: u64) -> Self {
        Self::new(
            peer.slot_id(),
            peer.lease_id().generation(),
            peer.lease_id().lease_epoch(),
            session_epoch,
        )
    }

    pub const fn slot_id(self) -> u32 {
        self.slot_id
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }

    pub const fn session_epoch(self) -> u64 {
        self.session_epoch
    }
}

/// Logical target that connects a worker-built filter to backend scan probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeFilterTarget {
    /// Execution namespace that scopes scan identifiers.
    pub exec_id: RuntimeFilterExecId,
    /// Backend scan identifier.
    pub scan_id: u64,
    /// Output column to inspect before tuple-to-Arrow encoding.
    pub output_column: u32,
    /// Key type expected at `output_column`.
    pub key_type: RuntimeFilterKeyType,
}

/// Fixed shared-memory pool configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeFilterPoolConfig {
    slot_count: u32,
    params: BloomParams,
}

impl RuntimeFilterPoolConfig {
    /// Create a pool configuration with `slot_count` independent filters.
    pub fn new(slot_count: u32, params: BloomParams) -> Self {
        Self { slot_count, params }
    }

    /// Number of filter slots in the pool.
    pub fn slot_count(self) -> u32 {
        self.slot_count
    }

    /// Bloom parameters used by every slot.
    pub fn params(self) -> BloomParams {
        self.params
    }
}

/// Size and alignment required by a [`RuntimeFilterPool`] region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeFilterPoolLayout {
    /// Required byte length.
    pub size: usize,
    /// Required base-pointer alignment.
    pub align: usize,
}

/// Failure to initialize or attach a runtime-filter pool.
#[derive(Debug)]
pub enum RuntimeFilterPoolAttachError {
    NullBase,
    Layout(LayoutError),
    LayoutOverflow,
    Misaligned { required: usize, actual: usize },
    TooSmall { required: usize, actual: usize },
    InvalidMagic { actual: u64 },
    InvalidVersion { expected: u32, actual: u32 },
    ConfigMismatch,
    Bloom(BloomAttachError),
    Lifecycle(LifecycleError),
}

impl fmt::Display for RuntimeFilterPoolAttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NullBase => f.write_str("runtime filter pool base pointer is null"),
            Self::Layout(err) => write!(f, "runtime filter pool layout error: {err}"),
            Self::LayoutOverflow => f.write_str("runtime filter pool layout size overflow"),
            Self::Misaligned { required, actual } => write!(
                f,
                "runtime filter pool base alignment {actual} does not satisfy {required}",
            ),
            Self::TooSmall { required, actual } => write!(
                f,
                "runtime filter pool region has {actual} bytes, but {required} are required",
            ),
            Self::InvalidMagic { actual } => {
                write!(f, "runtime filter pool has invalid magic {actual:#x}")
            }
            Self::InvalidVersion { expected, actual } => write!(
                f,
                "runtime filter pool version mismatch: expected {expected}, got {actual}",
            ),
            Self::ConfigMismatch => f.write_str("runtime filter pool config mismatch"),
            Self::Bloom(err) => write!(f, "runtime filter pool bloom attach error: {err}"),
            Self::Lifecycle(err) => write!(f, "runtime filter pool lifecycle error: {err}"),
        }
    }
}

impl Error for RuntimeFilterPoolAttachError {}

impl From<LayoutError> for RuntimeFilterPoolAttachError {
    fn from(value: LayoutError) -> Self {
        Self::Layout(value)
    }
}

impl From<BloomAttachError> for RuntimeFilterPoolAttachError {
    fn from(value: BloomAttachError) -> Self {
        Self::Bloom(value)
    }
}

impl From<LifecycleError> for RuntimeFilterPoolAttachError {
    fn from(value: LifecycleError) -> Self {
        Self::Lifecycle(value)
    }
}

#[repr(C)]
struct PoolHeader {
    magic: u64,
    version: u32,
    slot_count: u32,
    bit_count: u64,
    hash_count: u32,
    _reserved0: u32,
    seed: u64,
    word_count: u64,
    region_size: u64,
}

#[repr(C)]
struct PoolSlot {
    meta: AtomicU64,
    generation: AtomicU64,
    exec_slot_id: AtomicU32,
    exec_generation: AtomicU64,
    exec_lease_epoch: AtomicU64,
    exec_session_epoch: AtomicU64,
    scan_id: AtomicU64,
    output_column: AtomicU32,
    key_type: AtomicU32,
    header: RuntimeFilterHeader,
}

impl PoolSlot {
    fn new() -> Self {
        Self {
            meta: AtomicU64::new(pack_pool_word(SLOT_FREE, 0, 0)),
            generation: AtomicU64::new(0),
            exec_slot_id: AtomicU32::new(0),
            exec_generation: AtomicU64::new(0),
            exec_lease_epoch: AtomicU64::new(0),
            exec_session_epoch: AtomicU64::new(0),
            scan_id: AtomicU64::new(0),
            output_column: AtomicU32::new(0),
            key_type: AtomicU32::new(0),
            header: RuntimeFilterHeader::free(),
        }
    }
}

fn pack_pool_word(state: u64, refs: u32, publication_epoch: u64) -> u64 {
    debug_assert!(state <= SLOT_STATE_MASK);
    debug_assert!((refs as u64) <= SLOT_REF_MASK);
    debug_assert!(publication_epoch <= SLOT_EPOCH_MASK);
    state | ((refs as u64) << SLOT_REF_SHIFT) | (publication_epoch << SLOT_EPOCH_SHIFT)
}

fn pool_word_state(word: u64) -> u64 {
    word & SLOT_STATE_MASK
}

fn pool_word_refs(word: u64) -> u32 {
    ((word >> SLOT_REF_SHIFT) & SLOT_REF_MASK) as u32
}

fn pool_word_epoch(word: u64) -> u64 {
    (word >> SLOT_EPOCH_SHIFT) & SLOT_EPOCH_MASK
}

fn claim_initializing_slot(slot: &PoolSlot) -> Option<u64> {
    let mut current = slot.meta.load(Ordering::Acquire);
    loop {
        if pool_word_state(current) != SLOT_FREE || pool_word_refs(current) != 0 {
            return None;
        }
        let publication_epoch = pool_word_epoch(current).checked_add(1)?;
        if publication_epoch > SLOT_MAX_EPOCH {
            return None;
        }
        let next = pack_pool_word(SLOT_INITIALIZING, 0, publication_epoch);
        match slot
            .meta
            .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return Some(publication_epoch),
            Err(actual) => current = actual,
        }
    }
}

fn acquire_probe_ref(slot: &PoolSlot) -> bool {
    let mut current = slot.meta.load(Ordering::Acquire);
    loop {
        if pool_word_state(current) != SLOT_ALLOCATED {
            return false;
        }
        let refs = pool_word_refs(current);
        if refs == SLOT_MAX_REFS {
            return false;
        }
        let next = pack_pool_word(SLOT_ALLOCATED, refs + 1, pool_word_epoch(current));
        match slot
            .meta
            .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ComputedLayout {
    layout: Layout,
    slots_offset: usize,
    bits_offset: usize,
}

impl ComputedLayout {
    fn new(config: RuntimeFilterPoolConfig) -> Result<Self, RuntimeFilterPoolAttachError> {
        let header = Layout::new::<PoolHeader>();
        let slots = Layout::array::<PoolSlot>(config.slot_count as usize)?;
        let (layout, slots_offset) = header.extend(slots)?;
        let Some(total_words) =
            (config.slot_count as usize).checked_mul(config.params.word_count())
        else {
            return Err(RuntimeFilterPoolAttachError::LayoutOverflow);
        };
        let bits = Layout::array::<AtomicU64>(total_words)?;
        let (layout, bits_offset) = layout.extend(bits)?;
        Ok(Self {
            layout: layout.pad_to_align(),
            slots_offset,
            bits_offset,
        })
    }
}

/// Fixed-slot shared-memory owner for runtime filters.
///
/// The pool manages slot metadata, target lookup, generation ownership, and
/// probe reference counts. It is the production-safe way to reuse Bloom storage
/// without clearing bits under old probes.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeFilterPool {
    header: Option<NonNull<PoolHeader>>,
    slots: Option<NonNull<PoolSlot>>,
    bits: Option<NonNull<AtomicU64>>,
    config: RuntimeFilterPoolConfig,
}

unsafe impl Send for RuntimeFilterPool {}
unsafe impl Sync for RuntimeFilterPool {}

impl PartialEq for RuntimeFilterPool {
    fn eq(&self, other: &Self) -> bool {
        self.header.map(NonNull::as_ptr) == other.header.map(NonNull::as_ptr)
            && self.config == other.config
    }
}

impl Eq for RuntimeFilterPool {}

impl Default for RuntimeFilterPool {
    fn default() -> Self {
        Self {
            header: None,
            slots: None,
            bits: None,
            config: RuntimeFilterPoolConfig::new(
                0,
                BloomParams::new(1, 1, 0).expect("valid default bloom params"),
            ),
        }
    }
}

impl RuntimeFilterPool {
    /// Compute the required shared-memory layout for `config`.
    pub fn layout(
        config: RuntimeFilterPoolConfig,
    ) -> Result<RuntimeFilterPoolLayout, RuntimeFilterPoolAttachError> {
        let computed = ComputedLayout::new(config)?;
        Ok(RuntimeFilterPoolLayout {
            size: computed.layout.size(),
            align: computed.layout.align(),
        })
    }

    /// Initialize a shared-memory pool in caller-owned storage.
    ///
    /// # Safety
    ///
    /// `base` must point to a zero or scratch region at least
    /// `Self::layout(config).size` bytes long with the required alignment.
    /// No other process may concurrently attach or use the region until this
    /// method returns.
    pub unsafe fn init_in_place(
        base: *mut u8,
        len: usize,
        config: RuntimeFilterPoolConfig,
    ) -> Result<Self, RuntimeFilterPoolAttachError> {
        let computed = ComputedLayout::new(config)?;
        validate_region(base, len, computed.layout)?;
        let header = base.cast::<PoolHeader>();
        header.write(PoolHeader {
            magic: POOL_MAGIC,
            version: RUNTIME_FILTER_POOL_VERSION,
            slot_count: config.slot_count,
            bit_count: config.params.bit_count() as u64,
            hash_count: config.params.hash_count() as u32,
            _reserved0: 0,
            seed: config.params.seed(),
            word_count: config.params.word_count() as u64,
            region_size: computed.layout.size() as u64,
        });

        let slots = base.add(computed.slots_offset).cast::<PoolSlot>();
        for slot_index in 0..config.slot_count as usize {
            slots.add(slot_index).write(PoolSlot::new());
        }

        let bits = base.add(computed.bits_offset).cast::<AtomicU64>();
        for word_index in 0..total_word_count(config) {
            bits.add(word_index).write(AtomicU64::new(0));
        }

        Ok(Self {
            header: Some(NonNull::new_unchecked(header)),
            slots: Some(NonNull::new_unchecked(slots)),
            bits: Some(NonNull::new_unchecked(bits)),
            config,
        })
    }

    /// Attach to an initialized shared-memory pool.
    ///
    /// # Safety
    ///
    /// `base` must remain mapped and valid for the lifetime of all returned
    /// handles and probes.
    pub unsafe fn attach(
        base: *mut u8,
        len: usize,
        config: RuntimeFilterPoolConfig,
    ) -> Result<Self, RuntimeFilterPoolAttachError> {
        let computed = ComputedLayout::new(config)?;
        validate_region(base, len, computed.layout)?;
        let header = &*base.cast::<PoolHeader>();
        if header.magic != POOL_MAGIC {
            return Err(RuntimeFilterPoolAttachError::InvalidMagic {
                actual: header.magic,
            });
        }
        if header.version != RUNTIME_FILTER_POOL_VERSION {
            return Err(RuntimeFilterPoolAttachError::InvalidVersion {
                expected: RUNTIME_FILTER_POOL_VERSION,
                actual: header.version,
            });
        }
        if header.slot_count != config.slot_count
            || header.bit_count != config.params.bit_count() as u64
            || header.hash_count != config.params.hash_count() as u32
            || header.seed != config.params.seed()
            || header.word_count != config.params.word_count() as u64
            || header.region_size != computed.layout.size() as u64
        {
            return Err(RuntimeFilterPoolAttachError::ConfigMismatch);
        }

        Ok(Self {
            header: Some(NonNull::new_unchecked(base.cast::<PoolHeader>())),
            slots: Some(NonNull::new_unchecked(
                base.add(computed.slots_offset).cast::<PoolSlot>(),
            )),
            bits: Some(NonNull::new_unchecked(
                base.add(computed.bits_offset).cast::<AtomicU64>(),
            )),
            config,
        })
    }

    /// Return whether this handle is attached to a real shared-memory region.
    pub fn is_attached(self) -> bool {
        self.header.is_some()
    }

    /// Return the pool configuration.
    pub fn config(self) -> RuntimeFilterPoolConfig {
        self.config
    }

    /// Allocate a filter slot for a worker build.
    ///
    /// Returns `Ok(None)` when the pool is unavailable or exhausted. Callers
    /// should treat that as a performance fallback and continue without a
    /// runtime filter.
    pub fn allocate_build(
        self,
        target: RuntimeFilterTarget,
    ) -> Result<Option<RuntimeFilterBuildHandle>, RuntimeFilterPoolAttachError> {
        if !self.is_attached() || self.config.slot_count == 0 {
            return Ok(None);
        }

        for slot_index in 0..self.config.slot_count {
            let slot = unsafe { self.slot(slot_index) };
            let Some(publication_epoch) = claim_initializing_slot(slot) else {
                continue;
            };

            store_exec_id(slot, target.exec_id);
            slot.scan_id.store(target.scan_id, Ordering::Release);
            slot.output_column
                .store(target.output_column, Ordering::Release);
            slot.key_type
                .store(target.key_type as u32, Ordering::Release);

            let builder_result = unsafe { self.runtime_slot(slot_index) }
                .and_then(|runtime_slot| runtime_slot.try_acquire_builder().map_err(Into::into));
            match builder_result {
                Ok(builder) => {
                    let generation = builder.detach();
                    slot.generation.store(generation, Ordering::Release);
                    slot.meta.store(
                        pack_pool_word(SLOT_ALLOCATED, 1, publication_epoch),
                        Ordering::Release,
                    );
                    return Ok(Some(RuntimeFilterBuildHandle {
                        pool: self,
                        slot_index,
                        generation,
                        released: false,
                    }));
                }
                Err(err) => {
                    clear_slot_metadata(slot);
                    slot.meta.store(
                        pack_pool_word(SLOT_FREE, 0, publication_epoch),
                        Ordering::Release,
                    );
                    return Err(err);
                }
            }
        }

        Ok(None)
    }

    /// Find probe handles matching `(exec_id, scan_id)`.
    ///
    /// Matching handles are pushed into `probes` and hold pool references until
    /// dropped.
    pub fn lookup_probes(
        self,
        exec_id: RuntimeFilterExecId,
        scan_id: u64,
        probes: &mut Vec<RuntimeFilterProbeHandle>,
    ) {
        if !self.is_attached() {
            return;
        }

        for slot_index in 0..self.config.slot_count {
            let slot = unsafe { self.slot(slot_index) };
            if !acquire_probe_ref(slot) {
                continue;
            }

            let matches =
                load_exec_id(slot) == exec_id && slot.scan_id.load(Ordering::Acquire) == scan_id;
            if !matches {
                self.release_ref(slot_index);
                continue;
            }

            let Some(key_type) =
                RuntimeFilterKeyType::from_raw(slot.key_type.load(Ordering::Acquire))
            else {
                self.release_ref(slot_index);
                continue;
            };
            probes.push(RuntimeFilterProbeHandle {
                pool: self,
                slot_index,
                generation: slot.generation.load(Ordering::Acquire),
                output_column: slot.output_column.load(Ordering::Acquire),
                key_type,
                released: false,
            });
        }
    }

    fn insert_hash(
        self,
        slot_index: u32,
        generation: u64,
        hash: u64,
    ) -> Result<(), RuntimeFilterPoolAttachError> {
        let slot = unsafe { self.slot(slot_index) };
        let snapshot = slot.header.load(Ordering::Acquire);
        if snapshot.generation == generation && snapshot.state == RuntimeFilterState::Building {
            let bloom =
                unsafe { AtomicBloomRef::new(self.bits_for_slot(slot_index), self.config.params)? };
            bloom.insert_hash(hash);
        }
        Ok(())
    }

    fn publish_ready(
        self,
        slot_index: u32,
        generation: u64,
    ) -> Result<(), RuntimeFilterPoolAttachError> {
        let runtime_slot = unsafe { self.runtime_slot(slot_index)? };
        runtime_slot.publish_build(generation).map(|_| ())?;
        Ok(())
    }

    fn disable_build(
        self,
        slot_index: u32,
        generation: u64,
    ) -> Result<(), RuntimeFilterPoolAttachError> {
        let runtime_slot = unsafe { self.runtime_slot(slot_index)? };
        runtime_slot.disable_build(generation)?;
        Ok(())
    }

    fn release_owner(self, slot_index: u32) {
        let slot = unsafe { self.slot(slot_index) };
        let mut current = slot.meta.load(Ordering::Acquire);
        loop {
            if pool_word_state(current) != SLOT_ALLOCATED {
                return;
            }
            let refs = pool_word_refs(current);
            if refs == 0 {
                debug_assert!(refs > 0, "allocated runtime filter slot has no owner ref");
                return;
            }

            let next_refs = refs - 1;
            let publication_epoch = pool_word_epoch(current);
            let next = pack_pool_word(SLOT_RETIRING, next_refs, publication_epoch);
            match slot
                .meta
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if next_refs == 0 {
                        self.finish_retire(slot_index, publication_epoch);
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn release_ref(self, slot_index: u32) {
        let slot = unsafe { self.slot(slot_index) };
        let mut current = slot.meta.load(Ordering::Acquire);
        loop {
            let state = pool_word_state(current);
            if state != SLOT_ALLOCATED && state != SLOT_RETIRING {
                debug_assert!(
                    state == SLOT_ALLOCATED || state == SLOT_RETIRING,
                    "runtime filter ref released from inactive slot",
                );
                return;
            }
            let refs = pool_word_refs(current);
            if refs == 0 {
                debug_assert!(refs > 0, "runtime filter reference count underflowed");
                return;
            }
            debug_assert!(
                state != SLOT_ALLOCATED || refs > 1,
                "allocated runtime filter slot lost owner ref",
            );

            let next_refs = refs - 1;
            let publication_epoch = pool_word_epoch(current);
            let next = pack_pool_word(state, next_refs, publication_epoch);
            match slot
                .meta
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if state == SLOT_RETIRING && next_refs == 0 {
                        self.finish_retire(slot_index, publication_epoch);
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn finish_retire(self, slot_index: u32, publication_epoch: u64) {
        let slot = unsafe { self.slot(slot_index) };
        debug_assert_eq!(
            slot.meta.load(Ordering::Acquire),
            pack_pool_word(SLOT_RETIRING, 0, publication_epoch),
        );
        let generation = slot.generation.load(Ordering::Acquire);
        if let Ok(runtime_slot) = unsafe { self.runtime_slot(slot_index) } {
            match runtime_slot.snapshot().state {
                RuntimeFilterState::Ready => {
                    // SAFETY: this is the last pool reference after the owner
                    // entered RETIRING, so no old probe can still be inside a
                    // bit read and no new probe can attach.
                    let _ = unsafe { runtime_slot.retire_ready_after_quiescence(generation) };
                }
                RuntimeFilterState::Building => {
                    let _ = runtime_slot.disable_build(generation);
                }
                RuntimeFilterState::Free | RuntimeFilterState::Disabled => {}
            }
        }
        clear_slot_metadata(slot);
        slot.meta.store(
            pack_pool_word(SLOT_FREE, 0, publication_epoch),
            Ordering::Release,
        );
    }

    #[cfg(test)]
    pub(crate) fn initialize_slot_for_test(self, slot_index: u32, target: RuntimeFilterTarget) {
        let slot = unsafe { self.slot(slot_index) };
        let current = slot.meta.load(Ordering::Acquire);
        assert_eq!(pool_word_state(current), SLOT_FREE);
        assert_eq!(pool_word_refs(current), 0);
        let publication_epoch = pool_word_epoch(current)
            .checked_add(1)
            .expect("test publication epoch should not overflow");
        slot.meta
            .compare_exchange(
                current,
                pack_pool_word(SLOT_INITIALIZING, 0, publication_epoch),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("test slot should be free");
        store_exec_id(slot, target.exec_id);
        slot.scan_id.store(target.scan_id, Ordering::Release);
        slot.output_column
            .store(target.output_column, Ordering::Release);
        slot.key_type
            .store(target.key_type as u32, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn refs_for_test(self, slot_index: u32) -> u32 {
        let slot = unsafe { self.slot(slot_index) };
        pool_word_refs(slot.meta.load(Ordering::Acquire))
    }

    unsafe fn slot(self, slot_index: u32) -> &'static PoolSlot {
        debug_assert!(slot_index < self.config.slot_count);
        &*self
            .slots
            .expect("attached pool must have slots")
            .as_ptr()
            .add(slot_index as usize)
    }

    unsafe fn bits_for_slot(self, slot_index: u32) -> &'static [AtomicU64] {
        debug_assert!(slot_index < self.config.slot_count);
        let offset = slot_index as usize * self.config.params.word_count();
        std::slice::from_raw_parts(
            self.bits
                .expect("attached pool must have bits")
                .as_ptr()
                .add(offset),
            self.config.params.word_count(),
        )
    }

    unsafe fn runtime_slot(
        self,
        slot_index: u32,
    ) -> Result<RuntimeFilterSlot<'static>, RuntimeFilterPoolAttachError> {
        let slot = self.slot(slot_index);
        Ok(RuntimeFilterSlot::new(
            &slot.header,
            self.bits_for_slot(slot_index),
            self.config.params,
        )?)
    }
}

fn store_exec_id(slot: &PoolSlot, exec_id: RuntimeFilterExecId) {
    slot.exec_slot_id
        .store(exec_id.slot_id(), Ordering::Release);
    slot.exec_generation
        .store(exec_id.generation(), Ordering::Release);
    slot.exec_lease_epoch
        .store(exec_id.lease_epoch(), Ordering::Release);
    slot.exec_session_epoch
        .store(exec_id.session_epoch(), Ordering::Release);
}

fn load_exec_id(slot: &PoolSlot) -> RuntimeFilterExecId {
    RuntimeFilterExecId::new(
        slot.exec_slot_id.load(Ordering::Acquire),
        slot.exec_generation.load(Ordering::Acquire),
        slot.exec_lease_epoch.load(Ordering::Acquire),
        slot.exec_session_epoch.load(Ordering::Acquire),
    )
}

fn clear_exec_id(slot: &PoolSlot) {
    store_exec_id(slot, RuntimeFilterExecId::default());
}

fn clear_slot_metadata(slot: &PoolSlot) {
    clear_exec_id(slot);
    slot.scan_id.store(0, Ordering::Release);
    slot.output_column.store(0, Ordering::Release);
    slot.key_type.store(0, Ordering::Release);
    slot.generation.store(0, Ordering::Release);
}

#[derive(Debug)]
pub struct RuntimeFilterBuildHandle {
    pool: RuntimeFilterPool,
    slot_index: u32,
    generation: u64,
    released: bool,
}

unsafe impl Send for RuntimeFilterBuildHandle {}
unsafe impl Sync for RuntimeFilterBuildHandle {}

impl RuntimeFilterBuildHandle {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn insert_hash(&self, hash: u64) -> Result<(), RuntimeFilterPoolAttachError> {
        self.pool
            .insert_hash(self.slot_index, self.generation, hash)
    }

    pub fn publish_ready(&self) -> Result<(), RuntimeFilterPoolAttachError> {
        self.pool.publish_ready(self.slot_index, self.generation)
    }

    pub fn disable_build(&self) -> Result<(), RuntimeFilterPoolAttachError> {
        self.pool.disable_build(self.slot_index, self.generation)
    }

    pub fn release_owner(&mut self) {
        if !self.released {
            self.released = true;
            self.pool.release_owner(self.slot_index);
        }
    }
}

impl Drop for RuntimeFilterBuildHandle {
    fn drop(&mut self) {
        self.release_owner();
    }
}

#[derive(Debug)]
pub struct RuntimeFilterProbeHandle {
    pool: RuntimeFilterPool,
    slot_index: u32,
    generation: u64,
    output_column: u32,
    key_type: RuntimeFilterKeyType,
    released: bool,
}

unsafe impl Send for RuntimeFilterProbeHandle {}
unsafe impl Sync for RuntimeFilterProbeHandle {}

impl RuntimeFilterProbeHandle {
    pub fn output_column(&self) -> u32 {
        self.output_column
    }

    pub fn key_type(&self) -> RuntimeFilterKeyType {
        self.key_type
    }

    pub fn decision_for_hash(&self, hash: u64) -> ProbeDecision {
        let Ok(runtime_slot) = (unsafe { self.pool.runtime_slot(self.slot_index) }) else {
            return ProbeDecision::PassUnfiltered;
        };
        runtime_slot.probe(self.generation).decision_for_hash(hash)
    }

    pub fn decision_for_null(&self) -> ProbeDecision {
        let Ok(runtime_slot) = (unsafe { self.pool.runtime_slot(self.slot_index) }) else {
            return ProbeDecision::PassUnfiltered;
        };
        runtime_slot.probe(self.generation).decision_for_null()
    }

    pub fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.pool.release_ref(self.slot_index);
        }
    }
}

impl Drop for RuntimeFilterProbeHandle {
    fn drop(&mut self) {
        self.release();
    }
}

fn total_word_count(config: RuntimeFilterPoolConfig) -> usize {
    config.slot_count as usize * config.params.word_count()
}

unsafe fn validate_region(
    base: *mut u8,
    len: usize,
    layout: Layout,
) -> Result<(), RuntimeFilterPoolAttachError> {
    let Some(base) = NonNull::new(base) else {
        return Err(RuntimeFilterPoolAttachError::NullBase);
    };
    let actual_align = base.as_ptr() as usize & (layout.align() - 1);
    if actual_align != 0 {
        return Err(RuntimeFilterPoolAttachError::Misaligned {
            required: layout.align(),
            actual: actual_align,
        });
    }
    if len < layout.size() {
        return Err(RuntimeFilterPoolAttachError::TooSmall {
            required: layout.size(),
            actual: len,
        });
    }
    Ok(())
}
