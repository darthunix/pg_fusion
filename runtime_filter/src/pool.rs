use std::alloc::{Layout, LayoutError};
use std::error::Error;
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::{
    AtomicBloomRef, BloomAttachError, BloomParams, LifecycleError, ProbeDecision,
    RuntimeFilterHeader, RuntimeFilterSlot, RuntimeFilterState,
};

const POOL_MAGIC: u64 = 0x5047_4655_5246_5031;
/// Shared-memory pool format version.
pub const RUNTIME_FILTER_POOL_VERSION: u32 = 1;

const SLOT_FREE: u32 = 0;
const SLOT_ALLOCATED: u32 = 1;
const SLOT_RETIRING: u32 = 2;

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
}

impl RuntimeFilterKeyType {
    fn from_raw(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::Int16),
            2 => Some(Self::Int32),
            3 => Some(Self::Int64),
            _ => None,
        }
    }
}

/// Logical target that connects a worker-built filter to backend scan probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeFilterTarget {
    /// Session epoch that scopes scan identifiers.
    pub session_epoch: u64,
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
    state: AtomicU32,
    refs: AtomicU32,
    generation: AtomicU64,
    session_epoch: AtomicU64,
    scan_id: AtomicU64,
    output_column: AtomicU32,
    key_type: AtomicU32,
    header: RuntimeFilterHeader,
}

impl PoolSlot {
    fn new() -> Self {
        Self {
            state: AtomicU32::new(SLOT_FREE),
            refs: AtomicU32::new(0),
            generation: AtomicU64::new(0),
            session_epoch: AtomicU64::new(0),
            scan_id: AtomicU64::new(0),
            output_column: AtomicU32::new(0),
            key_type: AtomicU32::new(0),
            header: RuntimeFilterHeader::free(),
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
            if slot
                .state
                .compare_exchange(
                    SLOT_FREE,
                    SLOT_ALLOCATED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }

            slot.refs.store(1, Ordering::Release);
            slot.session_epoch
                .store(target.session_epoch, Ordering::Release);
            slot.scan_id.store(target.scan_id, Ordering::Release);
            slot.output_column
                .store(target.output_column, Ordering::Release);
            slot.key_type
                .store(target.key_type as u32, Ordering::Release);

            let runtime_slot = unsafe { self.runtime_slot(slot_index)? };
            match runtime_slot.try_acquire_builder() {
                Ok(builder) => {
                    let generation = builder.detach();
                    slot.generation.store(generation, Ordering::Release);
                    return Ok(Some(RuntimeFilterBuildHandle {
                        pool: self,
                        slot_index,
                        generation,
                        released: false,
                    }));
                }
                Err(err) => {
                    slot.refs.store(0, Ordering::Release);
                    slot.state.store(SLOT_FREE, Ordering::Release);
                    return Err(err.into());
                }
            }
        }

        Ok(None)
    }

    /// Find probe handles matching `(session_epoch, scan_id)`.
    ///
    /// Matching handles are pushed into `probes` and hold pool references until
    /// dropped.
    pub fn lookup_probes(
        self,
        session_epoch: u64,
        scan_id: u64,
        probes: &mut Vec<RuntimeFilterProbeHandle>,
    ) {
        if !self.is_attached() {
            return;
        }

        for slot_index in 0..self.config.slot_count {
            let slot = unsafe { self.slot(slot_index) };
            if slot.state.load(Ordering::Acquire) != SLOT_ALLOCATED {
                continue;
            }
            slot.refs.fetch_add(1, Ordering::AcqRel);

            let state = slot.state.load(Ordering::Acquire);
            let matches = state == SLOT_ALLOCATED
                && slot.session_epoch.load(Ordering::Acquire) == session_epoch
                && slot.scan_id.load(Ordering::Acquire) == scan_id;
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
        let _ = slot.state.compare_exchange(
            SLOT_ALLOCATED,
            SLOT_RETIRING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.release_ref(slot_index);
    }

    fn release_ref(self, slot_index: u32) {
        let slot = unsafe { self.slot(slot_index) };
        let old_refs = slot.refs.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(old_refs > 0);
        if old_refs == 1 && slot.state.load(Ordering::Acquire) == SLOT_RETIRING {
            let generation = slot.generation.load(Ordering::Acquire);
            if let Ok(runtime_slot) = unsafe { self.runtime_slot(slot_index) } {
                match runtime_slot.snapshot().state {
                    RuntimeFilterState::Ready => {
                        // SAFETY: this is the last pool reference after the
                        // owner entered RETIRING, so no old probe can still be
                        // inside a bit read and no new probe can attach.
                        let _ = unsafe { runtime_slot.retire_ready_after_quiescence(generation) };
                    }
                    RuntimeFilterState::Building => {
                        let _ = runtime_slot.disable_build(generation);
                    }
                    RuntimeFilterState::Free | RuntimeFilterState::Disabled => {}
                }
            }
            slot.session_epoch.store(0, Ordering::Release);
            slot.scan_id.store(0, Ordering::Release);
            slot.output_column.store(0, Ordering::Release);
            slot.key_type.store(0, Ordering::Release);
            slot.generation.store(0, Ordering::Release);
            slot.state.store(SLOT_FREE, Ordering::Release);
        }
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
