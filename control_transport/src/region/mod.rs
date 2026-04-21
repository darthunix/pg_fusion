mod backend;
mod layout;
mod lifecycle;
mod raw;
mod shared;
mod worker;

use crate::ring::FramedRing;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};

use self::layout::{compute_layout, ComputedLayout};

pub(super) const CONTROL_TRANSPORT_MAGIC: u64 = 0x4354_5241_4E53_5031;
/// v10 replaces the old sentinel-based ring wrap protocol with split-frame
/// prefix/payload I/O and keeps the token-handoff slot lifecycle from v9.
pub(super) const CONTROL_TRANSPORT_VERSION: u32 = 10;
pub(super) const LEASE_STATE_FREE_PUBLISHED: u32 = 0;
pub(super) const LEASE_STATE_ACQUIRE_RESERVED: u32 = 1;
pub(super) const LEASE_STATE_LEASED: u32 = 2;
pub(super) const LEASE_STATE_FREE_PENDING: u32 = 3;
pub(super) const LEASE_STATE_FREE_PUSH_CLAIMED: u32 = 4;
pub(super) const LEASE_STATE_FREE_PUSHED: u32 = 5;
pub(super) const LEASE_STATE_FREE_POPPED: u32 = 6;
pub(super) const OWNER_BACKEND: u32 = 1 << 0;
pub(super) const OWNER_WORKER: u32 = 1 << 1;
pub(super) const OWNER_WORKER_PENDING: u32 = 1 << 2;
pub(super) const OWNER_ANY_WORKER: u32 = OWNER_WORKER | OWNER_WORKER_PENDING;
pub(super) const SLOT_META_OWNER_BITS: u32 = 3;
pub(super) const SLOT_META_STATE_BITS: u32 = 3;
pub(super) const SLOT_META_OWNER_MASK: u64 = (1u64 << SLOT_META_OWNER_BITS) - 1;
pub(super) const SLOT_META_STATE_SHIFT: u32 = SLOT_META_OWNER_BITS;
pub(super) const SLOT_META_STATE_MASK: u64 =
    ((1u64 << SLOT_META_STATE_BITS) - 1) << SLOT_META_STATE_SHIFT;
pub(super) const SLOT_META_EPOCH_SHIFT: u32 = SLOT_META_OWNER_BITS + SLOT_META_STATE_BITS;
pub(super) const SLOT_META_MAX_LEASE_EPOCH: u64 = u64::MAX >> SLOT_META_EPOCH_SHIFT;
pub(super) const WORKER_STATE_OFFLINE: u32 = 0;
pub(super) const WORKER_STATE_RESTARTING: u32 = 1;
pub(super) const WORKER_STATE_ONLINE: u32 = 2;
pub(super) const WORKER_STATE_REINITING: u32 = 3;
pub(super) const REGION_META_STATE_BITS: u32 = 2;
pub(super) const REGION_META_STATE_MASK: u64 = (1u64 << REGION_META_STATE_BITS) - 1;
pub(super) const REGION_META_GENERATION_SHIFT: u32 = REGION_META_STATE_BITS;
pub(super) const REGION_META_MAX_GENERATION: u64 = u64::MAX >> REGION_META_GENERATION_SHIFT;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Shared-memory size and alignment requirements for one `control_transport` region.
pub struct TransportRegionLayout {
    /// Total region size in bytes.
    pub size: usize,
    /// Required base alignment for the region.
    pub align: usize,
    slot_count: u32,
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
}

impl TransportRegionLayout {
    /// Computes the full shared-memory layout for one `control_transport` region.
    pub fn new(
        slot_count: u32,
        backend_to_worker_cap: usize,
        worker_to_backend_cap: usize,
    ) -> Result<Self, crate::ConfigError> {
        if slot_count == 0 {
            return Err(crate::ConfigError::ZeroSlotCount);
        }
        let computed = compute_layout(slot_count, backend_to_worker_cap, worker_to_backend_cap)?;
        Ok(Self {
            size: computed.region.size(),
            align: computed.region.align(),
            slot_count,
            backend_to_worker_cap,
            worker_to_backend_cap,
        })
    }

    pub fn slot_count(self) -> u32 {
        self.slot_count
    }

    pub fn backend_to_worker_capacity(self) -> usize {
        self.backend_to_worker_cap
    }

    pub fn worker_to_backend_capacity(self) -> usize {
        self.worker_to_backend_cap
    }
}

#[derive(Clone, Copy)]
/// Process-local view over one initialized shared-memory transport region.
///
/// Intentionally `Copy`: this is a lightweight handle to shared memory, not an
/// owning region allocation.
pub struct TransportRegion {
    base: NonNull<u8>,
    region_meta: NonNull<AtomicU64>,
    next_lease_epoch: NonNull<AtomicU64>,
    freelist_epoch: NonNull<AtomicU64>,
    worker_pid: NonNull<AtomicI32>,
    slot_count: u32,
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
    computed: ComputedLayout,
}

unsafe impl Send for TransportRegion {}
unsafe impl Sync for TransportRegion {}

/// Process-local backend lease for one raw transport slot in one worker generation.
pub struct BackendSlotLease {
    region: TransportRegion,
    incarnation: LeaseIncarnation,
    slot_id: u32,
    active: bool,
}

/// Stable identity for one leased backend slot incarnation.
///
/// A `slot_id` may be reused by a different backend after release, so higher
/// layers that retain state across cleanup boundaries must key that state by
/// `BackendLeaseId`, not by `slot_id` alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackendLeaseId {
    generation: u64,
    lease_epoch: u64,
}

impl BackendLeaseId {
    pub const fn new(generation: u64, lease_epoch: u64) -> Self {
        Self {
            generation,
            lease_epoch,
        }
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }
}

/// Stable peer key for one backend slot lease incarnation.
///
/// This combines the physical slot address with the lease incarnation that
/// currently owns it. Higher layers must retain this full key, not just
/// `slot_id`, whenever work can outlive the original backend lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackendLeaseSlot {
    slot_id: u32,
    lease_id: BackendLeaseId,
}

impl BackendLeaseSlot {
    pub const fn new(slot_id: u32, lease_id: BackendLeaseId) -> Self {
        Self { slot_id, lease_id }
    }

    pub const fn slot_id(self) -> u32 {
        self.slot_id
    }

    pub const fn lease_id(self) -> BackendLeaseId {
        self.lease_id
    }
}

/// Worker-side process-local attachment to the transport region.
pub struct WorkerTransport {
    region: TransportRegion,
}

/// Raw worker-side view over one leased slot in the current generation.
///
/// Construction is `unsafe` because the caller must guarantee exclusive SPSC
/// ownership for this slot and its ring directions.
pub struct WorkerSlot<'a> {
    region: &'a TransportRegion,
    incarnation: LeaseIncarnation,
    slot_id: u32,
    attached: bool,
}

/// Iterator over backend slots that currently have pending backend-to-worker
/// traffic in the current generation.
pub struct ReadySlots<'a> {
    transport: &'a WorkerTransport,
    generation: u64,
    next: u32,
}

/// Iterator over backend lease peers that currently have pending
/// backend-to-worker traffic in the current generation.
pub struct ReadyBackendLeases<'a> {
    transport: &'a WorkerTransport,
    generation: u64,
    next: u32,
}

impl From<LeaseIncarnation> for BackendLeaseId {
    fn from(value: LeaseIncarnation) -> Self {
        Self::new(value.generation, value.lease_epoch)
    }
}

/// Low-level framed transport sender for one ring direction.
pub struct ControlTx<'a> {
    ring: FramedRing<'a>,
    ready_flag: &'a AtomicBool,
    peer_pid: &'a AtomicI32,
}

/// Low-level framed transport receiver for one ring direction.
pub struct ControlRx<'a> {
    ring: FramedRing<'a>,
    ready_flag: &'a AtomicBool,
}

/// Backend-owned sender for the backend-to-worker ring.
pub struct BackendTx<'lease, 'region> {
    lease: &'lease BackendSlotLease,
    inner: ControlTx<'region>,
}

/// Backend-owned receiver for the worker-to-backend ring.
pub struct BackendRx<'lease, 'region> {
    lease: &'lease BackendSlotLease,
    inner: ControlRx<'region>,
}

/// Worker-owned sender for the worker-to-backend ring.
pub struct WorkerTx<'slot, 'region> {
    slot: &'slot WorkerSlot<'region>,
    inner: ControlTx<'region>,
}

/// Worker-owned receiver for the backend-to-worker ring.
pub struct WorkerRx<'slot, 'region> {
    slot: &'slot WorkerSlot<'region>,
    inner: ControlRx<'region>,
}

#[derive(Debug)]
/// Outcome of publishing one frame and optionally signaling the peer.
pub enum CommitOutcome {
    Notified,
    PeerMissing,
    NotifyFailed(crate::NotifyError),
}

#[derive(Clone, Copy)]
pub(super) struct SlotView<'a> {
    backend_to_worker: FramedRing<'a>,
    worker_to_backend: FramedRing<'a>,
    to_worker_ready: &'a AtomicBool,
    to_backend_ready: &'a AtomicBool,
    slot_generation: &'a AtomicU64,
    slot_meta: &'a AtomicU64,
    backend_pid: &'a AtomicI32,
    worker_pid: &'a AtomicI32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct RegionMeta {
    raw: u64,
}

impl RegionMeta {
    pub(super) const OFFLINE: Self = Self::new(0, WORKER_STATE_OFFLINE);

    pub(super) const fn new(generation: u64, worker_state: u32) -> Self {
        debug_assert!(worker_state <= REGION_META_STATE_MASK as u32);
        debug_assert!(generation <= REGION_META_MAX_GENERATION);
        Self {
            raw: (generation << REGION_META_GENERATION_SHIFT) | worker_state as u64,
        }
    }

    pub(super) const fn from_raw(raw: u64) -> Self {
        Self { raw }
    }

    pub(super) const fn raw(self) -> u64 {
        self.raw
    }

    pub(super) const fn generation(self) -> u64 {
        self.raw >> REGION_META_GENERATION_SHIFT
    }

    pub(super) const fn worker_state(self) -> u32 {
        (self.raw & REGION_META_STATE_MASK) as u32
    }

    pub(super) const fn is_online_generation(self, generation: u64) -> bool {
        generation != 0
            && self.generation() == generation
            && self.worker_state() == WORKER_STATE_ONLINE
    }

    pub(super) const fn is_reiniting(self) -> bool {
        self.worker_state() == WORKER_STATE_REINITING
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct SlotMeta {
    raw: u64,
}

impl SlotMeta {
    pub(super) const ACQUIRE_RESERVED: Self = Self::new(LEASE_STATE_ACQUIRE_RESERVED, 0, 0);
    pub(super) const FREE_PENDING: Self = Self::new(LEASE_STATE_FREE_PENDING, 0, 0);

    pub(super) const fn new(lease_state: u32, lease_epoch: u64, owner_mask: u32) -> Self {
        debug_assert!(lease_state < (1u32 << SLOT_META_STATE_BITS));
        debug_assert!(owner_mask & !(SLOT_META_OWNER_MASK as u32) == 0);
        debug_assert!(lease_epoch <= SLOT_META_MAX_LEASE_EPOCH);
        Self {
            raw: (lease_epoch << SLOT_META_EPOCH_SHIFT)
                | ((lease_state as u64) << SLOT_META_STATE_SHIFT)
                | owner_mask as u64,
        }
    }

    pub(super) const fn from_raw(raw: u64) -> Self {
        Self { raw }
    }

    pub(super) const fn raw(self) -> u64 {
        self.raw
    }

    pub(super) const fn lease_state(self) -> u32 {
        ((self.raw & SLOT_META_STATE_MASK) >> SLOT_META_STATE_SHIFT) as u32
    }

    pub(super) const fn lease_epoch(self) -> u64 {
        self.raw >> SLOT_META_EPOCH_SHIFT
    }

    pub(super) const fn owner_mask(self) -> u32 {
        (self.raw & SLOT_META_OWNER_MASK) as u32
    }

    pub(super) const fn is_leased(self) -> bool {
        self.lease_state() == LEASE_STATE_LEASED
    }

    pub(super) const fn is_free_published(self) -> bool {
        self.lease_state() == LEASE_STATE_FREE_PUBLISHED
    }

    pub(super) const fn is_acquire_reserved(self) -> bool {
        self.lease_state() == LEASE_STATE_ACQUIRE_RESERVED
    }

    pub(super) const fn is_free_pending(self) -> bool {
        self.lease_state() == LEASE_STATE_FREE_PENDING
    }

    pub(super) const fn is_free_popped(self) -> bool {
        self.lease_state() == LEASE_STATE_FREE_POPPED
    }

    pub(super) const fn is_free_push_claimed(self) -> bool {
        self.lease_state() == LEASE_STATE_FREE_PUSH_CLAIMED
    }

    pub(super) const fn is_free_pushed(self) -> bool {
        self.lease_state() == LEASE_STATE_FREE_PUSHED
    }

    pub(super) const fn is_current_epoch_published(self, freelist_epoch: u64) -> bool {
        self.is_free_published() && self.lease_epoch() == freelist_epoch
    }

    pub(super) const fn is_republishable_after_reinit(self, freelist_epoch: u64) -> bool {
        self.is_free_pending()
            || (self.is_free_published() && self.lease_epoch() != freelist_epoch)
            || (self.is_free_pushed() && self.lease_epoch() != freelist_epoch)
    }

    pub(super) const fn free_published(epoch: u64) -> Self {
        Self::new(LEASE_STATE_FREE_PUBLISHED, epoch, 0)
    }

    pub(super) const fn free_popped(epoch: u64) -> Self {
        Self::new(LEASE_STATE_FREE_POPPED, epoch, 0)
    }

    pub(super) const fn free_push_claimed(epoch: u64) -> Self {
        Self::new(LEASE_STATE_FREE_PUSH_CLAIMED, epoch, 0)
    }

    pub(super) const fn free_pushed(epoch: u64) -> Self {
        Self::new(LEASE_STATE_FREE_PUSHED, epoch, 0)
    }

    pub(super) const fn is_ownerless(self) -> bool {
        self.owner_mask() == 0
    }

    pub(super) const fn has_backend_owner(self) -> bool {
        self.owner_mask() & OWNER_BACKEND != 0
    }

    pub(super) const fn has_worker_owner(self) -> bool {
        self.owner_mask() & OWNER_WORKER != 0
    }

    pub(super) const fn has_any_worker_owner(self) -> bool {
        self.owner_mask() & OWNER_ANY_WORKER != 0
    }

    pub(super) const fn with_owner_mask(self, owner_mask: u32) -> Self {
        Self::new(self.lease_state(), self.lease_epoch(), owner_mask)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct LeaseIncarnation {
    pub(super) generation: u64,
    pub(super) lease_epoch: u64,
}

impl LeaseIncarnation {
    pub(super) const FREE: Self = Self {
        generation: 0,
        lease_epoch: 0,
    };

    pub(super) const fn new(generation: u64, lease_epoch: u64) -> Self {
        Self {
            generation,
            lease_epoch,
        }
    }

    pub(super) const fn is_free(self) -> bool {
        self.generation == 0 && self.lease_epoch == 0
    }
}
