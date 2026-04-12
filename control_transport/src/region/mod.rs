mod backend;
mod layout;
mod raw;
mod shared;
mod worker;

use crate::ring::FramedRing;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64};

use self::layout::{compute_layout, ComputedLayout};

pub(super) const CONTROL_TRANSPORT_MAGIC: u64 = 0x4354_5241_4E53_5031;
pub(super) const CONTROL_TRANSPORT_VERSION: u32 = 4;
pub(super) const LEASE_STATE_FREE: u32 = 0;
pub(super) const LEASE_STATE_LEASED: u32 = 1;
pub(super) const OWNER_BACKEND: u32 = 1 << 0;
pub(super) const OWNER_WORKER: u32 = 1 << 1;
pub(super) const WORKER_STATE_OFFLINE: u32 = 0;
pub(super) const WORKER_STATE_RESTARTING: u32 = 1;
pub(super) const WORKER_STATE_ONLINE: u32 = 2;

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
pub struct TransportRegion {
    base: NonNull<u8>,
    region_generation: NonNull<AtomicU64>,
    worker_state: NonNull<AtomicU32>,
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
    generation: u64,
    slot_id: u32,
    active: bool,
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
    generation: u64,
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
    lease_state: &'a AtomicU32,
    slot_generation: &'a AtomicU64,
    owner_mask: &'a AtomicU32,
    backend_pid: &'a AtomicI32,
    worker_pid: &'a AtomicI32,
}
