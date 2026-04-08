mod backend;
mod layout;
mod raw;
mod shared;
mod worker;

use crate::ring::FramedRing;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64};

use self::layout::{compute_layout, BankComputedLayout};

pub(super) const CONTROL_IPC_MAGIC: u64 = 0x4349_5043_4E54_5231;
pub(super) const CONTROL_IPC_VERSION: u32 = 2;
pub(super) const BANK_COUNT: usize = 2;
pub(super) const LEASE_STATE_FREE: u32 = 0;
pub(super) const LEASE_STATE_LEASED: u32 = 1;
pub(super) const WORKER_CLAIM_NONE: u64 = 0;
pub(super) const WORKER_CLAIM_BLOCKED: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Shared-memory size and alignment requirements for one `control_ipc` region.
///
/// The computed layout includes both generation banks, the global worker PID
/// cell, the global region generation, and the per-bank slot freelists/rings.
pub struct ControlRegionLayout {
    /// Total region size in bytes.
    pub size: usize,
    /// Required base alignment for the region.
    pub align: usize,
    slot_count: u32,
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
}

impl ControlRegionLayout {
    /// Computes the full shared-memory layout for a `control_ipc` region.
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
/// Process-local view over one initialized shared-memory control region.
///
/// One region contains two banks. The worker publishes the currently active
/// bank through a monotonically increasing `region_generation`, and all
/// backend/worker handles are bound to that generation.
pub struct ControlRegion {
    region_generation: NonNull<AtomicU64>,
    worker_pid: NonNull<AtomicI32>,
    banks_base: NonNull<u8>,
    slot_count: u32,
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
    bank_stride: usize,
    bank_layout: BankComputedLayout,
}

unsafe impl Send for ControlRegion {}
unsafe impl Sync for ControlRegion {}

pub struct BackendSlotLease {
    region: ControlRegion,
    generation: u64,
    bank_index: usize,
    slot_id: u32,
    active: bool,
}

/// Worker-side process-local attachment to the control region.
///
/// This type does not hold a slot claim by itself. Claims are created through
/// [`WorkerControl::slot`] and remain alive until the returned [`WorkerSlot`]
/// and its derived wrappers are dropped.
pub struct WorkerControl {
    region: ControlRegion,
}

/// Exclusive worker-side claim for one `(slot_id, session_epoch)` pair inside
/// one `region_generation`.
///
/// Worker receive/send wrappers borrowed from this slot are session-bound and
/// fail once the slot is released, the epoch changes, or the worker activates a
/// new region generation.
pub struct WorkerSlot<'a> {
    region: &'a ControlRegion,
    generation: u64,
    bank_index: usize,
    slot_id: u32,
    claimed_epoch: u64,
}

/// Iterator over backend slots that currently have pending backend-to-worker
/// traffic in the active generation.
pub struct ReadySlots<'a> {
    control: &'a WorkerControl,
    generation: u64,
    bank_index: Option<usize>,
    next: u32,
}

/// Low-level sender for one framed control ring direction.
///
/// This raw type is payload-oriented only. Higher layers usually use the
/// generation/session-bound backend or worker wrappers instead.
pub struct ControlTx<'a> {
    ring: FramedRing<'a>,
    ready_flag: &'a AtomicBool,
    peer_pid: &'a AtomicI32,
}

/// Low-level receiver for one framed control ring direction.
pub struct ControlRx<'a> {
    ring: FramedRing<'a>,
    ready_flag: &'a AtomicBool,
}

/// Backend-owned sender for the current generation.
pub struct BackendTx<'lease, 'region> {
    lease: &'lease BackendSlotLease,
    inner: ControlTx<'region>,
}

/// Backend-owned receiver for the current generation.
pub struct BackendRx<'lease, 'region> {
    lease: &'lease BackendSlotLease,
    inner: ControlRx<'region>,
}

/// Worker-owned sender tied to one claimed slot and session epoch.
pub struct WorkerTx<'slot, 'region> {
    slot: &'slot WorkerSlot<'region>,
    inner: ControlTx<'region>,
}

/// Worker-owned receiver tied to one claimed slot and session epoch.
pub struct WorkerRx<'slot, 'region> {
    slot: &'slot WorkerSlot<'region>,
    inner: ControlRx<'region>,
}

/// Raw reserved frame writer for one control ring.
///
/// A frame becomes visible only after [`FrameWriter::commit`]. Dropping an
/// uncommitted writer leaves the reservation invisible because the ring tail is
/// not advanced.
pub struct FrameWriter<'a> {
    data: NonNull<u8>,
    tail: &'a AtomicU32,
    ready_flag: &'a AtomicBool,
    peer_pid: &'a AtomicI32,
    payload_start: u32,
    payload_len: u32,
    publish_tail: u32,
    committed: bool,
    _marker: PhantomData<&'a mut [u8]>,
}

/// Backend-owned frame writer that revalidates the lease generation before
/// publish.
pub struct BackendFrameWriter<'tx, 'lease, 'region> {
    lease: &'lease BackendSlotLease,
    inner: FrameWriter<'tx>,
    _marker: PhantomData<&'region ()>,
}

/// Worker-owned frame writer that revalidates the claimed slot epoch before
/// publish.
pub struct WorkerFrameWriter<'tx, 'slot, 'region> {
    slot: &'slot WorkerSlot<'region>,
    inner: FrameWriter<'tx>,
}

/// Outcome of publishing one committed frame.
///
/// The frame is visible to the peer for all variants of this enum. Notification
/// is best-effort only.
#[derive(Debug)]
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
    worker_claim_epoch: &'a AtomicU64,
    lease_state: &'a AtomicU32,
    backend_pid: &'a AtomicI32,
    session_epoch: &'a AtomicU64,
    worker_pid: &'a AtomicI32,
}
