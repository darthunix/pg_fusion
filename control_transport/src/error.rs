use std::error::Error;
use std::fmt;
use std::io;

use crate::region::TransportRegionLayout;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    ZeroSlotCount,
    RingTooSmall { minimum: usize, actual: usize },
    RingTooLarge { maximum: usize, actual: usize },
    LayoutOverflow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitError {
    InvalidConfig(ConfigError),
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
    AlreadyInitialized,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReinitError {
    InvalidExistingRegion(AttachError),
    LayoutMismatch {
        existing: TransportRegionLayout,
        requested: TransportRegionLayout,
    },
    BackendProbeFailed {
        slot_id: u32,
        error_kind: io::ErrorKind,
        raw_os_error: Option<i32>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachError {
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
    BadMagic { expected: u64, actual: u64 },
    UnsupportedVersion { expected: u32, actual: u32 },
    InvalidConfig(ConfigError),
    LayoutMismatch { expected: usize, actual: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquireError {
    WorkerOffline,
    BackendProbeFailed {
        slot_id: u32,
        error_kind: io::ErrorKind,
        raw_os_error: Option<i32>,
    },
    Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerLifecycleError {
    HandlesAlive { live_slots: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerAttachError {
    RegionAlreadyAttached {
        existing_region_key: usize,
        requested_region_key: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseError {
    StaleGeneration {
        slot_id: u32,
        claimed_generation: u64,
        current_generation: u64,
    },
    StaleLeaseEpoch {
        slot_id: u32,
        claimed_generation: u64,
        claimed_lease_epoch: u64,
        current_lease_epoch: u64,
    },
    Released {
        slot_id: u32,
        claimed_generation: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotAccessError {
    BadSlotId {
        slot_id: u32,
        slot_count: u32,
    },
    Busy {
        slot_id: u32,
        claimed_generation: u64,
    },
    WorkerOffline,
    StaleGeneration {
        slot_id: u32,
        claimed_generation: u64,
        current_generation: u64,
    },
    StaleLeaseEpoch {
        slot_id: u32,
        claimed_generation: u64,
        claimed_lease_epoch: u64,
        current_lease_epoch: u64,
    },
    BackendProbeFailed {
        slot_id: u32,
        claimed_generation: u64,
        error_kind: io::ErrorKind,
        raw_os_error: Option<i32>,
    },
    Released {
        slot_id: u32,
        claimed_generation: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxError {
    PayloadTooLarge { actual: usize, maximum: usize },
    Full { required: usize, available: usize },
    CorruptState { head: u32, tail: u32, capacity: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RxError {
    BufferTooSmall {
        required: usize,
        available: usize,
    },
    CorruptState {
        head: u32,
        tail: u32,
        capacity: u32,
    },
    CorruptFrameLen {
        len: u32,
        available: usize,
        capacity: u32,
    },
}

#[derive(Debug)]
pub enum NotifyError {
    Signal(io::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendTxError {
    Lease(LeaseError),
    Ring(TxError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendRxError {
    Lease(LeaseError),
    Ring(RxError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerTxError {
    Slot(SlotAccessError),
    Ring(TxError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerRxError {
    Slot(SlotAccessError),
    Ring(RxError),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSlotCount => write!(f, "slot count must be non-zero"),
            Self::RingTooSmall { minimum, actual } => {
                write!(
                    f,
                    "ring capacity {actual} is smaller than minimum {minimum}"
                )
            }
            Self::RingTooLarge { maximum, actual } => {
                write!(
                    f,
                    "ring capacity {actual} exceeds maximum encodable size {maximum}"
                )
            }
            Self::LayoutOverflow => write!(f, "control_transport layout overflowed usize"),
        }
    }
}

impl Error for ConfigError {}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(err) => write!(f, "invalid control_transport config: {err}"),
            Self::RegionTooSmall { expected, actual } => write!(
                f,
                "control_transport region too small: expected at least {expected} bytes, got {actual}"
            ),
            Self::BadAlignment { expected, actual } => write!(
                f,
                "control_transport region base 0x{actual:x} is not aligned to {expected} bytes"
            ),
            Self::AlreadyInitialized => write!(
                f,
                "control_transport region is already initialized; use reinit_in_place() for same-layout reset"
            ),
        }
    }
}

impl Error for InitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfig(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for ReinitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExistingRegion(err) => {
                write!(f, "cannot reinitialize invalid control_transport region: {err}")
            }
            Self::LayoutMismatch { existing, requested } => write!(
                f,
                "control_transport reinit requires the same layout; existing slots={} b2w_cap={} w2b_cap={}, requested slots={} b2w_cap={} w2b_cap={}",
                existing.slot_count(),
                existing.backend_to_worker_capacity(),
                existing.worker_to_backend_capacity(),
                requested.slot_count(),
                requested.backend_to_worker_capacity(),
                requested.worker_to_backend_capacity(),
            ),
            Self::BackendProbeFailed {
                slot_id,
                error_kind,
                raw_os_error,
            } => write!(
                f,
                "control_transport reinit backend liveness probe failed for slot {slot_id}: {error_kind} (raw_os_error={raw_os_error:?})"
            ),
        }
    }
}

impl Error for ReinitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidExistingRegion(err) => Some(err),
            Self::LayoutMismatch { .. } => None,
            Self::BackendProbeFailed { .. } => None,
        }
    }
}

impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegionTooSmall { expected, actual } => write!(
                f,
                "control_transport region too small: expected at least {expected} bytes, got {actual}"
            ),
            Self::BadAlignment { expected, actual } => write!(
                f,
                "control_transport region base 0x{actual:x} is not aligned to {expected} bytes"
            ),
            Self::BadMagic { expected, actual } => write!(
                f,
                "unexpected control_transport magic 0x{actual:x}, expected 0x{expected:x}"
            ),
            Self::UnsupportedVersion { expected, actual } => write!(
                f,
                "unsupported control_transport version {actual}, expected {expected}"
            ),
            Self::InvalidConfig(err) => write!(f, "invalid control_transport header config: {err}"),
            Self::LayoutMismatch { expected, actual } => write!(
                f,
                "control_transport region layout mismatch: expected {expected} bytes, got {actual}"
            ),
        }
    }
}

impl Error for AttachError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfig(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerOffline => write!(f, "control worker generation is not active"),
            Self::BackendProbeFailed {
                slot_id,
                error_kind,
                raw_os_error,
            } => write!(
                f,
                "backend liveness probe failed while reaping slot {slot_id}: kind={error_kind}, raw_os_error={raw_os_error:?}"
            ),
            Self::Empty => write!(f, "no backend transport slots are currently available"),
        }
    }
}

impl Error for AcquireError {}

impl fmt::Display for WorkerAttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegionAlreadyAttached {
                existing_region_key,
                requested_region_key,
            } => write!(
                f,
                "worker process already attached control_transport region 0x{existing_region_key:x}, cannot attach region 0x{requested_region_key:x}"
            ),
        }
    }
}

impl Error for WorkerAttachError {}

impl fmt::Display for WorkerLifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HandlesAlive { live_slots } => write!(
                f,
                "worker generation switch requires all local worker slots to be detached, but {live_slots} are still alive"
            ),
        }
    }
}

impl Error for WorkerLifecycleError {}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGeneration {
                slot_id,
                claimed_generation,
                current_generation,
            } => write!(
                f,
                "slot {slot_id} belongs to generation {claimed_generation}, but current generation is {current_generation}"
            ),
            Self::StaleLeaseEpoch {
                slot_id,
                claimed_generation,
                claimed_lease_epoch,
                current_lease_epoch,
            } => write!(
                f,
                "slot {slot_id} from generation {claimed_generation} belongs to lease epoch {claimed_lease_epoch}, but current lease epoch is {current_lease_epoch}"
            ),
            Self::Released {
                slot_id,
                claimed_generation,
            } => write!(
                f,
                "slot {slot_id} from generation {claimed_generation} is no longer leased"
            ),
        }
    }
}

impl Error for LeaseError {}

impl fmt::Display for SlotAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadSlotId { slot_id, slot_count } => write!(
                f,
                "slot id {slot_id} is out of range for slot_count {slot_count}"
            ),
            Self::Busy {
                slot_id,
                claimed_generation,
            } => write!(
                f,
                "slot {slot_id} from generation {claimed_generation} is already claimed by a worker"
            ),
            Self::WorkerOffline => write!(f, "control worker generation is not active"),
            Self::StaleGeneration {
                slot_id,
                claimed_generation,
                current_generation,
            } => write!(
                f,
                "slot {slot_id} handle claimed generation {claimed_generation}, but current generation is {current_generation}"
            ),
            Self::StaleLeaseEpoch {
                slot_id,
                claimed_generation,
                claimed_lease_epoch,
                current_lease_epoch,
            } => write!(
                f,
                "slot {slot_id} handle from generation {claimed_generation} claimed lease epoch {claimed_lease_epoch}, but current lease epoch is {current_lease_epoch}"
            ),
            Self::BackendProbeFailed {
                slot_id,
                claimed_generation,
                error_kind,
                raw_os_error,
            } => write!(
                f,
                "slot {slot_id} from generation {claimed_generation} could not probe backend liveness: kind={error_kind:?} raw_os_error={raw_os_error:?}"
            ),
            Self::Released {
                slot_id,
                claimed_generation,
            } => write!(
                f,
                "slot {slot_id} from generation {claimed_generation} is no longer leased"
            ),
        }
    }
}

impl Error for SlotAccessError {}

impl fmt::Display for TxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { actual, maximum } => write!(
                f,
                "frame payload length {actual} exceeds maximum encodable size {maximum}"
            ),
            Self::Full {
                required,
                available,
            } => write!(
                f,
                "control ring is full: need {required} bytes, only {available} available"
            ),
            Self::CorruptState {
                head,
                tail,
                capacity,
            } => write!(
                f,
                "control ring has corrupt state head={head} tail={tail} capacity={capacity}"
            ),
        }
    }
}

impl Error for TxError {}

impl fmt::Display for RxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { required, available } => write!(
                f,
                "destination buffer is too small for next frame: need {required} bytes, only {available} available"
            ),
            Self::CorruptState { head, tail, capacity } => write!(
                f,
                "control ring has corrupt state head={head} tail={tail} capacity={capacity}"
            ),
            Self::CorruptFrameLen { len, available, capacity } => write!(
                f,
                "control ring has corrupt frame length {len} with {available} available bytes and capacity {capacity}"
            ),
        }
    }
}

impl Error for RxError {}

impl fmt::Display for NotifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Signal(err) => write!(f, "failed to send SIGUSR1: {err}"),
        }
    }
}

impl Error for NotifyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Signal(err) => Some(err),
        }
    }
}

impl fmt::Display for BackendTxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lease(err) => write!(f, "{err}"),
            Self::Ring(err) => write!(f, "{err}"),
        }
    }
}

impl Error for BackendTxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Lease(err) => Some(err),
            Self::Ring(err) => Some(err),
        }
    }
}

impl fmt::Display for BackendRxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lease(err) => write!(f, "{err}"),
            Self::Ring(err) => write!(f, "{err}"),
        }
    }
}

impl Error for BackendRxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Lease(err) => Some(err),
            Self::Ring(err) => Some(err),
        }
    }
}

impl fmt::Display for WorkerTxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Slot(err) => write!(f, "{err}"),
            Self::Ring(err) => write!(f, "{err}"),
        }
    }
}

impl Error for WorkerTxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Slot(err) => Some(err),
            Self::Ring(err) => Some(err),
        }
    }
}

impl fmt::Display for WorkerRxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Slot(err) => write!(f, "{err}"),
            Self::Ring(err) => write!(f, "{err}"),
        }
    }
}

impl Error for WorkerRxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Slot(err) => Some(err),
            Self::Ring(err) => Some(err),
        }
    }
}
