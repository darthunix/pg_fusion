//! Versioned wire-message families and top-level message enums.
//!
//! The protocol distinguishes execution control from scan control at the wire
//! level:
//!
//! - backend-to-worker execution control is carried only on the primary slot
//! - backend-to-worker scan terminals are carried only on dedicated scan slots
//! - worker-to-backend execution control is carried only on the primary slot
//! - worker-to-backend scan control is carried only on dedicated scan slots

use crate::error::DecodeError;
use crate::scan::{
    PlanFlowDescriptor, ScanChannelSet, ScanChannelSetRef, ScanFlowDescriptor,
    ScanFlowDescriptorRef,
};

/// Runtime wire-message family carried in the fixed binary envelope header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeMessageFamily {
    /// Backend execution control sent to the worker on the primary slot.
    BackendExecutionToWorker = 1,
    /// Worker execution control sent back to the backend on the primary slot.
    WorkerExecutionToBackend = 2,
    /// Worker scan control sent back to the backend on dedicated scan slots.
    WorkerScanToBackend = 3,
    /// Backend scan terminal signals sent to the worker on dedicated scan slots.
    BackendScanToWorker = 4,
}

impl TryFrom<u8> for RuntimeMessageFamily {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::BackendExecutionToWorker),
            2 => Ok(Self::WorkerExecutionToBackend),
            3 => Ok(Self::WorkerScanToBackend),
            4 => Ok(Self::BackendScanToWorker),
            actual => Err(DecodeError::UnexpectedMessageFamily { actual }),
        }
    }
}

/// Versioned failure codes for runtime control-plane failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ExecutionFailureCode {
    /// Execution was cancelled explicitly.
    Cancelled = 1,
    /// The peer violated the runtime protocol contract.
    ProtocolViolation = 2,
    /// Transport restarted while the execution was in flight.
    TransportRestarted = 3,
    /// Execution failed locally for an internal reason.
    Internal = 4,
}

impl TryFrom<u8> for ExecutionFailureCode {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Cancelled),
            2 => Ok(Self::ProtocolViolation),
            3 => Ok(Self::TransportRestarted),
            4 => Ok(Self::Internal),
            actual => Err(DecodeError::InvalidFailureCode { actual }),
        }
    }
}

/// Query-scoped worker execution options captured by the backend at start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionOptionsWire {
    pub scan_batch_channel_capacity: u32,
    pub scan_idle_poll_interval_us: u32,
}

impl Default for ExecutionOptionsWire {
    fn default() -> Self {
        Self {
            scan_batch_channel_capacity: 8,
            scan_idle_poll_interval_us: 100,
        }
    }
}

/// Encode-side backend execution control messages carried on the primary slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionToWorker<'a> {
    /// Start one execution with its plan descriptor and published scan channels.
    StartExecution {
        session_epoch: u64,
        plan: PlanFlowDescriptor,
        options: ExecutionOptionsWire,
        scans: ScanChannelSet<'a>,
    },
    /// Cancel one execution identified by `session_epoch`.
    CancelExecution { session_epoch: u64 },
    /// Fail one execution identified by `session_epoch`.
    FailExecution {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
}

impl BackendExecutionToWorker<'_> {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::StartExecution { session_epoch, .. }
            | Self::CancelExecution { session_epoch }
            | Self::FailExecution { session_epoch, .. } => session_epoch,
        }
    }
}

/// Decode-side borrowed backend execution control messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionToWorkerRef<'a> {
    /// Borrowed `StartExecution` view with validated borrowed scan-channel set.
    StartExecution {
        session_epoch: u64,
        plan: PlanFlowDescriptor,
        options: ExecutionOptionsWire,
        scans: ScanChannelSetRef<'a>,
    },
    /// Borrowed `CancelExecution` view.
    CancelExecution { session_epoch: u64 },
    /// Borrowed `FailExecution` view.
    FailExecution {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
}

impl BackendExecutionToWorkerRef<'_> {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::StartExecution { session_epoch, .. }
            | Self::CancelExecution { session_epoch }
            | Self::FailExecution { session_epoch, .. } => session_epoch,
        }
    }
}

/// Execution-level worker-to-backend control sent only on the primary slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerExecutionToBackend {
    /// Mark one execution as complete.
    CompleteExecution { session_epoch: u64 },
    /// Mark one execution as failed.
    FailExecution {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
}

impl WorkerExecutionToBackend {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::CompleteExecution { session_epoch }
            | Self::FailExecution { session_epoch, .. } => session_epoch,
        }
    }
}

/// Scan-level worker-to-backend control sent only on dedicated scan slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanToBackend<'a> {
    /// Open one scan identified by `scan_id`.
    OpenScan {
        session_epoch: u64,
        scan_id: u64,
        scan: ScanFlowDescriptor<'a>,
    },
    /// Cancel one scan identified by `scan_id`.
    CancelScan { session_epoch: u64, scan_id: u64 },
}

impl WorkerScanToBackend<'_> {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::OpenScan { session_epoch, .. } | Self::CancelScan { session_epoch, .. } => {
                session_epoch
            }
        }
    }
}

/// Borrowed decode-side scan-level worker-to-backend control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanToBackendRef<'a> {
    /// Borrowed `OpenScan` view with validated borrowed producer set.
    OpenScan {
        session_epoch: u64,
        scan_id: u64,
        scan: ScanFlowDescriptorRef<'a>,
    },
    /// Borrowed `CancelScan` view.
    CancelScan { session_epoch: u64, scan_id: u64 },
}

impl WorkerScanToBackendRef<'_> {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::OpenScan { session_epoch, .. } | Self::CancelScan { session_epoch, .. } => {
                session_epoch
            }
        }
    }
}

/// Scan-level backend-to-worker terminal control carried only on dedicated scan slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendScanToWorker<'a> {
    /// One producer reached logical EOF.
    ScanFinished {
        session_epoch: u64,
        scan_id: u64,
        producer_id: u16,
    },
    /// One producer failed and terminated the logical scan.
    ///
    /// `message` is bounded to
    /// [`crate::MAX_SCAN_FAILURE_MESSAGE_LEN`] UTF-8 bytes because this
    /// terminal still rides over one framed `control_transport` slot.
    ScanFailed {
        session_epoch: u64,
        scan_id: u64,
        producer_id: u16,
        message: &'a str,
    },
}

impl BackendScanToWorker<'_> {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::ScanFinished { session_epoch, .. } | Self::ScanFailed { session_epoch, .. } => {
                session_epoch
            }
        }
    }
}

/// Borrowed decode-side scan-level backend-to-worker terminal control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendScanToWorkerRef<'a> {
    /// Borrowed `ScanFinished` view.
    ScanFinished {
        session_epoch: u64,
        scan_id: u64,
        producer_id: u16,
    },
    /// Borrowed `ScanFailed` view.
    ScanFailed {
        session_epoch: u64,
        scan_id: u64,
        producer_id: u16,
        message: &'a str,
    },
}

impl BackendScanToWorkerRef<'_> {
    /// Return the `session_epoch` targeted by this message.
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::ScanFinished { session_epoch, .. } | Self::ScanFailed { session_epoch, .. } => {
                session_epoch
            }
        }
    }
}
