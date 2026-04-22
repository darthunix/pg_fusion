//! Versioned wire-message families and top-level message enums.
//!
//! The protocol distinguishes execution control from scan control at the wire
//! level:
//!
//! - backend-to-worker execution control is carried only on the primary slot
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
}

impl TryFrom<u8> for RuntimeMessageFamily {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::BackendExecutionToWorker),
            2 => Ok(Self::WorkerExecutionToBackend),
            3 => Ok(Self::WorkerScanToBackend),
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

/// Encode-side backend execution control messages carried on the primary slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionToWorker<'a> {
    /// Start one execution with its plan descriptor and published scan channels.
    StartExecution {
        session_epoch: u64,
        plan: PlanFlowDescriptor,
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
