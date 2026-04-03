use crate::fsm::{BackendCoordinatorState, BackendProducerState, WorkerScanState};
use crate::{FlowId, ProducerId};
use issuance::IssuedRxError;
use std::fmt;
use thiserror::Error;

/// Validation errors for one [`crate::ScanOpen`] descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ScanOpenError {
    #[error("scan open must declare at least one producer")]
    EmptyProducerSet,
    #[error("duplicate producer id {producer_id} in scan open")]
    DuplicateProducerId { producer_id: ProducerId },
    #[error("scan open may declare at most one leader producer")]
    MultipleLeaders,
}

/// Producer-runtime misuse and open/close errors.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum BackendProducerError {
    #[error("invalid scan open: {0}")]
    InvalidScanOpen(#[from] ScanOpenError),
    #[error("cannot {action} backend producer while in state {state:?}")]
    InvalidState {
        action: &'static str,
        state: BackendProducerState,
    },
    #[error("producer id {producer_id} is not declared in scan open")]
    UnknownProducer { producer_id: ProducerId },
    #[error("failed to open backend page source: {0}")]
    SourceOpen(String),
    #[error("failed to close backend page source: {0}")]
    SourceClose(String),
}

/// Terminal error surfaced by a backend producer step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendStreamError {
    Source(String),
    Transport(String),
    InvalidPayloadLen { actual: usize, capacity: usize },
}

impl fmt::Display for BackendStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(message) => write!(f, "source error: {message}"),
            Self::Transport(message) => write!(f, "transport error: {message}"),
            Self::InvalidPayloadLen { actual, capacity } => write!(
                f,
                "source returned invalid payload length {actual} for page capacity {capacity}"
            ),
        }
    }
}

/// Logical coordinator misuse errors.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum BackendCoordinatorError {
    #[error("invalid scan open: {0}")]
    InvalidScanOpen(#[from] ScanOpenError),
    #[error("cannot {action} backend coordinator while in state {state:?}")]
    InvalidState {
        action: &'static str,
        state: BackendCoordinatorState,
    },
    #[error("unexpected flow id {actual:?}, expected {expected:?}")]
    UnexpectedFlow { expected: FlowId, actual: FlowId },
    #[error("producer id {producer_id} is not active in this logical scan")]
    UnknownProducer { producer_id: ProducerId },
    #[error("producer id {producer_id} is already terminal")]
    ProducerAlreadyTerminal { producer_id: ProducerId },
}

/// Worker-runtime misuse and retryable ingress errors.
#[derive(Debug, Error)]
pub enum WorkerRoleError {
    #[error("invalid scan open: {0}")]
    InvalidScanOpen(#[from] ScanOpenError),
    #[error("cannot {action} worker scan while in state {state:?}")]
    InvalidState {
        action: &'static str,
        state: WorkerScanState,
    },
    #[error("unexpected flow id {actual:?}, expected {expected:?}")]
    UnexpectedFlow { expected: FlowId, actual: FlowId },
    #[error("producer id {producer_id} is not active in this logical scan")]
    UnknownProducer { producer_id: ProducerId },
    #[error("producer id {producer_id} is already terminal")]
    ProducerAlreadyTerminal { producer_id: ProducerId },
    #[error("ingress transport is not ready for this frame: {0}")]
    Ingress(#[from] IssuedRxError),
}
