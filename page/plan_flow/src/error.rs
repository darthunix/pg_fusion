use crate::fsm::{BackendPlanState, WorkerPlanState};
use crate::FlowId;
use issuance::IssuedRxError;
use thiserror::Error;

/// Backend-role misuse and open/close errors.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum BackendPlanError {
    /// The caller attempted an operation that is incompatible with the current
    /// backend FSM state.
    #[error("cannot {action} backend plan flow while in state {state:?}")]
    InvalidState {
        action: &'static str,
        state: BackendPlanState,
    },
    /// Building the inner `plan_codec` encoder failed before streaming began.
    #[error("failed to initialize plan encoder: {0}")]
    EncoderInit(String),
    /// The outer backend FSM rejected an internal transition.
    #[error("backend plan FSM rejected transition: {0}")]
    StateMachine(String),
}

/// Worker-role misuse and retryable ingress errors.
#[derive(Debug, Error)]
pub enum WorkerPlanError {
    /// The caller attempted an operation that is incompatible with the current
    /// worker FSM state.
    #[error("cannot {action} worker plan flow while in state {state:?}")]
    InvalidState {
        action: &'static str,
        state: WorkerPlanState,
    },
    /// The incoming frame belongs to a different logical flow.
    #[error("unexpected flow id {actual:?}, expected {expected:?}")]
    UnexpectedFlow { expected: FlowId, actual: FlowId },
    /// The lower-level issued receiver is not ready to accept the frame yet.
    #[error("ingress transport is not ready for this frame: {0}")]
    Ingress(#[from] IssuedRxError),
    /// The outer worker FSM rejected an internal transition.
    #[error("worker plan FSM rejected transition: {0}")]
    StateMachine(String),
}
