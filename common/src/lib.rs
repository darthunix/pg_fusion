use smol_str::SmolStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FusionError {
    #[error("Buffer is too small: {0} bytes")]
    BufferTooSmall(usize),
    #[error("Unsupported type: {0}")]
    UnsupportedType(SmolStr),
    #[error("Failed to parse the query: {0}")]
    ParseQuery(SmolStr),
    #[error("Payload is too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("Failed to deserialize {0}: {1}")]
    Deserialize(SmolStr, u64),
    #[error("{0} not found: {1}")]
    NotFound(SmolStr, SmolStr),
    #[error("{0} name is not valid: {1}")]
    InvalidName(SmolStr, SmolStr),
    #[error("Failed to {0}: {1}")]
    FailedTo(SmolStr, SmolStr),
    #[error("Invalid header: {0}")]
    InvalidHeader(anyhow::Error),
    #[error("Invalid transition in FSM: {0}")]
    InvalidTransition(anyhow::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
