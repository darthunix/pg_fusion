use smol_str::SmolStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FusionError {
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
}
