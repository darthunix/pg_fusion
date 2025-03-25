use thiserror::Error;

// TODO: replace String with SmolStr
#[derive(Error, Debug)]
pub enum FusionError {
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
    #[error("Failed to parse the query: {0}")]
    ParseQuery(String),
    #[error("Payload is too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("Failed to deserialize {0}: {1}")]
    Deserialize(String, u64),
    #[error("{0} not found: {1}")]
    NotFound(String, String),
    #[error("{0} name is not valid: {1}")]
    InvalidName(String, String),
}
