use thiserror::Error;

// TODO: replace String with SmolStr
#[derive(Error, Debug)]
pub(crate) enum FusionError {
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
    #[error("Failed to parse the query: {0}")]
    ParseQuery(String),
    #[error("Payload is too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("Failed to deserialize {0}: {1}")]
    DeserializeU8(String, u8),
}
