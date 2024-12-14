use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum FusionError {
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
}
