use arrow_layout::TypeTag;
use thiserror::Error;

/// Projector construction errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("tuple descriptor pointer is null")]
    NullTupleDesc,
    #[error("per-tuple memory context pointer is null")]
    NullPerTupleMemoryContext,
    #[error("schema column count mismatch: expected {expected}, got {actual}")]
    SchemaColumnCountMismatch { expected: usize, actual: usize },
    #[error("dropped tuple descriptor attribute at column {index} is not supported")]
    DroppedAttribute { index: usize },
    #[error(
        "PostgreSQL type mismatch at column {index}: oid {oid} is incompatible with {type_tag:?}"
    )]
    PgLayoutTypeMismatch {
        index: usize,
        oid: u32,
        type_tag: TypeTag,
    },
    #[error(
        "text-like layout columns require UTF-8 server encoding, got PostgreSQL encoding id {encoding}"
    )]
    NonUtf8ServerEncoding { encoding: i32 },
    #[error(transparent)]
    Decoder(#[from] import::ConfigError),
}

/// Page projection errors.
#[derive(Debug, Error)]
pub enum ProjectError {
    #[error(transparent)]
    Import(#[from] import::ImportError),
    #[error("PostgreSQL error: {0}")]
    Postgres(String),
    #[error("slot pointer is null")]
    NullSlot,
    #[error("slot must use TTSOpsVirtual")]
    UnsupportedSlotOps,
    #[error("slot tuple descriptor does not match the projector tuple descriptor")]
    SlotTupleDescMismatch,
    #[error("slot values array is not initialized")]
    SlotValuesNotInitialized,
    #[error("slot nulls array is not initialized")]
    SlotNullsNotInitialized,
    #[error("imported page column {index} is not the expected {expected}")]
    ImportedArrayTypeMismatch {
        index: usize,
        expected: &'static str,
    },
    #[error("projected NAME value at column {index} is too long: len {len}, max {max_len}")]
    NameTooLong {
        index: usize,
        len: usize,
        max_len: usize,
    },
}
