use arrow_layout::{LayoutError, TypeTag};
use arrow_schema::ArrowError;
use thiserror::Error;
use transfer::MessageKind;

/// Decoder construction errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unsupported Arrow type at column {index}: {data_type}")]
    UnsupportedArrowType { index: usize, data_type: String },
}

/// Import errors for page-backed Arrow batches.
#[derive(Debug, Error)]
pub enum ImportError {
    #[error("unexpected page kind: expected {expected}, got {actual}")]
    WrongKind {
        expected: MessageKind,
        actual: MessageKind,
    },
    #[error("unsupported page flags: expected 0, got {actual}")]
    UnsupportedFlags { actual: u16 },
    #[error("schema column count mismatch: expected {expected}, got {actual}")]
    SchemaColumnCountMismatch { expected: usize, actual: usize },
    #[error("schema type mismatch at column {index}: expected {expected}, got {actual:?}")]
    SchemaTypeMismatch {
        index: usize,
        expected: String,
        actual: TypeTag,
    },
    #[error("schema nullability mismatch at column {index}: expected {expected}, got {actual}")]
    SchemaNullabilityMismatch {
        index: usize,
        expected: bool,
        actual: bool,
    },
    #[error("invalid null count {null_count} at column {index} for row_count {row_count}")]
    InvalidNullCount {
        index: usize,
        row_count: u32,
        null_count: u32,
    },
    #[error("null bitmap count mismatch at column {index}: descriptor says {expected}, bitmap contains {actual} nulls")]
    NullBitmapCountMismatch {
        index: usize,
        expected: u32,
        actual: u32,
    },
    #[error("view at column {index}, row {row} points before the allocated tail region: offset {offset}, allocated tail starts at {allocated_tail_start}")]
    ViewOffsetBeforeAllocatedTail {
        index: usize,
        row: u32,
        offset: u32,
        allocated_tail_start: u32,
    },
    #[error("payload range overflow: offset {offset}, len {len}")]
    PayloadRangeOverflow { offset: usize, len: usize },
    #[error("payload range offset {offset}, len {len} is out of bounds for payload of length {payload_len}")]
    PayloadOutOfBounds {
        payload_len: usize,
        offset: usize,
        len: usize,
    },
    #[error("payload offset {offset} does not provide {required_headroom} bytes of aligned headroom for alignment {alignment}")]
    MissingAlignedHeadroom {
        offset: usize,
        alignment: usize,
        required_headroom: usize,
    },
    #[error(transparent)]
    Layout(#[from] LayoutError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}
