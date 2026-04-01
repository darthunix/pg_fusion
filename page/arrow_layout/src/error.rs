use crate::TypeTag;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LayoutError {
    #[error("block slice is too small: need at least {required} bytes, got {actual}")]
    BlockSliceTooSmall { required: usize, actual: usize },
    #[error("unsupported Arrow type at column {index}: {data_type}")]
    UnsupportedArrowType { index: usize, data_type: String },
    #[error("too many columns for page layout: {actual}")]
    TooManyColumns { actual: usize },
    #[error("page layout size computation overflowed")]
    SizeOverflow,
    #[error("layout does not fit into block size {block_size}; need at least {required}")]
    LayoutDoesNotFit { block_size: u32, required: u32 },
    #[error("invalid type tag {raw}")]
    InvalidTypeTag { raw: u16 },
    #[error(
        "column {index} has inconsistent view flag for type {type_tag:?}: flags=0x{flags:04x}"
    )]
    InconsistentViewFlag {
        index: usize,
        type_tag: TypeTag,
        flags: u16,
    },
    #[error("block magic mismatch: expected 0x{expected:08x}, got 0x{actual:08x}")]
    InvalidMagic { expected: u32, actual: u32 },
    #[error("block version mismatch: expected {expected}, got {actual}")]
    InvalidVersion { expected: u16, actual: u16 },
    #[error("row_count {row_count} exceeds max_rows {max_rows}")]
    RowCountExceedsMaxRows { row_count: u32, max_rows: u32 },
    #[error("column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("front_base mismatch: expected {expected}, got {actual}")]
    FrontBaseMismatch { expected: u32, actual: u32 },
    #[error("pool_base mismatch: expected {expected}, got {actual}")]
    PoolBaseMismatch { expected: u32, actual: u32 },
    #[error("invalid header bounds for front/pool/tail region")]
    InvalidHeaderBounds,
    #[error("misaligned front region: front_base={front_base}, pool_base={pool_base}, alignment={alignment}")]
    MisalignedFrontRegion {
        front_base: u32,
        pool_base: u32,
        alignment: u32,
    },
    #[error("column descriptor mismatch at index {index}")]
    ColumnDescMismatch { index: usize },
    #[error("column index {index} is out of bounds for block with {col_count} columns")]
    ColumnIndexOutOfBounds { index: usize, col_count: usize },
    #[error("invalid buffer alignment {alignment}")]
    InvalidAlignment { alignment: u32 },
    #[error("byte view length {len} is invalid")]
    InvalidByteViewLength { len: usize },
    #[error("byte view length is negative: {len}")]
    NegativeByteViewLength { len: i32 },
    #[error("inline byte view is too large: len={len}, inline_capacity={inline_capacity}")]
    InlineValueTooLarge { len: usize, inline_capacity: usize },
    #[error("outline byte view is too small: len={len}, inline_capacity={inline_capacity}")]
    OutlineValueTooSmall { len: usize, inline_capacity: usize },
    #[error("byte view offset {offset} does not fit into i32 encoding")]
    InvalidViewOffsetEncoding { offset: u32 },
    #[error("byte view offset is negative: {offset}")]
    NegativeViewOffset { offset: i32 },
    #[error("invalid view buffer index {actual}; expected 0")]
    InvalidViewBufferIndex { actual: i32 },
    #[error(
        "byte view offset {offset} with len {len} exceeds shared pool capacity {pool_capacity}"
    )]
    ViewOffsetOutOfBounds {
        offset: u32,
        len: u32,
        pool_capacity: u32,
    },
}
