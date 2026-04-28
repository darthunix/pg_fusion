use arrow_layout::{LayoutError, TypeTag};
use pgrx_pg_sys as pg_sys;
use row_encoder::RowEncodeError;
use thiserror::Error;

#[derive(Debug, Error)]
/// Configuration-time failures when binding a PostgreSQL `TupleDesc` to an
/// initialized `arrow_layout` block.
pub enum ConfigError {
    #[error("TupleDesc pointer is null")]
    NullTupleDesc,
    #[error(
        "layout block has {layout_cols} columns but TupleDesc has {tuple_desc_cols} attributes"
    )]
    ColumnCountMismatch {
        layout_cols: usize,
        tuple_desc_cols: usize,
    },
    #[error("layout block has {layout_cols} columns but projection has {projection_cols} entries")]
    ProjectionLengthMismatch {
        layout_cols: usize,
        projection_cols: usize,
    },
    #[error(
        "projection column {index} references TupleDesc attribute {source_index}, but TupleDesc has {tuple_desc_cols} attributes"
    )]
    ProjectionIndexOutOfBounds {
        index: usize,
        source_index: usize,
        tuple_desc_cols: usize,
    },
    #[error("dropped attributes are not supported in slot_encoder v2 (column {index})")]
    DroppedAttribute { index: usize },
    #[error("projection column {index} references dropped TupleDesc attribute {source_index}")]
    ProjectedDroppedAttribute { index: usize, source_index: usize },
    #[error(
        "PostgreSQL type oid {oid} at column {index} is incompatible with layout type {type_tag:?}"
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
    #[error("invalid layout block: {0}")]
    Layout(#[from] LayoutError),
}

#[derive(Debug, Error)]
/// Row-encoding failures while appending PostgreSQL slots into a block.
pub enum EncodeError {
    #[error("TupleTableSlot pointer is null")]
    NullSlot,
    #[error("TupleTableSlot is missing values/null arrays")]
    InvalidSlotStorage,
    #[error("TupleTableSlot has a null TupleDesc")]
    NullSlotTupleDesc,
    #[error("TupleTableSlot TupleDesc does not match the encoder TupleDesc")]
    SlotTupleDescMismatch,
    #[error("TupleTableSlot attr access failed for attnum {attnum}")]
    SlotAttrAccess { attnum: usize },
    #[error("column {index} expected non-null Datum storage")]
    NullDatumPointer { index: usize },
    #[error("row value at column {index} is too large for ByteView encoding: {len} bytes")]
    RowValueTooLarge { index: usize, len: usize },
    #[error("invalid UTF-8 at column {index}: {source}")]
    InvalidUtf8 {
        index: usize,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("packed varlena at column {index} is external and must be detoasted before encoding")]
    ExternalVarlena { index: usize },
    #[error(
        "packed varlena at column {index} is compressed and must be detoasted before encoding"
    )]
    CompressedVarlena { index: usize },
    #[error("packed varlena at column {index} is malformed")]
    MalformedVarlena { index: usize },
    #[error("column {index} is not nullable in the target layout")]
    NullInNonNullableColumn { index: usize },
    #[error("unsupported row access at column {index}")]
    UnsupportedRowAccess { index: usize },
    #[error("layout write failed: {0}")]
    Layout(#[from] LayoutError),
}

impl From<RowEncodeError> for EncodeError {
    fn from(error: RowEncodeError) -> Self {
        match error {
            RowEncodeError::RowValueTooLarge { index, len } => {
                Self::RowValueTooLarge { index, len }
            }
            RowEncodeError::NullInNonNullableColumn { index } => {
                Self::NullInNonNullableColumn { index }
            }
            RowEncodeError::Layout(error) => Self::Layout(error),
            RowEncodeError::TypeMismatch { index, .. }
            | RowEncodeError::InvalidUuidWidth { index, .. } => {
                Self::UnsupportedRowAccess { index }
            }
        }
    }
}

pub(crate) fn oid_u32(oid: pg_sys::Oid) -> u32 {
    oid.to_u32()
}
