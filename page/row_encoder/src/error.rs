use arrow_layout::{LayoutError, TypeTag};
use thiserror::Error;

use crate::CellType;

#[derive(Debug, Error)]
pub enum RowEncodeError {
    #[error("row value at column {index} is too large for ByteView encoding: {len} bytes")]
    RowValueTooLarge { index: usize, len: usize },
    #[error("column {index} is not nullable in the target layout")]
    NullInNonNullableColumn { index: usize },
    #[error("column {index} expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        index: usize,
        expected: TypeTag,
        actual: CellType,
    },
    #[error("uuid value at column {index} has {len} bytes; expected 16")]
    InvalidUuidWidth { index: usize, len: usize },
    #[error("layout write failed: {0}")]
    Layout(#[from] LayoutError),
}
