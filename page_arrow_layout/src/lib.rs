//! Shared zero-copy Arrow page layout contract.
//!
//! `page_arrow_layout` defines the binary page layout shared by producer-side
//! and consumer-side crates for direct zero-copy page batches.

mod access;
mod bitmap;
mod constants;
mod error;
mod internals;
mod plan;
mod raw;
#[cfg(test)]
mod tests;
mod types;
mod validate;

pub use access::{init_block, BlockMut, BlockRef};
pub use bitmap::{bitmap_bytes, bitmap_get, bitmap_set};
pub use constants::{
    BLOCK_MAGIC, BLOCK_VERSION, BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS, SHARED_VIEW_BUFFER_INDEX,
    UUID_WIDTH_BYTES, VIEW_INLINE_LEN, VIEW_PREFIX_LEN,
};
pub use error::LayoutError;
pub use plan::LayoutPlan;
pub use raw::{BlockHeader, ByteView, ColumnDesc};
pub use types::{BlockFlags, ColumnFlags, ColumnLayout, ColumnSpec, TypeTag};
pub use validate::{validate_block, validate_header};
