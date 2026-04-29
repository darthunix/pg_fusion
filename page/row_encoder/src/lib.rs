//! PostgreSQL-free row writer for initialized `arrow_layout` blocks.
//!
//! This crate contains the hot page-writing core used by `slot_encoder`.
//! PostgreSQL-specific work such as slot deformation and detoasting belongs in
//! the caller; this crate only consumes typed [`CellRef`] values.

mod bitmap;
mod encoder;
mod error;

#[cfg(test)]
mod tests;

pub use encoder::{
    AppendStatus, CellRef, CellType, EncodedBatch, FixedWidthCell, FixedWidthRowSource,
    PageRowEncoder, RowSource,
};
pub use error::RowEncodeError;
