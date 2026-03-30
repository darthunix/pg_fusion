//! Shared zero-copy Arrow page layout contract.
//!
//! `page_arrow_layout` defines the binary page layout shared by producer-side
//! and consumer-side crates for direct zero-copy page batches.
//!
//! Recommended entry points:
//!
//! - [`LayoutPlan`] to plan one block shape from Arrow schema or logical specs
//! - [`init_block`] to initialize a byte slice in-place from that plan
//! - [`BlockMut`] to write rows directly into an initialized block
//! - [`BlockRef`] to validate and read an initialized block without allocation
//!
//! Lower-level escape hatches remain available under namespaced modules:
//!
//! - [`raw`] for `#[repr(C)]` on-page structs
//! - [`validate`] for standalone validation helpers
//! - [`constants`] for format constants
//! - [`bitmap`] for raw bitmap operations

mod access;
pub mod bitmap;
pub mod constants;
mod error;
mod internals;
mod plan;
pub mod raw;
#[cfg(test)]
mod tests;
mod types;
pub mod validate;

pub use access::{init_block, BlockMut, BlockRef, ViewWriteStatus};
pub use error::LayoutError;
pub use plan::LayoutPlan;
pub use raw::ByteView;
pub use types::{BlockFlags, ColumnFlags, ColumnLayout, ColumnSpec, TypeTag};
