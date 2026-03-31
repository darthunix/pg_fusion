//! Producer-side direct writer for `layout` blocks over PostgreSQL
//! `TupleTableSlot`s.
//!
//! The crate intentionally does not depend on `import`, `transfer`,
//! `storage`, or DataFusion. It only maps PostgreSQL slot values into a
//! caller-initialized `layout` block.
//!
//! The intended flow is:
//!
//! 1. Build a [`layout::LayoutPlan`] for the target Arrow schema.
//! 2. Allocate a payload buffer and initialize it with
//!    [`layout::init_block`].
//! 3. Construct [`PageBatchEncoder`] from the PostgreSQL
//!    [`pgrx_pg_sys::TupleDesc`]
//!    and the mutable payload.
//! 4. Feed [`pgrx_pg_sys::TupleTableSlot`] rows through
//!    [`PageBatchEncoder::append_slot`].
//! 5. When the block is complete, call [`PageBatchEncoder::finish`] and use the
//!    returned [`EncodedBatch`] metadata to publish the written payload.
//!
//! The encoder writes fixed-width values, validity bits, `ByteView` slots, and
//! view payload bytes directly into the target block. It does not build
//! heap-backed intermediate Arrow arrays.

mod bitmap;
mod datum;
mod encoder;
mod error;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use datum::set_test_database_encoding;
pub use encoder::{AppendStatus, EncodedBatch, PageBatchEncoder};
pub use error::{ConfigError, EncodeError};
