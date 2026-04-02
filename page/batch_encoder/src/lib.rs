//! No-allocation `RecordBatch` writer for one initialized `arrow_layout` block.
//!
//! `batch_encoder` is intentionally narrow:
//!
//! - it writes into one caller-initialized block
//! - it does not allocate in the append/finish path
//! - it does not manage multi-page output
//! - it accepts the existing `arrow_layout` surface, lowering plain
//!   `Utf8` / `Binary` inputs into `Utf8View` / `BinaryView` slots
//! - it writes fixed-width values in the native-endian form required by
//!   `arrow_layout`
//!
//! The intended flow is:
//!
//! 1. Build an [`arrow_layout::LayoutPlan`]
//! 2. Initialize a payload block with [`arrow_layout::init_block`]
//! 3. Construct [`BatchPageEncoder`]
//! 4. Append the maximal fitting row prefix from one [`arrow_array::RecordBatch`]
//! 5. Finalize the block with [`BatchPageEncoder::finish`]
//!
//! As with `arrow_layout` itself, this crate is intended only for same-host
//! shared-memory exchange. It does not produce a portable cross-endian format.

mod encoder;
mod error;

#[cfg(test)]
mod tests;

pub use encoder::{AppendResult, BatchPageEncoder, EncodedBatch};
pub use error::{ConfigError, EncodeError};
