//! Page-backed Arrow row projection into PostgreSQL virtual slots.
//!
//! `slot_import` is the inverse of `slot_encoder`: it consumes a
//! [`transfer::ReceivedPage`] whose payload is an `arrow_layout` block, imports
//! the page through [`import::ArrowPageDecoder`], and iterates rows into a
//! caller-owned `TTSOpsVirtual` [`pgrx_pg_sys::TupleTableSlot`].
//!
//! The crate is intentionally narrow. It mirrors the current reversible
//! `slot_encoder` surface and does not attempt general Arrow-to-PostgreSQL
//! coercions.
//!
//! The supported mappings are:
//!
//! - `Boolean -> BOOLOID`
//! - `Int16 -> INT2OID`
//! - `Int32 -> INT4OID`
//! - `Int64 -> INT8OID`
//! - `Float32 -> FLOAT4OID`
//! - `Float64 -> FLOAT8OID`
//! - `FixedSizeBinary(16) -> UUIDOID`
//! - `Utf8View -> TEXTOID | VARCHAROID | BPCHAROID | NAMEOID`
//! - `BinaryView -> BYTEAOID`
//!
//! Text-like `Utf8View` mappings require a PostgreSQL database with `UTF8`
//! server encoding. `VARCHAR` and `BPCHAR` values are normalized through
//! PostgreSQL's typmod-aware input functions.
//!
//! Returned tuples are page-backed. In v1 that zero-copy path is used only for
//! `UUIDOID`; text-like values and `bytea` are copied into the supplied
//! per-tuple memory context. A projector may have only one active cursor at a
//! time; `open_page()` requires `&mut self` so that this guarantee is enforced
//! by Rust's borrow checker.

mod error;
mod projector;
#[cfg(test)]
mod tests;

pub use error::{ConfigError, ProjectError};
#[cfg(test)]
pub(crate) use projector::set_test_database_encoding;
pub use projector::{ArrowSlotProjector, PageSlotCursor};
