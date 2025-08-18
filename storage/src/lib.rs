//! Pointer-based layouts for decoding Postgres heap pages
//!
//! This crate wraps `pgrx-pg-sys` structs (from Postgres `bufpage.h`) with
//! pointer-based views suitable for working with data in shared buffers. All
//! dereferences are `unsafe` and require the caller to uphold validity and
//! lifetime guarantees of the underlying memory.

pub mod heap;
