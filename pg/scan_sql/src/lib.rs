//! Compile DataFusion scan pushdown inputs into PostgreSQL SQL for a single base-table scan.
//!
//! This crate is intentionally narrow:
//!
//! - input is `TableProvider::scan()`-shaped metadata: relation, Arrow schema, projection,
//!   logical filters, and optional limit
//! - output is a deterministic PostgreSQL `SELECT ... FROM ... WHERE ... LIMIT ...` string
//!   plus metadata about which filters compiled into PostgreSQL SQL and which remain residual
//! - unsupported expressions are left in `residual_filters` instead of failing the compile
//! - malformed inputs such as unknown columns or invalid projection indices return
//!   [`CompileError`]
//!
//! The compiler targets a single base relation and renders PostgreSQL SQL text. It does not
//! depend on `pgrx`, does not build PostgreSQL planner nodes, and does not execute the query.
//!
//! Contract:
//!
//! - expressions compiled into SQL are expected to run with PostgreSQL semantics
//! - residual filters are whatever remains for a caller to evaluate above the scan
//! - this crate does not try to preserve exact DataFusion semantics across the engine boundary

mod compile;
mod error;
mod literal;
mod quote;
mod render;
mod types;

pub use crate::compile::compile_scan;
pub use crate::error::CompileError;
pub use crate::types::{CompileScanInput, CompiledFilter, CompiledScan, PgRelation};

#[cfg(test)]
mod tests;
