//! Compile DataFusion scan pushdown inputs into PostgreSQL SQL for a single base-table scan.
//!
//! This crate is intentionally narrow:
//!
//! - input is `TableProvider::scan()`-shaped metadata: relation, Arrow schema, projection,
//!   logical filters, and optional fetch/limit hint
//! - callers also provide the live PostgreSQL identifier byte limit used to
//!   reject overlong schema, relation, and column names
//! - output is a deterministic PostgreSQL `SELECT ... FROM ... WHERE ...` string by default,
//!   plus metadata about which filters compiled into PostgreSQL SQL, which remain residual,
//!   and how the requested limit was lowered
//! - unsupported expressions are left in `residual_filters` instead of failing the compile
//! - malformed inputs such as unknown columns, overlong identifiers, or invalid
//!   projection indices return [`CompileError`]
//!
//! The compiler targets a single base relation and renders PostgreSQL SQL text. It does not
//! depend on `pgrx`, does not build PostgreSQL planner nodes, and does not execute the query.
//!
//! Contract:
//!
//! - expressions compiled into SQL are expected to run with PostgreSQL semantics
//! - residual filters are whatever remains for a caller to evaluate above the scan
//! - this crate does not try to preserve exact DataFusion semantics across the engine boundary
//! - this crate is the intended upstream producer of trusted scan SQL for
//!   `slot_scan`
//! - requested limits are treated as fetch hints by default and are not rendered
//!   into SQL unless explicitly requested via [`LimitLowering::SqlClause`]
//! - in the default `scan_sql -> slot_scan` path, `requested_limit` should be
//!   lowered into both `slot_scan::ScanOptions::planner_fetch_hint` and
//!   `slot_scan::ScanOptions::local_row_cap`

mod compile;
mod error;
mod identifier;
mod literal;
mod quote;
mod render;
mod types;

pub use crate::compile::{compile_scan, render_unprojected_scan_sql};
pub use crate::error::CompileError;
pub use crate::types::{CompileScanInput, CompiledFilter, CompiledScan, LimitLowering, PgRelation};

#[cfg(test)]
mod tests;
