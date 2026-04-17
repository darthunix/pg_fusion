//! PostgreSQL-side runtime for trusted scan SQL with callback-driven slot
//! consumption.
//!
//! `slot_scan` keeps a narrow boundary:
//!
//! - input: trusted, already-compiled PostgreSQL SQL text, typically produced
//!   by `scan_sql`
//! - output: callback-driven scan execution over `TupleTableSlot`s plus
//!   run-time scan statistics
//! - scope: plan inspection, read-only cursor execution, and optional local
//!   row cap
//!
//! It does **not** handle Arrow encoding, shared-memory page transport,
//! executor/provider integration, or exact global `LIMIT`.
//! It also does **not** try to sandbox arbitrary `SELECT` text or prove
//! expression-level side-effect safety. That responsibility belongs to the
//! upstream SQL compiler or caller contract.
//!
//! Typical flow:
//!
//! 1. Call [`prepare_scan()`] to validate the SQL shape and save a reusable SPI
//!    plan.
//! 2. Define a static [`SlotSinkMethods`] table.
//! 3. Build a [`SlotSink`] from those methods plus sink-private state.
//! 4. Call [`PreparedScan::run()`] to open a read-only cursor and stream slots
//!    into the callback, or use [`PreparedScan::open_cursor()`] together with
//!    [`PreparedScanCursor::drain_rows()`] for retryable incremental draining.
//!
//! Result schema and plan metadata are treated as run-time properties of the
//! revalidated portal opened in [`PreparedScan::run`]. Sink initialization gets
//! the current `TupleDesc` for that run; [`PreparedScan`] does not expose
//! stable schema metadata captured during `prepare_scan()`.
//! Every sink callback runs behind a PostgreSQL exception boundary, so callback-
//! side PostgreSQL errors and panics are converted into ordinary
//! [`ScanError`]s. If `run()` exits unsuccessfully after sink construction, the
//! sink `abort` callback is invoked best-effort exactly once.
//!
//! Read-only cursor execution reuses the caller's active snapshot so
//! `slot_scan` stays MVCC-consistent with the surrounding PostgreSQL
//! statement. `PreparedScanCursor` keeps the revalidated portal alive across
//! drain steps, but each [`PreparedScanCursor::drain_rows()`] call reconnects
//! to SPI only for the duration of that step and closes SPI again before
//! returning. Saved scans are prepared with `CURSOR_OPT_PARALLEL_OK` so
//! PostgreSQL can choose parallel-safe plans.
//!
//! In the default `scan_sql -> slot_scan` integration, `scan_sql` should pass
//! SQL without `LIMIT` and lower `CompiledScan.requested_limit` into both:
//!
//! - [`ScanOptions::planner_fetch_hint`] for fast-start planning
//! - [`ScanOptions::local_row_cap`] for best-effort early stop during `run()`

mod error;
mod plan;
mod run;
mod types;

pub use error::{ScanError, SinkError};
pub use plan::prepare_scan;
pub use types::{
    CursorDrainOutcome, CursorRowAction, PreparedScan, PreparedScanCursor, ScanOptions,
    ScanPlanKind, ScanStats, SlotSink, SlotSinkAction, SlotSinkContext, SlotSinkMethods,
};
