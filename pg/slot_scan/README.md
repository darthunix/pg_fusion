# `slot_scan`

`slot_scan` is a compact PostgreSQL-side runtime for trusted scan SQL.

It accepts trusted, already-compiled SQL, validates that PostgreSQL produced a
narrow scan-oriented plan, saves the SPI plan for reuse, and then executes the
scan through a callback over `TupleTableSlot`s. Prepared scans are created with
`CURSOR_OPT_PARALLEL_OK`, so PostgreSQL may choose a parallel-safe plan.

This crate is intended for compiler-generated, side-effect-free scan SQL,
typically from `scan_sql`. It is not a general-purpose safe executor for
arbitrary `SELECT` text, and it does not try to sandbox expression-level side
effects.

Current scope:

- PostgreSQL-only
- no dependency on `scan_sql`, `slot_encoder`, `pool`, `transfer`, `import`,
  or executor-side code
- callback-driven slot consumption
- optional planner fetch hint for fast-start planning
- optional soft local row cap
- plan-shape validation for a narrow set of scan-oriented plans
- read-only cursor execution under the caller's active snapshot

Current non-goals:

- exact global `LIMIT`
- shared-memory page transport
- Arrow encoding
- executor/provider integration
- worker-local callback dispatch below `Gather`
- sandboxing arbitrary SQL or proving expression-level side-effect safety

## Flow

The crate is meant to be used in two steps:

1. Build a reusable prepared scan with `prepare_scan(sql, options)`.
2. Execute it with `PreparedScan::run(sink)`.

`run()` opens a read-only SPI cursor, revalidates the saved plan, reads the
current `TupleDesc` from the opened portal, and then invokes the sink.
Every sink callback runs behind a PostgreSQL exception boundary, so callback-
side PostgreSQL errors and panics surface as ordinary `ScanError` values.
If `run()` exits unsuccessfully after sink construction, `abort` is invoked
best-effort exactly once.

For retryable incremental execution, callers can instead open a
`PreparedScanCursor` with `PreparedScan::open_cursor()` and drive it with
`PreparedScanCursor::drain_rows(...)`. The cursor keeps the revalidated portal
alive across drain steps, but each drain step reconnects to SPI only for the
duration of that call and closes SPI again before returning. This makes
backpressure-style `Blocked` / retry loops safe for higher layers such as
`backend_service`: between drain steps there is no live `SPI_connect()` frame.

The important contract boundary is:

- upstream compiler/caller: responsible for producing trusted side-effect-free
  scan SQL
- `slot_scan`: responsible for PostgreSQL plan-shape validation, snapshot-safe
  cursor execution, and callback delivery over the current portal schema

For the intended `scan_sql` integration, the upstream compiler should pass
`CompiledScan.sql` without `LIMIT` and lower `CompiledScan.requested_limit`
into both:

- `ScanOptions.planner_fetch_hint` to bias PostgreSQL toward fast-start plans
- `ScanOptions.local_row_cap` as a soft run-time early-stop hint

## Callback Contract

`slot_scan` uses a static callback table:

```rust,no_run
use slot_scan::{
    prepare_scan, ScanOptions, SlotSink, SlotSinkAction, SlotSinkContext,
    SlotSinkMethods,
};
use std::ffi::c_void;

#[derive(Default)]
struct RowCounter {
    rows: usize,
    natts: usize,
}

unsafe fn sink_init(
    ctx: &mut SlotSinkContext,
    private: *mut c_void,
    tuple_desc: pgrx::pg_sys::TupleDesc,
) -> Result<(), slot_scan::SinkError> {
    let state = &mut *(private as *mut RowCounter);
    state.natts = (*tuple_desc).natts as usize;
    let _parallel = ctx.parallel_capable();
    Ok(())
}

unsafe fn sink_consume_slot(
    _ctx: &mut SlotSinkContext,
    private: *mut c_void,
    _slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> Result<SlotSinkAction, slot_scan::SinkError> {
    let state = &mut *(private as *mut RowCounter);
    state.rows += 1;
    Ok(SlotSinkAction::Continue)
}

unsafe fn sink_finish(
    _ctx: &mut SlotSinkContext,
    _private: *mut c_void,
) -> Result<(), slot_scan::SinkError> {
    Ok(())
}

unsafe fn sink_abort(_ctx: &mut SlotSinkContext, _private: *mut c_void) {}

static SINK: SlotSinkMethods = SlotSinkMethods {
    init: Some(sink_init),
    consume_slot: sink_consume_slot,
    finish: Some(sink_finish),
    abort: Some(sink_abort),
};

let prepared = prepare_scan(
    "SELECT id FROM my_table WHERE id > 10",
    ScanOptions {
        planner_fetch_hint: Some(100),
        local_row_cap: Some(100),
    },
)?;
let mut state = RowCounter::default();
let stats = prepared.run(SlotSink::new(&SINK, &mut state))?;
assert_eq!(state.rows, stats.rows_seen);
# Ok::<(), slot_scan::ScanError>(())
```

How the callbacks are used:

- `init`: optional, called once after the cursor is opened and after run-time
  plan metadata is available in `SlotSinkContext`
- `consume_slot`: required, called for each produced row
- `finish`: optional, called once after successful completion
- `abort`: optional, called once if `run()` exits with an error

Important callback rules:

- `init` receives the current run-time `TupleDesc`; do not retain it past the
  current `run()` call
- the `TupleTableSlot` passed to `consume_slot` is reused across rows; copy
  anything you need inside the callback
- `private` is owned by the caller and must remain valid for the entire `run()`
  call
- prefer `SlotSink::new(&METHODS, &mut state)` when you have a typed sink state
- `SlotSink::from_raw(...)` is `unsafe` and should only be used when the caller
  can uphold the raw-pointer lifetime and type contract manually
- return `SlotSinkAction::Stop` from `consume_slot` to stop the local scan loop
  early

## Incremental Cursor Contract

`PreparedScan::open_cursor()` exposes the same trusted read-only portal through
an explicit incremental API:

- `PreparedScanCursor::drain_rows(visitor)` drains rows until EOF or until the
  visitor returns `CursorRowAction::Stop` / `ReplayCurrentAndStop`
- the slot passed to the visitor is valid only for the duration of that
  callback
- `CursorRowAction::ReplayCurrentAndStop` keeps the current row inside the
  cursor and replays it first on the next `drain_rows()` call
- `CursorDrainOutcome::Stopped` means only "this drain step stopped early";
  the cursor remains open and later calls continue from the same portal
- `PreparedScanCursor::close()` closes the portal and returns final `ScanStats`

## Important Semantics

- `slot_scan` is not a SQL sandbox; it expects trusted compiler-generated SQL,
  typically from `scan_sql`
- `ScanOptions.planner_fetch_hint` is a planner-only hint that currently lowers
  to PostgreSQL `CURSOR_OPT_FAST_PLAN`; it helps plan choice but does not imply
  exact row-goal semantics
- `ScanOptions.local_row_cap` is not an exact global limit; it is the intended
  run-time lowering target for `scan_sql` fetch hints in the default
  integration path
- `PreparedScan` does not expose a stable `TupleDesc`
- PostgreSQL may revalidate and replan a saved scan before `run()`
- read-only cursor execution reuses the caller's active snapshot, so
  `slot_scan` stays MVCC-consistent with the surrounding statement
- `ScanStats.parallel_capable`, `ScanStats.planned_workers`, and
  `ScanStats.plan_kind` reflect the current run-time plan, not metadata
  captured during `prepare_scan()`
- structural validation lives here; expression-level safety policy belongs to
  the upstream SQL compiler or caller contract
