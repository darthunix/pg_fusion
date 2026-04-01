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

The important contract boundary is:

- upstream compiler/caller: responsible for producing trusted side-effect-free
  scan SQL
- `slot_scan`: responsible for PostgreSQL plan-shape validation, snapshot-safe
  cursor execution, and callback delivery over the current portal schema

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

let prepared = prepare_scan("SELECT id FROM my_table WHERE id > 10", ScanOptions::default())?;
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

## Important Semantics

- `slot_scan` is not a SQL sandbox; it expects trusted compiler-generated SQL,
  typically from `scan_sql`
- `PreparedScan` does not expose a stable `TupleDesc`
- PostgreSQL may revalidate and replan a saved scan before `run()`
- read-only cursor execution reuses the caller's active snapshot, so
  `slot_scan` stays MVCC-consistent with the surrounding statement
- `ScanStats.parallel_capable` and `ScanStats.planned_workers` reflect the
  current run-time plan, not metadata captured during `prepare_scan()`
- structural validation lives here; expression-level safety policy belongs to
  the upstream SQL compiler or caller contract
