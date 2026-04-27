# `backend_service`

`backend_service` is the PostgreSQL backend-side execution bridge for the new
runtime stack.

It owns the backend-local execution lifecycle:

- build one logical plan with `plan_builder`
- publish that plan to the worker through `plan_flow`
- capture and hold the current PostgreSQL snapshot for the execution lifetime
- precompute reusable `slot_scan::PreparedScan` handles for every leaf
  `PgScanSpec`
- seed one `PageRowEstimator` per scan with `row_estimator_seed`
- serve worker `OpenScan` requests by running `slot_scan`, encoding rows with
  `slot_encoder`, and streaming pages through `scan_flow`

`backend_service` uses a stackful scan-streaming contract for the hot path:

- one backend scan session keeps a PostgreSQL portal, one `SPI_connect()` frame,
  and a direct `DestReceiver` alive while it drains portal rows into pages
- rows are encoded straight from PostgreSQL slots into `slot_encoder` pages
  without per-row SPI tuptable materialization; only the rare row that crosses
  an Arrow page boundary is copied into a retry tuple
- for relations without dropped attributes, scans execute as unprojected
  `SELECT *` leaves and `slot_encoder` applies the logical column projection;
  this avoids PostgreSQL executor projection nodes in simple leaf scans
- page boundaries are enforced by the row budget passed to `PortalRunFetch()`;
  the backend does not use `receiveSlot = false` as a resumable pause signal
- the backend uses the hidden fast `slot_scan` receiver without per-row
  `catch_unwind`; internal scan callbacks must return expected failures through
  `Result`, and a Rust panic is treated as a bug
- lower layers such as `issuance` and `scan_flow` remain non-blocking; when
  page permits are exhausted the scan driver yields explicit control back to
  the host loop with `ScanStreamStep::YieldForControl { PermitBackpressure }`

This is intentionally a strong invariant: once scan streaming starts, the
backend must keep driving that scan through its `ActiveScanDriver` and must
not interleave unrelated SPI or planning work in the same backend process.

`BackendServiceConfig` also exposes a backend-local tuning knob for scan
streaming:

- `scan_fetch_batch_rows` controls the row budget for one direct
  `PortalRunFetch()` drain from PostgreSQL into the slot encoder
- the default is `1024`
- `0` is normalized to `1`
- scans with variable-width transport columns use one-row drains so an
  overflowing row can be copied into the next page without losing the portal
  position
- public `slot_scan::ScanOptions` stay unchanged; this knob only affects the
  internal backend-service streaming path

The service can also accept externally launched scan producers through
`ScanWorkerLauncher`. The extension uses this for CTID block-range chunking:
the leader backend keeps producer `0`, dynamic scan workers use producer ids
`1..N`, and the worker runtime receives all producer channels in
`StartExecution`. Before per-scan launch, the launcher sees the whole query scan
set and may assign a query-wide worker budget across eligible scans. The
extension launcher uses `max_parallel_workers_per_gather` as that query-wide
budget, capped at `32` and bounded by `max_worker_processes`; dynamic worker
capacity failures degrade the current and remaining scans to leader-only
streaming. The launcher builds a standalone scan descriptor from the
already-resolved `PgScanSpec`; dynamic workers execute that descriptor directly
instead of replanning the original SQL, so they do not depend on backend-local
`search_path`. Standalone producers time out if `OpenScan` does not arrive after
launch, so a failed `begin_execution` cannot leave a scan worker waiting
forever. Worker readiness/protocol failures remain strict query errors, and the
launcher must still mark the current job failed and cancel any already-ready
producers before returning that error. After a standalone producer publishes
`ScanFinished`/`ScanFailed`, it keeps its backend lease alive until the worker
detaches that scan slot; otherwise the lower `control_transport` contract would
make the terminal frame unreadable as soon as the backend owner is released.

The crate is intentionally backend-local:

- it does not own shared-memory transport
- it does not decode or execute plans on the worker
- it does not expose a general-purpose SQL service
- it does not implement explain via worker round-trips

`backend_service` expects trusted execution inputs from higher layers. In
particular, the SQL passed into `begin_execution()` is compiled and planned by
the backend itself, and `scan_id` lookups are resolved only against the active
execution registered in the current PostgreSQL backend process.

## Testing

Standalone `cargo test -p backend_service` is disabled for this crate. The
crate links `slot_scan`, which references PostgreSQL SPI symbols that are only
available inside a PostgreSQL backend process. Use `cargo check -p
backend_service` for fast type coverage and run the backend-service regression
cases through the pgrx crate:

```sh
cargo pgrx test pg17 -p pg_test
```

## Lifecycle

One execution moves through a narrow FSM:

- `Idle`
- `Starting`: plan publication is still in progress
- `Running`: worker may open scans and terminal execution messages are accepted
- `Terminal`: cleanup-only transition before returning to `Idle`

The key contracts are:

- only one active execution exists per backend process
- every execution has an `ExecutionKey { slot_id, session_epoch }`
- stale control messages are ignored by comparing `session_epoch` against the
  current backend session
- `FailExecution` and `CancelExecution` are accepted both while the execution
  is still `Starting` and after it reaches `Running`
- `CompleteExecution` is accepted only after plan publication has completed
- once `open_scan()` returns an `ActiveScanDriver`, that driver owns all scan
  progress and terminal control until it is released
- a backend-observed fatal scan producer failure tears down the whole execution
  immediately, so late worker `CompleteExecution` messages for that
  `session_epoch` are ignored rather than overriding the failure

## Snapshot Contract

`ExecutionSnapshot` registers the current PostgreSQL active snapshot on the
current resource owner and unregisters it on drop.

This means:

- `begin_execution()` requires an active PostgreSQL snapshot
- the surrounding backend code must keep the matching resource-owner lifetime
  valid for the whole execution
- scan streaming runs under the saved snapshot, so worker `OpenScan` traffic
  sees the same MVCC snapshot as execution start

## Main API

Typical backend flow:

```rust,ignore
use backend_service::{BackendService, StartExecutionInput};
use control_transport::TransportRegion;

let scan_slot_region: &TransportRegion = /* dedicated scan-slot region */;

let begin = BackendService::begin_execution(StartExecutionInput {
    slot_id,
    sql,
    params,
    plan_tx,
    scan_slot_region,
    config,
    scan_worker_launcher: None,
})?;

send_control(begin.control())?;

loop {
    match BackendService::step_execution_start()? {
        plan_flow::BackendPlanStep::OutboundPage { outbound, .. } => {
            send_plan_page(outbound)?;
        }
        plan_flow::BackendPlanStep::CloseFrame { frame, .. } => {
            send_plan_close(frame)?;
            break;
        }
        plan_flow::BackendPlanStep::Blocked { .. } => retry_later(),
        plan_flow::BackendPlanStep::LogicalError { message, .. } => {
            return Err(anyhow::anyhow!(message));
        }
    }
}

let key = BackendService::finalize_execution_start()?;
assert_eq!(key, begin.key);
```

Typical worker-driven scan open:

```rust,ignore
use backend_service::{
    BackendService, OpenScanInput, ScanStreamStep, ScanYieldReason,
};

if let Some(mut driver) = BackendService::open_scan(OpenScanInput {
    peer,
    session_epoch,
    scan_id,
    scan,
    scan_tx,
})? {
    loop {
        match driver.step()? {
            ScanStreamStep::OutboundPage { outbound, .. } => send_scan_page(outbound)?,
            ScanStreamStep::YieldForControl {
                reason: ScanYieldReason::PermitBackpressure,
            } => {
                if let Some(msg) = try_poll_control()? {
                    match msg {
                        WorkerControl::Fail(code, detail) => {
                            driver.fail_execution(code, detail)?;
                            break;
                        }
                        WorkerControl::Cancel => {
                            driver.cancel_execution()?;
                            break;
                        }
                        _ => {}
                    }
                }
                continue;
            }
            ScanStreamStep::Finished { .. } => break,
            ScanStreamStep::Failed { message, .. } => {
                // The execution has already been failed and cleaned up before
                // this step is returned.
                return Err(anyhow::anyhow!(message));
            }
        }
    }

    driver.complete_execution()?;
}
```

Explain stays backend-local:

- `BackendService::render_explain()` builds the logical plan, lowers it to a
  DataFusion physical plan, and renders that physical plan directly in the
  backend
- PostgreSQL scan leaves are represented by explain-only physical exec nodes
  that include the scan soft limit/fetch hints and a multiline PostgreSQL plan
  block for the compiled leaf SQL
- no active execution is registered
- no worker or `plan_flow` / `scan_flow` interaction is involved

## Non-goals

- multiple concurrent executions in one backend process
- doing unrelated SPI/planning work while an `ActiveScanDriver` is alive
- resumable public scan cursors that survive arbitrary retry-later boundaries
- worker-side snapshot ownership
- general-purpose cancellation registry beyond the current process-local active
  execution
- integration with any retired raw-heap executor FSM
