---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["shm", "pgrx", "slot_scan", "arrow", "testing"]
updated_at: "2026-06-14"
importance: 0.7
---

# Gotchas & Pitfalls

- `pg_fusion` is the active extension crate at `pg/extension`; do not re-add
  the retired raw-heap `executor`/`scan`/`storage` stack.
- Integration tests that exercise the background worker/shared memory need
  `shared_preload_libraries = 'pg_fusion'` in the test cluster.
- `pg_fusion` queries in `pg/extension` smoke tests currently run through a
  normal PostgreSQL client connection, not SPI-owned execution contexts.
- `CustomScan.custom_scan_tlist` must describe the scan tuple with terminal
  expressions, not the same `INDEX_VAR` entries used by `plan.targetlist`.
  Sharing the lists makes PostgreSQL `EXPLAIN VERBOSE` recursively deparse
  custom scan output until it hits `max_stack_depth`. The current NULL
  constants can surface as `Output: NULL::...` in PostgreSQL's top-level custom
  scan verbose output; do not replace them without preserving the non-recursive
  deparse behavior.
- `slot_scan` should execute trusted compiler-generated scan SQL. SQL safety and
  expression pushdown policy belong in `scan_sql`.
- PostgreSQL-bound crates should not be included in standalone `cargo test` or
  library coverage: they reference PostgreSQL backend symbols. `pg_test` can
  run under `cargo llvm-cov` through pgrx, but `pg_fusion` extension smoke
  tests should run as plain `cargo pgrx test`; coverage flags retain dead
  PostgreSQL extern references that the pgrx test harness expects the Linux
  linker to discard.
- The committed `pg_compat` corpus under `pg/extension/pg_compat` is a
  differential pgrx test, not upstream `pg_regress`: fixtures are local,
  `passed.sql` cases must use `Custom Scan (PgFusionScan)`, and results are
  compared with `pg_fusion.enable` off versus on in the same cluster.
  The current PostgreSQL 17 corpus is intentionally split into executable
  `passed.sql` coverage and non-executed `failing.sql` backlog; widening it
  should preserve that split instead of making the pgrx runner execute known
  unsupported/crash repros.
- Standalone unit-test crates should not dev-depend on PostgreSQL/SPI-backed
  crates just for fixtures. For example, `plan_codec` tests use synthetic
  `PgScanNode` plans; live `PlanBuilder`/catalog roundtrips belong in
  `pg/test`.
- Page-backed Arrow batches must not outlive their transfer/issuance ownership
  contract. Release pages only through the existing page/issued-frame APIs.
- `PageMaterializeExec` is intentionally inserted above streaming scan-adjacent
  operators and below the first retaining DataFusion operator. Keep
  `ProjectionExec`, `FilterExec`, limits, coalescing, repartition, and plain
  aggregates zero-copy unless metrics prove they retain page-backed batches.
  For Arrow `Utf8View`/`BinaryView`, `MutableArrayData` alone is not a deep
  copy because it clones variadic payload buffers; use builders or an equivalent
  copy that owns the long-value payload.
- Runtime metrics reset is intended for experiments before a query. It advances
  `reset_epoch` so old page stamps are ignored, but concurrent increments can
  still race with a manual reset.
- Runtime filters must not reject until the shared lifecycle reports the exact
  target generation as `Ready`; otherwise a probe can observe a partially built
  Bloom filter and create false negatives. Reusing pool storage also requires
  the owner and all probe handles to drop first. `TxError::Full` style control
  messages are intentionally not part of readiness; backend probes poll shared
  memory and pass rows unfiltered while the filter is not ready.
- Runtime metrics keep backend scan timing coarse and always-on. They
  deliberately avoid per-row slot encode breakdown because clock reads distort
  the hot path; use flamegraphs for deformation and page-write detail.
- Backend scan producer failures must publish `ScanFailed` to the worker before
  tearing down `ACTIVE_EXECUTION`. The worker turns that into primary
  `FailExecution`, and only then should backend execution cleanup run; otherwise
  the user sees a generic missing-execution error instead of the original scan
  failure detail.
- PostgreSQL interrupts raised through `check_for_interrupts!()` behave like
  PostgreSQL `ERROR`, not ordinary Rust `Err` values. `CustomScan`
  Begin/Exec wait points that can hold `ACTIVE_EXECUTION` must catch them with
  pgrx `PgTryBuilder`, perform best-effort cleanup, and then raise a controlled
  PostgreSQL error; otherwise a canceled query can leave the backend process
  unable to start the next pg_fusion execution.
- After backend cancel/error cleanup, worker-side access to that backend's
  control slot may fail with `Released` or `StaleLeaseEpoch`. Treat that as a
  local execution abort for the active peer, not as a fatal primary-worker
  error; otherwise the worker exits, deactivates the control generation, and
  later queries fail with `control worker generation is not active`. The worker
  must probe its active backend peer even when `next_ready_backend_lease` has
  not reported it; backend cleanup can release/finalize the slot without
  leaving another frame to mark the peer ready. Retained peers must also route
  through runtime polling before admission parsing, because late
  session-scoped cancel/fail frames are stale controls, not malformed
  pre-session reservation traffic.
- Dynamic standalone scan workers must keep polling their scan control ring
  after `OpenScan`. `CancelScan` is the worker-side signal that releases those
  PostgreSQL backends and their relation locks when another producer fails or
  the result stream is dropped.
- `pg_fusion.max_fusion_tasks` must throttle at execution admission, not by
  ignoring ready backend slots. The backend reserves a worker execution slot
  before launching dynamic standalone scan workers; if the worker leaves
  `StartExecution` unread behind the task cap, those producers can time out
  waiting for `OpenScan` and turn throttling into query failure. Once a
  reservation request is queued, its frame has already been consumed, so newly
  ready backend slots must not receive grants ahead of that FIFO queue.
- `plan_builder` validates subquery shapes after DataFusion logical
  optimization. Subqueries that decorrelate into ordinary relational operators
  can lower PostgreSQL leaf scans; subquery nodes that survive optimization
  remain unsupported.
- The primary worker must drive final physical-plan output with DataFusion
  `execute_stream` inside its Tokio runtime. Calling `ExecutionPlan::execute(0,
  ...)` directly only drains partition `0` and breaks multi-partition roots
  such as `UNION`.
- `pg_fusion.worker_threads` controls the worker's Tokio runtime thread count.
  It does not make `WorkerPgScanExec` multi-partition, does not change the
  current worker planning contract of `target_partitions = 1`, and is separate
  from `pg_fusion.max_fusion_tasks`, which caps concurrent backend execution
  tasks. Scan/physical partitioning changes must be designed separately.
- Worker Tokio tasks must not call PostgreSQL APIs or write primary control
  transport directly. They report planning, result-page, close-frame, and
  terminal events back to the PostgreSQL background-worker scheduler thread,
  then wake it through the local scheduler wake fd that is waited together with
  the PG latch.
- Worker DataFusion spill is OS-path spill owned by pg_fusion. It is enabled
  only when `pg_fusion.worker_memory_limit_mb > 0`, creates per-execution
  directories below a cluster-scoped worker spill root, and relies on worker
  cleanup plus next-incarnation garbage collection. Startup cleanup must stay
  limited to pg_fusion-marked directories in the same cluster namespace; disabled
  spill must not create directories or run spill garbage collection. It does not
  honor PostgreSQL `temp_tablespaces`, `temp_file_limit`, or ResourceOwner
  semantics. On DataFusion 53, sorts, row hash aggregates, and
  `SortMergeJoinExec` buffered sides can spill; ordinary `HashJoinExec` cannot
  and will report resources exhausted under the finite memory pool. Very small
  memory limits can still fail before or during external sort merge reservations;
  that is a resource limit, not the old DataFusion 44 row-hash aggregate spill
  schema mismatch.
- DataFusion clones ordinary CTE plans at each reference. `plan_builder`
  rewrites non-recursive multi-use CTEs before SQL planning so references become
  `PgCteRefNode` reads over one materialized producer. Keep this path for
  floating aggregates such as TPC-H q15; exact equality against a separately
  recomputed aggregate can otherwise fail by a few floating-point bits.
- PostgreSQL text-like columns (`text`, `varchar`, `bpchar`, `name`) are
  exposed to DataFusion as `Utf8View`, not `Utf8`. This keeps the logical scan
  schema aligned with page-backed shared-memory batches and avoids copying
  string payloads at the scan boundary. `scan_sql` still renders these values as
  PostgreSQL `TEXT`.
- The supported `benches/tpch` harness is diagnostic rather than official
  TPC-H. Its Rust runner streams embedded `tpchgen` rows into a native
  PostgreSQL schema with `numeric(15,2)` decimal columns and `date` columns, so
  it intentionally exercises the finite Decimal128/date scan transport paths.
  Vanilla and pg_fusion outputs are compared as byte-identical CSV; numeric
  drift is a benchmark failure, not tolerated by the runner.
  Query selection is validated before schema preparation so invalid ids must
  fail before database mutation. pgrx socket autodetection is only a fallback
  when host and port are absent from both CLI args and environment.
  Connection resolution intentionally uses only CLI args and standard
  `PGDATABASE`/`PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD` variables to reduce
  destructive-prepare ambiguity. Row counts are derived from COPY CSV record
  terminators so one-column NULL rows emitted as a bare newline still count as
  rows. Each selected query runs a vanilla `EXPLAIN` preflight before Fusion
  `EXPLAIN`, so missing/stale schemas and ordinary SQL failures classify as
  `pg_fail` rather than `fusion_fail`. TPC-H query templates live in
  `benches/tpch/queries/` and are included by the runner; manual `psql`
  diagnostics should use those same files. Keep templates canonical-derived,
  including q2's minimum-cost supplier subquery and top-N `LIMIT` clauses; do
  not edit query semantics to fit current planner behavior.
- PostgreSQL `max_parallel_workers_per_gather` controls the query-wide CTID
  block-range dynamic scan worker budget for eligible heap scans. `0` means
  leader-only portal streaming; positive values allow up to that many dynamic
  producers across the whole pg_fusion query, capped at `32` and bounded by
  `max_worker_processes`. The path falls back to leader-only streaming for
  relations with dropped attributes or scan shapes that cannot use unprojected
  base-relation slots. Dynamic scan worker jobs must use the resolved
  standalone scan descriptor built by the leader, not the original user SQL;
  otherwise non-public schemas fail because scan workers do not inherit the
  backend `search_path`. Standalone scan producers wait for the worker
  `OpenScan` message with a bounded timeout; slow physical planning can surface
  as a scan-open failure. The generated CTID predicates normally plan as
  PostgreSQL `TidRangeScan`; `slot_scan` must keep that node type in its allowed
  scan-leaf list. The worker-to-backend scan ring must also be large enough for
  `OpenScan` with the full producer set; the current minimum is 256 bytes.
  Standalone scan producers must keep their backend lease alive after publishing
  `ScanFinished`/`ScanFailed` until the worker detaches the slot; otherwise
  `control_transport` correctly rejects worker-side reads after backend-owner
  release. Dynamic worker capacity failures should mark the allocated job
  failed, cancel already-ready producers, and continue leader-only for the
  current and remaining scans; worker readiness/protocol failures remain strict
  query errors.
- Misaligned pointer deref can panic when interpreting shared-memory bytes as
  atomics. Allocate ring regions through the established lockfree layout paths.
