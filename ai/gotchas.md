---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["shm", "pgrx", "slot_scan", "arrow", "testing"]
updated_at: "2026-04-29"
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
  custom scan output until it hits `max_stack_depth`.
- `slot_scan` should execute trusted compiler-generated scan SQL. SQL safety and
  expression pushdown policy belong in `scan_sql`.
- PostgreSQL-bound crates should not be included in standalone `cargo test` or
  library coverage: they reference PostgreSQL backend symbols. Keep their
  coverage in pgrx tests (`cargo pgrx test pg17 -p pg_test` and
  `cargo pgrx test pg17 -p pg_fusion --features pg_test`).
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
- `pg_fusion.scan_timing_detail` uses coarse backend scan timers. It deliberately
  avoids per-row slot encode breakdown because clock reads distort the hot path;
  use flamegraphs for deformation and page-write detail.
- `plan_builder` validates subquery shapes after DataFusion logical
  optimization. Subqueries that decorrelate into ordinary relational operators
  can lower PostgreSQL leaf scans; subquery nodes that survive optimization
  remain unsupported.
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
- The `benches/tpch` harness is diagnostic rather than official TPC-H. Its
  schema stores TPC-H decimal columns as `double precision` and date columns as
  ISO `text` so current page encoding can exercise scans, joins, and
  DataFusion operators before `numeric`/`date` transport support is expanded.
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
