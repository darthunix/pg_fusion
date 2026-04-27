---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["shm", "pgrx", "slot_scan", "arrow", "testing"]
updated_at: "2026-04-27"
importance: 0.7
---

# Gotchas & Pitfalls

- `pg_fusion` is the active extension crate at `pg/extension`; do not re-add
  the retired raw-heap `executor`/`scan`/`storage` stack.
- Integration tests that exercise the background worker/shared memory need
  `shared_preload_libraries = 'pg_fusion'` in the test cluster.
- `pg_fusion` queries in `pg/extension` smoke tests currently run through a
  normal PostgreSQL client connection, not SPI-owned execution contexts.
- `slot_scan` should execute trusted compiler-generated scan SQL. SQL safety and
  expression pushdown policy belong in `scan_sql`.
- PostgreSQL-bound crates should not be included in standalone `cargo test` or
  library coverage: they reference PostgreSQL backend symbols. Keep their
  coverage in pgrx tests (`cargo pgrx test pg17 -p pg_test` and
  `cargo pgrx test pg17 -p pg_fusion --features pg_test`).
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
- `pg_fusion.scan_timing_detail` adds per-row timing inside the backend scan
  receiver callback. Use it for diagnosis, not baseline latency measurements.
- `plan_builder` validates subquery shapes after DataFusion logical
  optimization. Subqueries that decorrelate into ordinary relational operators
  can lower PostgreSQL leaf scans; subquery nodes that survive optimization
  remain unsupported.
- PostgreSQL text-like columns (`text`, `varchar`, `bpchar`, `name`) are
  exposed to DataFusion as `Utf8View`, not `Utf8`. This keeps the logical scan
  schema aligned with page-backed shared-memory batches and avoids copying
  string payloads at the scan boundary. `scan_sql` still renders these values as
  PostgreSQL `TEXT`.
- The `benches/tpch` harness is diagnostic rather than official TPC-H. Its
  schema stores TPC-H decimal columns as `double precision` and date columns as
  ISO `text` so current page encoding can exercise scans, joins, and
  DataFusion operators before `numeric`/`date` transport support is expanded.
- PostgreSQL `max_parallel_workers_per_gather` controls CTID block-range dynamic
  scan workers for eligible heap scans. `0` means leader-only portal streaming;
  positive values add that many dynamic producers, capped at `32`. The path
  needs dynamic background worker capacity and falls back to leader-only
  streaming for relations with dropped attributes or scan shapes that cannot use
  unprojected base-relation slots. Dynamic scan worker jobs must use the
  resolved standalone scan descriptor built by the leader, not the original
  user SQL; otherwise non-public schemas fail because scan workers do not
  inherit the backend `search_path`. Standalone scan producers wait for the worker
  `OpenScan` message with a bounded timeout; slow physical planning can surface
  as a scan-open failure. The generated CTID predicates normally plan as
  PostgreSQL `TidRangeScan`; `slot_scan` must keep that node type in its allowed
  scan-leaf list. The worker-to-backend scan ring must also be large enough for
  `OpenScan` with the full producer set; the current minimum is 256 bytes.
  Standalone scan producers must keep their backend lease alive after publishing
  `ScanFinished`/`ScanFailed` until the worker detaches the slot; otherwise
  `control_transport` correctly rejects worker-side reads after backend-owner
  release. Dynamic worker launch failures are strict query errors, but the
  launcher must still mark the allocated job failed and cancel already-ready
  producers so failed starts do not leak `STARTING` jobs or workers waiting for
  `OpenScan`.
- Misaligned pointer deref can panic when interpreting shared-memory bytes as
  atomics. Allocate ring regions through the established lockfree layout paths.
