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
- `backend_service` should not have a standalone cargo test harness: it links
  `slot_scan`, which references PostgreSQL SPI symbols. Put those cases in
  `pg/test` and run them with `cargo pgrx test pg17 -p pg_test`.
- Page-backed Arrow batches must not outlive their transfer/issuance ownership
  contract. Release pages only through the existing page/issued-frame APIs.
- Runtime metrics reset is intended for experiments before a query. It advances
  `reset_epoch` so old page stamps are ignored, but concurrent increments can
  still race with a manual reset.
- `pg_fusion.scan_timing_detail` adds per-row timing inside the backend scan
  receiver callback. Use it for diagnosis, not baseline latency measurements.
- `pg_fusion.scan_parallel_workers` is experimental CTID block-range chunking.
  It needs dynamic background worker capacity and currently falls back for
  relations with dropped attributes or scan shapes that cannot use unprojected
  base-relation slots. Standalone scan producers wait for the worker `OpenScan`
  message with a bounded timeout; slow physical planning can therefore surface
  as a scan-open failure while this path is experimental. The generated CTID
  predicates normally plan as PostgreSQL `TidRangeScan`; `slot_scan` must keep
  that node type in its allowed scan-leaf list. The worker-to-backend scan ring
  must also be large enough for `OpenScan` with the full producer set; the
  current minimum is 256 bytes. Standalone scan producers must keep their
  backend lease alive after publishing `ScanFinished`/`ScanFailed` until the
  worker detaches the slot; otherwise `control_transport` correctly rejects
  worker-side reads after backend-owner release. Dynamic worker launch failures
  are strict query errors, but the launcher must still mark the allocated job
  failed and cancel already-ready producers so failed starts do not leak
  `STARTING` jobs or workers waiting for `OpenScan`.
- Misaligned pointer deref can panic when interpreting shared-memory bytes as
  atomics. Allocate ring regions through the established lockfree layout paths.
