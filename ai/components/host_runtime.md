---
id: comp-host-runtime-0001
type: fact
scope: host_runtime
tags: ["pgrx", "datafusion", "shared-memory", "runtime_protocol", "slot_scan"]
updated_at: "2026-04-27"
importance: 0.8
---

# Component: Host Runtime

- `pg/extension` is the active pgrx extension crate.
- Backend control uses `runtime_protocol` messages over `control_transport`.
- Backend scan production uses PostgreSQL `slot_scan` plus `slot_encoder` to
  stream Arrow layout pages to the worker.
- PostgreSQL `max_parallel_workers_per_gather` controls dynamic
  background-worker scan producers for eligible heap scans. Each scan always has
  a leader backend producer; a positive `max_parallel_workers_per_gather` adds
  that many dynamic producers, capped at `32`. Each producer owns a dedicated
  scan slot and writes pages directly to the shared page pool; the worker fan-ins
  all producers for the same `scan_id` with a separate issued-page receiver per
  producer stream. Scan worker jobs carry standalone scan descriptors built from
  resolved `PgScanSpec` values instead of the original user SQL, avoiding
  `search_path` dependence in dynamic background workers. Dynamic worker launch
  is all-or-error: partial launches are cleaned up by marking failed jobs and
  sending `CancelScan` to producers that became ready before the launch failed.
- Worker execution lives in `worker_runtime` and consumes scan pages as Arrow
  batches through `page/import`.
- Results return as issued Arrow pages and are projected into PostgreSQL tuple
  slots through `pg/slot_import`.
- The issuance permit pool is sized from `pg_fusion.page_count`; there is no
  separate host GUC for permit count.
- Runtime metrics are global shared-memory counters exposed by
  `pg_fusion_metrics()` and reset by `pg_fusion_metrics_reset()`. Page handoff
  latency is measured with page descriptor stamps, not by instrumenting ring
  internals.
- `pg_fusion.scan_timing_detail` enables per-row backend scan callback timing
  so `scan_page_fill_ns` can be split into PostgreSQL read time and
  slot-to-Arrow serialization time. It is diagnostic-only and defaults off.
- `EXPLAIN` stays backend-local: `backend_service` lowers the planned query to
  a DataFusion physical plan, renders PostgreSQL scan leaves with present
  soft-limit/fetch-hint metadata, and prints the nested multiline `slot_scan`
  plan directly below the leaf.
- The retired raw heap page stack (`executor`, `scan`, `storage`, `protocol`,
  `common`) is no longer part of the workspace.
