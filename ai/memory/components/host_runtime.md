---
id: comp-host-runtime-0001
type: fact
scope: host_runtime
tags: ["pgrx", "datafusion", "shared-memory", "runtime_protocol", "slot_scan"]
updated_at: "2026-04-24"
importance: 0.8
---

# Component: Host Runtime

- `pg/host_extension` is the active pgrx extension crate.
- Backend control uses `runtime_protocol` messages over `control_transport`.
- Backend scan production uses PostgreSQL `slot_scan` plus `slot_encoder` to
  stream Arrow layout pages to the worker.
- Worker execution lives in `worker_runtime` and consumes scan pages as Arrow
  batches through `page/import`.
- Results return as issued Arrow pages and are projected into PostgreSQL tuple
  slots through `pg/slot_import`.
- The retired raw heap page stack (`pg/extension`, `executor`, `scan`,
  `storage`, `protocol`, `common`) is no longer part of the workspace.
