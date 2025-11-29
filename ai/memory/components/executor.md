---
id: comp-executor-0001
type: fact
scope: executor
tags: ["datafusion", "runtime", "pgscan", "ipc", "arrow"]
updated_at: "2025-11-29"
importance: 0.8
---

# Component: Executor (DataFusion Runtime)

## Essentials

- Table source: `PgTableProvider` → physical node `PgScanExec` → `PgScanStream`.
- On `scan.execute()`, register the heap‑block receiver in a per‑connection `ScanRegistry`.
- `PgScanStream` decodes heap pages (via `storage::heap`) to Arrow, then results are encoded as wire MinimalTuple and written to the result ring.
- Partition strategy: temporarily force `target_partitions = 1` (see decision `dec-0004`).

## Public Surfaces

- `server::{parse, optimize, translate, start_data_flow, end_data_flow}` — control‑path FSM.
- `pgscan::{PgTableProvider, PgScanExec, ScanRegistry}` — sources/scans.

## Gotchas

- JOIN in multi‑partition mode emits no rows without partition‑aware reading — use single partition.
- SHM: borrowed slices must not outlive the producer’s write; do not cache references.
