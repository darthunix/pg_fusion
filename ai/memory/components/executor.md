---
id: comp-executor-0001
type: fact
scope: executor
tags: ["datafusion", "scan", "result-ring", "wire-tuple"]
updated_at: "2026-01-07"
importance: 0.7
---

# Component: Executor

- Planning/exec: DataFusion with single partition; `PgTableProvider -> PgScanExec -> PgScanStream`.
- Scans: per‑connection `ScanRegistry` with bounded channels; issues `request_heap_block` and pipelines next on receipt.
- Heap: borrows page/bitmap from SHM; decodes tuples via `storage::heap::decode_tuple_project` using iterator projection; builds Arrow batches.
- Results: encodes each row via `encode_wire_tuple` and writes to per‑connection result ring; signals backend.
