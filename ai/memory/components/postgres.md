---
id: comp-postgres-0001
type: fact
scope: backend
tags: ["pgrx", "customscan", "heap", "shm", "result"]
updated_at: "2026-01-07"
importance: 0.7
---

# Component: PostgreSQL Extension (Backend)

- Planning: builds `TargetEntry` list and sends `ColumnLayout` for executor encoding.
- Scan lifecycle: `BeginScan` registers channels; `ExecScan` waits `ExecReady`, loops reading result ring and serving heap requests; `EndScan` closes.
- Heap path: reads `request_heap_block`, copies page to SHM; computes per‑page visibility bitmap via `HeapTupleSatisfiesVisibility` against the active snapshot, writes bitmap directly into SHM (no heap allocation), sends metadata; on end, sends per‑scan `Eof`.
- Result path: reads frames from result ring, decodes wire tuple, assembles `MinimalTuple` via `heap_form_minimal_tuple`, and stores into `TupleTableSlot`.

Notes
- For LP state, avoid non‑portable macros; unpack `ItemIdData` flags from the raw 32‑bit value and compare to `LP_NORMAL`.
- Fill `HeapTupleData.t_self` and set `t_tableOid` before visibility check to satisfy assertions in PG17.
- Avoid sending `EndScan` during `EXPLAIN` (no ANALYZE); gate by whether execution started.
