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
- Heap path: reads `request_heap_block`, copies page + vis bitmap to SHM, sends metadata; on end, sends per‑scan `Eof`.
- Result path: reads frames from result ring, decodes wire tuple, assembles `MinimalTuple` via `heap_form_minimal_tuple`, and stores into `TupleTableSlot`.
