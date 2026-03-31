---
id: comp-postgres-0001
type: fact
scope: backend
tags: ["pgrx", "customscan", "heap", "shm", "result", "visibility", "utility_hook"]
updated_at: "2026-03-31"
importance: 0.7
---

# Component: extension

- `pg/extension/` is the PostgreSQL backend-side extension crate for `pg_fusion`.
- Planning: builds `TargetEntry` list and sends `ColumnLayout` for executor encoding.
- Scan lifecycle: `BeginScan` registers channels; `ExecScan` waits `ExecReady`, loops reading result ring and serving heap requests; `EndScan` closes.
- Heap path: reads `request_heap_block`, copies page to SHM; computes per‑page visibility bitmap via `HeapTupleSatisfiesVisibility` against the active snapshot, writes bitmap directly into SHM (no heap allocation), sends metadata (`vis_len`) and immediately allows slot reuse; executor is responsible for copying page/bitmap out of SHM on receipt to avoid races; on end, sends per‑scan `Eof`.
- Result path: reads frames from result ring, decodes wire tuple, assembles `MinimalTuple` via `heap_form_minimal_tuple`, and stores into `TupleTableSlot`.
- Long-term redesign direction: backend should likely remain the owner of physical access semantics (seq/index scan, snapshot visibility, TOAST/detoast) and eventually stream Arrow-friendly batches to the worker instead of raw heap blocks.

Notes
- For LP state, avoid non‑portable macros; unpack `ItemIdData` flags from the raw 32‑bit value and compare to `LP_NORMAL`.
- Fill `HeapTupleData.t_self` and set `t_tableOid` before visibility check to satisfy assertions in PG17.
- Avoid sending `EndScan` during `EXPLAIN` (no ANALYZE); gate by whether execution started.
- CTAS/SELECT INTO: handled by a `ProcessUtility` hook that detects `CreateTableAsStmt` and sets a thread‑local guard; the planner hook checks this guard and skips intercepting the inner SELECT. Do not rely on `Query` fields like `intoClause` or `canSetTag`.

Opportunities
- If `PD_ALL_VISIBLE` flag is set in `PageHeader`, we can skip per‑tuple MVCC checks and mark all offsets visible when building the bitmap.
- Planning ping-pong is also a candidate for removal: backend may eventually build/serialize DataFusion logical plans directly and send plan bytes to the worker instead of iterating through the current multi-step control handshake.
