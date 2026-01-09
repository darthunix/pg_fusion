---
id: comp-executor-0001
type: fact
scope: executor
tags: ["datafusion", "scan", "result-ring", "wire-tuple", "shm"]
updated_at: "2026-01-09"
importance: 0.7
---

# Component: Executor

- Planning/exec: DataFusion with single partition; `PgTableProvider -> PgScanExec -> PgScanStream`.
- Scans: per‑connection `ScanRegistry` with bounded channels; issues `request_heap_block` and pipelines next on receipt (single in‑flight block per scan).
- Heap: on receiving a `Heap` meta packet, immediately copies the heap page and visibility bitmap out of SHM into owned `Vec<u8>` to avoid slot reuse races; applies visibility bitmap (LSB‑first, 1‑based LP indices) to filter tuples; decodes via `storage::heap::decode_tuple_project` using iterator projection; builds Arrow batches.
- Results: encodes each row via `encode_wire_tuple` and writes to per‑connection result ring; signals backend.

Current Limitations
- Filter pushdown: not implemented — `supports_filters_pushdown()` returns Unsupported; `_filters` in `scan()` is ignored (FilterExec runs upstream).
- Limit pushdown: `_limit` ignored; no early stop after producing K rows.
- Aggregates: tuples fully decoded into Arrow and then aggregated by DF (extra allocations/copies).

Planned Optimizations
- Filters: Inexact pushdown for simple predicates (col op literal: =, !=, <, ≤, >, ≥; AND/OR); evaluate per tuple before decoding varlena; retain upstream FilterExec.
- Limits: honor `_limit` to stop requesting new blocks and end scan after ≥ K visible rows (esp. LIMIT without ORDER BY).
- Aggregates:
  - COUNT fast‑path via empty projection (ensure plans hit it for COUNT(*), COUNT(1)).
  - Block‑local partials for SUM/MIN/MAX on fixed‑width and AVG as (sum,count); emit tiny per‑block batches for AggregateExec to combine.
  - Optional GROUP BY partials for low‑cardinality keys via per‑block hashmap; AggregateExec merges.
  - Longer‑term: dedicated `PgAggScanExec` to fully push down supported aggregates/groups and avoid building intermediate Arrow rows.
