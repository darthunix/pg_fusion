---
id: comp-executor-0001
type: fact
scope: executor
tags: ["datafusion", "scan", "result-ring", "wire-tuple", "shm"]
updated_at: "2026-03-14"
importance: 0.7
---

# Component: Executor

- Planning/exec: DataFusion with single partition; scan provider/exec/stream live in separate `scan` crate (`HeapScanProvider -> HeapScanExec -> HeapScanStream`).
- Query orchestration now uses layered FSMs: top-level `query_flow` consumes `protocol::QueryPacket`, `data_flow` models execution-session lifecycle, and `scan_flow` models per-scan lifecycle.
- `executor/src/server.rs` wires `data_flow` and `scan_flow` in shadow mode: runtime behavior is still driven by existing code paths, while shadow FSMs observe `BeginScan/ExecScan/EndScan`, per-scan register, heap page, EOF, and failure/reset paths for validation.
- The same module now also contains an unwired `process_message_flow_driven` draft path: it shows the intended layering where `query_flow` drives `data_flow`, `ColumnLayout` also goes through `query_flow`, and `TaskFinished` arrives back to the owner loop as an internal `mpsc` runtime event tagged with a per-data-flow epoch.
- FSM contract: `Explain` is handled only after `Translate` in `PhysicalPlan`; `ColumnLayout` then moves `query_flow` into `Execution`, and only `Execution` accepts `BeginScan`/`ExecScan`/`EndScan`.
- Scans: per‑connection `HeapScanRegistry` (from `scan` crate) with bounded channels; issues `request_heap_block` and pipelines next on receipt (single in‑flight block per scan).
- Heap: on receiving a `Heap` meta packet, immediately copies the heap page and visibility bitmap out of SHM into owned `Vec<u8>` to avoid slot reuse races; applies visibility bitmap (LSB‑first, 1‑based LP indices) to filter tuples; decodes via `storage::heap::decode_tuple_project` using iterator projection; builds Arrow batches.
  - Heap scanner implementation now lives under `scan/src/heap/` with focused submodules: `visibility.rs` (bitmap checks), `page_iter.rs` (visible tuple iteration over heap page), `decode.rs` (tuple->Arrow decode/builders).
  - Decode path now avoids eager empty-batch allocation, estimates row-capacity from visibility bitmap, and reuses full-schema projection without allocating `proj_indices` for identity/full scans.
- API boundary: `executor` consumes the external `scan` crate API and injects telemetry via `HeapScanTelemetryHooks`.
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
