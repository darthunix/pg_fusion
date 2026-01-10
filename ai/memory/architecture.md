---
id: arch-overview-0001
type: fact
scope: repo
tags: ["architecture", "datafusion", "pgrx", "shared-memory", "ipc", "visibility", "utility_hook"]
updated_at: "2026-01-10"
importance: 0.8
---

# pg_fusion Architecture Overview

In short: a PostgreSQL (pgrx) extension intercepts planning/execution and delegates to a separate Apache DataFusion runtime. Communication uses shared memory (lock‑free rings + slot buffers for heap pages). The wire protocol is defined under `protocol/`.

## Top‑Level Directories

- `postgres/`: pgrx extension — plan/execute, IPC with runtime, builds Slot/MinimalTuple.
- `executor/`: DataFusion runtime — parse/optimize/plan/execute, PgScanExec/Stream, encodes results into wire tuples.
- `protocol/`: control and data messages, wire tuple/attribute formats, types.
- `storage/`: low‑level heap page reader and attribute decoder to ScalarValue.
- `common/`: shared errors/types (FusionError).

## Control Path

0. Utility gating: `ProcessUtility` hook guards CTAS/SELECT INTO (`CreateTableAsStmt`) by setting a thread‑local flag; planner hook respects the flag and skips the inner SELECT.
1. Parse → Metadata → Compile (logical plan)
2. Bind (Columns) → Optimize → Translate (physical plan)
3. BeginScan (register channels/slots) → ExecScan (start) → ExecReady
4. EndScan (state reset)

## Data Path

- Executor requests heap blocks (scan_id, table_oid, slot_id).
- Backend reads blocks, copies into SHM slots, sends metadata + visibility bitmap length.
- `PgScanStream` copies the page and visibility bitmap out of SHM on receipt (to avoid slot reuse races), applies the per-page visibility bitmap to skip invisible LPs, decodes tuples via `storage::heap`, and builds Arrow RecordBatches.
- Results are encoded to wire MinimalTuple and written to the result ring; backend reads frames and fills `TupleTableSlot`.

## Responsibilities

- Backend (`postgres/`): PG memory safety, `TupleTableSlot` formation, control FSM, heap IO.
- Executor (`executor/`): DataFusion planning/execution, heap requests, decode/encode results, backpressure.
- Protocol: stable binary formats/messages.
- Storage: precise heap/attribute decoder (zero‑copy where possible).

## Result Path Status

- Column layout: backend sends `ColumnLayout` with `PgAttrWire { atttypid, typmod, attlen, attalign, attbyval, nullable }` during planning; executor caches it.
- Wire tuples: executor encodes rows using `protocol::tuple::encode_wire_tuple` (header + optional null bitmap + aligned data area; byval in host‑endian; varlena as length-prefixed bytes, no TOAST/compression).
- Result ring: executor writes length‑prefixed wire tuples to the per‑connection lock‑free result ring and nudges backend (SIGUSR1).
- Backend assembly: backend reads frames, decodes wire header, reconstructs `Datum[]/isnull[]` by `attlen/attalign/attbyval`, forms `MinimalTuple` via `heap_form_minimal_tuple`, and stores into `TupleTableSlot`.
- Visibility: heap page visibility bitmap is carried out‑of‑band in SHM and applied in `PgScanStream` to filter invisible tuples. Backend computes bitmap via `HeapTupleSatisfiesVisibility` against the active snapshot.
