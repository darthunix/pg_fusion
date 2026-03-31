---
id: arch-overview-0001
type: fact
scope: repo
tags: ["architecture", "datafusion", "pgrx", "shared-memory", "ipc", "visibility", "utility_hook"]
updated_at: "2026-03-26"
importance: 0.8
---

# pg_fusion Architecture Overview

In short: a PostgreSQL (pgrx) extension intercepts planning/execution and delegates to a separate Apache DataFusion runtime. Communication uses shared memory (lock‑free rings + slot buffers for heap pages). The wire protocol is defined under `protocol/`.

## Top‑Level Directories

- `pg/extension/`: pgrx extension — plan/execute, IPC with runtime, builds Slot/MinimalTuple.
- `pg/slot_encoder/`: producer-side encoder from PostgreSQL slots into page-backed Arrow layout blocks.
- `executor/`: DataFusion runtime — parse/optimize/plan/execute, PgScanExec/Stream, encodes results into wire tuples.
- `protocol/`: control and data messages, wire tuple/attribute formats, types.
- `storage/`: low‑level heap page reader and attribute decoder to ScalarValue.
- `common/`: shared errors/types (FusionError).
- `page/pool/`: fixed-page shared-memory ownership pool.
- `page/transfer/`: page handoff protocol built on `page/pool`.
- `page/layout/`: shared zero-copy Arrow page layout contract.
- `page/import/`: import-only zero-copy Arrow `RecordBatch` wrapper over `page/transfer` pages.
- `testing/pg_test/`: pgrx benchmark/test crate for Postgres-side storage and page pipeline experiments.

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

- Backend (`pg/extension/`): PG memory safety, `TupleTableSlot` formation, control FSM, heap IO.
- Executor (`executor/`): DataFusion planning/execution, heap requests, decode/encode results, backpressure.
- Protocol: stable binary formats/messages.
- Storage: precise heap/attribute decoder (zero‑copy where possible).

## Result Path Status

- Column layout: backend sends `ColumnLayout` with `PgAttrWire { atttypid, typmod, attlen, attalign, attbyval, nullable }` during planning; executor caches it.
- Wire tuples: executor encodes rows using `protocol::tuple::encode_wire_tuple` (header + optional null bitmap + aligned data area; byval in host‑endian; varlena as length-prefixed bytes, no TOAST/compression).
- Result ring: executor writes length‑prefixed wire tuples to the per‑connection lock‑free result ring and nudges backend (SIGUSR1).
- Backend assembly: backend reads frames, decodes wire header, reconstructs `Datum[]/isnull[]` by `attlen/attalign/attbyval`, forms `MinimalTuple` via `heap_form_minimal_tuple`, and stores into `TupleTableSlot`.
- Visibility: heap page visibility bitmap is carried out‑of‑band in SHM and applied in `PgScanStream` to filter invisible tuples. Backend computes bitmap via `HeapTupleSatisfiesVisibility` against the active snapshot.

## Proposed Redesign Direction

This is not implemented yet, but the current architecture is increasingly seen as transitional:

- Control plane: remove the planning ping-pong (`Parse -> Metadata -> Bind -> Optimize -> Translate -> Columns -> ColumnLayout`) and instead build the DataFusion logical plan on the backend, serialize it through SHM, and let the worker only deserialize + continue execution planning.
- Data plane: stop shipping raw PostgreSQL heap pages/visibility bitmaps to the worker. That boundary forces the worker to reimplement PostgreSQL tuple decoding semantics, complicates TOAST handling, and does not extend naturally to index scans.
- Preferred future boundary: PostgreSQL backend keeps ownership of table/index access, snapshot visibility, TOAST/detoast, and slot materialization. It then repacks rows into an Arrow-friendly batch/wire format and streams batches to the worker.
- Consequence: worker-side `storage::heap`/page-oriented scan logic would become legacy or transitional. The worker would instead consume batch streams, while PostgreSQL remains the source of truth for physical access methods.
- Supporting building blocks now exist:
  - `page/layout` defines the shared front-and-tail binary layout: fixed-width values, validity bitmaps, and view slots live in a preplanned front region, while long `Utf8View`/`BinaryView` payloads share one reverse-growing tail arena with `buffer_index = 0`.
  - `page/import` consumes a `page/transfer::ReceivedPage` whose payload is one validated `page/layout` block and exposes it as a plain `RecordBatch` backed directly by the page bytes. It is intentionally strict in v1: external schema, zero-copy for data-bearing batches, no dictionaries, and page release tied to final Arrow buffer drop except for zero-buffer owned fallbacks.
  - `pg/slot_encoder` writes PostgreSQL `TupleTableSlot` rows directly into initialized `page/layout` blocks. It supports bool/int/float, text-like Utf8View, `bytea` as BinaryView, and `uuid`, and asks PostgreSQL to deform the slot on demand when needed.
