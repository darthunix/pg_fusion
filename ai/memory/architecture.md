---
id: arch-overview-0001
type: fact
scope: repo
tags: ["architecture", "datafusion", "pgrx", "shared-memory", "ipc"]
updated_at: "2025-11-29"
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

1. Parse → Metadata → Compile (logical plan)
2. Bind (Columns) → Optimize → Translate (physical plan)
3. BeginScan (register channels/slots) → ExecScan (start) → ExecReady
4. EndScan (state reset)

## Data Path

- Executor requests heap blocks (scan_id, table_oid, slot_id).
- Backend reads blocks, copies into SHM slots, sends metadata + visibility bitmap length.
- `PgScanStream` reads pages from SHM, decodes tuples via `storage::heap`, builds Arrow RecordBatches.
- Results are encoded to wire MinimalTuple and written to the result ring; backend reads frames and fills `TupleTableSlot`.

## Responsibilities

- Backend (`postgres/`): PG memory safety, `TupleTableSlot` formation, control FSM, heap IO.
- Executor (`executor/`): DataFusion planning/execution, heap requests, decode/encode results, backpressure.
- Protocol: stable binary formats/messages.
- Storage: precise heap/attribute decoder (zero‑copy where possible).
