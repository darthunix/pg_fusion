---
id: dec-0010
type: decision
scope: architecture
tags: ["planning", "ipc", "arrow", "datafusion", "toast", "index-scan"]
updated_at: "2026-03-14"
importance: 0.85
status: proposed
decision_makers: ["Denis Smirnov"]
---

# Decision Draft: Move Plan Construction to Backend and Replace Raw Heap Block IPC

## Context

Current architecture has two growing pain points:

- Planning control plane is a multi-step ping-pong between backend and worker (`Parse -> Metadata -> Bind -> Optimize -> Translate -> Columns -> ColumnLayout`).
- Data plane ships raw PostgreSQL heap pages plus visibility metadata to the worker, which then reimplements PostgreSQL tuple decoding semantics on the DataFusion side.

This boundary is increasingly considered too expensive and too brittle, especially for:

- TOAST / varlena correctness
- future index scan support
- avoiding duplication of PostgreSQL storage semantics in Rust worker code

## Proposed Direction

1. Backend builds the DataFusion logical plan itself and serializes it through SHM to the worker.
2. Worker deserializes the plan and continues with physical planning / execution instead of participating in the current planning handshake.
3. Backend remains the owner of PostgreSQL access semantics:
   - seq / index / bitmap scans
   - snapshot visibility
   - TOAST / detoast behavior
   - slot / tuple materialization
4. Backend repacks rows into an Arrow-friendly batch format and streams batches to the worker.
5. Worker consumes batches, runs DataFusion operators, and returns result tuples as today or via a later simplified result protocol.

## Rationale

- Removes most planning ping-pong and shrinks control protocol complexity.
- Stops duplicating PostgreSQL heap decoding rules in the worker.
- Makes TOAST handling live where PostgreSQL already has the correct semantics.
- Creates a natural path toward index-backed execution without teaching the worker PostgreSQL access methods.
- Moves the backend/worker boundary from "physical pages" to "typed batches", which better matches DataFusion’s execution model.

## Expected Consequences

- Existing raw heap block protocol and worker-side heap decoding become transitional infrastructure.
- `scan_flow` and budgeting may simplify from page/credit scheduling to batch-stream backpressure.
- Planning protocol can collapse into a much smaller set of messages once plan serialization is introduced.
- A new plan serialization format must be chosen and integrated; DataFusion-native plan serialization is preferred over inventing a custom format when possible.

## Status Notes

This is a design direction, not an accepted implementation plan yet. Current code still uses:

- planning ping-pong over `QueryPacket`
- raw heap block transport over SHM
- worker-side tuple decoding via `storage::heap`
