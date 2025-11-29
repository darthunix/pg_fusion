---
id: dec-0006
type: decision
scope: architecture
tags: ["extension", "background-worker", "rust", "datafusion", "vulcano"]
updated_at: "2025-11-29"
importance: 0.9
status: accepted
decision_makers: ["Denis Smirnov"]
---

# Decision: Executor Architecture (Extension + Background Runtime)

## Problem

PostgreSQL’s Volcano (row‑at‑a‑time) engine is great for OLTP but suboptimal for OLAP. We need a CPU‑efficient engine integrated into PostgreSQL for faster scans/joins on heap tables.

## Constraints & Drivers

1. Minimize development cost:
   - Ship as an extension (not a fork) via hooks and `CustomScan`.
   - Implement in Rust: safety, ecosystem (pgrx), community appeal.
2. Reuse an existing SQL engine — don’t build from scratch.
3. Engine must be Rust‑native and extensible.

## Decision

- Use Apache DataFusion as the execution engine (see `dec-0001`).
- Run DataFusion in a dedicated background worker with a Tokio runtime.
- Keep backends thin; delegate compute to the runtime over SHM IPC.

## Consequences

- Implement `PgTableProvider`/`PgScanExec` integration and SHM IPC (see `dec-0002`, `dec-0005`).
- Carefully manage partitioning and backpressure to fit IPC constraints.

