---
id: dec-0001
type: decision
scope: executor
tags: ["datafusion", "engine", "query"]
updated_at: "2025-11-29"
importance: 0.9
---

# Decision: Use Apache DataFusion as the Execution Engine

## Motivation

High‑level SQL/Arrow engine in Rust, solid performance/extendability, fits our stack.

## Consequences

- Implement custom `TableProvider`/`ExecutionPlan` for PG scans.
- Tune partitions/scheduler to IPC constraints.
