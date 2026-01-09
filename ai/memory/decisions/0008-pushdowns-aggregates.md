---
id: dec-0008
type: decision
scope: executor
tags: ["filters", "limit", "aggregate", "pushdown", "performance"]
updated_at: "2026-01-09"
importance: 0.8
status: proposed
decision_makers: ["Denis Smirnov"]
---

# Decision: Filter/Limit Pushdown and Block‑Local Aggregates

## Problem

Current scans decode all visible tuples (per projection) into Arrow and let DataFusion apply filters and aggregates. This allocates and copies more than necessary and ignores LIMIT for early stop.

## Proposal

1) Filters (Inexact pushdown):
   - Support simple predicates `col op literal` (op ∈ {=, !=, <, ≤, >, ≥}) and boolean AND/OR.
   - Evaluate per tuple in `PgScanStream` before decoding varlena. Keep FilterExec upstream for correctness.

2) LIMIT pushdown:
   - Use `_limit` from `TableProvider::scan` to stop requesting new blocks once ≥ K visible rows produced; end the scan.

3) Aggregates:
   - COUNT fast‑path via empty projection (already supported; ensure plans use it for COUNT(*), COUNT(1)).
   - Block‑local partials for SUM/MIN/MAX (fixed‑width) and AVG as (sum,count): compute while scanning tuples; emit tiny per‑block batches for AggregateExec to combine.
   - Optional: block‑local GROUP BY partials for low‑cardinality keys (hashmap), merged by AggregateExec.
   - Long‑term: `PgAggScanExec` to fully push down supported aggregates/groups and avoid building intermediate Arrow rows.

## Consequences

- Reduced decoding/allocations for selective filters and small LIMIT.
- Aggregates read fewer bytes and build far smaller batches, improving throughput/latency.
- Maintains correctness by keeping upstream operators where pushdown is Inexact.

