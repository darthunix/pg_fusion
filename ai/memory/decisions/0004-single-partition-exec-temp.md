---
id: dec-0004
type: decision
scope: executor
tags: ["partitions", "joins", "workaround"]
updated_at: "2025-11-29"
importance: 0.8
---

# Decision: Temporarily Force Single‑Partition Execution

## Motivation

Current `PgScanExec` is not partition‑aware, which breaks JOINs (no rows emitted). For correctness, force `target_partitions = 1` until partition‑aware scans are implemented.

## Alternative (Future)

Implement partition‑distributed channels/registries and a proper merge.
