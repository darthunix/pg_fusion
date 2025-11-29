---
id: inv-project-0001
type: invariant
scope: project
tags: ["safety", "pgrx", "errors", "ipc", "datafusion"]
updated_at: "2025-11-29"
importance: 0.95
---

# Project Invariants

1) No panics in PG extension paths

- In `postgres/` avoid panicking; return structured errors (`common::FusionError`).

2) SHM slices/buffers must stay within bounds

- Any slice from shared memory must be clamped to the layout capacity.
- Readers must not race writers — follow signaling/barrier order.

3) Wire tuple format strictly matches TupleDesc

- Fields and alignment (attlen/attalign/attbyval) must match `PgAttrWire`.

4) DataFusion plan executes with a single partition (for now)

- Until partition‑aware `PgScanExec` lands, force one partition so JOINs produce rows.

5) Heap decoder treats varlena carefully

- Inline text is OK; projected compressed/external varlena → error; non‑projected → safely skip.
