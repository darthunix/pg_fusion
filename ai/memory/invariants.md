---
id: inv-project-0001
type: invariant
scope: project
tags: ["safety", "pgrx", "errors", "ipc", "datafusion", "visibility"]
updated_at: "2026-01-09"
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

6) No heap allocations on hot SHM paths

- Backend fills per‑page visibility bitmap directly into SHM buffers (no intermediate Vec copies).
- Executor must copy page and bitmap out of SHM immediately upon receipt of metadata to avoid slot reuse races; do not borrow SHM slices during decode.
- Clamp all SHM-derived lengths to layout capacities.

7) Aligned ring buffers and atomics

- All lock‑free ring buffers (`LockFreeBuffer`) must be constructed from regions allocated with `executor::layout::lockfree_buffer_layout` (or wrappers) to guarantee alignment for `AtomicU32` head/tail words.
- Do not construct buffers on arbitrary `[u8]`; if unavoidable (tests), ensure proper allocation via `Layout` or guard with `debug_assert!` on alignment in debug builds.
