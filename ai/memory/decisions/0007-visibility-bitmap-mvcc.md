---
id: dec-0007
type: decision
scope: executor/backend
tags: ["visibility", "mvcc", "bitmap", "shm", "performance"]
updated_at: "2026-01-09"
importance: 0.85
status: accepted
decision_makers: ["Denis Smirnov"]
---

# Decision: Apply MVCC Visibility via Per-Page Bitmap (SHM, zero alloc)

## Context

We must not decode or return invisible tuples. A per-page visibility bitmap is already part of the protocol, but executor did not apply it and backend previously filled it with all-ones.

## Decision

- Backend computes a per-page visibility bitmap using `HeapTupleSatisfiesVisibility` against the active snapshot while holding a shared buffer lock.
- Bitmap is written directly into per-connection SHM (no heap `Vec<u8>` allocations) and its length is sent in the `Heap` metadata packet.
- Executor (`PgScanStream`) borrows the page and bitmap from SHM and filters line pointers by consulting the bitmap (LSB-first, 1-based offsets) before decoding tuples.

## Notes

- For LP state, avoid non-portable macros; unpack flags from raw `ItemIdData` and compare with `LP_NORMAL`.
- Fill `HeapTupleData.t_self` and `t_tableOid` prior to visibility checks to satisfy PG17 asserts.
- Preserve SHM bounds by clamping `vis_len` to `layout.vis_bytes_per_block`.

## Consequences

- Correct visibility semantics end-to-end with minimal overhead.
- Executor fully respects backend MVCC decisions; fuzzy reads are avoided.
- Shared memory path stays allocation-free on the hot loop.

