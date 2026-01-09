---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["joins", "partitions", "varlena", "signals", "shm"]
updated_at: "2026-01-09"
importance: 0.7
---

# Gotchas & Pitfalls

- JOIN without partition‑awareness: with `partitions > 1` no rows are emitted → force `1` for now.
- TOAST/Compressed varlena: in projection → error; when not projected → skip, don’t crash.
- SIGUSR1: requires a valid client PID; not available on non‑Unix.
- SHM races: backend can reuse per-connection slots after metadata is sent; executor must copy page/bitmap immediately on receipt and avoid borrowing during decode; always clamp lengths.

## Executor borrow patterns

- Historical: we previously cached tuple `(off,len)` pairs in `PgScanStream` and scoped the iterator to avoid borrow conflicts. After the slot-race fix we iterate LPs by index over the owned page buffer; if reintroducing cached pairs, keep borrows tightly scoped and avoid holding `&mut self` while borrowing internal buffers.

## Testing & snapshots

- pgrx tests run each test in a transaction; READ COMMITTED uses a new snapshot per statement. Avoid manual `GetActiveSnapshot()` in tests to “recompute” visibility — compare results of core SELECT vs. pg_fusion SELECT instead.
- Integration tests that exercise bgworker/SHM require `shared_preload_libraries = 'pg_fusion'` in the test cluster; otherwise skip or expect failures.

## Alignment pitfalls

- Misaligned pointer deref can panic when interpreting `[u8]` as `AtomicU32` head/tail in ring buffers. Always allocate via `lockfree_buffer_layout` + `std::alloc::alloc(layout.layout)` and construct with `LockFreeBuffer::from_layout`.
- In tests, avoid static byte arrays for buffers; CI environments may place them at misaligned addresses. A `debug_assert!` on alignment in `LockFreeBuffer::new` helps catch misuse in debug builds.
