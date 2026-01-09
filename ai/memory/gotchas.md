---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["joins", "partitions", "varlena", "signals"]
updated_at: "2025-12-07"
importance: 0.7
---

# Gotchas & Pitfalls

- JOIN without partition‑awareness: with `partitions > 1` no rows are emitted → force `1` for now.
- TOAST/Compressed varlena: in projection → error; when not projected → skip, don’t crash.
- SIGUSR1: requires a valid client PID; not available on non‑Unix.
- SHM races: don’t cache borrowed slices beyond one read cycle; clamp lengths.

## Executor borrow patterns

- Caching tuple `(off,len)` pairs in `PgScanStream`: fill a self-owned `Vec<(u16,u16)>` and create the iterator inside a tight scope. Clone needed metadata (`attrs_full`, `proj_indices`) into locals and avoid using `self` while the iterator (borrowing the pairs slice) is alive. This sidesteps borrow checker conflicts between a mutable `&mut self` and an immutable borrow of `self.pairs`.

## Testing & snapshots

- pgrx tests run each test in a transaction; READ COMMITTED uses a new snapshot per statement. Avoid manual `GetActiveSnapshot()` in tests to “recompute” visibility — compare results of core SELECT vs. pg_fusion SELECT instead.
- Integration tests that exercise bgworker/SHM require `shared_preload_libraries = 'pg_fusion'` in the test cluster; otherwise skip or expect failures.

## Alignment pitfalls

- Misaligned pointer deref can panic when interpreting `[u8]` as `AtomicU32` head/tail in ring buffers. Always allocate via `lockfree_buffer_layout` + `std::alloc::alloc(layout.layout)` and construct with `LockFreeBuffer::from_layout`.
- In tests, avoid static byte arrays for buffers; CI environments may place them at misaligned addresses. A `debug_assert!` on alignment in `LockFreeBuffer::new` helps catch misuse in debug builds.
