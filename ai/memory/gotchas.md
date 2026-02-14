---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["joins", "partitions", "varlena", "signals", "shm", "utility_hook", "performance"]
updated_at: "2026-02-14"
importance: 0.7
---

# Gotchas & Pitfalls

- JOIN without partition‑awareness: with `partitions > 1` no rows are emitted → force `1` for now.
- TOAST/Compressed varlena: in projection → error; when not projected → skip, don’t crash.
- SIGUSR1: requires a valid client PID; not available on non‑Unix.
- SHM races: backend can reuse per-connection slots after metadata is sent; executor must copy page/bitmap immediately on receipt and avoid borrowing during decode; always clamp lengths.

## Executor borrow patterns

- Historical: we previously cached tuple `(off,len)` pairs in `PgScanStream` and scoped the iterator to avoid borrow conflicts. After the slot-race fix we iterate LPs by index over the owned page buffer; if reintroducing cached pairs, keep borrows tightly scoped and avoid holding `&mut self` while borrowing internal buffers.

## Planner vs CTAS/SELECT INTO

- Не полагайтесь на поля `Query` для распознавания CTAS/SELECT INTO в pgrx (PG17): используйте `ProcessUtility` hook для `CreateTableAsStmt` и thread‑local guard, чтобы planner hook не перехватывал внутренний SELECT.

## Testing & snapshots

- pgrx tests run each test in a transaction; READ COMMITTED uses a new snapshot per statement. Avoid manual `GetActiveSnapshot()` in tests to “recompute” visibility — compare results of core SELECT vs. pg_fusion SELECT instead.
- Integration tests that exercise bgworker/SHM require `shared_preload_libraries = 'pg_fusion'` in the test cluster; otherwise skip or expect failures.

## Alignment pitfalls

- Misaligned pointer deref can panic when interpreting `[u8]` as `AtomicU32` head/tail in ring buffers. Always allocate via `lockfree_buffer_layout` + `std::alloc::alloc(layout.layout)` and construct with `LockFreeBuffer::from_layout`.
- In tests, avoid static byte arrays for buffers; CI environments may place them at misaligned addresses. A `debug_assert!` on alignment in `LockFreeBuffer::new` helps catch misuse in debug builds.

## Scan performance (see dec-0009)

- SIGUSR1 per page: worker and backend exchange signals for every heap page. On macOS each `kill(SIGUSR1)` costs ~5-20 us. With 1000 pages this is 2000+ signals. Batch messages and signal once per batch.
- One message per wake: both worker loop (`worker.rs`) and backend loop (`exec_df_scan`) process a single message/request per wakeup cycle. Drain all available messages before signaling back.
- `wait_latch(1ms)`: backend falls into 1ms sleep when result ring is empty after 128-spin. This adds up to seconds on large scans. Replace with latch-only wait (no timeout) + proper nudge signals.
- `relation_open`/`relation_close` per page: `process_pending_heap_request` opens and closes the relation for every heap block. Cache the open relation for the scan lifetime.
- Result ring 64KB: fills quickly with wide rows; execution task blocks waiting for backend to drain. Consider 256-512 KB and batch reading on backend side.
- Full Arrow round-trip for SELECT *: heap tuple -> ScalarValue -> Arrow -> wire tuple -> MinimalTuple is ~6 transformation steps vs native PG's 1 step. Batch pages into larger RecordBatches to amortize overhead.
