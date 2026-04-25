---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["shm", "pgrx", "slot_scan", "arrow", "testing"]
updated_at: "2026-04-25"
importance: 0.7
---

# Gotchas & Pitfalls

- `pg_fusion` is the active extension crate at `pg/extension`; do not re-add
  the retired raw-heap `executor`/`scan`/`storage` stack.
- Integration tests that exercise the background worker/shared memory need
  `shared_preload_libraries = 'pg_fusion'` in the test cluster.
- `pg_fusion` queries in `pg/extension` smoke tests currently run through a
  normal PostgreSQL client connection, not SPI-owned execution contexts.
- `slot_scan` should execute trusted compiler-generated scan SQL. SQL safety and
  expression pushdown policy belong in `scan_sql`.
- `backend_service` should not have a standalone cargo test harness: it links
  `slot_scan`, which references PostgreSQL SPI symbols. Put those cases in
  `pg/test` and run them with `cargo pgrx test pg17 -p pg_test`.
- Page-backed Arrow batches must not outlive their transfer/issuance ownership
  contract. Release pages only through the existing page/issued-frame APIs.
- Misaligned pointer deref can panic when interpreting shared-memory bytes as
  atomics. Allocate ring regions through the established lockfree layout paths.
