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
