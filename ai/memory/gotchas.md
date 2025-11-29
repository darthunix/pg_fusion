---
id: gotchas-0001
type: gotcha
scope: repo
tags: ["joins", "partitions", "varlena", "signals"]
updated_at: "2025-11-29"
importance: 0.7
---

# Gotchas & Pitfalls

- JOIN without partition‑awareness: with `partitions > 1` no rows are emitted → force `1` for now.
- TOAST/Compressed varlena: in projection → error; when not projected → skip, don’t crash.
- SIGUSR1: requires a valid client PID; not available on non‑Unix.
- SHM races: don’t cache borrowed slices beyond one read cycle; clamp lengths.
