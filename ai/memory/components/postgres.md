---
id: comp-postgres-0001
type: fact
scope: postgres
tags: ["pgrx", "executor", "TupleTableSlot", "heap", "ipc"]
updated_at: "2025-11-29"
importance: 0.8
---

# Component: Postgres (pgrx Extension)

## Responsibilities

- Drives lifecycle: Parse/Metadata/Bind/Optimize/Translate/BeginScan/ExecScan/EndScan.
- Reads heap blocks and publishes them into SHM slots + sends metadata (scan_id/slot_id/vis_len).
- Reads the result ring, decodes wire MinimalTuple frames, and fills a `TupleTableSlot`.

## Safety

- No panics; errors via `FusionError`.
- Minimize work outside PostgreSQL’s safe APIs; always unlock/release buffers/locks.

## Known Limitations

- MVCC visibility is simplified (all visible) — TODO: proper XMIN/XMAX/hints checks.
- SIGUSR1 requires a valid client PID; unsupported on non‑Unix.
