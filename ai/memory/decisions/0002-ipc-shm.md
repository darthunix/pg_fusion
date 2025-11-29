---
id: dec-0002
type: decision
scope: ipc
tags: ["shared-memory", "ring", "lock-free", "slots"]
updated_at: "2025-11-29"
importance: 0.9
---

# Decision: IPC via Shared Memory (Lock‑Free Rings + Slot Buffers)

## Motivation

Low latency and zero‑copy paths for heap pages/bitmaps and result tuples.

## Consequences

- Per‑connection: receive/send ring + result ring.
- Per‑scan: dedicated `slot_id` for pages/bitmaps.
- Requires careful memory barriers and bounded SHM slicing.
