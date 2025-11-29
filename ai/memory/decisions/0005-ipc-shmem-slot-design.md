---
id: dec-0005
type: decision
scope: ipc
tags: ["shared-memory", "slots", "latch", "shmem.c", "dsm"]
updated_at: "2025-11-29"
importance: 0.85
status: accepted
decision_makers: ["Denis Smirnov"]
---

# Decision: SHM Slot Design for IPC (Backend ↔ Runtime)

## Problem

PostgreSQL uses a process model (one backend per client). We run the DataFusion executor in a separate background process (Tokio runtime). We need low‑overhead IPC between backends and the runtime.

## Considered Options

- OS sockets/queues: rejected — excessive kernel transitions; limited portability (e.g., macOS queues).
- PostgreSQL shared memory (SHM): accepted — battle‑tested infra, user‑space transfers, supported by pgrx.

## Chosen Approach

- Use pre‑fork shared memory (`shmem.c`), not dynamic SHM (`dsm.c`):
  - pre‑fork SHM gives identical virtual addresses in children; easy pointer math;
  - dynamic SHM forces relative addressing and later attachment.
- Per‑connection slots are leased from a free list (Treiber stack over SHM) with atomics and a spinlock for safety.
- Signaling uses PostgreSQL latches and POSIX `SIGUSR1` to wake a sleeping peer.
- Each connection owns a receive ring, a send ring, and a result ring. Heap pages/bitmaps are staged via per‑slot buffers.

## Pros

- Cross‑platform; minimal kernel crossings; well‑known PG primitives.

## Cons

- Requires controlled use of `unsafe` to access SHM layouts; strict adherence to memory ordering and bounds.

