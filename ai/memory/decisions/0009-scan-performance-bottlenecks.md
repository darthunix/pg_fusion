---
id: dec-0009
type: decision
scope: executor/backend
tags: ["performance", "signals", "ipc", "scan", "wait_latch", "result-ring", "prefetch"]
updated_at: "2026-02-14"
importance: 0.9
status: proposed
decision_makers: ["Denis Smirnov"]
---

# Decision: Fix Heap Scan Performance Bottlenecks (6x slower than native PG)

## Problem

`SELECT * FROM t` through pg_fusion runs ~6x slower than native PostgreSQL.
Root cause is not a single issue but a cascade of per-page and per-row overheads
in the IPC hot path between backend and the DataFusion worker.

## Identified Bottlenecks (sorted by estimated impact)

### 1. SIGUSR1 ping-pong per page (highest impact)

Every heap page requires at least 2 signal round-trips:
- backend -> worker: page ready in SHM (`signal_server`)
- worker -> backend: next heap request + result rows (`signal_client`)

Each `kill(SIGUSR1)` costs ~5-20 us on macOS (Mach kernel). For a 1000-page table
that is 2000+ signals = 10-40 ms of pure signal overhead.

**Fix:** Batch IPC: accumulate multiple messages before signaling. Worker should
drain all available messages from the recv buffer per wake instead of one-at-a-time.
Signal once after the full batch is processed.

### 2. Backend processes ONE heap request per exec_df_scan iteration

`process_pending_heap_request` in `backend.rs` reads at most one Heap request per
call. Even with prefetch=6, the backend serves pages one at a time, then attempts
to read a result row, spins, and falls into `wait_latch(1ms)`.

**Fix:** Process ALL pending heap requests in a batch before checking the result
ring. Keep the relation open across the entire scan, not per-page.

### 3. `wait_latch(1ms)` baseline latency

When the result ring is empty and spin (128 iterations) fails, the backend sleeps
for up to 1 ms. For 1000 pages with occasional empty ring gaps, this accumulates
to hundreds of milliseconds.

**Fix:** Use `WL_LATCH_SET | WL_POSTMASTER_DEATH` without timeout for the
blocking wait. Ensure proper nudge signals are sent from the worker whenever
result rows are written. Add timeout only as a safety fallback (e.g. 100 ms).

### 4. `relation_open` / `relation_close` per page

`process_pending_heap_request` opens and closes the relation (with AccessShareLock)
for every single heap page. This involves relcache lookup and lock acquire/release.

**Fix:** Cache the open relation in a thread-local for the duration of the scan.
Open once in `begin_df_scan` or on first heap request, close in `end_df_scan`.

### 5. Worker loop: one message per wake + signal after each

`worker.rs` main loop: `poll() -> process_message() -> signal_client()`.
If 6 Heap responses arrive at once, the worker processes them one by one,
sending SIGUSR1 after each.

**Fix:** After `poll()`, drain all available messages in a tight loop. Signal
the client once after the batch. Same approach on the executor side in
`Connection::serve`.

### 6. Full Arrow round-trip per row for SELECT *

Each row goes through:
heap tuple -> ScalarValue -> ColBuilder -> Arrow Array -> RecordBatch ->
encode_wire_tuple (per row) -> result ring -> decode_wire_tuple -> Datum[] ->
heap_form_minimal_tuple -> TupleTableSlot.

Native PG: heap tuple -> TupleTableSlot (one step).

**Fix (short-term):** Batch multiple pages into one larger RecordBatch to amortize
builder/array overhead. Increase batch size to 4096+ rows instead of per-page.

**Fix (long-term):** For `SELECT *` without filters/aggregates, consider a
passthrough mode that copies heap tuples directly into result ring without
Arrow conversion.

### 7. Result ring too small (64 KB)

`RESULT_RING_CAP = 64 * 1024`. With ~100-200 byte rows the ring holds only
300-600 rows and fills quickly. The execution task blocks on a full ring while
the backend reads one row at a time.

**Fix:** Increase to 256 KB or 512 KB. Also consider reading multiple rows from
the result ring per `exec_df_scan` call to drain faster.

## Proposed Fix Priority

1. Batch message processing in worker loop (drain all, signal once)
2. Batch heap request processing in backend (drain all, keep relation open)
3. Remove 1ms wait_latch; use latch-only wait with proper signals
4. Cache relation across scan lifetime
5. Increase result ring capacity
6. Batch pages into larger RecordBatches
7. (Longer-term) Passthrough mode for simple scans

## Measurement Approach

- Use existing tracing (`RUST_LOG=trace`): analyze elapsed_us fields in worker log
- Add histogram counters for: time in wait_latch, time in signal delivery,
  time in decode/encode per page, time in relation_open/close
- Use `samply` or Instruments Time Profiler attached to worker PID
- Compare with `PG_FUSION_PREFETCH` varied from 1 to 16

## Consequences

- Significant reduction in signal overhead (2000 signals -> ~50-100)
- Reduced baseline latency from wait_latch removal
- Lower per-page cost from relation caching
- Maintains correctness: visibility semantics unchanged, SHM safety preserved
