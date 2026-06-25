---
id: comp-host-runtime-0001
type: fact
scope: host_runtime
tags: ["pgrx", "datafusion", "shared-memory", "protocol", "slot_scan"]
updated_at: "2026-06-25"
importance: 0.8
---

# Component: Host Runtime

- `pg/extension` is the active pgrx extension crate.
- Backend control uses `protocol` messages over `control_transport`.
- Backend scan production uses PostgreSQL `slot_scan` plus `slot_encoder` to
  stream Arrow layout pages to the worker.
- Backend-to-worker scan control ring `Full` is scan-stream backpressure. The
  host keeps unsent scan pages and terminal scan frames pending and retries the
  same frame later; `IssuedOutboundPage::mark_sent()` only follows successful
  carrier delivery.
- PostgreSQL `max_parallel_workers_per_gather` controls the query-wide dynamic
  background-worker scan producer budget for eligible heap scans. Each scan
  always has a leader backend producer; positive budget is shared across scans,
  capped at `32`, and bounded by `max_worker_processes`. Each producer owns a
  dedicated scan slot and writes pages directly to the shared page pool; the
  worker fan-ins all producers for the same `scan_id` with a separate
  issued-page receiver per producer stream. Scan worker jobs carry standalone
  scan descriptors built from resolved `PgScanSpec` values instead of the
  original user SQL, avoiding `search_path` dependence in dynamic background
  workers. Dynamic worker capacity failures clean up partial launches and
  continue leader-only for the current and remaining scans; readiness/protocol
  failures still fail the query.
- The primary worker owns a Tokio runtime for DataFusion physical planning and
  result-stream execution. The PostgreSQL background-worker thread remains the
  transport scheduler and PG latch/signal integration point; Tokio tasks do not
  own control transport or PostgreSQL APIs. `pg_fusion.worker_threads` controls
  the runtime thread count (`0` chooses from available CPU parallelism), and
  `pg_fusion.max_fusion_tasks` limits concurrent backend execution tasks (`0`
  defaults to the primary control slot count). Worker physical planning still
  fixes DataFusion `target_partitions` at `1`. Root physical plans are driven
  through DataFusion `execute_stream`, so multi-partition roots such as `UNION`
  are collected by DataFusion rather than by manually executing only partition
  `0`. PostgreSQL scan producers remain ordinary backend/scan-worker threads:
  they communicate through shared-memory scan transport and never call
  PostgreSQL APIs from Tokio tasks.
- Worker-side DataFusion spill is opt-in through Postmaster GUC
  `pg_fusion.worker_memory_limit_mb`. `0` preserves the default unbounded
  DataFusion runtime; positive values use one finite `FairSpillPool` shared by
  concurrent worker tasks and per-execution OS temp directories under a
  cluster-scoped worker spill root.
  The primary worker marks owned worker-incarnation directories, removes stale
  marked directories in the same cluster namespace on startup, and removes
  execution directories on success, failure, or cancel. Disabled spill does not
  create directories or run spill garbage collection. This is not PostgreSQL
  `BufFile` storage and does not honor `temp_tablespaces`, `temp_file_limit`,
  or ResourceOwner cleanup. With DataFusion 53, this covers sort and row-hash
  aggregate spill paths; ordinary `HashJoinExec` remains non-spilling and fails
  under the finite pool when it cannot reserve memory.
- Worker execution lives in `runtime/worker` and consumes scan pages as Arrow
  batches through `page/import`. Transport scan streams use a bounded
  DataFusion batch channel and short idle polling interval so scan threads can
  absorb minor downstream polling gaps without sleeping for millisecond-scale
  page handoff latency. The defaults are `32` batches and `50us`; the backend
  captures `pg_fusion.scan_batch_channel_capacity` and
  `pg_fusion.scan_idle_poll_interval_us` at query start and passes them to the
  worker through `StartExecution`. Scan transport normalization allows empty
  schemas so dummy-projection scans can pass row counts as zero-column batches;
  result-page transport still rejects non-empty empty-schema result batches.
- Runtime Bloom filters are controlled by `pg_fusion.runtime_filter_enable`
  (default `on`) plus postmaster-sized pool settings
  `pg_fusion.runtime_filter_count`, `pg_fusion.runtime_filter_bits`, and
  `pg_fusion.runtime_filter_hashes`. Worker physical planning allocates filters
  from a shared-memory pool and records metadata keyed by `RuntimeFilterExecId`
  (primary control slot id, generation, lease epoch, session epoch), scan id,
  output column, and key type. Backend scan producers, including dynamic
  standalone scan workers, attach probes by `(RuntimeFilterExecId, scan_id)` at
  scan open and test supported bool, integer, float, uuid, binary, and
  text-like keys before Arrow encoding. No control-ring message is needed for
  readiness; probes read the shared lifecycle word and pass rows unfiltered
  until the matching generation is `Ready`.
- Results return as issued Arrow pages and are projected into PostgreSQL tuple
  slots through `pg/slot_import`.
- Worker-originated execution failures send a bounded UTF-8 detail string in
  `WorkerExecutionToBackend::FailExecution`, and the backend includes that text
  in the SQL-facing `pg_fusion execution failed` error. Do not collapse these
  failures back to `detail=None`; the worker log is diagnostic backup, not the
  primary user-visible error channel.
- The issuance permit pool is sized from `pg_fusion.page_count`; there is no
  separate host GUC for permit count.
- Runtime metrics are global shared-memory counters exposed by
  `pg_fusion_metrics()` and reset by `pg_fusion_metrics_reset()`. Page handoff
  latency is measured with page descriptor stamps, not by instrumenting ring
  internals. Worker scan-thread metrics additionally split scan page handoff
  into idle sleep time, page read/import time, `tx.send(Ok(batch))` time, and
  frame-read-to-DataFusion-batch-delivery time. Runtime filter metrics count
  allocations, ready publications, pool exhaustion, build rows, probed rows,
  rejected rows, and probe rows that passed unfiltered because a filter was not
  ready. Worker DataFusion spill metrics are summed from the executed physical
  plan tree after execution and exposed as global spill count, spilled row, and
  spilled byte counters. The worker also records spill cleanup diagnostics:
  leaked DataFusion files/bytes should stay zero after execution, and
  pg_fusion-owned execution spill directory create/remove/error counters track
  explicit directory lifecycle cleanup.
- Backend scan timing is always on and intentionally coarse: `scan_page_fill_ns`
  covers successful emitted page fills, with cheap prepare/finish buckets and a
  retry counter. It does not instrument slot-to-Arrow internals; use external
  flamegraphs for deformation, detoast, and page-write attribution.
- The fast backend scan receiver keeps the Rust row callback monomorphized
  through `slot_scan`; the unavoidable indirect boundary is PostgreSQL's
  `DestReceiver.receiveSlot` function pointer.
- `EXPLAIN` stays backend-local: `backend_service` lowers the planned query to
  a DataFusion physical plan, renders PostgreSQL scan leaves with present
  soft-limit/local-row-cap metadata, and prints the nested multiline
  `slot_scan` plan directly below the leaf. `EXPLAIN VERBOSE` also includes the
  compiled scan SQL but omits internal scan ids, table oids, planner fetch
  hints, and raw Arrow schema debug dumps. Scan leaves also render pg_fusion's
  planned dynamic scan producers and, for `EXPLAIN ANALYZE`, the producer set
  installed during execution.
- The retired raw heap page stack (`executor`, `scan`, `storage`, `protocol`,
  `common`) is no longer part of the workspace.
