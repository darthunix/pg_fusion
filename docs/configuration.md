# Configuration

[Documentation home](index.md)

`pg_fusion` uses PostgreSQL GUCs to configure the runtime path, the DataFusion
worker, shared-memory transport, scan streaming, runtime filters, spill, and
diagnostics.

The [architecture](architecture.md) explains why the runtime is shaped as one
background worker plus preallocated shared memory. [Glossary](glossary.md)
defines the DataFusion, Arrow, page-pool, filter, DPHyp, and CTID terms used
below. This page lists the knobs that configure that shape.

## Required Preload

```conf
shared_preload_libraries = 'pg_fusion'
```

`pg_fusion` must be loaded at postmaster start because it registers PostgreSQL
hooks, shared memory, and a background worker.

## Enable The Runtime Path

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.enable` | `off` | User/session | Enables the strict pg_fusion path for supported SELECT queries in the session. |
| `pg_fusion.backend_log_level` | `0` | User/session | Backend diagnostics: `0=off`, `1=basic`, `2=trace`. |

Use `pg_fusion.enable` when comparing a query against vanilla PostgreSQL:

```sql
SET pg_fusion.enable = off;
-- run PostgreSQL plan

SET pg_fusion.enable = on;
-- run pg_fusion plan, or fail with a pg_fusion planning error if unsupported
```

## Size The Worker

The worker is the DataFusion resource box. These settings are postmaster-level.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.worker_threads` | `0` | DataFusion Tokio runtime thread count. `0` chooses automatically from available CPU parallelism. |
| `pg_fusion.max_fusion_tasks` | `0` | Maximum concurrent backend execution tasks inside the worker. `0` uses `pg_fusion.control_slot_count`; explicit values are capped at the control slot count. |
| `pg_fusion.worker_memory_limit_mb` | `0` | DataFusion worker memory limit. `0` uses the default unbounded runtime and disables worker spill. |
| `pg_fusion.worker_spill_directory` | `''` | Base directory for worker-owned spill files. Empty uses OS temporary storage. |
| `pg_fusion.worker_log_filter` | `warn` | Worker tracing filter. |
| `pg_fusion.log_path` | `/tmp/pg_fusion.log` | Worker diagnostic log path. |

Set `pg_fusion.worker_memory_limit_mb` above `0` only when you want a finite
DataFusion memory pool and worker-owned spill. Spill files are not PostgreSQL
temporary files.

`pg_fusion.worker_threads` controls the Tokio runtime threads inside the
pg_fusion worker process. It does not control PostgreSQL dynamic scan workers,
and it does not by itself change DataFusion physical plan partitioning. Set it
to `1` to run the multi-thread Tokio runtime with one worker thread.

`pg_fusion.max_fusion_tasks` limits how many backend executions the worker can
have active at once. The default `0` uses the primary control slot count, so one
backend can normally own one worker task. Lower explicit values can be used to
serialize or throttle worker execution without resizing shared memory.

## Size Shared Memory

Shared-memory settings are postmaster-level because they define fixed transport
layout at PostgreSQL startup.

Primary execution channels carry session lifecycle messages such as start,
cancel, completion, and errors.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.control_slot_count` | `64` | Number of primary backend/worker control slots. |
| `pg_fusion.control_backend_to_worker_capacity` | `8192` | Per-slot primary control ring capacity from backend to worker. |
| `pg_fusion.control_worker_to_backend_capacity` | `8192` | Per-slot primary control ring capacity from worker to backend. |

Scan channels are separate because scan requests and responses can be frequent.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.scan_slot_count` | `64` | Number of dedicated scan control slots. |
| `pg_fusion.scan_backend_to_worker_capacity` | `256` | Dedicated scan ring capacity from backend to worker. |
| `pg_fusion.scan_worker_to_backend_capacity` | `256` | Dedicated scan ring capacity from worker to backend. |

The page pool carries Arrow scan pages to the worker and result pages back to
the backend.

| Setting | Default | Description |
| --- | ---: | --- |
| `pg_fusion.page_size` | `65536` | Shared page size in bytes. |
| `pg_fusion.page_count` | `256` | Number of shared pages. Also sizes the issued-page permit pool. |

More pages can reduce backpressure but increase fixed shared-memory footprint.
The page pool is shared by scan and result traffic, and pages return to the pool
after the last owner releases them. See [Memory And Pages](memory-and-pages.md)
for the block format, zero-copy imports, materialization boundaries, and the
progress-not-fairness model.

## Tune Scan Streaming

Scan tuning controls how PostgreSQL scan producers feed Arrow pages to the
worker.

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.scan_fetch_batch_rows` | `1024` | Postmaster | Rows requested per PostgreSQL portal drain in backend scan streaming. |
| `pg_fusion.scan_batch_channel_capacity` | `32` | User/session | Bounded worker scan batch channel capacity per PostgreSQL scan stream. |
| `pg_fusion.scan_idle_poll_interval_us` | `50` | User/session | Worker scan idle poll interval in microseconds. |
| `pg_fusion.estimator_initial_tail_bytes_per_row` | `64` | Postmaster | Initial variable-width Arrow page tail estimate. |

Scan producers can be leader-only, or they can be dynamic PostgreSQL
background workers scanning disjoint CTID block ranges for eligible heap scans.
Each producer writes its own Arrow pages into the shared page pool. The worker
fans those producer streams into one logical scan, as described in
[Execution Model](execution-model.md#scan-production).

If scan metrics show high backend page fill time, the bottleneck may be
PostgreSQL scanning, tuple decoding, detoast, or slot-to-Arrow encoding rather
than worker execution.

## Configure Planning Optimizations

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.join_reordering` | `on` | User/session | Enables statistics-based join reordering for eligible joins. |

PostgreSQL scan planning still matters because scan leaves execute trusted
PostgreSQL scan SQL through PostgreSQL executor portals.

Useful PostgreSQL planner experiment settings include:

```conf
max_parallel_workers_per_gather = 2
min_parallel_table_scan_size = '8MB'
parallel_setup_cost = 1000
parallel_tuple_cost = 0.1
```

These settings affect PostgreSQL-side scan planning. They do not configure
DataFusion worker memory.

`max_parallel_workers_per_gather` is especially important for pg_fusion CTID
range scans. `0` keeps scan production leader-only. A positive value gives
pg_fusion a query-wide budget for dynamic PostgreSQL scan producers, still
capped by pg_fusion limits and by available PostgreSQL worker capacity. It does
not control DataFusion's Tokio tasks or the DataFusion worker thread count.

## Configure Runtime Filters

Runtime filters can reduce rows before slot-to-Arrow encoding on eligible hash
join probe scans.

| Setting | Default | Level | Description |
| --- | ---: | --- | --- |
| `pg_fusion.runtime_filter_enable` | `on` | User/session | Enables runtime Bloom filters for eligible hash joins. |
| `pg_fusion.runtime_filter_count` | `64` | Postmaster | Number of shared-memory runtime filter slots. |
| `pg_fusion.runtime_filter_bits` | `1048576` | Postmaster | Bloom filter bit count per slot. |
| `pg_fusion.runtime_filter_hashes` | `4` | Postmaster | Number of Bloom hash probes per slot. |

If the pool is exhausted, execution continues without the missing filter and
records a diagnostic counter.

## Worker Spill

`pg_fusion.worker_memory_limit_mb = 0` keeps DataFusion on the default
unbounded runtime and disables worker spill.

Setting it above `0` enables a finite DataFusion memory pool and worker-owned
OS temporary spill files. `pg_fusion.worker_spill_directory` may point at an
absolute spill root; empty uses OS temporary storage under `pg_fusion/spill`.

This v1 spill path is owned by the pg_fusion worker. It does not use PostgreSQL
`temp_tablespaces`, `temp_file_limit`, or `ResourceOwner` cleanup.

See [Metrics](metrics.md#did-spill-happen) for spill diagnostics.
