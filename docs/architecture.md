# Architecture

[Documentation home](index.md)

`pg_fusion` adds a DataFusion execution path to PostgreSQL without taking table
access away from PostgreSQL. If the terms are new, start with the
[Glossary](glossary.md).

The central rule is that PostgreSQL remains responsible for table access, while
DataFusion runs the selected analytical operators above PostgreSQL scan streams.

This page is written for administrators and users who need to understand what
the extension adds to a PostgreSQL instance, where resources are spent, and why
some queries are a better fit than others.

## Runtime Shape

A running `pg_fusion` installation has three important pieces.

**PostgreSQL backends** are the ordinary per-connection PostgreSQL processes.
They receive SQL, run PostgreSQL hooks, own snapshots, start pg_fusion custom
scan execution, and return final tuple slots to the client.

**PostgreSQL scan producers** are the processes that read table data for one
pg_fusion scan. Producer `0` is the leader backend. For eligible heap scans,
additional dynamic PostgreSQL background workers can scan disjoint CTID block
ranges. Each producer reads through PostgreSQL executor paths and writes its own
Arrow pages into shared memory.

**The pg_fusion worker** is one PostgreSQL background worker process that hosts
the DataFusion runtime. Eligible backends send plans and scan pages to this
worker. DataFusion is not embedded as a separate runtime in every backend.

**Shared memory** is the preallocated communication area between backends and
the worker. It contains page storage, execution channels, scan channels,
runtime filters, wakeups, and metrics.

## What Happens To A Query

When a backend receives a top-level `SELECT`, the pg_fusion planner hook checks
whether the query is supported. With `pg_fusion.enable = on`, unsupported user
SELECT shapes fail closed with a controlled pg_fusion planning error.

For an eligible query:

1. PostgreSQL resolves relations, types, and the execution snapshot.
2. pg_fusion builds a DataFusion plan with PostgreSQL table leaves represented
   as PostgreSQL scan streams.
3. The backend starts an execution session with the shared worker.
4. PostgreSQL scan producers read heap tuples through PostgreSQL executor
   paths. Eligible heap scans may be split across CTID block ranges.
5. Each producer encodes selected rows and columns into page-backed Arrow
   blocks in shared memory.
6. The worker imports those Arrow blocks, fans producer streams into logical
   DataFusion scans, and runs DataFusion operators.
7. Result batches are written back into shared-memory pages.
8. The backend imports result pages and returns PostgreSQL tuple slots to the
   client.

The worker never scans PostgreSQL heap storage directly. PostgreSQL remains the
owner of MVCC visibility, TOAST, tuple decoding, and final tuple materialization.

## The Expensive Boundary

PostgreSQL heap tuples are row oriented. DataFusion runs on Arrow batches. Every
row that crosses into the worker must be decoded from PostgreSQL slots and
encoded into Arrow page layout.

That conversion is useful only when the work moved to DataFusion pays for the
boundary cost. For that reason, pg_fusion tries to keep scan output small:

- push filters into PostgreSQL scan SQL when possible;
- project only the columns needed above the scan;
- apply eligible runtime filters before encoding probe-side scan rows;
- stream rows page by page instead of building large backend-local buffers.

If a query sends many wide rows to the worker and does little analytical work
after the scan, the round trip through Arrow pages can be a net cost.

The shared page format and zero-copy lifetime rules are described in
[Memory And Pages](memory-and-pages.md).

## Resource Model

The intended operational model is a resource box.

- PostgreSQL backends own sessions, snapshots, and table access.
- Backend scan producers write output page by page into the shared page pool.
  Eligible heap scans can use CTID range producers; see
  [Execution Model](execution-model.md#scan-production).
- The DataFusion worker owns analytical CPU scheduling, its memory pool, and
  worker spill files.
- Shared memory owns fixed transport capacity: pages, execution channels, scan
  channels, filters, wakeups, and metrics.

This is different from putting a separate DataFusion runtime in each backend.
Many backends can feed work into the same cooperative DataFusion/Tokio runtime,
and the worker can reuse its configured threads across submitted executions.
The worker's PostgreSQL background-worker thread still owns primary transport
and PG latch/signal integration; physical planning and execution run as
per-backend Tokio tasks capped by `pg_fusion.max_fusion_tasks`. At the same
time, memory, spill, and shared-memory capacity are easier to reason about
because they are configured as one worker plus one preallocated transport area.

The page pool bounds scan memory behavior. If scan pages or scan channels are
exhausted, execution applies backpressure instead of allocating an unbounded
amount of memory inside each backend.

This bounded resource model is about progress, not strict fairness. The page
lifetime and progress model are detailed in
[Memory And Pages](memory-and-pages.md#progress-not-fairness).

## Shared Memory In Plain Terms

Shared memory is not a cache of table data. It is a transport area.

- Primary execution channels carry session lifecycle messages such as start,
  cancellation, completion, and errors.
- Scan channels let the worker ask PostgreSQL scan producers for table data.
- The page pool carries Arrow scan pages to the worker and result pages back to
  the backend.
- Runtime filter slots let the worker publish compact Bloom filters that
  PostgreSQL scan producers can use before encoding rows.
- Metrics record where time and data movement are spent.

Shared-memory size is fixed at PostgreSQL startup. See
[Configuration](configuration.md) for the settings that size the worker and
transport area, and [Metrics](metrics.md) for runtime diagnostics.

## What This Means In Practice

The runtime shape is most useful when worker-side analytical work pays for the
scan/result boundary crossing. See [Workloads](workloads.md) for candidate
query shapes and what to collect when evaluating a workload.
