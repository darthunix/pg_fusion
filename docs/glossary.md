# Glossary

[Documentation home](index.md)

This page defines the terms used by the rest of the pg_fusion documentation.
It assumes PostgreSQL familiarity, but not DataFusion or Arrow familiarity.

## DataFusion

Apache DataFusion is a Rust query execution engine for analytical plans. In
this project, it is useful because it already has vectorized operators for
joins, filters, aggregates, sorts, windows, repartitioning, and expression
evaluation.

DataFusion is not a PostgreSQL storage engine. pg_fusion does not ask
DataFusion to read PostgreSQL heap pages, check MVCC visibility, detoast
values, or return tuples to clients. PostgreSQL still owns those parts.

## Arrow

Apache Arrow is a columnar in-memory data format. Instead of storing one whole
row after another, Arrow stores one column's values together, plus null
bitmaps and type metadata.

DataFusion runs on Arrow batches. PostgreSQL heap scans produce PostgreSQL
tuples. pg_fusion's main runtime cost is crossing that boundary: selected
PostgreSQL tuple values have to be encoded into Arrow-compatible shared-memory
blocks before DataFusion can process them.

## RecordBatch

An Arrow `RecordBatch` is a group of same-length Arrow arrays with a schema.
Inside the worker, scan pages become `RecordBatch` values and DataFusion
operators consume and produce more batches.

## Heap Tuple And TupleTableSlot

A PostgreSQL heap tuple is PostgreSQL's row storage representation. A
`TupleTableSlot` is the executor's row carrier used while a plan is running.

pg_fusion reads table rows through PostgreSQL executor paths, receives
`TupleTableSlot` values, and encodes selected columns from those slots into
Arrow blocks. That keeps MVCC, TOAST, snapshots, permissions, and tuple
decoding inside PostgreSQL.

## CustomScan

`CustomScan` is a PostgreSQL extension hook that lets pg_fusion install a plan
node into PostgreSQL execution. The custom scan root is still executed by
PostgreSQL, but it coordinates with the pg_fusion worker for eligible
analytical execution.

## Shared Memory

Shared memory is a preallocated communication area between PostgreSQL backends,
dynamic scan workers, and the pg_fusion background worker. It is not a table
cache.

pg_fusion uses shared memory for control rings, scan rings, page storage,
runtime filter slots, wakeups, and metrics. Because this memory is fixed at
PostgreSQL startup, it bounds resource use and creates backpressure when the
worker or scan producers fall behind.

## Page Pool

The page pool is a shared-memory pool of fixed-size blocks. Scan producers
acquire a free page, write one Arrow-compatible block into it, detach it, and
send a descriptor to the consumer. When the last owner releases the page, it
returns to the pool and can be reused for a later scan or result page.

See [Memory And Pages](memory-and-pages.md) for the block format and lifetime
rules.

## Control Slots And Scan Slots

Shared-memory slots are fixed communication entries in preallocated shared
memory. A slot gives one participant a place to exchange control messages; it
does not run code.

Primary control slots carry execution lifecycle messages between a PostgreSQL
backend and the pg_fusion worker. Scan slots coordinate individual PostgreSQL
scan producers. Increasing these counts changes shared-memory capacity and
concurrency limits. It does not start PostgreSQL workers, create DataFusion
tasks, or change the DataFusion worker thread pool by itself.

## Zero-Copy

Zero-copy means a consumer can build Arrow arrays that point at bytes already
stored in the shared-memory page instead of copying those bytes into a new
buffer.

In pg_fusion, zero-copy is a constrained transport property, not a promise that
the whole query never copies. Streaming scan-adjacent operators can consume
page-backed batches directly. Operators that retain input batches, such as hash
join build sides, sorts, windows, and multi-use CTE materialization, need owned
copies so shared pages can return to the pool.

## PostgreSQL Pushdown Filter

A PostgreSQL pushdown filter is a query predicate that pg_fusion can render into
the PostgreSQL scan SQL. PostgreSQL applies it before the row is encoded into an
Arrow block. This avoids moving rows that PostgreSQL can already reject.

Pushdown filters are part of scan SQL planning. They are different from runtime
filters.

## Runtime Bloom Filter

A runtime Bloom filter is built during DataFusion execution for selected inner
hash joins. The worker observes values from the build side, publishes a compact
filter through shared memory, and PostgreSQL scan producers can probe it before
encoding rows on the probe side.

Runtime filters are opportunistic. If a filter is not ready or no filter slot is
available, rows pass through unfiltered and correctness is unchanged.

## DPHyp

DPHyp is the join-order search used by the `join_order` crate. pg_fusion feeds
it PostgreSQL statistics for eligible inner/cross equi-join components. The
result can reorder joins and choose hash build sides before the plan runs in
DataFusion.

Unsupported join shapes keep the DataFusion-planned order.

## CTID Range Scan

`ctid` identifies a physical tuple location in a PostgreSQL relation. For
eligible heap scans, pg_fusion can split a scan into disjoint CTID block ranges.
The leader backend and dynamic PostgreSQL background workers scan different
ranges and each producer writes its own Arrow pages into shared memory.

This is PostgreSQL-side scan parallelism. DataFusion still sees one logical
scan stream after the worker fans in the producer streams.

## Tokio And The Worker

The pg_fusion worker is one PostgreSQL background worker process that owns a
DataFusion runtime. Tokio drives DataFusion's async execution and internal
tasks inside that process. `pg_fusion.worker_threads` controls the Tokio
runtime threads for this worker, not PostgreSQL scan-producer processes.
`pg_fusion.max_fusion_tasks` caps concurrent backend execution tasks in that
runtime and defaults to the primary control slot count.

PostgreSQL scan producers remain PostgreSQL backend or background-worker
threads/processes. They do not call PostgreSQL APIs from Tokio tasks.

DataFusion tasks are scheduled inside the pg_fusion worker. PostgreSQL dynamic
workers are separate PostgreSQL processes used for PostgreSQL-owned scan work,
for example CTID range producers.
