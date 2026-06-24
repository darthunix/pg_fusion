# Execution Model

[Documentation home](index.md)

This page follows one eligible query from PostgreSQL planning to result rows.
For the higher-level process and resource model, see [Architecture](architecture.md).
For terminology, see [Glossary](glossary.md).

## Startup

`pg_fusion` must be loaded at PostgreSQL startup because it registers:

- PostgreSQL planner and custom scan hooks;
- shared-memory regions;
- the DataFusion background worker.

The setup command is documented in [Configuration](configuration.md#required-preload).

## Planning

`pg_fusion` currently targets top-level `SELECT` statements. The planner hook
bypasses pg_fusion for disabled `pg_fusion.enable`, non-`SELECT` statements,
and pg_fusion management SQL such as metrics functions. With
`pg_fusion.enable = on`, unsupported user SELECT shapes, including modifying
CTEs, fail closed with a controlled pg_fusion planning error.

Supported queries are planned for DataFusion, but PostgreSQL table access is
kept as PostgreSQL scan streams. PostgreSQL remains the authority for relation
identity, snapshots, and PostgreSQL type metadata. pg_fusion uses its typed
PostgreSQL `Query` frontend; there is no SQL-text planner fallback.

During planning, pg_fusion tries to push filters and projections into
PostgreSQL scans before rows cross into Arrow pages:

- pushed filters run inside PostgreSQL scan SQL;
- projections remove unused columns before slot-to-Arrow encoding;
- unsupported residual filters stay above the scan or make the shape
  ineligible, depending on where they are discovered.

For eligible inner and cross equi-join components, pg_fusion also uses
PostgreSQL statistics with the DPHyp join-order optimizer to choose a better
join order before execution. Join shapes outside that supported subset keep the
order produced by the normal planning path.

This pushdown matters. Tuple decoding and slot-to-Arrow encoding happen in
PostgreSQL backends, so every unnecessary row or column sent to the worker adds
conversion and shared-memory transport cost.

## Execution

At execution time PostgreSQL runs a `PgFusionScan` custom scan root.

The backend opens an execution session with the shared worker and starts scan
producers for PostgreSQL table leaves. Each producer reads rows through
PostgreSQL executor paths, using the query snapshot owned by the backend.

## Scan Production

Scan producers do not build an unbounded backend-local result set. They fill
shared-memory pages with Arrow batches and hand those pages to the worker. If
the shared page pool or scan channels are full, scan production waits for the
worker to catch up.

For ordinary leader-only scans, the backend drains a PostgreSQL portal and
encodes selected tuple-slot values into Arrow blocks.

For eligible heap scans, pg_fusion can parallelize scan production by CTID block
range:

1. A query-wide budget is taken from PostgreSQL's
   `max_parallel_workers_per_gather`, capped by pg_fusion and by PostgreSQL
   worker capacity.
2. The leader divides the relation into disjoint CTID block ranges.
3. Producer `0` stays in the leader backend.
4. Additional dynamic PostgreSQL background workers receive standalone scan
   descriptors for their ranges.
5. Each producer reads through PostgreSQL executor paths and writes its own
   Arrow pages into the shared page pool.
6. The DataFusion worker fans producer streams into one logical `PgScanExec`.

Every producer writes to pages it acquired from the shared page pool. Producers
do not write into the same page concurrently.

## Shared-Memory Transport

Execution uses several shared-memory resources with separate purposes:

- primary control rings carry execution lifecycle messages;
- scan control rings let the worker open and coordinate individual scan
  producers;
- issued page descriptors transfer ownership of pages;
- the page pool holds scan and result blocks;
- runtime filter slots publish optional Bloom filters;
- metrics record timing and counters.

The page pool and block format are described in
[Memory And Pages](memory-and-pages.md). The important execution rule is that
shared-memory pages are reused. A page can return to the pool only after the
last Arrow/page owner drops it.

## Worker Execution

The worker imports scan pages, runs DataFusion physical operators, and writes
result pages back to shared memory. Tokio drives DataFusion execution inside the
worker process. DataFusion may split physical operators into partitions and
tasks; those tasks are scheduled inside the worker's Tokio runtime and run on
the configured worker thread pool. The current worker planning contract still
sets DataFusion `target_partitions` to `1`, so thread count and physical plan
partition count are separate controls.

The PostgreSQL background-worker thread remains the scheduler for primary
control transport, result-frame delivery, and PostgreSQL latch/signal handling.
Physical planning and execution run as per-backend Tokio tasks. The
`pg_fusion.max_fusion_tasks` GUC caps how many backend executions can be active
inside the worker; its default `0` uses the primary control slot count.

PostgreSQL scan producers are not Tokio tasks. They remain PostgreSQL backend or
background-worker execution paths because they call PostgreSQL APIs. Scan
control slots coordinate those producer streams, but slots are fixed
shared-memory channels. They do not create producer processes or DataFusion
tasks.

Streaming scan-adjacent DataFusion operators can consume imported page-backed
Arrow batches without copying. When the physical plan reaches an operator that
can retain input batches, such as a hash join build side, sort, window, or
multi-use CTE materialization, pg_fusion inserts materialization so the shared
page can be released and reused.

## Result Import

Worker result batches are encoded into Arrow blocks and sent back through the
same page pool. The backend imports result pages and stores rows into the
PostgreSQL tuple slot returned to the client.

Some result values are copied into PostgreSQL memory because final tuple slots
need PostgreSQL-owned datums. That is separate from worker-side zero-copy scan
import.

## Runtime Filters

For selected inner hash joins, the worker can build a runtime Bloom filter from
one side of the join and publish it through shared memory. PostgreSQL scan
producers can then test probe-side rows before encoding them into Arrow pages.

Runtime filters are an optimization, not a semantic requirement. If a filter is
not available or no shared filter slot can be acquired, execution continues
without it.

Runtime filters are different from PostgreSQL pushdown filters:

- pushdown filters come from planning and run as PostgreSQL scan SQL;
- runtime filters are built while the query is already executing;
- pushdown filters can remove rows based on the original query predicate;
- runtime filters can only reject rows that are definitely absent from an
  eligible hash join build side.

## Cancellation And Cleanup

The backend owns the PostgreSQL execution lifecycle. When a pg_fusion query
finishes, errors, or is canceled, the backend tears down active scan producers,
releases transport leases, and releases result ingress state. Worker-side
execution directories for spill are cleaned up by the worker runtime.

## EXPLAIN

`EXPLAIN` shows the DataFusion plan and the PostgreSQL scan SQL used by scan
leaves. For CTID-range scan producers, verbose output shows representative scan
SQL with range placeholders rather than exposing one concrete worker range as
if it were the only scan.

`EXPLAIN ANALYZE` can also report actual scan producer counts installed during
execution. Use [Metrics](metrics.md) when you need timing and transport
diagnostics beyond plan shape.
