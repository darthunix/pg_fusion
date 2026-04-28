---
id: arch-overview-0001
type: fact
scope: repo
tags: ["architecture", "datafusion", "pgrx", "shared-memory", "ipc", "slot_scan"]
updated_at: "2026-04-27"
importance: 0.8
---

# pg_fusion Architecture Overview

The active PostgreSQL extension is `pg/extension` (`pg_fusion`). It
hooks PostgreSQL planning/execution and delegates selected query work to an
in-process DataFusion worker runtime through shared-memory control rings and
page-backed Arrow batches.

## Top-Level Runtime Path

- `pg/extension/`: pgrx host extension, GUCs, planner/custom-scan hooks,
  shared-memory bootstrap, and background worker entrypoint.
- `pg/backend_service/`: backend execution state, scan coordination, and
  PostgreSQL slot-scan page production.
- `worker_runtime/`: worker-side DataFusion runtime, physical scan integration,
  and result page production.
- `runtime_protocol/`: typed backend/worker control-plane messages.
- `control_transport/`: shared-memory control rings and backend/worker leases.
- `page/pool`, `page/transfer`, `page/issuance`: fixed-page ownership,
  transfer, and issued-frame flow.
- `runtime_metrics`: shared-memory runtime counters and page-slot handoff
  stamps exposed through SQL.
- `page/arrow_layout`, `page/import`, `pg/slot_encoder`, `pg/slot_import`:
  page-backed Arrow layout, PostgreSQL slot encoding, and result projection.
- `pg/df_catalog`, `pg/plan_builder`, `pg/scan_node`, `pg/scan_sql`,
  `pg/slot_scan`: backend-side DataFusion planning and trusted PostgreSQL scan
  SQL execution.
- `join_order`: standalone compact join-order optimizer core. It has no
  DataFusion or PostgreSQL dependency; callers provide filtered relation
  statistics, join edges, and opaque predicate handles.

## Data Path

1. Backend planning resolves PostgreSQL catalog metadata and lowers scan leaves
   to `PgScanNode`/`scan_sql` descriptors. Non-recursive CTEs referenced more
   than once are planned as `PgCteRefNode` reads over a single lowered CTE
   producer so worker execution materializes the CTE once and reuses the owned
   batches. PostgreSQL text-like columns are represented as Arrow `Utf8View` in
   the DataFusion logical schema so scan pages can stay zero-copy for string
   payloads.
2. Worker DataFusion execution opens scans through the runtime protocol.
3. Backend executes trusted scan SQL through `slot_scan`, drains PostgreSQL
   executor slots with a custom `DestReceiver` and explicit fetch row budgets,
   encodes `TupleTableSlot` rows into initialized `arrow_layout` pages with
   `slot_encoder`, and sends issued pages to the worker. Each scan always has a
   leader backend producer. PostgreSQL `max_parallel_workers_per_gather` is a
   query-wide budget for additional dynamic background-worker producers across
   eligible heap scans, capped at `32` and bounded by `max_worker_processes`;
   each producer owns a dedicated scan control slot and writes its own Arrow
   pages into shared memory.
4. Worker imports scan pages as Arrow `RecordBatch` values, runs DataFusion
   operators, writes Arrow result pages, and sends issued frames back.
5. Backend imports result pages with `slot_import` and projects rows into
   PostgreSQL tuple slots.

Page-backed scan batches stay zero-copy through streaming DataFusion operators.
After physical planning, `scan_node` inserts `PageMaterializeExec` only before
operators that can retain input batches beyond immediate streaming, such as
sort/window operators and join build sides. The wrapper copies Arrow arrays into
ordinary allocations at that boundary so shared-memory pages and permits can be
released while preserving zero-copy for simple scans, filters, projections,
limits, and plain aggregates. Multi-use CTE materialization is another owned
boundary: `CteScanExec` deep-copies the producer output once before replaying it
to multiple consumers.

Runtime metrics live in a separate shared-memory region. The runtime does not
wrap control rings for v1 metrics; scan/result page senders stamp page
descriptors, and receivers use those stamps to measure backend-to-worker and
worker-to-backend page handoff latency. Detailed scan timing is opt-in through
`pg_fusion.scan_timing_detail`; it splits backend scan page fill time into
PostgreSQL read time and slot-to-Arrow serialization time.

Dynamic scan workers use CTID block-range chunking as the first parallel scan
strategy. The leader backend scans one heap block range, additional dynamic
background workers scan disjoint ranges, and `worker_runtime` fans all producer
streams into one logical `PgScanExec`. The query-wide worker budget is assigned
before execution starts; if PostgreSQL cannot launch more dynamic workers at
runtime, pg_fusion cancels any partially launched producers for that scan and
continues leader-only for that and later scans. Each producer has its own
ordered issued-page receive stream because producer-local page transfer ids
start at `1`. Relations with dropped attributes or unsupported scan shapes stay
on leader-only portal streaming. Dynamic scan worker jobs carry a resolved
standalone scan descriptor for one PostgreSQL leaf scan rather than the original
user SQL, so worker startup does not depend on backend-local `search_path` or
repeat full DataFusion planning.

## Retired Legacy Stack

The old raw-heap-page stack has been retired from the workspace: `executor`,
`scan`, `storage`, `protocol`, and `common` are no longer active crates. The
active extension crate now lives at `pg/extension`. `lockfree` remains active
because it underpins the new transport/page stack.
