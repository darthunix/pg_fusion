---
id: arch-overview-0001
type: fact
scope: repo
tags: ["architecture", "datafusion", "pgrx", "shared-memory", "ipc", "slot_scan"]
updated_at: "2026-04-24"
importance: 0.8
---

# pg_fusion Architecture Overview

The active PostgreSQL extension is `pg/host_extension` (`pg_fusion_host`). It
hooks PostgreSQL planning/execution and delegates selected query work to an
in-process DataFusion worker runtime through shared-memory control rings and
page-backed Arrow batches.

## Top-Level Runtime Path

- `pg/host_extension/`: pgrx host extension, GUCs, planner/custom-scan hooks,
  shared-memory bootstrap, and background worker entrypoint.
- `pg/backend_service/`: backend execution state, scan coordination, and
  PostgreSQL slot-scan page production.
- `worker_runtime/`: worker-side DataFusion runtime, physical scan integration,
  and result page production.
- `runtime_protocol/`: typed backend/worker control-plane messages.
- `control_transport/`: shared-memory control rings and backend/worker leases.
- `page/pool`, `page/transfer`, `page/issuance`: fixed-page ownership,
  transfer, and issued-frame flow.
- `page/arrow_layout`, `page/import`, `pg/slot_encoder`, `pg/slot_import`:
  page-backed Arrow layout, PostgreSQL slot encoding, and result projection.
- `pg/df_catalog`, `pg/plan_builder`, `pg/scan_node`, `pg/scan_sql`,
  `pg/slot_scan`: backend-side DataFusion planning and trusted PostgreSQL scan
  SQL execution.

## Data Path

1. Backend planning resolves PostgreSQL catalog metadata and lowers scan leaves
   to `PgScanNode`/`scan_sql` descriptors.
2. Worker DataFusion execution opens scans through the runtime protocol.
3. Backend executes trusted scan SQL through `slot_scan`, encodes
   `TupleTableSlot` rows into initialized `arrow_layout` pages with
   `slot_encoder`, and sends issued pages to the worker.
4. Worker imports scan pages as Arrow `RecordBatch` values, runs DataFusion
   operators, writes Arrow result pages, and sends issued frames back.
5. Backend imports result pages with `slot_import` and projects rows into
   PostgreSQL tuple slots.

## Retired Legacy Stack

The old raw-heap-page extension stack has been retired from the workspace:
`pg/extension`, `executor`, `scan`, `storage`, `protocol`, and `common` are no
longer active crates. `lockfree` remains active because it underpins the new
transport/page stack.
