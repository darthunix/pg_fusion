# pg_fusion

pg_fusion is a PostgreSQL extension that delegates selected query execution to an
in-process Apache DataFusion worker runtime. The active extension crate is
`pg/extension` (`pg_fusion`).

The current runtime boundary keeps PostgreSQL responsible for catalog lookup,
planning integration, snapshots, table access, and slot materialization. Backend
scan rows are encoded into page-backed Arrow layout blocks and streamed to the
worker over shared-memory transport; worker results return as Arrow pages and are
projected back into PostgreSQL tuple slots.

## Architecture

- `pg/extension/` - pgrx host extension, GUCs, planner/custom-scan hooks,
  shared-memory bootstrap, background worker entrypoint.
- `pg/backend_service/` - backend-side execution orchestration and scan page
  production.
- `worker_runtime/` - DataFusion worker runtime and transport-backed scan/result
  handling.
- `runtime_protocol/` - typed control-plane protocol for backend/worker runtime
  messages.
- `control_transport/` - shared-memory control rings and backend/worker leases.
- `page/pool/`, `page/transfer/`, `page/issuance/` - fixed-page ownership and
  page handoff infrastructure.
- `page/arrow_layout/`, `page/import/`, `pg/slot_encoder/`, `pg/slot_import/` -
  zero-copy Arrow page layout, PostgreSQL slot encoding, and result projection.
- `pg/plan_builder/`, `pg/df_catalog/`, `pg/scan_node/`, `pg/scan_sql/`,
  `pg/slot_scan/` - backend-side SQL planning and PostgreSQL scan execution.
- `pg/test/` - pgrx integration tests for the active runtime path and
  page/slot pipeline.
- `lockfree/` - shared-memory lock-free primitives used by the transport/page
  stack.

## Build & Test

Workspace targets Rust 1.89.

```sh
cargo check --workspace
cargo test --workspace --exclude backend_service
```

`backend_service` is intentionally excluded from standalone `cargo test`:
through `slot_scan` it references PostgreSQL SPI symbols that only exist inside
a PostgreSQL backend. Its regression coverage lives in the pgrx test crate.

Build only the PostgreSQL extension crate:

```sh
cargo build -p pg_fusion
```

Format and lint:

```sh
cargo fmt --all
cargo clippy --all-targets --features "pg17, pg_test" --no-default-features
```

## pgrx Setup (PG 17)

Install and initialize pgrx:

```sh
cargo install cargo-pgrx
cargo pgrx init --pg17 $(which pg_config)
```

Configure `postgresql.conf` as described below before using the extension.

Run pgrx tests:

```sh
cargo pgrx test pg17 -p pg_fusion --features pg_test
cargo pgrx test pg17 -p pg_test
```

For the Postgres-side page pipeline benchmark under `pg_test`, see
[`pg/test/README.md`](pg/test/README.md).

## PostgreSQL Configuration

`pg_fusion` must be loaded by PostgreSQL at postmaster start because it
registers shared memory, hooks, and a background worker. For a pgrx PG 17 dev
cluster:

```sh
cat >> ~/.pgrx/data-17/postgresql.conf <<'EOF'
shared_preload_libraries = 'pg_fusion'

# Worker/runtime diagnostics.
pg_fusion.worker_threads = 0
pg_fusion.log_path = '/tmp/pg_fusion.log'
pg_fusion.worker_log_filter = 'warn'

# Primary backend <-> worker control transport.
pg_fusion.control_slot_count = 64
pg_fusion.control_backend_to_worker_capacity = 8192
pg_fusion.control_worker_to_backend_capacity = 8192

# Dedicated scan control transport.
pg_fusion.scan_slot_count = 64
pg_fusion.scan_backend_to_worker_capacity = 256
pg_fusion.scan_worker_to_backend_capacity = 44

# Shared page pool and issued-page flow.
pg_fusion.page_size = 65536
pg_fusion.page_count = 256

# Backend PostgreSQL cursor scan streaming.
pg_fusion.scan_fetch_batch_rows = 1024
pg_fusion.estimator_initial_tail_bytes_per_row = 64
EOF
```

All settings above are `Postmaster` GUCs and require a cluster restart after
changes. `pg_fusion.worker_threads = 0` lets the worker runtime choose its
thread count automatically. The scan ring capacities must stay at least `256`
backend-to-worker and `44` worker-to-backend.
The issued-page permit pool is sized from `pg_fusion.page_count`, so each
shared page can have one outstanding issued handoff.

After configuring the pgrx cluster, install the extension and open `psql`:

```sh
cargo pgrx run pg17 -p pg_fusion
```

For a non-pgrx cluster, install the extension artifacts with that cluster's
`pg_config`, add `shared_preload_libraries = 'pg_fusion'`, restart PostgreSQL,
and create the extension object in the target database:

```sh
cargo pgrx install -p pg_fusion --release --pg-config /path/to/pg_config
```

```sql
CREATE EXTENSION IF NOT EXISTS pg_fusion;
```

Useful PostgreSQL planner/runtime knobs for scan experiments can be placed in
`postgresql.conf` or set per session with ordinary `SET` commands:

```conf
# Cursor planning bias used by PostgreSQL itself. Lower values favor fast-start
# plans; higher values favor total scan cost.
cursor_tuple_fraction = 0.1

# Normal PostgreSQL memory and parallel scan planning knobs. These are not
# pg_fusion settings, but they affect the PostgreSQL cursor plans that
# pg_fusion streams through slot_scan.
work_mem = '64MB'
max_parallel_workers_per_gather = 2
min_parallel_table_scan_size = '8MB'
parallel_setup_cost = 1000
parallel_tuple_cost = 0.1
```

Treat these as workload-specific tuning inputs. `slot_scan` executes trusted
PostgreSQL scan SQL through a read-only cursor, so PostgreSQL's ordinary planner
settings still influence whether it chooses seq scan, index scan, bitmap scan,
or a parallel-capable plan.

## Runtime Use

Enable pg_fusion per session or transaction:

```sql
SET pg_fusion.enable = on;
```

For scoped experiments:

```sql
BEGIN;
SET LOCAL pg_fusion.enable = on;
SELECT count(*) FROM my_table WHERE id > 100;
COMMIT;
```

Backend diagnostics can also be enabled without restarting:

```sql
SET pg_fusion.backend_log_level = 1; -- 0=off, 1=basic, 2=trace
```

`pg_fusion.scan_fetch_batch_rows` controls how many rows the backend asks
PostgreSQL to deliver from the scan portal per direct `PortalRunFetch()` call.
Rows are encoded from executor `TupleTableSlot`s directly into Arrow pages; no
SPI tuptable batch is materialized in the hot path. The default is `1024`; `0`
is not a valid configured value and the internal scan path normalizes only
defensive runtime inputs to at least one row. Page boundaries are handled by
the fetch row budget, not by pausing the PostgreSQL receiver mid-fetch. Scans
with variable-width transport columns use one-row drains so an overflowing row
can be retried on the next Arrow page without losing the cursor position.

DataFusion fetch/limit hints are lowered into `slot_scan` as a PostgreSQL
fast-start planning hint plus a local soft row cap. They are not documented as
an exact global SQL `LIMIT` guarantee; exact SQL limit semantics must remain in
the logical query plan above the scan path.

## Developer Guidelines

- Rust 2021; keep changes small and focused; avoid panics in extension paths.
- Before PR: `cargo fmt`, `cargo clippy -D warnings`, and
  `cargo test --workspace`.
- Commit style: `area: concise change`.

## Agent Context

We maintain a small living context under `ai/`.

- `ai/README.md` - how to read and maintain the context
- `ai/architecture.md` - current architecture
- `ai/invariants.md` - project invariants
- `ai/gotchas.md` - practical pitfalls
- `ai/components/` - component notes

After behavior or architecture changes, update the relevant files under
`ai/` so future agents have accurate context.
