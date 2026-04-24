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
- `testing/pg_test/` - pgrx integration tests for the active runtime path and
  page/slot pipeline.
- `lockfree/` - shared-memory lock-free primitives used by the transport/page
  stack.

## Build & Test

Workspace targets Rust 1.89.

```sh
cargo check --workspace
cargo test --workspace
```

Lint/format:

```sh
cargo fmt --all
cargo clippy --all-targets --features "pg17, pg_test" --no-default-features
```

## pgrx Quickstart (PG 17)

Install and initialize pgrx:

```sh
cargo install cargo-pgrx
cargo pgrx init --pg17 $(which pg_config)
```

Enable the active extension in the dev cluster:

```sh
echo "shared_preload_libraries = 'pg_fusion'" >> ~/.pgrx/data-17/postgresql.conf
```

Build only the host extension crate:

```sh
cargo build -p pg_fusion
```

Run pgrx tests for the shared test extension:

```sh
cargo pgrx test pg17 -p pg_test
```

For the Postgres-side page pipeline benchmark under `pg_test`, see
[`testing/pg_test/README.md`](testing/pg_test/README.md).

## Developer Guidelines

- Rust 2021; keep changes small and focused; avoid panics in extension paths.
- Before PR: `cargo fmt`, `cargo clippy -D warnings`, and
  `cargo test --workspace`.
- Commit style: `area: concise change`.

## Memory Bank For Agents

We maintain a human-readable memory bank under `ai/memory`.

- `ai/memory/index.md` - how to read the bank
- `ai/memory/architecture.md` - overview
- `ai/memory/components/` - component facts
- `ai/memory/decisions/` - ADR-lite decisions
- `ai/memory/invariants.md` - project invariants

After behavior or architecture changes, update the relevant files under
`ai/memory` so future agents have accurate context.
