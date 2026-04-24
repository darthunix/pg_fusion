# Repository Guidelines

## Project Structure & Module Organization
- `pg/extension/`: active pgrx host extension (`pg_fusion`), GUCs,
  planner/custom-scan hooks, shared-memory bootstrap, and background worker.
- `pg/backend_service/`: backend-side execution state, scan orchestration, and
  PostgreSQL slot-scan page production.
- `worker_runtime/`: DataFusion worker runtime, physical scan integration, and
  page-backed result production.
- `runtime_protocol/`: typed backend/worker control-plane messages.
- `control_transport/`: shared-memory control rings, backend/worker leases.
- `lockfree/`: shared-memory lock-free primitives used by transport/page crates.
- `page/pool/`: fixed-page shared-memory ownership pool.
- `page/transfer/`: page handoff protocol built on `page/pool`.
- `page/issuance/`: issued-page framing and permit flow.
- `page/arrow_layout/`: shared zero-copy Arrow page layout contract.
- `page/import/`: zero-copy Arrow `RecordBatch` import over `page/transfer`.
- `pg/slot_encoder/`: producer-side encoder from PostgreSQL `TupleTableSlot`
  rows into page-backed Arrow layout blocks.
- `pg/slot_import/`: backend result projection from Arrow pages into PostgreSQL
  tuple slots.
- `pg/df_catalog/`, `pg/plan_builder/`, `pg/scan_node/`, `pg/scan_sql/`,
  `pg/slot_scan/`: backend-side DataFusion planning and PostgreSQL scan SQL
  execution support.
- `testing/pg_test/`: pgrx-based integration tests for the active runtime path
  and page/slot pipeline.
- Workspace is managed by the root `Cargo.toml`.

## Build, Test, and Development Commands
- Build all crates: `cargo build --workspace` (use `--release` for optimized
  artifacts).
- Check types fast: `cargo check --workspace`.
- Unit/integration tests: `cargo test --workspace`.
- pgrx toolchain:
  - Install: `cargo install cargo-pgrx` then
    `cargo pgrx init --pg17 $(which pg_config)`.
  - Build only the host extension crate: `cargo build -p pg_fusion`.
  - pgrx tests (PG 17): `cargo pgrx test pg17 -p pg_test`.

## Coding Style & Naming Conventions
- Rust 2021 edition. Prefer idiomatic Rust: small modules, explicit `use`, and
  clear error paths.
- Formatting: run `cargo fmt` before pushing.
- Linting: run `cargo clippy -D warnings` and address issues.
- Naming: modules `snake_case`, types `CamelCase`, constants
  `SCREAMING_SNAKE_CASE`.

## Testing Guidelines
- Place Rust tests in `tests/` or `mod tests { ... }` within modules.
- For extension-level behavior, add pgrx tests in `testing/pg_test` where
  possible and run with `cargo pgrx test pg17 -p pg_test`.
- Keep tests deterministic; prefer table-driven cases and cover error paths.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise scope first line (for example,
  `backend_service: fix scan cleanup`), followed by context where helpful.
- PRs: include motivation, summary of changes, testing notes, and any config/doc
  updates.
- Ensure `cargo fmt`, `cargo clippy -D warnings`, and `cargo test --workspace`
  pass locally before requesting review.

## Security & Configuration Tips
- PostgreSQL: add `shared_preload_libraries = 'pg_fusion'` in the target
  cluster.
- GUC: toggle runtime via `pg_fusion.enable`.
- Avoid panics in extension code paths; return structured errors.

## Architecture Overview
- The host extension hooks PostgreSQL planning/execution and communicates with a
  background worker via `runtime_protocol`, `control_transport`, and page-backed
  Arrow buffers. PostgreSQL remains responsible for physical table access through
  the `slot_scan` path; the worker consumes Arrow batches through DataFusion and
  returns Arrow result pages.
