# Repository Guidelines

## Agent Context
- Read `ai/README.md` before architecture analysis or behavior changes.
- Treat `ai/` as the current project context: architecture, invariants,
  gotchas, and component notes live there.
- After behavior or architecture changes, update the relevant file under `ai/`
  in the same change.

## Build, Test, and Development Commands
- Build all crates: `cargo build --workspace` (use `--release` for optimized
  artifacts).
- Check types fast: `cargo check --workspace`.
- Standalone unit/integration tests exclude PostgreSQL-bound crates:
  `cargo test --workspace --exclude backend_service --exclude df_catalog
  --exclude pg_fusion --exclude pg_test --exclude plan_builder --exclude
  row_estimator_seed --exclude slot_encoder --exclude slot_import --exclude
  slot_scan`.
- PostgreSQL-bound crate tests run through pgrx because they reference
  PostgreSQL backend symbols.
- Extension crate: `cargo build -p pg_fusion`.
- pgrx setup: `cargo install cargo-pgrx` then
  `cargo pgrx init --pg17 $(which pg_config)`.
- pgrx tests (PG 17): `cargo pgrx test pg17 -p pg_fusion --features pg_test`
  and `cargo pgrx test pg17 -p pg_test`.
- Runtime setup and GUC details are documented in `README.md`.

## Coding Style
- Rust 2021 edition. Prefer small modules, explicit `use`, and clear error
  paths.
- Format with `cargo fmt --all`.
- Lint with `cargo clippy -D warnings` when practical.
- Naming: modules `snake_case`, types `CamelCase`, constants
  `SCREAMING_SNAKE_CASE`.

## Testing Guidelines
- Place Rust tests in `tests/` or `mod tests { ... }` within modules.
- For extension-level behavior, add pgrx tests in `pg/test` where
  possible.
- Keep tests deterministic; prefer table-driven cases and cover error paths.

## Commit & PR Guidelines
- Commits: imperative mood, concise scope first line, for example
  `backend_service: fix scan cleanup`.
- PRs: include motivation, summary of changes, testing notes, and any
  config/doc updates.
- Before review, aim to pass `cargo fmt`, `cargo clippy -D warnings`, the
  standalone test command above, and the relevant pgrx tests.

## Safety Notes
- PostgreSQL must preload the extension with
  `shared_preload_libraries = 'pg_fusion'`.
- Toggle the runtime with `pg_fusion.enable`.
- Avoid panics in extension code paths; return structured errors or controlled
  PostgreSQL errors.
