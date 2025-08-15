# Repository Guidelines

## Project Structure & Module Organization
- `postgres/`: pgrx-based PostgreSQL extension (`pg_fusion`), planner/worker hooks and embedding. Binary: `src/bin/pgrx_embed.rs`.
- `executor/`: DataFusion-powered execution runtime (buffers, layout, server, SQL helpers).
- `protocol/`: IPC/message definitions shared between extension and runtime (bind/parse/columns/etc.).
- `common/`: Shared types and errors (e.g., `FusionError`).
- Workspace is managed by the root `Cargo.toml`.

## Build, Test, and Development Commands
- Build (all crates): `cargo build --workspace` (use `--release` for optimized artifacts).
- Check types fast: `cargo check --workspace`.
- Unit/integration tests: `cargo test --workspace`.
- pgrx toolchain:
  - Install: `cargo install cargo-pgrx` then `cargo pgrx init --pg17 $(which pg_config)`.
  - Run extension in a dev cluster: `cargo pgrx run`.
  - pgrx tests (PG 17): `cargo pgrx test pg17`.
- Build only the extension crate: `cargo build -p pg_fusion`.

## Coding Style & Naming Conventions
- Rust 2021 edition. Prefer idiomatic Rust: small modules, explicit `use`, and clear error paths.
- Formatting: run `cargo fmt` before pushing.
- Linting: run `cargo clippy -D warnings` and address issues.
- Naming: modules `snake_case`, types `CamelCase`, constants `SCREAMING_SNAKE_CASE`.

## Testing Guidelines
- Place Rust tests in `tests/` or `mod tests { ... }` within modules.
- For extension-level behavior, add pgrx tests and run with `cargo pgrx test pg17`.
- Keep tests deterministic; prefer table-driven cases and cover error paths.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise scope first line (e.g., `executor: fix buffer rollback`), followed by context where helpful.
- PRs: include motivation, summary of changes, testing notes (commands used), and any config/doc updates.
- Ensure `cargo fmt`, `cargo clippy -D warnings`, and `cargo test --workspace` pass locally before requesting review.

## Security & Configuration Tips
- PostgreSQL: add `shared_preload_libraries = 'pg_fusion'` in the target cluster.
- GUC: toggle runtime via `pg_fusion.enable`.
- Avoid panics in extension code paths; return structured errors (`common::FusionError`).

## Architecture Overview
- The extension hooks Postgres planner/worker, communicates with the runtime via the `protocol` crate, and executes queries through the `executor` backed by Apache DataFusion.

