# Repository Guidelines

## Project Structure & Module Organization
- `executor/`: Core execution engine (DataFusion integration, IPC, buffers, protocol). Library crate.
- `postgres/`: pgrx extension glue (planner hook, background worker, IPC). Builds a `cdylib`.
- `doc/adr/`: Architectural notes and decisions (if any).
- `target/`: Build artifacts (ignored).

## Build, Test, and Development Commands
- Build workspace: `cargo build --workspace`
- Build only executor: `cargo build -p executor`
- Set up pgrx (once): `cargo pgrx init --pg17 $(which pg_config)`
- Enable extension for dev cluster: `echo "shared_preload_libraries = 'pg_fusion'" >> ~/.pgrx/data-17/postgresql.conf`
- Run dev Postgres with extension: `cargo pgrx run`
- Test executor crate: `cargo test -p executor`
- Test extension (PG): `cargo pgrx test` (supports PG 17 via `pg17` feature)

## Coding Style & Naming Conventions
- Language: Rust 2021; prefer safe Rust; isolate `unsafe` behind minimal, well-documented APIs.
- Formatting: `rustfmt` defaults. Run `cargo fmt --all` before pushing.
- Linting: `cargo clippy --workspace --all-targets -D warnings` for CI-quality checks.
- Naming: modules/files `snake_case`; types/traits `PascalCase`; functions/vars `snake_case`.
- Errors: use `thiserror` for error types and `anyhow` at boundaries.
- Logging: use `tracing`; enable with `RUST_LOG=info` (e.g., `RUST_LOG=pg_fusion=debug cargo pgrx run`).

## Testing Guidelines
- Unit tests: colocate with modules using `#[cfg(test)]` in both crates; name test fns descriptively.
- Extension tests: use `pgrx-tests` with `#[pg_test]`; run via `cargo pgrx test`.
- Add focused tests for planner hooks, IPC paths, and executor protocol; avoid flaky timing-based tests.

## Commit & Pull Request Guidelines
- Commits: imperative mood, small, and scoped; prefix scope when helpful, e.g., `executor: ...`, `postgres: ...`.
- PRs: include a clear summary, motivation, benchmarks or repro SQL if performance-related, and linked issues.
- Checks: ensure `cargo fmt`, `cargo clippy`, and tests pass locally before requesting review.

## Security & Configuration Tips
- Supported PG: 17 (`pg17` feature). Re-init pgrx when upgrading Postgres.
- Dev cluster path: `~/.pgrx/data-17`. Keep `shared_preload_libraries` configured for `pg_fusion`.
- Avoid long-running or blocking work on Postgres backends; prefer background workers/IPC already provided.

