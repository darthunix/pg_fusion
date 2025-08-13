# Repository Guidelines

## Project Structure & Module Organization
- server/: Core query engine, IPC, and protocol. Pure Rust library with unit tests.
- client/: PostgreSQL extension (pgrx) shim exposing the engine to Postgres.
- postgres/: Integration tests for the extension using pgrx; used in CI, runnable locally.
- doc/adr/: Architectural Decision Records and template.

## Build, Test, and Development Commands
- Build engine: `cargo build -p server`
- Unit tests (engine): `cargo test -p server`
- Format & lint: `cargo fmt --all -- --check` and `cargo clippy --all-targets --features "pg17, pg_test" --no-default-features`
- pgrx setup (once): `cargo install cargo-pgrx && cargo pgrx init --pg17 $(which pg_config)`
- Run extension locally: `cargo pgrx run` (see README for `shared_preload_libraries`)
- Integration tests (from postgres/): `cargo build --features "pg17, pg_test" --no-default-features && cargo pgrx test pg17 --no-default-features`

## Coding Style & Naming Conventions
- Rust 2021; rustfmt defaults (4‑space indent). Run `cargo fmt` before committing.
- Use snake_case for modules/functions, CamelCase for types, SCREAMING_SNAKE_CASE for consts.
- Prefer explicit types, small modules under `server/src/` by domain (e.g., `protocol/`, `ipc/`).

## Testing Guidelines
- Unit tests live alongside code with `#[cfg(test)]`; name tests by unit under test.
- Extension tests use `pgrx-tests` and run via `cargo pgrx test pg17` in `postgres/`.
- Coverage (CI uses cargo-llvm-cov): locally you can `cargo install cargo-llvm-cov && cargo llvm-cov report` in `postgres/`.

## Commit & Pull Request Guidelines
- Conventional Commits: `feat:`, `fix:`, `chore:`, optional scope (e.g., `chore(worker): ...`).
- PRs: clear description, link issues, list test steps, note perf/compat impacts. Ensure fmt/clippy/tests pass.

## Security & Configuration Tips
- macOS builds rely on `.cargo/config.toml` for dynamic Postgres symbols; don’t remove.
- Postgres: set `shared_preload_libraries = 'pg_fusion'` for local runs (see README). Align with PG 17 by default.
