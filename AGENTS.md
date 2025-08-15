# Repository Guidelines

## Project Structure & Module Organization
- `server/` (crate `executor`): Core query engine, IPC, and protocol; pure Rust with unit tests.
- `client/`: PostgreSQL extension (pgrx) that exposes the engine to Postgres.
- `postgres/`: Integration tests for the extension (run with pgrx); used in CI and locally.
- `doc/adr/`: Architectural Decision Records and template.

## Build, Test, and Development Commands
- Build engine: `cargo build -p executor` — compiles the core library.
- Unit tests (engine): `cargo test -p executor` — runs server crate tests.
- Format & lint: `cargo fmt --all -- --check` and `cargo clippy --all-targets --features "pg17, pg_test" --no-default-features`.
- pgrx setup (once): `cargo install cargo-pgrx && cargo pgrx init --pg17 $(which pg_config)`.
- Run extension locally: `cargo pgrx run` (ensure `shared_preload_libraries = 'pg_fusion'`).
- Integration tests (from `postgres/`): `cargo build --features "pg17, pg_test" --no-default-features && cargo pgrx test pg17 --no-default-features`.

## Coding Style & Naming Conventions
- Rust 2021; rustfmt defaults (4‑space indent). Run `cargo fmt` before committing.
- Naming: `snake_case` for modules/functions, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for consts.
- Keep modules small by domain under `server/src/` (e.g., `protocol/`, `ipc/`). Prefer explicit types.

## Testing Guidelines
- Unit tests live next to code behind `#[cfg(test)]`; name tests by the unit under test.
- Extension tests use `pgrx-tests`; run with `cargo pgrx test pg17` from `postgres/`.
- Optional coverage: `cargo install cargo-llvm-cov && cargo llvm-cov report` (run in `postgres/`).

## Commit & Pull Request Guidelines
- Conventional Commits: `feat:`, `fix:`, `chore:`, with optional scope (e.g., `feat(protocol): ...`).
- PRs: concise description, link issues, list test steps, call out perf/compat impacts. Ensure fmt/clippy/tests pass.

## Security & Configuration Tips
- macOS builds rely on `.cargo/config.toml` for dynamic Postgres symbols — keep it intact.
- Default Postgres target is PG 17. Set `shared_preload_libraries = 'pg_fusion'` for local runs.
