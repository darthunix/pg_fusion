# pg_fusion

pg_fusion is a PostgreSQL extension that delegates query execution to an external Apache DataFusion runtime. The extension hooks PG planning/execution, streams heap pages via shared memory to the runtime, and streams back results as wire‑friendly MinimalTuple frames.

Why: PostgreSQL’s Volcano (row‑at‑a‑time) engine is great for OLTP but slow for OLAP. DataFusion provides a modern, vectorized execution engine in Rust. pg_fusion integrates it in‑process via a background worker and shared memory IPC.

## Architecture (high level)

- Extension (pgrx) sits in the backend process and drives Parse/Bind/Optimize/Translate/Begin/Exec/End.
- Executor (runtime) runs DataFusion; `PgTableProvider → PgScanExec → PgScanStream` scans heap pages from shared memory and produces Arrow RecordBatches.
- Protocol defines control/data packets and a compact wire tuple format with explicit alignment.

See: `ai/memory/architecture.md` and component notes in `ai/memory/components/`.

## Repository layout

- `postgres/` — pgrx extension (hooks, IPC, TupleTableSlot fill)
- `executor/` — DataFusion runtime (planning/execution, SHM access, result encoding)
- `protocol/` — shared packets and wire formats
- `storage/` — heap page reader + zero‑allocation tuple decoder to DataFusion `ScalarValue`
- `common/` — shared errors/types

## Build & test

Workspace targets Rust 1.89.

Basics:

```
cargo check --workspace
cargo test --workspace
```

Lint/format:

```
cargo fmt --all
cargo clippy --all-targets --features "pg17, pg_test" --no-default-features
```

## pgrx quickstart (PG 17)

Install and initialize pgrx:

```
# install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# install cargo-pgrx
cargo install cargo-pgrx

# configure pgrx
cargo pgrx init --pg17 $(which pg_config)

# enable extension in the dev cluster
echo "shared_preload_libraries = 'pg_fusion'" >> ~/.pgrx/data-17/postgresql.conf

# run the dev cluster and build/install the extension
cargo pgrx run
```

Extension‑specific commands:

```
# build only the extension crate
cargo build -p pg_fusion

# run pgrx tests for storage’s pg_test
cargo pgrx test pg17 -p pg_test
```

## Developer guidelines

- Rust 2021; keep changes small and focused; surface structured errors (no panics in extension paths).
- Before PR: `cargo fmt`, `cargo clippy -D warnings`, `cargo test --workspace`.
- Commit style: `area: concise change` (e.g., `executor: fix buffer rollback`).

## Memory bank for agents (RAG)

We maintain a human‑readable “memory bank” for agents and humans under `/ai/memory` (Markdown + YAML frontmatter). Start with:

- `/ai/memory/index.md` — how to read the bank
- `/ai/memory/architecture.md` — overview
- `/ai/memory/components/` — component facts
- `/ai/memory/decisions/` — ADR‑lite decisions
- `/ai/memory/invariants.md` — project invariants

Agent workflow requirement: after you implement or change behavior, update the relevant files under `/ai/memory` (components, decisions, invariants, architecture) so future agents have accurate context.
