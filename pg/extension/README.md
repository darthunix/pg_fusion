# `pg_fusion`

Pgrx extension crate for the `pg_fusion` runtime path.

This crate owns the new pgrx host integration:

- GUC registration
- shared-memory bootstrap for the new transport/page stack
- planner/custom-scan registration
- background worker entrypoints
- thin glue to `backend_service` and `worker_runtime`

Current scope:

- backend-local `EXPLAIN`
- custom scan callbacks on top of `backend_service`
- worker bootstrap on top of `worker_runtime`
- page-backed worker result production and backend result ingress
- pgrx smoke tests for simple `SELECT`, `EXPLAIN`, and heap-backed `SELECT`

## Current limitation

`pg_fusion` queries must currently run as top-level SQL over the normal
Postgres client protocol.

Running `pg_fusion` queries from SPI-owned execution contexts is not supported
yet. In practice this means:

- no `Spi::run` / `Spi::get_one` wrappers around the query under test
- no PL/pgSQL or other function-internal SQL invocation paths for `pg_fusion`
  queries

The smoke tests intentionally use `pgrx_tests::client()` plus `simple_query()`
to execute queries over the regular Postgres protocol for this reason.

`bind`/prepared-statement parameters are intentionally deferred in the first
cutover. See the explicit `TODO` in `src/planner.rs`.
