# `pg_fusion_host`

Temporary thin-host scaffold for the new `pg_fusion` extension cutover.

This crate is where the new pgrx host integration will move:

- GUC registration
- shared-memory bootstrap for the new transport/page stack
- planner/custom-scan registration
- background worker entrypoints
- thin glue to `backend_service` and `worker_runtime`

Current scope:

- define the new GUC surface
- normalize and validate host-side configuration
- reserve the final `pg_fusion.*` GUC prefix

Planned next slices:

- shared-memory bootstrap for the new transport/page pools
- backend-local `EXPLAIN`
- custom scan callbacks on top of `backend_service`
- worker bootstrap on top of `worker_runtime`

`bind`/prepared-statement parameters are intentionally deferred in the first
cutover. See the explicit `TODO` in `src/planner.rs`.
