# Testing

[Documentation home](index.md)

`pg_fusion` has two kinds of tests:

1. standalone Rust tests for PostgreSQL-free crates;
2. pgrx tests that run inside a live PostgreSQL backend.

## Format And Lint

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --features "pg17, pg_test" --no-default-features
```

## Standalone Workspace Tests

Some crates reference PostgreSQL backend symbols and must be excluded from
ordinary `cargo test`.

```sh
cargo test --workspace \
  --exclude backend_service \
  --exclude df_catalog \
  --exclude pg_fusion \
  --exclude pg_statistics \
  --exclude pg_test \
  --exclude plan_builder \
  --exclude row_estimator_seed \
  --exclude slot_encoder \
  --exclude slot_import \
  --exclude slot_scan
```

## pgrx Tests

```sh
cargo pgrx test pg17 -p pg_fusion --features pg_test
cargo pgrx test pg17 -p pg_test
```

These tests require the pgrx PostgreSQL 17 environment described in
[Development](development.md#development-environment).

The `pg_fusion` extension smoke tests are run outside `cargo llvm-cov` in CI.
Coverage flags retain PostgreSQL extension symbols that the Linux pgrx test
harness normally lets the linker discard.

## Spill Tests

Spill tests require finite worker memory. The smoke test is opt-in:

```sh
PG_FUSION_SPILL_PG_TEST=1 \
  cargo pgrx test pg17 -p pg_fusion --features pg_test pg_fusion_spill_metrics_smoke
```

## Slot Deformation Benchmark

The pgrx-backed `pg/test` crate includes a manual benchmark that compares
PostgreSQL slot deformation with pg_fusion slot-to-Arrow page encoding.

```sh
cargo pgrx run pg17 -p pg_test --release --features pg_test
```

Inside `psql`:

```sql
DROP EXTENSION IF EXISTS pg_test CASCADE;
CREATE EXTENSION pg_test;

SELECT jsonb_pretty(tests.slot_deform_vs_page_encode_bench('fixed', 100000, 3));
SELECT jsonb_pretty(tests.slot_deform_vs_page_encode_bench('mixed', 100000, 3));
```
