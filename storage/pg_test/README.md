# `pg_test`

`pg_test` is the pgrx-backed integration test crate for `storage` and the
standalone page pipeline crates. It contains:

- ordinary `#[pg_test]` integration tests run inside a live PostgreSQL backend
- a manual deformation benchmark that compares:
  `slot_getallattrs()` vs `pg_slot_arrow::PageBatchEncoder::append_slot()`
- a full-pipeline correctness test for:
  `TupleTableSlot -> pg_slot_arrow -> page_transfer -> page_arrow -> RecordBatch`

## Running the slot deformation benchmark

Use a release build and enable the test-only SQL surface:

```bash
cargo pgrx run pg17 -p pg_test --release --features pg_test
```

If `cargo pgrx run` reports that it is reusing an existing `pg_test` database,
recreate the extension objects once in `psql` so the `tests` schema and new SQL
functions are installed into the database:

```sql
DROP EXTENSION IF EXISTS pg_test CASCADE;
CREATE EXTENSION pg_test;
```

Then run the benchmark:

```sql
SELECT jsonb_pretty(tests.slot_deform_vs_page_encode_bench('fixed', 100000, 3));
SELECT jsonb_pretty(tests.slot_deform_vs_page_encode_bench('mixed', 100000, 3));
SELECT jsonb_pretty(tests.slot_deform_vs_page_encode_bench('fixed', 100000, 3, 4096, 1048556));
SELECT jsonb_pretty(tests.slot_deform_vs_page_encode_bench('mixed', 100000, 3, 8192, 1048556));
```

For flamegraph-friendly split runs, prepare the source table once and then
profile only the path you care about:

```sql
SELECT tests.slot_deform_bench_prepare('mixed', 100000);
SELECT jsonb_pretty(tests.slot_deform_baseline_bench('mixed', 3));
SELECT jsonb_pretty(tests.slot_deform_arrow_bench('mixed', 3, 8192, 1048556));
```

## Benchmark output

The JSON result contains:

- `profile`: `fixed` or `mixed`
- `rows`: source table row count per scan
- `iterations`: number of full rescans
- `rows_per_page`: current `pg_slot_arrow` batch/page target
- `payload_capacity_bytes`: writable payload budget passed to `page_arrow_layout`
- `baseline`: PostgreSQL slot deformation metrics via `slot_getallattrs()`
- `arrow`: `pg_slot_arrow::append_slot()` metrics, produced page count, and `block_size_bytes`
- `ratio.arrow_vs_baseline`: Arrow rows/sec divided by PostgreSQL rows/sec
- `ratio.slowdown_vs_baseline`: PostgreSQL rows/sec divided by Arrow rows/sec

## Practical notes

- The first run is usually slower because of warmup effects.
- `fixed` isolates fixed-width deformation cost more clearly.
- `mixed` includes `Utf8View` and `BinaryView`, so it exercises varlen packing too.
- The fourth argument is the target row cap per page; the fifth is writable payload
  capacity, not the full shared-memory page size.
- For larger shared-memory pages, raise both `rows_per_page` and
  `payload_capacity_bytes`; otherwise the benchmark will still stop at the row cap.
- `slot_deform_bench_prepare(...)` must run in the same session before
  `slot_deform_baseline_bench(...)` or `slot_deform_arrow_bench(...)`, because
  the source table is a `pg_temp` relation.
- For flamegraphs, profile `slot_deform_baseline_bench(...)` and
  `slot_deform_arrow_bench(...)` directly. The combined function still includes
  source-table setup and both measurement branches.
- This benchmark does not include `page_transfer` or `page_arrow` import.
- The full pipeline still has correctness coverage in `page_arrow_pipeline_roundtrip_inside_postgres`.
