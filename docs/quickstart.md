# Quick Start

[Documentation home](index.md)

This guide builds `pg_fusion` and runs one local query in a pgrx PostgreSQL 17
development cluster.

## Prerequisites

- Rust 1.89 or newer.
- PostgreSQL 17 development headers and `pg_config`.
- `cargo-pgrx`.

For contributor setup, see [Development](development.md).

## Install pgrx

`cargo-pgrx` and the `pgrx` crates must use the same exact patch version.
Check the selected `pgrx` version:

```sh
cargo tree -p pgrx --depth 0
```

Then install the matching `cargo-pgrx` binary:

```sh
PGRX_VERSION=$(cargo tree -p pgrx --depth 0 | sed -n 's/^pgrx v//p')
cargo install cargo-pgrx --version "$PGRX_VERSION" --locked --force
cargo pgrx --version
cargo pgrx init --pg17 $(which pg_config)
```

Use the full path to the PostgreSQL 17 `pg_config` if multiple PostgreSQL
versions are installed.

## Build

```sh
cargo build --release -p pg_fusion
```

Use a release build for local experiments. Debug builds add enough overhead to
make pg_fusion and PostgreSQL comparisons misleading.

## Configure PostgreSQL

`pg_fusion` must be preloaded because it registers hooks, shared memory, and a
background worker.

For a 16 GiB development machine, add:

```conf
shared_preload_libraries = 'pg_fusion'

pg_fusion.worker_threads = 0
pg_fusion.max_fusion_tasks = 0
pg_fusion.log_path = '/tmp/pg_fusion.log'
pg_fusion.worker_log_filter = 'warn'
pg_fusion.worker_memory_limit_mb = 2048
pg_fusion.worker_spill_directory = '/tmp/pg_fusion_spill'

pg_fusion.control_slot_count = 128
pg_fusion.control_backend_to_worker_capacity = 65536
pg_fusion.control_worker_to_backend_capacity = 65536

pg_fusion.scan_slot_count = 256
pg_fusion.scan_backend_to_worker_capacity = 4096
pg_fusion.scan_worker_to_backend_capacity = 4096

pg_fusion.page_size = 262144
pg_fusion.page_count = 1024

pg_fusion.scan_fetch_batch_rows = 4096
pg_fusion.scan_batch_channel_capacity = 128
pg_fusion.scan_idle_poll_interval_us = 50
pg_fusion.estimator_initial_tail_bytes_per_row = 64

pg_fusion.join_reordering = on

pg_fusion.runtime_filter_enable = on
pg_fusion.runtime_filter_count = 128
pg_fusion.runtime_filter_bits = 4194304
pg_fusion.runtime_filter_hashes = 4
```

This profile reserves about 256 MiB for the shared page pool and about 64 MiB
for runtime Bloom filters, plus control-ring overhead. It also gives the
DataFusion worker a 2 GiB memory pool and enables worker-owned spill under
`/tmp/pg_fusion_spill`.

For smaller or heavily loaded machines, reduce `pg_fusion.page_count` first.

Restart PostgreSQL after changing postmaster-level settings.

## Start psql

```sh
cargo pgrx run pg17 -p pg_fusion --release
```

Then create the extension:

```sql
CREATE EXTENSION IF NOT EXISTS pg_fusion;
```

## Run A Query

```sql
CREATE TABLE t AS
SELECT i AS id, i % 10 AS group_id, i::double precision AS value
FROM generate_series(1, 1000000) AS i;

ANALYZE t;

SET pg_fusion.enable = on;

SELECT count(*), avg(value)
FROM t
WHERE group_id >= 0;
```

## Try A Larger Aggregate Query

This example returns one row after PostgreSQL scan rows have crossed into Arrow
pages and DataFusion has computed the aggregate.

```sql
DROP TABLE IF EXISTS t;
CREATE TABLE t (a int PRIMARY KEY, b int);

INSERT INTO t
SELECT g, g % 1000
FROM generate_series(1, 1000000) g;

ANALYZE t;

SET pg_fusion.enable = on;

SELECT count(*)
FROM t
WHERE b >= 0;
```

Expected result:

```text
  count
------------
 1000000
```

Treat timing as workload-specific; compare on your machine with
`pg_fusion.enable` off and on.

`COPY (SELECT ...) TO STDOUT` can use the same pg_fusion path when the nested
`SELECT` is eligible:

```sql
COPY (
  SELECT count(*)
  FROM t
  WHERE b >= 0
) TO STDOUT WITH (FORMAT csv);
```

## Inspect The Plan

```sql
EXPLAIN
SELECT count(*), avg(value)
FROM t
WHERE group_id >= 0;
```

Look for `Custom Scan (PgFusionScan)` and PostgreSQL scan leaves. The scan
leaves show the SQL that PostgreSQL executes before rows are encoded into Arrow
pages.

## Compare With PostgreSQL

```sql
SET pg_fusion.enable = off;
EXPLAIN ANALYZE
SELECT count(*), avg(value)
FROM t
WHERE group_id >= 0;

SET pg_fusion.enable = on;
EXPLAIN ANALYZE
SELECT count(*), avg(value)
FROM t
WHERE group_id >= 0;
```

If pg_fusion is slower, check whether the query sends many rows or columns to
the worker. [Metrics](metrics.md) shows how to inspect scan encoding, transport,
worker execution, and result transfer.

## Next Steps

- Read [Architecture](architecture.md) for the runtime and resource model.
- Read [Query support](query-support.md) before trying application queries.
- Read [Configuration](configuration.md) before changing shared-memory or worker
  limits.
