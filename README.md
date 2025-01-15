# pg_fusion

Currently, PostgreSQL operates a row-based engine built on the Volcano
architecture. While this is a good solution for OLTP workloads, it is
very slow for analytical processing. Modern OLAP engines leverage columnar
data representation in memory to enable SIMD optimizations and take advantage
of data locality in CPU caches.

Additionally, PostgreSQL operates on a process-based model, where each process
executes a single thread. The issue with this approach is that these threads
handle not only data processing but also I/O tasks: reading blocks from disk
into a shared memory cache, and communicating with clients over the network.
As a result, the operating system scheduler frequently removes threads from CPU
cores and later reinstates them, which negatively impacts TLB efficiency.

These limitations make historical analytics in PostgreSQL a slow and cumbersome
process. In comparison, DataFusion processes queries an order of magnitude faster.
This leads to the hypothesis that PostgreSQL users need a CPU-efficient engine
capable of significantly accelerating most read-heavy queries on heap tables.
This is the motivation behind the development of `pg_fusion`.

## How to run

After installing `postgres`, you need to set up `rustup`, `cargo-pgrx` to build
the extension.

```
# install rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# install cargo-pgrx
cargo install cargo-pgrx

# configure pgrx
cargo pgrx init --pg17 $(which pg_config)

# append the extension to shared_preload_libraries in ~/.pgrx/data-17/postgresql.conf
echo "shared_preload_libraries = 'pg_fusion'" >> ~/.pgrx/data-17/postgresql.conf

# run cargo-pgrx to build and install the extension
cargo pgrx run
```
