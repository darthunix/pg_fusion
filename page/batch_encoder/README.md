# batch_encoder

`batch_encoder` writes one `RecordBatch` prefix into one caller-initialized
`arrow_layout` block without heap allocations in the write path.

It is intentionally block-scoped:

1. caller builds an `arrow_layout::LayoutPlan`
2. caller initializes a payload buffer with `arrow_layout::init_block`
3. caller constructs `BatchPageEncoder`
4. caller appends as many rows as fit from a `RecordBatch`
5. caller finalizes the block and publishes it through a higher-level page flow

The target format is the same same-host shared-memory `arrow_layout` contract:

- fixed-width numeric values are written in native-endian form
- producer and consumer are expected to run on the same machine and architecture
- this crate does not produce a portable cross-endian or cross-machine format

The crate supports the full current `arrow_layout` surface:

- `Boolean`
- `Int16`
- `Int32`
- `Int64`
- `Float32`
- `Float64`
- `FixedSizeBinary(16)` as `Uuid`
- `Utf8` / `Utf8View`
- `Binary` / `BinaryView`

Plain `Utf8` / `Binary` inputs are lowered into `Utf8View` / `BinaryView`
slots in the target page.

## Benchmarking

This crate exposes a local microbenchmark for the hot write path:

```bash
cargo bench -p batch_encoder
```

The benchmark measures only `BatchPageEncoder::append_batch()` into an already
initialized block. It intentionally excludes `LayoutPlan` construction, payload
allocation, `arrow_layout::init_block()`, and encoder construction so the
reported numbers reflect encode throughput rather than page setup overhead.
