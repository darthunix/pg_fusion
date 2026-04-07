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

## Producer Loop

`BatchPageEncoder::append_batch()` has two distinct full-page outcomes:

- `Ok(AppendResult { rows_written: n, full: true })` with `n > 0`: the block
  accepted a non-empty prefix and the caller should publish it, then continue
  from `start_row + n`
- `Ok(AppendResult { rows_written: 0, full: true })` on an empty page: the
  caller may have overestimated `LayoutPlan::max_rows()` for a variable-width
  workload and can retry the same first row on a fresh page with a smaller
  `max_rows`

`EncodeError::RowTooLargeForPage` is reserved for the terminal case where the
first row still does not fit on an empty page with `max_rows = 1`.

## Example

```rust,ignore
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_layout::{ColumnSpec, LayoutPlan, TypeTag, init_block};
use arrow_schema::{DataType, Field, Schema};
use batch_encoder::{AppendResult, BatchPageEncoder, EncodeError};

let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
let block_size = 160;
let batch = RecordBatch::try_new(
    Arc::clone(&schema),
    vec![Arc::new(StringArray::from(vec![Some("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")]))
        as ArrayRef],
) ?;

let plan = LayoutPlan::new(&specs, 2, block_size)?;
let mut payload = vec![0u8; plan.block_size() as usize];
init_block(&mut payload, &plan)?;
let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload)?;

match encoder.append_batch(&batch, 0)? {
    AppendResult {
        rows_written: 0,
        full: true,
    } => {
        // The first row did not fit with max_rows = 2.
        // Retry it on a fresh page with a smaller LayoutPlan::max_rows().
    }
    AppendResult {
        rows_written,
        full: true,
    } => {
        // Publish the non-empty page, then continue from start_row + rows_written.
        let _encoded = encoder.finish()?;
    }
    AppendResult {
        rows_written,
        full: false,
    } => {
        // All remaining rows fit in this block.
        let _encoded = encoder.finish()?;
        assert_eq!(rows_written, batch.num_rows());
    }
}

let single_row_plan = LayoutPlan::new(&specs, 1, block_size)?;
let mut retry_payload = vec![0u8; single_row_plan.block_size() as usize];
init_block(&mut retry_payload, &single_row_plan)?;
let mut retry_encoder =
    BatchPageEncoder::new(schema.as_ref(), &single_row_plan, &mut retry_payload)?;

match retry_encoder.append_batch(&batch, 0) {
    Ok(AppendResult {
        rows_written: 1,
        full: false,
    }) => {}
    Err(EncodeError::RowTooLargeForPage { .. }) => {
        // Even max_rows = 1 cannot fit this row in the page size.
    }
    other => unreachable!("unexpected append result: {other:?}"),
}
```

## Benchmarking

This crate exposes a local microbenchmark for the hot write path:

```bash
cargo bench -p batch_encoder
```

The benchmark measures only `BatchPageEncoder::append_batch()` into an already
initialized block. It intentionally excludes `LayoutPlan` construction, payload
allocation, `arrow_layout::init_block()`, and encoder construction so the
reported numbers reflect encode throughput rather than page setup overhead.
