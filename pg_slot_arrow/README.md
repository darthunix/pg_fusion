# pg_slot_arrow

`pg_slot_arrow` writes PostgreSQL `TupleTableSlot` rows directly into an initialized
`page_arrow_layout` block.

It is intentionally narrow:

- producer-side only
- direct page writes into a caller-provided mutable block
- slot-only API over PostgreSQL `TupleTableSlot`
- no dependency on `page_arrow`, `page_transfer`, `storage`, or DataFusion

The main API shape is:

- initialize a raw block externally with `page_arrow_layout`
- create a `PageBatchEncoder` over that mutable block and a PostgreSQL `TupleDesc`
- append rows from `TupleTableSlot`
- finalize the block and return `row_count` plus the written payload length

The encoder does not maintain Rust heap-backed column state. Fixed-size values,
validity bits, `ByteView` slots, and long view payloads are written directly into
the target page as rows are appended.

The current type surface is:

- `bool`
- `int16`, `int32`, `int64`
- `float32`, `float64`
- `uuid`
- `Utf8View`
- `BinaryView`

For text-like columns, the direct path requires PostgreSQL server encoding
`UTF8`. `TupleTableSlot` input may contain toasted/compressed `text` and `bytea`;
those values are detoasted through PostgreSQL and copied directly into the block
without Rust heap staging.

## Typical usage

The crate expects the caller to allocate and initialize the target block with
`page_arrow_layout`, then stream slots into it:

```rust,ignore
use page_arrow_layout::{BlockRef, LayoutPlan, init_block};
use pg_slot_arrow::{AppendStatus, PageBatchEncoder};

let plan = LayoutPlan::from_arrow_schema(&schema, rows_per_page, payload_capacity)?;
let mut payload = vec![0u8; plan.block_size()];
init_block(&mut payload, &plan)?;

let mut encoder = unsafe { PageBatchEncoder::new(tuple_desc, &mut payload)? };
loop {
    match encoder.append_slot(slot)? {
        AppendStatus::Appended => {
            // Slot was written into the current block.
        }
        AppendStatus::Full => {
            let batch = encoder.finish()?;
            // Emit or transport `payload[..batch.payload_len]`, then start a new block.
            break;
        }
    }
}

let batch = encoder.finish()?;
assert!(batch.row_count > 0);
```

In a real producer, `slot` usually comes from a scan or executor node and the
caller creates a fresh block whenever `AppendStatus::Full` is returned.

## API summary

- `PageBatchEncoder::new(tuple_desc, payload)` validates that the initialized
  block matches the PostgreSQL `TupleDesc`.
- `append_slot(slot)` accepts undeformed or partially deformed slots and asks
  PostgreSQL to deform enough attributes when needed.
- `finish()` writes final header state back into the block and returns
  `EncodedBatch { row_count, payload_len }`.

## Constraints

- The block must already be initialized by `page_arrow_layout`.
- The `TupleDesc` and appended slots must match the target layout exactly.
- Dropped PostgreSQL attributes are rejected.
- `Utf8View` columns require a UTF-8 PostgreSQL server encoding.
- `AppendStatus::Full` means the current row did not fit and must be retried on a
  fresh block.
