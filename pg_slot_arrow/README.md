# pg_slot_arrow

`pg_slot_arrow` writes already-deformed PostgreSQL rows directly into an initialized
`page_arrow_layout` block.

It is intentionally narrow:

- producer-side only
- direct page writes into a caller-provided mutable block
- `TupleTableSlot` convenience path plus a generic one-pass row interface
- no dependency on `page_arrow`, `page_transfer`, `storage`, or DataFusion

The main API shape is:

- initialize a raw block externally with `page_arrow_layout`
- create a `PageBatchEncoder` over that mutable block and a PostgreSQL `TupleDesc`
- append rows from a generic row source or directly from `TupleTableSlot`
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
