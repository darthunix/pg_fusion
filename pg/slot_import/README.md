# slot_import

`slot_import` is the page-backed inverse of `slot_encoder`.

It takes one `transfer::ReceivedPage` whose payload is an `arrow_layout` block,
imports it through `page/import`, and projects rows into a caller-owned
PostgreSQL `TTSOpsVirtual` slot.

The crate is intentionally narrow:

- it accepts only the current reversible `slot_encoder` type surface
- it builds on `import::ArrowPageDecoder`, not on direct raw block parsing
- it does not create slots; the caller creates and reuses a virtual slot
- it does not own page transport or pool orchestration outside one imported page
- it allows only one active cursor per projector; `open_page()` takes `&mut self`

## Supported mappings

- `Boolean -> BOOLOID`
- `Int16 -> INT2OID`
- `Int32 -> INT4OID`
- `Int64 -> INT8OID`
- `Float32 -> FLOAT4OID`
- `Float64 -> FLOAT8OID`
- `FixedSizeBinary(16) -> UUIDOID`
- `Utf8View -> TEXTOID | VARCHAROID | BPCHAROID | NAMEOID`
- `BinaryView -> BYTEAOID`

Text-like mappings are only supported on PostgreSQL databases with `UTF8`
server encoding. `varchar(n)` and `char(n)` values are normalized through
PostgreSQL's typmod-aware input functions, so overlength `varchar` inputs still
raise the usual error and `bpchar` values are padded/truncated per PostgreSQL
rules.

## Lifetime contract

`PageSlotCursor::next_into_slot()` may place page-backed by-reference datums into
the target slot. In v1 that zero-copy path is only used for `UUIDOID`.

The returned tuple remains valid until:

- the next `next_into_slot()` call on the same cursor, or
- the cursor is dropped

`ArrowSlotProjector` requires exclusive access while a cursor is open. That
lets Rust prevent multiple live cursors from sharing the same per-tuple memory
context.

On the first call after the last row, the cursor:

1. clears the target slot
2. resets the per-tuple memory context
3. drops the imported page-backed arrays
4. returns `None`

That first `None` call is therefore the point where the page is released back to
the pool if no other Arrow owners remain.

## Basic flow

```rust,no_run
use std::sync::Arc;

use arrow_schema::Schema;
use pgrx::{pg_sys, PgMemoryContexts};
use slot_import::ArrowSlotProjector;

# fn example(schema: Arc<Schema>, page: transfer::ReceivedPage, tupdesc: pg_sys::TupleDesc) -> anyhow::Result<()> {
let mut per_tuple_memory = PgMemoryContexts::new("slot_import_per_tuple");
let mut projector = unsafe {
    ArrowSlotProjector::new(schema, tupdesc, per_tuple_memory.value())?
};

let mut cursor = projector.open_page(page)?;
let slot = unsafe { pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsVirtual) };

while let Some(slot) = unsafe { cursor.next_into_slot(slot)? } {
    // consume one projected virtual tuple
    let _ = slot;
}

unsafe { pg_sys::ExecDropSingleTupleTableSlot(slot) };
# Ok(())
# }
```

## Memory behavior

- fixed-width byval values are written directly into `tts_values`
- `UUIDOID` points directly into page-backed Arrow storage
- text-like values and `bytea` are copied into the supplied per-tuple
  `MemoryContext`
- `next_into_slot()` resets that per-tuple context before building the next row

The crate assumes the same-host, native-endian `arrow_layout` contract used by
`slot_encoder`, `batch_encoder`, and `import`.
