---
id: comp-pg-slot-arrow-0001
type: fact
scope: slot_encoder
tags: ["arrow", "layout", "postgres", "slot", "transfer"]
updated_at: "2026-03-31"
importance: 0.72
---

# Component: slot_encoder

- `slot_encoder` is a standalone workspace crate that writes PostgreSQL `TupleTableSlot` rows directly into an initialized `layout` block.
- It does not depend on `import`, `transfer`, `storage`, or DataFusion.
- Public boundary in v1:
  - caller plans and initializes a block externally through `layout`
  - `PageBatchEncoder::new(tuple_desc, payload)` validates the block against the PostgreSQL `TupleDesc`
  - `append_slot(slot)` appends one PostgreSQL row and returns `AppendStatus::{Appended, Full}`
  - `finish()` writes final header state and returns `{ row_count, payload_len }`
- Hot-path details:
  - encoder writes fixed-width values, validity bits, `ByteView` slots, and long view payload bytes directly into the target block
  - local `row_count` and tail cursor are staged in the encoder and flushed back to the block header at `finish()`
  - `append_slot` uses a slot-specific fast path over `tts_values` / `tts_isnull`
  - it calls `slot_getsomeattrs_int` when the slot is not yet sufficiently deformed
  - projected text-like and binary varlena values may be detoasted through `pg_detoast_datum_packed`
  - packed `varlena` parsing depends on PostgreSQL `varatt.h` header macros (`VARATT_IS_1B`, `VARATT_IS_1B_E`, `VARATT_IS_4B_C`, `VARSIZE_1B`, `VARSIZE_4B`)
  - when upgrading to a new PostgreSQL major, re-check those `varatt.h` macros against the Rust parser before trusting the existing bit layout assumptions
- Supported v1 type surface:
  - `bool`
  - `int2/int4/int8`
  - `float4/float8`
  - `text/varchar/bpchar/name -> Arrow Utf8View`
  - `bytea -> Arrow BinaryView`
  - `uuid -> Arrow FixedSizeBinary(16)`
- Output contract:
  - caller-provided payload already contains one initialized `layout` block
  - `payload_len` currently equals the block size published by `layout`
  - `AppendStatus::Full` means the current row did not fit and must be retried on a fresh block
