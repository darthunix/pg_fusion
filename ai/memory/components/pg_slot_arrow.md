---
id: comp-pg-slot-arrow-0001
type: fact
scope: pg_slot_arrow
tags: ["arrow", "ipc", "postgres", "slot", "page_transfer"]
updated_at: "2026-03-31"
importance: 0.72
---

# Component: pg_slot_arrow

- `pg_slot_arrow` is a standalone workspace crate that encodes already-deformed PostgreSQL rows into a `page_arrow`-compatible Arrow IPC payload.
- It does not depend on `page_arrow`, `page_transfer`, `storage`, or DataFusion.
- Public boundary in v1:
  - compile a `BatchPlan` from `TupleDesc + SchemaRef`
  - append rows through a generic `RowDatumAccess` trait or directly from `TupleTableSlot`
  - write into a caller-provided mutable payload slice and return `payload_len`
- `TupleTableSlot` integration details:
  - `append_slot` reads `tts_values` / `tts_isnull`
  - calls `slot_getsomeattrs_int` if the slot is not yet fully deformed
  - detoasts projected text-like and binary varlena values through `pg_detoast_datum_packed`
  - keeps detoasted allocations only for the duration of one row append via `SlotScratch`
  - packed `varlena` parsing depends on PostgreSQL `varatt.h` header macros (`VARATT_IS_1B`, `VARATT_IS_1B_E`, `VARATT_IS_4B_C`, `VARSIZE_1B`, `VARSIZE_4B`)
  - when upgrading to a new PostgreSQL major, re-check those `varatt.h` macros against the Rust parser before trusting the existing bit layout assumptions
- Supported v1 type surface:
  - `bool`
  - `int2/int4/int8`
  - `float4/float8`
  - `text/varchar/bpchar/name -> Arrow Utf8`
  - `bytea -> Arrow Binary`
  - `uuid -> Arrow FixedSizeBinary(16)`
- Output contract:
  - 12-byte `BatchPageHeader`
  - one encapsulated Arrow IPC `RecordBatch` message plus body
  - alignment `16`
  - metadata version `V5`
  - no compression
- Current implementation uses staged Arrow builders plus exact payload-size accounting and writes the final IPC bytes into the provided payload slice at `finish()`.
