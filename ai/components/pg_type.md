---
id: component-pg-type-0001
type: fact
scope: component
tags: ["postgres", "arrow", "datafusion", "types", "transport"]
updated_at: "2026-05-28"
importance: 0.8
---

# pg_type

`pg/type` is the shared PostgreSQL type-policy crate for pg_fusion. It is the
source of truth for the supported PostgreSQL OID surface, typmod/collation
checks, PostgreSQL-to-Arrow transport mapping, page-layout `TypeTag` mapping,
Arrow transport schema normalization, typed literal metadata, and DataFusion
`ScalarValue` construction for typed NULLs and frontend constants.

PostgreSQL `date` uses `DateADT` days from 2000-01-01, while Arrow `Date32`
uses days from 1970-01-01. `pg/type` owns the conversion helpers; scan encoding,
result import, runtime filter keys, and frontend constants must use those
helpers instead of passing raw date integers through. PostgreSQL `DATE
'-infinity'` and `DATE 'infinity'` are sentinel `DateADT` values with no Arrow
`Date32` equivalent, so the helpers reject them explicitly.

The crate intentionally does not read or write PostgreSQL `Datum` values.
PostgreSQL-bound crates such as `slot_encoder`, `slot_import`, and
`pg_frontend` adapters keep ownership of memory contexts, TOAST/detoast,
generic varlena pointer handling, fixed-size `NameData`, interval struct
access, and tuple-slot projection.

`pg/type::numeric` owns the PostgreSQL-runtime-free conversion between finite
PostgreSQL `numeric` varlena layout and Arrow Decimal128. Backend scan encoding
decodes detoasted PostgreSQL numeric bytes into Decimal128, and result slot
projection encodes Decimal128 back into PostgreSQL numeric varlena bytes using
stack-backed scratch buffers before the PostgreSQL-bound `slot_import` layer
copies them into the per-tuple memory context. Decimal result projection must
not round-trip through decimal strings or `numeric_in`; the remaining allocation
on this path is the required PostgreSQL `palloc` for the result `Datum`. Numeric
varlena length headers must use PostgreSQL's endian-specific 4-byte varlena
packing; numeric payload fields remain native-endian PostgreSQL layout.

`timestamp` and `timestamptz` currently share the same Arrow transport type
(`Timestamp(Microsecond, None)`), so callers that render SQL or expose
PostgreSQL result metadata must keep original PostgreSQL type identity when it
matters.
