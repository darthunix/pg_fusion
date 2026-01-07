---
id: dec-wire-tuple-0001
type: decision
scope: protocol/executor/backend
tags: ["result", "wire-tuple", "minimal-tuple", "alignment", "varlena"]
updated_at: "2026-01-07"
importance: 0.85
---

# Decision: Result Rows as Protocol Wire Tuples

Context: We need a PG‑friendly row format across the executor↔backend boundary that avoids per‑type mapping churn and excessive allocations.

- Chosen: executor encodes rows as `protocol::tuple::encode_wire_tuple` — a protocol‑level wire format (header + optional null bitmap + aligned data area), not a literal `HeapTuple` header.
- Backend behavior: read frame → decode header/bitmap → reconstruct `Datum[]/isnull[]` using `attlen/attalign/attbyval` from `PgAttrWire` → form `MinimalTuple` via `heap_form_minimal_tuple` → `ExecStoreMinimalTuple`.

Why:
- Matches `TupleTableSlot` machinery; sidesteps per‑OID msgpack mapping and reduces copies.
- Stable across types; varlena handled uniformly without TOAST.

Format notes:
- Null bitmap: LSB‑first; bit set means NULL.
- Alignment: each attribute aligned to `attalign` (1/2/4/8) in the data area.
- Byval: 1/2/4/8‑byte little‑endian (host‑endian assumption; executor and backend co‑locate).
- Varlena: `u32 LE` length prefix + raw bytes (no compression/TOAST on wire).

Future:
- Option to switch to direct `HeapTuple` memcpy if benefits outweigh complexity.
- Expand type coverage (dates/time/interval already supported in storage decode; extend encoder/assembler accordingly).
