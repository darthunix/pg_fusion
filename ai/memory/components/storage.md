---
id: comp-storage-0001
type: fact
scope: storage
tags: ["heap", "decoder", "scalarvalue", "varlena", "arrow"]
updated_at: "2025-11-29"
importance: 0.9
---

# Component: Storage (Heap)

## Functions

- Read heap page; iterate LP_NORMAL by offsets.
- Zero‑allocation decoding: fixed‑width, date/time/timestamp/interval, inline varlena text.
- Projections: `decode_tuple_project(page_hdr, tuple, attrs, indices)`.

## Constraints & Gotchas

- Projected compressed/external varlena → error; if not projected — skip safely.
- Observe alignment when advancing across attributes (attalign).

## Tests

- `storage/pg_test`: pgrx‑based tests against a live PG; cover iteration and decoding.
