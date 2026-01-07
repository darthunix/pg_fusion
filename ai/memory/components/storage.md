---
id: comp-storage-0001
type: fact
scope: storage
tags: ["heap", "tuple", "decoder", "varlena"]
updated_at: "2026-01-07"
importance: 0.75
---

# Component: Storage

- Heap page: iterators over line pointers; zero‑copy tuple slicing; sorted by offset variant for reuse.
- Decoder: `decode_tuple_project(page_hdr, tuple, attrs, indices: Iterator)`; supports fixed‑width, date/time/timestamp/interval, inline varlena text.
- Varlenas: projected compressed/external → error; non‑projected safely skipped.
