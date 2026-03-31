---
id: comp-page-arrow-layout-0001
type: fact
scope: layout
tags: ["arrow", "layout", "shared-memory", "zero-copy", "view-types"]
updated_at: "2026-03-31"
importance: 0.7
---

# Component: layout

- `layout` is a standalone workspace crate that defines the shared binary contract for the next zero-copy Arrow page format.
- It is intentionally layout-only in its first change:
  - `#[repr(C)]` raw page structs
  - constants and type tags
  - Arrow-schema-to-layout planning helpers
  - layout validators
  - `ByteView` inline and out-of-line helpers
- The format is a front-and-tail page layout:
  - `BlockHeader` and `ColumnDesc[]` live at the front
  - the front region reserves fixed-size buffers for all rows up to `max_rows`
  - fixed-width values, validity bitmaps, and `ByteView` slots live in that front region
  - long `Utf8View` and `BinaryView` payloads live in one shared tail arena that grows toward smaller offsets
  - all long views use `buffer_index = 0`
- V1 type surface is intentionally narrow:
  - `bool`
  - `int16`
  - `int32`
  - `int64`
  - `float32`
  - `float64`
  - `uuid`
  - `Utf8View`
  - `BinaryView`
- Current status:
  - crate is implemented and tested in isolation
  - `page/import` consumes this layout directly
  - `pg/slot_encoder` writes PostgreSQL slot rows directly into this layout
