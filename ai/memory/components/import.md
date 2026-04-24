---
id: comp-page-arrow-0001
type: fact
scope: import
tags: ["arrow", "layout", "transfer", "shared-memory", "zero-copy"]
updated_at: "2026-04-01"
importance: 0.72
---

# Component: import

- `import` is a standalone workspace crate that imports a `transfer::ReceivedPage` as a plain Arrow `RecordBatch` without copying the batch payload.
- Scope is intentionally narrow in v1:
  - import only
  - schema comes from the caller
  - strict zero-copy semantics
  - no dictionary support
  - no schema registry or schema-in-page
- Wire contract:
  - outer `transfer::MessageKind` must be `import::ARROW_LAYOUT_BATCH_KIND`
  - outer `transfer` flags must be `0`
  - page payload is exactly one validated `arrow_layout` block
  - external Arrow schema must match the on-page layout exactly
  - string/binary columns must use `Utf8View` / `BinaryView`
- Ownership model:
  - importer consumes `ReceivedPage`
  - Arrow buffers are created with `arrow_buffer::Buffer::from_custom_allocation`
  - the custom allocation owner retains the `ReceivedPage`
  - retaining that `ReceivedPage` keeps the detached page leased, but no longer keeps `PageRx` busy for later accepts
  - ordinary page-backed batches release the page back to `pool` only after the last Arrow buffer reference drops
  - zero-buffer batches such as empty-schema payloads decode as owned Arrow structures and may release the page before `import()` returns
- Current status:
  - crate is implemented and tested in isolation
  - it imports `arrow_layout` pages produced by `slot_encoder` and by the active
    host/runtime scan path
