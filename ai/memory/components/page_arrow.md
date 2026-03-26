---
id: comp-page-arrow-0001
type: fact
scope: page_arrow
tags: ["arrow", "ipc", "page_transfer", "shared-memory", "zero-copy"]
updated_at: "2026-03-26"
importance: 0.72
---

# Component: page_arrow

- `page_arrow` is a standalone workspace crate that imports a `page_transfer::ReceivedPage` as a plain Arrow `RecordBatch` without copying the batch payload.
- Scope is intentionally narrow in v1:
  - import only
  - schema comes from the caller
  - strict zero-copy semantics
  - no dictionary support
  - no compressed IPC payloads
  - no schema registry or schema-in-page
- Wire contract:
  - outer `page_transfer::MessageKind` must be `page_arrow::ARROW_IPC_BATCH_KIND`
  - outer `page_transfer` flags must be `0`
  - page payload starts with a fixed 12-byte little-endian batch header
  - bytes after that header contain exactly one Arrow IPC encapsulated `RecordBatch` message plus aligned body; trailing bytes are rejected
  - producer contract is Arrow IPC metadata version `V5`, alignment `16`, and no compression
- Ownership model:
  - importer consumes `ReceivedPage`
  - Arrow buffers are created with `arrow_buffer::Buffer::from_custom_allocation`
  - the custom allocation owner retains the `ReceivedPage`
  - ordinary page-backed batches release the page back to `page_pool` only after the last Arrow buffer reference drops
  - zero-buffer batches such as empty-schema or `Null`-only payloads decode as owned Arrow structures and may release the page before `import()` returns
- Current status:
  - crate is implemented and tested in isolation
  - executor/scan runtime paths do not use it yet
