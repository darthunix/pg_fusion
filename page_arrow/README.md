# page_arrow

`page_arrow` imports a `page_transfer::ReceivedPage` as a zero-copy Apache Arrow `RecordBatch`.

It is intentionally narrow:

- import only
- external schema
- plain `RecordBatch` output
- strict zero-copy semantics with no copy fallback

The page payload contract for v1 is:

- `page_transfer::MessageKind == page_arrow::ARROW_IPC_BATCH_KIND`
- `page_transfer` flags must be `0`
- page payload starts with a fixed 12-byte little-endian `BatchPageHeader`
- bytes after that header contain one Arrow IPC encapsulated `RecordBatch` message plus body
- producer must write IPC data with alignment `16`, metadata version `V5`, and no compression

Ordinary imported batches keep the page alive through Arrow buffer ownership. When the last Arrow reference drops, the page is returned to the underlying `page_pool`.

Zero-buffer batches such as empty-schema or `Null`-only payloads decode as owned Arrow structures and may release the page before `import()` returns.

Keeping a page-backed batch alive does not pin `page_transfer::PageRx`; later page accepts can proceed while earlier imported batches are still in scope.
