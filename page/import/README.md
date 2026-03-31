# import

`import` imports a `transfer::ReceivedPage` as a zero-copy Apache Arrow `RecordBatch`.

It is intentionally narrow:

- import only
- external schema
- plain `RecordBatch` output
- zero-copy page-backed data buffers with no staging/copy fallback

The page payload contract is:

- `transfer::MessageKind == import::ARROW_LAYOUT_BATCH_KIND`
- `transfer` flags must be `0`
- the payload is one validated `layout` block
- the external Arrow schema must exactly match the on-page layout surface
- string/binary columns must use `Utf8View` / `BinaryView`

Ordinary imported batches keep the page alive through Arrow buffer ownership. When the last Arrow reference drops, the page is returned to the underlying `pool`.

The crate uses only atomic shared ownership for this lifetime management. There is no internal mutex or other blocking synchronization primitive in the import path.

Empty-schema batches decode as owned Arrow structures and may release the page before `import()` returns.

Keeping a page-backed batch alive does not pin `transfer::PageRx`; later page accepts can proceed while earlier imported batches are still in scope.
