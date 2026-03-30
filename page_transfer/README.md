# page_transfer

`page_transfer` is a standalone workspace crate for transferring ownership of shared-memory pages managed by `page_pool`.

It solves one narrow problem: writing one message into one page, handing that page to another process through a small sans-IO control frame, and returning the page to the pool when the receiver is done. It does not allocate shared memory, implement transport I/O, or define retries/acks.

## What it is

- A sender/receiver layer over `page_pool`.
- A fixed-size transport control frame encoded with RMP.
- An in-page message header stored at the front of each transferred page.
- Owned wrappers that work in both synchronous code and async runtimes.

## What it is not

- Not a socket, ring-buffer, or async runtime abstraction.
- Not a retry, credit, or acknowledgment protocol.
- Not a shared-memory allocator.
- Not a batching protocol; v1 is one message per page.

## Ownership model

Sender flow:

```text
PageTx::begin -> PageWriter::write -> PageWriter::finish -> OutboundPage
OutboundPage::frame -> encode/send -> OutboundPage::mark_sent
```

Structured producer flow:

```text
PageTx::begin -> PageWriter::payload_mut -> producer fills bytes
-> PageWriter::finish_with_payload_len -> OutboundPage
```

Receiver flow:

```text
carrier bytes -> FrameDecoder -> OwnedFrame -> PageRx::accept -> ReceivedPage
ReceivedPage::payload -> ReceivedPage::release
```

Important behavior:

- One `PageTx` allows at most one active writer or unsent outbound page.
- One `PageRx` serializes `accept()` calls, but a live `ReceivedPage` does not block later accepts.
- Dropping `OutboundPage` before `mark_sent()` rolls the page back locally.
- If a detached page is accepted but its in-page header is malformed, an internal RAII guard releases it automatically so malformed input does not leak pool capacity.
- `Write` is a convenience API; direct payload-slice access is the low-level producer primitive.

## Wire and page format

- Transport frames contain only control information.
- A `Page` frame carries `transfer_id` and `PageDescriptor`.
- A `Close` frame terminates the stream.
- Payload bytes never travel in the transport frame; they already live in shared memory.
- Each transferred page begins with a fixed-size RMP message header containing:
  - `kind`
  - `flags`
  - `payload_len`

Because `payload_len` is encoded as `u32`, `page_transfer` rejects page configurations whose payload capacity exceeds `u32::MAX`.

## Sync and async use

`page_transfer` stays runtime-agnostic:

- no `tokio` dependency
- no `async fn` API
- owned `PageWriter`, `OutboundPage`, and `ReceivedPage`

That means PostgreSQL backend code can use it synchronously, while an async worker can hold the owned wrappers across `.await` points and perform transport I/O outside the crate.

## Minimal example

```rust
use page_pool::{PagePool, PagePoolConfig};
use page_transfer::{encode_frame, FrameDecoder, PageRx, PageTx, ReceiveEvent};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::io::Write;
use std::ptr::NonNull;

let config = PagePoolConfig::new(4096, 1).unwrap();
let region = PagePool::layout(config).unwrap();
let layout = Layout::from_size_align(region.size, region.align).unwrap();
let base = NonNull::new(unsafe { alloc_zeroed(layout) }).unwrap();

let pool = unsafe { PagePool::init_in_place(base, region.size, config) }.unwrap();
let tx = PageTx::new(pool);
let rx = PageRx::new(pool);

let mut writer = tx.begin(1, 0).unwrap();
writer.payload_mut()[..5].copy_from_slice(b"hello");
let outbound = writer.finish_with_payload_len(5).unwrap();
let frame = encode_frame(outbound.frame()).unwrap();
outbound.mark_sent();

let mut decoder = FrameDecoder::new();
let frame = decoder.push(&frame).next().unwrap().unwrap();
let page = match rx.accept(frame).unwrap() {
    ReceiveEvent::Page(page) => page,
    ReceiveEvent::Closed => unreachable!(),
};
assert_eq!(page.payload(), b"hello");
page.release().unwrap();

drop(pool);
unsafe { dealloc(base.as_ptr(), layout) };
```

## Testing

Useful commands:

```sh
cargo test -p page_transfer
cargo clippy -p page_transfer --tests -- -D warnings
cargo doc -p page_transfer --no-deps
```
