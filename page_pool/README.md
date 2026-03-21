# page_pool

`page_pool` is a standalone workspace crate for fixed-size page ownership inside a caller-provided shared-memory region.

It solves one narrow problem: page allocation and ownership transfer between processes. It does not allocate shared memory, map it, transport descriptors, or define when page contents become visible to readers.

## What it is

- A fixed-page pool over an existing shared-memory region.
- A process-local `PagePool` handle that can `init`, `attach`, `acquire`, `detach`, `release`, and report metrics.
- A pool-identity and generation-checked `PageDescriptor` for cross-process ownership handoff.
- A shared metrics block for pool gauges and counters.

## What it is not

- Not a ring buffer or message transport.
- Not a publish/read/ack protocol.
- Not a refcounted shared-borrow system.
- Not a `mmap`, `memfd`, or Postgres shared-memory allocation wrapper.

The intended layering is:

1. Some outer layer allocates or maps a shared region.
2. `page_pool` manages which process owns which page.
3. Some outer transport layer moves `PageDescriptor` values between processes.
4. Some outer protocol defines when page bytes may be read or written.

## Ownership model

Each page moves through these states:

- `Free`: the page is available in the freelist.
- `Attached`: an in-process `PageLease` owns the page and may access its bytes safely.
- `Detached`: ownership has been converted into a `PageDescriptor` and may be handed to another process.
- `Exhausted`: the page reached the maximum generation and is permanently retired.

State flow:

```text
Free -> try_acquire -> Attached
Attached -> drop -> Free
Attached -> into_descriptor -> Detached
Detached -> release -> Free
Detached/Attached at max generation -> release/drop -> Exhausted
```

`PageLease::descriptor()` only returns a snapshot of the current lease identity. It is not itself a releasable ownership transfer. To hand ownership off safely, call `PageLease::into_descriptor()`. Detached APIs also validate the descriptor's pool identity, so a descriptor from another pool or an earlier pool lifetime is rejected.

## Memory layout and process model

The shared region contains:

- a fixed header with magic/version/config
- shared metrics
- per-page atomic state words
- a freelist backed by the `lockfree` crate
- page bytes

The region is address-independent. Each process attaches its own local `PagePool` handle to the same bytes.

Minimal process sequence:

```text
Process A: init/attach -> try_acquire -> fill page -> into_descriptor -> send descriptor
Process B: attach -> page_bytes/page_bytes_mut -> consume page -> release
```

## Metrics

`PagePool::snapshot()` returns:

- Gauges: `page_size`, `page_count`, `free_pages`, `leased_pages`, `exhausted_pages`, `high_watermark_pages`
- Counters: `acquire_ok`, `acquire_empty`, `release_ok`, `release_exhausted`, `release_bad_page`, `release_stale_generation`, `release_still_attached`, `release_not_leased`, `acquire_retry_loops`, `release_retry_loops`

These metrics live in shared memory, so all attached processes observe the same pool-wide totals.

## Generation exhaustion

Generations do not wrap. When a page reaches the maximum representable generation, releasing it retires the page into `Exhausted` instead of returning it to the freelist. This prevents a stale descriptor from becoming valid again after long uptime.

In practice, with a 62-bit generation counter, exhaustion is effectively unreachable for normal deployments. It is still modeled explicitly to keep the ownership contract correct.

## Minimal example

```rust
use page_pool::{PagePool, PagePoolConfig};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

let config = PagePoolConfig::new(4096, 8).unwrap();
let region = PagePool::layout(config).unwrap();
let layout = Layout::from_size_align(region.size, region.align).unwrap();
let base = NonNull::new(unsafe { alloc_zeroed(layout) }).unwrap();

let pool = unsafe { PagePool::init_in_place(base, region.size, config) }.unwrap();

let mut lease = pool.try_acquire().unwrap();
lease.bytes_mut()[0] = 1;

let desc = lease.into_descriptor().unwrap();
assert_eq!(unsafe { pool.page_bytes(desc).unwrap() }[0], 1);
pool.release(desc).unwrap();

drop(pool);
unsafe { dealloc(base.as_ptr(), layout) };
```

## Testing

The crate is intended to stay independently testable from the rest of the workspace.

Useful commands:

```sh
cargo test -p page_pool
cargo clippy -p page_pool --tests -- -D warnings
cargo doc -p page_pool --no-deps
```
