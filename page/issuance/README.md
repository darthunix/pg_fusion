# issuance

`issuance` adds one narrow capability on top of `transfer`: a shared-memory
permit budget that limits how many page-backed handoffs may remain live at once.

The crate is intentionally simple:

- one global permit pool
- non-blocking `try_acquire` only
- no fairness policy
- no wakeups, retries, or scheduler

Its main purpose is preventing deadlock-by-exhaustion when page-backed batches
may stay alive across process boundaries. A permit is acquired before
`transfer::PageTx::begin()` and is only returned after the corresponding
received page is finally dropped or explicitly released.

## What it provides

- `IssuancePool`: shared-memory permit pool
- `PermitLease`: RAII permit handle
- `IssuedTx` / `IssuedWriter` / `IssuedOutboundPage`
- `IssuedRx` / `IssuedReceivedPage`
- wrapper frame encoding with `permit_id`
- per-frame issuance-pool identity checks

## Contract

- The crate guarantees only overall progress when permits are eventually
  released.
- It does **not** guarantee fairness between producers.
- Long-lived consumers are expected to copy data out and drop page-backed
  owners quickly.

## Basic flow

```rust
use issuance::{encode_issued_frame, IssueEvent, IssuedFrameDecoder, IssuedRx, IssuedTx};

let issued_tx = IssuedTx::new(page_tx, permits);
let issued_rx = IssuedRx::new(page_rx, permits);

let mut writer = issued_tx.begin(import::ARROW_LAYOUT_BATCH_KIND, 0)?;
// fill writer.payload_mut()
let outbound = writer.finish()?;
let wire = encode_issued_frame(outbound.frame())?;
outbound.mark_sent();

let mut decoder = IssuedFrameDecoder::new();
let frame = decoder.push(&wire).next().transpose()?.unwrap();
let page = match issued_rx.accept(&frame)? {
    IssueEvent::Page(page) => page,
    IssueEvent::Closed => unreachable!(),
};

assert_eq!(page.kind(), import::ARROW_LAYOUT_BATCH_KIND);
# Ok::<(), Box<dyn std::error::Error>>(())
```

`IssuedRx::accept()` borrows the decoded frame. If the underlying
`transfer::PageRx` returns a retryable error such as `Busy` or
`UnexpectedTransferId`, the caller still owns the same frame and may retry it
later without over-releasing the permit. `IssuedRx` also rejects page frames
whose encoded issuance-pool identity does not match the receiver's attached
`IssuancePool`; in that case neither the page nor the permit are consumed.

## Zero-copy Arrow import

`issuance` composes directly with `import::ArrowPageDecoder::import_owned()`.
The permit stays leased for as long as the last page-backed Arrow owner remains
alive.

```rust
let batch = import::ArrowPageDecoder::new(schema)?.import_owned(page)?;
let first_column = batch.column(0).clone();
drop(batch);
// permit is still leased because `first_column` retains the page owner
drop(first_column);
// permit is returned here
# Ok::<(), Box<dyn std::error::Error>>(())
```
