# control_ipc

`control_ipc` is an allocation-free shared-memory control layer for the new
backend/worker runtime path.

It intentionally stays low-level:

- one shared region with two generation banks
- one persistent backend-owned slot per backend process
- one `session_epoch` per execution on that slot inside one region generation
- framed control rings in both directions
- ready flags and PID-based wakeup hints
- no message schema and no runtime FSM

This crate carries opaque framed bytes only. Higher layers define the message
format and execution lifecycle.

The worker owns generation activation. A worker start or restart must publish a
fresh generation before any backend acquires a slot. All handles from older
generations become stale and must be dropped.

## Termination contract

Backend termination:

- on normal backend exit, the embedding layer should call `BackendSlotLease::release()`
  from PostgreSQL exit hooks and then let the process-local lease drop
- `Drop` is only a best-effort fast path; higher layers should not rely on it
  as the only cleanup mechanism
- abnormal backend death is not recovered in-band inside `control_ipc`

Worker termination:

- a worker restart must call `activate_generation(...)` before serving traffic
- activating a new generation switches `control_ipc` to the other bank and
  invalidates every old backend lease, worker slot, session epoch, ready flag,
  and queued control frame from the previous generation
- higher layers must treat that generation switch as an execution abort
  boundary and reacquire fresh backend leases

## Typical usage

```rust,ignore
use control_ipc::{BackendSlotLease, ControlRegion, ControlRegionLayout, WorkerControl};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

let layout = ControlRegionLayout::new(8, 4096, 4096)?;
let region_layout = Layout::from_size_align(layout.size, layout.align)?;
let base = NonNull::new(unsafe { alloc_zeroed(region_layout) }).unwrap();

let region = unsafe { ControlRegion::init_in_place(base, layout.size, layout) }?;
let worker = WorkerControl::attach(&region);
let generation = worker.activate_generation(1234);
assert_eq!(generation, 1);

let mut backend = BackendSlotLease::acquire(&region)?;
let epoch = backend.begin_session()?;

{
    let mut tx = backend.to_worker_tx();
    let mut writer = tx.try_reserve(5)?;
    writer.payload_mut().copy_from_slice(b"hello");
    let _outcome = writer.commit();
}

let mut slot = worker.slot(backend.slot_id())?;
assert_eq!(slot.session_epoch(), epoch);
let mut rx = slot.from_backend_rx()?;
let frame = rx.peek_frame()?.unwrap();
assert_eq!(frame, b"hello");
rx.consume_frame()?;

backend.release();
unsafe { dealloc(base.as_ptr(), region_layout) };
# Ok::<(), Box<dyn std::error::Error>>(())
```

`commit()` publishes the frame before it attempts `SIGUSR1`. A notification
failure does not roll the frame back and must not be retried as a second send.
Worker-side slot access is session-bound: a live `WorkerSlot`, `WorkerRx`,
`WorkerTx`, or `WorkerFrameWriter` keeps the current claim alive, and
`begin_session()` fails until that claim is dropped.

`BackendSlotLease`, `BackendTx`, `BackendRx`, `WorkerSlot`, `WorkerTx`, and
`WorkerRx` are all generation-bound. If the worker activates a new generation,
all handles from older generations start returning stale-generation errors
instead of touching the new bank's traffic.
