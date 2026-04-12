# control_transport

`control_transport` is the allocation-free shared-memory transport layer for
the new backend/worker runtime path.

Both sides are expected to be PostgreSQL child processes:

- backend leases belong to PostgreSQL backend processes
- worker handles belong to the PostgreSQL background worker process that owns
  the transport generation

It intentionally stays below any session-aware control-plane lifecycle:

- one shared region with one transport bank
- one backend-owned slot per live backend lease
- one raw worker owner per leased slot
- framed control rings in both directions
- ready flags and PID-based wakeup hints
- no message schema
- no execution/session FSM

## Worker restart contract

`control_transport` treats worker restart as a hard invalidation boundary:

- there is exactly one active transport bank
- worker restart bumps `region_generation`
- all backend and worker handles from older generations become stale
- old-generation slots are recycled lazily, only after both backend and worker
  owners detached
- higher layers must abort in-flight work and reacquire fresh transport state

This crate does **not** try to preserve old connections across a worker
restart, and it does **not** expose long-lived borrows into shared-memory frame
payloads. Send/receive APIs are copy-in/copy-out so stale handles can fail with
`WorkerOffline` or `StaleGeneration` without keeping unsafe access to recycled
memory alive.

`WorkerTransport::deactivate_generation()` invalidates the current generation
and leaves the transport offline. `WorkerTransport::activate_generation(pid)`
publishes a fresh online generation.

These lifecycle calls are intentionally fallible: the current worker process
must not switch generations while it still has live `WorkerSlot` owners. In an
orderly shutdown path the worker must:

1. stop issuing worker I/O
2. call `release_owned_slots_for_exit()` from its PostgreSQL termination
   callback
3. call `deactivate_generation()`

`activate_generation(pid)` is therefore a startup-time operation for a worker
process. It may sweep stale worker owners left behind by a previously dead
worker process, but it must not be used as an in-process hot restart while old
worker slot handles are still alive.

For already-acquired backend leases, the transport only guarantees local ring
publication and consumption. If worker restart or deactivation races an
existing backend lease, `send_frame()` may still publish locally and later be
treated as lost traffic once the higher layer observes invalidation. This is an
accepted lower-layer semantic: `control_transport` does not guarantee that the
worker will actually service traffic published after shutdown has begun.

## Safety contract

Backend slot access is safe because each lease comes from the shared freelist.

Worker slot access is raw transport only, so it is intentionally `unsafe`:

- `WorkerTransport::slot_unchecked(slot_id)` requires the caller to guarantee
  exclusive ownership of that slot and its ring directions
- only one live worker owner is allowed per slot; a second attach returns
  `SlotAccessError::Busy`
- this crate does not coordinate worker claims across processes or tasks
- higher layers such as the future session-aware `control_ipc` wrapper are
  expected to provide that coordination

## Typical usage

```rust,ignore
use control_transport::{BackendSlotLease, TransportRegion, TransportRegionLayout, WorkerTransport};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

let layout = TransportRegionLayout::new(8, 4096, 4096)?;
let region_layout = Layout::from_size_align(layout.size, layout.align)?;
let base = NonNull::new(unsafe { alloc_zeroed(region_layout) }).unwrap();

let region = unsafe { TransportRegion::init_in_place(base, layout.size, layout) }?;
let worker = WorkerTransport::attach(&region);
let generation = worker.activate_generation(1234)?;
assert_eq!(generation, 1);

let mut backend = BackendSlotLease::acquire(&region)?;
backend.to_worker_tx().send_frame(b"hello")?;

let mut slot = unsafe { worker.slot_unchecked(backend.slot_id())? };
let mut rx = slot.from_backend_rx()?;
let mut buf = [0u8; 32];
let len = rx.recv_frame_into(&mut buf)?.unwrap();
assert_eq!(&buf[..len], b"hello");

backend.release();
unsafe { dealloc(base.as_ptr(), region_layout) };
# Ok::<(), Box<dyn std::error::Error>>(())
```

`send_frame()` publishes the frame before it attempts `SIGUSR1`. A
notification failure does not roll the frame back and must not be retried as a
second send.

Normal backend teardown must call `BackendSlotLease::release()` from a
PostgreSQL backend exit hook such as `before_shmem_exit` / `on_proc_exit`.
`Drop` is only a best-effort fast path and is not sufficient for correctness if
the backend exits abnormally.

Normal worker teardown must likewise call
`WorkerTransport::release_owned_slots_for_exit()` from its PostgreSQL worker
termination callback before deactivating the generation. `Drop` on worker
handles is only a best-effort fast path.

Higher layers should call `deactivate_generation()` or
`activate_generation()` when the worker shuts down or restarts so stale handles
stop being usable. Generation changes may cause in-flight reads or writes to
return `StaleGeneration` after touching old-generation ring state; such traffic
is considered lost and must be retried at a higher layer if needed. New backend
lease admission, however, is gated by `worker_state`: once worker shutdown
begins and the state leaves `ONLINE`, fresh `BackendSlotLease::acquire()` calls
must fail with `AcquireError::WorkerOffline` even before the generation bump is
published.
