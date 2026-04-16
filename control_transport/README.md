# control_transport

`control_transport` is the allocation-free shared-memory transport layer for
the new backend/worker runtime path.

This crate is intentionally Unix-only. It relies on Unix PID probing and
`SIGUSR1` delivery semantics because it is designed for PostgreSQL child
processes on Unix-like systems.

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

The high-level safety contract lives in `spec/`, and
`spec/IMPLEMENTATION_REFINEMENT.md` records how the multi-step Rust operations
are reviewed against the atomic TLA+ transitions and reduced-core `loom`
harnesses.

Same-generation reuse safety is implemented with a packed per-slot metadata
word that carries the current `lease_epoch`, lease state, and owner bits in
one authoritative CAS domain. `slot_generation` remains separate for worker
restart invalidation, but lifecycle mutations are guarded by exact
`(slot_generation, lease_epoch)` snapshots and mutate ownership only through
that packed metadata word.

Worker-generation admission safety is implemented the same way at the region
level: `region_generation` and `worker_state` are published through one packed
region lifecycle word, so lease admission and worker attach rechecks never
reconstruct an "online generation" predicate from torn reads of separate
atomics.

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
- higher layers are expected to provide that coordination
- dead-backend reaping is conservative: Unix PID reuse can make `kill(pid, 0)`
  observe an unrelated new process as alive, so PID probes are only a
  best-effort liveness hint, not a proof of backend identity

## Typical usage

```rust,ignore
use control_transport::{BackendSlotLease, TransportRegion, TransportRegionLayout, WorkerTransport};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

let layout = TransportRegionLayout::new(8, 4096, 4096)?;
let region_layout = Layout::from_size_align(layout.size, layout.align)?;
let base = NonNull::new(unsafe { alloc_zeroed(region_layout) }).unwrap();

let region = unsafe { TransportRegion::init_in_place(base, layout.size, layout) }?;
let worker = WorkerTransport::attach(&region)?;
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

`init_in_place()` is the fresh-mapping path and must only be used once for a
given initialized shared-memory region. For a logical reset of the same
mapping and the same layout, use `reinit_in_place()` instead.
`reinit_in_place()` preserves monotonic lifecycle identity and retains old
leased slots out of the freelist until exact old-incarnation release/finalize
or later dead-backend reap, so stale backend handles cannot collide with fresh
leases after reset. During `reinit_in_place()` the region passes through a
transient non-online `REINITING` state, rebuilds the freelist from empty, and
republishes only sanitized free slots. Retained old leased slots remain
unpublished until their normal retirement path completes.

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
lease admission, however, is gated by the packed region lifecycle word: once
worker shutdown begins and the worker leaves `ONLINE`, fresh
`BackendSlotLease::acquire()` calls must fail with `AcquireError::WorkerOffline`
even before a later restart publishes a new generation.

Worker-side local ownership tracking is process-local and atomics-only. A
single worker process may attach multiple `WorkerTransport` handles to the same
region, but attaching two different `control_transport` regions in the same
PID lifetime returns `WorkerAttachError::RegionAlreadyAttached`; switching to a
different region requires a new process. The local registry is keyed by the
current worker-process PID as well as the region identity, so an inherited
registry after `fork()` is rebuilt from empty state on the next attach.
Same-layout `reinit_in_place()` also resets the local registry for that region
so late stale worker drops become harmless no-ops. Fresh backend lease
admission after `reinit_in_place()` may therefore remain blocked with
`AcquireError::Empty` until retained old slots are retired.
