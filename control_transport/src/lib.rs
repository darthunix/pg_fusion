//! Allocation-free shared-memory control transport for one worker and multiple
//! backend slots.
//!
//! `control_transport` sits above [`lockfree`] and below any session-aware
//! control-plane lifecycle:
//!
//! - it owns the shared-memory layout
//! - it manages backend slot leasing
//! - it provides framed byte rings in both directions
//! - it uses ready flags plus `SIGUSR1` as wakeup hints
//! - it does not interpret payloads or model execution sessions
//!
//! This crate is intentionally Unix-only because it relies on Unix PID probing
//! and `SIGUSR1` delivery semantics.
//!
//! The crate intentionally models worker restart as a hard invalidation
//! boundary:
//!
//! - there is a single shared transport bank
//! - a restart bumps `region_generation`
//! - all old backend/worker handles become stale
//! - old-generation slots are recycled lazily after both sides detached
//! - higher layers must abort in-flight work and reacquire fresh transport
//!   state after the worker comes back
//!
//! For already-acquired backend leases, the lower transport only guarantees
//! local publication/consumption against the current slot state. If worker
//! restart or deactivation races an existing backend lease, a frame may be
//! published locally and then become lost once the higher layer observes
//! invalidation. This is an accepted transport semantic. Fresh backend lease
//! admission is stricter: once `worker_state` leaves `ONLINE`, new
//! `BackendSlotLease::acquire()` calls must fail even before the next
//! `region_generation` is published.
//!
//! Worker-side raw slot access is intentionally `unsafe`: the caller must
//! guarantee exclusive ownership for each slot/direction to preserve the SPSC
//! ring contract. Normal backend shutdown must explicitly call
//! `BackendSlotLease::release()` from a PostgreSQL exit hook; `Drop` is only a
//! best-effort fast path. Normal worker shutdown must likewise call
//! `WorkerTransport::release_owned_slots_for_exit()` from its PostgreSQL
//! termination callback before it deactivates the current generation.

#[cfg(not(unix))]
compile_error!("control_transport currently supports Unix only");
#[cfg(not(target_has_atomic = "128"))]
compile_error!("control_transport currently requires targets with 128-bit atomics");

mod error;
mod process;
mod region;
mod ring;

#[cfg(test)]
mod tests;

pub use error::{
    AcquireError, AttachError, BackendRxError, BackendTxError, ConfigError, InitError, LeaseError,
    NotifyError, ReinitError, RxError, SlotAccessError, TxError, WorkerAttachError,
    WorkerLifecycleError, WorkerRxError, WorkerTxError,
};
pub use region::{BackendRx, BackendSlotLease, BackendTx, ControlRx, ControlTx};
pub use region::{CommitOutcome, TransportRegion, TransportRegionLayout};
pub use region::{ReadySlots, WorkerRx, WorkerSlot, WorkerTransport, WorkerTx};
