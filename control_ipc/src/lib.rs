//! Allocation-free shared-memory control connection layer for one worker and
//! multiple backend slots.
//!
//! `control_ipc` sits above [`lockfree`] and below any message schema or
//! orchestration FSM:
//!
//! - it owns the shared-memory layout
//! - it manages backend slot leasing
//! - it provides framed byte rings in both directions
//! - it uses ready flags plus `SIGUSR1` as wakeup hints
//! - it does not interpret control messages
//!
//! The intended flow is:
//!
//! 1. Compute the region shape with [`ControlRegionLayout::new`].
//! 2. Initialize the shared region once with [`ControlRegion::init_in_place`].
//! 3. Attach process-local handles with [`ControlRegion::attach`] or reuse the
//!    initialized handle.
//! 4. A worker activates a fresh region generation with
//!    [`ControlRegion::activate_worker_generation`].
//! 5. A backend acquires one persistent [`BackendSlotLease`] from that active
//!    generation.
//! 6. Each execution starts with [`BackendSlotLease::begin_session`]. Backend
//!    traffic to the worker is invalid before this step.
//! 7. Backends exchange opaque framed payloads through generation-bound
//!    [`BackendTx`] and [`BackendRx`], while workers use session-bound
//!    [`WorkerTx`] and [`WorkerRx`] obtained from [`WorkerSlot`].
//!
//! Termination rules:
//!
//! - normal backend teardown should call [`BackendSlotLease::release`] from the
//!   embedding layer's exit hooks; `Drop` is only a best-effort fast path
//! - abnormal backend death is not reclaimed in-band inside `control_ipc`
//! - worker restart must call [`ControlRegion::activate_worker_generation`]
//!   again, which switches the active bank and makes every old handle return a
//!   stale-generation error
//!
//! This crate intentionally has no runtime FSM. Execution/session semantics
//! belong to higher layers.

mod error;
mod region;
mod ring;

#[cfg(test)]
mod tests;

pub use error::{
    AcquireError, AttachError, BackendRxError, BackendTxError, BeginSessionError, ConfigError,
    InitError, LeaseError, NotifyError, RxError, SlotError, TxError, WorkerRxError, WorkerTxError,
};
pub use region::{
    BackendFrameWriter, BackendRx, BackendSlotLease, BackendTx, CommitOutcome, ControlRegion,
    ControlRegionLayout, ControlRx, ControlTx, FrameWriter, ReadySlots, WorkerControl,
    WorkerFrameWriter, WorkerRx, WorkerSlot, WorkerTx,
};
