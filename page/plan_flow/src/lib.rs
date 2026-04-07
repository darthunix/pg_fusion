//! Sans-IO single-sender multi-page logical plan transfer.
//!
//! `plan_flow` streams one backend-built logical plan through page-backed
//! chunks:
//!
//! - backend-side bytes come from [`plan_codec::PlanEncodeSession`]
//! - transport ownership comes from [`issuance`]
//! - worker-side reconstruction comes from [`plan_codec::PlanDecodeSession`]
//!
//! Normal EOF is encoded as [`issuance::IssuedOwnedFrame::Close`]. Logical
//! backend failures are reported out-of-band with
//! [`WorkerPlanRole::accept_sender_error`].
//!
//! The intended call sequence is:
//!
//! 1. open [`BackendPlanRole`] and [`WorkerPlanRole`] with the same [`PlanOpen`]
//! 2. repeatedly call [`BackendPlanRole::step`] until it emits page frames and
//!    eventually one terminal close frame
//! 3. feed those frames into [`WorkerPlanRole::accept_frame`]
//! 4. close both roles with [`BackendPlanRole::close`] and
//!    [`WorkerPlanRole::close`] after a terminal success or failure
//!
//! On the worker side, each accepted page is consumed synchronously and
//! released before [`WorkerPlanRole::accept_frame`] returns. On the backend
//! side, a sender that has emitted the terminal close frame becomes exhausted
//! after [`BackendPlanRole::close`] because the underlying issued sender stream
//! is permanently closed.
//!
//! This flow is intentionally separate from `scan_flow` and is expected to
//! finish before any scan-page streaming for the decoded plan begins.

mod backend;
mod error;
mod fsm;
mod types;
mod worker;

#[cfg(test)]
mod tests;

pub use backend::{BackendPlanRole, BackendPlanStep};
pub use error::{BackendPlanError, WorkerPlanError};
pub use fsm::{
    BackendPlanAction, BackendPlanEvent, BackendPlanState, WorkerPlanAction, WorkerPlanEvent,
    WorkerPlanState,
};
pub use types::{FlowId, PlanOpen};
pub use worker::{WorkerPlanRole, WorkerStep};
