//! Sans-IO multi-producer scan page flow between backend and worker roles.
//!
//! `scan_flow` coordinates one logical scan whose pages are produced by one or
//! more backend producers and consumed by one worker-side fan-in role.
//!
//! The crate is intentionally narrow:
//!
//! - one logical scan at a time per runtime
//! - one [`issuance::IssuedTx`] / [`issuance::IssuedRx`] stream per producer
//! - worker output is opaque [`issuance::IssuedReceivedPage`] ownership
//! - producer output keeps one unsent [`issuance::IssuedOutboundPage`] alive
//!   until the outer carrier confirms delivery and calls `mark_sent()`
//! - backpressure is only page-pool / issuance exhaustion
//! - no direct dependency on `pgrx`, `DataFusion`, `import`, or workspace
//!   `protocol`
//!
//! Normal scan EOF and scan failure are explicit logical events. They are not
//! encoded through `transfer::Close`, which remains a lower-level stream event.
//! Backend page sources return the exact payload length for each produced page,
//! so the final page in a variable-sized stream may expose only a prefix of the
//! writable payload region.

mod backend;
mod error;
mod fsm;
mod types;
mod worker;

#[cfg(test)]
mod tests;

pub use backend::{
    BackendPageSource, BackendProducerRole, BackendProducerStep, BackendScanCoordinator,
};
pub use error::{
    BackendCoordinatorError, BackendProducerError, BackendStreamError, ScanOpenError,
    WorkerRoleError,
};
pub use fsm::{
    BackendCoordinatorAction, BackendCoordinatorEvent, BackendCoordinatorState,
    BackendProducerAction, BackendProducerEvent, BackendProducerState, WorkerScanAction,
    WorkerScanEvent, WorkerScanState,
};
pub use types::{
    FlowId, LogicalTerminal, ProducerDescriptor, ProducerId, ProducerRoleKind, ScanOpen,
    SourcePageStatus,
};
pub use worker::{WorkerScanRole, WorkerStep};
