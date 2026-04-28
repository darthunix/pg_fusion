//! Typed control-plane protocol carried in `control_transport` frames.
//!
//! `runtime_protocol` stays intentionally small:
//!
//! - one protocol payload fits into one `control_transport` frame
//! - `session_epoch` is always explicit
//! - plan and scan page streams are referenced through narrow descriptor types
//! - decode of scan producer descriptors is borrow-friendly and allocation-free
//! - scan terminal signals use their own dedicated wire family
//! - dedicated scan peers require at least `256` bytes inbound and `256` bytes
//!   outbound raw ring capacity
//!
//! The crate is organized into a few public modules:
//!
//! - [`session`] for `session_epoch` ordering and frame-size helpers
//! - [`scan`] for plan descriptors, scan descriptors, and borrowed set views
//! - [`message`] for versioned wire-message families and enums
//! - [`codec`] for encode/decode entrypoints
//! - [`error`] for protocol encoding and decoding failures
//!
//! The crate does not own execution state or flow runtimes. Higher layers are
//! responsible for classifying `session_epoch`, dropping stale traffic, and
//! reconstructing concrete `plan_flow` / `scan_flow` descriptors from the
//! values returned here.
//!
//! Typical control encode/decode stays available from the crate root:
//!
//! ```rust,ignore
//! use runtime_protocol::{
//!     decode_backend_execution_to_worker, encode_backend_execution_to_worker_into,
//!     BackendExecutionToWorker, ExecutionOptionsWire, PlanFlowDescriptor, ScanChannelSet,
//! };
//!
//! let message = BackendExecutionToWorker::StartExecution {
//!     session_epoch: 7,
//!     plan: PlanFlowDescriptor {
//!         plan_id: 42,
//!         page_kind: 0x4152,
//!         page_flags: 0,
//!     },
//!     options: ExecutionOptionsWire::default(),
//!     scans: ScanChannelSet::empty(),
//! };
//!
//! let mut encoded = [0u8; 128];
//! let len = encode_backend_execution_to_worker_into(message, &mut encoded)?;
//! let decoded = decode_backend_execution_to_worker(&encoded[..len])?;
//! let _ = decoded.session_epoch();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Dedicated scan slots also carry backend-to-worker terminal signals:
//!
//! ```rust,ignore
//! use runtime_protocol::{decode_backend_scan_to_worker, BackendScanToWorkerRef};
//!
//! match decode_backend_scan_to_worker(frame_bytes)? {
//!     BackendScanToWorkerRef::ScanFinished {
//!         session_epoch,
//!         scan_id,
//!         producer_id,
//!     } => {
//!         let _ = (session_epoch, scan_id, producer_id);
//!     }
//!     BackendScanToWorkerRef::ScanFailed {
//!         session_epoch,
//!         scan_id,
//!         producer_id,
//!         message,
//!     } => {
//!         let _ = (session_epoch, scan_id, producer_id, message);
//!     }
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod codec;
pub mod error;
pub mod message;
pub mod scan;
pub mod session;

mod envelope;
mod msgpack;
mod validation;

pub use crate::codec::{
    decode_backend_execution_to_worker, decode_backend_scan_to_worker,
    decode_runtime_message_family, decode_worker_execution_to_backend,
    decode_worker_scan_to_backend, encode_backend_execution_to_worker_into,
    encode_backend_scan_to_worker_into, encode_worker_execution_to_backend_into,
    encode_worker_scan_to_backend_into, encoded_len_backend_execution_to_worker,
    encoded_len_backend_scan_to_worker, encoded_len_worker_execution_to_backend,
    encoded_len_worker_scan_to_backend,
};
pub use crate::envelope::RUNTIME_ENVELOPE_HEADER_LEN;
pub use crate::error::{DecodeError, EncodeError};
pub use crate::message::{
    BackendExecutionToWorker, BackendExecutionToWorkerRef, BackendScanToWorker,
    BackendScanToWorkerRef, ExecutionFailureCode, ExecutionOptionsWire, RuntimeMessageFamily,
    WorkerExecutionToBackend, WorkerScanToBackend, WorkerScanToBackendRef,
};
pub use crate::scan::{
    BackendLeaseSlotWire, PlanFlowDescriptor, ProducerDescriptorWire, ProducerIter, ProducerRole,
    ProducerSetError, ProducerSetRef, ScanChannelDescriptorWire, ScanChannelIter, ScanChannelSet,
    ScanChannelSetError, ScanChannelSetRef, ScanFlowDescriptor, ScanFlowDescriptorRef,
};
pub use crate::session::{
    classify_session, max_message_len_for_ring_capacity, SessionDisposition,
    CONTROL_TRANSPORT_PAYLOAD_OVERHEAD, MAX_SCAN_FAILURE_MESSAGE_LEN,
    MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
};

#[cfg(test)]
mod tests;
