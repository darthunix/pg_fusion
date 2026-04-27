//! Session ordering helpers and frame-capacity calculations.
//!
//! These helpers do not own any runtime state. They only encode the small
//! invariants shared by higher layers: how much payload fits into one framed
//! `control_transport` slot, and how one incoming `session_epoch` compares to a
//! local current one.

/// Bytes unavailable for `control_transport` payload data because framed rings
/// reserve a four-byte prefix plus one extra byte to distinguish empty from
/// full.
pub const CONTROL_TRANSPORT_PAYLOAD_OVERHEAD: usize = 5;

/// Return the maximum protocol payload size that can fit into one
/// `control_transport` ring with the given raw data capacity.
pub fn max_message_len_for_ring_capacity(capacity: usize) -> usize {
    capacity.saturating_sub(CONTROL_TRANSPORT_PAYLOAD_OVERHEAD)
}

/// Minimum raw `control_transport` ring capacity required for dedicated
/// backend-to-worker scan peers.
///
/// This bound covers both:
///
/// - fixed-size `issuance::ISSUED_HEADER_LEN` page headers
/// - bounded `BackendScanToWorker::ScanFailed` terminals
pub const MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY: usize = 256;

/// Minimum raw `control_transport` ring capacity required for dedicated
/// worker-to-backend scan peers.
///
/// This bound covers `OpenScan` control payloads with one leader producer plus
/// the current pg_fusion maximum of 32 additional scan worker producers.
pub const MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY: usize = 256;

/// Maximum UTF-8 byte length allowed for `BackendScanToWorker::ScanFailed`
/// text.
///
/// This is chosen so a worst-case `ScanFailed` message still fits into the
/// minimum dedicated inbound scan ring:
///
/// - raw ring capacity = `256`
/// - payload budget = `256 - CONTROL_TRANSPORT_PAYLOAD_OVERHEAD = 251`
/// - fixed worst-case `ScanFailed` overhead = `31`
/// - remaining text budget = `220`
pub const MAX_SCAN_FAILURE_MESSAGE_LEN: usize = 220;

/// How an incoming `session_epoch` compares to the local current one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDisposition {
    /// The incoming message targets the current execution.
    Current,
    /// The incoming message belongs to an older execution.
    Stale,
    /// The incoming message belongs to a newer execution.
    Future,
}

/// Classify an incoming `session_epoch` against the local current one.
#[inline]
pub fn classify_session(current: u64, incoming: u64) -> SessionDisposition {
    match incoming.cmp(&current) {
        std::cmp::Ordering::Equal => SessionDisposition::Current,
        std::cmp::Ordering::Less => SessionDisposition::Stale,
        std::cmp::Ordering::Greater => SessionDisposition::Future,
    }
}
