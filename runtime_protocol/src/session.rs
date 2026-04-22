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
