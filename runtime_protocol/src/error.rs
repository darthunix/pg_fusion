//! Error types returned by `runtime_protocol` encode and decode entrypoints.
//!
//! `EncodeError` reports locally constructed payloads that cannot be written to
//! the provided buffer or violate local invariants. `DecodeError` reports
//! malformed remote payloads after envelope parsing or MsgPack decoding.

use crate::scan::ScanChannelSetError;
use thiserror::Error;

/// Errors returned while encoding one runtime-protocol message.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    #[error("output buffer too small: expected at least {expected} bytes, got {actual}")]
    BufferTooSmall { expected: usize, actual: usize },
    #[error("too many scan producers to encode: {count}")]
    TooManyProducers { count: usize },
    #[error("too many scan channels to encode: {count}")]
    TooManyScanChannels { count: usize },
    #[error("scan failure message is too long: {actual} bytes exceeds maximum {maximum}")]
    ScanFailureMessageTooLong { actual: usize, maximum: usize },
    #[error("MsgPack encoding failed: {0}")]
    MsgPack(String),
    #[error("runtime envelope encoding failed: {0}")]
    Envelope(String),
    #[error(transparent)]
    ScanChannels(#[from] ScanChannelSetError),
}

/// Errors returned while decoding one runtime-protocol message.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("protocol array expected length {expected}, got {actual}")]
    InvalidArrayLen { expected: u32, actual: u32 },
    #[error("runtime envelope is truncated: expected at least {expected} bytes, got {actual}")]
    TruncatedEnvelope { expected: usize, actual: usize },
    #[error("invalid runtime magic: expected 0x{expected:08x}, got 0x{actual:08x}")]
    InvalidMagic { expected: u32, actual: u32 },
    #[error("unsupported runtime protocol version: expected {expected}, got {actual}")]
    UnsupportedVersion { expected: u16, actual: u16 },
    #[error("unexpected runtime message family {actual}")]
    UnexpectedMessageFamily { actual: u8 },
    #[error("unexpected message tag {actual}")]
    UnexpectedTag { actual: u8 },
    #[error("invalid failure code {actual}")]
    InvalidFailureCode { actual: u8 },
    #[error("invalid producer role {actual}")]
    InvalidProducerRole { actual: u8 },
    #[error("duplicate scan_id {scan_id} in scan channel set")]
    DuplicateScanId { scan_id: u64 },
    #[error("scan channel set is not sorted by scan_id: previous={previous}, current={current}")]
    ScanIdOutOfOrder { previous: u64, current: u64 },
    #[error("scan open must declare at least one producer")]
    EmptyProducerSet,
    #[error("duplicate producer id {producer_id} in scan open")]
    DuplicateProducerId { producer_id: u16 },
    #[error("scan open may declare at most one leader producer")]
    MultipleLeaders,
    #[error("decoded payload has trailing bytes: {remaining}")]
    TrailingBytes { remaining: usize },
    #[error("MsgPack decoding failed: {0}")]
    MsgPack(String),
}
