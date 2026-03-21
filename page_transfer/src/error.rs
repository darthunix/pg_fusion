use page_pool::{AccessError, AcquireError, DetachError, ReleaseError};
use std::error::Error;
use std::fmt;
use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    BufferTooSmall { expected: usize, actual: usize },
    HeaderEncodeFailed { expected: usize, written: usize },
}

/// Errors returned while decoding transport control frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    BadArrayLen { expected: u32, actual: u32 },
    InvalidMagic { expected: u32, actual: u32 },
    UnsupportedVersion { expected: u16, actual: u16 },
    InvalidFrameKind { actual: u8 },
    NonZeroFlags { actual: u8 },
    NonZeroCloseDescriptor,
    TrailingBytes { expected: usize, actual: usize },
}

/// Errors returned when the in-page message header is malformed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidPageError {
    BadArrayLen { expected: u32, actual: u32 },
    InvalidMagic { expected: u32, actual: u32 },
    UnsupportedVersion { expected: u16, actual: u16 },
    PayloadTooLarge { payload_len: u32, capacity: usize },
    TrailingBytes { expected: usize, actual: usize },
}

/// Sender-side errors for page acquisition, message writing, and detach.
#[derive(Debug)]
pub enum TxError {
    Closed,
    Busy,
    PageTooSmall { required: usize, actual: usize },
    PayloadCapacityTooLarge { capacity: usize, max: usize },
    PayloadTooLarge { actual: usize, max: usize },
    Acquire(AcquireError),
    Detach(DetachError),
    Encode(EncodeError),
}

/// Receiver-side errors for frame acceptance, page validation, and release.
#[derive(Debug)]
pub enum RxError {
    Closed,
    Busy,
    UnexpectedTransferId { expected: u64, actual: u64 },
    Access(AccessError),
    InvalidPage(InvalidPageError),
    Release(ReleaseError),
    Decode(DecodeError),
}

impl From<EncodeError> for TxError {
    fn from(value: EncodeError) -> Self {
        Self::Encode(value)
    }
}

impl From<DecodeError> for RxError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall { expected, actual } => {
                write!(
                    f,
                    "buffer too small: expected {expected} bytes, got {actual}"
                )
            }
            Self::HeaderEncodeFailed { expected, written } => {
                write!(
                    f,
                    "header encoding wrote {written} bytes, expected exactly {expected}"
                )
            }
        }
    }
}

impl Error for EncodeError {}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadArrayLen { expected, actual } => {
                write!(
                    f,
                    "invalid header array length {actual}, expected {expected}"
                )
            }
            Self::InvalidMagic { expected, actual } => {
                write!(
                    f,
                    "invalid transport magic 0x{actual:08x}, expected 0x{expected:08x}"
                )
            }
            Self::UnsupportedVersion { expected, actual } => {
                write!(
                    f,
                    "unsupported transport version {actual}, expected {expected}"
                )
            }
            Self::InvalidFrameKind { actual } => {
                write!(f, "invalid transport frame kind {actual}")
            }
            Self::NonZeroFlags { actual } => {
                write!(f, "transport flags must be zero in v1, got {actual}")
            }
            Self::NonZeroCloseDescriptor => {
                write!(f, "close frame must zero descriptor fields in v1")
            }
            Self::TrailingBytes { expected, actual } => {
                write!(
                    f,
                    "decoder consumed {expected} bytes but input had {actual} bytes"
                )
            }
        }
    }
}

impl Error for DecodeError {}

impl fmt::Display for InvalidPageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadArrayLen { expected, actual } => {
                write!(
                    f,
                    "invalid page header array length {actual}, expected {expected}"
                )
            }
            Self::InvalidMagic { expected, actual } => {
                write!(
                    f,
                    "invalid page magic 0x{actual:08x}, expected 0x{expected:08x}"
                )
            }
            Self::UnsupportedVersion { expected, actual } => {
                write!(f, "unsupported page version {actual}, expected {expected}")
            }
            Self::PayloadTooLarge {
                payload_len,
                capacity,
            } => {
                write!(
                    f,
                    "page payload length {payload_len} exceeds page payload capacity {capacity}"
                )
            }
            Self::TrailingBytes { expected, actual } => {
                write!(
                    f,
                    "page header decoder consumed {expected} bytes but input had {actual} bytes"
                )
            }
        }
    }
}

impl Error for InvalidPageError {}

impl fmt::Display for TxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "page sender is closed"),
            Self::Busy => write!(
                f,
                "page sender already has an active writer or outbound page"
            ),
            Self::PageTooSmall { required, actual } => {
                write!(
                    f,
                    "page size {actual} is too small for page header prefix of {required} bytes"
                )
            }
            Self::PayloadCapacityTooLarge { capacity, max } => {
                write!(
                    f,
                    "page payload capacity {capacity} exceeds maximum encodable payload length {max}"
                )
            }
            Self::PayloadTooLarge { actual, max } => {
                write!(
                    f,
                    "page payload length {actual} exceeds maximum encodable payload length {max}"
                )
            }
            Self::Acquire(err) => write!(f, "failed to acquire page: {err}"),
            Self::Detach(err) => write!(f, "failed to detach page: {err}"),
            Self::Encode(err) => write!(f, "failed to encode page header: {err}"),
        }
    }
}

impl Error for TxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Acquire(err) => Some(err),
            Self::Detach(err) => Some(err),
            Self::Encode(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for RxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "page receiver is closed"),
            Self::Busy => write!(f, "page receiver already has an active page"),
            Self::UnexpectedTransferId { expected, actual } => {
                write!(f, "unexpected transfer id {actual}, expected {expected}")
            }
            Self::Access(err) => write!(f, "failed to access detached page: {err}"),
            Self::InvalidPage(err) => write!(f, "invalid page payload header: {err}"),
            Self::Release(err) => write!(f, "failed to release received page: {err}"),
            Self::Decode(err) => write!(f, "failed to decode transport frame: {err}"),
        }
    }
}

impl Error for RxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Access(err) => Some(err),
            Self::InvalidPage(err) => Some(err),
            Self::Release(err) => Some(err),
            Self::Decode(err) => Some(err),
            _ => None,
        }
    }
}

pub(crate) fn write_zero() -> io::Error {
    io::Error::new(io::ErrorKind::WriteZero, "page payload is full")
}
