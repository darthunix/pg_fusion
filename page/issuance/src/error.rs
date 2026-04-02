use std::error::Error;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    ZeroPermitCount,
    LayoutOverflow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitError {
    InvalidConfig(ConfigError),
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachError {
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
    BadMagic { expected: u64, actual: u64 },
    UnsupportedVersion { expected: u32, actual: u32 },
    InvalidConfig(ConfigError),
    LayoutMismatch { expected: usize, actual: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquireError {
    Empty,
    CorruptState { permit_id: u32, state: u8 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseError {
    BadPermitId { permit_id: u32, permit_count: u32 },
    DoubleFree { permit_id: u32 },
    CorruptState { permit_id: u32, state: u8 },
    CorruptFreeList { permit_id: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    BufferTooSmall { expected: usize, actual: usize },
    HeaderEncodeFailed { expected: usize, written: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    BadArrayLen { expected: u32, actual: u32 },
    InvalidMagic { expected: u32, actual: u32 },
    UnsupportedVersion { expected: u16, actual: u16 },
    InvalidFrameKind { actual: u8 },
    NonZeroFlags { actual: u8 },
    NonZeroCloseFields,
    TrailingBytes { expected: usize, actual: usize },
}

#[derive(Debug)]
pub enum IssuedTxError {
    NoPermits,
    Acquire(AcquireError),
    Tx(transfer::TxError),
}

#[derive(Debug)]
pub enum IssuedRxError {
    PermitPoolMismatch {
        expected: u64,
        actual: u64,
    },
    Rx(transfer::RxError),
    PermitRelease(ReleaseError),
    RxAndPermitRelease {
        rx: transfer::RxError,
        permit: ReleaseError,
    },
}

impl From<transfer::TxError> for IssuedTxError {
    fn from(value: transfer::TxError) -> Self {
        Self::Tx(value)
    }
}

impl From<transfer::RxError> for IssuedRxError {
    fn from(value: transfer::RxError) -> Self {
        Self::Rx(value)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPermitCount => write!(f, "permit count must be non-zero"),
            Self::LayoutOverflow => write!(f, "issuance layout overflowed usize"),
        }
    }
}

impl Error for ConfigError {}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(err) => write!(f, "invalid issuance config: {err}"),
            Self::RegionTooSmall { expected, actual } => {
                write!(
                    f,
                    "issuance region too small: expected {expected} bytes, got {actual}"
                )
            }
            Self::BadAlignment { expected, actual } => {
                write!(
                    f,
                    "issuance region has bad alignment: expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl Error for InitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfig(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegionTooSmall { expected, actual } => {
                write!(
                    f,
                    "issuance region too small: expected {expected} bytes, got {actual}"
                )
            }
            Self::BadAlignment { expected, actual } => {
                write!(
                    f,
                    "issuance region has bad alignment: expected {expected}, got {actual}"
                )
            }
            Self::BadMagic { expected, actual } => {
                write!(
                    f,
                    "issuance header has bad magic 0x{actual:016x}, expected 0x{expected:016x}"
                )
            }
            Self::UnsupportedVersion { expected, actual } => {
                write!(
                    f,
                    "unsupported issuance version {actual}, expected {expected}"
                )
            }
            Self::InvalidConfig(err) => {
                write!(f, "invalid issuance config in shared header: {err}")
            }
            Self::LayoutMismatch { expected, actual } => {
                write!(
                    f,
                    "issuance region layout mismatch: expected at least {expected} bytes, got {actual}"
                )
            }
        }
    }
}

impl Error for AttachError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfig(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "no issuance permits are currently available"),
            Self::CorruptState { permit_id, state } => {
                write!(f, "permit {permit_id} has corrupt acquire state {state}")
            }
        }
    }
}

impl Error for AcquireError {}

impl fmt::Display for ReleaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadPermitId {
                permit_id,
                permit_count,
            } => {
                write!(
                    f,
                    "permit id {permit_id} is out of range for permit_count {permit_count}"
                )
            }
            Self::DoubleFree { permit_id } => write!(f, "permit {permit_id} was already free"),
            Self::CorruptState { permit_id, state } => {
                write!(f, "permit {permit_id} has corrupt release state {state}")
            }
            Self::CorruptFreeList { permit_id } => {
                write!(f, "freelist rejected permit {permit_id} during release")
            }
        }
    }
}

impl Error for ReleaseError {}

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
                    "invalid issued frame array length {actual}, expected {expected}"
                )
            }
            Self::InvalidMagic { expected, actual } => {
                write!(
                    f,
                    "invalid issuance transport magic 0x{actual:08x}, expected 0x{expected:08x}"
                )
            }
            Self::UnsupportedVersion { expected, actual } => {
                write!(
                    f,
                    "unsupported issuance transport version {actual}, expected {expected}"
                )
            }
            Self::InvalidFrameKind { actual } => write!(f, "invalid issuance frame kind {actual}"),
            Self::NonZeroFlags { actual } => {
                write!(
                    f,
                    "issuance transport flags must be zero in v1, got {actual}"
                )
            }
            Self::NonZeroCloseFields => {
                write!(
                    f,
                    "close frame must zero permit and descriptor fields in v1"
                )
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

impl fmt::Display for IssuedTxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPermits => write!(f, "no issuance permits are currently available"),
            Self::Acquire(err) => write!(f, "failed to acquire issuance permit: {err}"),
            Self::Tx(err) => write!(f, "transfer sender error: {err}"),
        }
    }
}

impl Error for IssuedTxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Acquire(err) => Some(err),
            Self::Tx(err) => Some(err),
            Self::NoPermits => None,
        }
    }
}

impl fmt::Display for IssuedRxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PermitPoolMismatch { expected, actual } => write!(
                f,
                "issued frame targets issuance pool 0x{actual:016x}, but receiver is attached to 0x{expected:016x}"
            ),
            Self::Rx(err) => write!(f, "transfer receiver error: {err}"),
            Self::PermitRelease(err) => write!(f, "failed to release issuance permit: {err}"),
            Self::RxAndPermitRelease { rx, permit } => write!(
                f,
                "transfer receiver error: {rx}; additionally failed to release issuance permit: {permit}"
            ),
        }
    }
}

impl Error for IssuedRxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rx(err) => Some(err),
            Self::PermitRelease(err) => Some(err),
            Self::RxAndPermitRelease { rx, .. } => Some(rx),
            Self::PermitPoolMismatch { .. } => None,
        }
    }
}
