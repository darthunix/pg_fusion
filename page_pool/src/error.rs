use std::error::Error;
use std::fmt;

/// Configuration validation errors for [`crate::PagePoolConfig`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    ZeroPageSize,
    ZeroPageCount,
    PageSizeTooSmall { minimum: usize, actual: usize },
    PageSizeNotAligned { align: usize, actual: usize },
    LayoutOverflow,
}

/// Errors returned by [`crate::PagePool::init_in_place`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitError {
    InvalidConfig(ConfigError),
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
}

/// Errors returned by [`crate::PagePool::attach`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachError {
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
    BadMagic { expected: u64, actual: u64 },
    UnsupportedVersion { expected: u32, actual: u32 },
    InvalidConfig(ConfigError),
    LayoutMismatch { expected: usize, actual: usize },
}

/// Errors returned by [`crate::PagePool::try_acquire`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquireError {
    Empty,
    CorruptState {
        page_id: u32,
        generation: u64,
        raw_state: u8,
    },
}

/// Errors returned by [`crate::PageLease::into_descriptor`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetachError {
    StaleGeneration {
        page_id: u32,
        expected: u64,
        actual: u64,
    },
    NotAttached {
        page_id: u32,
        generation: u64,
        raw_state: u8,
    },
    CorruptState {
        page_id: u32,
        generation: u64,
        raw_state: u8,
    },
}

/// Errors returned by [`crate::PagePool::release`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseError {
    PoolMismatch {
        expected_pool_id: u64,
        actual_pool_id: u64,
    },
    BadPageId {
        page_id: u32,
        page_count: u32,
    },
    StaleGeneration {
        page_id: u32,
        expected: u64,
        actual: u64,
    },
    LeaseStillAttached {
        page_id: u32,
        generation: u64,
    },
    Exhausted {
        page_id: u32,
        generation: u64,
    },
    NotLeased {
        page_id: u32,
        generation: u64,
    },
    CorruptState {
        page_id: u32,
        generation: u64,
        raw_state: u8,
    },
    CorruptFreeList {
        page_id: u32,
    },
}

/// Errors returned by descriptor-based page accessors such as
/// [`crate::PagePool::page_bytes`] and [`crate::PagePool::page_bytes_mut`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessError {
    PoolMismatch {
        expected_pool_id: u64,
        actual_pool_id: u64,
    },
    BadPageId {
        page_id: u32,
        page_count: u32,
    },
    StaleGeneration {
        page_id: u32,
        expected: u64,
        actual: u64,
    },
    LeaseStillAttached {
        page_id: u32,
        generation: u64,
    },
    Exhausted {
        page_id: u32,
        generation: u64,
    },
    NotLeased {
        page_id: u32,
        generation: u64,
    },
    CorruptState {
        page_id: u32,
        generation: u64,
        raw_state: u8,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPageSize => write!(f, "page size must be non-zero"),
            Self::ZeroPageCount => write!(f, "page count must be non-zero"),
            Self::PageSizeTooSmall { minimum, actual } => {
                write!(f, "page size {actual} is smaller than minimum {minimum}")
            }
            Self::PageSizeNotAligned { align, actual } => {
                write!(f, "page size {actual} is not aligned to {align} bytes")
            }
            Self::LayoutOverflow => write!(f, "page pool layout overflowed usize"),
        }
    }
}

impl Error for ConfigError {}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(err) => write!(f, "invalid page pool config: {err}"),
            Self::RegionTooSmall { expected, actual } => {
                write!(
                    f,
                    "region too small: expected at least {expected} bytes, got {actual}"
                )
            }
            Self::BadAlignment { expected, actual } => {
                write!(
                    f,
                    "region base 0x{actual:x} is not aligned to {expected} bytes"
                )
            }
        }
    }
}

impl Error for InitError {}

impl fmt::Display for AttachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegionTooSmall { expected, actual } => {
                write!(
                    f,
                    "region too small: expected at least {expected} bytes, got {actual}"
                )
            }
            Self::BadAlignment { expected, actual } => {
                write!(
                    f,
                    "region base 0x{actual:x} is not aligned to {expected} bytes"
                )
            }
            Self::BadMagic { expected, actual } => {
                write!(
                    f,
                    "unexpected page pool magic 0x{actual:x}, expected 0x{expected:x}"
                )
            }
            Self::UnsupportedVersion { expected, actual } => {
                write!(
                    f,
                    "unsupported page pool version {actual}, expected {expected}"
                )
            }
            Self::InvalidConfig(err) => write!(f, "invalid page pool header config: {err}"),
            Self::LayoutMismatch { expected, actual } => {
                write!(
                    f,
                    "page pool region size mismatch: expected {expected}, header reports {actual}"
                )
            }
        }
    }
}

impl Error for AttachError {}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "page pool is empty"),
            Self::CorruptState {
                page_id,
                generation,
                raw_state,
            } => write!(
                f,
                "page {page_id} entered freelist with invalid state generation={generation} raw_state={raw_state}"
            ),
        }
    }
}

impl Error for AcquireError {}

impl fmt::Display for DetachError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGeneration {
                page_id,
                expected,
                actual,
            } => write!(
                f,
                "page {page_id} generation mismatch while detaching: expected {expected}, got {actual}"
            ),
            Self::NotAttached {
                page_id,
                generation,
                raw_state,
            } => write!(
                f,
                "page {page_id} generation {generation} is not attached (raw_state={raw_state})"
            ),
            Self::CorruptState {
                page_id,
                generation,
                raw_state,
            } => write!(
                f,
                "page {page_id} has corrupt state while detaching: generation={generation} raw_state={raw_state}"
            ),
        }
    }
}

impl Error for DetachError {}

impl fmt::Display for ReleaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PoolMismatch {
                expected_pool_id,
                actual_pool_id,
            } => write!(
                f,
                "descriptor pool mismatch: expected pool_id=0x{expected_pool_id:016x}, got pool_id=0x{actual_pool_id:016x}"
            ),
            Self::BadPageId {
                page_id,
                page_count,
            } => write!(
                f,
                "page id {page_id} is out of range for pool with {page_count} pages"
            ),
            Self::StaleGeneration {
                page_id,
                expected,
                actual,
            } => write!(
                f,
                "page {page_id} generation mismatch: expected {expected}, got {actual}"
            ),
            Self::LeaseStillAttached {
                page_id,
                generation,
            } => write!(
                f,
                "page {page_id} generation {generation} is still attached to a live lease"
            ),
            Self::Exhausted {
                page_id,
                generation,
            } => write!(
                f,
                "page {page_id} generation {generation} is exhausted and cannot be reused"
            ),
            Self::NotLeased {
                page_id,
                generation,
            } => write!(f, "page {page_id} generation {generation} is not leased"),
            Self::CorruptState {
                page_id,
                generation,
                raw_state,
            } => write!(
                f,
                "page {page_id} has corrupt release state: generation={generation} raw_state={raw_state}"
            ),
            Self::CorruptFreeList { page_id } => {
                write!(f, "freelist rejected page {page_id} during release")
            }
        }
    }
}

impl Error for ReleaseError {}

impl fmt::Display for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PoolMismatch {
                expected_pool_id,
                actual_pool_id,
            } => write!(
                f,
                "descriptor pool mismatch: expected pool_id=0x{expected_pool_id:016x}, got pool_id=0x{actual_pool_id:016x}"
            ),
            Self::BadPageId {
                page_id,
                page_count,
            } => write!(
                f,
                "page id {page_id} is out of range for pool with {page_count} pages"
            ),
            Self::StaleGeneration {
                page_id,
                expected,
                actual,
            } => write!(
                f,
                "page {page_id} generation mismatch: expected {expected}, got {actual}"
            ),
            Self::LeaseStillAttached {
                page_id,
                generation,
            } => write!(
                f,
                "page {page_id} generation {generation} is still attached to a live lease"
            ),
            Self::Exhausted {
                page_id,
                generation,
            } => write!(
                f,
                "page {page_id} generation {generation} is exhausted and cannot be accessed"
            ),
            Self::NotLeased {
                page_id,
                generation,
            } => write!(f, "page {page_id} generation {generation} is not leased"),
            Self::CorruptState {
                page_id,
                generation,
                raw_state,
            } => write!(
                f,
                "page {page_id} has corrupt access state: generation={generation} raw_state={raw_state}"
            ),
        }
    }
}

impl Error for AccessError {}
