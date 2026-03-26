use arrow_ipc::MetadataVersion;
use arrow_schema::ArrowError;
use page_transfer::MessageKind;
use thiserror::Error;

/// Decoder construction errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Dictionary-encoded schemas are intentionally out of scope in v1.
    #[error("dictionary-encoded schemas are unsupported in page_arrow v1")]
    UnsupportedDictionarySchema,
}

/// Import errors for page-backed Arrow batches.
#[derive(Debug, Error)]
pub enum ImportError {
    #[error("unexpected page kind: expected {expected}, got {actual}")]
    WrongKind {
        expected: MessageKind,
        actual: MessageKind,
    },
    #[error("unsupported page flags: expected 0, got {actual}")]
    UnsupportedFlags { actual: u16 },
    #[error("payload too small: expected at least {expected} bytes, got {actual}")]
    PayloadTooSmall { expected: usize, actual: usize },
    #[error("invalid batch page magic: expected {expected:#x}, got {actual:#x}")]
    InvalidMagic { expected: u32, actual: u32 },
    #[error("unsupported batch page version: expected {expected}, got {actual}")]
    UnsupportedBatchPageVersion { expected: u16, actual: u16 },
    #[error("batch page reserved field must be 0, got {actual}")]
    NonZeroReserved { actual: u16 },
    #[error("invalid metadata length {meta_len}; available bytes after header: {available}")]
    InvalidMetaLen { meta_len: u32, available: usize },
    #[error("IPC body start is not aligned to 16 bytes; meta_len is {actual}")]
    MisalignedIpcBody { actual: usize },
    #[error("truncated encapsulated IPC message: expected at least {expected_at_least} bytes, got {actual}")]
    TruncatedEncapsulatedMessage {
        expected_at_least: usize,
        actual: usize,
    },
    #[error("invalid encapsulated IPC message length {actual}")]
    InvalidEncapsulatedMessageLength { actual: i32 },
    #[error("invalid IPC message: {0}")]
    InvalidIpcMessage(String),
    #[error("unexpected IPC message header {actual}; expected RecordBatch")]
    UnexpectedMessageHeader { actual: String },
    #[error("invalid Arrow IPC body length {actual}")]
    InvalidBodyLen { actual: i64 },
    #[error("truncated Arrow IPC body: expected at least {expected_at_least} bytes, got {actual}")]
    TruncatedIpcBody {
        expected_at_least: usize,
        actual: usize,
    },
    #[error("trailing bytes after Arrow IPC body: expected {expected} bytes, got {actual}")]
    TrailingPayloadBytes { expected: usize, actual: usize },
    #[error("compressed Arrow IPC payloads are unsupported in page_arrow v1")]
    UnsupportedCompression,
    #[error("unsupported Arrow IPC metadata version: expected {expected:?}, got {actual:?}")]
    UnsupportedMetadataVersion {
        expected: MetadataVersion,
        actual: MetadataVersion,
    },
    #[error("record batch decoder returned no batch")]
    UnexpectedEmptyMessage,
    #[error("payload range overflow: offset {offset}, len {len}")]
    PayloadRangeOverflow { offset: usize, len: usize },
    #[error("payload range offset {offset}, len {len} is out of bounds for payload of length {payload_len}")]
    PayloadOutOfBounds {
        payload_len: usize,
        offset: usize,
        len: usize,
    },
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}
