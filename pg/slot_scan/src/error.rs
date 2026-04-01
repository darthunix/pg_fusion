use thiserror::Error;

/// Errors returned by scan preparation or execution.
#[derive(Debug, Error)]
pub enum ScanError {
    /// The SQL string contains an interior NUL byte and cannot be passed to
    /// PostgreSQL C APIs.
    #[error("scan SQL contains an interior nul byte")]
    InvalidSql,
    /// `slot_scan` only accepts a single SQL statement.
    #[error("expected exactly one SQL statement")]
    MultipleStatements,
    /// The opened cursor did not expose a result descriptor.
    #[error("failed to initialize scan result tuple descriptor")]
    MissingTupleDesc,
    /// PostgreSQL produced a plan shape outside the narrow scan-only contract.
    #[error("unsupported plan shape: {0}")]
    UnsupportedPlan(String),
    /// PostgreSQL raised an error while parsing, planning, or executing the
    /// scan.
    #[error("PostgreSQL error: {0}")]
    Postgres(String),
    /// The user-provided slot sink returned an error.
    #[error("slot sink error: {0}")]
    Sink(#[from] SinkError),
}

/// Error returned by user-provided slot sink callbacks.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct SinkError {
    message: String,
}

impl SinkError {
    /// Creates a new sink error with the provided message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
