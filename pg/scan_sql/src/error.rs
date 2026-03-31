use thiserror::Error;

/// Errors returned when a scan input is structurally invalid for PostgreSQL SQL compilation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompileError {
    #[error("projection index {index} is out of range for schema with {schema_len} fields")]
    InvalidProjectionIndex { index: usize, schema_len: usize },

    #[error("column `{column}` was not found in the provided schema")]
    UnknownColumn { column: String },

    #[error(
        "column `{column}` refers to relation `{relation}`, but the scan targets `{expected}`"
    )]
    UnexpectedRelation {
        column: String,
        relation: String,
        expected: String,
    },
}
