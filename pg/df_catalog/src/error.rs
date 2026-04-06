use thiserror::Error;

/// Errors returned by the live PostgreSQL catalog resolver.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResolveError {
    #[error("catalog-qualified table references are not supported")]
    FullReferenceUnsupported,
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error(
        "table not found: {table}{schema_suffix}",
        schema_suffix = .schema.as_deref().map(|s| format!(" in schema {s}")).unwrap_or_default()
    )]
    TableNotFound {
        schema: Option<String>,
        table: String,
    },
    #[error("unsupported relation kind: {0}")]
    UnsupportedRelationKind(char),
    #[error("unsupported PostgreSQL type oid {type_oid} for column {column}")]
    UnsupportedType { column: String, type_oid: u32 },
    #[error("{kind} identifier `{identifier}` exceeds PostgreSQL limit of {max_bytes} bytes")]
    OverlongIdentifier {
        kind: &'static str,
        identifier: String,
        max_bytes: usize,
    },
    #[error("invalid {0} identifier")]
    InvalidIdentifier(&'static str),
    #[error("{0}")]
    Postgres(String),
}
