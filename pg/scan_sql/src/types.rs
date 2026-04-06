use std::collections::BTreeSet;

use arrow_schema::Schema;
use datafusion_common::TableReference;
use datafusion_expr::Expr;

use crate::error::CompileError;
use crate::identifier::validate_identifier;
use crate::quote::quote_identifier;

/// A PostgreSQL relation reference limited to `schema.table` addressing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRelation {
    pub schema: Option<String>,
    pub table: String,
}

impl PgRelation {
    pub fn new(schema: Option<impl Into<String>>, table: impl Into<String>) -> Self {
        Self {
            schema: schema.map(Into::into),
            table: table.into(),
        }
    }

    pub(crate) fn matches_reference(
        &self,
        relation: &TableReference,
        identifier_max_bytes: usize,
    ) -> Result<bool, CompileError> {
        self.validate(identifier_max_bytes)?;
        match relation {
            TableReference::Bare { table } => {
                validate_identifier(table.as_ref(), identifier_max_bytes, "table")?;
                Ok(table.as_ref() == self.table)
            }
            TableReference::Partial { schema, table }
            | TableReference::Full { schema, table, .. } => {
                validate_identifier(schema.as_ref(), identifier_max_bytes, "schema")?;
                validate_identifier(table.as_ref(), identifier_max_bytes, "table")?;
                Ok(self.schema.as_deref().is_some_and(|expected| {
                    expected == schema.as_ref() && self.table == table.as_ref()
                }))
            }
        }
    }

    pub(crate) fn validate(&self, identifier_max_bytes: usize) -> Result<(), CompileError> {
        if let Some(schema) = &self.schema {
            validate_identifier(schema, identifier_max_bytes, "schema")?;
        }
        validate_identifier(&self.table, identifier_max_bytes, "table")
    }

    pub(crate) fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.table),
            None => self.table.clone(),
        }
    }

    pub(crate) fn render_sql(&self) -> String {
        match &self.schema {
            Some(schema) => {
                format!(
                    "{}.{}",
                    quote_identifier(schema),
                    quote_identifier(&self.table)
                )
            }
            None => quote_identifier(&self.table),
        }
    }
}

/// Input for compiling a PostgreSQL base-table scan.
#[derive(Debug, Clone)]
pub struct CompileScanInput<'a> {
    pub relation: &'a PgRelation,
    pub schema: &'a Schema,
    /// Live PostgreSQL identifier byte limit for the backend that produced this schema.
    ///
    /// Callers should pass `pg_sys::NAMEDATALEN as usize - 1` from the linked
    /// PostgreSQL build so `scan_sql` rejects overlong schemas, relations, and
    /// columns consistently with the backend planner contract.
    pub identifier_max_bytes: usize,
    pub projection: Option<&'a [usize]>,
    pub filters: &'a [Expr],
    pub requested_limit: Option<usize>,
    pub limit_lowering: LimitLowering,
}

/// Controls how a requested DataFusion fetch/limit hint is lowered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LimitLowering {
    /// Keep the limit as metadata for a downstream runtime such as `slot_scan`.
    ///
    /// This is the default because `TableProvider::scan(limit)` in DataFusion
    /// is a hint, not an exact global `LIMIT` contract.
    #[default]
    ExternalHint,
    /// Render the requested limit directly as a PostgreSQL `LIMIT` clause.
    ///
    /// This is intended for consumers that explicitly want SQL-level limit
    /// semantics and should not be used for the default `scan_sql -> slot_scan`
    /// path.
    SqlClause,
}

/// A filter from the original input that was successfully compiled into PostgreSQL SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledFilter {
    pub original_index: usize,
    pub sql: String,
}

/// Result of compiling a base-table scan into PostgreSQL SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledScan {
    pub sql: String,
    pub requested_limit: Option<usize>,
    pub sql_limit: Option<usize>,
    pub selected_columns: Vec<usize>,
    pub output_columns: Vec<usize>,
    pub filter_only_columns: Vec<usize>,
    pub residual_filter_columns: Vec<usize>,
    pub pushed_filters: Vec<CompiledFilter>,
    pub residual_filters: Vec<Expr>,
    /// True when every input filter compiled into PostgreSQL SQL.
    ///
    /// This flag does not claim semantic equivalence with DataFusion. It only
    /// means the entire filter set was accepted by the PostgreSQL SQL compiler.
    pub all_filters_compiled: bool,
    pub uses_dummy_projection: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RenderedExpr {
    pub(crate) sql: String,
    pub(crate) referenced_columns: BTreeSet<usize>,
}

impl RenderedExpr {
    pub(crate) fn new(sql: String) -> Self {
        Self {
            sql,
            referenced_columns: BTreeSet::new(),
        }
    }

    pub(crate) fn from_column(sql: String, column_index: usize) -> Self {
        let mut referenced_columns = BTreeSet::new();
        referenced_columns.insert(column_index);
        Self {
            sql,
            referenced_columns,
        }
    }

    pub(crate) fn merge(sql: String, parts: impl IntoIterator<Item = RenderedExpr>) -> Self {
        let mut referenced_columns = BTreeSet::new();
        for part in parts {
            referenced_columns.extend(part.referenced_columns);
        }
        Self {
            sql,
            referenced_columns,
        }
    }
}
