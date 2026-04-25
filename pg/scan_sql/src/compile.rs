use std::collections::BTreeSet;

use arrow_schema::Schema;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::{Expr, Operator};

use crate::error::CompileError;
use crate::identifier::validate_identifier;
use crate::quote::quote_identifier;
use crate::render::{render_column, render_expr};
use crate::types::{CompileScanInput, CompiledFilter, CompiledScan, LimitLowering, PgRelation};

const DUMMY_PROJECTION_ALIAS: &str = "__pg_fusion_scan_dummy";

/// Compile a single base-table DataFusion scan input into PostgreSQL SQL.
///
/// Anything that compiles is expected to run with PostgreSQL semantics. Any
/// residual filters remain the caller's responsibility above the scan.
pub fn compile_scan(input: CompileScanInput<'_>) -> Result<CompiledScan, CompileError> {
    validate_scan_identifiers(input.schema, input.relation, input.identifier_max_bytes)?;
    let selected_columns = validate_projection(input.schema, input.projection)?;
    let mut pushed_filters = Vec::new();
    let mut residual_filters = Vec::new();
    let mut pushed_columns = BTreeSet::new();
    let mut residual_columns = BTreeSet::new();

    for (filter_index, filter) in input.filters.iter().enumerate() {
        for conjunct in split_top_level_and(filter) {
            match render_expr(
                conjunct,
                input.schema,
                input.relation,
                input.identifier_max_bytes,
            )? {
                Some(rendered) => {
                    pushed_columns.extend(rendered.referenced_columns.iter().copied());
                    pushed_filters.push(CompiledFilter {
                        original_index: filter_index,
                        sql: rendered.sql,
                    });
                }
                None => {
                    residual_columns.extend(collect_filter_columns(
                        conjunct,
                        input.schema,
                        input.relation,
                        input.identifier_max_bytes,
                    )?);
                    residual_filters.push(conjunct.clone());
                }
            }
        }
    }

    let residual_filter_columns = residual_columns
        .into_iter()
        .filter(|index| !selected_columns.contains(index))
        .collect::<Vec<_>>();
    let output_columns = selected_columns
        .iter()
        .copied()
        .chain(residual_filter_columns.iter().copied())
        .collect::<Vec<_>>();
    let uses_dummy_projection = output_columns.is_empty();
    let select_list = if uses_dummy_projection {
        format!(
            "NULL::boolean AS {}",
            quote_identifier(DUMMY_PROJECTION_ALIAS)
        )
    } else {
        output_columns
            .iter()
            .map(|&index| quote_identifier(input.schema.field(index).name()))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let filter_only_columns = pushed_columns
        .into_iter()
        .filter(|index| !output_columns.contains(index))
        .collect::<Vec<_>>();
    let all_filters_compiled = residual_filters.is_empty();

    let requested_limit = input.requested_limit;
    let sql_limit = match input.limit_lowering {
        LimitLowering::ExternalHint => None,
        LimitLowering::SqlClause => requested_limit,
    };

    let mut sql = format!("SELECT {select_list} FROM {}", input.relation.render_sql());
    if !pushed_filters.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(
            &pushed_filters
                .iter()
                .map(|filter| filter.sql.as_str())
                .collect::<Vec<_>>()
                .join(" AND "),
        );
    }
    if let Some(limit) = sql_limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }

    Ok(CompiledScan {
        sql,
        requested_limit,
        sql_limit,
        selected_columns,
        output_columns,
        filter_only_columns,
        residual_filter_columns,
        pushed_filters,
        residual_filters,
        all_filters_compiled,
        uses_dummy_projection,
    })
}

/// Render the same pushed PostgreSQL scan predicates without narrowing the
/// relation projection.
///
/// This is intended for runtimes that can receive a raw base-relation slot and
/// apply the logical projection themselves while still using the same pushed
/// filters and optional SQL-level limit as [`compile_scan`].
pub fn render_unprojected_scan_sql(relation: &PgRelation, scan: &CompiledScan) -> String {
    let mut sql = format!("SELECT * FROM {}", relation.render_sql());
    if !scan.pushed_filters.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(
            &scan
                .pushed_filters
                .iter()
                .map(|filter| filter.sql.as_str())
                .collect::<Vec<_>>()
                .join(" AND "),
        );
    }
    if let Some(limit) = scan.sql_limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }
    sql
}

fn validate_scan_identifiers(
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<(), CompileError> {
    relation.validate(identifier_max_bytes)?;
    for field in schema.fields() {
        validate_identifier(field.name(), identifier_max_bytes, "column")?;
    }
    Ok(())
}

fn validate_projection(
    schema: &Schema,
    projection: Option<&[usize]>,
) -> Result<Vec<usize>, CompileError> {
    let fields_len = schema.fields().len();
    let projection = projection.map_or_else(
        || (0..fields_len).collect::<Vec<_>>(),
        |projection| projection.to_vec(),
    );

    for &index in &projection {
        if index >= fields_len {
            return Err(CompileError::InvalidProjectionIndex {
                index,
                schema_len: fields_len,
            });
        }
    }

    Ok(projection)
}

fn split_top_level_and(expr: &Expr) -> Vec<&Expr> {
    let mut conjuncts = Vec::new();
    split_top_level_and_inner(expr, &mut conjuncts);
    conjuncts
}

fn split_top_level_and_inner<'a>(expr: &'a Expr, conjuncts: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Alias(alias) => split_top_level_and_inner(&alias.expr, conjuncts),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            split_top_level_and_inner(left, conjuncts);
            split_top_level_and_inner(right, conjuncts);
        }
        _ => conjuncts.push(expr),
    }
}

fn collect_filter_columns(
    expr: &Expr,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<BTreeSet<usize>, CompileError> {
    let mut columns = BTreeSet::new();
    let mut error = None;
    expr.apply(|node| {
        if error.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Expr::Column(column) = node {
            match render_column(column, schema, relation, identifier_max_bytes) {
                Ok(rendered) => columns.extend(rendered.referenced_columns),
                Err(err) => {
                    error = Some(err);
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("column collection traversal is infallible");
    match error {
        Some(err) => Err(err),
        None => Ok(columns),
    }
}
