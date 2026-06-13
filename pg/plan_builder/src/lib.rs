//! Backend-side builder for DataFusion logical plans with PostgreSQL scan leaves.
//!
//! `plan_builder` accepts an already typed DataFusion logical plan, runs the
//! pg_fusion post-frontend planning passes, and builds PostgreSQL table scans
//! as [`scan_node::PgScanNode`] leaves.
//!
//! The result contains no snapshot identity. Snapshot ownership stays in the
//! later backend execution state that serves scan requests.
//!
//! DataFusion logical optimization is pinned to one target partition by default.
//! This is a DataFusion-level contract only: PostgreSQL-side parallel plans can
//! still be produced later by `slot_scan`.
//!
//! Scalar subquery expressions can survive into scan building; scan leaves inside
//! their nested plans are still converted into `PgScanNode`s. Semi/anti subquery
//! forms remain unsupported unless DataFusion rewrites them before this phase.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::Column;
use datafusion_common::{
    DFSchema, DFSchemaRef, DataFusionError, Result as DataFusionResult, ScalarValue, TableReference,
};
use datafusion_expr::expr_fn::cast;
use datafusion_expr::logical_plan::{EmptyRelation, Filter, LogicalPlan, Projection, TableScan};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, JoinType};
use pgrx::pg_sys;
use scan_node::{PgCteId, PgCteRefNode, PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{compile_scan, CompileError, CompileScanInput, LimitLowering};
use thiserror::Error;

mod join_reorder;

pub use df_catalog::PgPlanningTableSource;
pub use join_reorder::{JoinStatsProvider, LiveJoinStatsProvider};

const SPECIAL_NUMERIC_ERROR: &str =
    "pg_fusion Decimal128 avg cannot represent PostgreSQL numeric NaN/Infinity values";

/// Output of a successful hybrid plan build.
#[derive(Debug)]
pub struct HybridPlan {
    /// Optimized logical plan with PostgreSQL table scans built as custom nodes.
    pub logical_plan: LogicalPlan,
    /// PostgreSQL scan plan referenced by custom scan leaves in `logical_plan`.
    pub scan_plan: PgScanPlan,
}

/// Query-local PostgreSQL scan plan built alongside a DataFusion logical plan.
#[derive(Debug)]
pub struct PgScanPlan {
    /// Query-local scan specs in allocation order.
    pub scans: Vec<Arc<PgScanSpec>>,
}

impl PgScanPlan {
    fn new(scans: Vec<Arc<PgScanSpec>>) -> Self {
        Self { scans }
    }

    pub fn scans(&self) -> &[Arc<PgScanSpec>] {
        &self.scans
    }

    pub fn into_scans(self) -> Vec<Arc<PgScanSpec>> {
        self.scans
    }
}

/// Build a pg_fusion execution plan from an already-typed DataFusion logical plan.
///
/// This is the shared post-frontend pipeline: it validates supported plan
/// shapes, runs DataFusion optimization, applies pg_fusion join ordering, turns
/// PostgreSQL table leaves into [`scan_node::PgScanNode`], and normalizes root
/// output transport types.
pub fn build_preplanned_logical_plan(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
) -> Result<HybridPlan, PlanBuildError> {
    build_preplanned_logical_plan_with_stats(plan, config, &LiveJoinStatsProvider)
}

/// [`build_preplanned_logical_plan`] with an explicit statistics provider.
pub fn build_preplanned_logical_plan_with_stats<S>(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
    stats_provider: &S,
) -> Result<HybridPlan, PlanBuildError>
where
    S: JoinStatsProvider,
{
    let mut scan_builder = PgScanBuilder::new(config);
    let logical_plan = build_preplanned_logical_plan_with_scan_builder(
        plan,
        config,
        stats_provider,
        &mut scan_builder,
    )?;
    Ok(HybridPlan {
        logical_plan,
        scan_plan: PgScanPlan::new(scan_builder.scans),
    })
}

/// Build a pg_fusion execution plan for PostgreSQL query-tree frontend output.
///
/// The frontend path intentionally builds PostgreSQL scan leaves without first
/// running generic DataFusion optimization except for analyzer rewrites that
/// are required to make the logical plan executable. Frontend scan predicates
/// carry PostgreSQL type metadata and must reach `scan_sql` before DataFusion
/// can fold or rewrite them with DataFusion semantics.
pub fn build_frontend_logical_plan(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
) -> Result<HybridPlan, PlanBuildError> {
    let mut scan_builder = PgScanBuilder::new(config);
    let logical_plan =
        build_frontend_logical_plan_with_scan_builder(plan, config, &mut scan_builder)?;
    reject_residual_frontend_filters(&scan_builder.scans)?;
    Ok(HybridPlan {
        logical_plan,
        scan_plan: PgScanPlan::new(scan_builder.scans),
    })
}

fn build_preplanned_logical_plan_with_scan_builder<S>(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
    stats_provider: &S,
    scan_builder: &mut PgScanBuilder,
) -> Result<LogicalPlan, PlanBuildError>
where
    S: JoinStatsProvider,
{
    validate_no_special_numeric_literals(&plan)?;
    let optimized = optimize_logical_plan(plan, config.target_partitions)?;
    validate_no_special_numeric_literals(&optimized)?;
    validate_supported_plan_shape(&optimized)?;
    let optimized = if config.join_reordering_enabled {
        join_reorder::rewrite_join_order(optimized, config, stats_provider)?
    } else {
        optimized
    };

    let logical_plan = scan_builder.build_scans(optimized)?;
    normalize_root_output_types(logical_plan).map_err(PlanBuildError::from)
}

fn build_frontend_logical_plan_with_scan_builder(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
    scan_builder: &mut PgScanBuilder,
) -> Result<LogicalPlan, PlanBuildError> {
    validate_no_special_numeric_literals(&plan)?;
    let frontend_schema = Arc::clone(plan.schema());
    let plan = if plan_requires_frontend_analysis(&plan)? {
        let optimized = optimize_logical_plan(plan, config.target_partitions)?;
        validate_no_special_numeric_literals(&optimized)?;
        optimized
    } else {
        plan
    };
    validate_supported_plan_shape(&plan)?;
    let logical_plan = scan_builder.build_scans(plan)?;
    let logical_plan = normalize_root_output_types(logical_plan)?;
    Ok(restore_empty_root_schema(logical_plan, &frontend_schema))
}

fn plan_requires_frontend_analysis(plan: &LogicalPlan) -> Result<bool, DataFusionError> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        if frontend_plan_node_requires_analysis(node) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if frontend_expr_requires_analysis(expr) {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
        })?;
        if found {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}

fn frontend_plan_node_requires_analysis(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Distinct(_) | LogicalPlan::Union(_) => true,
        LogicalPlan::Join(join) => {
            join.join_type == JoinType::Inner && join.on.is_empty() && join.filter.is_none()
        }
        _ => false,
    }
}

fn frontend_expr_requires_analysis(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::GroupingSet(_) => true,
        Expr::AggregateFunction(function) => function.func.name() == "grouping",
        _ => false,
    }
}

fn reject_residual_frontend_filters(scans: &[Arc<PgScanSpec>]) -> Result<(), PlanBuildError> {
    let count = scans
        .iter()
        .map(|scan| scan.compiled_scan.residual_filters.len())
        .sum::<usize>();
    if count == 0 {
        Ok(())
    } else {
        Err(PlanBuildError::Plan(format!(
            "pg_frontend v1 requires all WHERE filters to execute inside PostgreSQL scan SQL; {count} residual filter(s) would execute in DataFusion"
        )))
    }
}

/// Configuration for pg_fusion logical-plan building.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanBuilderConfig {
    /// DataFusion optimizer target partitions. v1 keeps this at one unless
    /// explicitly overridden by tests or future integration code.
    pub target_partitions: usize,
    /// Live PostgreSQL identifier byte limit for this backend.
    pub identifier_max_bytes: usize,
    /// First query-local scan id to allocate.
    pub first_scan_id: u64,
    /// Enable statistics-based join reordering for eligible inner/cross joins.
    pub join_reordering_enabled: bool,
    /// Search limits and cross-join policy for the compact join-order optimizer.
    pub join_order_config: join_order::JoinOrderConfig,
}

impl Default for PlanBuilderConfig {
    fn default() -> Self {
        let join_order_config = join_order::JoinOrderConfig {
            allow_cross_joins: true,
            ..join_order::JoinOrderConfig::default()
        };
        Self {
            target_partitions: 1,
            identifier_max_bytes: pg_identifier_max_bytes(),
            first_scan_id: 1,
            join_reordering_enabled: true,
            join_order_config,
        }
    }
}

/// Build errors for logical planning and PostgreSQL scan building.
#[derive(Debug, Error)]
pub enum PlanBuildError {
    #[error("DataFusion planning failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("PostgreSQL scan SQL compilation failed: {0}")]
    ScanSql(#[from] CompileError),
    #[error("PostgreSQL statistics failed: {0}")]
    Statistics(String),
    #[error("join order optimization failed: {0}")]
    JoinOrder(#[from] join_order::OptimizeError),
    #[error("join order rewrite failed: {0}")]
    JoinReorder(String),
    #[error("unsupported logical plan shape for PostgreSQL scan planning: {0}")]
    UnsupportedSubquery(String),
    #[error("{0}")]
    Plan(String),
}

fn optimize_logical_plan(
    plan: LogicalPlan,
    target_partitions: usize,
) -> Result<LogicalPlan, DataFusionError> {
    let mut options = ConfigOptions::default();
    options.execution.target_partitions = target_partitions;
    let mut state = SessionStateBuilder::new()
        .with_config(options.into())
        .with_optimizer_rules(pg_fusion_optimizer_rules())
        .build();
    let _ = state.register_udf(df_functions::pg_format_udf());
    let _ = state.register_udf(df_functions::pg_int_add_checked_udf());
    let _ = state.register_udf(df_functions::pg_int_sub_checked_udf());
    let _ = state.register_udf(df_functions::pg_int_mul_checked_udf());
    let _ = state.register_udf(df_functions::pg_interval_out_udf());
    let _ = state.register_udf(df_functions::pg_numeric_round_scale_udf());
    let _ = state.register_udf(df_functions::pg_numeric_trunc_scale_udf());
    let _ = state.register_udf(df_functions::pg_regex_match_udf());
    let _ = state.register_udf(df_functions::pg_regex_not_match_udf());
    let _ = state.register_udf(df_functions::pg_varchar_typmod_udf());
    let _ = state.register_udf(df_functions::pg_bpchar_typmod_udf());
    let _ = state.register_udf(df_functions::pg_bpchar_cmp_key_udf());
    let _ = state.register_udf(df_functions::pg_bpchar_length_udf());
    let _ = state.register_udf(df_functions::pg_quote_literal_udf());
    let _ = state.register_udaf(df_functions::pg_avg_udaf());
    let _ = state.register_udaf(df_functions::pg_scalar_subquery_value_udaf());
    let _ = state.register_udaf(datafusion::functions_aggregate::first_last::first_value_udaf());
    let _ = state.register_udaf(datafusion::functions_aggregate::grouping::grouping_udaf());
    state.optimize(&plan)
}

fn pg_fusion_optimizer_rules() -> Vec<Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync>> {
    datafusion::optimizer::Optimizer::new()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != "single_distinct_aggregation_to_group_by")
        .collect()
}

fn normalize_root_output_types(plan: LogicalPlan) -> DataFusionResult<LogicalPlan> {
    let fields = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| {
            (
                qualifier.cloned(),
                field.name().to_owned(),
                field.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let columns = plan.schema().columns();

    let mut changed = false;
    let expr = columns
        .into_iter()
        .zip(fields)
        .map(|(column, (qualifier, name, data_type))| {
            let expr = Expr::Column(column);
            match data_type {
                DataType::UInt64 => {
                    changed = true;
                    cast(expr, DataType::Int64).alias_qualified(qualifier, name)
                }
                DataType::LargeUtf8 => {
                    changed = true;
                    cast(expr, DataType::Utf8).alias_qualified(qualifier, name)
                }
                _ => expr,
            }
        })
        .collect::<Vec<_>>();

    if !changed {
        return Ok(plan);
    }

    Projection::try_new(expr, Arc::new(plan)).map(LogicalPlan::Projection)
}

fn restore_empty_root_schema(plan: LogicalPlan, expected_schema: &DFSchemaRef) -> LogicalPlan {
    match plan {
        LogicalPlan::EmptyRelation(empty)
            if !empty.produce_one_row
                && empty.schema.fields().is_empty()
                && !expected_schema.fields().is_empty() =>
        {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(expected_schema),
            })
        }
        other => other,
    }
}

fn pg_identifier_max_bytes() -> usize {
    (pg_sys::NAMEDATALEN as usize).saturating_sub(1)
}

fn validate_no_special_numeric_literals(plan: &LogicalPlan) -> Result<(), PlanBuildError> {
    let special_string_columns = special_numeric_string_columns(plan);
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }

        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if casts_special_numeric_literal(expr)
                    || casts_special_numeric_column(expr, &special_string_columns)
                {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
        })?;

        if found {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;

    if found {
        Err(PlanBuildError::Plan(SPECIAL_NUMERIC_ERROR.to_owned()))
    } else {
        Ok(())
    }
}

fn special_numeric_string_columns(plan: &LogicalPlan) -> HashSet<Column> {
    match plan {
        LogicalPlan::Values(values) => {
            let mut columns = HashSet::new();
            for (column_index, field) in values.schema.fields().iter().enumerate() {
                if values.values.iter().any(|row| {
                    row.get(column_index)
                        .is_some_and(string_literal_is_special_numeric)
                }) {
                    columns.insert(Column::new_unqualified(field.name().as_str()));
                }
            }
            columns
        }
        LogicalPlan::Projection(projection) => {
            let input_columns = special_numeric_string_columns(&projection.input);
            projection
                .expr
                .iter()
                .zip(projection.schema.fields())
                .filter(|(expr, _field)| {
                    expr_contains_special_numeric_string_column(expr, &input_columns)
                })
                .map(|(_expr, field)| Column::new_unqualified(field.name().as_str()))
                .collect()
        }
        LogicalPlan::SubqueryAlias(alias) => special_numeric_string_columns(&alias.input)
            .into_iter()
            .map(|column| Column::new(Some(alias.alias.clone()), column.name))
            .collect(),
        LogicalPlan::Filter(filter) => special_numeric_string_columns(&filter.input),
        LogicalPlan::Repartition(repartition) => special_numeric_string_columns(&repartition.input),
        LogicalPlan::Sort(sort) => special_numeric_string_columns(&sort.input),
        LogicalPlan::Limit(limit) => special_numeric_string_columns(&limit.input),
        LogicalPlan::Join(join) => {
            let mut columns = special_numeric_string_columns(&join.left);
            columns.extend(special_numeric_string_columns(&join.right));
            columns
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .flat_map(|input| special_numeric_string_columns(input))
            .collect(),
        _ => plan
            .inputs()
            .into_iter()
            .flat_map(special_numeric_string_columns)
            .collect(),
    }
}

fn casts_special_numeric_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Cast(cast) => {
            is_decimal_type(&cast.data_type) && string_literal_is_special_numeric(&cast.expr)
        }
        Expr::TryCast(cast) => {
            is_decimal_type(&cast.data_type) && string_literal_is_special_numeric(&cast.expr)
        }
        _ => false,
    }
}

fn casts_special_numeric_column(expr: &Expr, special_string_columns: &HashSet<Column>) -> bool {
    match expr {
        Expr::Cast(cast) => {
            is_decimal_type(&cast.data_type)
                && expr_contains_special_numeric_string_column(&cast.expr, special_string_columns)
        }
        Expr::TryCast(cast) => {
            is_decimal_type(&cast.data_type)
                && expr_contains_special_numeric_string_column(&cast.expr, special_string_columns)
        }
        _ => false,
    }
}

fn expr_contains_special_numeric_string_column(
    expr: &Expr,
    special_string_columns: &HashSet<Column>,
) -> bool {
    let mut found = false;
    let _ = expr.apply(|expr| {
        if string_literal_is_special_numeric(expr) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Expr::Column(column) = expr {
            if column_matches_special_numeric_string(column, special_string_columns) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn column_matches_special_numeric_string(
    column: &Column,
    special_string_columns: &HashSet<Column>,
) -> bool {
    special_string_columns.contains(column)
        || special_string_columns.contains(&Column::new_unqualified(&column.name))
}

fn is_decimal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
}

fn string_literal_is_special_numeric(expr: &Expr) -> bool {
    let value = match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => value,
        _ => return false,
    };
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "nan" | "inf" | "+inf" | "-inf" | "infinity" | "+infinity" | "-infinity"
    )
}

#[derive(Debug, Error)]
enum UnsupportedSubqueryShape {
    #[error("scalar subqueries")]
    ScalarSubquery,
    #[error("EXISTS(...) expressions")]
    Exists,
    #[error("IN (SELECT ...) expressions")]
    InSubquery,
    #[error("set-comparison subqueries")]
    SetComparison,
    #[error("correlated subqueries")]
    OuterReferenceColumn,
}

fn validate_supported_plan_shape(plan: &LogicalPlan) -> Result<(), PlanBuildError> {
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| match expr {
                Expr::ScalarSubquery(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::ScalarSubquery,
                ))),
                Expr::Exists(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::Exists,
                ))),
                Expr::InSubquery(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::InSubquery,
                ))),
                Expr::SetComparison(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::SetComparison,
                ))),
                Expr::OuterReferenceColumn(_, _) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::OuterReferenceColumn,
                ))),
                _ => Ok(TreeNodeRecursion::Continue),
            })
        })
    })
    .map_err(map_subquery_validation_error)?;
    Ok(())
}

fn map_subquery_validation_error(error: DataFusionError) -> PlanBuildError {
    match recover_subquery_validation_error(error) {
        Ok(shape) => PlanBuildError::UnsupportedSubquery(shape.to_string()),
        Err(error) => PlanBuildError::DataFusion(error),
    }
}

fn recover_subquery_validation_error(
    error: DataFusionError,
) -> Result<UnsupportedSubqueryShape, DataFusionError> {
    match error {
        DataFusionError::External(source) => match source.downcast::<UnsupportedSubqueryShape>() {
            Ok(shape) => Ok(*shape),
            Err(source) => Err(DataFusionError::External(source)),
        },
        DataFusionError::Context(context, source) => {
            match recover_subquery_validation_error(*source) {
                Ok(shape) => Ok(shape),
                Err(source) => Err(DataFusionError::Context(context, Box::new(source))),
            }
        }
        other => Err(other),
    }
}

#[derive(Debug)]
struct PgScanBuilder {
    config: PlanBuilderConfig,
    next_scan_id: u64,
    scans: Vec<Arc<PgScanSpec>>,
}

impl PgScanBuilder {
    fn new(config: PlanBuilderConfig) -> Self {
        Self {
            next_scan_id: config.first_scan_id,
            config,
            scans: Vec::new(),
        }
    }

    fn build_scans(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, PlanBuildError> {
        let transformed = plan.transform_up_with_subqueries(|node| self.build_node(node))?;
        let logical_plan = deduplicate_cte_inputs(transformed.data)?;
        self.retain_scans_used_by(&logical_plan)?;
        Ok(logical_plan)
    }

    fn build_node(&mut self, plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::TableScan(table_scan) => {
                self.build_table_scan(table_scan).map(Transformed::yes)
            }
            other => Ok(Transformed::no(other)),
        }
    }

    fn build_table_scan(&mut self, table_scan: TableScan) -> DataFusionResult<LogicalPlan> {
        let source = table_scan
            .source
            .as_any()
            .downcast_ref::<PgPlanningTableSource>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "TableScan {} is not backed by pg/plan_builder catalog source",
                    table_scan.table_name
                ))
            })?;
        let resolved = source.resolved();
        let source_schema = DFSchema::try_from_qualified_schema(
            table_scan.table_name.clone(),
            resolved.schema.as_ref(),
        )?;
        let scan_relation =
            scan_relation_for_table_scan(&resolved.relation, &table_scan.table_name);

        let compiled = compile_scan(CompileScanInput {
            relation: &scan_relation,
            schema: resolved.schema.as_ref(),
            identifier_max_bytes: self.config.identifier_max_bytes,
            projection: table_scan.projection.as_deref(),
            filters: &table_scan.filters,
            requested_limit: table_scan.fetch,
            limit_lowering: LimitLowering::ExternalHint,
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let residual_filters = compiled.residual_filters.clone();
        let selected_output_len = compiled.selected_columns.len();
        let needs_output_projection = !compiled.residual_filter_columns.is_empty();
        let scan_id = self.allocate_scan_id()?;
        let spec = Arc::new(PgScanSpec::try_new(
            scan_id,
            resolved.table_oid,
            resolved.relation.clone(),
            &source_schema,
            compiled,
        )?);
        let mut plan = PgScanNode::new(Arc::clone(&spec)).into_logical_plan();
        self.scans.push(spec);

        if let Some(predicate) = conjunction(residual_filters) {
            plan = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(plan))?);
        }

        if needs_output_projection {
            let expr = (0..selected_output_len)
                .map(|index| {
                    let (qualifier, field) = plan.schema().qualified_field(index);
                    Expr::Column(datafusion_common::Column::from((qualifier, field)))
                })
                .collect::<Vec<_>>();
            plan = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(plan))?);
        }

        Ok(plan)
    }

    fn allocate_scan_id(&mut self) -> DataFusionResult<PgScanId> {
        let scan_id = self.next_scan_id;
        self.next_scan_id = self
            .next_scan_id
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Plan("PgScanId counter overflowed".into()))?;
        Ok(PgScanId::new(scan_id))
    }

    fn retain_scans_used_by(&mut self, plan: &LogicalPlan) -> DataFusionResult<()> {
        let mut used = HashSet::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node {
                if let Some(pg_scan) = extension.node.as_any().downcast_ref::<PgScanNode>() {
                    used.insert(pg_scan.spec().scan_id);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        self.scans.retain(|scan| used.contains(&scan.scan_id));
        Ok(())
    }
}

fn deduplicate_cte_inputs(plan: LogicalPlan) -> DataFusionResult<LogicalPlan> {
    let mut inputs = HashMap::<PgCteId, LogicalPlan>::new();
    let transformed = plan.transform_up(|node| {
        let LogicalPlan::Extension(extension) = &node else {
            return Ok(Transformed::no(node));
        };
        let Some(cte_ref) = extension.node.as_any().downcast_ref::<PgCteRefNode>() else {
            return Ok(Transformed::no(node));
        };

        if let Some(input) = inputs.get(&cte_ref.cte_id()) {
            return Ok(Transformed::yes(
                PgCteRefNode::new(
                    cte_ref.cte_id(),
                    cte_ref.name().to_owned(),
                    input.clone(),
                    Arc::clone(cte_ref.schema()),
                    cte_ref.projection().map(|projection| projection.to_vec()),
                    cte_ref.fetch(),
                )
                .into_logical_plan(),
            ));
        }

        inputs.insert(cte_ref.cte_id(), cte_ref.input().clone());
        Ok(Transformed::no(node))
    })?;
    Ok(transformed.data)
}

fn scan_relation_for_table_scan(
    relation: &scan_sql::PgRelation,
    table_name: &TableReference,
) -> scan_sql::PgRelation {
    match table_name {
        TableReference::Bare { table } if table.as_ref() != relation.table => {
            relation.clone().with_alias(table.as_ref())
        }
        _ => relation.clone(),
    }
}

#[cfg(test)]
mod tests;
