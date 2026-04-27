//! Backend-side builder for DataFusion logical plans with PostgreSQL scan leaves.
//!
//! `plan_builder` accepts SQL plus DataFusion scalar parameters, resolves table
//! metadata through `df_catalog`, runs DataFusion logical optimization, and
//! lowers PostgreSQL table scans into [`scan_node::PgScanNode`].
//!
//! The result contains no snapshot identity. Snapshot ownership stays in the
//! later backend execution state that serves scan requests.
//!
//! DataFusion logical optimization is pinned to one target partition by default.
//! This is a DataFusion-level contract only: PostgreSQL-side parallel plans can
//! still be produced later by `slot_scan`.
//!
//! Subquery expressions are accepted when DataFusion can decorrelate/rewrite
//! them into ordinary relational operators before scan lowering. Any subquery
//! nodes that survive optimization are rejected before `PgScanNode` lowering.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::SessionStateDefaults;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::TableReference;
use datafusion_common::{DFSchema, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::logical_plan::{Filter, LogicalPlan, Projection, TableScan};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    AggregateUDF, Expr, ScalarUDF, TableProviderFilterPushDown, TableSource, WindowUDF,
};
use datafusion_sql::parser::{DFParser, Statement as DFStatement};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::ast::Statement as SqlStatement;
use datafusion_sql::sqlparser::parser::ParserError;
use df_catalog::{CatalogResolver, PgrxCatalogResolver, ResolveError, ResolvedTable};
use once_cell::sync::Lazy;
use pgrx::pg_sys;
use scan_node::{PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{compile_scan, CompileError, CompileScanInput, LimitLowering};
use thiserror::Error;

static BUILTINS: Lazy<Arc<Builtins>> = Lazy::new(|| Arc::new(Builtins::new()));

/// Input for one backend logical-plan build.
#[derive(Debug, Clone)]
pub struct PlanBuildInput<'a> {
    /// SQL text. v1 accepts exactly one query-shaped statement.
    pub sql: &'a str,
    /// Positional DataFusion parameter values for `$1`, `$2`, ...
    pub params: Vec<ScalarValue>,
}

/// Output of a successful plan build.
#[derive(Debug)]
pub struct BuiltPlan {
    /// Optimized logical plan with PostgreSQL table scans lowered to custom nodes.
    pub logical_plan: LogicalPlan,
    /// Query-local scan specs in allocation order.
    pub scans: Vec<Arc<PgScanSpec>>,
}

/// Configuration for [`PlanBuilder`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanBuilderConfig {
    /// DataFusion optimizer target partitions. v1 keeps this at one unless
    /// explicitly overridden by tests or future integration code.
    pub target_partitions: usize,
    /// Live PostgreSQL identifier byte limit for this backend.
    pub identifier_max_bytes: usize,
    /// First query-local scan id to allocate.
    pub first_scan_id: u64,
}

impl Default for PlanBuilderConfig {
    fn default() -> Self {
        Self {
            target_partitions: 1,
            identifier_max_bytes: pg_identifier_max_bytes(),
            first_scan_id: 1,
        }
    }
}

/// Build errors for SQL planning and PostgreSQL scan lowering.
#[derive(Debug, Error)]
pub enum PlanBuildError {
    #[error("failed to parse SQL: {0}")]
    Parse(#[from] ParserError),
    #[error("expected exactly one SQL statement, got {count}")]
    MultipleStatements { count: usize },
    #[error("unsupported statement for PostgreSQL scan planning: {0}")]
    UnsupportedStatement(String),
    #[error("catalog resolution failed: {0}")]
    Catalog(#[from] ResolveError),
    #[error("DataFusion planning failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("PostgreSQL scan SQL compilation failed: {0}")]
    ScanSql(#[from] CompileError),
    #[error("unsupported SQL shape for PostgreSQL scan planning: {0}")]
    UnsupportedSubquery(String),
    #[error("{0}")]
    Plan(String),
}

/// Backend-side SQL-to-logical-plan builder.
#[derive(Debug, Clone)]
pub struct PlanBuilder<R = PgrxCatalogResolver> {
    resolver: R,
    config: PlanBuilderConfig,
}

impl PlanBuilder<PgrxCatalogResolver> {
    /// Create a builder backed by live PostgreSQL catalogs.
    pub fn new() -> Self {
        Self::with_resolver(PgrxCatalogResolver::new())
    }
}

impl Default for PlanBuilder<PgrxCatalogResolver> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> PlanBuilder<R> {
    /// Create a builder with a custom catalog resolver.
    pub fn with_resolver(resolver: R) -> Self {
        Self {
            resolver,
            config: PlanBuilderConfig::default(),
        }
    }

    /// Override the default builder configuration.
    pub fn with_config(mut self, config: PlanBuilderConfig) -> Self {
        self.config = config;
        self
    }

    /// Return the effective configuration.
    pub fn config(&self) -> PlanBuilderConfig {
        self.config
    }
}

impl<R> PlanBuilder<R>
where
    R: CatalogResolver + Send + Sync,
{
    /// Build an optimized logical plan and lower PostgreSQL table scans.
    pub fn build(&self, input: PlanBuildInput<'_>) -> Result<BuiltPlan, PlanBuildError> {
        let statement = parse_one_query(input.sql)?;
        let context = PgPlanningContext::new(&self.resolver, self.config);
        let planner = SqlToRel::new(&context);
        let plan = planner.statement_to_plan(statement)?;
        let plan = plan.with_param_values(input.params)?;
        let optimized = optimize_logical_plan(plan, self.config.target_partitions)?;
        validate_supported_plan_shape(&optimized)?;

        let mut lowerer = ScanLowerer::new(self.config);
        let logical_plan = lowerer.lower(optimized)?;
        Ok(BuiltPlan {
            logical_plan,
            scans: lowerer.scans,
        })
    }
}

fn parse_one_query(sql: &str) -> Result<DFStatement, PlanBuildError> {
    let mut statements = DFParser::parse_sql(sql)?;
    let count = statements.len();
    if count != 1 {
        return Err(PlanBuildError::MultipleStatements { count });
    }
    let statement = statements.pop_front().expect("checked count above");
    ensure_query_statement(&statement)?;
    Ok(statement)
}

fn ensure_query_statement(statement: &DFStatement) -> Result<(), PlanBuildError> {
    match statement {
        DFStatement::Statement(inner) if matches!(inner.as_ref(), SqlStatement::Query(_)) => Ok(()),
        other => Err(PlanBuildError::UnsupportedStatement(other.to_string())),
    }
}

fn optimize_logical_plan(
    plan: LogicalPlan,
    target_partitions: usize,
) -> Result<LogicalPlan, DataFusionError> {
    let mut options = ConfigOptions::default();
    options.execution.target_partitions = target_partitions;
    let state = SessionStateBuilder::new()
        .with_config(options.into())
        .build();
    state.optimize(&plan)
}

fn pg_identifier_max_bytes() -> usize {
    (pg_sys::NAMEDATALEN as usize).saturating_sub(1)
}

#[derive(Debug, Error)]
enum UnsupportedSubqueryShape {
    #[error("EXISTS(...) expressions")]
    Exists,
    #[error("IN (SELECT ...) expressions")]
    InSubquery,
    #[error("scalar subquery expressions")]
    ScalarSubquery,
    #[error("correlated subqueries")]
    OuterReferenceColumn,
    #[error("logical subquery plan nodes")]
    SubqueryPlan,
}

fn validate_supported_plan_shape(plan: &LogicalPlan) -> Result<(), PlanBuildError> {
    plan.apply_with_subqueries(|node| {
        if matches!(node, LogicalPlan::Subquery(_)) {
            return Err(DataFusionError::External(Box::new(
                UnsupportedSubqueryShape::SubqueryPlan,
            )));
        }

        node.apply_expressions(|expr| {
            expr.apply(|expr| match expr {
                Expr::Exists(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::Exists,
                ))),
                Expr::InSubquery(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::InSubquery,
                ))),
                Expr::ScalarSubquery(_) => Err(DataFusionError::External(Box::new(
                    UnsupportedSubqueryShape::ScalarSubquery,
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
struct PgPlanningContext<'a, R> {
    resolver: &'a R,
    config: PlanBuilderConfig,
    options: ConfigOptions,
    builtins: Arc<Builtins>,
    tables: Mutex<HashMap<TableReference, Arc<PgPlanningTableSource>>>,
}

impl<'a, R> PgPlanningContext<'a, R> {
    fn new(resolver: &'a R, config: PlanBuilderConfig) -> Self {
        let mut options = ConfigOptions::default();
        options.execution.target_partitions = config.target_partitions;
        Self {
            resolver,
            config,
            options,
            builtins: Arc::clone(&BUILTINS),
            tables: Mutex::new(HashMap::new()),
        }
    }
}

impl<R> ContextProvider for PgPlanningContext<'_, R>
where
    R: CatalogResolver + Send + Sync,
{
    fn get_table_source(&self, table: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        validate_table_reference_identifiers(&table, self.config.identifier_max_bytes)?;

        let mut tables = self.tables.lock().map_err(|error| {
            DataFusionError::Plan(format!("catalog cache lock poisoned: {error}"))
        })?;
        if let Some(source) = tables.get(&table) {
            return Ok(Arc::clone(source) as Arc<dyn TableSource>);
        }

        let resolved = self
            .resolver
            .resolve_table(&table)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let source = Arc::new(PgPlanningTableSource::new(resolved));
        tables.insert(table, Arc::clone(&source));
        Ok(source)
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.builtins.scalar_udf.get(name).map(Arc::clone)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.builtins.agg_udf.get(name).map(Arc::clone)
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.builtins.window_udf.get(name).map(Arc::clone)
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<arrow_schema::DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        self.builtins.scalar_udf.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.builtins.agg_udf.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.builtins.window_udf.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.builtins.expr_planners
    }
}

fn validate_table_reference_identifiers(
    table: &TableReference,
    max_bytes: usize,
) -> DataFusionResult<()> {
    validate_identifier(table.table(), max_bytes, "table")?;
    if let Some(schema) = table.schema() {
        validate_identifier(schema, max_bytes, "schema")?;
    }
    Ok(())
}

fn validate_identifier(
    identifier: &str,
    max_bytes: usize,
    kind: &'static str,
) -> DataFusionResult<()> {
    if identifier.len() > max_bytes {
        return Err(DataFusionError::Plan(format!(
            "{kind} identifier `{identifier}` exceeds PostgreSQL limit of {max_bytes} bytes"
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct PgPlanningTableSource {
    resolved: ResolvedTable,
}

impl PgPlanningTableSource {
    fn new(resolved: ResolvedTable) -> Self {
        Self { resolved }
    }

    fn resolved(&self) -> &ResolvedTable {
        &self.resolved
    }
}

impl TableSource for PgPlanningTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.resolved.schema)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

#[derive(Debug)]
struct Builtins {
    agg_udf: HashMap<String, Arc<AggregateUDF>>,
    scalar_udf: HashMap<String, Arc<ScalarUDF>>,
    window_udf: HashMap<String, Arc<WindowUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl Builtins {
    fn new() -> Self {
        let mut agg_udf = HashMap::new();
        for function in SessionStateDefaults::default_aggregate_functions() {
            register_aggregate_udf(&mut agg_udf, function);
        }

        let mut scalar_udf = HashMap::new();
        for function in SessionStateDefaults::default_scalar_functions() {
            register_scalar_udf(&mut scalar_udf, function);
        }

        let mut window_udf = HashMap::new();
        for function in SessionStateDefaults::default_window_functions() {
            register_window_udf(&mut window_udf, function);
        }

        Self {
            agg_udf,
            scalar_udf,
            window_udf,
            expr_planners: SessionStateDefaults::default_expr_planners(),
        }
    }
}

fn register_scalar_udf(registry: &mut HashMap<String, Arc<ScalarUDF>>, udf: Arc<ScalarUDF>) {
    for alias in udf.aliases() {
        registry.insert(alias.clone(), Arc::clone(&udf));
    }
    registry.insert(udf.name().to_owned(), udf);
}

fn register_aggregate_udf(
    registry: &mut HashMap<String, Arc<AggregateUDF>>,
    udf: Arc<AggregateUDF>,
) {
    for alias in udf.aliases() {
        registry.insert(alias.clone(), Arc::clone(&udf));
    }
    registry.insert(udf.name().to_owned(), udf);
}

fn register_window_udf(registry: &mut HashMap<String, Arc<WindowUDF>>, udf: Arc<WindowUDF>) {
    for alias in udf.aliases() {
        registry.insert(alias.clone(), Arc::clone(&udf));
    }
    registry.insert(udf.name().to_owned(), udf);
}

#[derive(Debug)]
struct ScanLowerer {
    config: PlanBuilderConfig,
    next_scan_id: u64,
    scans: Vec<Arc<PgScanSpec>>,
}

impl ScanLowerer {
    fn new(config: PlanBuilderConfig) -> Self {
        Self {
            next_scan_id: config.first_scan_id,
            config,
            scans: Vec::new(),
        }
    }

    fn lower(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, PlanBuildError> {
        let transformed = plan.transform_up(|node| self.lower_node(node))?;
        Ok(transformed.data)
    }

    fn lower_node(&mut self, plan: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::TableScan(table_scan) => {
                self.lower_table_scan(table_scan).map(Transformed::yes)
            }
            other => Ok(Transformed::no(other)),
        }
    }

    fn lower_table_scan(&mut self, table_scan: TableScan) -> DataFusionResult<LogicalPlan> {
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

        let compiled = compile_scan(CompileScanInput {
            relation: &resolved.relation,
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
}

#[cfg(test)]
mod tests;
