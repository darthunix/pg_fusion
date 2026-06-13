use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_expr::registry::FunctionRegistry;
use futures::executor::block_on;
use scan_node::{insert_page_materializers, PgScanExecFactory, PgScanExtensionPlanner, PgScanSpec};
use slot_scan::{explain_scan, ScanExplainOptions, ScanOptions};

use crate::{
    build_execution_plan, BackendServiceError, CtidBlockRange, ExplainInput,
    ExplainScanParallelism, ExplainScanParallelismStrategy, ExplainScanProducerRole,
};

pub(crate) fn render_physical_explain(
    input: ExplainInput<'_>,
) -> Result<String, BackendServiceError> {
    let ExplainInput {
        plan_source,
        options,
        config,
        scan_worker_launcher,
        actual_scan_parallelism,
    } = input;
    let source_label = plan_source.label();
    let built = build_execution_plan(plan_source, &config)?;

    let planned_scan_parallelism = if let Some(launcher) = scan_worker_launcher {
        launcher.explain_query(crate::ScanWorkerQueryInput {
            scans: &built.scans,
        })?
    } else {
        BTreeMap::new()
    };
    let pg_leaf_explains = render_pg_leaf_explains(
        &built.scans,
        &config,
        options,
        &planned_scan_parallelism,
        &actual_scan_parallelism,
    )?;
    let pg_scan_planner = PgScanExtensionPlanner::new(Arc::new(ExplainPgScanExecFactory {
        pg_leaf_explains,
        planned_scan_parallelism,
        actual_scan_parallelism,
    }));
    let physical_planner =
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(pg_scan_planner)]);
    let session_state = build_explain_session_state();
    let physical_plan =
        block_on(physical_planner.create_physical_plan(&built.logical_plan, &session_state))
            .map_err(BackendServiceError::PhysicalPlan)?;
    let physical_plan = insert_page_materializers(physical_plan, &|plan| {
        plan.as_any().is::<ExplainPgScanExec>()
    })
    .map_err(BackendServiceError::PhysicalPlan)?;

    let mut rendered = render_physical_plan(physical_plan.as_ref(), options.verbose);
    if options.verbose {
        rendered.push_str(&format!("Planning Source: {source_label}\n"));
    }
    Ok(rendered)
}

fn render_pg_leaf_explains(
    scans: &[Arc<PgScanSpec>],
    config: &crate::BackendServiceConfig,
    options: crate::ExplainRenderOptions,
    planned_scan_parallelism: &BTreeMap<u64, ExplainScanParallelism>,
    actual_scan_parallelism: &BTreeMap<u64, ExplainScanParallelism>,
) -> Result<BTreeMap<u64, PgLeafExplain>, BackendServiceError> {
    let explain_options = ScanExplainOptions {
        verbose: options.verbose,
        costs: options.costs,
    };
    let mut rendered = BTreeMap::new();
    for spec in scans {
        let scan_id = spec.scan_id.get();
        let representative_range = representative_ctid_range(
            planned_scan_parallelism.get(&scan_id),
            actual_scan_parallelism.get(&scan_id),
        );
        let execution_sql = representative_scan_execution_sql(spec, representative_range)?;
        let pg_plan = explain_scan(
            &execution_sql,
            ScanOptions {
                planner_fetch_hint: spec.fetch_hints.planner_fetch_hint,
                local_row_cap: spec.fetch_hints.local_row_cap,
                diagnostics: config.diagnostics.clone(),
            },
            explain_options,
        )?;
        let (display_sql, pg_plan) = if let Some(range) = representative_range {
            (
                mask_ctid_range_sql(&execution_sql, range),
                mask_ctid_range_literals(&pg_plan, range),
            )
        } else {
            (spec.compiled_scan.sql.clone(), pg_plan)
        };
        rendered.insert(
            scan_id,
            PgLeafExplain {
                display_sql: Arc::from(display_sql),
                pg_explain: Arc::from(pg_plan.trim_end()),
            },
        );
    }
    Ok(rendered)
}

fn mask_ctid_range_sql(sql: &str, range: CtidBlockRange) -> String {
    let start = ctid_bound_literal(range.start_block);
    let end = ctid_bound_literal(range.end_block);
    let concrete = format!("ctid >= {start} AND ctid < {end}");
    let masked = "ctid >= $1::tid AND ctid < $2::tid";
    sql.replacen(&concrete, masked, 1)
}

fn mask_ctid_range_literals(text: &str, range: CtidBlockRange) -> String {
    text.replace(&ctid_bound_literal(range.start_block), "$1")
        .replace(&ctid_bound_literal(range.end_block), "$2")
}

fn ctid_bound_literal(block: u64) -> String {
    format!("'({block},1)'::tid")
}

fn representative_scan_execution_sql(
    spec: &PgScanSpec,
    representative_range: Option<CtidBlockRange>,
) -> Result<String, BackendServiceError> {
    match representative_range {
        Some(range) => {
            crate::scan_execution_shape_for_ctid_range(spec, range).map(|shape| shape.sql)
        }
        None => crate::scan_execution_sql(spec),
    }
}

fn representative_ctid_range(
    planned: Option<&ExplainScanParallelism>,
    actual: Option<&ExplainScanParallelism>,
) -> Option<CtidBlockRange> {
    let planned = planned?;
    if planned.strategy != ExplainScanParallelismStrategy::CtidBlockRange {
        return None;
    }
    if let Some(actual) = actual {
        let has_actual_worker = actual
            .producers
            .iter()
            .any(|producer| producer.role == ExplainScanProducerRole::Worker);
        if actual.strategy == ExplainScanParallelismStrategy::LeaderOnly || !has_actual_worker {
            return None;
        }
    }
    planned
        .producers
        .iter()
        .find(|producer| producer.role == ExplainScanProducerRole::Leader)
        .and_then(|producer| producer.ctid_range)
        .or_else(|| {
            planned
                .producers
                .iter()
                .find_map(|producer| producer.ctid_range)
        })
}

#[derive(Debug, Clone)]
struct PgLeafExplain {
    display_sql: Arc<str>,
    pg_explain: Arc<str>,
}

fn build_explain_session_state() -> SessionState {
    let mut options = ConfigOptions::default();
    options.execution.target_partitions = 1;
    let mut state = SessionStateBuilder::new()
        .with_config(options.into())
        .with_default_features()
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
    state
}

#[derive(Debug)]
struct ExplainPgScanExecFactory {
    pg_leaf_explains: BTreeMap<u64, PgLeafExplain>,
    planned_scan_parallelism: BTreeMap<u64, ExplainScanParallelism>,
    actual_scan_parallelism: BTreeMap<u64, ExplainScanParallelism>,
}

impl PgScanExecFactory for ExplainPgScanExecFactory {
    fn create(&self, spec: Arc<PgScanSpec>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scan_id = spec.scan_id.get();
        let pg_explain = self
            .pg_leaf_explains
            .get(&scan_id)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "missing PostgreSQL explain text for scan_id {scan_id}"
                ))
            })?;
        Ok(Arc::new(ExplainPgScanExec::new(
            spec,
            pg_explain.display_sql.clone(),
            pg_explain.pg_explain.clone(),
            self.planned_scan_parallelism.get(&scan_id).cloned(),
            self.actual_scan_parallelism.get(&scan_id).cloned(),
        )))
    }
}

#[derive(Debug)]
struct ExplainPgScanExec {
    spec: Arc<PgScanSpec>,
    output_schema: SchemaRef,
    display_sql: Arc<str>,
    pg_explain: Arc<str>,
    planned_parallelism: Option<ExplainScanParallelism>,
    actual_parallelism: Option<ExplainScanParallelism>,
    props: Arc<PlanProperties>,
}

impl ExplainPgScanExec {
    fn new(
        spec: Arc<PgScanSpec>,
        display_sql: Arc<str>,
        pg_explain: Arc<str>,
        planned_parallelism: Option<ExplainScanParallelism>,
        actual_parallelism: Option<ExplainScanParallelism>,
    ) -> Self {
        let output_schema = spec.arrow_schema();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            spec,
            output_schema,
            display_sql,
            pg_explain,
            planned_parallelism,
            actual_parallelism,
            props,
        }
    }

    fn display_line(&self, verbose: bool) -> String {
        let relation = display_relation(&self.spec);
        let mut line = format!("PostgreSQL Scan: {relation}");
        let mut params = Vec::new();
        if let Some(limit) = self.spec.compiled_scan.requested_limit {
            params.push(format!("soft_limit={limit}"));
        }
        if let Some(cap) = self.spec.fetch_hints.local_row_cap {
            params.push(format!("local_row_cap={cap}"));
        }
        if verbose {
            params.push(format!("sql=\"{}\"", self.display_sql));
        }
        if !params.is_empty() {
            line.push_str(" (");
            line.push_str(&params.join(", "));
            line.push(')');
        }
        line
    }
}

impl DisplayAs for ExplainPgScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let verbose = matches!(t, DisplayFormatType::Verbose);
        write!(f, "{}", self.display_line(verbose))
    }
}

impl ExecutionPlan for ExplainPgScanExec {
    fn name(&self) -> &str {
        "PgFusionPgScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "PgFusionPgScanExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Err(DataFusionError::Plan(
            "PgFusionPgScanExec is explain-only and cannot execute".into(),
        ))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.output_schema))
    }
}

struct PlanLine<'a> {
    plan: &'a dyn ExecutionPlan,
    format_type: DisplayFormatType,
}

impl fmt::Display for PlanLine<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.plan.fmt_as(self.format_type, f)
    }
}

fn render_physical_plan(plan: &dyn ExecutionPlan, verbose: bool) -> String {
    let mut out = String::new();
    render_physical_plan_node(plan, verbose, 0, &mut out);
    out
}

fn render_physical_plan_node(
    plan: &dyn ExecutionPlan,
    verbose: bool,
    depth: usize,
    out: &mut String,
) {
    let format_type = if verbose {
        DisplayFormatType::Verbose
    } else {
        DisplayFormatType::Default
    };
    let indent = "  ".repeat(depth);

    if let Some(pg_scan) = plan.as_any().downcast_ref::<ExplainPgScanExec>() {
        let _ = writeln!(out, "{}{}", indent, pg_scan.display_line(verbose));
        render_scan_parallelism(pg_scan, depth + 1, verbose, out);
        render_pg_scan_explain(pg_scan, depth + 1, out);
    } else {
        let _ = writeln!(out, "{}{}", indent, PlanLine { plan, format_type });
    }

    for child in plan.children() {
        render_physical_plan_node(child.as_ref(), verbose, depth + 1, out);
    }
}

fn render_scan_parallelism(
    pg_scan: &ExplainPgScanExec,
    depth: usize,
    verbose: bool,
    out: &mut String,
) {
    let Some(planned) = pg_scan.planned_parallelism.as_ref() else {
        return;
    };

    let body_indent = "  ".repeat(depth);
    let mut line = format!(
        "{body_indent}PgFusion Producers: planned={}",
        producer_summary(planned)
    );
    if let Some(actual) = pg_scan.actual_parallelism.as_ref() {
        line.push_str(&format!(", actual={}", producer_summary(actual)));
    }
    line.push_str(&format!(
        ", strategy={}",
        explain_strategy(planned.strategy)
    ));
    if let Some(block_count) = planned.block_count {
        line.push_str(&format!(", blocks={block_count}"));
    }
    if let Some(reason) = planned.reason.as_deref() {
        line.push_str(&format!(", reason={reason}"));
    }
    let _ = writeln!(out, "{line}");

    if verbose {
        for producer in &planned.producers {
            let mut producer_line = format!(
                "{body_indent}PgFusion Producer {}: {}",
                producer.producer_id,
                explain_role(producer.role)
            );
            if let Some(range) = producer.ctid_range {
                producer_line.push_str(&format!(
                    ", ctid_blocks=[{}, {})",
                    range.start_block, range.end_block
                ));
            }
            let _ = writeln!(out, "{producer_line}");
        }
    }
}

fn producer_summary(parallelism: &ExplainScanParallelism) -> String {
    let leaders = parallelism
        .producers
        .iter()
        .filter(|producer| producer.role == ExplainScanProducerRole::Leader)
        .count();
    let workers = parallelism
        .producers
        .iter()
        .filter(|producer| producer.role == ExplainScanProducerRole::Worker)
        .count();
    match (leaders, workers) {
        (1, 0) => "1 (leader-only)".to_string(),
        (1, workers) => format!("{} (leader + {workers} workers)", workers + 1),
        _ => format!(
            "{} (leaders={leaders}, workers={workers})",
            leaders + workers
        ),
    }
}

fn explain_strategy(strategy: ExplainScanParallelismStrategy) -> &'static str {
    match strategy {
        ExplainScanParallelismStrategy::LeaderOnly => "leader_only",
        ExplainScanParallelismStrategy::CtidBlockRange => "ctid_range",
    }
}

fn explain_role(role: ExplainScanProducerRole) -> &'static str {
    match role {
        ExplainScanProducerRole::Leader => "leader",
        ExplainScanProducerRole::Worker => "worker",
    }
}

fn render_pg_scan_explain(pg_scan: &ExplainPgScanExec, depth: usize, out: &mut String) {
    if pg_scan.pg_explain.trim().is_empty() {
        return;
    }

    let body_indent = "  ".repeat(depth);
    for line in pg_scan.pg_explain.lines() {
        if line.trim().is_empty() {
            let _ = writeln!(out);
        } else {
            let _ = writeln!(out, "{body_indent}{line}");
        }
    }
}

fn display_relation(spec: &PgScanSpec) -> String {
    match &spec.relation.schema {
        Some(schema) => format!("{schema}.{}", spec.relation.table),
        None => spec.relation.table.clone(),
    }
}
