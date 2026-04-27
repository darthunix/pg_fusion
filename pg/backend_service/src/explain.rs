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
use futures::executor::block_on;
use plan_builder::{PlanBuildInput, PlanBuilder};
use scan_node::{insert_page_materializers, PgScanExecFactory, PgScanExtensionPlanner, PgScanSpec};
use slot_scan::{explain_scan, ScanExplainOptions, ScanOptions};

use crate::{BackendServiceError, ExplainInput};

pub(crate) fn render_physical_explain(
    input: ExplainInput<'_>,
) -> Result<String, BackendServiceError> {
    let ExplainInput {
        sql,
        params,
        options,
        config,
    } = input;
    let built = PlanBuilder::new().build(PlanBuildInput { sql, params })?;

    let pg_leaf_explains = render_pg_leaf_explains(&built.scans, &config, options)?;
    let pg_scan_planner =
        PgScanExtensionPlanner::new(Arc::new(ExplainPgScanExecFactory { pg_leaf_explains }));
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

    Ok(render_physical_plan(
        physical_plan.as_ref(),
        options.verbose,
    ))
}

fn render_pg_leaf_explains(
    scans: &[Arc<PgScanSpec>],
    config: &crate::BackendServiceConfig,
    options: crate::ExplainRenderOptions,
) -> Result<BTreeMap<u64, Arc<str>>, BackendServiceError> {
    let explain_options = ScanExplainOptions {
        verbose: options.verbose,
        costs: options.costs,
    };
    let mut rendered = BTreeMap::new();
    for spec in scans {
        let scan_id = spec.scan_id.get();
        let execution_sql = crate::scan_execution_sql(spec)?;
        let pg_plan = explain_scan(
            &execution_sql,
            ScanOptions {
                planner_fetch_hint: spec.fetch_hints.planner_fetch_hint,
                local_row_cap: spec.fetch_hints.local_row_cap,
                diagnostics: config.diagnostics.clone(),
            },
            explain_options,
        )?;
        rendered.insert(scan_id, Arc::from(pg_plan.trim_end()));
    }
    Ok(rendered)
}

fn build_explain_session_state() -> SessionState {
    let mut options = ConfigOptions::default();
    options.execution.target_partitions = 1;
    SessionStateBuilder::new()
        .with_config(options.into())
        .with_default_features()
        .build()
}

#[derive(Debug)]
struct ExplainPgScanExecFactory {
    pg_leaf_explains: BTreeMap<u64, Arc<str>>,
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
        Ok(Arc::new(ExplainPgScanExec::new(spec, pg_explain)))
    }
}

#[derive(Debug)]
struct ExplainPgScanExec {
    spec: Arc<PgScanSpec>,
    output_schema: SchemaRef,
    pg_explain: Arc<str>,
    props: PlanProperties,
}

impl ExplainPgScanExec {
    fn new(spec: Arc<PgScanSpec>, pg_explain: Arc<str>) -> Self {
        let output_schema = spec.arrow_schema();
        let props = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            spec,
            output_schema,
            pg_explain,
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
            params.push(format!("scan_id={}", self.spec.scan_id.get()));
            params.push(format!("table_oid={}", self.spec.table_oid));
            if let Some(hint) = self.spec.fetch_hints.planner_fetch_hint {
                params.push(format!("planner_fetch_hint={hint}"));
            }
            params.push(format!("output_schema={:?}", self.output_schema));
            params.push(format!("sql=\"{}\"", self.spec.compiled_scan.sql));
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

    fn properties(&self) -> &PlanProperties {
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

    fn statistics(&self) -> DFResult<Statistics> {
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
        render_pg_scan_explain(pg_scan, depth + 1, out);
    } else {
        let _ = writeln!(out, "{}{}", indent, PlanLine { plan, format_type });
    }

    for child in plan.children() {
        render_physical_plan_node(child.as_ref(), verbose, depth + 1, out);
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
