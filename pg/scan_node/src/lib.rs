//! DataFusion custom logical node for PostgreSQL scan leaves.
//!
//! `scan_node` is the bridge between backend-side scan compilation and later
//! worker-side execution. It stores the already-compiled PostgreSQL scan SQL
//! plus stable planning metadata, but deliberately does not open connections,
//! own snapshots, execute `slot_scan`, or stream pages.
//!
//! The intended flow is:
//!
//! 1. backend planning resolves a relation with `df_catalog`
//! 2. backend planning compiles pushdown with `scan_sql`
//! 3. backend planning creates [`PgScanSpec`] and [`PgScanNode`]
//! 4. worker planning lowers [`PgScanNode`] through [`PgScanExtensionPlanner`]
//!    into a runtime-specific `ExecutionPlan`
//!
//! Residual filters from [`scan_sql::CompiledScan`] are not evaluated by this
//! node. Plan-building code must keep required residual predicates above the
//! custom scan node.

use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::logical_plan::{
    Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion_expr::Expr;
use scan_sql::{CompiledScan, PgRelation};

/// Stable identifier for one PostgreSQL scan leaf within one planned query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgScanId(u64);

impl PgScanId {
    /// Create a new scan id.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw scan id value.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for PgScanId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

/// Fetch hints that later backend code can lower into `slot_scan::ScanOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PgScanFetchHints {
    pub planner_fetch_hint: Option<usize>,
    pub local_row_cap: Option<usize>,
}

impl PgScanFetchHints {
    /// Derive the default `scan_sql -> slot_scan` fetch hints.
    ///
    /// A planner hint is safe even when residual filters remain above the scan,
    /// but a local row cap is only safe once all filters have been compiled into
    /// the PostgreSQL scan SQL. Otherwise `slot_scan` could stop before
    /// residual predicates have produced enough qualifying rows.
    pub fn from_compiled(scan: &CompiledScan) -> Self {
        Self {
            planner_fetch_hint: scan.requested_limit,
            local_row_cap: if scan.all_filters_compiled {
                scan.requested_limit
            } else {
                None
            },
        }
    }
}

/// One logical PostgreSQL scan leaf.
#[derive(Debug, Clone)]
pub struct PgScanSpec {
    pub scan_id: PgScanId,
    pub table_oid: u32,
    pub relation: PgRelation,
    pub compiled_scan: CompiledScan,
    pub fetch_hints: PgScanFetchHints,
    schema: DFSchemaRef,
}

impl PgScanSpec {
    /// Build a scan spec from a full source schema and an already compiled scan.
    ///
    /// `compiled_scan.output_columns` indexes into `source_schema`. The derived
    /// output schema preserves DataFusion field qualifiers so residual filters
    /// kept above this node can still bind their columns.
    pub fn try_new(
        scan_id: impl Into<PgScanId>,
        table_oid: u32,
        relation: PgRelation,
        source_schema: &DFSchema,
        compiled_scan: CompiledScan,
    ) -> Result<Self> {
        let schema = build_output_schema(source_schema, &compiled_scan)?;
        let fetch_hints = PgScanFetchHints::from_compiled(&compiled_scan);
        Ok(Self {
            scan_id: scan_id.into(),
            table_oid,
            relation,
            compiled_scan,
            fetch_hints,
            schema,
        })
    }

    /// Build a scan spec from an already-derived logical output schema.
    ///
    /// This constructor is intended for decode paths such as `plan_codec`,
    /// where the original source schema is no longer available but the logical
    /// output `DFSchema` has already been serialized alongside the scan spec.
    pub fn try_new_with_schema(
        scan_id: impl Into<PgScanId>,
        table_oid: u32,
        relation: PgRelation,
        compiled_scan: CompiledScan,
        fetch_hints: PgScanFetchHints,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        let expected_len = compiled_scan.output_columns.len();
        let actual_len = schema.fields().len();
        if actual_len != expected_len {
            return Err(DataFusionError::Plan(format!(
                "PgScanSpec output schema has {actual_len} fields, but compiled scan expects {expected_len} output columns"
            )));
        }

        Ok(Self {
            scan_id: scan_id.into(),
            table_oid,
            relation,
            compiled_scan,
            fetch_hints,
            schema,
        })
    }

    /// DataFusion logical output schema for this scan.
    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    /// Arrow output schema for physical execution.
    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::clone(self.schema.inner())
    }
}

fn build_output_schema(source_schema: &DFSchema, scan: &CompiledScan) -> Result<DFSchemaRef> {
    let field_count = source_schema.fields().len();
    let mut fields = Vec::with_capacity(scan.output_columns.len());
    for &index in &scan.output_columns {
        if index >= field_count {
            return Err(DataFusionError::Plan(format!(
                "PgScanSpec output column index {index} out of bounds for source schema with {field_count} fields"
            )));
        }
        let (qualifier, field) = source_schema.qualified_field(index);
        fields.push((qualifier.cloned(), Arc::new(field.clone())));
    }
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        source_schema.as_arrow().metadata().clone(),
    )?))
}

/// DataFusion custom logical leaf for one PostgreSQL scan.
#[derive(Debug, Clone)]
pub struct PgScanNode {
    spec: Arc<PgScanSpec>,
}

impl PgScanNode {
    /// Create a logical node for a scan spec.
    pub fn new(spec: Arc<PgScanSpec>) -> Self {
        Self { spec }
    }

    /// Return the scan spec carried by this node.
    pub fn spec(&self) -> Arc<PgScanSpec> {
        Arc::clone(&self.spec)
    }

    /// Convert this node into a DataFusion logical extension plan.
    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

impl PartialEq for PgScanNode {
    fn eq(&self, other: &Self) -> bool {
        self.spec.scan_id == other.spec.scan_id
    }
}

impl Eq for PgScanNode {}

impl Hash for PgScanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.spec.scan_id.hash(state);
    }
}

impl PartialOrd for PgScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.spec.scan_id.partial_cmp(&other.spec.scan_id)
    }
}

impl UserDefinedLogicalNodeCore for PgScanNode {
    fn name(&self) -> &str {
        "PgScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        self.spec.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let spec = &self.spec;
        write!(
            f,
            "PgScan: scan_id={}, table_oid={}, relation={}, output_columns={}, pushed_filters={}, residual_filters={}, requested_limit={:?}",
            spec.scan_id.get(),
            spec.table_oid,
            display_relation(&spec.relation),
            spec.compiled_scan.output_columns.len(),
            spec.compiled_scan.pushed_filters.len(),
            spec.compiled_scan.residual_filters.len(),
            spec.compiled_scan.requested_limit,
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return Err(DataFusionError::Plan(
                "PgScanNode does not expose rewritable expressions".into(),
            ));
        }
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "PgScanNode is a leaf and does not accept inputs".into(),
            ));
        }
        Ok(self.clone())
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

/// Factory used by [`PgScanExtensionPlanner`] to build the runtime execution plan.
pub trait PgScanExecFactory: Debug + Send + Sync {
    fn create(&self, spec: Arc<PgScanSpec>) -> Result<Arc<dyn ExecutionPlan>>;
}

/// DataFusion physical-planner hook for [`PgScanNode`].
#[derive(Debug, Clone)]
pub struct PgScanExtensionPlanner {
    factory: Arc<dyn PgScanExecFactory>,
}

impl PgScanExtensionPlanner {
    /// Create a planner hook backed by a runtime-specific factory.
    pub fn new(factory: Arc<dyn PgScanExecFactory>) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl ExtensionPlanner for PgScanExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(pg_scan) = node.as_any().downcast_ref::<PgScanNode>() else {
            return Ok(None);
        };

        if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "PgScanNode physical planning received unexpected inputs".into(),
            ));
        }

        self.factory.create(pg_scan.spec()).map(Some)
    }
}

#[cfg(test)]
fn stable_node_hash(node: &PgScanNode) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node.hash(&mut hasher);
    hasher.finish()
}

fn display_relation(relation: &PgRelation) -> String {
    match &relation.schema {
        Some(schema) => format!("{schema}.{}", relation.table),
        None => relation.table.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_planner::DefaultPhysicalPlanner;
    use datafusion_common::{DFSchema, ScalarValue, TableReference};
    use datafusion_expr::logical_plan::EmptyRelation;

    fn source_schema() -> DFSchema {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);
        DFSchema::try_from_qualified_schema(TableReference::partial("public", "users"), &schema)
            .unwrap()
    }

    fn compiled_scan(output_columns: Vec<usize>) -> CompiledScan {
        CompiledScan {
            sql: "SELECT id FROM public.users".into(),
            requested_limit: Some(10),
            sql_limit: None,
            selected_columns: vec![0],
            output_columns,
            filter_only_columns: Vec::new(),
            residual_filter_columns: Vec::new(),
            pushed_filters: Vec::new(),
            residual_filters: Vec::new(),
            all_filters_compiled: true,
            uses_dummy_projection: false,
        }
    }

    fn spec(scan_id: u64) -> Arc<PgScanSpec> {
        Arc::new(
            PgScanSpec::try_new(
                scan_id,
                42,
                PgRelation::new(Some("public"), "users"),
                &source_schema(),
                compiled_scan(vec![0, 2]),
            )
            .unwrap(),
        )
    }

    #[test]
    fn spec_builds_output_schema_and_fetch_hints() {
        let spec = PgScanSpec::try_new(
            7,
            42,
            PgRelation::new(Some("public"), "users"),
            &source_schema(),
            compiled_scan(vec![2, 0]),
        )
        .unwrap();

        assert_eq!(spec.scan_id.get(), 7);
        assert_eq!(spec.fetch_hints.planner_fetch_hint, Some(10));
        assert_eq!(spec.fetch_hints.local_row_cap, Some(10));
        assert_eq!(spec.schema().fields().len(), 2);
        assert_eq!(spec.schema().field(0).name(), "score");
        assert_eq!(spec.schema().field(1).name(), "id");
        assert_eq!(
            spec.schema().qualified_field(0).0,
            Some(&TableReference::partial("public", "users"))
        );
    }

    #[test]
    fn residual_filters_disable_local_row_cap_but_keep_planner_hint() {
        let mut scan = compiled_scan(vec![0]);
        scan.all_filters_compiled = false;
        scan.residual_filters = vec![Expr::Literal(ScalarValue::Boolean(Some(true)))];

        let spec = PgScanSpec::try_new(
            25,
            42,
            PgRelation::new(Some("public"), "users"),
            &source_schema(),
            scan,
        )
        .unwrap();

        assert_eq!(spec.fetch_hints.planner_fetch_hint, Some(10));
        assert_eq!(spec.fetch_hints.local_row_cap, None);
    }

    #[test]
    fn spec_supports_dummy_projection_empty_output_schema() {
        let mut scan = compiled_scan(Vec::new());
        scan.uses_dummy_projection = true;
        scan.selected_columns.clear();

        let spec = PgScanSpec::try_new(
            8,
            42,
            PgRelation::new(Some("public"), "users"),
            &source_schema(),
            scan,
        )
        .unwrap();

        assert!(spec.schema().fields().is_empty());
        assert!(spec.arrow_schema().fields().is_empty());
    }

    #[test]
    fn spec_rejects_invalid_output_column() {
        let err = PgScanSpec::try_new(
            9,
            42,
            PgRelation::new(Some("public"), "users"),
            &source_schema(),
            compiled_scan(vec![3]),
        )
        .unwrap_err();

        assert!(err.to_string().contains("out of bounds"));
    }

    #[test]
    fn logical_node_is_leaf_and_keeps_schema() {
        let spec = spec(11);
        let node = PgScanNode::new(Arc::clone(&spec));

        assert_eq!(UserDefinedLogicalNodeCore::name(&node), "PgScan");
        assert!(UserDefinedLogicalNodeCore::inputs(&node).is_empty());
        assert!(UserDefinedLogicalNodeCore::expressions(&node).is_empty());
        assert!(!UserDefinedLogicalNodeCore::supports_limit_pushdown(&node));
        assert_eq!(UserDefinedLogicalNodeCore::schema(&node), spec.schema());
        assert!(format!("{node:?}").contains("PgScanNode"));
    }

    #[test]
    fn logical_node_rewrite_contract_is_empty_only() {
        let node = PgScanNode::new(spec(12));
        assert!(
            UserDefinedLogicalNodeCore::with_exprs_and_inputs(&node, Vec::new(), Vec::new())
                .is_ok()
        );

        let expr_err = UserDefinedLogicalNodeCore::with_exprs_and_inputs(
            &node,
            vec![Expr::Literal(ScalarValue::Int64(Some(1)))],
            Vec::new(),
        )
        .unwrap_err();
        assert!(expr_err.to_string().contains("expressions"));

        let input_schema = DFSchemaRef::new(DFSchema::empty());
        let input_err = UserDefinedLogicalNodeCore::with_exprs_and_inputs(
            &node,
            Vec::new(),
            vec![LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: input_schema,
            })],
        )
        .unwrap_err();
        assert!(input_err.to_string().contains("leaf"));
    }

    #[test]
    fn logical_node_identity_uses_scan_id() {
        let left = PgScanNode::new(spec(13));
        let right = PgScanNode::new(spec(13));
        let other = PgScanNode::new(spec(14));

        assert_eq!(left, right);
        assert_ne!(left, other);
        assert_eq!(stable_node_hash(&left), stable_node_hash(&right));
    }

    #[derive(Debug, Clone)]
    struct OtherNode {
        schema: DFSchemaRef,
    }

    impl PartialEq for OtherNode {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for OtherNode {}

    impl PartialOrd for OtherNode {
        fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
            Some(Ordering::Equal)
        }
    }

    impl Hash for OtherNode {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0usize.hash(state);
        }
    }

    impl UserDefinedLogicalNodeCore for OtherNode {
        fn name(&self) -> &str {
            "Other"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            Vec::new()
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            Vec::new()
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Other")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            _inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(self.clone())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingFactory {
        calls: AtomicUsize,
        scan_ids: Mutex<Vec<PgScanId>>,
    }

    impl PgScanExecFactory for RecordingFactory {
        fn create(&self, spec: Arc<PgScanSpec>) -> Result<Arc<dyn ExecutionPlan>> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.scan_ids.lock().unwrap().push(spec.scan_id);
            Ok(Arc::new(MemoryExec::try_new(
                &[vec![]],
                spec.arrow_schema(),
                None,
            )?))
        }
    }

    #[test]
    fn extension_planner_ignores_unknown_nodes() {
        let factory = Arc::new(RecordingFactory::default());
        let planner = PgScanExtensionPlanner::new(factory.clone());
        let physical_planner = DefaultPhysicalPlanner::default();
        let session = SessionStateBuilder::new().build();
        let other = OtherNode {
            schema: DFSchemaRef::new(DFSchema::empty()),
        };

        let planned = futures::executor::block_on(planner.plan_extension(
            &physical_planner,
            &other,
            &[],
            &[],
            &session,
        ))
        .unwrap();

        assert!(planned.is_none());
        assert_eq!(factory.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn extension_planner_delegates_pg_scan_nodes() {
        let factory = Arc::new(RecordingFactory::default());
        let planner = PgScanExtensionPlanner::new(factory.clone());
        let physical_planner = DefaultPhysicalPlanner::default();
        let session = SessionStateBuilder::new().build();
        let node = PgScanNode::new(spec(21));

        let planned = futures::executor::block_on(planner.plan_extension(
            &physical_planner,
            &node,
            &[],
            &[],
            &session,
        ))
        .unwrap();

        assert!(planned.is_some());
        assert_eq!(factory.calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            factory.scan_ids.lock().unwrap().as_slice(),
            &[PgScanId::new(21)]
        );
    }

    #[test]
    fn extension_planner_rejects_pg_scan_inputs() {
        let factory = Arc::new(RecordingFactory::default());
        let planner = PgScanExtensionPlanner::new(factory);
        let physical_planner = DefaultPhysicalPlanner::default();
        let session = SessionStateBuilder::new().build();
        let node = PgScanNode::new(spec(22));
        let input_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: DFSchemaRef::new(DFSchema::empty()),
        });

        let err = futures::executor::block_on(planner.plan_extension(
            &physical_planner,
            &node,
            &[&input_plan],
            &[],
            &session,
        ))
        .unwrap_err();

        assert!(err.to_string().contains("unexpected inputs"));
    }

    #[test]
    fn node_converts_to_logical_plan_extension() {
        let plan = PgScanNode::new(spec(23)).into_logical_plan();
        assert!(matches!(plan, LogicalPlan::Extension(_)));
    }

    #[test]
    fn source_schema_can_be_unqualified() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let source =
            DFSchema::from_unqualified_fields(schema.fields().clone(), HashMap::new()).unwrap();
        let scan = compiled_scan(vec![0]);
        let spec = PgScanSpec::try_new(
            24,
            42,
            PgRelation::new(Some("public"), "users"),
            &source,
            scan,
        )
        .unwrap();

        assert_eq!(spec.schema().qualified_field(0).0, None);
    }
}
