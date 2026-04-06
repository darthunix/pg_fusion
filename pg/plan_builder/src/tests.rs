use super::*;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::Column;
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::{lit, Operator};
use scan_sql::PgRelation;

const TEST_IDENTIFIER_MAX_BYTES: usize = 63;

#[derive(Debug, Clone)]
struct FakeResolver {
    tables: HashMap<TableReference, ResolvedTable>,
}

impl FakeResolver {
    fn new(tables: impl IntoIterator<Item = (TableReference, ResolvedTable)>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }
}

impl CatalogResolver for FakeResolver {
    fn resolve_table(&self, table: &TableReference) -> Result<ResolvedTable, ResolveError> {
        self.tables
            .get(table)
            .cloned()
            .ok_or_else(|| ResolveError::TableNotFound {
                schema: table.schema().map(|schema| schema.to_string()),
                table: table.table().to_owned(),
            })
    }
}

fn user_table() -> ResolvedTable {
    ResolvedTable {
        table_oid: 42,
        relation: PgRelation::new(Some("public"), "users"),
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ])),
    }
}

fn order_table() -> ResolvedTable {
    ResolvedTable {
        table_oid: 77,
        relation: PgRelation::new(Some("public"), "orders"),
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ])),
    }
}

fn builder() -> PlanBuilder<FakeResolver> {
    PlanBuilder::with_resolver(FakeResolver::new([
        (TableReference::bare("users"), user_table()),
        (TableReference::bare("orders"), order_table()),
        (
            TableReference::partial("public", "users"),
            ResolvedTable {
                relation: PgRelation::new(Some("public"), "users"),
                ..user_table()
            },
        ),
    ]))
    .with_config(PlanBuilderConfig {
        target_partitions: 1,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        first_scan_id: 1,
    })
}

fn build_sql(sql: &str) -> BuiltPlan {
    builder()
        .build(PlanBuildInput {
            sql,
            params: Vec::new(),
        })
        .unwrap()
}

fn contains_table_scan(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::TableScan(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .unwrap();
    found
}

fn count_pg_scan_nodes(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::Extension(extension) = node {
            if extension.node.as_any().is::<PgScanNode>() {
                count += 1;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

#[test]
fn builds_simple_query_with_one_pg_scan_node() {
    let built = build_sql("SELECT id, name FROM users WHERE id > 10");

    assert_eq!(built.scans.len(), 1);
    assert!(!contains_table_scan(&built.logical_plan));
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 1);

    let spec = &built.scans[0];
    assert_eq!(spec.scan_id.get(), 1);
    assert_eq!(spec.table_oid, 42);
    assert_eq!(
        spec.compiled_scan.sql,
        "SELECT \"id\", \"name\" FROM \"public\".\"users\" WHERE (\"id\" > 10)"
    );
}

#[test]
fn binds_params_before_scan_sql_compilation() {
    let built = builder()
        .build(PlanBuildInput {
            sql: "SELECT id FROM users WHERE id > $1",
            params: vec![ScalarValue::Int64(Some(10))],
        })
        .unwrap();

    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"users\" WHERE (\"id\" > 10)"
    );
}

#[test]
fn multiple_table_scans_get_sequential_ids() {
    let built = build_sql(
        "SELECT users.id, orders.id \
         FROM users JOIN orders ON users.id = orders.user_id",
    );

    let scan_ids = built
        .scans
        .iter()
        .map(|scan| scan.scan_id.get())
        .collect::<Vec<_>>();
    assert_eq!(scan_ids, vec![1, 2]);
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 2);
}

#[test]
fn residual_filters_are_restored_and_extra_columns_projected_away() {
    let source = Arc::new(PgPlanningTableSource::new(user_table())) as Arc<dyn TableSource>;
    let regex_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("name"))),
        Operator::RegexMatch,
        Box::new(lit("^a")),
    ));
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        source,
        Some(vec![0]),
        vec![regex_filter],
        None,
    )
    .unwrap();
    let mut lowerer = ScanLowerer::new(PlanBuilderConfig {
        target_partitions: 1,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        first_scan_id: 1,
    });

    let plan = lowerer
        .lower(LogicalPlan::TableScan(table_scan))
        .expect("lower residual scan");

    assert_eq!(lowerer.scans.len(), 1);
    assert_eq!(lowerer.scans[0].compiled_scan.output_columns, vec![0, 1]);
    assert_eq!(
        lowerer.scans[0].compiled_scan.residual_filter_columns,
        vec![1]
    );
    assert_eq!(plan.schema().fields().len(), 1);
    assert_eq!(plan.schema().field(0).name(), "id");
    assert!(format!("{}", plan.display_indent()).contains("Filter"));
    assert!(matches!(plan, LogicalPlan::Projection(_)));
}

#[test]
fn residual_filters_disable_local_row_cap_but_keep_planner_hint() {
    let source = Arc::new(PgPlanningTableSource::new(user_table())) as Arc<dyn TableSource>;
    let regex_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("name"))),
        Operator::RegexMatch,
        Box::new(lit("^a")),
    ));
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        source,
        None,
        vec![regex_filter],
        Some(10),
    )
    .unwrap();
    let mut lowerer = ScanLowerer::new(PlanBuilderConfig {
        target_partitions: 1,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        first_scan_id: 1,
    });

    let _ = lowerer
        .lower(LogicalPlan::TableScan(table_scan))
        .expect("lower residual scan");

    assert_eq!(lowerer.scans[0].fetch_hints.planner_fetch_hint, Some(10));
    assert_eq!(lowerer.scans[0].fetch_hints.local_row_cap, None);
}

#[test]
fn resolves_default_scalar_function_aliases() {
    let built = build_sql("SELECT length(name) AS len FROM users");

    assert_eq!(built.scans.len(), 1);
    assert_eq!(built.logical_plan.schema().field(0).name(), "len");
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"name\" FROM \"public\".\"users\""
    );

    let built = build_sql("SELECT char_length(name) AS len FROM users");
    assert_eq!(built.scans.len(), 1);
    assert_eq!(built.logical_plan.schema().field(0).name(), "len");
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"name\" FROM \"public\".\"users\""
    );
}

#[test]
fn installs_non_core_expression_planners() {
    let built = build_sql("SELECT substring(name FROM 1 FOR 1), position('a' in name) FROM users");

    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"name\" FROM \"public\".\"users\""
    );
    let rendered = built.logical_plan.display_indent().to_string();
    assert!(rendered.contains("Projection"));
    assert!(rendered.contains("PgScan:"));

    let built = builder()
        .build(PlanBuildInput {
            sql: "SELECT extract(day from now())",
            params: Vec::new(),
        })
        .expect("build extract plan");
    assert!(built.scans.is_empty());
    assert!(built
        .logical_plan
        .display_indent()
        .to_string()
        .contains("Projection"));
}

#[test]
fn rejects_multiple_and_non_query_statements() {
    let multiple = builder()
        .build(PlanBuildInput {
            sql: "SELECT 1; SELECT 2",
            params: Vec::new(),
        })
        .unwrap_err();
    assert!(matches!(
        multiple,
        PlanBuildError::MultipleStatements { count: 2 }
    ));

    let ddl = builder()
        .build(PlanBuildInput {
            sql: "CREATE TABLE t (id bigint)",
            params: Vec::new(),
        })
        .unwrap_err();
    assert!(matches!(ddl, PlanBuildError::UnsupportedStatement(_)));
}

#[test]
fn config_defaults_to_single_target_partition() {
    let builder = builder();
    assert_eq!(builder.config().target_partitions, 1);
    assert_eq!(
        builder.config().identifier_max_bytes,
        TEST_IDENTIFIER_MAX_BYTES
    );
}
