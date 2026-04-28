use super::*;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::Column;
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::{lit, Operator};
use pg_statistics::{PgColumnStats, PgScanEstimate, PgUniqueKey};
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
        column_attnums: vec![1, 2, 3],
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("score", DataType::Float64, true),
        ])),
    }
}

fn order_table() -> ResolvedTable {
    ResolvedTable {
        table_oid: 77,
        relation: PgRelation::new(Some("public"), "orders"),
        column_attnums: vec![1, 2],
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ])),
    }
}

fn item_table() -> ResolvedTable {
    ResolvedTable {
        table_oid: 88,
        relation: PgRelation::new(Some("public"), "items"),
        column_attnums: vec![1, 2],
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ])),
    }
}

#[derive(Debug, Clone, Default)]
struct FakeStatsProvider;

impl JoinStatsProvider for FakeStatsProvider {
    fn estimate_scan_sql(&self, sql: &str) -> Result<PgScanEstimate, PlanBuildError> {
        let (rows, width) = if sql.contains("\"users\"") {
            (1_000_000.0, 24)
        } else if sql.contains("\"orders\"") {
            (10.0, 16)
        } else if sql.contains("\"items\"") {
            (100_000.0, 16)
        } else {
            (100.0, 8)
        };
        Ok(PgScanEstimate {
            rows,
            width,
            bytes: rows * f64::from(width),
            startup_cost: 0.0,
            total_cost: rows,
        })
    }

    fn load_column_stats(
        &self,
        relation_oid: u32,
        attnums: &[i16],
    ) -> Result<Vec<PgColumnStats>, PlanBuildError> {
        Ok(attnums
            .iter()
            .copied()
            .map(|attnum| {
                let ndv = match (relation_oid, attnum) {
                    (42, 1) => Some(1_000_000.0),
                    (77, 2) => Some(10.0),
                    (88, 2) => Some(100_000.0),
                    _ => None,
                };
                PgColumnStats {
                    relation_oid,
                    attnum,
                    inherited: false,
                    null_frac: Some(0.0),
                    avg_width: Some(8),
                    stadistinct: ndv,
                    ndv,
                }
            })
            .collect())
    }

    fn load_unique_keys(&self, _relation_oid: u32) -> Result<Vec<PgUniqueKey>, PlanBuildError> {
        Ok(Vec::new())
    }
}

fn builder() -> PlanBuilder<FakeResolver, FakeStatsProvider> {
    PlanBuilder::with_resolver(FakeResolver::new([
        (TableReference::bare("users"), user_table()),
        (TableReference::bare("orders"), order_table()),
        (TableReference::bare("items"), item_table()),
        (
            TableReference::partial("public", "users"),
            ResolvedTable {
                relation: PgRelation::new(Some("public"), "users"),
                ..user_table()
            },
        ),
    ]))
    .with_stats_provider(FakeStatsProvider)
    .with_config(PlanBuilderConfig {
        target_partitions: 1,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        first_scan_id: 1,
        ..PlanBuilderConfig::default()
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

fn build_err(sql: &str) -> PlanBuildError {
    builder()
        .build(PlanBuildInput {
            sql,
            params: Vec::new(),
        })
        .unwrap_err()
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

fn count_cte_ref_nodes(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::Extension(extension) = node {
            if extension.node.as_any().is::<PgCteRefNode>() {
                count += 1;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

fn bottom_join_on(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Join(join) => {
            let left_nested = bottom_join_on(&join.left);
            let right_nested = bottom_join_on(&join.right);
            left_nested.or(right_nested).or_else(|| {
                Some(
                    join.on
                        .iter()
                        .map(|(left, right)| format!("{left} = {right}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            })
        }
        LogicalPlan::Projection(projection) => bottom_join_on(&projection.input),
        LogicalPlan::Filter(filter) => bottom_join_on(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => bottom_join_on(&alias.input),
        _ => None,
    }
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
    assert_eq!(
        spec.arrow_schema().field(1).data_type(),
        &DataType::Utf8View
    );
}

#[test]
fn keeps_pg_text_columns_as_utf8view_for_string_predicates() {
    let built = build_sql("SELECT name FROM users WHERE name = 'alice'");

    assert_eq!(built.scans.len(), 1);
    let spec = &built.scans[0];
    assert_eq!(
        spec.arrow_schema().field(0).data_type(),
        &DataType::Utf8View
    );
    assert_eq!(
        spec.compiled_scan.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (\"name\" = 'alice')"
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
        ..PlanBuilderConfig::default()
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
        ..PlanBuilderConfig::default()
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
fn rejects_exists_projection_that_survives_optimization() {
    let error = build_err("SELECT EXISTS(SELECT 1 FROM orders) FROM users");

    match error {
        PlanBuildError::UnsupportedSubquery(message) => {
            assert!(message.contains("EXISTS"), "{message}");
        }
        other => panic!("expected unsupported subquery error, got {other:?}"),
    }
}

#[test]
fn rewrites_in_subquery_predicate_after_optimization() {
    let built = build_sql("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)");

    assert_eq!(built.scans.len(), 2);
    assert!(!contains_table_scan(&built.logical_plan));
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 2);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"users\""
    );
    assert_eq!(
        built.scans[1].compiled_scan.sql,
        "SELECT \"user_id\" FROM \"public\".\"orders\""
    );
    assert!(built
        .logical_plan
        .display_indent()
        .to_string()
        .contains("LeftSemi Join"));
}

#[test]
fn rewrites_scalar_subquery_after_optimization() {
    let built = build_sql("SELECT id FROM users WHERE id = (SELECT max(user_id) FROM orders)");

    assert_eq!(built.scans.len(), 2);
    assert!(!contains_table_scan(&built.logical_plan));
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 2);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"users\""
    );
    assert_eq!(
        built.scans[1].compiled_scan.sql,
        "SELECT \"user_id\" FROM \"public\".\"orders\""
    );
    let rendered = built.logical_plan.display_indent().to_string();
    assert!(rendered.contains("Inner Join"), "{rendered}");
    assert!(rendered.contains("Aggregate"), "{rendered}");
}

#[test]
fn join_reordering_uses_filtered_statistics_for_inner_join_components() {
    let built = build_sql(
        "SELECT u.id, i.id, o.id \
         FROM users u \
         JOIN items i ON u.id = i.user_id \
         JOIN orders o ON u.id = o.user_id",
    );

    assert_eq!(built.scans.len(), 3);
    assert_eq!(
        bottom_join_on(&built.logical_plan).as_deref(),
        Some("u.id = o.user_id")
    );
}

#[test]
fn join_reordering_preserves_original_output_order() {
    let built = build_sql(
        "SELECT * \
         FROM users u \
         JOIN items i ON u.id = i.user_id \
         JOIN orders o ON u.id = o.user_id",
    );

    let fields = built
        .logical_plan
        .schema()
        .iter()
        .map(|(qualifier, field)| (qualifier.map(ToString::to_string), field.name().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        vec![
            (Some("u".into()), "id".into()),
            (Some("u".into()), "name".into()),
            (Some("u".into()), "score".into()),
            (Some("i".into()), "id".into()),
            (Some("i".into()), "user_id".into()),
            (Some("o".into()), "id".into()),
            (Some("o".into()), "user_id".into()),
        ]
    );
}

#[test]
fn join_reordering_handles_disconnected_cross_join_components() {
    let built = build_sql(
        "SELECT u.id, i.id, o.id \
         FROM users u CROSS JOIN items i CROSS JOIN orders o",
    );

    assert_eq!(built.scans.len(), 3);
    assert!(!contains_table_scan(&built.logical_plan));
    let rendered = built.logical_plan.display_indent().to_string();
    assert!(rendered.contains("Cross Join"), "{rendered}");
}

#[test]
fn join_reordering_can_be_disabled() {
    let built = builder()
        .with_config(PlanBuilderConfig {
            join_reordering_enabled: false,
            ..builder().config()
        })
        .build(PlanBuildInput {
            sql: "SELECT u.id, i.id, o.id \
                  FROM users u \
                  JOIN items i ON u.id = i.user_id \
                  JOIN orders o ON u.id = o.user_id",
            params: Vec::new(),
        })
        .unwrap();

    assert_eq!(
        bottom_join_on(&built.logical_plan).as_deref(),
        Some("u.id = i.user_id")
    );
}

#[test]
fn join_reordering_skips_non_inner_and_filtered_joins() {
    let outer = build_sql(
        "SELECT u.id, i.id \
         FROM users u LEFT JOIN items i ON u.id = i.user_id",
    );
    assert!(outer
        .logical_plan
        .display_indent()
        .to_string()
        .contains("Left Join"));

    let filtered = build_sql(
        "SELECT u.id, i.id \
         FROM users u JOIN items i ON u.id = i.user_id AND u.score > i.id",
    );
    let rendered = filtered.logical_plan.display_indent().to_string();
    assert!(rendered.contains("Inner Join"), "{rendered}");
    assert!(rendered.contains("Filter:"), "{rendered}");
}

#[test]
fn materializes_multi_use_cte_once() {
    let built = build_sql(
        "WITH u AS (SELECT id, score FROM users) \
         SELECT a.id FROM u a JOIN u b ON a.id = b.id",
    );

    assert_eq!(built.scans.len(), 1);
    assert_eq!(built.scans[0].scan_id.get(), 1);
    assert_eq!(count_cte_ref_nodes(&built.logical_plan), 2);
    assert!(!contains_table_scan(&built.logical_plan));
    let rendered = built.logical_plan.display_indent().to_string();
    assert!(rendered.contains("PgCteRef"), "{rendered}");
}

#[test]
fn leaves_single_use_cte_inline() {
    let built = build_sql("WITH u AS (SELECT id FROM users) SELECT id FROM u");

    assert_eq!(built.scans.len(), 1);
    assert_eq!(count_cte_ref_nodes(&built.logical_plan), 0);
    assert!(!contains_table_scan(&built.logical_plan));
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
