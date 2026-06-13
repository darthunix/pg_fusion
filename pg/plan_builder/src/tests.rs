use super::*;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Column, DFSchema, ScalarValue, TableReference};
use datafusion_expr::expr::{BinaryExpr, GroupingSet};
use datafusion_expr::logical_plan::{Aggregate, Distinct, DistinctOn, Values};
use datafusion_expr::{lit, Operator, TableSource};
use df_catalog::{ResolvedColumn, ResolvedTable};
use pg_type::PgTypeRef;
use scan_sql::{pg_type_metadata, PgRelation};
use std::sync::Arc;

const TEST_IDENTIFIER_MAX_BYTES: usize = 63;

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
        columns: vec![
            resolved_column(1, "id", pg_type(pgrx::pg_sys::INT8OID), false),
            resolved_column(2, "name", pg_type(pgrx::pg_sys::TEXTOID), true),
            resolved_column(3, "score", pg_type(pgrx::pg_sys::FLOAT8OID), true),
        ],
    }
}

fn resolved_column(attnum: i16, name: &str, pg_type: PgTypeRef, nullable: bool) -> ResolvedColumn {
    ResolvedColumn {
        attnum,
        name: name.to_owned(),
        pg_type,
        nullable,
    }
}

fn pg_type(oid: pgrx::pg_sys::Oid) -> PgTypeRef {
    PgTypeRef::new(u32::from(oid), -1, 0)
}

fn user_table_source() -> Arc<dyn TableSource> {
    Arc::new(PgPlanningTableSource::new(user_table())) as Arc<dyn TableSource>
}

fn plan_builder_config(first_scan_id: u64) -> PlanBuilderConfig {
    PlanBuilderConfig {
        target_partitions: 1,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        first_scan_id,
        ..PlanBuilderConfig::default()
    }
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

fn unsupported_score_residual_filter() -> Expr {
    Expr::IsNotNull(Box::new(Expr::TryCast(datafusion_expr::TryCast::new(
        Box::new(Expr::Column(Column::from_name("score"))),
        DataType::Float64,
    ))))
}

#[test]
fn preplanned_logical_plan_uses_shared_scan_building() {
    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("id"))),
        Operator::Gt,
        Box::new(lit(10_i64)),
    ));
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        user_table_source(),
        Some(vec![0]),
        vec![filter],
        None,
    )
    .unwrap();

    let built =
        build_preplanned_logical_plan(LogicalPlan::TableScan(table_scan), plan_builder_config(7))
            .expect("preplanned logical plan should build scan leaves");

    assert_eq!(built.scan_plan.scans.len(), 1);
    assert_eq!(built.scan_plan.scans[0].scan_id.get(), 7);
    assert_eq!(
        built.scan_plan.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"users\" WHERE (\"id\" > 10)"
    );
    assert!(!contains_table_scan(&built.logical_plan));
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 1);
}

#[test]
fn frontend_logical_plan_builds_scans_before_optimizer_can_fold_pg_typed_literals() {
    let left = Expr::Literal(
        datafusion_common::ScalarValue::Utf8View(Some("a ".into())),
        Some(pg_type_metadata(1042, 6, 0)),
    );
    let right = Expr::Literal(
        datafusion_common::ScalarValue::Utf8View(Some("a".into())),
        Some(pg_type_metadata(1042, 5, 0)),
    );
    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Eq,
        Box::new(right),
    ));
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        user_table_source(),
        Some(vec![0]),
        vec![filter],
        None,
    )
    .unwrap();

    let built =
        build_frontend_logical_plan(LogicalPlan::TableScan(table_scan), plan_builder_config(7))
            .expect("frontend logical plan should build scans before DataFusion optimization");

    assert_eq!(built.scan_plan.scans.len(), 1);
    assert_eq!(
        built.scan_plan.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"users\" WHERE (CAST('a ' AS CHARACTER(2)) = CAST('a' AS CHARACTER(1)))"
    );
    assert!(!contains_table_scan(&built.logical_plan));
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 1);
}

#[test]
fn regex_filters_compile_into_postgres_scan_sql() {
    let regex_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("name"))),
        Operator::RegexMatch,
        Box::new(lit("^a")),
    ));
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        user_table_source(),
        Some(vec![1]),
        vec![regex_filter],
        Some(10),
    )
    .unwrap();
    let mut scan_builder = PgScanBuilder::new(plan_builder_config(1));

    let plan = scan_builder
        .build_scans(LogicalPlan::TableScan(table_scan))
        .expect("build regex scan");

    assert_eq!(scan_builder.scans.len(), 1);
    assert_eq!(
        scan_builder.scans[0].compiled_scan.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (\"name\" ~ '^a')"
    );
    assert!(scan_builder.scans[0]
        .compiled_scan
        .residual_filters
        .is_empty());
    assert_eq!(
        scan_builder.scans[0].fetch_hints.planner_fetch_hint,
        Some(10)
    );
    assert_eq!(scan_builder.scans[0].fetch_hints.local_row_cap, Some(10));
    assert!(!matches!(plan, LogicalPlan::Filter(_)));
}

#[test]
fn frontend_logical_plan_optimizes_distinct_on_after_scan_building() {
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        user_table_source(),
        Some(vec![0, 2]),
        Vec::new(),
        None,
    )
    .unwrap();
    let input = LogicalPlan::TableScan(table_scan);
    let distinct_on = LogicalPlan::Distinct(Distinct::On(
        DistinctOn::try_new(
            vec![Expr::Column(Column::from_name("id"))],
            vec![
                Expr::Column(Column::from_name("id")),
                Expr::Column(Column::from_name("score")),
            ],
            Some(vec![
                Expr::Column(Column::from_name("id")).sort(true, false),
                Expr::Column(Column::from_name("score")).sort(false, false),
            ]),
            Arc::new(input),
        )
        .expect("distinct on plan"),
    ));

    let built = build_frontend_logical_plan(distinct_on, plan_builder_config(7))
        .expect("frontend logical plan should optimize DISTINCT ON");

    assert_eq!(built.scan_plan.scans.len(), 1);
    assert!(!contains_table_scan(&built.logical_plan));
    assert_eq!(count_pg_scan_nodes(&built.logical_plan), 1);
    let rendered = built.logical_plan.display_indent().to_string();
    assert!(rendered.contains("Aggregate"), "{rendered}");
    assert!(rendered.contains("first_value"), "{rendered}");
}

#[test]
fn frontend_logical_plan_rewrites_grouping_function_before_runtime() {
    let schema = Arc::new(
        DFSchema::try_from(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
            .expect("values schema"),
    );
    let input = LogicalPlan::Values(Values {
        schema,
        values: vec![vec![Expr::Literal(ScalarValue::Int32(None), None)]],
    });
    let grouping_arg = Expr::Column(Column::from_name("a"));
    let aggregate = LogicalPlan::Aggregate(
        Aggregate::try_new(
            Arc::new(input),
            vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                vec![grouping_arg.clone()],
                Vec::new(),
            ]))],
            vec![
                datafusion::functions_aggregate::grouping::grouping_udaf().call(vec![grouping_arg])
            ],
        )
        .expect("grouping aggregate plan"),
    );

    let built = build_frontend_logical_plan(aggregate, plan_builder_config(7))
        .expect("frontend logical plan should analyze GROUPING");

    let rendered = built.logical_plan.display_indent().to_string();
    assert!(!rendered.contains("grouping("), "{rendered}");
    assert!(rendered.contains("__grouping_id"), "{rendered}");
}

#[test]
fn residual_filters_are_restored_and_extra_columns_projected_away() {
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        user_table_source(),
        Some(vec![0]),
        vec![unsupported_score_residual_filter()],
        None,
    )
    .unwrap();
    let mut scan_builder = PgScanBuilder::new(plan_builder_config(1));

    let plan = scan_builder
        .build_scans(LogicalPlan::TableScan(table_scan))
        .expect("build residual scan");

    assert_eq!(scan_builder.scans.len(), 1);
    assert_eq!(
        scan_builder.scans[0].compiled_scan.output_columns,
        vec![0, 2]
    );
    assert_eq!(
        scan_builder.scans[0].compiled_scan.residual_filter_columns,
        vec![2]
    );
    assert_eq!(plan.schema().fields().len(), 1);
    assert_eq!(plan.schema().field(0).name(), "id");
    assert!(format!("{}", plan.display_indent()).contains("Filter"));
    assert!(matches!(plan, LogicalPlan::Projection(_)));
}

#[test]
fn residual_filters_disable_local_row_cap_but_keep_planner_hint() {
    let table_scan = TableScan::try_new(
        TableReference::bare("users"),
        user_table_source(),
        None,
        vec![unsupported_score_residual_filter()],
        Some(10),
    )
    .unwrap();
    let mut scan_builder = PgScanBuilder::new(plan_builder_config(1));

    let _ = scan_builder
        .build_scans(LogicalPlan::TableScan(table_scan))
        .expect("build residual scan");

    assert_eq!(
        scan_builder.scans[0].fetch_hints.planner_fetch_hint,
        Some(10)
    );
    assert_eq!(scan_builder.scans[0].fetch_hints.local_row_cap, None);
}

#[test]
fn config_defaults_to_single_target_partition() {
    let config = PlanBuilderConfig::default();
    assert_eq!(config.target_partitions, 1);
    assert_eq!(
        config.identifier_max_bytes,
        (pgrx::pg_sys::NAMEDATALEN as usize).saturating_sub(1)
    );
}
