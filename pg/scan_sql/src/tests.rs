use super::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion_common::{Column, TableReference};
use datafusion_expr::expr::{BinaryExpr, Cast, InList, Like};
use datafusion_expr::{lit, Expr, Operator};

const TEST_IDENTIFIER_MAX_BYTES: usize = 63;

fn test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ])
}

fn test_relation() -> PgRelation {
    PgRelation::new(Some("public"), "users")
}

fn exact_limit_identifier(prefix: &str) -> String {
    assert!(prefix.len() < TEST_IDENTIFIER_MAX_BYTES);
    format!(
        "{prefix}{}",
        "x".repeat(TEST_IDENTIFIER_MAX_BYTES - prefix.len())
    )
}

#[test]
fn compiles_projection_fetch_hint_and_supported_filters() {
    let schema = test_schema();
    let filters = vec![
        Expr::Column(Column::from_name("id")).gt(lit(10_i64)),
        Expr::Column(Column::from_name("name")).is_not_null(),
    ];

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0, 1]),
        filters: &filters,
        requested_limit: Some(25),
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert_eq!(compiled.requested_limit, Some(25));
    assert_eq!(compiled.sql_limit, None);
    assert_eq!(compiled.selected_columns, vec![0, 1]);
    assert_eq!(compiled.output_columns, vec![0, 1]);
    assert_eq!(compiled.filter_only_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert!(compiled.all_filters_compiled);
    assert_eq!(compiled.residual_filters, Vec::<Expr>::new());
    assert_eq!(
        compiled.sql,
        "SELECT \"id\", \"name\" FROM \"public\".\"users\" WHERE (\"id\" > 10) AND (\"name\" IS NOT NULL)"
    );
}

#[test]
fn compiles_sql_limit_when_requested() {
    let schema = test_schema();
    let filters = vec![Expr::Column(Column::from_name("id")).gt(lit(10_i64))];

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: Some(7),
        limit_lowering: LimitLowering::SqlClause,
    })
    .unwrap();

    assert_eq!(compiled.requested_limit, Some(7));
    assert_eq!(compiled.sql_limit, Some(7));
    assert_eq!(
        compiled.sql,
        "SELECT \"id\" FROM \"public\".\"users\" WHERE (\"id\" > 10) LIMIT 7"
    );
}

#[test]
fn renders_unprojected_scan_sql_with_same_filters_and_sql_limit() {
    let schema = test_schema();
    let filters = vec![Expr::Column(Column::from_name("id")).gt(lit(10_i64))];

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: &filters,
        requested_limit: Some(7),
        limit_lowering: LimitLowering::SqlClause,
    })
    .unwrap();

    assert_eq!(
        render_unprojected_scan_sql(&test_relation(), &compiled),
        "SELECT * FROM \"public\".\"users\" WHERE (\"id\" > 10) LIMIT 7"
    );
}

#[test]
fn computes_filter_only_columns() {
    let schema = test_schema();
    let filters = vec![Expr::Column(Column::from_name("score")).gt(lit(1.5_f64))];

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert_eq!(compiled.selected_columns, vec![0]);
    assert_eq!(compiled.output_columns, vec![0]);
    assert_eq!(compiled.filter_only_columns, vec![2]);
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(
        compiled.sql,
        "SELECT \"id\" FROM \"public\".\"users\" WHERE (\"score\" > 1.5)"
    );
}

#[test]
fn leaves_unsupported_filters_as_residual() {
    let schema = test_schema();
    let supported = Expr::Column(Column::from_name("id")).eq(lit(1_i64));
    let unsupported = Expr::TryCast(datafusion_expr::TryCast::new(
        Box::new(Expr::Column(Column::from_name("score"))),
        DataType::Float64,
    ));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: None,
        filters: &[supported.clone(), unsupported.clone()],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(!compiled.all_filters_compiled);
    assert_eq!(compiled.output_columns, vec![0, 1, 2, 3]);
    assert_eq!(compiled.pushed_filters.len(), 1);
    assert_eq!(compiled.filter_only_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filters, vec![unsupported]);
    assert_eq!(
        compiled.sql,
        "SELECT \"id\", \"name\", \"score\", \"created_at\" FROM \"public\".\"users\" WHERE (\"id\" = 1)"
    );
}

#[test]
fn splits_top_level_and_for_partial_pushdown() {
    let schema = test_schema();
    let supported = Expr::Column(Column::from_name("id")).eq(lit(1_i64));
    let unsupported = Expr::TryCast(datafusion_expr::TryCast::new(
        Box::new(Expr::Column(Column::from_name("name"))),
        DataType::Utf8,
    ));
    let filter = supported.clone().and(unsupported.clone());

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: &[filter],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert_eq!(compiled.selected_columns, vec![1]);
    assert_eq!(compiled.output_columns, vec![1]);
    assert!(!compiled.all_filters_compiled);
    assert_eq!(compiled.pushed_filters.len(), 1);
    assert_eq!(compiled.filter_only_columns, vec![0]);
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filters, vec![unsupported]);
    assert_eq!(
        compiled.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (\"id\" = 1)"
    );
}

#[test]
fn includes_residual_filter_columns_in_output() {
    let schema = test_schema();
    let pushed = Expr::Column(Column::from_name("id")).eq(lit(1_i64));
    let residual = Expr::TryCast(datafusion_expr::TryCast::new(
        Box::new(Expr::Column(Column::from_name("score"))),
        DataType::Float64,
    ));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: &[pushed, residual.clone()],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert_eq!(compiled.selected_columns, vec![1]);
    assert_eq!(compiled.output_columns, vec![1, 2]);
    assert_eq!(compiled.filter_only_columns, vec![0]);
    assert_eq!(compiled.residual_filter_columns, vec![2]);
    assert_eq!(compiled.residual_filters, vec![residual]);
    assert_eq!(
        compiled.sql,
        "SELECT \"name\", \"score\" FROM \"public\".\"users\" WHERE (\"id\" = 1)"
    );
}

#[test]
fn renders_like_filter_without_sql_limit_by_default() {
    let schema = test_schema();
    let filter = Expr::Like(Like::new(
        false,
        Box::new(Expr::Column(Column::from_name("name"))),
        Box::new(lit("al%")),
        None,
        true,
    ));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: &[filter],
        requested_limit: Some(5),
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert_eq!(compiled.requested_limit, Some(5));
    assert_eq!(compiled.sql_limit, None);
    assert_eq!(compiled.output_columns, vec![1]);
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(
        compiled.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (\"name\" ILIKE 'al%')"
    );
}

#[test]
fn uses_dummy_projection_for_zero_column_scan() {
    let schema = test_schema();

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[]),
        filters: &[],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(compiled.uses_dummy_projection);
    assert_eq!(compiled.requested_limit, None);
    assert_eq!(compiled.sql_limit, None);
    assert_eq!(compiled.selected_columns, Vec::<usize>::new());
    assert_eq!(compiled.output_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(
        compiled.sql,
        "SELECT NULL::boolean AS \"__pg_fusion_scan_dummy\" FROM \"public\".\"users\""
    );
}

#[test]
fn errors_on_unknown_column() {
    let schema = test_schema();
    let err = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: None,
        filters: &[Expr::Column(Column::from_name("missing")).eq(lit(1_i64))],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap_err();

    assert_eq!(
        err,
        CompileError::UnknownColumn {
            column: "missing".into()
        }
    );
}

#[test]
fn errors_on_relation_mismatch() {
    let schema = test_schema();
    let err = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: None,
        filters: &[
            Expr::Column(Column::new(Some(TableReference::bare("orders")), "id")).eq(lit(1_i64)),
        ],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap_err();

    assert_eq!(
        err,
        CompileError::UnexpectedRelation {
            column: "orders.id".into(),
            relation: "orders".into(),
            expected: "public.users".into(),
        }
    );
}

#[test]
fn rejects_overlong_column_names() {
    let long_column = format!("score_{}", "x".repeat(80));
    let schema = Schema::new(vec![Field::new("score", DataType::Float64, true)]);
    let filter = Expr::Column(Column::from_name(&long_column)).lt(lit(10.5_f64));

    let err = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap_err();

    assert_eq!(
        err,
        CompileError::OverlongIdentifier {
            kind: "column",
            identifier: long_column,
            max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        }
    );
}

#[test]
fn rejects_overlong_relation_qualifiers() {
    let long_schema = format!("schema_{}", "s".repeat(80));
    let long_table = format!("table_{}", "t".repeat(80));
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let relation = test_relation();
    let qualified = TableReference::partial(long_schema.as_str(), long_table.as_str());
    let filter = Expr::Column(Column::new(Some(qualified), "id")).eq(lit(1_i64));

    let err = compile_scan(CompileScanInput {
        relation: &relation,
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap_err();

    assert_eq!(
        err,
        CompileError::OverlongIdentifier {
            kind: "schema",
            identifier: long_schema,
            max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        }
    );
}

#[test]
fn accepts_exact_limit_column_names() {
    let exact_column = exact_limit_identifier("score_");
    let schema = Schema::new(vec![Field::new(&exact_column, DataType::Float64, true)]);
    let filter = Expr::Column(Column::from_name(&exact_column)).lt(lit(10.5_f64));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert_eq!(compiled.output_columns, vec![0]);
    assert_eq!(
        compiled.sql,
        format!(
            "SELECT \"{exact_column}\" FROM \"public\".\"users\" WHERE (\"{exact_column}\" < 10.5)"
        )
    );
}

#[test]
fn leaves_regex_filters_residual() {
    let schema = test_schema();
    let regex = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("name"))),
        Operator::RegexMatch,
        Box::new(lit("^al.*")),
    ));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: std::slice::from_ref(&regex),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(!compiled.all_filters_compiled);
    assert_eq!(compiled.output_columns, vec![1]);
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filters, vec![regex]);
    assert_eq!(compiled.sql, "SELECT \"name\" FROM \"public\".\"users\"");
}

#[test]
fn leaves_non_finite_float_literals_residual() {
    let schema = test_schema();
    let filter = Expr::Column(Column::from_name("score")).lt(lit(f64::INFINITY));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(!compiled.all_filters_compiled);
    assert_eq!(compiled.output_columns, vec![1, 2]);
    assert_eq!(compiled.residual_filter_columns, vec![2]);
    assert_eq!(compiled.residual_filters, vec![filter]);
    assert_eq!(
        compiled.sql,
        "SELECT \"name\", \"score\" FROM \"public\".\"users\""
    );
}

#[test]
fn leaves_temporal_cast_targets_residual() {
    let schema = test_schema();
    let filter = Expr::Cast(Cast::new(
        Box::new(Expr::Column(Column::from_name("id"))),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ))
    .is_not_null();

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(!compiled.all_filters_compiled);
    assert_eq!(compiled.output_columns, vec![1, 0]);
    assert_eq!(compiled.residual_filter_columns, vec![0]);
    assert_eq!(compiled.residual_filters, vec![filter]);
    assert_eq!(
        compiled.sql,
        "SELECT \"name\", \"id\" FROM \"public\".\"users\""
    );
}

#[test]
fn renders_nested_negative_without_comment_syntax() {
    let schema = test_schema();
    let filter = Expr::Negative(Box::new(lit(-1_i64))).eq(lit(1_i64));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(compiled.all_filters_compiled);
    assert!(!compiled.sql.contains("--"));
    assert_eq!(
        compiled.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE ((-(-1)) = 1)"
    );
}

#[test]
fn folds_empty_in_list_to_false_with_postgresql_semantics() {
    let schema = test_schema();
    let filter = Expr::InList(InList::new(
        Box::new(Expr::Column(Column::from_name("id"))),
        vec![],
        false,
    ));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(compiled.all_filters_compiled);
    assert_eq!(compiled.output_columns, vec![1]);
    assert_eq!(compiled.filter_only_columns, Vec::<usize>::new());
    assert_eq!(
        compiled.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (FALSE)"
    );
}

#[test]
fn compiles_int8_cast_using_postgresql_smallint_target() {
    let schema = test_schema();
    let filter = Expr::Cast(Cast::new(
        Box::new(Expr::Column(Column::from_name("id"))),
        DataType::Int8,
    ))
    .gt(lit(5_i16));

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .unwrap();

    assert!(compiled.all_filters_compiled);
    assert_eq!(compiled.output_columns, vec![1]);
    assert_eq!(compiled.filter_only_columns, vec![0]);
    assert_eq!(
        compiled.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (CAST(\"id\" AS SMALLINT) > 5)"
    );
}
