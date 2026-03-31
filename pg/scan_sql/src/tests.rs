use super::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion_common::{Column, TableReference};
use datafusion_expr::expr::{BinaryExpr, Cast, InList, Like};
use datafusion_expr::{lit, Expr, Operator};

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

#[test]
fn compiles_projection_limit_and_supported_filters() {
    let schema = test_schema();
    let filters = vec![
        Expr::Column(Column::from_name("id")).gt(lit(10_i64)),
        Expr::Column(Column::from_name("name")).is_not_null(),
    ];

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        projection: Some(&[0, 1]),
        filters: &filters,
        limit: Some(25),
    })
    .unwrap();

    assert_eq!(compiled.selected_columns, vec![0, 1]);
    assert_eq!(compiled.output_columns, vec![0, 1]);
    assert_eq!(compiled.filter_only_columns, Vec::<usize>::new());
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert!(compiled.all_filters_compiled);
    assert_eq!(compiled.residual_filters, Vec::<Expr>::new());
    assert_eq!(
        compiled.sql,
        "SELECT \"id\", \"name\" FROM \"public\".\"users\" WHERE (\"id\" > 10) AND (\"name\" IS NOT NULL) LIMIT 25"
    );
}

#[test]
fn computes_filter_only_columns() {
    let schema = test_schema();
    let filters = vec![Expr::Column(Column::from_name("score")).gt(lit(1.5_f64))];

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        projection: Some(&[0]),
        filters: &filters,
        limit: None,
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
        projection: None,
        filters: &[supported.clone(), unsupported.clone()],
        limit: None,
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
        projection: Some(&[1]),
        filters: &[filter],
        limit: None,
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
        projection: Some(&[1]),
        filters: &[pushed, residual.clone()],
        limit: None,
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
fn renders_like_filter_and_limit() {
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
        projection: Some(&[1]),
        filters: &[filter],
        limit: Some(5),
    })
    .unwrap();

    assert_eq!(compiled.output_columns, vec![1]);
    assert_eq!(compiled.residual_filter_columns, Vec::<usize>::new());
    assert_eq!(
        compiled.sql,
        "SELECT \"name\" FROM \"public\".\"users\" WHERE (\"name\" ILIKE 'al%') LIMIT 5"
    );
}

#[test]
fn uses_dummy_projection_for_zero_column_scan() {
    let schema = test_schema();

    let compiled = compile_scan(CompileScanInput {
        relation: &test_relation(),
        schema: &schema,
        projection: Some(&[]),
        filters: &[],
        limit: None,
    })
    .unwrap();

    assert!(compiled.uses_dummy_projection);
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
        projection: None,
        filters: &[Expr::Column(Column::from_name("missing")).eq(lit(1_i64))],
        limit: None,
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
        projection: None,
        filters: &[
            Expr::Column(Column::new(Some(TableReference::bare("orders")), "id")).eq(lit(1_i64)),
        ],
        limit: None,
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
        projection: Some(&[1]),
        filters: std::slice::from_ref(&regex),
        limit: None,
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
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        limit: None,
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
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        limit: None,
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
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        limit: None,
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
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        limit: None,
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
        projection: Some(&[1]),
        filters: std::slice::from_ref(&filter),
        limit: None,
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
