#![allow(clippy::module_inception)]

use super::*;
use crate::typed_query::PgTypeRef;

#[cfg(test)]
fn typed_null(pg_type: PgTypeRef) -> Result<ScalarValue, PgFrontendError> {
    pg_type::typed_null_scalar(pg_type).map_err(|err| PgFrontendError::unsupported(err.to_string()))
}

#[cfg(test)]
fn arrow_type(pg_type: PgTypeRef) -> Option<arrow_schema::DataType> {
    arrow_type_for_pg_type(pg_type)
}

#[cfg(test)]
fn oid_u32(oid: pgrx::pg_sys::Oid) -> u32 {
    u32::from(oid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_query::{
        AggregateFunction, BoolOp, BooleanTestKind, ColumnRef, Const, CteDef, CteRangeRef,
        DistinctSpec, FromItem, GroupingSetSpec, JoinKind, Param, ParamKind, PgConstValue,
        QueryCommand, QueryOperator, QueryUnaryOperator, ScalarFunction, SortKey, SubqueryRef, Var,
        WindowFrameBound, WindowFrameSpec, WindowFrameUnits, WindowFunctionKind, WindowSpec,
    };
    use arrow_schema::{DataType, IntervalUnit};
    use datafusion::arrow::array::Array;
    use df_catalog::CatalogResolver;

    #[test]
    fn accepts_row_locks_as_read_only_marker() {
        let mut query = base_query();
        query.has_row_marks = true;
        validate_supported_query_shape(&query).expect("row marks do not block frontend planning");
    }

    #[test]
    fn validates_projection_expression_shapes() {
        assert!(validate_target_expr(&target_var()).is_ok());

        let binary = target_binary_op();
        assert!(validate_target_expr(&binary).is_ok());

        let relabeled_binary = QueryExpr::RelabelType(Box::new(binary));
        assert!(validate_target_expr(&relabeled_binary).is_ok());
    }

    #[test]
    fn rejects_parameters_in_targets_and_filters() {
        let param = QueryExpr::Param(Param {
            kind: ParamKind::External,
            id: 1,
            pg_type: int4_type(),
        });

        assert_target_expr_unsupported_contains(&param, "parameters");

        let resolved = resolved_table();
        let query = query_for_resolved_table();
        let ctx = compile_context([(1, resolved)]);
        let err = compile_expr(&param, &query, &ctx).expect_err("Param must be rejected");
        assert!(
            err.to_string().contains("parameters"),
            "error {err} must mention parameters"
        );
    }

    #[test]
    fn scan_projection_uses_only_visible_target_vars_without_sort() {
        let resolved = resolved_table();
        let mut query = query_for_resolved_table();
        query.targets = vec![
            target("second", target_var_attnum(2)),
            target(
                "is_first_null",
                QueryExpr::NullTest {
                    arg: Box::new(target_var_attnum(1)),
                    is_null: true,
                },
            ),
            target("second_again", target_var_attnum(2)),
            Target {
                expr: target_var_attnum(3),
                name: Some("hidden".into()),
                pg_type: int4_type(),
                resno: 4,
                ressortgroupref: 0,
                resjunk: true,
            },
        ];

        let projection = target_projection(&query, 1, &resolved, false).unwrap();
        assert_eq!(projection, vec![1, 0]);
    }

    #[test]
    fn scan_projection_keeps_resjunk_sort_vars() {
        let resolved = resolved_table();
        let mut query = query_for_resolved_table();
        query.targets = vec![
            target("first", target_var_attnum(1)),
            Target {
                expr: target_var_attnum(2),
                name: Some("hidden_sort".into()),
                pg_type: int4_type(),
                resno: 2,
                ressortgroupref: 1,
                resjunk: true,
            },
        ];
        query.sort = vec![SortKey {
            target_ref: 1,
            asc: true,
            nulls_first: false,
        }];

        let projection = target_projection(&query, 1, &resolved, true).unwrap();
        assert_eq!(projection, vec![0, 1]);
    }

    #[test]
    fn scan_projection_is_empty_for_constant_only_targets() {
        let resolved = resolved_table();
        let mut query = query_for_resolved_table();
        query.targets = vec![target(
            "one",
            QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            }),
        )];

        let projection = target_projection(&query, 1, &resolved, false).unwrap();
        assert!(projection.is_empty());
    }

    #[test]
    fn compile_query_uses_distinct_all_for_plain_distinct() {
        let mut query = query_for_resolved_table();
        query.targets = vec![
            target_with_ref("first", target_var_attnum(1), 1, 0, false),
            target_with_ref("second", target_var_attnum(2), 2, 0, false),
        ];
        query.distinct = DistinctSpec::FullRow;

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("plain DISTINCT query should lower into a typed logical plan");

        let LogicalPlan::Distinct(Distinct::All(input)) = &output.logical_plan else {
            panic!(
                "plain DISTINCT should compile to Distinct::All: {}",
                output.logical_plan.display_indent()
            );
        };
        assert!(
            matches!(input.as_ref(), LogicalPlan::Projection(_)),
            "DISTINCT input should be the target projection"
        );
    }

    #[test]
    fn compile_query_uses_distinct_on_with_hidden_sort_key() {
        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![
                ColumnRef {
                    attnum: 1,
                    name: "a".into(),
                    pg_type: int4_type(),
                    nullable: false,
                },
                ColumnRef {
                    attnum: 2,
                    name: "b".into(),
                    pg_type: int4_type(),
                    nullable: false,
                },
            ],
            rows: vec![
                int4_row(1, 2),
                int4_row(1, 1),
                int4_row(2, 4),
                int4_row(2, 3),
            ],
        }];
        query.targets = vec![
            target_with_ref("a", target_var_attnum(1), 1, 1, false),
            target_with_ref("b", target_var_attnum(2), 2, 2, true),
        ];
        query.distinct = DistinctSpec::On {
            target_refs: vec![1],
        };
        query.sort = vec![
            SortKey {
                target_ref: 1,
                asc: true,
                nulls_first: false,
            },
            SortKey {
                target_ref: 2,
                asc: true,
                nulls_first: false,
            },
        ];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("DISTINCT ON query should lower into a typed logical plan");
        let LogicalPlan::Distinct(Distinct::On(distinct_on)) = &output.logical_plan else {
            panic!(
                "DISTINCT ON should compile to Distinct::On without an outer Sort: {}",
                output.logical_plan.display_indent()
            );
        };
        assert_eq!(distinct_on.on_expr.len(), 1);
        assert_eq!(distinct_on.select_expr.len(), 1);
        assert_eq!(distinct_on.sort_expr.as_ref().unwrap().len(), 2);
        assert_eq!(distinct_on.input.schema().fields().len(), 2);
        assert_eq!(distinct_on.schema.fields().len(), 1);
    }

    #[test]
    fn compile_query_lowers_integer_arithmetic_to_checked_udf() {
        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: int4_type(),
                nullable: false,
            }],
            rows: vec![vec![int4_const(1)]],
        }];
        query.targets = vec![target("sum", target_binary_op())];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("integer arithmetic query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(
            rendered.contains("pg_fusion_int_add_checked"),
            "integer addition must use PostgreSQL-compatible checked UDF: {rendered}"
        );

        let decoded = roundtrip_plan(output.logical_plan);
        let decoded_rendered = decoded.display_indent().to_string();
        assert!(
            decoded_rendered.contains("pg_fusion_int_add_checked"),
            "encoded integer arithmetic plan must decode with the checked UDF: {decoded_rendered}"
        );
    }

    #[test]
    fn compile_query_lowers_text_typmod_casts_to_pg_udfs() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![
            Target {
                expr: eq_expr(
                    QueryExpr::Cast {
                        arg: Box::new(text_const("abc")),
                        pg_type: varchar_type(2),
                    },
                    text_const("ab"),
                ),
                name: Some("varchar_matches".into()),
                pg_type: bool_type(),
                resno: 1,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: eq_expr(
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(3),
                    },
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(1),
                    },
                ),
                name: Some("bpchar_matches".into()),
                pg_type: bool_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::Cast {
                    arg: Box::new(text_const("a")),
                    pg_type: bpchar_type(3),
                },
                name: Some("bpchar_value".into()),
                pg_type: bpchar_type(3),
                resno: 3,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("text typmod casts should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(
            rendered.contains("pg_fusion_varchar_typmod"),
            "varchar typmod cast must use PostgreSQL-compatible UDF: {rendered}"
        );
        assert!(
            rendered.contains("pg_fusion_bpchar_typmod"),
            "bpchar typmod cast must use PostgreSQL-compatible UDF: {rendered}"
        );
        assert!(
            rendered.contains("pg_fusion_bpchar_cmp_key"),
            "bpchar equality must normalize trailing-space semantics: {rendered}"
        );

        let decoded = roundtrip_plan(output.logical_plan);
        let decoded_rendered = decoded.display_indent().to_string();
        assert!(
            decoded_rendered.contains("pg_fusion_varchar_typmod"),
            "encoded plan must decode with varchar typmod UDF: {decoded_rendered}"
        );
        assert!(
            decoded_rendered.contains("pg_fusion_bpchar_typmod"),
            "encoded plan must decode with bpchar typmod UDF: {decoded_rendered}"
        );
        assert!(
            decoded_rendered.contains("pg_fusion_bpchar_cmp_key"),
            "encoded plan must decode with bpchar comparison UDF: {decoded_rendered}"
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(decoded).await?;
            dataframe.collect().await
        })
        .expect("text typmod cast plan should execute in DataFusion");
        let bools = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .expect("varchar equality should produce BooleanArray");
        assert!(bools.value(0));
        let bpchar_bools = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .expect("bpchar equality should produce BooleanArray");
        assert!(bpchar_bools.value(0));
        let bpchar = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
            .expect("bpchar typmod cast should produce StringViewArray");
        assert_eq!(bpchar.value(0), "a  ");
    }

    #[test]
    fn compile_query_lowers_bpchar_distinct_comparisons_to_pg_udf() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![
            Target {
                expr: binary_op_expr(
                    QueryOperator::NotEq,
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(3),
                    },
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(1),
                    },
                ),
                name: Some("bpchar_not_eq".into()),
                pg_type: bool_type(),
                resno: 1,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: binary_op_expr(
                    QueryOperator::IsDistinctFrom,
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(3),
                    },
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(1),
                    },
                ),
                name: Some("bpchar_is_distinct".into()),
                pg_type: bool_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: binary_op_expr(
                    QueryOperator::IsNotDistinctFrom,
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(3),
                    },
                    QueryExpr::Cast {
                        arg: Box::new(text_const("a")),
                        pg_type: bpchar_type(1),
                    },
                ),
                name: Some("bpchar_is_not_distinct".into()),
                pg_type: bool_type(),
                resno: 3,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("bpchar distinct comparisons should lower into a typed logical plan");

        let decoded = roundtrip_plan(output.logical_plan);
        let rendered = decoded.display_indent().to_string();
        assert!(
            rendered.contains("pg_fusion_bpchar_cmp_key"),
            "bpchar distinct comparisons must normalize trailing spaces: {rendered}"
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(decoded).await?;
            dataframe.collect().await
        })
        .expect("bpchar distinct comparison plan should execute in DataFusion");
        let bools = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .expect("bpchar not-eq target should produce BooleanArray");
        assert!(!bools.value(0));
        let bools = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .expect("bpchar is-distinct target should produce BooleanArray");
        assert!(!bools.value(0));
        let bools = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .expect("bpchar is-not-distinct target should produce BooleanArray");
        assert!(bools.value(0));
    }

    #[test]
    fn compile_query_lowers_bpchar_length_to_pg_udf() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![Target {
            expr: QueryExpr::FunctionCall {
                func: ScalarFunction::Length,
                args: vec![QueryExpr::Cast {
                    arg: Box::new(text_const("a")),
                    pg_type: bpchar_type(3),
                }],
                pg_type: int4_type(),
            },
            name: Some("bpchar_len".into()),
            pg_type: int4_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("bpchar length should lower into a typed logical plan");

        let decoded = roundtrip_plan(output.logical_plan);
        let rendered = decoded.display_indent().to_string();
        assert!(
            rendered.contains("pg_fusion_bpchar_length"),
            "length(bpchar) must use PostgreSQL-compatible UDF: {rendered}"
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(decoded).await?;
            dataframe.collect().await
        })
        .expect("bpchar length plan should execute in DataFusion");
        let lengths = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("bpchar length should produce Int32Array");
        assert_eq!(lengths.value(0), 1);
    }

    #[test]
    fn compile_query_lowers_boolean_concat_args_to_pg_boolout_udf() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![
            Target {
                expr: QueryExpr::FunctionCall {
                    func: ScalarFunction::Concat,
                    args: vec![QueryExpr::Bool {
                        op: BoolOp::And,
                        args: vec![bool_const(true), bool_const(false)],
                    }],
                    pg_type: text_type(),
                },
                name: Some("concat_bool".into()),
                pg_type: text_type(),
                resno: 1,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::FunctionCall {
                    func: ScalarFunction::ConcatWs,
                    args: vec![
                        text_const("|"),
                        QueryExpr::BooleanTest {
                            arg: Box::new(bool_const(true)),
                            kind: BooleanTestKind::IsTrue,
                        },
                        QueryExpr::NullTest {
                            arg: Box::new(int4_null()),
                            is_null: true,
                        },
                    ],
                    pg_type: text_type(),
                },
                name: Some("concat_ws_bool".into()),
                pg_type: text_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("boolean concat arguments should lower into a typed logical plan");

        let decoded = roundtrip_plan(output.logical_plan);
        let rendered = decoded.display_indent().to_string();
        assert!(
            rendered.contains("pg_fusion_boolout"),
            "boolean concat arguments must use single-evaluation PostgreSQL boolout UDF: {rendered}"
        );
        assert!(
            !rendered.contains("CASE"),
            "boolean concat arguments must not duplicate input evaluation through CASE: {rendered}"
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(decoded).await?;
            dataframe.collect().await
        })
        .expect("boolean concat plan should execute in DataFusion");
        let concat = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("concat bool should produce StringArray");
        assert_eq!(concat.value(0), "f");
        let concat_ws = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("concat_ws bool should produce StringArray");
        assert_eq!(concat_ws.value(0), "t|t");
    }

    #[test]
    fn compile_query_lowers_numeric_round_trunc_to_decimal_results() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![
            Target {
                expr: QueryExpr::FunctionCall {
                    func: ScalarFunction::Round,
                    args: vec![numeric_const("1.234"), int4_const(2)],
                    pg_type: numeric_type(),
                },
                name: Some("rounded".into()),
                pg_type: numeric_type(),
                resno: 1,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::FunctionCall {
                    func: ScalarFunction::Trunc,
                    args: vec![numeric_const("1.234"), int4_const(2)],
                    pg_type: numeric_type(),
                },
                name: Some("truncated".into()),
                pg_type: numeric_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("numeric round/trunc should lower into a typed logical plan");

        let decoded = roundtrip_plan(output.logical_plan);
        let schema = decoded.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(38, 2));

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(decoded).await?;
            dataframe.collect().await
        })
        .expect("numeric round/trunc plan should execute in DataFusion");
        let rounded = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .expect("round(numeric, int4) should produce Decimal128Array");
        let truncated = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .expect("trunc(numeric, int4) should produce Decimal128Array");
        assert_eq!(rounded.value(0), 123);
        assert_eq!(truncated.value(0), 123);
    }

    #[test]
    fn numeric_arithmetic_policy_bounds_tpch_q01_scales() {
        let extended_price = var_attnum(1, 1, numeric_typmod_type(15, 2));
        let discount = var_attnum(1, 2, numeric_typmod_type(15, 2));
        let tax = var_attnum(1, 3, numeric_typmod_type(15, 2));
        let discounted = numeric_op_expr(
            QueryOperator::Multiply,
            extended_price,
            numeric_op_expr(QueryOperator::Minus, numeric_const("1"), discount),
        );
        let charge = numeric_op_expr(
            QueryOperator::Multiply,
            discounted.clone(),
            numeric_op_expr(QueryOperator::Plus, numeric_const("1"), tax),
        );

        assert_eq!(numeric_arithmetic_expr_scale(&discounted).unwrap(), Some(4));
        assert_eq!(numeric_arithmetic_expr_scale(&charge).unwrap(), Some(6));

        let QueryExpr::BinaryOp { left, right, .. } = &charge else {
            panic!("charge expression should be a binary op");
        };
        let policy =
            numeric_decimal_arithmetic_policy(QueryOperator::Multiply, numeric_type(), left, right)
                .unwrap()
                .expect("numeric multiplication should use decimal policy");
        assert_eq!(policy.left_type, DataType::Decimal128(38, 4));
        assert_eq!(policy.right_type, DataType::Decimal128(38, 2));
        assert_eq!(policy.result_type, DataType::Decimal128(38, 6));
    }

    #[test]
    fn numeric_arithmetic_policy_preserves_literal_trailing_zero_scale() {
        assert_eq!(
            numeric_arithmetic_expr_scale(&numeric_const("1.20")).unwrap(),
            Some(2)
        );
        assert_eq!(
            numeric_arithmetic_expr_scale(&numeric_const("1.2000")).unwrap(),
            Some(4)
        );
        assert_eq!(
            numeric_arithmetic_expr_scale(&numeric_const("1.0000")).unwrap(),
            Some(4)
        );
    }

    #[test]
    fn compile_query_executes_numeric_addition_with_literal_display_scale() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![Target {
            expr: numeric_op_expr(
                QueryOperator::Plus,
                numeric_const("1.20"),
                numeric_const("3"),
            ),
            name: Some("sum".into()),
            pg_type: numeric_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("numeric addition should compile");
        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(output.logical_plan).await?;
            dataframe.collect().await
        })
        .expect("numeric addition should execute in DataFusion");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .expect("numeric addition should produce Decimal128Array");
        assert_eq!(values.value(0), 420);
    }

    #[test]
    fn compile_query_executes_numeric_multiplication_with_literal_display_scale() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![Target {
            expr: numeric_op_expr(
                QueryOperator::Multiply,
                numeric_const("1.20"),
                numeric_const("3"),
            ),
            name: Some("product".into()),
            pg_type: numeric_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("numeric multiplication should compile");
        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(output.logical_plan).await?;
            dataframe.collect().await
        })
        .expect("numeric multiplication should execute in DataFusion");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .expect("numeric multiplication should produce Decimal128Array");
        assert_eq!(values.value(0), 360);
    }

    #[test]
    fn numeric_values_target_uses_literal_display_scale() {
        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "n".into(),
                pg_type: numeric_type(),
                nullable: false,
            }],
            rows: vec![vec![numeric_const("1.20")]],
        }];
        query.targets = vec![numeric_target("n", var_attnum(1, 1, numeric_type()))];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("bare numeric VALUES source should compile");

        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
    }

    #[test]
    fn numeric_values_mixed_scales_use_column_scale() {
        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "n".into(),
                pg_type: numeric_type(),
                nullable: false,
            }],
            rows: vec![vec![numeric_const("1.2")], vec![numeric_const("1.20")]],
        }];
        query.targets = vec![numeric_target("n", var_attnum(1, 1, numeric_type()))];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("mixed-scale bare numeric VALUES source should compile");

        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
    }

    #[test]
    fn numeric_typmod_target_uses_typmod_scale() {
        let numeric_10_2 = numeric_typmod_type(10, 2);
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![Target {
            expr: QueryExpr::Cast {
                arg: Box::new(numeric_const("1.20")),
                pg_type: numeric_10_2,
            },
            name: Some("n".into()),
            pg_type: numeric_10_2,
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("typmodded numeric target should compile");

        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn values_numeric_column_scale_preserves_literal_trailing_zeros() {
        let values = ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: numeric_type(),
                nullable: false,
            }],
            rows: vec![vec![numeric_const("1.20")]],
        };

        assert_eq!(
            values_column_data_type(&values, 0, numeric_type()).unwrap(),
            Some(DataType::Decimal128(38, 2))
        );
    }

    #[test]
    fn values_numeric_column_scale_uses_arithmetic_policy_for_multiply() {
        let values = ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: numeric_type(),
                nullable: false,
            }],
            rows: vec![vec![numeric_op_expr(
                QueryOperator::Multiply,
                numeric_const("1.20"),
                numeric_const("3.40"),
            )]],
        };

        assert_eq!(
            values_column_data_type(&values, 0, numeric_type()).unwrap(),
            Some(DataType::Decimal128(38, 4))
        );
    }

    #[test]
    fn values_numeric_column_scale_uses_arithmetic_policy_for_division() {
        let values = ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: numeric_type(),
                nullable: false,
            }],
            rows: vec![vec![numeric_op_expr(
                QueryOperator::Divide,
                numeric_const("1"),
                numeric_const("3"),
            )]],
        };

        assert_eq!(
            values_column_data_type(&values, 0, numeric_type()).unwrap(),
            Some(DataType::Decimal128(38, pg_type::NUMERIC_FALLBACK_SCALE))
        );
    }

    #[test]
    fn compile_query_executes_tpch_q01_numeric_sum_charge_expression() {
        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.has_aggregates = true;
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("lineitem".into()),
            columns: vec![
                ColumnRef {
                    attnum: 1,
                    name: "l_extendedprice".into(),
                    pg_type: numeric_typmod_type(15, 2),
                    nullable: false,
                },
                ColumnRef {
                    attnum: 2,
                    name: "l_discount".into(),
                    pg_type: numeric_typmod_type(15, 2),
                    nullable: false,
                },
                ColumnRef {
                    attnum: 3,
                    name: "l_tax".into(),
                    pg_type: numeric_typmod_type(15, 2),
                    nullable: false,
                },
            ],
            rows: vec![vec![
                numeric_const("10.00"),
                numeric_const("0.10"),
                numeric_const("0.05"),
            ]],
        }];
        query.targets = vec![Target {
            expr: QueryExpr::AggregateCall {
                func: AggregateFunction::Sum,
                args: vec![numeric_op_expr(
                    QueryOperator::Multiply,
                    numeric_op_expr(
                        QueryOperator::Multiply,
                        var_attnum(1, 1, numeric_typmod_type(15, 2)),
                        numeric_op_expr(
                            QueryOperator::Minus,
                            numeric_const("1"),
                            var_attnum(1, 2, numeric_typmod_type(15, 2)),
                        ),
                    ),
                    numeric_op_expr(
                        QueryOperator::Plus,
                        numeric_const("1"),
                        var_attnum(1, 3, numeric_typmod_type(15, 2)),
                    ),
                )],
                distinct: false,
                filter: None,
                pg_type: numeric_type(),
            },
            name: Some("sum_charge".into()),
            pg_type: numeric_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("q01 numeric charge expression should compile");
        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(38, 6)
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(output.logical_plan).await?;
            dataframe.collect().await
        })
        .expect("q01 numeric charge expression should execute in DataFusion");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .expect("sum(charge expression) should produce Decimal128Array");
        assert_eq!(values.value(0), 9_450_000);
    }

    #[test]
    fn numeric_arithmetic_policy_uses_fallback_scale_for_division() {
        let policy = numeric_decimal_arithmetic_policy(
            QueryOperator::Divide,
            numeric_type(),
            &var_attnum(1, 1, numeric_typmod_type(15, 4)),
            &var_attnum(1, 2, numeric_typmod_type(15, 2)),
        )
        .unwrap()
        .expect("numeric division should use decimal policy");

        assert_eq!(policy.left_type, DataType::Decimal128(38, 12));
        assert_eq!(policy.right_type, DataType::Decimal128(38, 2));
        assert_eq!(
            policy.result_type,
            DataType::Decimal128(38, pg_type::NUMERIC_FALLBACK_SCALE)
        );
    }

    #[test]
    fn compile_query_executes_numeric_division_at_fallback_scale() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![Target {
            expr: numeric_op_expr(
                QueryOperator::Divide,
                numeric_const("1"),
                numeric_const("3"),
            ),
            name: Some("quotient".into()),
            pg_type: numeric_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("numeric division should compile");
        assert_eq!(
            output.logical_plan.schema().field(0).data_type(),
            &DataType::Decimal128(38, pg_type::NUMERIC_FALLBACK_SCALE)
        );

        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(output.logical_plan).await?;
            dataframe.collect().await
        })
        .expect("numeric division should execute in DataFusion");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .expect("numeric division should produce Decimal128Array");
        assert_eq!(values.value(0), 3_333_333_333_333_333);
    }

    #[test]
    fn numeric_arithmetic_policy_uses_integer_cast_scale_zero() {
        let int_cast = QueryExpr::Cast {
            arg: Box::new(var_attnum(1, 1, numeric_typmod_type(15, 2))),
            pg_type: int4_type(),
        };
        assert_eq!(numeric_arithmetic_expr_scale(&int_cast).unwrap(), Some(0));

        let policy = numeric_decimal_arithmetic_policy(
            QueryOperator::Multiply,
            numeric_type(),
            &int_cast,
            &var_attnum(1, 2, numeric_typmod_type(15, 2)),
        )
        .unwrap()
        .expect("numeric multiplication with integer cast should use decimal policy");
        assert_eq!(policy.left_type, DataType::Decimal128(38, 0));
        assert_eq!(policy.right_type, DataType::Decimal128(38, 2));
        assert_eq!(policy.result_type, DataType::Decimal128(38, 2));
    }

    #[test]
    fn numeric_arithmetic_policy_rejects_scale_overflow() {
        let err = numeric_decimal_arithmetic_policy(
            QueryOperator::Multiply,
            numeric_type(),
            &var_attnum(1, 1, numeric_typmod_type(38, 30)),
            &var_attnum(1, 2, numeric_typmod_type(38, 9)),
        )
        .expect_err("numeric multiplication scale above 38 should fail before DataFusion");

        assert!(
            err.to_string().contains("exceeds Decimal128 max scale 38"),
            "error should explain Decimal128 scale overflow: {err}"
        );
    }

    #[test]
    fn relation_projection_keeps_join_and_post_join_filter_columns() {
        let resolved = resolved_table();
        let mut query = query_for_resolved_table();
        query.relations.push(RelationRef {
            rtindex: 2,
            relid: 43,
            schema: "public".into(),
            name: "u".into(),
            alias: Some("u".into()),
            columns: Vec::new(),
            catalog_resolved: false,
        });
        let left_id = target_var_attnum(1);
        let left_filter = target_var_attnum(2);
        let right_id = QueryExpr::Var(Var {
            rtindex: 2,
            attnum: 1,
            pg_type: int4_type(),
        });
        let right_score = QueryExpr::Var(Var {
            rtindex: 2,
            attnum: 2,
            pg_type: int4_type(),
        });
        query.from = FromItem::Join {
            kind: JoinKind::Inner,
            left: Box::new(FromItem::Relation { rtindex: 1 }),
            right: Box::new(FromItem::Relation { rtindex: 2 }),
            quals: Some(QueryExpr::BinaryOp {
                op: QueryOperator::Eq,
                left: Box::new(left_id),
                right: Box::new(right_id),
                pg_type: type_ref(pgrx::pg_sys::BOOLOID),
            }),
        };
        query.selection = Some(QueryExpr::BinaryOp {
            op: QueryOperator::Eq,
            left: Box::new(left_filter),
            right: Box::new(QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            pg_type: type_ref(pgrx::pg_sys::BOOLOID),
        });
        query.targets = vec![target("right_score", right_score)];

        let left_projection = relation_projection(&query, 1, &resolved, false).unwrap();
        let right_projection = relation_projection(&query, 2, &resolved, false).unwrap();

        assert_eq!(left_projection, vec![1, 0]);
        assert_eq!(right_projection, vec![1, 0]);
    }

    #[test]
    fn inner_join_where_pushdown_splits_single_relation_filters() {
        let mut query = join_query(JoinKind::Inner);
        query.selection = Some(QueryExpr::Bool {
            op: BoolOp::And,
            args: vec![
                eq_expr(var_attnum(1, 2, int4_type()), int4_const(1)),
                eq_expr(var_attnum(2, 2, int4_type()), int4_const(2)),
                eq_expr(var_attnum(1, 1, int4_type()), var_attnum(2, 1, int4_type())),
            ],
        });

        let pushdown = split_selection_for_scan_pushdown(&query)
            .expect("inner join WHERE filters should be split");

        assert_eq!(pushdown.scan_filters.get(&1).map(Vec::len), Some(1));
        assert_eq!(pushdown.scan_filters.get(&2).map(Vec::len), Some(1));
        let residual = pushdown
            .residual
            .as_ref()
            .expect("join-spanning predicate must remain residual");
        assert_eq!(single_predicate_rtindex(residual), None);
    }

    #[test]
    fn inner_join_where_pushdown_keeps_sibling_not_in_subquery_residual() {
        let mut query = join_query(JoinKind::Inner);
        query.selection = Some(QueryExpr::Bool {
            op: BoolOp::And,
            args: vec![
                binary_op_expr(
                    QueryOperator::NotLikeMatch,
                    var_attnum(2, 2, text_type()),
                    text_const("MEDIUM POLISHED%"),
                ),
                QueryExpr::Bool {
                    op: BoolOp::Not,
                    args: vec![QueryExpr::InSubquery {
                        expr: Box::new(var_attnum(1, 1, int4_type())),
                        subquery: Box::new(query_for_resolved_table()),
                        pg_type: bool_type(),
                    }],
                },
            ],
        });

        let pushdown = split_selection_for_scan_pushdown(&query)
            .expect("relation-local NOT LIKE should push beside NOT IN subquery residual");

        let scan_filters = pushdown
            .scan_filters
            .get(&2)
            .expect("NOT LIKE predicate should be pushed into the right relation scan");
        assert_eq!(scan_filters.len(), 1);
        assert!(matches!(
            scan_filters[0],
            QueryExpr::BinaryOp {
                op: QueryOperator::NotLikeMatch,
                ..
            }
        ));
        let residual = pushdown
            .residual
            .as_ref()
            .expect("NOT IN subquery must remain residual");
        assert!(contains_predicate_subquery(residual));
    }

    #[test]
    fn left_join_where_pushdown_keeps_nullable_side_residual() {
        let mut query = join_query(JoinKind::Left);
        query.selection = Some(QueryExpr::Bool {
            op: BoolOp::And,
            args: vec![
                eq_expr(var_attnum(1, 2, int4_type()), int4_const(1)),
                eq_expr(var_attnum(2, 2, int4_type()), int4_const(2)),
            ],
        });

        let pushdown = split_selection_for_scan_pushdown(&query)
            .expect("left join preserved-side filter should be pushable");

        assert_eq!(pushdown.scan_filters.get(&1).map(Vec::len), Some(1));
        assert!(!pushdown.scan_filters.contains_key(&2));
        assert_eq!(
            pushdown
                .residual
                .as_ref()
                .and_then(single_predicate_rtindex),
            Some(2)
        );
    }

    #[test]
    fn full_join_where_pushdown_keeps_single_relation_filters_residual() {
        let mut query = join_query(JoinKind::Full);
        query.selection = Some(eq_expr(var_attnum(1, 2, int4_type()), int4_const(1)));

        let pushdown = split_selection_for_scan_pushdown(&query)
            .expect("full join integer residual filter can stay in DataFusion");

        assert!(pushdown.scan_filters.is_empty());
        assert_eq!(
            pushdown
                .residual
                .as_ref()
                .and_then(single_predicate_rtindex),
            Some(1)
        );
    }

    #[test]
    fn nullable_side_like_residual_filter_fails_closed() {
        let mut query = join_query(JoinKind::Left);
        query.selection = Some(binary_op_expr(
            QueryOperator::LikeMatch,
            var_attnum(2, 2, text_type()),
            text_const("a%"),
        ));

        let err = split_selection_for_scan_pushdown(&query)
            .expect_err("nullable-side LIKE cannot run with DataFusion semantics");
        assert!(
            err.to_string().contains("residual text-like WHERE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nullable_side_regex_residual_filter_fails_closed() {
        let mut query = join_query(JoinKind::Left);
        query.selection = Some(binary_op_expr(
            QueryOperator::RegexMatch,
            var_attnum(2, 2, text_type()),
            text_const("^a"),
        ));

        let err = split_selection_for_scan_pushdown(&query)
            .expect_err("nullable-side regex cannot run with DataFusion semantics");
        assert!(
            err.to_string().contains("residual text-like WHERE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nullable_side_bpchar_residual_equality_is_allowed() {
        let mut query = join_query(JoinKind::Left);
        let bpchar = PgTypeRef {
            oid: oid_u32(pgrx::pg_sys::BPCHAROID),
            typmod: pgrx::pg_sys::VARHDRSZ as i32 + 3,
            collation: 0,
        };
        query.selection = Some(eq_expr(
            var_attnum(2, 2, bpchar),
            QueryExpr::Const(Const {
                pg_type: bpchar,
                value: Some(PgConstValue::Text("a".into())),
            }),
        ));

        let pushdown = split_selection_for_scan_pushdown(&query)
            .expect("nullable-side bpchar equality can run through pg_fusion UDF semantics");
        assert!(pushdown.scan_filters.is_empty());
        assert_eq!(
            pushdown
                .residual
                .as_ref()
                .and_then(single_predicate_rtindex),
            Some(2)
        );
    }

    #[test]
    fn nullable_side_bpchar_length_residual_filter_is_allowed() {
        let mut query = join_query(JoinKind::Left);
        let bpchar = PgTypeRef {
            oid: oid_u32(pgrx::pg_sys::BPCHAROID),
            typmod: pgrx::pg_sys::VARHDRSZ as i32 + 3,
            collation: 0,
        };
        query.selection = Some(eq_expr(
            QueryExpr::FunctionCall {
                func: ScalarFunction::Length,
                args: vec![var_attnum(2, 2, bpchar)],
                pg_type: int4_type(),
            },
            int4_const(1),
        ));

        let pushdown = split_selection_for_scan_pushdown(&query).expect(
            "nullable-side length(bpchar) residual can run through pg_fusion UDF semantics",
        );
        assert!(pushdown.scan_filters.is_empty());
        assert_eq!(
            pushdown
                .residual
                .as_ref()
                .and_then(single_predicate_rtindex),
            Some(2)
        );
    }

    #[test]
    fn nullable_side_bpchar_ordering_residual_filter_fails_closed() {
        let mut query = join_query(JoinKind::Left);
        let bpchar = PgTypeRef {
            oid: oid_u32(pgrx::pg_sys::BPCHAROID),
            typmod: pgrx::pg_sys::VARHDRSZ as i32 + 3,
            collation: 0,
        };
        query.selection = Some(binary_op_expr(
            QueryOperator::Lt,
            var_attnum(2, 2, bpchar),
            QueryExpr::Const(Const {
                pg_type: bpchar,
                value: Some(PgConstValue::Text("b".into())),
            }),
        ));

        let err = split_selection_for_scan_pushdown(&query)
            .expect_err("nullable-side bpchar ordering cannot run with DataFusion semantics");
        assert!(
            err.to_string().contains("residual text-like WHERE"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn compile_query_builds_typed_table_scan_plan() {
        let mut query = query_for_resolved_table();
        query.targets = vec![target("second", target_var_attnum(2))];
        query.selection = Some(QueryExpr::BinaryOp {
            op: QueryOperator::Eq,
            left: Box::new(target_var_attnum(1)),
            right: Box::new(QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            pg_type: type_ref(pgrx::pg_sys::BOOLOID),
        });

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("frontend query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Projection"), "{rendered}");
        assert!(rendered.contains("TableScan"), "{rendered}");
        assert!(rendered.contains("first"), "{rendered}");
        assert!(rendered.contains("second"), "{rendered}");
    }

    #[test]
    fn compile_query_builds_inner_join_plan() {
        let mut query = query_for_resolved_table();
        query.relations.push(RelationRef {
            rtindex: 2,
            relid: 43,
            schema: "public".into(),
            name: "u".into(),
            alias: Some("u".into()),
            columns: Vec::new(),
            catalog_resolved: false,
        });
        query.from = FromItem::Join {
            kind: JoinKind::Inner,
            left: Box::new(FromItem::Relation { rtindex: 1 }),
            right: Box::new(FromItem::Relation { rtindex: 2 }),
            quals: Some(QueryExpr::BinaryOp {
                op: QueryOperator::Eq,
                left: Box::new(target_var_attnum(1)),
                right: Box::new(QueryExpr::Var(Var {
                    rtindex: 2,
                    attnum: 1,
                    pg_type: int4_type(),
                })),
                pg_type: type_ref(pgrx::pg_sys::BOOLOID),
            }),
        };
        query.targets = vec![
            target("left_first", target_var_attnum(1)),
            target(
                "right_second",
                QueryExpr::Var(Var {
                    rtindex: 2,
                    attnum: 2,
                    pg_type: int4_type(),
                }),
            ),
        ];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend join query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("frontend join query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Projection"), "{rendered}");
        assert!(rendered.contains("Inner Join"), "{rendered}");
        assert!(rendered.matches("TableScan").count() >= 2, "{rendered}");
    }

    #[test]
    fn compile_query_builds_simple_aggregate_plan() {
        let mut query = query_for_resolved_table();
        query.has_aggregates = true;
        query.targets = vec![count_target("rows", 1, false)];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("aggregate query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Aggregate"), "{rendered}");
        assert!(rendered.contains("Projection"), "{rendered}");
        assert!(
            !rendered.contains("__pgfusion_aggregate"),
            "aggregate aliases should be readable in plan output: {rendered}"
        );
    }

    #[test]
    fn compile_query_uses_readable_aggregate_aliases_in_join_group_plan() {
        let mut query = join_query(JoinKind::Inner);
        query.has_aggregates = true;
        query.has_group_by = true;
        query.targets = vec![
            target_with_ref("b", var_attnum(1, 2, int4_type()), 1, 1, false),
            Target {
                resno: 2,
                ..count_target("count", 0, false)
            },
        ];
        query.group_refs = vec![1];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend join aggregate query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("join aggregate query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Aggregate"), "{rendered}");
        assert!(rendered.contains("count"), "{rendered}");
        assert!(
            !rendered.contains("__pgfusion_aggregate"),
            "join aggregate aliases should be readable in plan output: {rendered}"
        );
    }

    #[test]
    fn compile_query_deduplicates_readable_aggregate_aliases() {
        let mut query = query_for_resolved_table();
        query.has_aggregates = true;
        query.targets = vec![
            Target {
                expr: QueryExpr::AggregateCall {
                    func: AggregateFunction::Min,
                    args: vec![target_var_attnum(1)],
                    distinct: false,
                    filter: None,
                    pg_type: int4_type(),
                },
                name: Some("min_first".into()),
                pg_type: int4_type(),
                resno: 1,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::AggregateCall {
                    func: AggregateFunction::Min,
                    args: vec![target_var_attnum(2)],
                    distinct: false,
                    filter: None,
                    pg_type: int4_type(),
                },
                name: Some("min_second".into()),
                pg_type: int4_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend aggregate query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("duplicate aggregate names should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("min_2"), "{rendered}");
        assert!(
            !rendered.contains("__pgfusion_aggregate"),
            "duplicate aggregate aliases should keep readable suffixes: {rendered}"
        );
    }

    #[test]
    fn compile_query_builds_casted_aggregate_plan() {
        let mut query = query_for_resolved_table();
        query.has_aggregates = true;
        query.targets = vec![Target {
            expr: QueryExpr::Cast {
                arg: Box::new(QueryExpr::AggregateCall {
                    func: AggregateFunction::Avg,
                    args: vec![target_var_attnum(1)],
                    distinct: false,
                    filter: None,
                    pg_type: int4_type(),
                }),
                pg_type: text_type(),
            },
            name: Some("avg_text".into()),
            pg_type: text_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("casted aggregate query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Aggregate"), "{rendered}");
        assert!(rendered.contains("CAST"), "{rendered}");
    }

    #[test]
    fn compile_query_builds_grouped_aggregate_plan() {
        let mut query = query_for_resolved_table();
        query.has_aggregates = true;
        query.has_group_by = true;
        query.targets = vec![
            Target {
                expr: target_var_attnum(1),
                name: Some("first".into()),
                pg_type: int4_type(),
                resno: 1,
                ressortgroupref: 1,
                resjunk: false,
            },
            count_target("rows", 0, false),
        ];
        query.group_refs = vec![1];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("grouped aggregate query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Aggregate"), "{rendered}");
        assert!(rendered.contains("groupBy"), "{rendered}");
    }

    #[test]
    fn compile_query_uses_readable_window_aliases() {
        let mut query = query_for_resolved_table();
        query.has_windows = true;
        query.windows = vec![WindowSpec {
            ref_id: 1,
            partition_refs: Vec::new(),
            order: Vec::new(),
            frame: WindowFrameSpec {
                units: WindowFrameUnits::Range,
                start: WindowFrameBound::UnboundedPreceding,
                end: WindowFrameBound::UnboundedFollowing,
            },
        }];
        query.targets = vec![
            Target {
                expr: QueryExpr::WindowCall {
                    func: WindowFunctionKind::RowNumber,
                    args: Vec::new(),
                    winref: 1,
                    filter: None,
                    distinct: false,
                    pg_type: int8_type(),
                },
                name: Some("rn".into()),
                pg_type: int8_type(),
                resno: 1,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::WindowCall {
                    func: WindowFunctionKind::Aggregate(AggregateFunction::Avg),
                    args: vec![target_var_attnum(1)],
                    winref: 1,
                    filter: None,
                    distinct: false,
                    pg_type: int4_type(),
                },
                name: Some("avg_first".into()),
                pg_type: int4_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::WindowCall {
                    func: WindowFunctionKind::Aggregate(AggregateFunction::Avg),
                    args: vec![target_var_attnum(2)],
                    winref: 1,
                    filter: None,
                    distinct: false,
                    pg_type: int4_type(),
                },
                name: Some("avg_second".into()),
                pg_type: int4_type(),
                resno: 3,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend window query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("window query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("row_number"), "{rendered}");
        assert!(rendered.contains("avg_2"), "{rendered}");
        assert!(
            !rendered.contains("__pgfusion_window"),
            "window aliases should be readable in plan output: {rendered}"
        );
    }

    #[test]
    fn compile_query_window_aliases_avoid_existing_aggregate_names() {
        let mut query = query_for_resolved_table();
        query.has_aggregates = true;
        query.has_group_by = true;
        query.has_windows = true;
        query.group_refs = vec![1, 2];
        query.windows = vec![WindowSpec {
            ref_id: 1,
            partition_refs: vec![2],
            order: vec![SortKey {
                target_ref: 1,
                asc: true,
                nulls_first: false,
            }],
            frame: WindowFrameSpec {
                units: WindowFrameUnits::Range,
                start: WindowFrameBound::UnboundedPreceding,
                end: WindowFrameBound::CurrentRow,
            },
        }];
        let sum_call = QueryExpr::AggregateCall {
            func: AggregateFunction::Sum,
            args: vec![target_var_attnum(3)],
            distinct: false,
            filter: None,
            pg_type: int8_type(),
        };
        query.targets = vec![
            target_with_ref("ten", target_var_attnum(1), 1, 1, false),
            target_with_ref("two", target_var_attnum(2), 2, 2, false),
            Target {
                expr: sum_call.clone(),
                name: Some("gsum".into()),
                pg_type: int8_type(),
                resno: 3,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                expr: QueryExpr::WindowCall {
                    func: WindowFunctionKind::Aggregate(AggregateFunction::Sum),
                    args: vec![sum_call],
                    winref: 1,
                    filter: None,
                    distinct: false,
                    pg_type: int8_type(),
                },
                name: Some("wsum".into()),
                pg_type: int8_type(),
                resno: 4,
                ressortgroupref: 0,
                resjunk: false,
            },
        ];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend aggregate window query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("aggregate window query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("sum_2"), "{rendered}");
        assert!(
            !rendered.contains("__pgfusion_aggregate") && !rendered.contains("__pgfusion_window"),
            "aggregate and window aliases should be readable and unique: {rendered}"
        );
    }

    #[test]
    fn grouping_uses_grouping_set_metadata_not_null_values() {
        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "a".into(),
                pg_type: int4_type(),
                nullable: true,
            }],
            rows: vec![vec![int4_null()]],
        }];
        query.has_aggregates = true;
        query.has_group_by = true;
        query.has_grouping_sets = true;
        query.group_refs = vec![1];
        query.grouping_sets = vec![GroupingSetSpec::Sets(vec![
            GroupingSetSpec::Simple(vec![1]),
            GroupingSetSpec::Empty,
        ])];
        query.targets = vec![
            target_with_ref("a", target_var_attnum(1), 1, 1, false),
            Target {
                expr: QueryExpr::AggregateCall {
                    func: AggregateFunction::Grouping,
                    args: vec![target_var_attnum(1)],
                    distinct: false,
                    filter: None,
                    pg_type: int4_type(),
                },
                name: Some("grp".into()),
                pg_type: int4_type(),
                resno: 2,
                ressortgroupref: 0,
                resjunk: false,
            },
            Target {
                resno: 3,
                ..count_target("rows", 0, false)
            },
        ];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("GROUPING query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("grouping"), "{rendered}");
        assert!(
            !rendered.contains("IS NULL"),
            "GROUPING must not be inferred from nullable grouped values: {rendered}"
        );

        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .build();
        let optimized = state
            .optimize(&output.logical_plan)
            .expect("GROUPING should be rewritten during DataFusion analysis");
        let optimized_rendered = optimized.display_indent().to_string();
        assert!(
            !optimized_rendered.contains("grouping("),
            "DataFusion analyzer should rewrite GROUPING before physical planning: {optimized_rendered}"
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio runtime");
        let ctx = datafusion::prelude::SessionContext::new();
        let batches = runtime
            .block_on(async {
                let dataframe = ctx.execute_logical_plan(optimized).await?;
                dataframe.collect().await
            })
            .expect("GROUPING plan should execute in DataFusion after analyzer rewrite");
        let mut rows = Vec::new();
        for batch in batches {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .expect("a should be Int32");
            let grouping = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .expect("grouping should be Int32");
            let count = batch
                .column(2)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("count should be Int64");
            for index in 0..batch.num_rows() {
                rows.push((
                    (!a.is_null(index)).then(|| a.value(index)),
                    grouping.value(index),
                    count.value(index),
                ));
            }
        }
        rows.sort();
        assert_eq!(rows, vec![(None, 0, 1), (None, 1, 1)]);
    }

    #[test]
    fn compile_query_builds_from_subquery_plan() {
        let mut inner = query_for_resolved_table();
        inner.targets = vec![target("first", target_var_attnum(1))];

        let mut query = base_query();
        query.from = FromItem::Subquery { rtindex: 1 };
        query.subqueries = vec![SubqueryRef {
            rtindex: 1,
            alias: Some("s".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "x".into(),
                pg_type: int4_type(),
                nullable: true,
            }],
            query: Box::new(inner),
        }];
        query.targets = vec![target("x", target_var_attnum(1))];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend subquery should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("subquery query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("SubqueryAlias: s"), "{rendered}");
        assert!(rendered.contains("Projection"), "{rendered}");
        assert!(rendered.contains("TableScan"), "{rendered}");
    }

    #[test]
    fn compile_query_wraps_scalar_subquery_with_cardinality_aggregate() {
        let mut subquery = base_query();
        subquery.from = FromItem::Values { rtindex: 1 };
        subquery.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("s".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: int4_type(),
                nullable: false,
            }],
            rows: vec![vec![int4_const(1)]],
        }];
        subquery.targets = vec![target("column1", target_var_attnum(1))];

        let mut query = base_query();
        query.from = FromItem::Values { rtindex: 1 };
        query.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("t".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: int4_type(),
                nullable: false,
            }],
            rows: vec![vec![int4_const(1)], vec![int4_const(2)]],
        }];
        query.selection = Some(QueryExpr::BinaryOp {
            op: QueryOperator::Eq,
            left: Box::new(target_var_attnum(1)),
            right: Box::new(QueryExpr::ScalarSubquery(Box::new(subquery))),
            pg_type: type_ref(pgrx::pg_sys::BOOLOID),
        });
        query.targets = vec![target("x", target_var_attnum(1))];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("scalar subquery predicate should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Cross Join"), "{rendered}");
        assert!(rendered.contains("Aggregate"), "{rendered}");
        assert!(rendered.contains("pg_scalar_subquery_value"), "{rendered}");
        assert!(rendered.contains("scalar_subquery_value"), "{rendered}");
        assert!(
            !rendered.contains("scalar_subquery_1\n  Values"),
            "scalar subquery binding must join the aggregate result, not raw subquery rows: {rendered}"
        );
        assert!(
            !rendered.contains("__pgfusion_scalar_subquery"),
            "scalar subquery aliases should be readable in plan output: {rendered}"
        );

        let decoded = roundtrip_plan(output.logical_plan);
        let decoded_rendered = decoded.display_indent().to_string();
        assert!(
            decoded_rendered.contains("pg_scalar_subquery_value"),
            "encoded scalar subquery plan must decode with the cardinality aggregate: {decoded_rendered}"
        );
        assert!(
            !decoded_rendered.contains("__pgfusion_scalar_subquery"),
            "encoded scalar subquery aliases should stay readable: {decoded_rendered}"
        );
    }

    #[test]
    fn compile_query_wraps_projected_scalar_subquery_with_cardinality_aggregate() {
        let mut subquery = base_query();
        subquery.from = FromItem::Values { rtindex: 1 };
        subquery.values = vec![ValuesRef {
            rtindex: 1,
            alias: Some("s".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: int4_type(),
                nullable: false,
            }],
            rows: vec![vec![int4_const(1)]],
        }];
        subquery.targets = vec![target("column1", target_var_attnum(1))];

        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![target(
            "scalar_value",
            QueryExpr::ScalarSubquery(Box::new(subquery)),
        )];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("projected scalar subquery should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Cross Join"), "{rendered}");
        assert!(rendered.contains("pg_scalar_subquery_value"), "{rendered}");
        assert!(rendered.contains("scalar_subquery_value"), "{rendered}");
        assert!(
            !rendered.contains("__pgfusion_scalar_subquery"),
            "projected scalar subquery aliases should be readable: {rendered}"
        );
        let decoded = roundtrip_plan(output.logical_plan);
        let decoded_rendered = decoded.display_indent().to_string();
        assert!(
            decoded_rendered.contains("pg_scalar_subquery_value"),
            "encoded projected scalar subquery plan must decode with the cardinality aggregate: {decoded_rendered}"
        );
        assert!(
            !decoded_rendered.contains("__pgfusion_scalar_subquery"),
            "encoded projected scalar subquery aliases should stay readable: {decoded_rendered}"
        );
    }

    #[test]
    fn compile_query_builds_cte_ref_plan() {
        let mut cte_query = query_for_resolved_table();
        cte_query.targets = vec![target("first", target_var_attnum(1))];

        let mut query = base_query();
        query.from = FromItem::Cte { rtindex: 1 };
        query.ctes = vec![CteDef {
            id: 1,
            name: "revenue".into(),
            query: Box::new(cte_query),
        }];
        query.cte_refs = vec![CteRangeRef {
            rtindex: 1,
            cte_id: 1,
            name: "revenue".into(),
            alias: None,
            columns: vec![ColumnRef {
                attnum: 1,
                name: "first".into(),
                pg_type: int4_type(),
                nullable: true,
            }],
        }];
        query.targets = vec![target("first", target_var_attnum(1))];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend CTE should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("CTE query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("PgCteRef"), "{rendered}");
        assert!(rendered.contains("revenue"), "{rendered}");
    }

    #[test]
    fn executes_float_avg_over_string_values_cast() {
        let mut values_query = base_query();
        values_query.from = FromItem::Values { rtindex: 1 };
        values_query.values = vec![ValuesRef {
            rtindex: 1,
            alias: None,
            columns: vec![ColumnRef {
                attnum: 1,
                name: "column1".into(),
                pg_type: text_type(),
                nullable: true,
            }],
            rows: vec![
                vec![QueryExpr::Const(Const {
                    pg_type: text_type(),
                    value: Some(PgConstValue::Text("Infinity".into())),
                })],
                vec![QueryExpr::Const(Const {
                    pg_type: text_type(),
                    value: Some(PgConstValue::Text("-Infinity".into())),
                })],
            ],
        }];
        values_query.targets = vec![target("column1", target_var_attnum(1))];

        let mut query = base_query();
        query.from = FromItem::Subquery { rtindex: 1 };
        query.subqueries = vec![SubqueryRef {
            rtindex: 1,
            alias: Some("v".into()),
            columns: vec![ColumnRef {
                attnum: 1,
                name: "x".into(),
                pg_type: text_type(),
                nullable: true,
            }],
            query: Box::new(values_query),
        }];
        query.has_aggregates = true;
        query.targets = vec![Target {
            expr: QueryExpr::AggregateCall {
                func: AggregateFunction::Avg,
                args: vec![QueryExpr::Cast {
                    arg: Box::new(target_var_attnum(1)),
                    pg_type: type_ref(pgrx::pg_sys::FLOAT8OID),
                }],
                distinct: false,
                filter: None,
                pg_type: type_ref(pgrx::pg_sys::FLOAT8OID),
            },
            name: Some("avg".into()),
            pg_type: type_ref(pgrx::pg_sys::FLOAT8OID),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("values aggregate query should lower into a typed logical plan");
        let plan = roundtrip_plan(output.logical_plan);
        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(plan).await?;
            dataframe.collect().await
        })
        .expect("values aggregate plan should execute in DataFusion");
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }

    #[test]
    fn executes_int2_shift_cast_to_text() {
        let mut query = base_query();
        query.from = FromItem::Empty;
        query.targets = vec![Target {
            expr: QueryExpr::Cast {
                arg: Box::new(QueryExpr::BinaryOp {
                    op: QueryOperator::BitwiseShiftLeft,
                    left: Box::new(QueryExpr::UnaryOp {
                        op: QueryUnaryOperator::Minus,
                        arg: Box::new(QueryExpr::Const(Const {
                            pg_type: int2_type(),
                            value: Some(PgConstValue::Int16(1)),
                        })),
                        pg_type: int2_type(),
                    }),
                    right: Box::new(QueryExpr::Const(Const {
                        pg_type: int4_type(),
                        value: Some(PgConstValue::Int32(15)),
                    })),
                    pg_type: int2_type(),
                }),
                pg_type: text_type(),
            },
            name: Some("text".into()),
            pg_type: text_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }];

        let output = compile_typed_query(
            &query,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("shift query should lower into a typed logical plan");
        let plan = roundtrip_plan(output.logical_plan);
        let ctx = datafusion::prelude::SessionContext::new();
        let batches = futures::executor::block_on(async {
            let dataframe = ctx.execute_logical_plan(plan).await?;
            dataframe.collect().await
        })
        .expect("shift plan should execute in DataFusion");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
            .expect("text cast should produce StringViewArray");
        assert_eq!(values.value(0), "-32768");
    }

    #[test]
    fn compile_query_sorts_aggregate_plan_before_hiding_resjunk_targets() {
        let mut query = query_for_resolved_table();
        query.has_aggregates = true;
        query.targets = vec![
            count_target("rows", 1, false),
            Target {
                expr: QueryExpr::AggregateCall {
                    func: AggregateFunction::Sum,
                    args: vec![target_var_attnum(1)],
                    distinct: false,
                    filter: None,
                    pg_type: int8_type(),
                },
                name: Some("order_expr".into()),
                pg_type: int8_type(),
                resno: 2,
                ressortgroupref: 2,
                resjunk: true,
            },
        ];
        query.sort = vec![SortKey {
            target_ref: 2,
            asc: false,
            nulls_first: false,
        }];

        let resolved = query
            .resolve_catalog(&FakeResolver)
            .expect("frontend query should resolve against catalog");
        let output = compile_query(
            resolved,
            CompileConfig {
                identifier_max_bytes: 63,
            },
        )
        .expect("sorted aggregate query should lower into a typed logical plan");

        let rendered = output.logical_plan.display_indent().to_string();
        assert!(rendered.contains("Projection"), "{rendered}");
        assert!(rendered.contains("Sort"), "{rendered}");
        assert!(rendered.contains("Aggregate"), "{rendered}");
        assert!(rendered.contains("rows"), "{rendered}");
        assert!(rendered.contains("order_expr"), "{rendered}");
    }

    #[test]
    fn resolve_catalog_rejects_resolver_oid_mismatch() {
        #[derive(Debug)]
        struct MismatchResolver;

        impl CatalogResolver for MismatchResolver {
            fn resolve_table(
                &self,
                table: &datafusion_common::TableReference,
            ) -> Result<ResolvedTable, df_catalog::ResolveError> {
                Err(df_catalog::ResolveError::Postgres(format!(
                    "unexpected name lookup for {table}"
                )))
            }

            fn resolve_relation_oid(
                &self,
                _relid: u32,
            ) -> Result<ResolvedTable, df_catalog::ResolveError> {
                let mut resolved = resolved_table();
                resolved.table_oid = 99;
                Ok(resolved)
            }
        }

        let mut query = query_for_resolved_table();
        let err = query
            .resolve_catalog(&MismatchResolver)
            .expect_err("oid mismatch must fail closed");
        assert!(
            err.to_string().contains("relation oid"),
            "error {err} should mention relation oid mismatch"
        );
    }

    #[test]
    fn typed_null_and_arrow_types_cover_uuid_and_interval() {
        assert_eq!(
            typed_null(type_ref(pgrx::pg_sys::UUIDOID)).unwrap(),
            ScalarValue::FixedSizeBinary(16, None)
        );
        assert_eq!(
            arrow_type(type_ref(pgrx::pg_sys::UUIDOID)),
            Some(DataType::FixedSizeBinary(16))
        );

        assert_eq!(
            typed_null(type_ref(pgrx::pg_sys::INTERVALOID)).unwrap(),
            ScalarValue::IntervalMonthDayNano(None)
        );
        assert_eq!(
            arrow_type(type_ref(pgrx::pg_sys::INTERVALOID)),
            Some(DataType::Interval(IntervalUnit::MonthDayNano))
        );
    }

    #[test]
    fn text_like_constants_keep_pg_type_metadata() {
        let constant = Const {
            pg_type: PgTypeRef {
                oid: oid_u32(pgrx::pg_sys::BPCHAROID),
                typmod: pgrx::pg_sys::VARHDRSZ as i32 + 2,
                collation: oid_u32(pgrx::pg_sys::DEFAULT_COLLATION_OID),
            },
            value: Some(PgConstValue::Text("a ".into())),
        };

        let expr = compile_const_expr(&constant).unwrap();
        let Expr::Literal(ScalarValue::Utf8View(Some(value)), Some(metadata)) = expr else {
            panic!("bpchar constant must compile to Utf8View literal with PostgreSQL metadata");
        };

        assert_eq!(value, "a ");
        assert_eq!(
            metadata.inner().get("pg_fusion.pg_type_oid"),
            Some(&oid_u32(pgrx::pg_sys::BPCHAROID).to_string())
        );
        assert_eq!(
            metadata.inner().get("pg_fusion.pg_type_typmod"),
            Some(&(pgrx::pg_sys::VARHDRSZ as i32 + 2).to_string())
        );
    }

    #[test]
    fn non_finite_float_constants_compile_to_datafusion_literals() {
        let infinity = Const {
            pg_type: type_ref(pgrx::pg_sys::FLOAT8OID),
            value: Some(PgConstValue::Float64(f64::INFINITY)),
        };
        let nan = Const {
            pg_type: type_ref(pgrx::pg_sys::FLOAT4OID),
            value: Some(PgConstValue::Float32(f32::NAN)),
        };

        assert!(matches!(
            compile_const_expr(&infinity).unwrap(),
            Expr::Literal(ScalarValue::Float64(Some(value)), _) if value.is_infinite()
        ));
        assert!(matches!(
            compile_const_expr(&nan).unwrap(),
            Expr::Literal(ScalarValue::Float32(Some(value)), _) if value.is_nan()
        ));
    }

    #[test]
    fn date_constants_compile_to_date32_literals() {
        let constant = Const {
            pg_type: type_ref(pgrx::pg_sys::DATEOID),
            value: Some(PgConstValue::Date32(9_204)),
        };

        assert!(matches!(
            compile_const_expr(&constant).unwrap(),
            Expr::Literal(ScalarValue::Date32(Some(9_204)), _)
        ));
    }

    fn assert_target_expr_unsupported_contains(expr: &QueryExpr, expected: &str) {
        let err = validate_target_expr(expr).expect_err("target expression must be rejected");
        assert!(
            err.to_string().contains(expected),
            "error {err} must contain {expected}"
        );
    }

    fn roundtrip_plan(plan: LogicalPlan) -> LogicalPlan {
        let mut encoder = plan_codec::PlanEncodeSession::new(&plan).expect("encode session");
        let mut encoded = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            match encoder.write_chunk(&mut chunk).expect("encode chunk") {
                plan_codec::EncodeProgress::NeedMoreOutput { written } => {
                    encoded.extend_from_slice(&chunk[..written]);
                }
                plan_codec::EncodeProgress::Done { written } => {
                    encoded.extend_from_slice(&chunk[..written]);
                    break;
                }
            }
        }

        let mut decoder = plan_codec::PlanDecodeSession::new();
        for chunk in encoded.chunks(4096) {
            let _ = decoder.push_chunk(chunk).expect("decode chunk");
        }
        match decoder.finish_input().expect("finish decode") {
            plan_codec::DecodeProgress::Done(plan) => *plan,
            plan_codec::DecodeProgress::NeedMoreInput => panic!("decoded plan should be complete"),
        }
    }

    fn target_binary_op() -> QueryExpr {
        QueryExpr::BinaryOp {
            op: QueryOperator::Plus,
            left: Box::new(target_var()),
            right: Box::new(QueryExpr::Const(Const {
                pg_type: int4_type(),
                value: Some(PgConstValue::Int32(1)),
            })),
            pg_type: int4_type(),
        }
    }

    fn target_var() -> QueryExpr {
        target_var_attnum(1)
    }

    fn target_var_attnum(attnum: i16) -> QueryExpr {
        var_attnum(1, attnum, int4_type())
    }

    fn var_attnum(rtindex: usize, attnum: i16, pg_type: PgTypeRef) -> QueryExpr {
        QueryExpr::Var(Var {
            rtindex,
            attnum,
            pg_type,
        })
    }

    fn eq_expr(left: QueryExpr, right: QueryExpr) -> QueryExpr {
        binary_op_expr(QueryOperator::Eq, left, right)
    }

    fn binary_op_expr(op: QueryOperator, left: QueryExpr, right: QueryExpr) -> QueryExpr {
        QueryExpr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            pg_type: type_ref(pgrx::pg_sys::BOOLOID),
        }
    }

    fn single_predicate_rtindex(expr: &QueryExpr) -> Option<usize> {
        let rtindexes = predicate_rtindexes(expr);
        if rtindexes.len() == 1 {
            rtindexes.into_iter().next()
        } else {
            None
        }
    }

    fn join_query(kind: JoinKind) -> TypedQuery {
        let mut query = query_for_resolved_table();
        query.relations.push(RelationRef {
            rtindex: 2,
            relid: 43,
            schema: "public".into(),
            name: "u".into(),
            alias: Some("u".into()),
            columns: Vec::new(),
            catalog_resolved: false,
        });
        query.from = FromItem::Join {
            kind,
            left: Box::new(FromItem::Relation { rtindex: 1 }),
            right: Box::new(FromItem::Relation { rtindex: 2 }),
            quals: Some(eq_expr(
                var_attnum(1, 1, int4_type()),
                var_attnum(2, 1, int4_type()),
            )),
        };
        query
    }

    fn target(name: &str, expr: QueryExpr) -> Target {
        target_with_ref(name, expr, 1, 0, false)
    }

    fn target_with_ref(
        name: &str,
        expr: QueryExpr,
        resno: i16,
        ressortgroupref: u32,
        resjunk: bool,
    ) -> Target {
        Target {
            expr,
            name: Some(name.into()),
            pg_type: int4_type(),
            resno,
            ressortgroupref,
            resjunk,
        }
    }

    fn int4_row(left: i32, right: i32) -> Vec<QueryExpr> {
        vec![int4_const(left), int4_const(right)]
    }

    fn int4_const(value: i32) -> QueryExpr {
        QueryExpr::Const(Const {
            pg_type: int4_type(),
            value: Some(PgConstValue::Int32(value)),
        })
    }

    fn bool_const(value: bool) -> QueryExpr {
        QueryExpr::Const(Const {
            pg_type: bool_type(),
            value: Some(PgConstValue::Bool(value)),
        })
    }

    fn text_const(value: &str) -> QueryExpr {
        QueryExpr::Const(Const {
            pg_type: text_type(),
            value: Some(PgConstValue::Text(value.into())),
        })
    }

    fn numeric_const(value: &str) -> QueryExpr {
        QueryExpr::Const(Const {
            pg_type: numeric_type(),
            value: Some(PgConstValue::Numeric(value.into())),
        })
    }

    fn numeric_op_expr(op: QueryOperator, left: QueryExpr, right: QueryExpr) -> QueryExpr {
        QueryExpr::BinaryOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
            pg_type: numeric_type(),
        }
    }

    fn numeric_target(name: &str, expr: QueryExpr) -> Target {
        Target {
            expr,
            name: Some(name.into()),
            pg_type: numeric_type(),
            resno: 1,
            ressortgroupref: 0,
            resjunk: false,
        }
    }

    fn int4_null() -> QueryExpr {
        QueryExpr::Const(Const {
            pg_type: int4_type(),
            value: None,
        })
    }

    fn count_target(name: &str, ressortgroupref: u32, resjunk: bool) -> Target {
        Target {
            expr: QueryExpr::AggregateCall {
                func: AggregateFunction::Count,
                args: Vec::new(),
                distinct: false,
                filter: None,
                pg_type: int8_type(),
            },
            name: Some(name.into()),
            pg_type: int8_type(),
            resno: 1,
            ressortgroupref,
            resjunk,
        }
    }

    fn query_for_resolved_table() -> TypedQuery {
        let mut query = base_query();
        query.relations = vec![RelationRef {
            rtindex: 1,
            relid: 42,
            schema: "public".into(),
            name: "t".into(),
            alias: None,
            columns: Vec::new(),
            catalog_resolved: false,
        }];
        query
    }

    fn resolved_table() -> ResolvedTable {
        resolved_table_for(42, "t")
    }

    fn resolved_table_for(table_oid: u32, table: &str) -> ResolvedTable {
        ResolvedTable {
            table_oid,
            relation: scan_sql::PgRelation::new(Some("public"), table),
            column_attnums: vec![1, 2, 3],
            schema: Arc::new(arrow_schema::Schema::new(vec![
                Field::new("first", DataType::Int32, true),
                Field::new("second", DataType::Int32, true),
                Field::new("unused", DataType::Int32, true),
            ])),
            columns: vec![
                ResolvedColumn {
                    attnum: 1,
                    name: "first".into(),
                    pg_type: int4_type(),
                    nullable: true,
                },
                ResolvedColumn {
                    attnum: 2,
                    name: "second".into(),
                    pg_type: int4_type(),
                    nullable: true,
                },
                ResolvedColumn {
                    attnum: 3,
                    name: "unused".into(),
                    pg_type: int4_type(),
                    nullable: true,
                },
            ],
        }
    }

    fn compile_context(tables: impl IntoIterator<Item = (usize, ResolvedTable)>) -> CompileContext {
        CompileContext {
            tables: tables.into_iter().collect(),
            values: HashMap::new(),
            ctes: HashMap::new(),
            subqueries: HashMap::new(),
            config: CompileConfig {
                identifier_max_bytes: 63,
            },
        }
    }

    #[derive(Debug)]
    struct FakeResolver;

    impl CatalogResolver for FakeResolver {
        fn resolve_table(
            &self,
            table: &datafusion_common::TableReference,
        ) -> Result<ResolvedTable, df_catalog::ResolveError> {
            Err(df_catalog::ResolveError::Postgres(format!(
                "unexpected name lookup for {table}"
            )))
        }

        fn resolve_relation_oid(
            &self,
            relid: u32,
        ) -> Result<ResolvedTable, df_catalog::ResolveError> {
            match relid {
                42 => Ok(resolved_table()),
                43 => Ok(resolved_table_for(43, "u")),
                _ => Err(df_catalog::ResolveError::Postgres(format!(
                    "relation oid {relid} not found"
                ))),
            }
        }
    }

    fn int4_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::INT4OID)
    }

    fn int2_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::INT2OID)
    }

    fn int8_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::INT8OID)
    }

    fn bool_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::BOOLOID)
    }

    fn text_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::TEXTOID)
    }

    fn numeric_type() -> PgTypeRef {
        type_ref(pgrx::pg_sys::NUMERICOID)
    }

    fn numeric_typmod_type(precision: i32, scale: i32) -> PgTypeRef {
        PgTypeRef {
            oid: oid_u32(pgrx::pg_sys::NUMERICOID),
            typmod: ((precision << 16) | (scale & 0x7ff)) + pgrx::pg_sys::VARHDRSZ as i32,
            collation: 0,
        }
    }

    fn varchar_type(length: i32) -> PgTypeRef {
        text_typmod_type(pgrx::pg_sys::VARCHAROID, length)
    }

    fn bpchar_type(length: i32) -> PgTypeRef {
        text_typmod_type(pgrx::pg_sys::BPCHAROID, length)
    }

    fn text_typmod_type(oid: pgrx::pg_sys::Oid, length: i32) -> PgTypeRef {
        PgTypeRef {
            oid: oid_u32(oid),
            typmod: pgrx::pg_sys::VARHDRSZ as i32 + length,
            collation: 0,
        }
    }

    fn type_ref(oid: pgrx::pg_sys::Oid) -> PgTypeRef {
        PgTypeRef {
            oid: oid_u32(oid),
            typmod: -1,
            collation: 0,
        }
    }

    fn base_query() -> TypedQuery {
        TypedQuery {
            command: QueryCommand::Select,
            relations: Vec::new(),
            values: Vec::new(),
            ctes: Vec::new(),
            cte_refs: Vec::new(),
            subqueries: Vec::new(),
            from: FromItem::Relation { rtindex: 1 },
            selection: None,
            having: None,
            targets: Vec::new(),
            group_refs: Vec::new(),
            grouping_sets: Vec::new(),
            windows: Vec::new(),
            set_operation: None,
            sort: Vec::new(),
            limit_count: None,
            limit_offset: None,
            has_aggregates: false,
            has_windows: false,
            has_sublinks: false,
            distinct: DistinctSpec::None,
            has_group_by: false,
            has_having: false,
            has_grouping_sets: false,
            has_set_operations: false,
            has_row_marks: false,
        }
    }
}
