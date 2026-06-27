use super::*;

pub(super) fn compile_expr(
    expr: &QueryExpr,
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    compile_expr_with_windows(expr, query, ctx, &[], &[], &[])
}

pub(super) fn compile_limit_bound(expr: &QueryExpr) -> Result<Expr, PgFrontendError> {
    match expr {
        QueryExpr::Const(constant) => compile_limit_const(constant),
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            compile_limit_bound(inner)
        }
        _ => Err(PgFrontendError::unsupported(
            "LIMIT/OFFSET expressions must be constant in pg_frontend v1",
        )),
    }
}

pub(super) fn limit_bound_is_zero(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::Const(constant) => matches!(
            constant.value.as_ref(),
            Some(pg_type::PgConstValue::Int16(0))
                | Some(pg_type::PgConstValue::Int32(0))
                | Some(pg_type::PgConstValue::Int64(0))
        ),
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            limit_bound_is_zero(inner)
        }
        _ => false,
    }
}

pub(super) fn compile_limit_const(constant: &Const) -> Result<Expr, PgFrontendError> {
    let value = match constant.value.as_ref() {
        None => None,
        Some(pg_type::PgConstValue::Int16(value)) => Some(i64::from(*value)),
        Some(pg_type::PgConstValue::Int32(value)) => Some(i64::from(*value)),
        Some(pg_type::PgConstValue::Int64(value)) => Some(*value),
        _ => {
            return Err(PgFrontendError::unsupported(
                "LIMIT/OFFSET constants must be integer in pg_frontend v1",
            ))
        }
    };
    Ok(Expr::Literal(ScalarValue::Int64(value), None))
}

pub(super) fn compile_expr_with_windows(
    expr: &QueryExpr,
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    if let Some(binding) = window_bindings
        .iter()
        .find(|binding| binding_expr_matches(&binding.expr, expr))
    {
        return Ok(cast_window_binding_column(expr, binding.column.clone()));
    }
    if let Some(binding) = aggregate_bindings
        .iter()
        .find(|binding| binding_expr_matches(&binding.expr, expr))
    {
        return Ok(binding.column.clone());
    }

    match expr {
        QueryExpr::Var(var) => compile_var(*var, query, ctx),
        QueryExpr::OuterVar(var) => compile_outer_var(var),
        QueryExpr::Const(constant) => compile_const_expr(constant),
        QueryExpr::Param(_) => Err(PgFrontendError::unsupported(
            "parameters are not supported by pg_frontend v1",
        )),
        QueryExpr::RelabelType(inner) => compile_expr_with_windows(
            inner,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::Cast { arg, pg_type } => compile_cast(
            arg,
            *pg_type,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::FunctionCall {
            func,
            args,
            pg_type,
        } => compile_scalar_function(
            *func,
            args,
            *pg_type,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::Array { elements, .. } => compile_array(
            elements,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::ArraySubscript { array, index, .. } => compile_array_subscript(
            array,
            index,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::Coalesce { args, .. } => compile_coalesce(
            args,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => compile_case(
            operand.as_deref(),
            when_then,
            else_expr.as_deref(),
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::ScalarSubquery(subquery) => {
            if let Some(binding) = scalar_bindings
                .iter()
                .find(|binding| binding.query == **subquery)
            {
                Ok(binding.column.clone())
            } else {
                compile_scalar_subquery(subquery, ctx)
            }
        }
        QueryExpr::ExistsSubquery { subquery, .. } => {
            let plan = compile_subquery_plan(subquery, ctx)?;
            Ok(datafusion_expr::expr_fn::exists(plan))
        }
        QueryExpr::InSubquery { expr, subquery, .. } => {
            let expr = compile_expr_with_windows(
                expr,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?;
            let plan = compile_subquery_plan(subquery, ctx)?;
            if plan.schema().fields().len() != 1 {
                return Err(PgFrontendError::unsupported(
                    "IN subquery must return exactly one column",
                ));
            }
            let outer_ref_columns = plan.all_out_ref_exprs();
            Ok(Expr::InSubquery(InSubquery::new(
                Box::new(expr),
                Subquery {
                    subquery: plan,
                    outer_ref_columns,
                    spans: Spans::new(),
                },
                false,
            )))
        }
        QueryExpr::Bool { op, args } => compile_bool(
            *op,
            args,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        ),
        QueryExpr::BinaryOp {
            op,
            left,
            right,
            pg_type,
        } => {
            let left_expr = compile_expr_with_windows(
                left,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?;
            let right_expr = compile_expr_with_windows(
                right,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?;
            compile_binary_expr(*op, *pg_type, left_expr, right_expr, left, right)
        }
        QueryExpr::UnaryOp { op, arg, .. } => {
            let arg = compile_expr_with_windows(
                arg,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?;
            Ok(compile_unary_op(*op, arg))
        }
        QueryExpr::AggregateCall {
            func,
            args,
            distinct,
            filter,
            ..
        } => compile_aggregate_call(*func, args, *distinct, filter.as_deref(), query, ctx),
        QueryExpr::WindowCall { .. } => Err(PgFrontendError::unsupported(
            "window function expression was not bound by a Window logical plan",
        )),
        QueryExpr::NullTest { arg, is_null } => {
            let arg = Box::new(compile_expr_with_windows(
                arg,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?);
            Ok(if *is_null {
                Expr::IsNull(arg)
            } else {
                Expr::IsNotNull(arg)
            })
        }
        QueryExpr::BooleanTest { arg, kind } => {
            let arg = Box::new(compile_expr_with_windows(
                arg,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?);
            Ok(match kind {
                BooleanTestKind::IsTrue => Expr::IsTrue(arg),
                BooleanTestKind::IsNotTrue => Expr::IsNotTrue(arg),
                BooleanTestKind::IsFalse => Expr::IsFalse(arg),
                BooleanTestKind::IsNotFalse => Expr::IsNotFalse(arg),
                BooleanTestKind::IsUnknown => Expr::IsUnknown(arg),
                BooleanTestKind::IsNotUnknown => Expr::IsNotUnknown(arg),
            })
        }
    }
}

pub(super) fn compile_cast(
    arg: &QueryExpr,
    pg_type: pg_type::PgTypeRef,
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID)
        && expr_contains_nonfinite_numeric_text_source(arg, query)
    {
        return Err(PgFrontendError::unsupported(
            "pg_fusion Decimal128 numeric cannot represent PostgreSQL numeric NaN/Infinity values",
        ));
    }
    if pg_type.oid == u32::from(pgrx::pg_sys::BOOLOID) && is_integer_expr(arg) {
        let arg_expr = compile_expr_with_windows(
            arg,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        )?;
        return Ok(binary_expr(
            arg_expr,
            Operator::NotEq,
            integer_zero_expr(arg),
        ));
    }
    if is_text_like_type(pg_type.oid)
        && expr_pg_type(arg)
            .is_some_and(|source| source.oid == u32::from(pgrx::pg_sys::INTERVALOID))
    {
        let arg = compile_expr_with_windows(
            arg,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        )?;
        return Ok(df_functions::pg_interval_out_udf().call(vec![arg]));
    }
    if let Some(udf) = text_typmod_cast_udf(pg_type) {
        let arg = compile_expr_with_windows(
            arg,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        )?;
        return Ok(udf.call(vec![
            arg,
            Expr::Literal(ScalarValue::Int32(Some(pg_type.typmod)), None),
        ]));
    }
    let data_type = arrow_type_for_pg_type(pg_type).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "cast target PostgreSQL type oid {} is not supported by pg_frontend v1",
            pg_type.oid
        ))
    })?;
    Ok(Expr::Cast(Cast::new(
        Box::new(compile_expr_with_windows(
            arg,
            query,
            ctx,
            window_bindings,
            scalar_bindings,
            aggregate_bindings,
        )?),
        data_type,
    )))
}

fn text_typmod_cast_udf(pg_type: pg_type::PgTypeRef) -> Option<Arc<ScalarUDF>> {
    pg_type::text_typmod_length(pg_type.typmod)?;
    if pg_type.oid == u32::from(pgrx::pg_sys::VARCHAROID) {
        return Some(df_functions::pg_varchar_typmod_udf());
    }
    if pg_type.oid == u32::from(pgrx::pg_sys::BPCHAROID) {
        return Some(df_functions::pg_bpchar_typmod_udf());
    }
    None
}

pub(super) fn expr_contains_nonfinite_numeric_text_source(
    expr: &QueryExpr,
    query: &TypedQuery,
) -> bool {
    match expr {
        QueryExpr::Const(Const {
            value: Some(PgConstValue::Text(value)),
            ..
        }) => is_nonfinite_numeric_text(value),
        QueryExpr::Var(var) => {
            if let Some((values, index)) = query.values.iter().find_map(|values| {
                if values.rtindex == var.rtindex {
                    values_column_index(*var, values)
                        .ok()
                        .map(|index| (values, index))
                } else {
                    None
                }
            }) {
                return values.rows.iter().any(|row| {
                    row.get(index).is_some_and(|value| {
                        expr_contains_nonfinite_numeric_text_source(value, query)
                    })
                });
            }
            if let Some((subquery, index)) = query.subqueries.iter().find_map(|subquery| {
                if subquery.rtindex == var.rtindex {
                    subquery
                        .columns
                        .iter()
                        .position(|column| column.attnum == var.attnum)
                        .map(|index| (subquery, index))
                } else {
                    None
                }
            }) {
                return visible_targets(&subquery.query)
                    .nth(index)
                    .is_some_and(|target| {
                        expr_contains_nonfinite_numeric_text_source(&target.expr, &subquery.query)
                    });
            }
            if let Some((cte_ref, index)) = query.cte_refs.iter().find_map(|cte_ref| {
                if cte_ref.rtindex == var.rtindex {
                    cte_ref
                        .columns
                        .iter()
                        .position(|column| column.attnum == var.attnum)
                        .map(|index| (cte_ref, index))
                } else {
                    None
                }
            }) {
                return query
                    .ctes
                    .iter()
                    .find(|cte| cte.id == cte_ref.cte_id)
                    .and_then(|cte| {
                        visible_targets(&cte.query)
                            .nth(index)
                            .map(|target| (cte, target))
                    })
                    .is_some_and(|(cte, target)| {
                        expr_contains_nonfinite_numeric_text_source(&target.expr, &cte.query)
                    });
            }
            false
        }
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            expr_contains_nonfinite_numeric_text_source(inner, query)
        }
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => args
            .iter()
            .any(|arg| expr_contains_nonfinite_numeric_text_source(arg, query)),
        QueryExpr::ArraySubscript { array, index, .. } => {
            expr_contains_nonfinite_numeric_text_source(array, query)
                || expr_contains_nonfinite_numeric_text_source(index, query)
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(|expr| expr_contains_nonfinite_numeric_text_source(expr, query))
                || when_then.iter().any(|(when, then)| {
                    expr_contains_nonfinite_numeric_text_source(when, query)
                        || expr_contains_nonfinite_numeric_text_source(then, query)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(|expr| expr_contains_nonfinite_numeric_text_source(expr, query))
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            expr_contains_nonfinite_numeric_text_source(left, query)
                || expr_contains_nonfinite_numeric_text_source(right, query)
        }
        QueryExpr::UnaryOp { arg, .. }
        | QueryExpr::NullTest { arg, .. }
        | QueryExpr::BooleanTest { arg, .. } => {
            expr_contains_nonfinite_numeric_text_source(arg, query)
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            args.iter()
                .any(|arg| expr_contains_nonfinite_numeric_text_source(arg, query))
                || filter
                    .as_deref()
                    .is_some_and(|arg| expr_contains_nonfinite_numeric_text_source(arg, query))
        }
        QueryExpr::InSubquery { expr, .. } => {
            expr_contains_nonfinite_numeric_text_source(expr, query)
        }
        QueryExpr::ExistsSubquery { .. }
        | QueryExpr::ScalarSubquery(_)
        | QueryExpr::OuterVar(_)
        | QueryExpr::Param(_)
        | QueryExpr::Const(_) => false,
    }
}

pub(super) fn is_nonfinite_numeric_text(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "nan" | "infinity" | "+infinity" | "-infinity" | "inf" | "+inf" | "-inf"
    )
}

pub(super) fn is_integer_expr(expr: &QueryExpr) -> bool {
    expr_pg_type(expr).is_some_and(|pg_type| {
        pg_type.oid == u32::from(pgrx::pg_sys::INT2OID)
            || pg_type.oid == u32::from(pgrx::pg_sys::INT4OID)
            || pg_type.oid == u32::from(pgrx::pg_sys::INT8OID)
    })
}

pub(super) fn integer_zero_expr(expr: &QueryExpr) -> Expr {
    match expr_pg_type(expr).map(|pg_type| pg_type.oid) {
        Some(oid) if oid == u32::from(pgrx::pg_sys::INT2OID) => {
            Expr::Literal(ScalarValue::Int16(Some(0)), None)
        }
        Some(oid) if oid == u32::from(pgrx::pg_sys::INT8OID) => {
            Expr::Literal(ScalarValue::Int64(Some(0)), None)
        }
        _ => Expr::Literal(ScalarValue::Int32(Some(0)), None),
    }
}

pub(super) fn expr_pg_type(expr: &QueryExpr) -> Option<pg_type::PgTypeRef> {
    match expr {
        QueryExpr::Var(var) => Some(var.pg_type),
        QueryExpr::OuterVar(var) => Some(var.pg_type),
        QueryExpr::Const(constant) => Some(constant.pg_type),
        QueryExpr::Param(param) => Some(param.pg_type),
        QueryExpr::RelabelType(inner) => expr_pg_type(inner),
        QueryExpr::Cast { pg_type, .. }
        | QueryExpr::Array { pg_type, .. }
        | QueryExpr::ArraySubscript { pg_type, .. }
        | QueryExpr::FunctionCall { pg_type, .. }
        | QueryExpr::BinaryOp { pg_type, .. }
        | QueryExpr::UnaryOp { pg_type, .. }
        | QueryExpr::AggregateCall { pg_type, .. }
        | QueryExpr::WindowCall { pg_type, .. }
        | QueryExpr::Coalesce { pg_type, .. }
        | QueryExpr::Case { pg_type, .. }
        | QueryExpr::ExistsSubquery { pg_type, .. }
        | QueryExpr::InSubquery { pg_type, .. } => Some(*pg_type),
        QueryExpr::Bool { .. } | QueryExpr::NullTest { .. } | QueryExpr::BooleanTest { .. } => {
            Some(pg_type::PgTypeRef::new(
                u32::from(pgrx::pg_sys::BOOLOID),
                -1,
                0,
            ))
        }
        QueryExpr::ScalarSubquery(query) => {
            visible_targets(query).next().map(|target| target.pg_type)
        }
    }
}

pub(super) fn cast_bound_column_to_pg_type(expr: &QueryExpr, column: Expr) -> Expr {
    let Some(pg_type) = expr_pg_type(expr) else {
        return column;
    };
    let Some(data_type) = arrow_type_for_pg_type(pg_type) else {
        return column;
    };
    Expr::Cast(Cast::new(Box::new(column), data_type))
}

pub(super) fn cast_window_binding_column(expr: &QueryExpr, column: Expr) -> Expr {
    let QueryExpr::WindowCall { func, .. } = expr else {
        return column;
    };
    if matches!(
        func,
        WindowFunctionKind::DenseRank
            | WindowFunctionKind::Ntile
            | WindowFunctionKind::Rank
            | WindowFunctionKind::RowNumber
    ) {
        cast_bound_column_to_pg_type(expr, column)
    } else {
        column
    }
}

pub(super) fn compile_array(
    elements: &[QueryExpr],
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let elements = elements
        .iter()
        .map(|element| {
            compile_expr_with_windows(
                element,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(datafusion::functions_nested::expr_fn::make_array(elements))
}

pub(super) fn compile_array_subscript(
    array: &QueryExpr,
    index: &QueryExpr,
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let array = compile_expr_with_windows(
        array,
        query,
        ctx,
        window_bindings,
        scalar_bindings,
        aggregate_bindings,
    )?;
    let index = compile_expr_with_windows(
        index,
        query,
        ctx,
        window_bindings,
        scalar_bindings,
        aggregate_bindings,
    )?;
    let index = Expr::Cast(Cast::new(Box::new(index), DataType::Int64));
    Ok(datafusion::functions_nested::expr_fn::array_element(
        array, index,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_scalar_function(
    func: ScalarFunction,
    args: &[QueryExpr],
    result_pg_type: pg_type::PgTypeRef,
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let source_args = args;
    let args = source_args
        .iter()
        .map(|arg| {
            compile_expr_with_windows(
                arg,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(match func {
        ScalarFunction::Abs => {
            call_unary_scalar_function("abs", datafusion::functions::math::abs(), args)?
        }
        ScalarFunction::Acosh => {
            call_unary_scalar_function("acosh", datafusion::functions::math::acosh(), args)?
        }
        ScalarFunction::Asinh => {
            call_unary_scalar_function("asinh", datafusion::functions::math::asinh(), args)?
        }
        ScalarFunction::Atanh => {
            call_unary_scalar_function("atanh", datafusion::functions::math::atanh(), args)?
        }
        ScalarFunction::Ceil => {
            call_unary_scalar_function("ceil", datafusion::functions::math::ceil(), args)?
        }
        ScalarFunction::Concat => {
            datafusion::functions::string::concat().call(cast_args_to_pg_text(source_args, args))
        }
        ScalarFunction::ConcatWs => {
            datafusion::functions::string::concat_ws().call(cast_args_to_pg_text(source_args, args))
        }
        ScalarFunction::Floor => {
            call_unary_scalar_function("floor", datafusion::functions::math::floor(), args)?
        }
        ScalarFunction::Format => {
            df_functions::pg_format_udf().call(cast_format_args(source_args, args))
        }
        ScalarFunction::Length => compile_length_function(source_args, args)?,
        ScalarFunction::Cosh => {
            call_unary_scalar_function("cosh", datafusion::functions::math::cosh(), args)?
        }
        ScalarFunction::Exp => {
            call_unary_scalar_function("exp", datafusion::functions::math::exp(), args)?
        }
        ScalarFunction::Ln => {
            call_unary_scalar_function("ln", datafusion::functions::math::ln(), args)?
        }
        ScalarFunction::NullIf => {
            if args.len() != 2 {
                return Err(PgFrontendError::unsupported(
                    "NULLIF requires exactly two arguments",
                ));
            }
            datafusion::functions::core::nullif().call(args)
        }
        ScalarFunction::Power => {
            if args.len() != 2 {
                return Err(PgFrontendError::unsupported(
                    "power() requires exactly two arguments",
                ));
            }
            datafusion::functions::math::power().call(args)
        }
        ScalarFunction::QuoteLiteral => {
            if args.len() != 1 {
                return Err(PgFrontendError::unsupported(
                    "quote_literal() requires exactly one argument",
                ));
            }
            df_functions::pg_quote_literal_udf().call(cast_args_to_utf8(args))
        }
        ScalarFunction::Random => {
            if !args.is_empty() {
                return Err(PgFrontendError::unsupported(
                    "random() does not accept arguments",
                ));
            }
            datafusion::functions::math::random().call(vec![])
        }
        ScalarFunction::Reverse => {
            call_unary_scalar_function("reverse", datafusion::functions::unicode::reverse(), args)?
        }
        ScalarFunction::Round => compile_round_trunc_function(
            "round",
            datafusion::functions::math::round(),
            args,
            result_pg_type,
            source_args,
        )?,
        ScalarFunction::Sinh => {
            call_unary_scalar_function("sinh", datafusion::functions::math::sinh(), args)?
        }
        ScalarFunction::Sqrt => {
            call_unary_scalar_function("sqrt", datafusion::functions::math::sqrt(), args)?
        }
        ScalarFunction::Tanh => {
            call_unary_scalar_function("tanh", datafusion::functions::math::tanh(), args)?
        }
        ScalarFunction::Trunc => compile_round_trunc_function(
            "trunc",
            datafusion::functions::math::trunc(),
            args,
            result_pg_type,
            source_args,
        )?,
    })
}

pub(super) fn compile_round_trunc_function(
    name: &'static str,
    udf: Arc<ScalarUDF>,
    mut args: Vec<Expr>,
    result_pg_type: pg_type::PgTypeRef,
    source_args: &[QueryExpr],
) -> Result<Expr, PgFrontendError> {
    if args.is_empty() || args.len() > 2 {
        return Err(PgFrontendError::unsupported(format!(
            "{name}() requires one or two arguments"
        )));
    }
    if let Some(precision) = args.get_mut(1) {
        *precision = Expr::Cast(Cast::new(Box::new(precision.clone()), DataType::Int64));
    }
    if result_pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) && args.len() == 2 {
        let udf = match name {
            "round" => df_functions::pg_numeric_round_scale_udf(),
            "trunc" => df_functions::pg_numeric_trunc_scale_udf(),
            _ => unreachable!("round/trunc helper called with unexpected function"),
        };
        let expr = udf.call(args);
        return Ok(match numeric_scale_result_type(source_args) {
            Some(data_type) => Expr::Cast(Cast::new(Box::new(expr), data_type)),
            None => expr,
        });
    }
    Ok(cast_numeric_function_result(udf.call(args), result_pg_type))
}

pub(super) fn numeric_scale_result_type(source_args: &[QueryExpr]) -> Option<DataType> {
    let scale = numeric_scale_const(source_args.get(1)?)?;
    let scale = scale.clamp(0, i64::from(pg_type::NUMERIC_FALLBACK_SCALE));
    Some(DataType::Decimal128(
        pg_type::NUMERIC_FALLBACK_PRECISION,
        i8::try_from(scale).ok()?,
    ))
}

pub(super) fn numeric_scale_const(expr: &QueryExpr) -> Option<i64> {
    match expr {
        QueryExpr::Const(Const {
            value: Some(pg_type::PgConstValue::Int16(value)),
            ..
        }) => Some(i64::from(*value)),
        QueryExpr::Const(Const {
            value: Some(pg_type::PgConstValue::Int32(value)),
            ..
        }) => Some(i64::from(*value)),
        QueryExpr::Const(Const {
            value: Some(pg_type::PgConstValue::Int64(value)),
            ..
        }) => Some(*value),
        QueryExpr::RelabelType(inner) | QueryExpr::Cast { arg: inner, .. } => {
            numeric_scale_const(inner)
        }
        _ => None,
    }
}

pub(super) fn cast_numeric_function_result(expr: Expr, result_pg_type: pg_type::PgTypeRef) -> Expr {
    if result_pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) {
        if let Some(data_type) = arrow_type_for_pg_type(result_pg_type) {
            return Expr::Cast(Cast::new(Box::new(expr), data_type));
        }
    }
    expr
}

pub(super) fn compile_length_function(
    source_args: &[QueryExpr],
    args: Vec<Expr>,
) -> Result<Expr, PgFrontendError> {
    if args.len() != 1 || source_args.len() != 1 {
        return Err(PgFrontendError::unsupported(
            "length() requires exactly one argument",
        ));
    }
    if expr_pg_type(&source_args[0])
        .is_some_and(|pg_type| pg_type.oid == u32::from(pgrx::pg_sys::BPCHAROID))
    {
        Ok(df_functions::pg_bpchar_length_udf().call(args))
    } else {
        call_unary_scalar_function(
            "length",
            datafusion::functions::unicode::character_length(),
            args,
        )
    }
}

pub(super) fn cast_args_to_utf8(args: Vec<Expr>) -> Vec<Expr> {
    args.into_iter()
        .map(|arg| Expr::Cast(Cast::new(Box::new(arg), DataType::Utf8)))
        .collect()
}

fn cast_args_to_pg_text(source_args: &[QueryExpr], args: Vec<Expr>) -> Vec<Expr> {
    args.into_iter()
        .enumerate()
        .map(|(index, arg)| {
            if source_args
                .get(index)
                .and_then(expr_pg_type)
                .is_some_and(|pg_type| pg_type.oid == u32::from(pgrx::pg_sys::BOOLOID))
            {
                df_functions::pg_boolout_udf().call(vec![arg])
            } else {
                Expr::Cast(Cast::new(Box::new(arg), DataType::Utf8))
            }
        })
        .collect()
}

fn cast_format_args(source_args: &[QueryExpr], args: Vec<Expr>) -> Vec<Expr> {
    args.into_iter()
        .enumerate()
        .map(|(index, arg)| {
            if index > 0
                && source_args
                    .get(index)
                    .and_then(expr_pg_type)
                    .is_some_and(|pg_type| pg_type.oid == u32::from(pgrx::pg_sys::BOOLOID))
            {
                arg
            } else {
                Expr::Cast(Cast::new(Box::new(arg), DataType::Utf8))
            }
        })
        .collect()
}

pub(super) fn call_unary_scalar_function(
    name: &'static str,
    udf: Arc<ScalarUDF>,
    args: Vec<Expr>,
) -> Result<Expr, PgFrontendError> {
    if args.len() != 1 {
        return Err(PgFrontendError::unsupported(format!(
            "{name}() requires exactly one argument"
        )));
    }
    Ok(udf.call(args))
}

pub(super) fn compile_coalesce(
    args: &[QueryExpr],
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let args = args
        .iter()
        .map(|arg| {
            compile_expr_with_windows(
                arg,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some((fallback, tests)) = args.split_last() else {
        return Err(PgFrontendError::unsupported(
            "COALESCE with no arguments is not supported by pg_frontend v1",
        ));
    };
    if tests.is_empty() {
        return Ok(fallback.clone());
    }

    let first = tests[0].clone();
    let mut builder =
        datafusion_expr::expr_fn::when(Expr::IsNotNull(Box::new(first.clone())), first);
    for arg in &tests[1..] {
        builder = builder.when(Expr::IsNotNull(Box::new(arg.clone())), arg.clone());
    }
    builder
        .otherwise(fallback.clone())
        .map_err(|err| PgFrontendError::unsupported(err.to_string()))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn compile_case(
    operand: Option<&QueryExpr>,
    when_then: &[(QueryExpr, QueryExpr)],
    else_expr: Option<&QueryExpr>,
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    let operand = operand
        .map(|expr| {
            compile_expr_with_windows(
                expr,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )
            .map(Box::new)
        })
        .transpose()?;
    let when_then_expr = when_then
        .iter()
        .map(|(when, then)| {
            Ok((
                Box::new(compile_expr_with_windows(
                    when,
                    query,
                    ctx,
                    window_bindings,
                    scalar_bindings,
                    aggregate_bindings,
                )?),
                Box::new(compile_expr_with_windows(
                    then,
                    query,
                    ctx,
                    window_bindings,
                    scalar_bindings,
                    aggregate_bindings,
                )?),
            ))
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let else_expr = else_expr
        .map(|expr| {
            compile_expr_with_windows(
                expr,
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )
            .map(Box::new)
        })
        .transpose()?;
    Ok(Expr::Case(Case {
        expr: operand,
        when_then_expr,
        else_expr,
    }))
}

pub(super) fn compile_scalar_subquery(
    subquery: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    if query_contains_outer_var(subquery) {
        let plan = compile_subquery_plan(subquery, ctx)?;
        return Ok(datafusion_expr::expr_fn::scalar_subquery(plan));
    }
    let plan = compile_typed_query(subquery, ctx.config)?.logical_plan;
    let plan = scalar_subquery_value_plan(plan, scalar_subquery_value_alias(0))?;
    let plan = Arc::new(plan);
    Ok(datafusion_expr::expr_fn::scalar_subquery(plan))
}

pub(super) fn binding_expr_matches(left: &QueryExpr, right: &QueryExpr) -> bool {
    left == right || format!("{left:?}") == format!("{right:?}")
}

pub(super) fn compile_subquery_plan(
    subquery: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Arc<LogicalPlan>, PgFrontendError> {
    Ok(Arc::new(
        compile_typed_query(subquery, ctx.config)?.logical_plan,
    ))
}

pub(super) fn coerce_binary_operands(
    op: QueryOperator,
    result_pg_type: pg_type::PgTypeRef,
    left_expr: Expr,
    right_expr: Expr,
    left_source: &QueryExpr,
    right_source: &QueryExpr,
) -> (Expr, Expr) {
    if matches!(
        op,
        QueryOperator::Eq
            | QueryOperator::NotEq
            | QueryOperator::IsDistinctFrom
            | QueryOperator::IsNotDistinctFrom
            | QueryOperator::Lt
            | QueryOperator::LtEq
            | QueryOperator::Gt
            | QueryOperator::GtEq
    ) {
        return coerce_comparison_operands(op, left_expr, right_expr, left_source, right_source);
    }
    if !matches!(
        op,
        QueryOperator::Plus
            | QueryOperator::Minus
            | QueryOperator::Multiply
            | QueryOperator::Divide
            | QueryOperator::Modulo
    ) {
        return (left_expr, right_expr);
    }
    let Some(left_type) = expr_pg_type(left_source) else {
        return (left_expr, right_expr);
    };
    let Some(right_type) = expr_pg_type(right_source) else {
        return (left_expr, right_expr);
    };
    let Some(data_type) =
        numeric_arithmetic_operand_type(result_pg_type, left_type.oid, right_type.oid)
    else {
        return (left_expr, right_expr);
    };
    (
        cast_expr_if_pg_arrow_type_differs(left_expr, left_type, &data_type),
        cast_expr_if_pg_arrow_type_differs(right_expr, right_type, &data_type),
    )
}

pub(super) fn coerce_comparison_operands(
    op: QueryOperator,
    left_expr: Expr,
    right_expr: Expr,
    left_source: &QueryExpr,
    right_source: &QueryExpr,
) -> (Expr, Expr) {
    if !matches!(
        op,
        QueryOperator::Eq
            | QueryOperator::NotEq
            | QueryOperator::IsDistinctFrom
            | QueryOperator::IsNotDistinctFrom
            | QueryOperator::Lt
            | QueryOperator::LtEq
            | QueryOperator::Gt
            | QueryOperator::GtEq
    ) {
        return (left_expr, right_expr);
    }
    let Some(left_type) = expr_pg_type(left_source) else {
        return (left_expr, right_expr);
    };
    let Some(right_type) = expr_pg_type(right_source) else {
        return (left_expr, right_expr);
    };
    if left_type.oid == u32::from(pgrx::pg_sys::NUMERICOID)
        && right_type.oid == u32::from(pgrx::pg_sys::NUMERICOID)
    {
        let left_arrow_type = arrow_type_for_pg_type(left_type);
        let right_arrow_type = arrow_type_for_pg_type(right_type);
        if left_arrow_type.is_some()
            && right_arrow_type.is_some()
            && left_arrow_type != right_arrow_type
        {
            let data_type = DataType::Decimal128(38, 16);
            return (
                cast_expr_if_pg_arrow_type_differs(left_expr, left_type, &data_type),
                cast_expr_if_pg_arrow_type_differs(right_expr, right_type, &data_type),
            );
        }
    }
    let Some(data_type) = common_numeric_comparison_type(left_type.oid, right_type.oid) else {
        return (left_expr, right_expr);
    };
    (
        cast_expr_if_pg_arrow_type_differs(left_expr, left_type, &data_type),
        cast_expr_if_pg_arrow_type_differs(right_expr, right_type, &data_type),
    )
}

pub(super) fn cast_expr_if_pg_arrow_type_differs(
    expr: Expr,
    pg_type: pg_type::PgTypeRef,
    data_type: &DataType,
) -> Expr {
    if arrow_type_for_pg_type(pg_type).as_ref() == Some(data_type) {
        expr
    } else {
        Expr::Cast(Cast::new(Box::new(expr), data_type.clone()))
    }
}

pub(super) fn numeric_arithmetic_operand_type(
    result_pg_type: pg_type::PgTypeRef,
    left_oid: u32,
    right_oid: u32,
) -> Option<DataType> {
    if !(is_numeric_comparison_oid(left_oid) && is_numeric_comparison_oid(right_oid)) {
        return None;
    }
    match result_pg_type.oid {
        oid if oid == u32::from(pgrx::pg_sys::INT2OID) => Some(DataType::Int16),
        oid if oid == u32::from(pgrx::pg_sys::INT4OID) => Some(DataType::Int32),
        oid if oid == u32::from(pgrx::pg_sys::INT8OID) => Some(DataType::Int64),
        oid if oid == u32::from(pgrx::pg_sys::FLOAT4OID) => Some(DataType::Float32),
        oid if oid == u32::from(pgrx::pg_sys::FLOAT8OID) => Some(DataType::Float64),
        oid if oid == u32::from(pgrx::pg_sys::NUMERICOID) => {
            arrow_type_for_pg_type(result_pg_type).or(Some(DataType::Decimal128(38, 16)))
        }
        _ => None,
    }
}

pub(super) fn common_numeric_comparison_type(left_oid: u32, right_oid: u32) -> Option<DataType> {
    if left_oid == right_oid {
        return None;
    }
    if !(is_numeric_comparison_oid(left_oid) && is_numeric_comparison_oid(right_oid)) {
        return None;
    }
    if left_oid == u32::from(pgrx::pg_sys::FLOAT8OID)
        || right_oid == u32::from(pgrx::pg_sys::FLOAT8OID)
        || left_oid == u32::from(pgrx::pg_sys::FLOAT4OID)
        || right_oid == u32::from(pgrx::pg_sys::FLOAT4OID)
    {
        Some(DataType::Float64)
    } else if left_oid == u32::from(pgrx::pg_sys::NUMERICOID)
        || right_oid == u32::from(pgrx::pg_sys::NUMERICOID)
    {
        Some(DataType::Decimal128(38, 16))
    } else {
        Some(DataType::Int64)
    }
}

pub(super) fn is_numeric_comparison_oid(oid: u32) -> bool {
    oid == u32::from(pgrx::pg_sys::INT2OID)
        || oid == u32::from(pgrx::pg_sys::INT4OID)
        || oid == u32::from(pgrx::pg_sys::INT8OID)
        || oid == u32::from(pgrx::pg_sys::FLOAT4OID)
        || oid == u32::from(pgrx::pg_sys::FLOAT8OID)
        || oid == u32::from(pgrx::pg_sys::NUMERICOID)
}

pub(super) fn compile_outer_var(var: &OuterVar) -> Result<Expr, PgFrontendError> {
    let data_type = arrow_type_for_pg_type(var.pg_type).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "outer-reference column {} has unsupported PostgreSQL type oid {}",
            var.name, var.pg_type.oid
        ))
    })?;
    let column = match var.relation.as_deref() {
        Some(relation) => Column::new(Some(relation), var.name.as_str()),
        None => Column::from_name(var.name.as_str()),
    };
    Ok(datafusion_expr::expr_fn::out_ref_col(data_type, column))
}

pub(super) fn compile_var(
    var: Var,
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<Expr, PgFrontendError> {
    if let Some(values) = ctx.values.get(&var.rtindex) {
        let index = values_column_index(var, values)?;
        return Ok(Expr::Column(Column::new(
            Some(table_reference_for_values(values)),
            values.columns[index].name.as_str(),
        )));
    }
    if let Some(cte) = ctx.ctes.get(&var.rtindex) {
        let index = cte_column_index(var, cte)?;
        return Ok(Expr::Column(Column::new(
            Some(table_reference_for_cte(cte)),
            cte.columns[index].name.as_str(),
        )));
    }
    if let Some(subquery) = ctx.subqueries.get(&var.rtindex) {
        let index = subquery_column_index(var, subquery)?;
        return Ok(Expr::Column(Column::new(
            Some(table_reference_for_subquery(subquery)),
            subquery.columns[index].name.as_str(),
        )));
    }
    let relation = relation_by_rtindex(query, var.rtindex)?;
    let resolved = ctx.table(var.rtindex)?;
    let index = var_column_index(var, resolved)?;
    let table_ref = table_reference_for_query_relation(relation, resolved);
    Ok(Expr::Column(Column::new(
        Some(table_ref),
        resolved.schema.field(index).name(),
    )))
}

pub(super) fn subquery_column_index(
    var: Var,
    subquery: &SubqueryRef,
) -> Result<usize, PgFrontendError> {
    subquery
        .columns
        .iter()
        .position(|column| column.attnum == var.attnum)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "attribute {} is not present in subquery range table",
                var.attnum
            ))
        })
}

pub(super) fn cte_column_index(var: Var, cte: &CteRangeRef) -> Result<usize, PgFrontendError> {
    cte.columns
        .iter()
        .position(|column| column.attnum == var.attnum)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "attribute {} is not present in CTE range table",
                var.attnum
            ))
        })
}

pub(super) fn values_column_index(var: Var, values: &ValuesRef) -> Result<usize, PgFrontendError> {
    values
        .columns
        .iter()
        .position(|column| column.attnum == var.attnum)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "attribute {} is not present in VALUES range table",
                var.attnum
            ))
        })
}

pub(super) fn var_column_index(
    var: Var,
    resolved: &ResolvedTable,
) -> Result<usize, PgFrontendError> {
    let index = resolved
        .column_attnums
        .iter()
        .position(|attnum| *attnum == var.attnum)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "attribute {} is not present in resolved relation",
                var.attnum
            ))
        })?;
    Ok(index)
}

pub(super) fn compile_bool(
    op: BoolOp,
    args: &[QueryExpr],
    query: &TypedQuery,
    ctx: &CompileContext,
    window_bindings: &[WindowBinding],
    scalar_bindings: &[ScalarSubqueryBinding],
    aggregate_bindings: &[AggregateBinding],
) -> Result<Expr, PgFrontendError> {
    match op {
        BoolOp::And | BoolOp::Or => {
            if args.is_empty() {
                return Err(PgFrontendError::unsupported(
                    "empty boolean expression is not supported",
                ));
            }
            let operator = if op == BoolOp::And {
                Operator::And
            } else {
                Operator::Or
            };
            let mut compiled = args
                .iter()
                .map(|arg| {
                    compile_expr_with_windows(
                        arg,
                        query,
                        ctx,
                        window_bindings,
                        scalar_bindings,
                        aggregate_bindings,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter();
            let first = compiled.next().expect("checked non-empty args");
            Ok(compiled.fold(first, |left, right| binary_expr(left, operator, right)))
        }
        BoolOp::Not => {
            if args.len() != 1 {
                return Err(PgFrontendError::unsupported(
                    "NOT expressions must have exactly one argument",
                ));
            }
            Ok(Expr::Not(Box::new(compile_expr_with_windows(
                &args[0],
                query,
                ctx,
                window_bindings,
                scalar_bindings,
                aggregate_bindings,
            )?)))
        }
    }
}
