use super::*;

pub(super) fn collect_join_quals(
    expr: &QueryExpr,
    left_relations: &HashSet<usize>,
    right_relations: &HashSet<usize>,
    query: &TypedQuery,
    ctx: &CompileContext,
    on: &mut Vec<(Expr, Expr)>,
    filters: &mut Vec<Expr>,
) -> Result<(), PgFrontendError> {
    if let QueryExpr::Bool {
        op: BoolOp::And,
        args,
    } = expr
    {
        for arg in args {
            collect_join_quals(
                arg,
                left_relations,
                right_relations,
                query,
                ctx,
                on,
                filters,
            )?;
        }
        return Ok(());
    }

    if let Some((left, right)) = equi_join_pair(expr, left_relations, right_relations) {
        let left_source = QueryExpr::Var(left);
        let right_source = QueryExpr::Var(right);
        let (left_expr, right_expr) = coerce_binary_operands(
            QueryOperator::Eq,
            pg_type::PgTypeRef::new(u32::from(pgrx::pg_sys::BOOLOID), -1, 0),
            compile_var(left, query, ctx)?,
            compile_var(right, query, ctx)?,
            &left_source,
            &right_source,
        );
        let (left_expr, right_expr) = compile_bpchar_equality_operands(
            QueryOperator::Eq,
            left_expr,
            right_expr,
            &left_source,
            &right_source,
        );
        on.push((left_expr, right_expr));
    } else {
        filters.push(compile_expr(expr, query, ctx)?);
    }
    Ok(())
}

pub(super) fn equi_join_pair(
    expr: &QueryExpr,
    left_relations: &HashSet<usize>,
    right_relations: &HashSet<usize>,
) -> Option<(Var, Var)> {
    let QueryExpr::BinaryOp {
        op: QueryOperator::Eq,
        left,
        right,
        ..
    } = expr
    else {
        return None;
    };
    let (left_var, right_var) = (expr_var(left)?, expr_var(right)?);
    if left_relations.contains(&left_var.rtindex) && right_relations.contains(&right_var.rtindex) {
        Some((left_var, right_var))
    } else if left_relations.contains(&right_var.rtindex)
        && right_relations.contains(&left_var.rtindex)
    {
        Some((right_var, left_var))
    } else {
        None
    }
}

pub(super) fn expr_var(expr: &QueryExpr) -> Option<Var> {
    match expr {
        QueryExpr::Var(var) => Some(*var),
        QueryExpr::RelabelType(inner) => expr_var(inner),
        _ => None,
    }
}

pub(super) fn from_rtindexes(item: &FromItem) -> HashSet<usize> {
    let mut indexes = HashSet::new();
    collect_from_rtindexes(item, &mut indexes);
    indexes
}

pub(super) fn collect_from_rtindexes(item: &FromItem, indexes: &mut HashSet<usize>) {
    match item {
        FromItem::Empty => {}
        FromItem::Relation { rtindex } => {
            indexes.insert(*rtindex);
        }
        FromItem::Values { rtindex } => {
            indexes.insert(*rtindex);
        }
        FromItem::Cte { rtindex } => {
            indexes.insert(*rtindex);
        }
        FromItem::Subquery { rtindex } => {
            indexes.insert(*rtindex);
        }
        FromItem::Join { left, right, .. } => {
            collect_from_rtindexes(left, indexes);
            collect_from_rtindexes(right, indexes);
        }
    }
}

pub(super) fn join_type(kind: JoinKind) -> JoinType {
    match kind {
        JoinKind::Inner => JoinType::Inner,
        JoinKind::Left => JoinType::Left,
        JoinKind::Right => JoinType::Right,
        JoinKind::Full => JoinType::Full,
    }
}

pub(super) fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

pub(super) fn compile_binary_expr(
    op: QueryOperator,
    result_pg_type: pg_type::PgTypeRef,
    left_expr: Expr,
    right_expr: Expr,
    left_source: &QueryExpr,
    right_source: &QueryExpr,
) -> Result<Expr, PgFrontendError> {
    if matches!(
        op,
        QueryOperator::BitwiseShiftLeft | QueryOperator::BitwiseShiftRight
    ) {
        return Ok(compile_bitwise_shift_expr(
            op,
            result_pg_type,
            left_expr,
            right_expr,
        ));
    }

    if let Some(udf) = regex_match_udf(op) {
        return Ok(udf.call(vec![left_expr, right_expr]));
    }

    if let Some(policy) =
        numeric_decimal_arithmetic_policy(op, result_pg_type, left_source, right_source)?
    {
        let left_expr =
            cast_expr_if_pg_arrow_type_differs(left_expr, policy.left_pg_type, &policy.left_type);
        let right_expr = cast_expr_if_pg_arrow_type_differs(
            right_expr,
            policy.right_pg_type,
            &policy.right_type,
        );
        let expr = binary_expr(left_expr, operator(op), right_expr);
        return Ok(Expr::Cast(Cast::new(Box::new(expr), policy.result_type)));
    }

    let (left_expr, right_expr) = coerce_binary_operands(
        op,
        result_pg_type,
        left_expr,
        right_expr,
        left_source,
        right_source,
    );
    Ok(
        if let Some(udf) = checked_integer_arithmetic_udf(op, result_pg_type) {
            udf.call(vec![left_expr, right_expr])
        } else {
            let (left_expr, right_expr) = compile_bpchar_equality_operands(
                op,
                left_expr,
                right_expr,
                left_source,
                right_source,
            );
            binary_expr(left_expr, operator(op), right_expr)
        },
    )
}

fn compile_bpchar_equality_operands(
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
    ) {
        return (left_expr, right_expr);
    }

    (
        compile_bpchar_equality_operand(left_expr, left_source),
        compile_bpchar_equality_operand(right_expr, right_source),
    )
}

fn compile_bpchar_equality_operand(expr: Expr, source: &QueryExpr) -> Expr {
    if expr_pg_type(source).is_some_and(|pg_type| pg_type.oid == u32::from(pgrx::pg_sys::BPCHAROID))
    {
        df_functions::pg_bpchar_cmp_key_udf().call(vec![expr])
    } else {
        expr
    }
}

fn checked_integer_arithmetic_udf(
    op: QueryOperator,
    result_pg_type: pg_type::PgTypeRef,
) -> Option<Arc<ScalarUDF>> {
    if !is_checked_integer_arithmetic_result(result_pg_type) {
        return None;
    }
    Some(match op {
        QueryOperator::Plus => df_functions::pg_int_add_checked_udf(),
        QueryOperator::Minus => df_functions::pg_int_sub_checked_udf(),
        QueryOperator::Multiply => df_functions::pg_int_mul_checked_udf(),
        _ => return None,
    })
}

fn regex_match_udf(op: QueryOperator) -> Option<Arc<ScalarUDF>> {
    Some(match op {
        QueryOperator::RegexMatch => df_functions::pg_regex_match_udf(),
        QueryOperator::RegexNotMatch => df_functions::pg_regex_not_match_udf(),
        _ => return None,
    })
}

fn is_checked_integer_arithmetic_result(pg_type: pg_type::PgTypeRef) -> bool {
    pg_type.oid == u32::from(pgrx::pg_sys::INT2OID)
        || pg_type.oid == u32::from(pgrx::pg_sys::INT4OID)
        || pg_type.oid == u32::from(pgrx::pg_sys::INT8OID)
}

pub(super) fn compile_bitwise_shift_expr(
    op: QueryOperator,
    result_pg_type: pg_type::PgTypeRef,
    left: Expr,
    right: Expr,
) -> Expr {
    let result_type = arrow_type_for_pg_type(result_pg_type).unwrap_or(DataType::Int64);
    let work_type = match result_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::UInt64,
        _ => result_type.clone(),
    };
    let shifted = binary_expr(
        Expr::Cast(Cast::new(Box::new(left), work_type.clone())),
        operator(op),
        Expr::Cast(Cast::new(Box::new(right), work_type.clone())),
    );
    if work_type == result_type {
        shifted
    } else {
        Expr::Cast(Cast::new(Box::new(shifted), result_type))
    }
}

pub(super) fn compile_unary_op(op: QueryUnaryOperator, arg: Expr) -> Expr {
    match op {
        QueryUnaryOperator::Plus => arg,
        QueryUnaryOperator::Minus => Expr::Negative(Box::new(arg)),
    }
}

pub(super) fn operator(op: QueryOperator) -> Operator {
    match op {
        QueryOperator::Eq => Operator::Eq,
        QueryOperator::NotEq => Operator::NotEq,
        QueryOperator::IsDistinctFrom => Operator::IsDistinctFrom,
        QueryOperator::IsNotDistinctFrom => Operator::IsNotDistinctFrom,
        QueryOperator::Lt => Operator::Lt,
        QueryOperator::LtEq => Operator::LtEq,
        QueryOperator::Gt => Operator::Gt,
        QueryOperator::GtEq => Operator::GtEq,
        QueryOperator::Plus => Operator::Plus,
        QueryOperator::Minus => Operator::Minus,
        QueryOperator::Multiply => Operator::Multiply,
        QueryOperator::Divide => Operator::Divide,
        QueryOperator::Modulo => Operator::Modulo,
        QueryOperator::BitwiseShiftLeft => Operator::BitwiseShiftLeft,
        QueryOperator::BitwiseShiftRight => Operator::BitwiseShiftRight,
        QueryOperator::StringConcat => Operator::StringConcat,
        QueryOperator::LikeMatch => Operator::LikeMatch,
        QueryOperator::NotLikeMatch => Operator::NotLikeMatch,
        QueryOperator::ILikeMatch => Operator::ILikeMatch,
        QueryOperator::NotILikeMatch => Operator::NotILikeMatch,
        QueryOperator::RegexMatch => Operator::RegexMatch,
        QueryOperator::RegexNotMatch => Operator::RegexNotMatch,
    }
}
