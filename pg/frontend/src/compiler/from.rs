use super::*;

pub(super) fn base_plan(
    query: &TypedQuery,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    if let Some(set_operation) = query.set_operation.as_ref() {
        let plan = compile_set_operation_tree(set_operation, ctx)?;
        let rtindex = first_set_operation_rtindex(set_operation)?;
        let subquery = ctx.subquery(rtindex)?;
        return Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(plan),
            table_reference_for_subquery(subquery),
        )?));
    }

    if let Some((selection, exists_subqueries)) = extract_top_level_correlated_exists(query) {
        let mut outer_query = query.clone();
        outer_query.selection = selection.clone();
        add_correlated_outer_targets(&mut outer_query, &exists_subqueries)?;
        let selection = split_selection_for_scan_pushdown(&outer_query)?;
        let mut plan = build_from_item(
            &outer_query,
            &outer_query.from,
            ctx,
            &selection.scan_filters,
        )?;
        if let Some(selection) = selection.residual.as_ref() {
            let mut scalar_bindings = Vec::new();
            plan = attach_scalar_subqueries(plan, selection, ctx, &mut scalar_bindings)?;
            let predicate = compile_expr_with_windows(
                selection,
                &outer_query,
                ctx,
                &[],
                &scalar_bindings,
                &[],
            )?;
            plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
                predicate,
                Arc::new(plan),
            )?);
        }
        for subquery in exists_subqueries {
            plan = apply_correlated_exists_join(plan, &subquery, ctx)?;
        }
        return Ok(plan);
    }

    if let Some((selection, in_subqueries)) = extract_top_level_correlated_in(query) {
        let mut outer_query = query.clone();
        outer_query.selection = selection.clone();
        add_correlated_in_outer_targets(&mut outer_query, &in_subqueries)?;
        let selection = split_selection_for_scan_pushdown(&outer_query)?;
        let mut plan = build_from_item(
            &outer_query,
            &outer_query.from,
            ctx,
            &selection.scan_filters,
        )?;
        if let Some(selection) = selection.residual.as_ref() {
            let mut scalar_bindings = Vec::new();
            plan = attach_scalar_subqueries(plan, selection, ctx, &mut scalar_bindings)?;
            let predicate = compile_expr_with_windows(
                selection,
                &outer_query,
                ctx,
                &[],
                &scalar_bindings,
                &[],
            )?;
            plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
                predicate,
                Arc::new(plan),
            )?);
        }
        for subquery in in_subqueries {
            plan = apply_correlated_in_join(plan, &subquery, &outer_query, ctx)?;
        }
        return Ok(plan);
    }

    let selection = split_selection_for_scan_pushdown(query)?;
    let mut plan = build_from_item(query, &query.from, ctx, &selection.scan_filters)?;
    if let Some(selection) = selection.residual.as_ref() {
        let mut scalar_bindings = Vec::new();
        plan = attach_scalar_subqueries(plan, selection, ctx, &mut scalar_bindings)?;
        let predicate =
            compile_expr_with_windows(selection, query, ctx, &[], &scalar_bindings, &[])?;
        plan = LogicalPlan::Filter(datafusion_expr::logical_plan::Filter::try_new(
            predicate,
            Arc::new(plan),
        )?);
    }
    Ok(plan)
}

#[derive(Debug, Default)]
pub(super) struct SelectionPushdown {
    pub scan_filters: HashMap<usize, Vec<QueryExpr>>,
    pub residual: Option<QueryExpr>,
}

pub(super) fn split_selection_for_scan_pushdown(
    query: &TypedQuery,
) -> Result<SelectionPushdown, PgFrontendError> {
    let Some(selection) = query.selection.as_ref() else {
        return Ok(SelectionPushdown::default());
    };

    let pushable_rtindexes = top_level_where_pushdown_rtindexes(&query.from);
    let mut scan_filters: HashMap<usize, Vec<QueryExpr>> = HashMap::new();
    let mut residual = Vec::new();

    for predicate in split_and_predicates(selection) {
        if can_push_predicate_into_scan(&predicate) {
            let referenced_rtindexes = predicate_rtindexes(&predicate);
            match referenced_rtindexes.len() {
                1 => {
                    let rtindex = *referenced_rtindexes.iter().next().expect("one rtindex");
                    if pushable_rtindexes.contains(&rtindex) {
                        scan_filters.entry(rtindex).or_default().push(predicate);
                        continue;
                    }
                }
                0 if residual_filter_needs_pg_text_semantics(&predicate) => {
                    if let Some(rtindex) = pushable_rtindexes.iter().next().copied() {
                        scan_filters.entry(rtindex).or_default().push(predicate);
                        continue;
                    }
                }
                _ => {}
            }
        }
        reject_pg_sensitive_residual_filter(&predicate)?;
        residual.push(predicate);
    }

    Ok(SelectionPushdown {
        scan_filters,
        residual: combine_and_predicates(residual),
    })
}

pub(super) fn can_push_predicate_into_scan(predicate: &QueryExpr) -> bool {
    !contains_scalar_subquery(predicate)
        && !contains_predicate_subquery(predicate)
        && !expr_contains_outer_var(predicate)
        && !contains_aggregate_call(predicate)
}

pub(super) fn top_level_where_pushdown_rtindexes(item: &FromItem) -> HashSet<usize> {
    let mut rtindexes = HashSet::new();
    collect_top_level_where_pushdown_rtindexes(item, &mut rtindexes);
    rtindexes
}

pub(super) fn collect_top_level_where_pushdown_rtindexes(
    item: &FromItem,
    rtindexes: &mut HashSet<usize>,
) {
    match item {
        FromItem::Relation { rtindex } => {
            rtindexes.insert(*rtindex);
        }
        FromItem::Join {
            kind: JoinKind::Inner,
            left,
            right,
            ..
        } => {
            collect_top_level_where_pushdown_rtindexes(left, rtindexes);
            collect_top_level_where_pushdown_rtindexes(right, rtindexes);
        }
        FromItem::Join {
            kind: JoinKind::Left,
            left,
            ..
        } => collect_top_level_where_pushdown_rtindexes(left, rtindexes),
        FromItem::Join {
            kind: JoinKind::Right,
            right,
            ..
        } => collect_top_level_where_pushdown_rtindexes(right, rtindexes),
        FromItem::Join {
            kind: JoinKind::Full,
            ..
        }
        | FromItem::Empty
        | FromItem::Values { .. }
        | FromItem::Cte { .. }
        | FromItem::Subquery { .. } => {}
    }
}

pub(super) fn predicate_rtindexes(expr: &QueryExpr) -> HashSet<usize> {
    let mut rtindexes = HashSet::new();
    collect_expr_rtindexes(expr, &mut rtindexes);
    rtindexes
}

pub(super) fn collect_expr_rtindexes(expr: &QueryExpr, rtindexes: &mut HashSet<usize>) {
    match expr {
        QueryExpr::Var(var) => {
            rtindexes.insert(var.rtindex);
        }
        QueryExpr::RelabelType(inner)
        | QueryExpr::Cast { arg: inner, .. }
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => collect_expr_rtindexes(inner, rtindexes),
        QueryExpr::FunctionCall { args, .. }
        | QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => {
            for arg in args {
                collect_expr_rtindexes(arg, rtindexes);
            }
        }
        QueryExpr::ArraySubscript { array, index, .. } => {
            collect_expr_rtindexes(array, rtindexes);
            collect_expr_rtindexes(index, rtindexes);
        }
        QueryExpr::BinaryOp { left, right, .. } => {
            collect_expr_rtindexes(left, rtindexes);
            collect_expr_rtindexes(right, rtindexes);
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            for arg in args {
                collect_expr_rtindexes(arg, rtindexes);
            }
            if let Some(filter) = filter {
                collect_expr_rtindexes(filter, rtindexes);
            }
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                collect_expr_rtindexes(operand, rtindexes);
            }
            for (when, then) in when_then {
                collect_expr_rtindexes(when, rtindexes);
                collect_expr_rtindexes(then, rtindexes);
            }
            if let Some(else_expr) = else_expr {
                collect_expr_rtindexes(else_expr, rtindexes);
            }
        }
        QueryExpr::InSubquery { expr, .. } => collect_expr_rtindexes(expr, rtindexes),
        QueryExpr::Const(_)
        | QueryExpr::Param(_)
        | QueryExpr::OuterVar(_)
        | QueryExpr::ScalarSubquery(_)
        | QueryExpr::ExistsSubquery { .. } => {}
    }
}

pub(super) fn reject_pg_sensitive_residual_filter(expr: &QueryExpr) -> Result<(), PgFrontendError> {
    if residual_filter_needs_pg_text_semantics(expr) {
        Err(PgFrontendError::unsupported(
            "pg_frontend cannot execute residual text-like WHERE filters above joins with DataFusion semantics",
        ))
    } else {
        Ok(())
    }
}

pub(super) fn residual_filter_needs_pg_text_semantics(expr: &QueryExpr) -> bool {
    match expr {
        QueryExpr::Cast { arg, pg_type } => {
            pg_text_cast_needs_pg_semantics(*pg_type)
                || residual_filter_needs_pg_text_semantics(arg)
        }
        QueryExpr::RelabelType(inner)
        | QueryExpr::UnaryOp { arg: inner, .. }
        | QueryExpr::NullTest { arg: inner, .. }
        | QueryExpr::BooleanTest { arg: inner, .. } => {
            residual_filter_needs_pg_text_semantics(inner)
        }
        QueryExpr::BinaryOp {
            op, left, right, ..
        } => {
            binary_op_needs_pg_text_semantics(*op, left, right)
                || residual_filter_needs_pg_text_semantics(left)
                || residual_filter_needs_pg_text_semantics(right)
        }
        QueryExpr::FunctionCall { func, args, .. } => {
            function_call_needs_pg_text_semantics(*func, args)
                || args.iter().any(residual_filter_needs_pg_text_semantics)
        }
        QueryExpr::Array { elements: args, .. }
        | QueryExpr::Coalesce { args, .. }
        | QueryExpr::Bool { args, .. } => args.iter().any(residual_filter_needs_pg_text_semantics),
        QueryExpr::ArraySubscript { array, index, .. } => {
            residual_filter_needs_pg_text_semantics(array)
                || residual_filter_needs_pg_text_semantics(index)
        }
        QueryExpr::AggregateCall { args, filter, .. }
        | QueryExpr::WindowCall { args, filter, .. } => {
            args.iter().any(residual_filter_needs_pg_text_semantics)
                || filter
                    .as_deref()
                    .is_some_and(residual_filter_needs_pg_text_semantics)
        }
        QueryExpr::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            operand
                .as_deref()
                .is_some_and(residual_filter_needs_pg_text_semantics)
                || when_then.iter().any(|(when, then)| {
                    residual_filter_needs_pg_text_semantics(when)
                        || residual_filter_needs_pg_text_semantics(then)
                })
                || else_expr
                    .as_deref()
                    .is_some_and(residual_filter_needs_pg_text_semantics)
        }
        QueryExpr::InSubquery { expr, .. } => residual_filter_needs_pg_text_semantics(expr),
        QueryExpr::Const(_)
        | QueryExpr::Param(_)
        | QueryExpr::Var(_)
        | QueryExpr::OuterVar(_)
        | QueryExpr::ScalarSubquery(_)
        | QueryExpr::ExistsSubquery { .. } => false,
    }
}

pub(super) fn binary_op_needs_pg_text_semantics(
    op: QueryOperator,
    left: &QueryExpr,
    right: &QueryExpr,
) -> bool {
    let left_type = expr_pg_type(left);
    let right_type = expr_pg_type(right);
    if matches!(op, QueryOperator::RegexMatch | QueryOperator::RegexNotMatch) {
        return left_type.is_some_and(pg_text_type_has_unsupported_collation)
            || right_type.is_some_and(pg_text_type_has_unsupported_collation);
    }
    if matches!(
        op,
        QueryOperator::StringConcat
            | QueryOperator::LikeMatch
            | QueryOperator::NotLikeMatch
            | QueryOperator::ILikeMatch
            | QueryOperator::NotILikeMatch
    ) && (left_type.is_some_and(is_pg_text_like) || right_type.is_some_and(is_pg_text_like))
    {
        return true;
    }
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
        return false;
    }
    let text_comparison =
        left_type.is_some_and(is_pg_text_like) || right_type.is_some_and(is_pg_text_like);
    if !text_comparison {
        return false;
    }
    if matches!(
        op,
        QueryOperator::Lt | QueryOperator::LtEq | QueryOperator::Gt | QueryOperator::GtEq
    ) {
        return true;
    }
    left_type.is_some_and(pg_text_type_needs_pg_equality_semantics)
        || right_type.is_some_and(pg_text_type_needs_pg_equality_semantics)
}

pub(super) fn function_call_needs_pg_text_semantics(
    func: ScalarFunction,
    args: &[QueryExpr],
) -> bool {
    let has_bpchar_arg = args
        .iter()
        .filter_map(expr_pg_type)
        .any(|pg_type| pg_type.oid == u32::from(pgrx::pg_sys::BPCHAROID));
    if !has_bpchar_arg {
        return false;
    }

    match func {
        ScalarFunction::Length => false,
        _ => true,
    }
}

pub(super) fn pg_text_cast_needs_pg_semantics(pg_type: pg_type::PgTypeRef) -> bool {
    pg_text_type_has_unsupported_collation(pg_type)
}

pub(super) fn pg_text_type_needs_pg_equality_semantics(pg_type: pg_type::PgTypeRef) -> bool {
    pg_text_type_has_unsupported_collation(pg_type)
}

pub(super) fn is_pg_text_like(pg_type: pg_type::PgTypeRef) -> bool {
    is_text_like_type(pg_type.oid)
}

pub(super) fn pg_text_type_has_unsupported_collation(pg_type: pg_type::PgTypeRef) -> bool {
    is_text_like_type(pg_type.oid)
        && pg_type.collation != 0
        && pg_type.collation != u32::from(pgrx::pg_sys::DEFAULT_COLLATION_OID)
        && !(pg_type.oid == u32::from(pgrx::pg_sys::NAMEOID)
            && pg_type.collation == u32::from(pgrx::pg_sys::C_COLLATION_OID))
}

pub(super) fn first_set_operation_rtindex(
    operation: &SetOperationTree,
) -> Result<usize, PgFrontendError> {
    match operation {
        SetOperationTree::Range { rtindex } => Ok(*rtindex),
        SetOperationTree::Operation { left, .. } => first_set_operation_rtindex(left),
    }
}

pub(super) fn compile_set_operation_tree(
    operation: &SetOperationTree,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    match operation {
        SetOperationTree::Range { rtindex } => build_subquery_scan(*rtindex, ctx),
        SetOperationTree::Operation {
            op,
            all,
            left,
            right,
        } => {
            let left = compile_set_operation_tree(left, ctx)?;
            let right = compile_set_operation_tree(right, ctx)?;
            match op {
                SetOperator::Union => {
                    let union = LogicalPlan::Union(Union::try_new_with_loose_types(vec![
                        Arc::new(left),
                        Arc::new(right),
                    ])?);
                    if *all {
                        Ok(union)
                    } else {
                        Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(union))))
                    }
                }
            }
        }
    }
}

pub(super) fn build_from_item(
    query: &TypedQuery,
    item: &FromItem,
    ctx: &CompileContext,
    scan_filters: &HashMap<usize, Vec<QueryExpr>>,
) -> Result<LogicalPlan, PgFrontendError> {
    match item {
        FromItem::Empty => empty_plan(query, ctx),
        FromItem::Relation { rtindex } => build_relation_scan(query, *rtindex, ctx, scan_filters),
        FromItem::Values { rtindex } => build_values_scan(query, *rtindex, ctx),
        FromItem::Cte { rtindex } => build_cte_scan(query, *rtindex, ctx),
        FromItem::Subquery { rtindex } => build_subquery_scan(*rtindex, ctx),
        FromItem::Join {
            kind,
            left,
            right,
            quals,
        } => build_join(query, *kind, left, right, quals.as_ref(), ctx, scan_filters),
    }
}

pub(super) fn build_subquery_scan(
    rtindex: usize,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let subquery = ctx.subquery(rtindex)?;
    let input = compile_typed_query(&subquery.query, ctx.config)?.logical_plan;
    if input.schema().fields().len() != subquery.columns.len() {
        return Err(PgFrontendError::unsupported(format!(
            "subquery range table has {} column(s) but compiled subquery has {} field(s)",
            subquery.columns.len(),
            input.schema().fields().len()
        )));
    }
    let projection = subquery
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let (qualifier, field) = input.schema().qualified_field(index);
            alias_with_field_metadata(
                Expr::Column(Column::from((qualifier, field))),
                column.name.clone(),
                field,
            )
        })
        .collect::<Vec<_>>();
    let projected = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(input))?);
    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(projected),
        table_reference_for_subquery(subquery),
    )?))
}

pub(super) fn build_cte_scan(
    query: &TypedQuery,
    rtindex: usize,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let cte_ref = ctx.cte(rtindex)?;
    let cte_def = query
        .ctes
        .iter()
        .find(|cte| cte.id == cte_ref.cte_id)
        .ok_or_else(|| {
            PgFrontendError::unsupported(format!(
                "CTE range table {} has no matching CTE definition",
                cte_ref.name
            ))
        })?;
    let input = compile_typed_query(&cte_def.query, ctx.config)?.logical_plan;
    if input.schema().fields().len() != cte_ref.columns.len() {
        return Err(PgFrontendError::unsupported(format!(
            "CTE {} has {} column(s) but compiled CTE query has {} field(s)",
            cte_ref.name,
            cte_ref.columns.len(),
            input.schema().fields().len()
        )));
    }
    let projection = cte_ref
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let (qualifier, field) = input.schema().qualified_field(index);
            alias_with_field_metadata(
                Expr::Column(Column::from((qualifier, field))),
                column.name.clone(),
                field,
            )
        })
        .collect::<Vec<_>>();
    let input = LogicalPlan::Projection(Projection::try_new(projection, Arc::new(input))?);
    let fields = cte_ref
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let field = input.schema().field(index);
            let mut output_field = pg_output_field(
                &column.name,
                field.data_type().clone(),
                column.nullable,
                column.pg_type,
            );
            if !field.metadata().is_empty() {
                output_field = output_field.with_metadata(field.metadata().clone());
            }
            Ok(output_field)
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let schema = Schema::new(fields);
    let schema = Arc::new(DFSchema::try_from_qualified_schema(
        table_reference_for_cte(cte_ref),
        &schema,
    )?);
    Ok(scan_node::PgCteRefNode::new(
        cte_ref.cte_id,
        cte_ref.name.clone(),
        input,
        schema,
        None,
        None,
    )
    .into_logical_plan())
}

pub(super) fn build_values_scan(
    query: &TypedQuery,
    rtindex: usize,
    ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    let values = ctx.values(rtindex)?;
    let fields = values
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let data_type =
                values_column_data_type(values, index, column.pg_type)?.ok_or_else(|| {
                    PgFrontendError::unsupported(format!(
                        "VALUES column {} has unsupported PostgreSQL type oid {}",
                        column.name, column.pg_type.oid
                    ))
                })?;
            Ok(pg_output_field(
                column.name.clone(),
                data_type,
                column.nullable,
                column.pg_type,
            ))
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let table_ref = table_reference_for_values(values);
    let data_types = fields
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    let schema = Schema::new(fields);
    let schema = Arc::new(DFSchema::try_from(schema)?);
    let rows = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(index, expr)| {
                    let expr = compile_expr(expr, query, ctx)?;
                    Ok::<_, PgFrontendError>(Expr::Cast(Cast::new(
                        Box::new(expr),
                        data_types[index].clone(),
                    )))
                })
                .collect::<Result<Vec<_>, PgFrontendError>>()
        })
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let values_plan = LogicalPlan::Values(Values {
        schema,
        values: rows,
    });
    Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(values_plan),
        table_ref,
    )?))
}

pub(super) fn values_column_data_type(
    values: &ValuesRef,
    index: usize,
    pg_type: pg_type::PgTypeRef,
) -> Result<Option<DataType>, PgFrontendError> {
    if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) && pg_type.typmod < 0 {
        if let Some(scale) = values_numeric_column_scale(values, index)? {
            return Ok(Some(DataType::Decimal128(38, scale)));
        }
    }
    Ok(arrow_type_for_pg_type(pg_type))
}

pub(super) fn values_numeric_column_scale(
    values: &ValuesRef,
    index: usize,
) -> Result<Option<i8>, PgFrontendError> {
    let mut max_scale = 0_i8;
    for row in &values.rows {
        let Some(expr) = row.get(index) else {
            return Ok(None);
        };
        let Some(scale) = numeric_arithmetic_expr_scale(expr)? else {
            return Ok(None);
        };
        max_scale = max_scale.max(scale);
    }
    Ok(Some(max_scale))
}

pub(super) fn empty_plan(
    _query: &TypedQuery,
    _ctx: &CompileContext,
) -> Result<LogicalPlan, PgFrontendError> {
    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(DFSchema::empty()),
    }))
}

pub(super) fn build_relation_scan(
    query: &TypedQuery,
    rtindex: usize,
    ctx: &CompileContext,
    scan_filters: &HashMap<usize, Vec<QueryExpr>>,
) -> Result<LogicalPlan, PgFrontendError> {
    let relation = relation_by_rtindex(query, rtindex)?;
    let resolved = ctx.table(rtindex)?;
    let table_ref = table_reference_for_query_relation(relation, resolved);
    let filters = scan_filters
        .get(&rtindex)
        .into_iter()
        .flat_map(|filters| filters.iter())
        .map(|expr| compile_expr(expr, query, ctx))
        .collect::<Result<Vec<_>, PgFrontendError>>()?;
    let scan_projection = if query.has_aggregates {
        None
    } else {
        let include_resjunk = !query.sort.is_empty()
            || query.has_windows
            || matches!(query.distinct, DistinctSpec::On { .. });
        Some(relation_projection(
            query,
            rtindex,
            resolved,
            include_resjunk,
        )?)
    };
    let source = Arc::new(PgPlanningTableSource::new(resolved.clone())) as Arc<dyn TableSource>;
    let table_scan = TableScan::try_new(table_ref, source, scan_projection, filters, None)?;
    Ok(LogicalPlan::TableScan(table_scan))
}

pub(super) fn build_join(
    query: &TypedQuery,
    kind: JoinKind,
    left: &FromItem,
    right: &FromItem,
    quals: Option<&QueryExpr>,
    ctx: &CompileContext,
    scan_filters: &HashMap<usize, Vec<QueryExpr>>,
) -> Result<LogicalPlan, PgFrontendError> {
    let left_plan = build_from_item(query, left, ctx, scan_filters)?;
    let right_plan = build_from_item(query, right, ctx, scan_filters)?;
    let left_relations = from_rtindexes(left);
    let right_relations = from_rtindexes(right);
    let mut on = Vec::new();
    let mut filters = Vec::new();
    if let Some(quals) = quals {
        collect_join_quals(
            quals,
            &left_relations,
            &right_relations,
            query,
            ctx,
            &mut on,
            &mut filters,
        )?;
    }
    let filter = filters
        .into_iter()
        .reduce(|left, right| binary_expr(left, Operator::And, right));
    if kind == JoinKind::Right {
        let projection = schema_projection_exprs(left_plan.schema())
            .into_iter()
            .chain(schema_projection_exprs(right_plan.schema()))
            .collect::<Vec<_>>();
        let join = Join::try_new(
            Arc::new(right_plan),
            Arc::new(left_plan),
            on.into_iter().map(|(left, right)| (right, left)).collect(),
            filter,
            JoinType::Left,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join = LogicalPlan::Join(join);
        return Ok(LogicalPlan::Projection(Projection::try_new(
            projection,
            Arc::new(join),
        )?));
    }
    let join = Join::try_new(
        Arc::new(left_plan),
        Arc::new(right_plan),
        on,
        filter,
        join_type(kind),
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;
    Ok(LogicalPlan::Join(join))
}

pub(super) fn schema_projection_exprs(schema: &DFSchema) -> Vec<Expr> {
    (0..schema.fields().len())
        .map(|index| {
            let (qualifier, field) = schema.qualified_field(index);
            Expr::Column(Column::from((qualifier, field)))
        })
        .collect()
}
