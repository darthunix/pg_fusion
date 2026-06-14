use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, CStr};
use std::rc::Rc;
use std::slice;
use std::str;

use datafusion_common::ScalarValue;
use pg_type::{
    date32_from_pg_date, numeric_display_scale, numeric_shape_from_typmod, numeric_to_decimal128,
    scalar_for_pg_const, NumericDecodeError, PgNumericConst,
};
use pgrx::datum::FromDatum;
use pgrx::pg_sys;

use crate::error::PgFrontendError;
use crate::shippability::{supported_non_null_const_type, supported_value_type};
use crate::typed_query::{
    AggregateFunction, BoolOp, BooleanTestKind, ColumnRef, Const, CteDef, CteRangeRef,
    DistinctSpec, FromItem, GroupingSetSpec, JoinKind, OuterVar, Param, PgConstValue, PgTypeRef,
    QueryCommand, QueryExpr, QueryOperator, RelationRef, ScalarFunction, SetOperationTree,
    SetOperator, SortKey, SubqueryRef, Target, TypedQuery, ValuesRef, Var, WindowFrameBound,
    WindowFrameSpec, WindowFrameUnits, WindowFunctionKind, WindowSpec,
};

use super::common::{
    bool_op, cstr_from_pg, expr_type_ref, list_int_at, list_len, list_oid_at, list_ptr_at,
    param_kind, read_operator, read_unary_operator, time_const, type_ref,
    unsupported_temporal_const,
};

pub(crate) unsafe fn read_query(query: *mut pg_sys::Query) -> Result<TypedQuery, PgFrontendError> {
    unsafe { read_query_with_scope(query, &CteScope::default()) }
}

unsafe fn read_query_with_scope(
    query: *mut pg_sys::Query,
    scope: &CteScope,
) -> Result<TypedQuery, PgFrontendError> {
    if query.is_null() {
        return Err(PgFrontendError::NullQuery);
    }
    let query_ref = unsafe { &*query };
    if query_ref.commandType != pg_sys::CmdType::CMD_SELECT {
        return Err(PgFrontendError::unsupported(
            "only SELECT queries are supported",
        ));
    }
    if query_ref.hasModifyingCTE {
        return Err(PgFrontendError::unsupported(
            "data-modifying CTEs are not supported",
        ));
    }
    validate_limit_option(query_ref.limitOption)?;

    let mut visible_ctes = scope.clone();
    unsafe { read_ctes(query_ref.cteList, &mut visible_ctes) }?;
    let rtable = unsafe { read_rtable(query_ref.rtable, &visible_ctes) }?;
    let expr_scope = visible_ctes
        .with_current_columns(rtable.outer_columns.clone())
        .with_join_aliases(rtable.join_aliases.clone());
    let from = unsafe {
        read_from_item(
            query_ref.jointree,
            &rtable.values_rtindexes(),
            &rtable.cte_rtindexes(),
            &rtable.subquery_rtindexes(),
            &expr_scope,
        )
    }?;
    let selection =
        if query_ref.jointree.is_null() || unsafe { (*query_ref.jointree).quals }.is_null() {
            None
        } else {
            Some(unsafe { read_expr((*query_ref.jointree).quals, &expr_scope) }?)
        };
    let having = if query_ref.havingQual.is_null() {
        None
    } else {
        Some(unsafe { read_expr(query_ref.havingQual, &expr_scope) }?)
    };
    let targets = unsafe { read_target_list(query_ref.targetList, &expr_scope) }?;
    let group_refs = unsafe { read_sort_group_refs(query_ref.groupClause) }?;
    let distinct = unsafe { read_distinct_spec(query_ref) }?;
    let grouping_sets = unsafe { read_grouping_sets(query_ref.groupingSets) }?;
    let windows = unsafe { read_window_specs(query_ref.windowClause) }?;
    let set_operation = if query_ref.setOperations.is_null() {
        None
    } else {
        Some(unsafe { read_set_operation_tree(query_ref.setOperations.cast()) }?)
    };
    let sort = unsafe { read_sort_clause(query_ref.sortClause) }?;
    let limit_count = if query_ref.limitCount.is_null() {
        None
    } else {
        Some(unsafe { read_expr(query_ref.limitCount.cast(), &expr_scope) }?)
    };
    let limit_offset = if query_ref.limitOffset.is_null() {
        None
    } else {
        Some(unsafe { read_expr(query_ref.limitOffset.cast(), &expr_scope) }?)
    };

    Ok(TypedQuery {
        command: QueryCommand::Select,
        relations: rtable.relations,
        values: rtable.values,
        ctes: visible_ctes.defs,
        cte_refs: rtable.ctes,
        subqueries: rtable.subqueries,
        from,
        selection,
        having,
        targets,
        group_refs,
        grouping_sets,
        windows,
        set_operation,
        sort,
        limit_count,
        limit_offset,
        has_aggregates: query_ref.hasAggs,
        has_windows: query_ref.hasWindowFuncs,
        has_sublinks: query_ref.hasSubLinks,
        distinct,
        has_group_by: !query_ref.groupClause.is_null(),
        has_having: !query_ref.havingQual.is_null(),
        has_grouping_sets: !query_ref.groupingSets.is_null(),
        has_set_operations: !query_ref.setOperations.is_null(),
        has_row_marks: !query_ref.rowMarks.is_null(),
    })
}

fn validate_limit_option(limit_option: pg_sys::LimitOption::Type) -> Result<(), PgFrontendError> {
    match limit_option {
        pg_sys::LimitOption::LIMIT_OPTION_COUNT => Ok(()),
        pg_sys::LimitOption::LIMIT_OPTION_WITH_TIES => Err(PgFrontendError::unsupported(
            "FETCH WITH TIES is not supported by pg_frontend v1",
        )),
        other => Err(PgFrontendError::unsupported(format!(
            "limit option {other} is not supported by pg_frontend v1"
        ))),
    }
}

unsafe fn read_set_operation_tree(
    node: *mut pg_sys::Node,
) -> Result<SetOperationTree, PgFrontendError> {
    if node.is_null() {
        return Err(PgFrontendError::unsupported("null set operation node"));
    }
    match unsafe { (*node).type_ } {
        pg_sys::NodeTag::T_RangeTblRef => {
            let range_ref = unsafe { &*node.cast::<pg_sys::RangeTblRef>() };
            Ok(SetOperationTree::Range {
                rtindex: range_ref.rtindex as usize,
            })
        }
        pg_sys::NodeTag::T_SetOperationStmt => {
            let stmt = unsafe { &*node.cast::<pg_sys::SetOperationStmt>() };
            let op = match stmt.op {
                pg_sys::SetOperation::SETOP_UNION => SetOperator::Union,
                _ => {
                    return Err(PgFrontendError::unsupported(
                        "only UNION set operations are supported by pg_frontend v1",
                    ))
                }
            };
            Ok(SetOperationTree::Operation {
                op,
                all: stmt.all,
                left: Box::new(unsafe { read_set_operation_tree(stmt.larg) }?),
                right: Box::new(unsafe { read_set_operation_tree(stmt.rarg) }?),
            })
        }
        tag => Err(PgFrontendError::unsupported(format!(
            "set operation node {:?} is not supported by pg_frontend v1",
            tag
        ))),
    }
}

mod clauses;
mod consts;
mod expr;
mod functions;
mod rtable;
mod scope;
mod window;

use clauses::*;
use consts::*;
use expr::*;
use functions::*;
use rtable::*;
use scope::*;
use window::*;

#[cfg(test)]
mod tests;
