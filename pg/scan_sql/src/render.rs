use arrow_schema::Schema;
use datafusion_common::Column;
use datafusion_expr::expr::{BinaryExpr, Case, Cast, InList, Like, ScalarFunction};
use datafusion_expr::{Expr, Operator};

use crate::error::CompileError;
use crate::identifier::validate_identifier;
use crate::literal::{render_cast_target, render_literal, render_string_literal};
use crate::quote::quote_identifier;
use crate::types::{PgRelation, RenderedExpr};

pub(crate) fn render_expr(
    expr: &Expr,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    match expr {
        Expr::Alias(alias) => render_expr(&alias.expr, schema, relation, identifier_max_bytes),
        Expr::Column(column) => {
            render_column(column, schema, relation, identifier_max_bytes).map(Some)
        }
        Expr::Literal(literal) => Ok(render_literal(literal).map(RenderedExpr::new)),
        Expr::BinaryExpr(binary) => {
            render_binary_expr(binary, schema, relation, identifier_max_bytes)
        }
        Expr::Like(like) => render_like_expr(like, false, schema, relation, identifier_max_bytes),
        Expr::SimilarTo(like) => {
            render_like_expr(like, true, schema, relation, identifier_max_bytes)
        }
        Expr::Not(inner) => {
            render_unary_predicate("NOT", inner, schema, relation, true, identifier_max_bytes)
        }
        Expr::IsNull(inner) => {
            render_postfix_predicate(inner, "IS NULL", schema, relation, identifier_max_bytes)
        }
        Expr::IsNotNull(inner) => {
            render_postfix_predicate(inner, "IS NOT NULL", schema, relation, identifier_max_bytes)
        }
        Expr::IsTrue(inner) => {
            render_postfix_predicate(inner, "IS TRUE", schema, relation, identifier_max_bytes)
        }
        Expr::IsFalse(inner) => {
            render_postfix_predicate(inner, "IS FALSE", schema, relation, identifier_max_bytes)
        }
        Expr::IsUnknown(inner) => {
            render_postfix_predicate(inner, "IS UNKNOWN", schema, relation, identifier_max_bytes)
        }
        Expr::IsNotTrue(inner) => {
            render_postfix_predicate(inner, "IS NOT TRUE", schema, relation, identifier_max_bytes)
        }
        Expr::IsNotFalse(inner) => render_postfix_predicate(
            inner,
            "IS NOT FALSE",
            schema,
            relation,
            identifier_max_bytes,
        ),
        Expr::IsNotUnknown(inner) => render_postfix_predicate(
            inner,
            "IS NOT UNKNOWN",
            schema,
            relation,
            identifier_max_bytes,
        ),
        Expr::Negative(inner) => {
            render_unary_predicate("-", inner, schema, relation, false, identifier_max_bytes)
        }
        Expr::Between(between) => {
            render_between_expr(between, schema, relation, identifier_max_bytes)
        }
        Expr::Case(case) => render_case_expr(case, schema, relation, identifier_max_bytes),
        Expr::Cast(cast) => render_cast_expr(cast, schema, relation, identifier_max_bytes),
        Expr::TryCast(_) => Ok(None),
        Expr::ScalarFunction(function) => {
            render_scalar_function(function, schema, relation, identifier_max_bytes)
        }
        Expr::InList(in_list) => render_in_list(in_list, schema, relation, identifier_max_bytes),
        Expr::ScalarVariable(_, _)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::Exists { .. }
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::Wildcard { .. }
        | Expr::GroupingSet(_)
        | Expr::Placeholder(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Unnest(_) => Ok(None),
    }
}

pub(crate) fn render_column(
    column: &Column,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<RenderedExpr, CompileError> {
    if let Some(column_relation) = &column.relation {
        if !relation.matches_reference(column_relation, identifier_max_bytes)? {
            return Err(CompileError::UnexpectedRelation {
                column: column.flat_name(),
                relation: column_relation.to_string(),
                expected: relation.display_name(),
            });
        }
    }

    validate_identifier(column.name.as_str(), identifier_max_bytes, "column")?;
    let index = schema
        .fields()
        .iter()
        .position(|field| field.name() == column.name.as_str())
        .ok_or_else(|| CompileError::UnknownColumn {
            column: column.flat_name(),
        })?;

    Ok(RenderedExpr::from_column(
        quote_identifier(schema.field(index).name()),
        index,
    ))
}

fn render_binary_expr(
    expr: &BinaryExpr,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let Some(left) = render_expr(&expr.left, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let Some(right) = render_expr(&expr.right, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let Some(operator_sql) = render_operator(expr.op) else {
        return Ok(None);
    };

    let sql = format!("({} {operator_sql} {})", left.sql, right.sql);
    Ok(Some(RenderedExpr::merge(sql, [left, right])))
}

fn render_like_expr(
    expr: &Like,
    similar_to: bool,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    if similar_to && expr.case_insensitive {
        return Ok(None);
    }

    let Some(value) = render_expr(&expr.expr, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let Some(pattern) = render_expr(&expr.pattern, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };

    let operator = if similar_to {
        if expr.negated {
            "NOT SIMILAR TO"
        } else {
            "SIMILAR TO"
        }
    } else if expr.case_insensitive {
        if expr.negated {
            "NOT ILIKE"
        } else {
            "ILIKE"
        }
    } else if expr.negated {
        "NOT LIKE"
    } else {
        "LIKE"
    };

    let mut sql = format!("({} {operator} {})", value.sql, pattern.sql);
    if let Some(escape_char) = expr.escape_char {
        sql.insert_str(
            sql.len() - 1,
            &format!(
                " ESCAPE {}",
                render_string_literal(&escape_char.to_string())
            ),
        );
    }
    Ok(Some(RenderedExpr::merge(sql, [value, pattern])))
}

fn render_unary_predicate(
    operator: &str,
    expr: &Expr,
    schema: &Schema,
    relation: &PgRelation,
    needs_space: bool,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let Some(inner) = render_expr(expr, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let sql = if operator == "-" {
        format!("({operator}({}))", inner.sql)
    } else if needs_space {
        format!("({operator} {})", inner.sql)
    } else {
        format!("({operator}{})", inner.sql)
    };
    Ok(Some(RenderedExpr::merge(sql, [inner])))
}

fn render_postfix_predicate(
    expr: &Expr,
    suffix: &str,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let Some(inner) = render_expr(expr, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let sql = format!("({} {suffix})", inner.sql);
    Ok(Some(RenderedExpr::merge(sql, [inner])))
}

fn render_between_expr(
    expr: &datafusion_expr::Between,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let Some(value) = render_expr(&expr.expr, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let Some(low) = render_expr(&expr.low, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let Some(high) = render_expr(&expr.high, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let not_sql = if expr.negated { " NOT" } else { "" };
    let sql = format!(
        "({}{} BETWEEN {} AND {})",
        value.sql, not_sql, low.sql, high.sql
    );
    Ok(Some(RenderedExpr::merge(sql, [value, low, high])))
}

fn render_case_expr(
    expr: &Case,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let mut parts = Vec::new();
    let mut sql = String::from("(CASE");

    if let Some(base) = &expr.expr {
        let Some(base) = render_expr(base, schema, relation, identifier_max_bytes)? else {
            return Ok(None);
        };
        sql.push(' ');
        sql.push_str(&base.sql);
        parts.push(base);
    }

    for (when, then) in &expr.when_then_expr {
        let Some(when_sql) = render_expr(when, schema, relation, identifier_max_bytes)? else {
            return Ok(None);
        };
        let Some(then_sql) = render_expr(then, schema, relation, identifier_max_bytes)? else {
            return Ok(None);
        };
        sql.push_str(" WHEN ");
        sql.push_str(&when_sql.sql);
        sql.push_str(" THEN ");
        sql.push_str(&then_sql.sql);
        parts.push(when_sql);
        parts.push(then_sql);
    }

    if let Some(otherwise) = &expr.else_expr {
        let Some(else_sql) = render_expr(otherwise, schema, relation, identifier_max_bytes)? else {
            return Ok(None);
        };
        sql.push_str(" ELSE ");
        sql.push_str(&else_sql.sql);
        parts.push(else_sql);
    }

    sql.push_str(" END)");
    Ok(Some(RenderedExpr::merge(sql, parts)))
}

fn render_cast_expr(
    expr: &Cast,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let Some(inner) = render_expr(&expr.expr, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };
    let Some(target) = render_cast_target(&expr.data_type) else {
        return Ok(None);
    };
    let sql = format!("CAST({} AS {target})", inner.sql);
    Ok(Some(RenderedExpr::merge(sql, [inner])))
}

fn render_scalar_function(
    function: &ScalarFunction,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    let rendered_args = function
        .args
        .iter()
        .map(|expr| render_expr(expr, schema, relation, identifier_max_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    if rendered_args.iter().any(Option::is_none) {
        return Ok(None);
    }
    let rendered_args = rendered_args.into_iter().flatten().collect::<Vec<_>>();
    let args_sql = rendered_args
        .iter()
        .map(|expr| expr.sql.as_str())
        .collect::<Vec<_>>();
    let name = function.name().to_ascii_lowercase();

    let sql = match (name.as_str(), args_sql.as_slice()) {
        ("lower", [value]) => format!("lower({value})"),
        ("upper", [value]) => format!("upper({value})"),
        ("trim", [value]) => format!("trim({value})"),
        ("ltrim", [value]) => format!("ltrim({value})"),
        ("rtrim", [value]) => format!("rtrim({value})"),
        ("btrim", [value]) => format!("btrim({value})"),
        ("length", [value]) | ("char_length", [value]) => format!("char_length({value})"),
        ("strpos", [haystack, needle]) => format!("strpos({haystack}, {needle})"),
        ("contains", [haystack, needle]) => format!("(strpos({haystack}, {needle}) > 0)"),
        ("concat", args) if !args.is_empty() => format!("concat({})", args.join(", ")),
        _ => return Ok(None),
    };

    Ok(Some(RenderedExpr::merge(sql, rendered_args)))
}

fn render_in_list(
    expr: &InList,
    schema: &Schema,
    relation: &PgRelation,
    identifier_max_bytes: usize,
) -> Result<Option<RenderedExpr>, CompileError> {
    if expr.list.is_empty() {
        let sql = if expr.negated { "(TRUE)" } else { "(FALSE)" };
        return Ok(Some(RenderedExpr::new(sql.into())));
    }

    let Some(value) = render_expr(&expr.expr, schema, relation, identifier_max_bytes)? else {
        return Ok(None);
    };

    let rendered_items = expr
        .list
        .iter()
        .map(|item| render_expr(item, schema, relation, identifier_max_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    if rendered_items.iter().any(Option::is_none) {
        return Ok(None);
    }
    let rendered_items = rendered_items.into_iter().flatten().collect::<Vec<_>>();
    let items_sql = rendered_items
        .iter()
        .map(|expr| expr.sql.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let not_sql = if expr.negated { " NOT" } else { "" };
    let sql = format!("({}{} IN ({items_sql}))", value.sql, not_sql);

    let mut parts = Vec::with_capacity(rendered_items.len() + 1);
    parts.push(value);
    parts.extend(rendered_items);
    Ok(Some(RenderedExpr::merge(sql, parts)))
}

fn render_operator(operator: Operator) -> Option<&'static str> {
    Some(match operator {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        Operator::Plus => "+",
        Operator::Minus => "-",
        Operator::Multiply => "*",
        Operator::Divide => "/",
        Operator::Modulo => "%",
        Operator::And => "AND",
        Operator::Or => "OR",
        Operator::IsDistinctFrom => "IS DISTINCT FROM",
        Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        Operator::RegexMatch => return None,
        Operator::RegexIMatch => return None,
        Operator::RegexNotMatch => return None,
        Operator::RegexNotIMatch => return None,
        Operator::LikeMatch => "LIKE",
        Operator::ILikeMatch => "ILIKE",
        Operator::NotLikeMatch => "NOT LIKE",
        Operator::NotILikeMatch => "NOT ILIKE",
        Operator::BitwiseAnd => "&",
        Operator::BitwiseOr => "|",
        Operator::BitwiseXor => "#",
        Operator::BitwiseShiftRight => ">>",
        Operator::BitwiseShiftLeft => "<<",
        Operator::StringConcat => "||",
        Operator::AtArrow => "@>",
        Operator::ArrowAt => "<@",
    })
}
