use pg_type::{numeric_shape_from_typmod, NUMERIC_FALLBACK_PRECISION, NUMERIC_FALLBACK_SCALE};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NumericDecimalArithmeticPolicy {
    pub left_pg_type: pg_type::PgTypeRef,
    pub right_pg_type: pg_type::PgTypeRef,
    pub left_type: DataType,
    pub right_type: DataType,
    pub result_type: DataType,
}

pub(super) fn numeric_decimal_arithmetic_policy(
    op: QueryOperator,
    result_pg_type: pg_type::PgTypeRef,
    left_source: &QueryExpr,
    right_source: &QueryExpr,
) -> Result<Option<NumericDecimalArithmeticPolicy>, PgFrontendError> {
    if !matches!(
        op,
        QueryOperator::Plus
            | QueryOperator::Minus
            | QueryOperator::Multiply
            | QueryOperator::Divide
            | QueryOperator::Modulo
    ) || result_pg_type.oid != u32::from(pgrx::pg_sys::NUMERICOID)
    {
        return Ok(None);
    }

    let Some(left_pg_type) = expr_pg_type(left_source) else {
        return Ok(None);
    };
    let Some(right_pg_type) = expr_pg_type(right_source) else {
        return Ok(None);
    };
    if !(is_decimal_arithmetic_operand_oid(left_pg_type.oid)
        && is_decimal_arithmetic_operand_oid(right_pg_type.oid))
    {
        return Ok(None);
    }
    if left_pg_type.oid != u32::from(pgrx::pg_sys::NUMERICOID)
        && right_pg_type.oid != u32::from(pgrx::pg_sys::NUMERICOID)
    {
        return Ok(None);
    }

    let Some(left_scale) = numeric_arithmetic_expr_scale(left_source)? else {
        return Ok(None);
    };
    let Some(right_scale) = numeric_arithmetic_expr_scale(right_source)? else {
        return Ok(None);
    };

    let result_scale =
        numeric_arithmetic_result_scale(op, result_pg_type, left_scale, right_scale)?;
    let (left_scale, right_scale) = match op {
        QueryOperator::Plus | QueryOperator::Minus | QueryOperator::Modulo => {
            (result_scale, result_scale)
        }
        QueryOperator::Multiply => (left_scale, right_scale),
        QueryOperator::Divide => {
            let left_work_scale = left_scale.max(result_scale.saturating_sub(4));
            (left_work_scale, right_scale)
        }
        _ => {
            return Err(PgFrontendError::unsupported(
                "unsupported numeric arithmetic operator in decimal policy",
            ))
        }
    };

    Ok(Some(NumericDecimalArithmeticPolicy {
        left_pg_type,
        right_pg_type,
        left_type: decimal128_with_scale(left_scale),
        right_type: decimal128_with_scale(right_scale),
        result_type: decimal128_with_scale(result_scale),
    }))
}

pub(super) fn numeric_arithmetic_expr_scale(
    expr: &QueryExpr,
) -> Result<Option<i8>, PgFrontendError> {
    match expr {
        QueryExpr::Const(constant)
            if constant.pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID)
                && constant.pg_type.typmod < 0 =>
        {
            return Ok(numeric_literal_display_scale(constant));
        }
        QueryExpr::RelabelType(inner) => return numeric_arithmetic_expr_scale(inner),
        QueryExpr::Cast { arg, pg_type } => {
            if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) && pg_type.typmod >= 0 {
                return numeric_type_scale(*pg_type);
            }
            if pg_type.oid == u32::from(pgrx::pg_sys::INT2OID)
                || pg_type.oid == u32::from(pgrx::pg_sys::INT4OID)
                || pg_type.oid == u32::from(pgrx::pg_sys::INT8OID)
            {
                return Ok(Some(0));
            }
            return numeric_arithmetic_expr_scale(arg);
        }
        QueryExpr::UnaryOp { arg, .. } => return numeric_arithmetic_expr_scale(arg),
        QueryExpr::BinaryOp {
            op,
            left,
            right,
            pg_type,
        } if pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID) => {
            let Some(left_scale) = numeric_arithmetic_expr_scale(left)? else {
                return numeric_type_scale(*pg_type);
            };
            let Some(right_scale) = numeric_arithmetic_expr_scale(right)? else {
                return numeric_type_scale(*pg_type);
            };
            return numeric_arithmetic_result_scale(*op, *pg_type, left_scale, right_scale)
                .map(Some);
        }
        _ => {}
    }

    let Some(pg_type) = expr_pg_type(expr) else {
        return Ok(None);
    };
    numeric_type_scale(pg_type)
}

fn numeric_arithmetic_result_scale(
    op: QueryOperator,
    result_pg_type: pg_type::PgTypeRef,
    left_scale: i8,
    right_scale: i8,
) -> Result<i8, PgFrontendError> {
    let scale = if result_pg_type.oid == u32::from(pgrx::pg_sys::NUMERICOID)
        && result_pg_type.typmod >= 0
    {
        numeric_type_scale(result_pg_type)?.unwrap_or(NUMERIC_FALLBACK_SCALE)
    } else {
        match op {
            QueryOperator::Plus | QueryOperator::Minus | QueryOperator::Modulo => {
                left_scale.max(right_scale)
            }
            QueryOperator::Multiply => left_scale.checked_add(right_scale).ok_or_else(|| {
                PgFrontendError::unsupported(
                    "numeric arithmetic result scale overflowed Decimal128 scale",
                )
            })?,
            QueryOperator::Divide => NUMERIC_FALLBACK_SCALE,
            _ => {
                return Err(PgFrontendError::unsupported(
                    "unsupported numeric arithmetic operator in scale policy",
                ))
            }
        }
    };
    validate_decimal128_scale(scale)?;
    Ok(scale)
}

fn numeric_type_scale(pg_type: pg_type::PgTypeRef) -> Result<Option<i8>, PgFrontendError> {
    match pg_type.oid {
        oid if oid == u32::from(pgrx::pg_sys::NUMERICOID) => {
            let (_, scale) = numeric_shape_from_typmod(pg_type.typmod).ok_or_else(|| {
                PgFrontendError::unsupported(format!(
                    "unsupported numeric typmod {} in arithmetic expression",
                    pg_type.typmod
                ))
            })?;
            validate_decimal128_scale(scale)?;
            Ok(Some(scale))
        }
        oid if oid == u32::from(pgrx::pg_sys::INT2OID)
            || oid == u32::from(pgrx::pg_sys::INT4OID)
            || oid == u32::from(pgrx::pg_sys::INT8OID) =>
        {
            Ok(Some(0))
        }
        _ => Ok(None),
    }
}

pub(super) fn numeric_literal_display_scale(constant: &Const) -> Option<i8> {
    match constant.value.as_ref()? {
        pg_type::PgConstValue::Int16(_)
        | pg_type::PgConstValue::Int32(_)
        | pg_type::PgConstValue::Int64(_) => Some(0),
        pg_type::PgConstValue::Numeric(value) => Some(value.display_scale),
        _ => None,
    }
}

fn is_decimal_arithmetic_operand_oid(oid: u32) -> bool {
    oid == u32::from(pgrx::pg_sys::NUMERICOID)
        || oid == u32::from(pgrx::pg_sys::INT2OID)
        || oid == u32::from(pgrx::pg_sys::INT4OID)
        || oid == u32::from(pgrx::pg_sys::INT8OID)
}

fn decimal128_with_scale(scale: i8) -> DataType {
    DataType::Decimal128(NUMERIC_FALLBACK_PRECISION, scale)
}

fn validate_decimal128_scale(scale: i8) -> Result<(), PgFrontendError> {
    if (0..=38).contains(&scale) {
        Ok(())
    } else {
        Err(PgFrontendError::unsupported(format!(
            "numeric arithmetic result scale {scale} exceeds Decimal128 max scale 38"
        )))
    }
}
