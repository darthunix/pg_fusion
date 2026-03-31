use arrow_schema::DataType;
use datafusion_common::ScalarValue;

use crate::quote::encode_hex;

pub(crate) fn render_literal(literal: &ScalarValue) -> Option<String> {
    match literal {
        ScalarValue::Null => Some("NULL".into()),
        ScalarValue::Boolean(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| Some(if value { "TRUE" } else { "FALSE" }.into()),
        ),
        ScalarValue::Float16(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| {
                let value = f32::from(value);
                value.is_finite().then(|| value.to_string())
            },
        ),
        ScalarValue::Float32(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| value.is_finite().then(|| value.to_string()),
        ),
        ScalarValue::Float64(value) => value.map_or_else(
            || Some("NULL".into()),
            |value| value.is_finite().then(|| value.to_string()),
        ),
        ScalarValue::Decimal128(value, precision, scale) => {
            render_decimal_literal(value.map(|value| value.to_string()), *precision, *scale)
        }
        ScalarValue::Decimal256(value, precision, scale) => {
            render_decimal_literal(value.map(|value| value.to_string()), *precision, *scale)
        }
        ScalarValue::Int8(value) => render_option_number(*value),
        ScalarValue::Int16(value) => render_option_number(*value),
        ScalarValue::Int32(value) => render_option_number(*value),
        ScalarValue::Int64(value) => render_option_number(*value),
        ScalarValue::UInt8(value) => render_option_number(*value),
        ScalarValue::UInt16(value) => render_option_number(*value),
        ScalarValue::UInt32(value) => render_option_number(*value),
        ScalarValue::UInt64(value) => render_option_number(*value),
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            value
                .as_ref()
                .map(|value| render_string_literal(value))
                .or_else(|| Some("NULL".into()))
        }
        ScalarValue::Binary(value)
        | ScalarValue::BinaryView(value)
        | ScalarValue::LargeBinary(value) => value
            .as_ref()
            .map(|value| render_bytea_literal(value))
            .or_else(|| Some("NULL".into())),
        ScalarValue::FixedSizeBinary(_, value) => value
            .as_ref()
            .map(|value| render_bytea_literal(value))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Date32(value) => value
            .map(render_date32_literal)
            .or_else(|| Some("NULL".into())),
        ScalarValue::Date64(value) => value
            .map(render_date64_literal)
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time32Second(value) => value
            .map(|value| render_time_literal(i64::from(value), "second"))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time32Millisecond(value) => value
            .map(|value| render_time_literal(i64::from(value), "millisecond"))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time64Microsecond(value) => value
            .map(|value| render_time_literal(value, "microsecond"))
            .or_else(|| Some("NULL".into())),
        ScalarValue::Time64Nanosecond(_) => None,
        ScalarValue::TimestampSecond(value, tz) => {
            render_timestamp_literal(*value, tz.as_deref(), "second")
        }
        ScalarValue::TimestampMillisecond(value, tz) => {
            render_timestamp_literal(*value, tz.as_deref(), "millisecond")
        }
        ScalarValue::TimestampMicrosecond(value, tz) => {
            render_timestamp_literal(*value, tz.as_deref(), "microsecond")
        }
        ScalarValue::TimestampNanosecond(_, _) => None,
        ScalarValue::Dictionary(_, value) => render_literal(value),
        ScalarValue::IntervalYearMonth(_)
        | ScalarValue::IntervalDayTime(_)
        | ScalarValue::IntervalMonthDayNano(_)
        | ScalarValue::DurationSecond(_)
        | ScalarValue::DurationMillisecond(_)
        | ScalarValue::DurationMicrosecond(_)
        | ScalarValue::DurationNanosecond(_)
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Union(_, _, _) => None,
    }
}

pub(crate) fn render_cast_target(data_type: &DataType) -> Option<String> {
    Some(match data_type {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Int8 | DataType::Int16 => "SMALLINT".into(),
        DataType::Int32 => "INTEGER".into(),
        DataType::Int64 => "BIGINT".into(),
        DataType::Float16 | DataType::Float32 => "REAL".into(),
        DataType::Float64 => "DOUBLE PRECISION".into(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("NUMERIC({precision}, {scale})")
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "TEXT".into(),
        DataType::Binary
        | DataType::BinaryView
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => "BYTEA".into(),
        _ => return None,
    })
}

pub(crate) fn render_string_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

fn render_decimal_literal(value: Option<String>, precision: u8, scale: i8) -> Option<String> {
    value
        .map(|value| {
            let decimal = apply_decimal_scale(value, scale);
            format!(
                "CAST({} AS NUMERIC({precision}, {scale}))",
                render_string_literal(&decimal)
            )
        })
        .or_else(|| Some("NULL".into()))
}

fn apply_decimal_scale(value: String, scale: i8) -> String {
    let (negative, digits) = match value.strip_prefix('-') {
        Some(stripped) => (true, stripped.to_string()),
        None => (false, value),
    };
    let scaled = if scale >= 0 {
        let scale = scale as usize;
        if scale == 0 {
            digits
        } else if digits.len() <= scale {
            let zeros = "0".repeat(scale - digits.len());
            format!("0.{zeros}{digits}")
        } else {
            let split = digits.len() - scale;
            format!("{}.{}", &digits[..split], &digits[split..])
        }
    } else {
        format!("{digits}{}", "0".repeat((-scale) as usize))
    };

    if negative {
        format!("-{scaled}")
    } else {
        scaled
    }
}

fn render_option_number<T: ToString>(value: Option<T>) -> Option<String> {
    value
        .map(|value| value.to_string())
        .or_else(|| Some("NULL".into()))
}

fn render_bytea_literal(bytes: &[u8]) -> String {
    format!("'\\\\x{}'::bytea", encode_hex(bytes))
}

fn render_date32_literal(days: i32) -> String {
    format!("(DATE '1970-01-01' + ({days}))")
}

fn render_date64_literal(milliseconds: i64) -> String {
    format!(
        "((TIMESTAMP '1970-01-01 00:00:00' + ({milliseconds}) * INTERVAL '1 millisecond')::date)"
    )
}

fn render_time_literal(value: i64, unit: &str) -> String {
    format!("((TIME '00:00:00' + ({value}) * INTERVAL '1 {unit}')::time)")
}

fn render_timestamp_literal(
    value: Option<i64>,
    timezone: Option<&str>,
    unit: &str,
) -> Option<String> {
    match (value, timezone) {
        (None, _) => Some("NULL".into()),
        (Some(_), Some(_)) => None,
        (Some(value), None) => Some(format!(
            "(TIMESTAMP '1970-01-01 00:00:00' + ({value}) * INTERVAL '1 {unit}')"
        )),
    }
}
