//! Shared PostgreSQL type policy for Arrow/DataFusion transport.
//!
//! This crate owns pg_fusion's supported PostgreSQL type surface and the
//! mapping from PostgreSQL type identity to Arrow/page transport types. It is
//! intentionally PostgreSQL-runtime free: pgrx-specific Datum decoding and
//! tuple-slot projection stay in the PostgreSQL-bound crates.

use arrow_layout::{ColumnSpec, TypeTag};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
#[cfg(feature = "datafusion")]
use datafusion_common::{metadata::FieldMetadata, ScalarValue};
use serde::{Deserialize, Serialize};
#[cfg(feature = "datafusion")]
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

pub mod numeric;
pub use numeric::{numeric_to_decimal128, NumericDecodeError};

pub mod oid {
    pub const BOOLOID: u32 = 16;
    pub const BYTEAOID: u32 = 17;
    pub const NAMEOID: u32 = 19;
    pub const INT8OID: u32 = 20;
    pub const INT2OID: u32 = 21;
    pub const INT4OID: u32 = 23;
    pub const TEXTOID: u32 = 25;
    pub const FLOAT4OID: u32 = 700;
    pub const FLOAT8OID: u32 = 701;
    pub const BPCHAROID: u32 = 1042;
    pub const VARCHAROID: u32 = 1043;
    pub const DATEOID: u32 = 1082;
    pub const TIMEOID: u32 = 1083;
    pub const TIMESTAMPOID: u32 = 1114;
    pub const TIMESTAMPTZOID: u32 = 1184;
    pub const INTERVALOID: u32 = 1186;
    pub const TIMETZOID: u32 = 1266;
    pub const NUMERICOID: u32 = 1700;
    pub const REGCLASSOID: u32 = 2205;
    pub const UUIDOID: u32 = 2950;
    pub const JSONBOID: u32 = 3802;

    pub const DEFAULT_COLLATION_OID: u32 = 100;
    pub const C_COLLATION_OID: u32 = 950;
    pub const PG_CATALOG_NAMESPACE: u32 = 11;
}

pub const VARHDRSZ: i32 = 4;
pub const NUMERIC_FALLBACK_PRECISION: u8 = 38;
pub const NUMERIC_FALLBACK_SCALE: i8 = 16;

pub const PG_TYPE_OID_METADATA_KEY: &str = "pg_fusion.pg_type_oid";
pub const PG_TYPE_TYPMOD_METADATA_KEY: &str = "pg_fusion.pg_type_typmod";
pub const PG_TYPE_COLLATION_METADATA_KEY: &str = "pg_fusion.pg_type_collation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PgTypeRef {
    pub oid: u32,
    pub typmod: i32,
    pub collation: u32,
}

impl PgTypeRef {
    pub const fn new(oid: u32, typmod: i32, collation: u32) -> Self {
        Self {
            oid,
            typmod,
            collation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgTypeMetadata {
    pub oid: u32,
    pub typmod: i32,
    pub collation: u32,
}

impl From<PgTypeRef> for PgTypeMetadata {
    fn from(value: PgTypeRef) -> Self {
        Self {
            oid: value.oid,
            typmod: value.typmod,
            collation: value.collation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PgConstValue {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Numeric(String),
    Text(String),
    Binary(Vec<u8>),
    Date32(i32),
    Time64Microsecond(i64),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PgTypeError {
    #[error("PostgreSQL type oid {oid} is not supported by pg_fusion")]
    UnsupportedType { oid: u32 },
    #[error("non-null PostgreSQL constant type {type_name} is not supported by pg_fusion")]
    UnsupportedConstType { type_name: String },
    #[error("non-default collation oid {collation} is not supported by pg_fusion")]
    UnsupportedCollation { collation: u32 },
    #[error("numeric typmod {typmod} cannot be represented as Arrow Decimal128")]
    UnsupportedNumericTypmod { typmod: i32 },
    #[error("Arrow type {data_type} at column {index} is not supported by pg_fusion transport")]
    UnsupportedArrowType { index: usize, data_type: String },
    #[error("pg_fusion result transport requires at least one output column")]
    EmptyResultSchema,
    #[error("PostgreSQL type oid {oid} cannot be represented as a typed NULL")]
    UnsupportedTypedNull { oid: u32 },
    #[error("PostgreSQL constant value cannot be represented as Arrow scalar for type oid {oid}")]
    UnsupportedConstValue { oid: u32 },
}

pub const POSTGRES_TO_UNIX_EPOCH_DAYS: i32 = 10_957;
pub const PG_DATE_NEG_INFINITY: i32 = i32::MIN;
pub const PG_DATE_POS_INFINITY: i32 = i32::MAX;

pub fn date32_from_pg_date(value: i32) -> Option<i32> {
    if is_pg_date_infinity(value) {
        return None;
    }
    value.checked_add(POSTGRES_TO_UNIX_EPOCH_DAYS)
}

pub fn pg_date_from_date32(value: i32) -> Option<i32> {
    let value = value.checked_sub(POSTGRES_TO_UNIX_EPOCH_DAYS)?;
    if is_pg_date_infinity(value) {
        return None;
    }
    Some(value)
}

pub const fn is_pg_date_infinity(value: i32) -> bool {
    value == PG_DATE_NEG_INFINITY || value == PG_DATE_POS_INFINITY
}

pub fn validate_supported_value_type(pg_type: PgTypeRef) -> Result<(), PgTypeError> {
    validate_supported_collation(pg_type)?;
    if is_supported_value_type(pg_type.oid) {
        Ok(())
    } else {
        Err(PgTypeError::UnsupportedType { oid: pg_type.oid })
    }
}

pub fn validate_supported_non_null_const_type(pg_type: PgTypeRef) -> Result<(), PgTypeError> {
    validate_supported_collation(pg_type)?;
    if is_supported_non_null_const_type(pg_type.oid) {
        Ok(())
    } else {
        Err(PgTypeError::UnsupportedConstType {
            type_name: type_name(pg_type.oid),
        })
    }
}

pub fn validate_supported_collation(pg_type: PgTypeRef) -> Result<(), PgTypeError> {
    if pg_type.collation != 0
        && pg_type.collation != oid::DEFAULT_COLLATION_OID
        && !(pg_type.oid == oid::NAMEOID && pg_type.collation == oid::C_COLLATION_OID)
    {
        return Err(PgTypeError::UnsupportedCollation {
            collation: pg_type.collation,
        });
    }
    Ok(())
}

pub fn is_supported_scalar_type(oid: u32) -> bool {
    is_supported_value_type(oid)
}

pub fn is_supported_value_type(oid: u32) -> bool {
    matches!(
        oid,
        oid::BOOLOID
            | oid::INT2OID
            | oid::INT4OID
            | oid::INT8OID
            | oid::FLOAT4OID
            | oid::FLOAT8OID
            | oid::TEXTOID
            | oid::VARCHAROID
            | oid::BPCHAROID
            | oid::NAMEOID
            | oid::BYTEAOID
            | oid::UUIDOID
            | oid::DATEOID
            | oid::TIMEOID
            | oid::TIMESTAMPOID
            | oid::TIMESTAMPTZOID
            | oid::INTERVALOID
            | oid::NUMERICOID
    )
}

pub fn is_supported_non_null_const_type(oid: u32) -> bool {
    matches!(
        oid,
        oid::BOOLOID
            | oid::INT2OID
            | oid::INT4OID
            | oid::INT8OID
            | oid::FLOAT4OID
            | oid::FLOAT8OID
            | oid::NUMERICOID
            | oid::TEXTOID
            | oid::VARCHAROID
            | oid::BPCHAROID
            | oid::NAMEOID
            | oid::BYTEAOID
            | oid::DATEOID
            | oid::TIMEOID
    )
}

pub fn is_text_like_type(oid: u32) -> bool {
    matches!(
        oid,
        oid::TEXTOID | oid::VARCHAROID | oid::BPCHAROID | oid::NAMEOID
    )
}

pub fn is_temporal_type(oid: u32) -> bool {
    matches!(
        oid,
        oid::DATEOID | oid::TIMEOID | oid::TIMESTAMPOID | oid::TIMESTAMPTZOID
    )
}

pub fn pg_oid_needs_detoast(oid: u32) -> bool {
    matches!(
        oid,
        oid::TEXTOID | oid::VARCHAROID | oid::BPCHAROID | oid::BYTEAOID
    )
}

pub fn type_name(oid: u32) -> String {
    match oid {
        oid::BOOLOID => "boolean".into(),
        oid::BYTEAOID => "bytea".into(),
        oid::NAMEOID => "name".into(),
        oid::INT2OID => "int2".into(),
        oid::INT4OID => "int4".into(),
        oid::INT8OID => "int8".into(),
        oid::FLOAT4OID => "float4".into(),
        oid::FLOAT8OID => "float8".into(),
        oid::TEXTOID => "text".into(),
        oid::VARCHAROID => "varchar".into(),
        oid::BPCHAROID => "bpchar".into(),
        oid::UUIDOID => "uuid".into(),
        oid::DATEOID => "date".into(),
        oid::TIMEOID => "time".into(),
        oid::TIMESTAMPOID => "timestamp".into(),
        oid::TIMESTAMPTZOID => "timestamptz".into(),
        oid::INTERVALOID => "interval".into(),
        oid::NUMERICOID => "numeric".into(),
        _ => format!("oid {oid}"),
    }
}

pub fn numeric_shape_from_typmod(atttypmod: i32) -> Option<(u8, i8)> {
    if atttypmod < 0 {
        return Some((NUMERIC_FALLBACK_PRECISION, NUMERIC_FALLBACK_SCALE));
    }

    let typmod = atttypmod.checked_sub(VARHDRSZ)?;
    let precision = (typmod >> 16) & 0xffff;
    let scale = ((typmod & 0x7ff) ^ 1024) - 1024;
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision {
        return None;
    }

    Some((precision as u8, scale as i8))
}

pub fn text_typmod_length(typmod: i32) -> Option<i32> {
    (typmod > VARHDRSZ).then_some(typmod - VARHDRSZ)
}

pub fn pg_text_cast_target(pg_type: PgTypeMetadata) -> Option<String> {
    match pg_type.oid {
        oid::TEXTOID => Some("TEXT".into()),
        oid::VARCHAROID => {
            if pg_type.typmod == -1 {
                Some("CHARACTER VARYING".into())
            } else {
                text_typmod_length(pg_type.typmod)
                    .map(|length| format!("CHARACTER VARYING({length})"))
            }
        }
        oid::BPCHAROID => {
            if pg_type.typmod == -1 {
                Some("pg_catalog.bpchar".into())
            } else {
                text_typmod_length(pg_type.typmod).map(|length| format!("CHARACTER({length})"))
            }
        }
        oid::NAMEOID => Some("NAME".into()),
        _ => None,
    }
}

pub fn arrow_type_for_pg_type(pg_type: PgTypeRef) -> Option<DataType> {
    match pg_type.oid {
        oid::BOOLOID => Some(DataType::Boolean),
        oid::INT2OID => Some(DataType::Int16),
        oid::INT4OID => Some(DataType::Int32),
        oid::INT8OID => Some(DataType::Int64),
        oid::FLOAT4OID => Some(DataType::Float32),
        oid::FLOAT8OID => Some(DataType::Float64),
        oid::TEXTOID | oid::VARCHAROID | oid::BPCHAROID | oid::NAMEOID => Some(DataType::Utf8View),
        oid::BYTEAOID => Some(DataType::BinaryView),
        oid::UUIDOID => Some(DataType::FixedSizeBinary(16)),
        oid::DATEOID => Some(DataType::Date32),
        oid::TIMEOID => Some(DataType::Time64(TimeUnit::Microsecond)),
        oid::TIMESTAMPOID | oid::TIMESTAMPTZOID => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        oid::INTERVALOID => Some(DataType::Interval(IntervalUnit::MonthDayNano)),
        oid::NUMERICOID => {
            let (precision, scale) = numeric_shape_from_typmod(pg_type.typmod)?;
            Some(DataType::Decimal128(precision, scale))
        }
        _ => None,
    }
}

pub fn type_tag_for_pg_type(pg_type: PgTypeRef) -> Option<TypeTag> {
    Some(match pg_type.oid {
        oid::BOOLOID => TypeTag::Boolean,
        oid::INT2OID => TypeTag::Int16,
        oid::INT4OID => TypeTag::Int32,
        oid::INT8OID => TypeTag::Int64,
        oid::FLOAT4OID => TypeTag::Float32,
        oid::FLOAT8OID => TypeTag::Float64,
        oid::TEXTOID | oid::VARCHAROID | oid::BPCHAROID | oid::NAMEOID => TypeTag::Utf8View,
        oid::BYTEAOID => TypeTag::BinaryView,
        oid::UUIDOID => TypeTag::Uuid,
        oid::DATEOID => TypeTag::Date32,
        oid::TIMEOID => TypeTag::Time64Microsecond,
        oid::TIMESTAMPOID | oid::TIMESTAMPTZOID => TypeTag::TimestampMicrosecond,
        oid::INTERVALOID => TypeTag::IntervalMonthDayNano,
        oid::NUMERICOID if numeric_shape_from_typmod(pg_type.typmod).is_some() => {
            TypeTag::Decimal128
        }
        _ => return None,
    })
}

pub fn column_spec_for_pg_type(pg_type: PgTypeRef, nullable: bool) -> Option<ColumnSpec> {
    type_tag_for_pg_type(pg_type).map(|type_tag| ColumnSpec::new(type_tag, nullable))
}

pub fn pg_oid_for_arrow_type(data_type: &DataType) -> Option<u32> {
    match data_type {
        DataType::Boolean => Some(oid::BOOLOID),
        DataType::Int16 => Some(oid::INT2OID),
        DataType::Int32 => Some(oid::INT4OID),
        DataType::Int64 => Some(oid::INT8OID),
        DataType::Float32 => Some(oid::FLOAT4OID),
        DataType::Float64 => Some(oid::FLOAT8OID),
        DataType::Decimal128(_, _) => Some(oid::NUMERICOID),
        DataType::Utf8 | DataType::Utf8View => Some(oid::TEXTOID),
        DataType::Binary | DataType::BinaryView => Some(oid::BYTEAOID),
        DataType::FixedSizeBinary(16) => Some(oid::UUIDOID),
        DataType::Interval(IntervalUnit::MonthDayNano) => Some(oid::INTERVALOID),
        DataType::Date32 => Some(oid::DATEOID),
        DataType::Time64(TimeUnit::Microsecond) => Some(oid::TIMEOID),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(oid::TIMESTAMPOID),
        _ => None,
    }
}

pub fn arrow_data_type_for_type_tag(type_tag: TypeTag) -> DataType {
    match type_tag {
        TypeTag::Boolean => DataType::Boolean,
        TypeTag::Int16 => DataType::Int16,
        TypeTag::Int32 => DataType::Int32,
        TypeTag::Int64 => DataType::Int64,
        TypeTag::Float32 => DataType::Float32,
        TypeTag::Float64 => DataType::Float64,
        TypeTag::Uuid => DataType::FixedSizeBinary(16),
        TypeTag::Utf8View => DataType::Utf8View,
        TypeTag::BinaryView => DataType::BinaryView,
        TypeTag::Decimal128 => DataType::Decimal128(38, 16),
        TypeTag::IntervalMonthDayNano => DataType::Interval(IntervalUnit::MonthDayNano),
        TypeTag::Date32 => DataType::Date32,
        TypeTag::Time64Microsecond => DataType::Time64(TimeUnit::Microsecond),
        TypeTag::TimestampMicrosecond => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}

pub fn normalize_arrow_transport_schema(
    input_schema: &SchemaRef,
) -> Result<SchemaRef, PgTypeError> {
    let mut fields = Vec::with_capacity(input_schema.fields().len());
    for (index, field) in input_schema.fields().iter().enumerate() {
        fields.push(normalize_arrow_transport_field(index, field)?);
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub fn normalize_result_transport_schema(
    input_schema: &SchemaRef,
) -> Result<(SchemaRef, Vec<ColumnSpec>), PgTypeError> {
    if input_schema.fields().is_empty() {
        return Err(PgTypeError::EmptyResultSchema);
    }

    let transport_schema = normalize_arrow_transport_schema(input_schema)?;
    let specs = column_specs_for_arrow_schema(&transport_schema)?;
    Ok((transport_schema, specs))
}

pub fn column_specs_for_arrow_schema(schema: &SchemaRef) -> Result<Vec<ColumnSpec>, PgTypeError> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            TypeTag::from_arrow_data_type(index, field.data_type())
                .map(|type_tag| ColumnSpec::new(type_tag, field.is_nullable()))
                .map_err(|_| PgTypeError::UnsupportedArrowType {
                    index,
                    data_type: field.data_type().to_string(),
                })
        })
        .collect()
}

pub fn normalize_arrow_transport_field(index: usize, field: &Field) -> Result<Field, PgTypeError> {
    let data_type = match field.data_type() {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Decimal128(_, _)
        | DataType::Interval(IntervalUnit::MonthDayNano) => field.data_type().clone(),
        DataType::Time64(TimeUnit::Microsecond)
        | DataType::Timestamp(TimeUnit::Microsecond, None) => field.data_type().clone(),
        DataType::FixedSizeBinary(width) if *width == 16 => field.data_type().clone(),
        DataType::Utf8 | DataType::Utf8View => DataType::Utf8View,
        DataType::Binary | DataType::BinaryView => DataType::BinaryView,
        other => {
            return Err(PgTypeError::UnsupportedArrowType {
                index,
                data_type: other.to_string(),
            })
        }
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone()))
}

#[cfg(feature = "datafusion")]
pub fn pg_type_metadata(oid: u32, typmod: i32, collation: u32) -> FieldMetadata {
    FieldMetadata::from(BTreeMap::from([
        (PG_TYPE_OID_METADATA_KEY.to_string(), oid.to_string()),
        (PG_TYPE_TYPMOD_METADATA_KEY.to_string(), typmod.to_string()),
        (
            PG_TYPE_COLLATION_METADATA_KEY.to_string(),
            collation.to_string(),
        ),
    ]))
}

#[cfg(feature = "datafusion")]
pub fn read_pg_type_metadata(metadata: &FieldMetadata) -> Option<Option<PgTypeMetadata>> {
    let values = metadata.inner();
    let has_pg_key = values.contains_key(PG_TYPE_OID_METADATA_KEY)
        || values.contains_key(PG_TYPE_TYPMOD_METADATA_KEY)
        || values.contains_key(PG_TYPE_COLLATION_METADATA_KEY);
    if !has_pg_key {
        return Some(None);
    }

    let oid = values.get(PG_TYPE_OID_METADATA_KEY)?.parse().ok()?;
    let typmod = values.get(PG_TYPE_TYPMOD_METADATA_KEY)?.parse().ok()?;
    let collation = values.get(PG_TYPE_COLLATION_METADATA_KEY)?.parse().ok()?;
    Some(Some(PgTypeMetadata {
        oid,
        typmod,
        collation,
    }))
}

#[cfg(feature = "datafusion")]
pub fn scalar_for_pg_const(
    value: Option<&PgConstValue>,
    pg_type: PgTypeRef,
) -> Result<ScalarValue, PgTypeError> {
    match value {
        None => typed_null_scalar(pg_type),
        Some(PgConstValue::Bool(value)) => Ok(ScalarValue::Boolean(Some(*value))),
        Some(PgConstValue::Int16(value)) => Ok(ScalarValue::Int16(Some(*value))),
        Some(PgConstValue::Int32(value)) => Ok(ScalarValue::Int32(Some(*value))),
        Some(PgConstValue::Int64(value)) => Ok(ScalarValue::Int64(Some(*value))),
        Some(PgConstValue::Float32(value)) => Ok(ScalarValue::Float32(Some(*value))),
        Some(PgConstValue::Float64(value)) => Ok(ScalarValue::Float64(Some(*value))),
        Some(PgConstValue::Numeric(value)) => {
            let (precision, scale) = numeric_shape_from_typmod(pg_type.typmod).ok_or(
                PgTypeError::UnsupportedNumericTypmod {
                    typmod: pg_type.typmod,
                },
            )?;
            Ok(ScalarValue::Decimal128(
                Some(decimal128_for_numeric_text(value, precision, scale)?),
                precision,
                scale,
            ))
        }
        Some(PgConstValue::Text(value)) => Ok(ScalarValue::Utf8View(Some(value.clone()))),
        Some(PgConstValue::Binary(value)) => Ok(ScalarValue::BinaryView(Some(value.clone()))),
        Some(PgConstValue::Date32(value)) => Ok(ScalarValue::Date32(Some(*value))),
        Some(PgConstValue::Time64Microsecond(value)) => {
            Ok(ScalarValue::Time64Microsecond(Some(*value)))
        }
    }
}

#[cfg(feature = "datafusion")]
fn decimal128_for_numeric_text(value: &str, precision: u8, scale: i8) -> Result<i128, PgTypeError> {
    let scale = usize::try_from(scale).map_err(|_| PgTypeError::UnsupportedConstValue {
        oid: oid::NUMERICOID,
    })?;
    let value = value.trim();
    if value.eq_ignore_ascii_case("nan")
        || value.eq_ignore_ascii_case("infinity")
        || value.eq_ignore_ascii_case("+infinity")
        || value.eq_ignore_ascii_case("-infinity")
    {
        return Err(PgTypeError::UnsupportedConstValue {
            oid: oid::NUMERICOID,
        });
    }

    let (negative, unsigned) = match value.as_bytes().first().copied() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fractional.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
        || fractional.len() > scale
    {
        return Err(PgTypeError::UnsupportedConstValue {
            oid: oid::NUMERICOID,
        });
    }

    let whole = whole.trim_start_matches('0');
    let significant_whole = if whole.is_empty() { "" } else { whole };
    let digit_count = significant_whole.len().saturating_add(scale);
    if digit_count > usize::from(precision) {
        return Err(PgTypeError::UnsupportedConstValue {
            oid: oid::NUMERICOID,
        });
    }

    let mut digits = String::with_capacity(significant_whole.len() + scale);
    digits.push_str(significant_whole);
    digits.push_str(fractional);
    for _ in fractional.len()..scale {
        digits.push('0');
    }
    if digits.is_empty() {
        digits.push('0');
    }
    let scaled = digits
        .parse::<i128>()
        .map_err(|_| PgTypeError::UnsupportedConstValue {
            oid: oid::NUMERICOID,
        })?;
    Ok(if negative { -scaled } else { scaled })
}

#[cfg(feature = "datafusion")]
pub fn typed_null_scalar(pg_type: PgTypeRef) -> Result<ScalarValue, PgTypeError> {
    match pg_type.oid {
        oid::BOOLOID => Ok(ScalarValue::Boolean(None)),
        oid::INT2OID => Ok(ScalarValue::Int16(None)),
        oid::INT4OID => Ok(ScalarValue::Int32(None)),
        oid::INT8OID => Ok(ScalarValue::Int64(None)),
        oid::FLOAT4OID => Ok(ScalarValue::Float32(None)),
        oid::FLOAT8OID => Ok(ScalarValue::Float64(None)),
        oid if is_text_like_type(oid) => Ok(ScalarValue::Utf8View(None)),
        oid::BYTEAOID => Ok(ScalarValue::BinaryView(None)),
        oid::UUIDOID => Ok(ScalarValue::FixedSizeBinary(16, None)),
        oid::DATEOID => Ok(ScalarValue::Date32(None)),
        oid::TIMEOID => Ok(ScalarValue::Time64Microsecond(None)),
        oid::TIMESTAMPOID | oid::TIMESTAMPTZOID => {
            Ok(ScalarValue::TimestampMicrosecond(None, None))
        }
        oid::INTERVALOID => Ok(ScalarValue::IntervalMonthDayNano(None)),
        oid::NUMERICOID => {
            let (precision, scale) = numeric_shape_from_typmod(pg_type.typmod).ok_or(
                PgTypeError::UnsupportedNumericTypmod {
                    typmod: pg_type.typmod,
                },
            )?;
            Ok(ScalarValue::Decimal128(None, precision, scale))
        }
        oid => Err(PgTypeError::UnsupportedTypedNull { oid }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_supported_pg_type_to_arrow() {
        let cases = [
            (oid::BOOLOID, DataType::Boolean),
            (oid::TEXTOID, DataType::Utf8View),
            (oid::VARCHAROID, DataType::Utf8View),
            (oid::BPCHAROID, DataType::Utf8View),
            (oid::NAMEOID, DataType::Utf8View),
            (oid::INT2OID, DataType::Int16),
            (oid::INT4OID, DataType::Int32),
            (oid::INT8OID, DataType::Int64),
            (oid::FLOAT4OID, DataType::Float32),
            (oid::FLOAT8OID, DataType::Float64),
            (oid::UUIDOID, DataType::FixedSizeBinary(16)),
            (oid::BYTEAOID, DataType::BinaryView),
            (oid::DATEOID, DataType::Date32),
            (oid::TIMEOID, DataType::Time64(TimeUnit::Microsecond)),
            (
                oid::TIMESTAMPOID,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                oid::TIMESTAMPTZOID,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                oid::INTERVALOID,
                DataType::Interval(IntervalUnit::MonthDayNano),
            ),
        ];

        for (oid, data_type) in cases {
            assert_eq!(
                arrow_type_for_pg_type(PgTypeRef::new(oid, -1, 0)),
                Some(data_type)
            );
        }
    }

    #[test]
    fn converts_postgres_date_to_arrow_date32_epoch() {
        assert_eq!(date32_from_pg_date(0), Some(POSTGRES_TO_UNIX_EPOCH_DAYS));
        assert_eq!(pg_date_from_date32(POSTGRES_TO_UNIX_EPOCH_DAYS), Some(0));
        assert_eq!(date32_from_pg_date(-1_753), Some(9_204));
        assert_eq!(pg_date_from_date32(9_204), Some(-1_753));
        assert_eq!(date32_from_pg_date(PG_DATE_NEG_INFINITY), None);
        assert_eq!(date32_from_pg_date(PG_DATE_POS_INFINITY), None);
        assert_eq!(pg_date_from_date32(i32::MIN), None);
        assert_eq!(
            pg_date_from_date32(PG_DATE_NEG_INFINITY + POSTGRES_TO_UNIX_EPOCH_DAYS),
            None
        );
    }

    #[test]
    fn rejects_unsupported_pg_type() {
        assert_eq!(
            arrow_type_for_pg_type(PgTypeRef::new(oid::TIMETZOID, -1, 0)),
            None
        );
        assert_eq!(
            arrow_type_for_pg_type(PgTypeRef::new(oid::JSONBOID, -1, 0)),
            None
        );
    }

    #[test]
    fn maps_numeric_typmods() {
        assert_eq!(numeric_shape_from_typmod(-1), Some((38, 16)));
        assert_eq!(
            numeric_shape_from_typmod(numeric_typmod(12, 3)),
            Some((12, 3))
        );
        assert_eq!(
            numeric_shape_from_typmod(numeric_typmod(38, 0)),
            Some((38, 0))
        );
        assert_eq!(numeric_shape_from_typmod(numeric_typmod(39, 0)), None);
        assert_eq!(numeric_shape_from_typmod(numeric_typmod(3, 4)), None);
        assert_eq!(numeric_shape_from_typmod(numeric_typmod(3, -1)), None);
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn maps_numeric_constants_to_decimal128() {
        let scalar = scalar_for_pg_const(
            Some(&PgConstValue::Numeric("-12.5".into())),
            PgTypeRef::new(oid::NUMERICOID, numeric_typmod(5, 2), 0),
        )
        .unwrap();
        assert_eq!(scalar, ScalarValue::Decimal128(Some(-1250), 5, 2));

        assert!(scalar_for_pg_const(
            Some(&PgConstValue::Numeric("1.234".into())),
            PgTypeRef::new(oid::NUMERICOID, numeric_typmod(5, 2), 0),
        )
        .is_err());
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn maps_date_constants_to_date32() {
        let scalar = scalar_for_pg_const(
            Some(&PgConstValue::Date32(9_204)),
            PgTypeRef::new(oid::DATEOID, -1, 0),
        )
        .unwrap();
        assert_eq!(scalar, ScalarValue::Date32(Some(9_204)));
    }

    #[test]
    fn normalizes_arrow_transport_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("txt", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, false),
            Field::new("n", DataType::Int64, true),
        ]));

        let normalized = normalize_arrow_transport_schema(&schema).unwrap();
        assert_eq!(normalized.field(0).data_type(), &DataType::Utf8View);
        assert_eq!(normalized.field(1).data_type(), &DataType::BinaryView);
        assert_eq!(normalized.field(2).data_type(), &DataType::Int64);
    }

    fn numeric_typmod(precision: i32, scale: i32) -> i32 {
        ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ
    }
}
