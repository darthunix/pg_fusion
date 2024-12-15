use std::char;

use crate::error::FusionError;
use anyhow::{bail, Result};
use datafusion::common::arrow::array::types::IntervalMonthDayNano;
use datafusion::common::{ParamValues, ScalarValue};
use pgrx::datum::{Date, FromDatum, Interval, Time, Timestamp};
use pgrx::pg_sys::{self, Datum, ParamListInfo};

fn datum_to_scalar(datum: Datum, ptype: pg_sys::Oid, is_null: bool) -> Result<ScalarValue> {
    unsafe {
        match ptype {
            pg_sys::BOOLOID => Ok(ScalarValue::Boolean(bool::from_datum(datum, is_null))),
            pg_sys::BPCHAROID | pg_sys::CSTRINGOID | pg_sys::TEXTOID | pg_sys::VARCHAROID => {
                Ok(ScalarValue::Utf8(String::from_datum(datum, is_null)))
            }
            pg_sys::CHAROID => {
                let value = char::from_datum(datum, is_null);
                Ok(ScalarValue::Utf8(value.map(|c| c.to_string())))
            }
            pg_sys::DATEOID => {
                let value = Date::from_datum(datum, is_null);
                Ok(ScalarValue::Date32(value.map(|d| d.to_unix_epoch_days())))
            }
            pg_sys::FLOAT4OID => Ok(ScalarValue::Float32(f32::from_datum(datum, is_null))),
            pg_sys::FLOAT8OID => Ok(ScalarValue::Float64(f64::from_datum(datum, is_null))),
            pg_sys::INT2OID => Ok(ScalarValue::Int16(i16::from_datum(datum, is_null))),
            pg_sys::INT4OID => Ok(ScalarValue::Int32(i32::from_datum(datum, is_null))),
            pg_sys::INT8OID => Ok(ScalarValue::Int64(i64::from_datum(datum, is_null))),
            pg_sys::INTERVALOID => {
                let value = Interval::from_datum(datum, is_null);
                Ok(ScalarValue::IntervalMonthDayNano(value.map(|t| {
                    IntervalMonthDayNano {
                        months: t.months() as i32,
                        days: t.days() as i32,
                        nanoseconds: t.micros() as i64 * 1000,
                    }
                })))
            }
            pg_sys::TIMEOID => {
                let value = Time::from_datum(datum, is_null);
                Ok(ScalarValue::Time64Microsecond(
                    value.map(|t| t.microseconds() as i64),
                ))
            }
            pg_sys::TIMESTAMPOID => {
                let value = Timestamp::from_datum(datum, is_null);
                Ok(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.microseconds() as i64),
                    None,
                ))
            }
            // TODO: Add support for Decimal128 (NUMERICOID with pgrx::AnyNumeric)
            _ => bail!(FusionError::UnsupportedType(format!("{:?}", ptype))),
        }
    }
}

pub(crate) fn repack_params(param_list: ParamListInfo) -> Result<ParamValues> {
    let num_params = unsafe { (*param_list).numParams } as usize;
    let mut values: Vec<ScalarValue> = Vec::with_capacity(num_params);
    let params = unsafe { (*param_list).params.as_slice(num_params) };
    for param in params {
        let value = datum_to_scalar(param.value, param.ptype, param.isnull)?;
        values.push(value);
    }
    Ok(ParamValues::List(values))
}
