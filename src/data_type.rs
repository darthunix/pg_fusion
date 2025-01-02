use crate::error::FusionError;
use anyhow::{bail, Result};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::arrow::array::types::IntervalMonthDayNano;
use datafusion::common::arrow::datatypes::Field;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
use libc::c_void;
use pgrx::datum::{Date, FromDatum, Interval, Time, Timestamp};
use pgrx::pg_schema;
use pgrx::pg_sys::SysCacheIdentifier::TYPEOID;
use pgrx::pg_sys::{
    self, list_append_unique_ptr, makeTargetEntry, makeVar, palloc0, Datum, Expr, List, NodeTag,
    ParamListInfo, TargetEntry, GETSTRUCT,
};
use std::char;
use std::ffi::c_char;

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
                        months: t.months(),
                        days: t.days(),
                        nanoseconds: t.micros() * 1000,
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

pub(crate) fn repack_params(param_list: ParamListInfo) -> Result<Vec<ScalarValue>> {
    let num_params = unsafe { (*param_list).numParams } as usize;
    let mut values: Vec<ScalarValue> = Vec::with_capacity(num_params);
    let params = unsafe { (*param_list).params.as_slice(num_params) };
    for param in params {
        let value = datum_to_scalar(param.value, param.ptype, param.isnull)?;
        values.push(value);
    }
    Ok(values)
}

fn type_to_oid(type_: &DataType) -> pg_sys::Oid {
    match type_ {
        DataType::Boolean => pg_sys::BOOLOID,
        DataType::Utf8 => pg_sys::TEXTOID,
        DataType::Int8 => pg_sys::INT8OID,
        DataType::Int16 => pg_sys::INT2OID,
        DataType::Int32 => pg_sys::INT4OID,
        DataType::Int64 => pg_sys::INT8OID,
        DataType::Float32 => pg_sys::FLOAT4OID,
        DataType::Float64 => pg_sys::FLOAT8OID,
        DataType::Date32 => pg_sys::DATEOID,
        DataType::Time64(_) => pg_sys::TIMEOID,
        DataType::Timestamp(_, _) => pg_sys::TIMESTAMPOID,
        DataType::Interval(_) => pg_sys::INTERVALOID,
        _ => unimplemented!(),
    }
}

fn filed_to_target_entry(field: Field, position: i16) -> *mut TargetEntry {
    let type_oid = type_to_oid(field.data_type());
    let tuple =
        unsafe { pg_sys::SearchSysCache1(TYPEOID as i32, pg_sys::ObjectIdGetDatum(type_oid)) };
    if tuple.is_null() {
        panic!("Cache lookup failed for type {:?}", type_oid);
    }
    unsafe {
        let typtup = GETSTRUCT(tuple) as pg_sys::Form_pg_type;
        let expr = makeVar(
            pg_sys::INDEX_VAR,
            position + 1,
            type_oid,
            (*typtup).typtypmod,
            (*typtup).typcollation,
            0,
        );
        let name = palloc0(field.name().len() + 1) as *mut u8;
        std::ptr::copy_nonoverlapping(field.name().as_ptr(), name, field.name().len());
        let entry = makeTargetEntry(expr as *mut Expr, position + 1, name as *mut i8, false);
        pg_sys::ReleaseSysCache(tuple);
        entry
    }
}

pub(crate) fn repack_output(columns: &[Field]) -> *mut List {
    let list = unsafe { palloc0(size_of::<List>()) as *mut List };
    assert!(columns.len() < i16::MAX as usize);
    for (i, column) in columns.iter().enumerate() {
        let entry = filed_to_target_entry(column.clone(), (i + 1) as i16);
        unsafe {
            list_append_unique_ptr(list, entry as *mut c_void);
        }
    }
    list
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::pg_sys;
    use pgrx::prelude::*;

    use super::*;

    #[pg_test]
    fn test_df_to_pg_column_convertion() {
        let field = Field::new("test", DataType::Int32, false);
        let entry_ptr = filed_to_target_entry(field, 0);
        let var = unsafe { *((*entry_ptr).expr as *mut pg_sys::Var) };
        assert_eq!(var.varno, pg_sys::INDEX_VAR);
        assert_eq!(var.varattno, 1);
        assert_eq!(var.vartype, pg_sys::INT4OID);
        assert_eq!(var.vartypmod, -1);

        let entry = unsafe { &*entry_ptr };
        assert_eq!(entry.resno, 1);
        let name = unsafe { std::ffi::CStr::from_ptr(entry.resname) };
        assert_eq!(name.to_str().unwrap(), "test");
        assert!(!entry.resjunk);
    }
}
