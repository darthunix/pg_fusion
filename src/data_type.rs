use crate::error::FusionError;
use crate::ipc::SlotStream;
use anyhow::{bail, Result};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::arrow::array::types::IntervalMonthDayNano;
use datafusion::common::arrow::datatypes::Field;
use datafusion::common::ScalarValue;
use libc::c_void;
use pgrx::datum::{Date, FromDatum, Interval, Time, Timestamp};
use pgrx::pg_sys::SysCacheIdentifier::TYPEOID;
use pgrx::pg_sys::{
    self, list_append_unique_ptr, makeTargetEntry, makeVar, palloc0, Datum, Expr, List,
    TargetEntry, GETSTRUCT,
};
use rmp::decode::{
    read_bool, read_f32, read_f64, read_i16, read_i32, read_i64, read_str_len, RmpRead,
};
use rmp::encode::{
    write_bool, write_f32, write_f64, write_i16, write_i32, write_i64, write_pfix, write_str,
    RmpWrite,
};
use rmp::Marker;
use std::char;

pub(crate) fn datum_to_scalar(
    datum: Datum,
    ptype: pg_sys::Oid,
    is_null: bool,
) -> Result<ScalarValue> {
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

fn type_to_oid(type_: &DataType) -> pg_sys::Oid {
    match type_ {
        DataType::Boolean => pg_sys::BOOLOID,
        DataType::Utf8 => pg_sys::TEXTOID,
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

#[repr(u8)]
pub(crate) enum EncodedType {
    Boolean = 0,
    Utf8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Float32 = 5,
    Float64 = 6,
    Date32 = 7,
    Time64 = 8,
    Timestamp = 9,
    Interval = 10,
}

impl EncodedType {
    pub(crate) fn to_arrow(&self) -> DataType {
        match self {
            EncodedType::Boolean => DataType::Boolean,
            EncodedType::Utf8 => DataType::Utf8,
            EncodedType::Int16 => DataType::Int16,
            EncodedType::Int32 => DataType::Int32,
            EncodedType::Int64 => DataType::Int64,
            EncodedType::Float32 => DataType::Float32,
            EncodedType::Float64 => DataType::Float64,
            EncodedType::Date32 => DataType::Date32,
            EncodedType::Time64 => DataType::Time64(TimeUnit::Microsecond),
            EncodedType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            EncodedType::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        }
    }
}

impl TryFrom<u8> for EncodedType {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EncodedType::Boolean),
            1 => Ok(EncodedType::Utf8),
            2 => Ok(EncodedType::Int16),
            3 => Ok(EncodedType::Int32),
            4 => Ok(EncodedType::Int64),
            5 => Ok(EncodedType::Float32),
            6 => Ok(EncodedType::Float64),
            7 => Ok(EncodedType::Date32),
            8 => Ok(EncodedType::Time64),
            9 => Ok(EncodedType::Timestamp),
            10 => Ok(EncodedType::Interval),
            _ => Err(FusionError::Deserialize(
                "encoded type".to_string(),
                value.into(),
            )),
        }
    }
}

impl TryFrom<pg_sys::Oid> for EncodedType {
    type Error = FusionError;

    fn try_from(value: pg_sys::Oid) -> Result<Self, Self::Error> {
        match value {
            pg_sys::BOOLOID => Ok(EncodedType::Boolean),
            pg_sys::TEXTOID => Ok(EncodedType::Utf8),
            pg_sys::INT2OID => Ok(EncodedType::Int16),
            pg_sys::INT4OID => Ok(EncodedType::Int32),
            pg_sys::INT8OID => Ok(EncodedType::Int64),
            pg_sys::FLOAT4OID => Ok(EncodedType::Float32),
            pg_sys::FLOAT8OID => Ok(EncodedType::Float64),
            pg_sys::DATEOID => Ok(EncodedType::Date32),
            pg_sys::TIMEOID => Ok(EncodedType::Time64),
            pg_sys::TIMESTAMPOID => Ok(EncodedType::Timestamp),
            pg_sys::INTERVALOID => Ok(EncodedType::Interval),
            _ => Err(FusionError::Deserialize(
                "encoded type".to_string(),
                value.as_u32().into(),
            )),
        }
    }
}

#[inline]
pub(crate) fn write_scalar_value(stream: &mut SlotStream, value: &ScalarValue) -> Result<()> {
    let write_null = |stream: &mut SlotStream| -> Result<()> {
        // Though it is not a valid msgpack, we use it to represent null values
        // as we don't want to waste bytes for additional marker.
        stream.write_u8(Marker::Null.to_u8())?;
        Ok(())
    };
    match value {
        ScalarValue::Boolean(v) => {
            write_pfix(stream, EncodedType::Boolean as u8)?;
            match v {
                Some(v) => write_bool(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Utf8(v) => {
            write_pfix(stream, EncodedType::Utf8 as u8)?;
            match v {
                Some(v) => write_str(stream, v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Int16(v) => {
            write_pfix(stream, EncodedType::Int16 as u8)?;
            match v {
                Some(v) => write_i16(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Int32(v) => {
            write_pfix(stream, EncodedType::Int32 as u8)?;
            match v {
                Some(v) => write_i32(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Int64(v) => {
            write_pfix(stream, EncodedType::Int64 as u8)?;
            match v {
                Some(v) => write_i64(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Float32(v) => {
            write_pfix(stream, EncodedType::Float32 as u8)?;
            match v {
                Some(v) => write_f32(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Float64(v) => {
            write_pfix(stream, EncodedType::Float64 as u8)?;
            match v {
                Some(v) => write_f64(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::Date32(v) => {
            write_pfix(stream, EncodedType::Date32 as u8)?;
            match v {
                Some(v) => write_i32(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        ScalarValue::IntervalMonthDayNano(v) => {
            write_pfix(stream, EncodedType::Interval as u8)?;
            match v {
                Some(v) => {
                    write_i32(stream, v.months)?;
                    write_i32(stream, v.days)?;
                    write_i64(stream, v.nanoseconds)?;
                }
                None => write_null(stream)?,
            }
        }
        ScalarValue::Time64Microsecond(v) => {
            write_pfix(stream, EncodedType::Time64 as u8)?;
            match v {
                Some(v) => write_i64(stream, *v)?,
                None => write_null(stream)?,
            }
        }
        _ => return Err(FusionError::UnsupportedType(format!("{value:?}")).into()),
    }
    Ok(())
}

#[inline]
pub(crate) fn read_scalar_value(stream: &mut SlotStream) -> Result<ScalarValue> {
    let etype = stream.read_u8()?;
    let value = match EncodedType::try_from(etype)? {
        EncodedType::Boolean => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Boolean(None)
            } else {
                ScalarValue::Boolean(Some(read_bool(stream)?))
            }
        }
        EncodedType::Utf8 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Utf8(None)
            } else {
                let len = read_str_len(stream)?;
                let mut buf: Vec<u8> = vec![0; len as usize];
                stream.read_exact_buf(&mut buf)?;
                let s = unsafe { String::from_utf8_unchecked(buf) };
                ScalarValue::Utf8(Some(s))
            }
        }
        EncodedType::Int16 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Int16(None)
            } else {
                ScalarValue::Int16(Some(read_i16(stream)?))
            }
        }
        EncodedType::Int32 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Int32(None)
            } else {
                ScalarValue::Int32(Some(read_i32(stream)? as i32))
            }
        }
        EncodedType::Int64 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Int64(None)
            } else {
                ScalarValue::Int64(Some(read_i64(stream)? as i64))
            }
        }
        EncodedType::Float32 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Float32(None)
            } else {
                ScalarValue::Float32(Some(read_f32(stream)?))
            }
        }
        EncodedType::Float64 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Float64(None)
            } else {
                ScalarValue::Float64(Some(read_f64(stream)? as f64))
            }
        }
        EncodedType::Date32 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Date32(None)
            } else {
                ScalarValue::Date32(Some(read_i32(stream)?))
            }
        }
        EncodedType::Interval => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::IntervalMonthDayNano(None)
            } else {
                let months = read_i32(stream)?;
                let days = read_i32(stream)?;
                let nanoseconds = read_i64(stream)?;
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds,
                }))
            }
        }
        EncodedType::Time64 => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Time64Microsecond(None)
            } else {
                ScalarValue::Time64Microsecond(Some(read_i64(stream)?))
            }
        }
        EncodedType::Timestamp => {
            let marker = stream.look_ahead(size_of::<u8>())?[0];
            if marker == u8::from(Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::TimestampMicrosecond(None, None)
            } else {
                ScalarValue::TimestampMicrosecond(Some(read_i64(stream)?), None)
            }
        }
    };
    Ok(value)
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
#[pgrx::pg_schema]
mod tests {
    use pgrx::pg_sys;
    use pgrx::prelude::*;

    use crate::ipc::Slot;

    use super::*;
    use std::ptr::addr_of_mut;
    const SLOT_SIZE: usize = 8204;

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

    #[pg_test]
    fn test_scalar_value_serialization() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let ptr = addr_of_mut!(slot_buf) as *mut u8;
        Slot::init(ptr, slot_buf.len());
        let slot = Slot::from_bytes(ptr, slot_buf.len());
        let mut stream: SlotStream = slot.into();
        let check = |stream: &mut SlotStream, value: ScalarValue| {
            stream.reset();
            write_scalar_value(stream, &value).expect("failed to write scalar value");
            stream.reset();
            let new_value = read_scalar_value(stream).expect("failed to read scalar value");
            assert_eq!(value, new_value);
        };
        check(&mut stream, ScalarValue::Boolean(None));
        check(&mut stream, ScalarValue::Boolean(Some(false)));
        check(&mut stream, ScalarValue::Boolean(Some(true)));
        check(&mut stream, ScalarValue::Utf8(None));
        check(&mut stream, ScalarValue::Utf8(Some("test".to_string())));
        check(&mut stream, ScalarValue::Utf8(Some("".to_string())));
        check(&mut stream, ScalarValue::Int16(None));
        check(&mut stream, ScalarValue::Int16(Some(42)));
        check(&mut stream, ScalarValue::Int16(Some(-42)));
        check(&mut stream, ScalarValue::Int32(None));
        check(&mut stream, ScalarValue::Int32(Some(42)));
        check(&mut stream, ScalarValue::Int32(Some(-42)));
        check(&mut stream, ScalarValue::Int64(None));
        check(&mut stream, ScalarValue::Int64(Some(42)));
        check(&mut stream, ScalarValue::Int64(Some(-42)));
        check(&mut stream, ScalarValue::Float32(None));
        check(&mut stream, ScalarValue::Float32(Some(42.0)));
        check(&mut stream, ScalarValue::Float32(Some(-42.0)));
        check(&mut stream, ScalarValue::Float64(None));
        check(&mut stream, ScalarValue::Float64(Some(42.0)));
        check(&mut stream, ScalarValue::Date32(None));
        check(&mut stream, ScalarValue::Date32(Some(42)));
        check(&mut stream, ScalarValue::Date32(Some(-42)));
        check(&mut stream, ScalarValue::IntervalMonthDayNano(None));
        check(
            &mut stream,
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                months: 1,
                days: 10,
                nanoseconds: 666,
            })),
        );
        check(&mut stream, ScalarValue::Time64Microsecond(None));
        check(&mut stream, ScalarValue::Time64Microsecond(Some(42)));
    }
}
