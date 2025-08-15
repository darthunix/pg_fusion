use crate::Tape;
use anyhow::{bail, Result};
use common::FusionError;
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::arrow::array::types::IntervalMonthDayNano;
use datafusion::common::ScalarValue;
use rmp::decode::{
    read_bool, read_f32, read_f64, read_i16, read_i32, read_i64, read_str_len, RmpRead,
};
use rmp::encode::{
    write_bool, write_f32, write_f64, write_i16, write_i32, write_i64, write_pfix, write_str,
    RmpWrite,
};
use smol_str::format_smolstr;
use std::io::Write;

#[repr(u8)]
pub enum EncodedType {
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
    pub fn to_arrow(&self) -> DataType {
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
                "encoded type".into(),
                value.into(),
            )),
        }
    }
}

impl TryFrom<&DataType> for EncodedType {
    type Error = FusionError;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Boolean => Ok(EncodedType::Boolean),
            DataType::Utf8 => Ok(EncodedType::Utf8),
            DataType::Int16 => Ok(EncodedType::Int16),
            DataType::Int32 => Ok(EncodedType::Int32),
            DataType::Int64 => Ok(EncodedType::Int64),
            DataType::Float32 => Ok(EncodedType::Float32),
            DataType::Float64 => Ok(EncodedType::Float64),
            DataType::Date32 => Ok(EncodedType::Date32),
            DataType::Time64(_) => Ok(EncodedType::Time64),
            DataType::Timestamp(_, _) => Ok(EncodedType::Timestamp),
            DataType::Interval(_) => Ok(EncodedType::Interval),
            _ => Err(FusionError::UnsupportedType(format_smolstr!("{:?}", value))),
        }
    }
}

#[inline]
pub fn write_scalar_value(stream: &mut impl Write, value: &ScalarValue) -> Result<()> {
    let write_null = |mut stream: &mut dyn Write| -> Result<()> {
        // Though it is not a valid msgpack, we use it to represent null values
        // as we don't want to waste bytes for additional marker.
        stream.write_u8(rmp::Marker::Null.to_u8())?;
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
        _ => return Err(FusionError::UnsupportedType(format_smolstr!("{value:?}")).into()),
    }
    Ok(())
}

#[inline]
fn read_u8(stream: &mut impl Tape) -> Result<u8> {
    let mut buf = [0; 1];
    stream.read_exact_buf(&mut buf)?;
    Ok(buf[0])
}

#[inline]
fn peak_u8(stream: &mut impl Tape) -> Result<u8> {
    let mut buf = [0; 1];
    let len = stream.peek(&mut buf);
    if len != 1 {
        bail!(FusionError::BufferTooSmall(len));
    }
    Ok(buf[0])
}

#[inline]
pub fn read_scalar_value(stream: &mut impl Tape) -> Result<ScalarValue> {
    let etype = read_u8(stream)?;
    let marker = peak_u8(stream)?;
    let value = match EncodedType::try_from(etype)? {
        EncodedType::Boolean => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = read_u8(stream)?;
                ScalarValue::Boolean(None)
            } else {
                ScalarValue::Boolean(Some(read_bool(stream)?))
            }
        }
        EncodedType::Utf8 => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = read_u8(stream)?;
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
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Int16(None)
            } else {
                ScalarValue::Int16(Some(read_i16(stream)?))
            }
        }
        EncodedType::Int32 => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Int32(None)
            } else {
                ScalarValue::Int32(Some(read_i32(stream)? as i32))
            }
        }
        EncodedType::Int64 => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Int64(None)
            } else {
                ScalarValue::Int64(Some(read_i64(stream)? as i64))
            }
        }
        EncodedType::Float32 => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Float32(None)
            } else {
                ScalarValue::Float32(Some(read_f32(stream)?))
            }
        }
        EncodedType::Float64 => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Float64(None)
            } else {
                ScalarValue::Float64(Some(read_f64(stream)? as f64))
            }
        }
        EncodedType::Date32 => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Date32(None)
            } else {
                ScalarValue::Date32(Some(read_i32(stream)?))
            }
        }
        EncodedType::Interval => {
            if marker == u8::from(rmp::Marker::Null) {
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
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::Time64Microsecond(None)
            } else {
                ScalarValue::Time64Microsecond(Some(read_i64(stream)?))
            }
        }
        EncodedType::Timestamp => {
            if marker == u8::from(rmp::Marker::Null) {
                let _ = stream.read_u8()?;
                ScalarValue::TimestampMicrosecond(None, None)
            } else {
                ScalarValue::TimestampMicrosecond(Some(read_i64(stream)?), None)
            }
        }
    };
    Ok(value)
}
