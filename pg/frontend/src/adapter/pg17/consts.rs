use super::*;

pub(super) unsafe fn read_const(constant: &pg_sys::Const) -> Result<Const, PgFrontendError> {
    let pg_type = if constant.consttype == pg_sys::UNKNOWNOID {
        type_ref(pg_sys::TEXTOID, -1, constant.constcollid)
    } else {
        type_ref(
            constant.consttype,
            constant.consttypmod,
            constant.constcollid,
        )
    };
    supported_value_type(pg_type).map_err(|reason| PgFrontendError::unsupported(reason.message))?;
    if constant.constisnull {
        return Ok(Const {
            pg_type,
            value: None,
        });
    }
    if constant.consttype != pg_sys::UNKNOWNOID {
        supported_non_null_const_type(pg_type)
            .map_err(|reason| PgFrontendError::unsupported(reason.message))?;
    }

    let value = match constant.consttype {
        oid if oid == pg_sys::UNKNOWNOID => {
            PgConstValue::Text(unsafe { read_unknown_const(constant.constvalue) }?)
        }
        oid if oid == pg_sys::BOOLOID => {
            PgConstValue::Bool(unsafe { bool::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::INT2OID => {
            PgConstValue::Int16(unsafe { i16::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::INT4OID => {
            PgConstValue::Int32(unsafe { i32::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::INT8OID => {
            PgConstValue::Int64(unsafe { i64::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::FLOAT4OID => {
            PgConstValue::Float32(unsafe { f32::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::FLOAT8OID => {
            PgConstValue::Float64(unsafe { f64::from_datum(constant.constvalue, false) }.unwrap())
        }
        oid if oid == pg_sys::NUMERICOID => unsafe {
            read_numeric_const(constant.constvalue, pg_type)?
        },
        oid if oid == pg_sys::TEXTOID || oid == pg_sys::VARCHAROID || oid == pg_sys::BPCHAROID => {
            PgConstValue::Text(
                unsafe {
                    String::from_polymorphic_datum(constant.constvalue, false, constant.consttype)
                }
                .unwrap(),
            )
        }
        oid if oid == pg_sys::NAMEOID => {
            PgConstValue::Text(unsafe { read_name_const(constant.constvalue) }?)
        }
        oid if oid == pg_sys::BYTEAOID => PgConstValue::Binary(
            unsafe {
                Vec::<u8>::from_polymorphic_datum(constant.constvalue, false, constant.consttype)
            }
            .unwrap(),
        ),
        oid if oid == pg_sys::DATEOID => {
            let value = unsafe { i32::from_datum(constant.constvalue, false) }.unwrap();
            PgConstValue::Date32(date32_from_pg_date(value).ok_or_else(|| {
                PgFrontendError::unsupported(
                    "date constant is outside the Arrow Date32 finite range",
                )
            })?)
        }
        oid if oid == pg_sys::TIMEOID => PgConstValue::Time64Microsecond(time_const(
            unsafe { i64::from_datum(constant.constvalue, false) }.unwrap(),
        )?),
        oid if oid == pg_sys::TIMESTAMPOID => {
            return Err(unsupported_temporal_const("timestamp"));
        }
        oid if oid == pg_sys::TIMESTAMPTZOID => {
            return Err(unsupported_temporal_const("timestamptz"));
        }
        oid => {
            return Err(PgFrontendError::unsupported(format!(
                "constant type oid {} is not supported by pg_frontend v1",
                u32::from(oid)
            )))
        }
    };

    Ok(Const {
        pg_type,
        value: Some(value),
    })
}

unsafe fn read_numeric_const(
    datum: pg_sys::Datum,
    pg_type: PgTypeRef,
) -> Result<PgConstValue, PgFrontendError> {
    let (precision, scale) = numeric_shape_from_typmod(pg_type.typmod).ok_or_else(|| {
        PgFrontendError::unsupported(format!(
            "unsupported numeric typmod {} in constant",
            pg_type.typmod
        ))
    })?;
    let original = datum.cast_mut_ptr::<pg_sys::varlena>();
    if original.is_null() {
        return Err(PgFrontendError::unsupported("null numeric datum pointer"));
    }
    let detoasted = unsafe { pg_sys::pg_detoast_datum(original) };
    if detoasted.is_null() {
        return Err(PgFrontendError::unsupported(
            "null detoasted numeric datum pointer",
        ));
    }

    let is_copy = !std::ptr::eq(detoasted, original);
    let result = (|| {
        let len = unsafe { pgrx::varlena::varsize_4b(detoasted) };
        let bytes = unsafe { slice::from_raw_parts(detoasted.cast::<u8>(), len) };
        let value = numeric_to_decimal128(bytes, scale, precision).map_err(numeric_const_error)?;
        let display_scale = numeric_display_scale(bytes).map_err(numeric_const_error)?;
        let display_scale = i8::try_from(display_scale).map_err(|_| {
            PgFrontendError::unsupported(
                "numeric constant display scale exceeds Decimal128 scale support",
            )
        })?;
        Ok(PgConstValue::Numeric(PgNumericConst {
            value,
            scale,
            display_scale,
        }))
    })();
    if is_copy {
        unsafe { pg_sys::pfree(detoasted.cast()) };
    }
    result
}

fn numeric_const_error(error: NumericDecodeError) -> PgFrontendError {
    match error {
        NumericDecodeError::Special => PgFrontendError::unsupported(
            "pg_fusion Decimal128 numeric cannot represent PostgreSQL numeric NaN/Infinity values",
        ),
        NumericDecodeError::OutOfRange => {
            PgFrontendError::unsupported("numeric constant is outside pg_fusion Decimal128 support")
        }
    }
}

pub(super) unsafe fn read_unknown_const(datum: pg_sys::Datum) -> Result<String, PgFrontendError> {
    let ptr = datum.cast_mut_ptr::<c_char>();
    if ptr.is_null() {
        return Err(PgFrontendError::unsupported(
            "unknown constants must contain a non-null C string",
        ));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(str::to_owned)
        .map_err(|_| PgFrontendError::unsupported("unknown constants must contain valid UTF-8"))
}

pub(super) unsafe fn read_name_const(datum: pg_sys::Datum) -> Result<String, PgFrontendError> {
    let ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
    if ptr.is_null() {
        return Err(PgFrontendError::unsupported("null name datum pointer"));
    }
    decode_name_data(unsafe { &*ptr })
}

pub(super) fn decode_name_data(name: &pg_sys::NameData) -> Result<String, PgFrontendError> {
    decode_name_bytes(&name.data)
}

pub(super) fn decode_name_bytes(bytes: &[c_char]) -> Result<String, PgFrontendError> {
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    let raw = unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), end) };
    str::from_utf8(raw)
        .map(str::to_owned)
        .map_err(|_| PgFrontendError::unsupported("name constants must contain valid UTF-8"))
}
