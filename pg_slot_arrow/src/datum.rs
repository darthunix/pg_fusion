use crate::error::oid_u32;
use crate::{ConfigError, EncodeError};
use page_arrow_layout::TypeTag;
use pgrx_pg_sys as pg_sys;
use std::ptr;
use std::slice;

#[cfg(test)]
use std::sync::atomic::{AtomicI32, Ordering};

#[cfg(target_endian = "little")]
const VARLENA_1B_FLAG: u8 = 0x01;

#[cfg(target_endian = "big")]
const VARLENA_1B_FLAG: u8 = 0x80;

#[cfg(target_endian = "little")]
const VARLENA_4B_COMPRESSED_FLAG: u32 = 0x02;

#[cfg(target_endian = "big")]
const VARLENA_4B_COMPRESSED_FLAG: u32 = 0x4000_0000;

pub(crate) fn validate_pg_layout_type(
    index: usize,
    oid: pg_sys::Oid,
    type_tag: TypeTag,
) -> Result<(), ConfigError> {
    let ok = match type_tag {
        TypeTag::Boolean => oid == pg_sys::BOOLOID,
        TypeTag::Int16 => oid == pg_sys::INT2OID,
        TypeTag::Int32 => oid == pg_sys::INT4OID,
        TypeTag::Int64 => oid == pg_sys::INT8OID,
        TypeTag::Float32 => oid == pg_sys::FLOAT4OID,
        TypeTag::Float64 => oid == pg_sys::FLOAT8OID,
        TypeTag::Uuid => oid == pg_sys::UUIDOID,
        TypeTag::Utf8View => {
            oid == pg_sys::TEXTOID
                || oid == pg_sys::VARCHAROID
                || oid == pg_sys::BPCHAROID
                || oid == pg_sys::NAMEOID
        }
        TypeTag::BinaryView => oid == pg_sys::BYTEAOID,
    };
    if ok {
        Ok(())
    } else {
        Err(ConfigError::PgLayoutTypeMismatch {
            index,
            oid: oid_u32(oid),
            type_tag,
        })
    }
}

pub(crate) fn pg_oid_needs_detoast(oid: pg_sys::Oid) -> bool {
    oid == pg_sys::TEXTOID
        || oid == pg_sys::VARCHAROID
        || oid == pg_sys::BPCHAROID
        || oid == pg_sys::BYTEAOID
}

#[cfg(not(test))]
pub(crate) fn with_detoasted_slot_datum<R, F>(
    datum: pg_sys::Datum,
    index: usize,
    f: F,
) -> Result<R, EncodeError>
where
    F: FnOnce(pg_sys::Datum) -> Result<R, EncodeError>,
{
    let original = datum.cast_mut_ptr::<pg_sys::varlena>();
    if original.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(original) };
    if detoasted.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let detoasted_datum = pg_sys::Datum::from(detoasted);
    let result = f(detoasted_datum);
    if detoasted != original {
        unsafe { pg_sys::pfree(detoasted.cast()) };
    }
    result
}

#[cfg(test)]
pub(crate) fn with_detoasted_slot_datum<R, F>(
    datum: pg_sys::Datum,
    index: usize,
    f: F,
) -> Result<R, EncodeError>
where
    F: FnOnce(pg_sys::Datum) -> Result<R, EncodeError>,
{
    if datum.cast_mut_ptr::<pg_sys::varlena>().is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    f(datum)
}

#[cfg(not(test))]
pub(crate) fn database_encoding() -> i32 {
    unsafe { pg_sys::GetDatabaseEncoding() }
}

#[cfg(test)]
static TEST_DATABASE_ENCODING: AtomicI32 = AtomicI32::new(pg_sys::pg_enc::PG_UTF8 as i32);

#[cfg(test)]
pub(crate) fn database_encoding() -> i32 {
    TEST_DATABASE_ENCODING.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn set_test_database_encoding(encoding: i32) -> i32 {
    TEST_DATABASE_ENCODING.swap(encoding, Ordering::Relaxed)
}

pub(crate) unsafe fn read_bool(datum: pg_sys::Datum, byval: bool) -> bool {
    if byval {
        datum.value() != 0
    } else {
        unsafe { *datum.cast_mut_ptr::<bool>() }
    }
}

pub(crate) unsafe fn read_i16(datum: pg_sys::Datum, byval: bool) -> i16 {
    if byval {
        datum.value() as i16
    } else {
        unsafe { *datum.cast_mut_ptr::<i16>() }
    }
}

pub(crate) unsafe fn read_i32(datum: pg_sys::Datum, byval: bool) -> i32 {
    if byval {
        datum.value() as i32
    } else {
        unsafe { *datum.cast_mut_ptr::<i32>() }
    }
}

pub(crate) unsafe fn read_i64(datum: pg_sys::Datum, byval: bool) -> i64 {
    if byval {
        datum.value() as i64
    } else {
        unsafe { *datum.cast_mut_ptr::<i64>() }
    }
}

pub(crate) unsafe fn read_f32(datum: pg_sys::Datum, byval: bool) -> f32 {
    let bits = if byval {
        datum.value() as u32
    } else {
        unsafe { ptr::read(datum.cast_mut_ptr::<u32>()) }
    };
    f32::from_bits(bits)
}

pub(crate) unsafe fn read_f64(datum: pg_sys::Datum, byval: bool) -> f64 {
    let bits = if byval {
        datum.value() as u64
    } else {
        unsafe { ptr::read(datum.cast_mut_ptr::<u64>()) }
    };
    f64::from_bits(bits)
}

pub(crate) unsafe fn read_name_bytes<'a>(
    datum: pg_sys::Datum,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let bytes = unsafe { &(*ptr).data };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(unsafe { slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), end) })
}

pub(crate) unsafe fn read_fixed_bytes<'a>(
    datum: pg_sys::Datum,
    width: usize,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    Ok(unsafe { slice::from_raw_parts(ptr, width) })
}

pub(crate) unsafe fn read_packed_varlena<'a>(
    datum: pg_sys::Datum,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }

    let b0 = unsafe { *ptr };
    if varlena_is_1b(b0) {
        if varlena_is_1b_external(b0) {
            return Err(EncodeError::ExternalVarlena { index });
        }
        let total_len = varlena_1b_total_len(b0);
        if total_len == 0 {
            return Err(EncodeError::MalformedVarlena { index });
        }
        let data_len = total_len
            .checked_sub(1)
            .ok_or(EncodeError::MalformedVarlena { index })?;
        return Ok(unsafe { slice::from_raw_parts(ptr.add(1), data_len) });
    }

    let header = unsafe { ptr::read_unaligned(ptr.cast::<u32>()) };
    if varlena_is_4b_compressed(header) {
        return Err(EncodeError::CompressedVarlena { index });
    }
    let total_len = varlena_4b_total_len(header);
    if total_len < std::mem::size_of::<u32>() {
        return Err(EncodeError::MalformedVarlena { index });
    }
    let data_len = total_len - std::mem::size_of::<u32>();
    Ok(unsafe { slice::from_raw_parts(ptr.add(std::mem::size_of::<u32>()), data_len) })
}

#[cfg(target_endian = "little")]
fn varlena_is_1b(b0: u8) -> bool {
    (b0 & VARLENA_1B_FLAG) == VARLENA_1B_FLAG
}

#[cfg(target_endian = "big")]
fn varlena_is_1b(b0: u8) -> bool {
    (b0 & VARLENA_1B_FLAG) == VARLENA_1B_FLAG
}

#[cfg(target_endian = "little")]
fn varlena_is_1b_external(b0: u8) -> bool {
    b0 == VARLENA_1B_FLAG
}

#[cfg(target_endian = "big")]
fn varlena_is_1b_external(b0: u8) -> bool {
    b0 == VARLENA_1B_FLAG
}

#[cfg(target_endian = "little")]
fn varlena_1b_total_len(b0: u8) -> usize {
    (b0 as usize) >> 1
}

#[cfg(target_endian = "big")]
fn varlena_1b_total_len(b0: u8) -> usize {
    (b0 & 0x7F) as usize
}

#[cfg(target_endian = "little")]
fn varlena_is_4b_compressed(header: u32) -> bool {
    (header & VARLENA_4B_COMPRESSED_FLAG) == VARLENA_4B_COMPRESSED_FLAG
}

#[cfg(target_endian = "big")]
fn varlena_is_4b_compressed(header: u32) -> bool {
    (header & VARLENA_4B_COMPRESSED_FLAG) == VARLENA_4B_COMPRESSED_FLAG
}

#[cfg(target_endian = "little")]
fn varlena_4b_total_len(header: u32) -> usize {
    (header >> 2) as usize
}

#[cfg(target_endian = "big")]
fn varlena_4b_total_len(header: u32) -> usize {
    (header & 0x3FFF_FFFF) as usize
}
