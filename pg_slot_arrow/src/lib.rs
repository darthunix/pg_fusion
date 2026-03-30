//! Producer-side direct writer for `page_arrow_layout` blocks over PostgreSQL
//! scan rows and `TupleTableSlot`s.
//!
//! The crate intentionally does not depend on `page_arrow`, `page_transfer`,
//! `storage`, or DataFusion. It only maps already-deformed PostgreSQL values
//! into a caller-initialized `page_arrow_layout` block.

mod error;

#[cfg(test)]
mod tests;

use error::oid_u32;
pub use error::{ConfigError, EncodeError};
use page_arrow_layout::constants::UUID_WIDTH_BYTES;
use page_arrow_layout::{BlockMut, TypeTag, ViewWriteStatus};
use pgrx_pg_sys as pg_sys;
use std::ptr;
use std::slice;

#[cfg(test)]
use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendStatus {
    Appended,
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedBatch {
    pub row_count: usize,
    pub payload_len: usize,
}

/// One-pass row access interface for direct page writers.
///
/// Implementations must keep any returned datum storage alive for the duration
/// of the callback only. This lets row sources reuse per-column scratch or
/// detoast buffers without the encoder retaining raw pointers across later
/// column reads.
pub trait RowDatumAccess {
    fn with_datum<R, F>(&mut self, col_idx: usize, f: F) -> Result<R, EncodeError>
    where
        F: FnOnce(pg_sys::Datum, bool) -> Result<R, EncodeError>;
}

#[derive(Debug)]
pub struct PageBatchEncoder<'payload> {
    tuple_desc: pg_sys::TupleDesc,
    block: BlockMut<'payload>,
}

impl<'payload> PageBatchEncoder<'payload> {
    /// # Safety
    ///
    /// `tuple_desc` must point to a valid PostgreSQL `TupleDescData` whose
    /// attribute array remains alive for the lifetime of the encoder.
    pub unsafe fn new(
        tuple_desc: pg_sys::TupleDesc,
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        if tuple_desc.is_null() {
            return Err(ConfigError::NullTupleDesc);
        }

        let block = BlockMut::open(payload)?;
        let layout_cols = block.column_count();
        let tuple_desc_cols = unsafe { (*tuple_desc).natts as usize };
        if layout_cols != tuple_desc_cols {
            return Err(ConfigError::ColumnCountMismatch {
                layout_cols,
                tuple_desc_cols,
            });
        }

        let mut needs_utf8 = false;
        for index in 0..layout_cols {
            let attr = unsafe { &*(*tuple_desc).attrs.as_ptr().add(index) };
            if attr.attisdropped {
                return Err(ConfigError::DroppedAttribute { index });
            }

            let layout = block.column_layout(index)?;
            validate_pg_layout_type(index, attr.atttypid, layout.type_tag)?;
            if layout.type_tag == TypeTag::Utf8View {
                needs_utf8 = true;
            }
        }

        if needs_utf8 {
            let encoding = database_encoding();
            if encoding != pg_sys::pg_enc::PG_UTF8 as i32 {
                return Err(ConfigError::NonUtf8ServerEncoding { encoding });
            }
        }

        Ok(Self { tuple_desc, block })
    }

    pub fn append_row<R>(&mut self, row: &mut R) -> Result<AppendStatus, EncodeError>
    where
        R: RowDatumAccess,
    {
        let row_idx = self.block.row_count();
        if row_idx >= self.block.max_rows() {
            return Ok(AppendStatus::Full);
        }

        let tail_before = self.block.tail_cursor();
        for col_idx in 0..self.block.column_count() {
            let result = row.with_datum(col_idx, |datum, is_null| {
                self.write_column_value(col_idx, row_idx, datum, is_null)
            });
            match result {
                Ok(CellWrite::Written) => {}
                Ok(CellWrite::Full) => {
                    self.block.rollback_tail(tail_before)?;
                    return Ok(AppendStatus::Full);
                }
                Err(error) => {
                    self.block.rollback_tail(tail_before)?;
                    return Err(error);
                }
            }
        }

        self.block.commit_current_row()?;
        Ok(AppendStatus::Appended)
    }

    pub fn append_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<AppendStatus, EncodeError> {
        let mut row = SlotRowAccess::new(slot, self.tuple_desc, self.block.column_count())?;
        self.append_row(&mut row)
    }

    pub fn finish(self) -> Result<EncodedBatch, EncodeError> {
        self.block.validate()?;
        Ok(EncodedBatch {
            row_count: usize::try_from(self.block.row_count())
                .map_err(|_| page_arrow_layout::LayoutError::SizeOverflow)?,
            payload_len: usize::try_from(self.block.block_size())
                .map_err(|_| page_arrow_layout::LayoutError::SizeOverflow)?,
        })
    }

    fn write_column_value(
        &mut self,
        index: usize,
        row_idx: u32,
        datum: pg_sys::Datum,
        is_null: bool,
    ) -> Result<CellWrite, EncodeError> {
        let layout = self.block.column_layout(index)?;
        if is_null {
            if !layout.flags.is_nullable() {
                return Err(EncodeError::NullInNonNullableColumn { index });
            }
            self.block.write_null(index, row_idx)?;
            return Ok(CellWrite::Written);
        }

        match layout.type_tag {
            TypeTag::Boolean => {
                let value = unsafe { read_bool(datum, self.attr(index)?.attbyval) };
                self.block.write_bool(index, row_idx, value)?;
                Ok(CellWrite::Written)
            }
            TypeTag::Int16 => {
                let value = unsafe { read_i16(datum, self.attr(index)?.attbyval) };
                self.write_fixed(index, row_idx, &value.to_le_bytes())
            }
            TypeTag::Int32 => {
                let value = unsafe { read_i32(datum, self.attr(index)?.attbyval) };
                self.write_fixed(index, row_idx, &value.to_le_bytes())
            }
            TypeTag::Int64 => {
                let value = unsafe { read_i64(datum, self.attr(index)?.attbyval) };
                self.write_fixed(index, row_idx, &value.to_le_bytes())
            }
            TypeTag::Float32 => {
                let bits = unsafe { read_f32(datum, self.attr(index)?.attbyval) }.to_bits();
                self.write_fixed(index, row_idx, &bits.to_le_bytes())
            }
            TypeTag::Float64 => {
                let bits = unsafe { read_f64(datum, self.attr(index)?.attbyval) }.to_bits();
                self.write_fixed(index, row_idx, &bits.to_le_bytes())
            }
            TypeTag::Uuid => {
                let bytes = unsafe { read_fixed_bytes(datum, UUID_WIDTH_BYTES as usize, index)? };
                self.write_fixed(index, row_idx, bytes)
            }
            TypeTag::Utf8View => {
                let bytes = if self.attr(index)?.atttypid == pg_sys::NAMEOID {
                    unsafe { read_name_bytes(datum, index)? }
                } else {
                    unsafe { read_packed_varlena(datum, index)? }
                };
                std::str::from_utf8(bytes)
                    .map_err(|source| EncodeError::InvalidUtf8 { index, source })?;
                self.write_view(index, row_idx, bytes)
            }
            TypeTag::BinaryView => {
                let bytes = unsafe { read_packed_varlena(datum, index)? };
                self.write_view(index, row_idx, bytes)
            }
        }
    }

    fn write_fixed(
        &mut self,
        index: usize,
        row_idx: u32,
        bytes: &[u8],
    ) -> Result<CellWrite, EncodeError> {
        self.block.write_fixed(index, row_idx, bytes)?;
        Ok(CellWrite::Written)
    }

    fn write_view(
        &mut self,
        index: usize,
        row_idx: u32,
        bytes: &[u8],
    ) -> Result<CellWrite, EncodeError> {
        if bytes.len() > i32::MAX as usize {
            return Err(EncodeError::RowValueTooLarge {
                index,
                len: bytes.len(),
            });
        }
        match self.block.write_view_bytes(index, row_idx, bytes)? {
            ViewWriteStatus::Written => Ok(CellWrite::Written),
            ViewWriteStatus::Full => Ok(CellWrite::Full),
        }
    }

    fn attr(&self, index: usize) -> Result<&pg_sys::FormData_pg_attribute, EncodeError> {
        Ok(unsafe { &*(*self.tuple_desc).attrs.as_ptr().add(index) })
    }
}

enum CellWrite {
    Written,
    Full,
}

struct SlotRowAccess {
    slot: *mut pg_sys::TupleTableSlot,
    tuple_desc: pg_sys::TupleDesc,
    needed: usize,
}

impl SlotRowAccess {
    fn new(
        slot: *mut pg_sys::TupleTableSlot,
        tuple_desc: pg_sys::TupleDesc,
        needed: usize,
    ) -> Result<Self, EncodeError> {
        if slot.is_null() {
            return Err(EncodeError::NullSlot);
        }
        let actual_tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        if actual_tuple_desc.is_null() {
            return Err(EncodeError::NullSlotTupleDesc);
        }
        if actual_tuple_desc != tuple_desc {
            return Err(EncodeError::SlotTupleDescMismatch);
        }
        Ok(Self {
            slot,
            tuple_desc: actual_tuple_desc,
            needed,
        })
    }

    fn ensure_deformed(&mut self) -> Result<(), EncodeError> {
        let valid = unsafe { (*self.slot).tts_nvalid as usize };
        if valid >= self.needed {
            return Ok(());
        }
        unsafe {
            pg_sys::slot_getsomeattrs_int(
                self.slot,
                i32::try_from(self.needed).map_err(|_| EncodeError::SlotAttrAccess {
                    attnum: self.needed,
                })?,
            );
        }
        Ok(())
    }
}

impl RowDatumAccess for SlotRowAccess {
    fn with_datum<R, F>(&mut self, col_idx: usize, f: F) -> Result<R, EncodeError>
    where
        F: FnOnce(pg_sys::Datum, bool) -> Result<R, EncodeError>,
    {
        self.ensure_deformed()?;

        let values = unsafe { (*self.slot).tts_values };
        let isnulls = unsafe { (*self.slot).tts_isnull };
        if values.is_null() || isnulls.is_null() {
            return Err(EncodeError::InvalidSlotStorage);
        }

        let is_null = unsafe { *isnulls.add(col_idx) };
        let datum = unsafe { *values.add(col_idx) };
        if is_null {
            return f(datum, true);
        }

        let attr = unsafe { &*(*self.tuple_desc).attrs.as_ptr().add(col_idx) };
        if attr.atttypid == pg_sys::NAMEOID || !pg_oid_needs_detoast(attr.atttypid) {
            return f(datum, false);
        }

        let original = datum.cast_mut_ptr::<pg_sys::varlena>();
        if original.is_null() {
            return Err(EncodeError::NullDatumPointer { index: col_idx });
        }
        let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(original) };
        if detoasted.is_null() {
            return Err(EncodeError::NullDatumPointer { index: col_idx });
        }
        let detoasted_datum = pg_sys::Datum::from(detoasted);
        let result = f(detoasted_datum, false);
        if detoasted != original {
            unsafe { pg_sys::pfree(detoasted.cast()) };
        }
        result
    }
}

fn validate_pg_layout_type(
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

fn pg_oid_needs_detoast(oid: pg_sys::Oid) -> bool {
    oid == pg_sys::TEXTOID
        || oid == pg_sys::VARCHAROID
        || oid == pg_sys::BPCHAROID
        || oid == pg_sys::BYTEAOID
}

#[cfg(not(test))]
fn database_encoding() -> i32 {
    unsafe { pg_sys::GetDatabaseEncoding() }
}

#[cfg(test)]
static TEST_DATABASE_ENCODING: AtomicI32 = AtomicI32::new(pg_sys::pg_enc::PG_UTF8 as i32);

#[cfg(test)]
fn database_encoding() -> i32 {
    TEST_DATABASE_ENCODING.load(Ordering::Relaxed)
}

#[cfg(test)]
fn set_test_database_encoding(encoding: i32) -> i32 {
    TEST_DATABASE_ENCODING.swap(encoding, Ordering::Relaxed)
}

unsafe fn read_bool(datum: pg_sys::Datum, byval: bool) -> bool {
    if byval {
        datum.value() != 0
    } else {
        *datum.cast_mut_ptr::<bool>()
    }
}

unsafe fn read_i16(datum: pg_sys::Datum, byval: bool) -> i16 {
    if byval {
        datum.value() as i16
    } else {
        *datum.cast_mut_ptr::<i16>()
    }
}

unsafe fn read_i32(datum: pg_sys::Datum, byval: bool) -> i32 {
    if byval {
        datum.value() as i32
    } else {
        *datum.cast_mut_ptr::<i32>()
    }
}

unsafe fn read_i64(datum: pg_sys::Datum, byval: bool) -> i64 {
    if byval {
        datum.value() as i64
    } else {
        *datum.cast_mut_ptr::<i64>()
    }
}

unsafe fn read_f32(datum: pg_sys::Datum, byval: bool) -> f32 {
    let bits = if byval {
        datum.value() as u32
    } else {
        ptr::read(datum.cast_mut_ptr::<u32>())
    };
    f32::from_bits(bits)
}

unsafe fn read_f64(datum: pg_sys::Datum, byval: bool) -> f64 {
    let bits = if byval {
        datum.value() as u64
    } else {
        ptr::read(datum.cast_mut_ptr::<u64>())
    };
    f64::from_bits(bits)
}

unsafe fn read_name_bytes<'a>(datum: pg_sys::Datum, index: usize) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    let bytes = &(*ptr).data;
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    Ok(slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), end))
}

unsafe fn read_fixed_bytes<'a>(
    datum: pg_sys::Datum,
    width: usize,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }
    Ok(slice::from_raw_parts(ptr, width))
}

unsafe fn read_packed_varlena<'a>(
    datum: pg_sys::Datum,
    index: usize,
) -> Result<&'a [u8], EncodeError> {
    let ptr = datum.cast_mut_ptr::<u8>();
    if ptr.is_null() {
        return Err(EncodeError::NullDatumPointer { index });
    }

    let b0 = *ptr;
    if (b0 & 0x01) == 0x01 {
        if b0 == 0x01 {
            return Err(EncodeError::ExternalVarlena { index });
        }
        let total_len = (b0 as usize) >> 1;
        if total_len == 0 {
            return Err(EncodeError::MalformedVarlena { index });
        }
        let data_len = total_len
            .checked_sub(1)
            .ok_or(EncodeError::MalformedVarlena { index })?;
        return Ok(slice::from_raw_parts(ptr.add(1), data_len));
    }

    let header = ptr::read_unaligned(ptr.cast::<u32>());
    if (header & 0x02) != 0 {
        return Err(EncodeError::CompressedVarlena { index });
    }
    let total_len = (header >> 2) as usize;
    if total_len < std::mem::size_of::<u32>() {
        return Err(EncodeError::MalformedVarlena { index });
    }
    let data_len = total_len - std::mem::size_of::<u32>();
    Ok(slice::from_raw_parts(
        ptr.add(std::mem::size_of::<u32>()),
        data_len,
    ))
}
