use crate::datum::{
    database_encoding, pg_oid_needs_detoast, read_bool, read_f32, read_f64, read_fixed_bytes,
    read_i16, read_i32, read_i64, read_name_bytes, read_packed_varlena, validate_pg_layout_type,
    with_detoasted_slot_datum,
};
use crate::{ConfigError, EncodeError};
use arrow_layout::TypeTag;
use pgrx_pg_sys as pg_sys;
use row_encoder::{CellRef, PageRowEncoder, RowSource};
use std::ptr;

pub use row_encoder::{AppendStatus, EncodedBatch};

/// Direct writer from PostgreSQL `TupleTableSlot` rows into an
/// `arrow_layout` block.
///
/// `PageBatchEncoder` is the PostgreSQL adapter over `row_encoder`: it
/// validates the `TupleDesc`, deforms enough slot attributes, detoasts varlena
/// values, and feeds typed cells into the PostgreSQL-free page writer.
#[derive(Debug)]
pub struct PageBatchEncoder<'payload> {
    tuple_desc: pg_sys::TupleDesc,
    attrs_ptr: *mut pg_sys::FormData_pg_attribute,
    needed_attrs: i32,
    projection_ptr: *const usize,
    projection_len: usize,
    inner: PageRowEncoder<'payload>,
    accepted_slot_desc: Option<pg_sys::TupleDesc>,
}

impl<'payload> PageBatchEncoder<'payload> {
    /// Creates a new encoder over an initialized `arrow_layout` block.
    ///
    /// This validates that the target block and PostgreSQL `TupleDesc` have the
    /// same column count and compatible logical types.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must point to a valid PostgreSQL `TupleDescData` whose
    /// attribute array remains alive for the lifetime of the encoder.
    pub unsafe fn new(
        tuple_desc: pg_sys::TupleDesc,
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        unsafe { Self::new_inner(tuple_desc, None, payload) }
    }

    /// Creates a new encoder over a projected view of a PostgreSQL slot.
    ///
    /// `source_columns[output_index]` is the zero-based attribute index in the
    /// incoming slot that should be written into the corresponding output
    /// layout column. The slice must remain alive until the encoder is dropped.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must point to a valid PostgreSQL `TupleDescData` whose
    /// attribute array remains alive for the lifetime of the encoder.
    pub unsafe fn new_projected(
        tuple_desc: pg_sys::TupleDesc,
        source_columns: &[usize],
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        unsafe { Self::new_inner(tuple_desc, Some(source_columns), payload) }
    }

    unsafe fn new_inner(
        tuple_desc: pg_sys::TupleDesc,
        source_columns: Option<&[usize]>,
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        if tuple_desc.is_null() {
            return Err(ConfigError::NullTupleDesc);
        }

        let inner = PageRowEncoder::new(payload)?;
        let layout_cols = inner.column_count();
        let tuple_desc_cols = unsafe { (*tuple_desc).natts as usize };
        if let Some(source_columns) = source_columns {
            if layout_cols != source_columns.len() {
                return Err(ConfigError::ProjectionLengthMismatch {
                    layout_cols,
                    projection_cols: source_columns.len(),
                });
            }
        } else if layout_cols != tuple_desc_cols {
            return Err(ConfigError::ColumnCountMismatch {
                layout_cols,
                tuple_desc_cols,
            });
        }

        let attrs_ptr = unsafe { (*tuple_desc).attrs.as_mut_ptr() };
        let mut needs_utf8 = false;
        let mut max_needed_attr = 0usize;
        for index in 0..layout_cols {
            let source_index = source_columns.map_or(index, |columns| columns[index]);
            if source_index >= tuple_desc_cols {
                return Err(ConfigError::ProjectionIndexOutOfBounds {
                    index,
                    source_index,
                    tuple_desc_cols,
                });
            }

            let attr = unsafe { &*attrs_ptr.add(source_index) };
            if attr.attisdropped {
                return if source_columns.is_some() {
                    Err(ConfigError::ProjectedDroppedAttribute {
                        index,
                        source_index,
                    })
                } else {
                    Err(ConfigError::DroppedAttribute { index })
                };
            }

            let type_tag = inner.column_type_tag(index)?;
            validate_pg_layout_type(index, attr.atttypid, type_tag)?;
            if type_tag == TypeTag::Utf8View {
                needs_utf8 = true;
            }
            max_needed_attr = max_needed_attr.max(source_index + 1);
        }

        if needs_utf8 {
            let encoding = database_encoding();
            if encoding != pg_sys::pg_enc::PG_UTF8 as i32 {
                return Err(ConfigError::NonUtf8ServerEncoding { encoding });
            }
        }

        Ok(Self {
            tuple_desc,
            attrs_ptr,
            needed_attrs: i32::try_from(max_needed_attr)
                .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
            projection_ptr: source_columns.map_or(ptr::null(), |columns| columns.as_ptr()),
            projection_len: source_columns.map_or(0, <[usize]>::len),
            inner,
            accepted_slot_desc: None,
        })
    }

    /// Appends one row from a PostgreSQL `TupleTableSlot`.
    ///
    /// The slot may be undeformed or partially deformed; the encoder will ask
    /// PostgreSQL to deform enough attributes for the target layout when
    /// needed.
    ///
    /// Returns [`AppendStatus::Full`] when the row does not fit into the
    /// current block. In that case the caller should finalize the current
    /// block, allocate a fresh one, and retry the same slot there.
    pub fn append_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<AppendStatus, EncodeError> {
        self.append_slot_inner(slot)
    }

    fn append_slot_inner(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<AppendStatus, EncodeError> {
        if slot.is_null() {
            return Err(EncodeError::NullSlot);
        }
        let actual_tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        if actual_tuple_desc.is_null() {
            return Err(EncodeError::NullSlotTupleDesc);
        }
        self.validate_slot_tuple_desc(actual_tuple_desc)?;

        let needed_attrs = usize::try_from(self.needed_attrs)
            .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?;
        let valid = unsafe { (*slot).tts_nvalid as usize };
        if valid < needed_attrs {
            unsafe {
                pg_sys::slot_getsomeattrs_int(slot, self.needed_attrs);
            }
        }

        let values = unsafe { (*slot).tts_values };
        let isnulls = unsafe { (*slot).tts_isnull };
        if values.is_null() || isnulls.is_null() {
            return Err(EncodeError::InvalidSlotStorage);
        }

        let mut source = PgSlotRow {
            attrs_ptr: self.attrs_ptr,
            projection_ptr: self.projection_ptr,
            projection_len: self.projection_len,
            values,
            isnulls,
        };
        self.inner.append_row(&mut source)
    }

    fn source_index(&self, output_index: usize) -> usize {
        if self.projection_len == 0 {
            output_index
        } else {
            debug_assert!(output_index < self.projection_len);
            unsafe { *self.projection_ptr.add(output_index) }
        }
    }

    fn validate_slot_tuple_desc(
        &mut self,
        actual_tuple_desc: pg_sys::TupleDesc,
    ) -> Result<(), EncodeError> {
        if actual_tuple_desc == self.tuple_desc
            || self.accepted_slot_desc == Some(actual_tuple_desc)
        {
            return Ok(());
        }

        let actual_cols = unsafe { (*actual_tuple_desc).natts as usize };
        if self.projection_len == 0 && actual_cols != self.inner.column_count() {
            return Err(EncodeError::SlotTupleDescMismatch);
        }

        let actual_attrs_ptr = unsafe { (*actual_tuple_desc).attrs.as_mut_ptr() };
        for index in 0..self.inner.column_count() {
            let source_index = self.source_index(index);
            if source_index >= actual_cols {
                return Err(EncodeError::SlotTupleDescMismatch);
            }

            let expected = unsafe { &*self.attrs_ptr.add(source_index) };
            let actual = unsafe { &*actual_attrs_ptr.add(source_index) };
            if actual.attisdropped
                || actual.atttypid != expected.atttypid
                || actual.attlen != expected.attlen
                || actual.attbyval != expected.attbyval
            {
                return Err(EncodeError::SlotTupleDescMismatch);
            }
        }

        self.accepted_slot_desc = Some(actual_tuple_desc);
        Ok(())
    }

    /// Finalizes the block and returns the written row count and payload
    /// length.
    pub fn finish(self) -> Result<EncodedBatch, EncodeError> {
        self.inner.finish().map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn tail_cursor(&self) -> u32 {
        self.inner.tail_cursor_for_tests()
    }
}

struct PgSlotRow {
    attrs_ptr: *mut pg_sys::FormData_pg_attribute,
    projection_ptr: *const usize,
    projection_len: usize,
    values: *mut pg_sys::Datum,
    isnulls: *mut bool,
}

impl PgSlotRow {
    fn source_index(&self, output_index: usize) -> usize {
        if self.projection_len == 0 {
            output_index
        } else {
            debug_assert!(output_index < self.projection_len);
            unsafe { *self.projection_ptr.add(output_index) }
        }
    }
}

impl RowSource for PgSlotRow {
    type Error = EncodeError;

    fn with_cell<R>(
        &mut self,
        index: usize,
        f: impl FnOnce(CellRef<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        let source_idx = self.source_index(index);
        let attr = unsafe { &*self.attrs_ptr.add(source_idx) };
        let is_null = unsafe { *self.isnulls.add(source_idx) };
        if is_null {
            return f(CellRef::Null);
        }

        let datum = unsafe { *self.values.add(source_idx) };
        match attr.atttypid {
            oid if oid == pg_sys::BOOLOID => {
                f(CellRef::Boolean(unsafe { read_bool(datum, attr.attbyval) }))
            }
            oid if oid == pg_sys::INT2OID => {
                f(CellRef::Int16(unsafe { read_i16(datum, attr.attbyval) }))
            }
            oid if oid == pg_sys::INT4OID => {
                f(CellRef::Int32(unsafe { read_i32(datum, attr.attbyval) }))
            }
            oid if oid == pg_sys::INT8OID => {
                f(CellRef::Int64(unsafe { read_i64(datum, attr.attbyval) }))
            }
            oid if oid == pg_sys::FLOAT4OID => {
                f(CellRef::Float32(unsafe { read_f32(datum, attr.attbyval) }))
            }
            oid if oid == pg_sys::FLOAT8OID => {
                f(CellRef::Float64(unsafe { read_f64(datum, attr.attbyval) }))
            }
            oid if oid == pg_sys::UUIDOID => {
                let bytes = unsafe { read_fixed_bytes(datum, 16, index)? };
                f(CellRef::Uuid(bytes))
            }
            oid if oid == pg_sys::NAMEOID => {
                let bytes = unsafe { read_name_bytes(datum, index)? };
                f(CellRef::Utf8(bytes))
            }
            oid if pg_oid_needs_detoast(oid) => {
                with_detoasted_slot_datum(datum, index, |detoasted| {
                    let bytes = unsafe { read_packed_varlena(detoasted, index)? };
                    if oid == pg_sys::BYTEAOID {
                        f(CellRef::Binary(bytes))
                    } else {
                        f(CellRef::Utf8(bytes))
                    }
                })
            }
            _ => Err(EncodeError::UnsupportedRowAccess { index }),
        }
    }
}
