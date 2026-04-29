use crate::datum::{
    database_encoding, pg_oid_needs_detoast, read_bool, read_f32, read_f64, read_fixed_bytes,
    read_i16, read_i32, read_i64, read_name_bytes, read_packed_varlena, validate_pg_layout_type,
    with_detoasted_slot_datum,
};
use crate::{ConfigError, EncodeError};
use arrow_layout::TypeTag;
use pgrx_pg_sys as pg_sys;
use row_encoder::{CellRef, FixedWidthCell, FixedWidthRowSource, PageRowEncoder, RowSource};
use std::ptr;

pub use row_encoder::{AppendStatus, EncodedBatch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotIntKeyType {
    Int16,
    Int32,
    Int64,
}

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
    fixed_width_fast_path: bool,
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
        let mut fixed_width_fast_path = layout_cols > 0;
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
            if inner.column_is_nullable(index)?
                || !matches!(
                    type_tag,
                    TypeTag::Int16
                        | TypeTag::Int32
                        | TypeTag::Int64
                        | TypeTag::Float32
                        | TypeTag::Float64
                )
            {
                fixed_width_fast_path = false;
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
            fixed_width_fast_path,
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
            unsafe { ensure_slot_deformed(slot, self.needed_attrs)? };
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
        if self.fixed_width_fast_path {
            self.inner.append_fixed_width_row(&mut source)
        } else {
            self.inner.append_row(&mut source)
        }
    }

    pub fn needed_attrs(&self) -> i32 {
        self.needed_attrs
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

    #[cfg(test)]
    pub(crate) fn fixed_width_fast_path_for_tests(&self) -> bool {
        self.fixed_width_fast_path
    }
}

pub unsafe fn ensure_slot_deformed(
    slot: *mut pg_sys::TupleTableSlot,
    needed_attrs: i32,
) -> Result<(), EncodeError> {
    if needed_attrs <= 0 {
        return Ok(());
    }

    let ops = unsafe { (*slot).tts_ops };
    if ops.is_null() {
        return Err(EncodeError::SlotAttrOpsUnavailable {
            attnum: needed_attrs as usize,
        });
    }
    let Some(getsomeattrs) = (unsafe { (*ops).getsomeattrs }) else {
        return Err(EncodeError::SlotAttrOpsUnavailable {
            attnum: needed_attrs as usize,
        });
    };

    unsafe {
        getsomeattrs(slot, needed_attrs);
    }

    let valid = i32::from(unsafe { (*slot).tts_nvalid });
    if valid < needed_attrs {
        unsafe {
            pg_sys::slot_getmissingattrs(slot, valid, needed_attrs);
            (*slot).tts_nvalid = needed_attrs as i16;
        }
    }

    if i32::from(unsafe { (*slot).tts_nvalid }) < needed_attrs {
        return Err(EncodeError::SlotAttrAccess {
            attnum: needed_attrs as usize,
        });
    }

    Ok(())
}

pub unsafe fn read_int_key(
    slot: *mut pg_sys::TupleTableSlot,
    source_index: usize,
    key_type: SlotIntKeyType,
) -> Result<Option<i64>, EncodeError> {
    if slot.is_null() {
        return Err(EncodeError::NullSlot);
    }
    let tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
    if tuple_desc.is_null() {
        return Err(EncodeError::NullSlotTupleDesc);
    }
    let tuple_desc_cols = unsafe { (*tuple_desc).natts as usize };
    if source_index >= tuple_desc_cols {
        return Err(EncodeError::SlotAttrAccess {
            attnum: source_index + 1,
        });
    }

    let values = unsafe { (*slot).tts_values };
    let isnulls = unsafe { (*slot).tts_isnull };
    if values.is_null() || isnulls.is_null() {
        return Err(EncodeError::InvalidSlotStorage);
    }

    if unsafe { *isnulls.add(source_index) } {
        return Ok(None);
    }

    let attrs_ptr = unsafe { (*tuple_desc).attrs.as_mut_ptr() };
    let attr = unsafe { &*attrs_ptr.add(source_index) };
    let datum = unsafe { *values.add(source_index) };
    let value = match key_type {
        SlotIntKeyType::Int16 if attr.atttypid == pg_sys::INT2OID => unsafe {
            read_i16(datum, attr.attbyval) as i64
        },
        SlotIntKeyType::Int32 if attr.atttypid == pg_sys::INT4OID => unsafe {
            read_i32(datum, attr.attbyval) as i64
        },
        SlotIntKeyType::Int64 if attr.atttypid == pg_sys::INT8OID => unsafe {
            read_i64(datum, attr.attbyval)
        },
        _ => {
            return Err(EncodeError::UnsupportedRowAccess {
                index: source_index,
            })
        }
    };
    Ok(Some(value))
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

    fn write_cell<R>(
        &mut self,
        cell: CellRef<'_>,
        f: impl FnOnce(CellRef<'_>) -> Result<R, EncodeError>,
    ) -> Result<R, EncodeError> {
        f(cell)
    }
}

impl FixedWidthRowSource for PgSlotRow {
    type Error = EncodeError;

    fn fixed_width_cell(
        &mut self,
        index: usize,
        type_tag: TypeTag,
    ) -> Result<FixedWidthCell, Self::Error> {
        let source_idx = self.source_index(index);
        let attr = unsafe { &*self.attrs_ptr.add(source_idx) };
        let is_null = unsafe { *self.isnulls.add(source_idx) };
        if is_null {
            return Err(EncodeError::NullInNonNullableColumn { index });
        }

        let datum = unsafe { *self.values.add(source_idx) };
        let cell = match type_tag {
            TypeTag::Int16 => FixedWidthCell::Int16(unsafe { read_i16(datum, attr.attbyval) }),
            TypeTag::Int32 => FixedWidthCell::Int32(unsafe { read_i32(datum, attr.attbyval) }),
            TypeTag::Int64 => FixedWidthCell::Int64(unsafe { read_i64(datum, attr.attbyval) }),
            TypeTag::Float32 => FixedWidthCell::Float32(unsafe { read_f32(datum, attr.attbyval) }),
            TypeTag::Float64 => FixedWidthCell::Float64(unsafe { read_f64(datum, attr.attbyval) }),
            _ => return Err(EncodeError::UnsupportedRowAccess { index }),
        };
        Ok(cell)
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
            return self.write_cell(CellRef::Null, f);
        }

        let datum = unsafe { *self.values.add(source_idx) };
        match attr.atttypid {
            oid if oid == pg_sys::BOOLOID => self.write_cell(
                CellRef::Boolean(unsafe { read_bool(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::INT2OID => {
                self.write_cell(CellRef::Int16(unsafe { read_i16(datum, attr.attbyval) }), f)
            }
            oid if oid == pg_sys::INT4OID => {
                self.write_cell(CellRef::Int32(unsafe { read_i32(datum, attr.attbyval) }), f)
            }
            oid if oid == pg_sys::INT8OID => {
                self.write_cell(CellRef::Int64(unsafe { read_i64(datum, attr.attbyval) }), f)
            }
            oid if oid == pg_sys::FLOAT4OID => self.write_cell(
                CellRef::Float32(unsafe { read_f32(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::FLOAT8OID => self.write_cell(
                CellRef::Float64(unsafe { read_f64(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::UUIDOID => {
                let bytes = unsafe { read_fixed_bytes(datum, 16, index)? };
                self.write_cell(CellRef::Uuid(bytes), f)
            }
            oid if oid == pg_sys::NAMEOID => {
                let bytes = unsafe { read_name_bytes(datum, index)? };
                self.write_cell(CellRef::Utf8(bytes), f)
            }
            oid if pg_oid_needs_detoast(oid) => {
                with_detoasted_slot_datum(datum, index, |detoasted| {
                    let bytes = unsafe { read_packed_varlena(detoasted, index)? };
                    if oid == pg_sys::BYTEAOID {
                        self.write_cell(CellRef::Binary(bytes), f)
                    } else {
                        self.write_cell(CellRef::Utf8(bytes), f)
                    }
                })
            }
            _ => Err(EncodeError::UnsupportedRowAccess { index }),
        }
    }
}
