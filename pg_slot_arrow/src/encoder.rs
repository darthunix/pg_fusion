use crate::bitmap::{bitmap_get_raw, bitmap_set_raw};
use crate::datum::{
    database_encoding, pg_oid_needs_detoast, read_bool, read_f32, read_f64, read_fixed_bytes,
    read_i16, read_i32, read_i64, read_name_bytes, read_packed_varlena, validate_pg_layout_type,
    with_detoasted_slot_datum,
};
use crate::{ConfigError, EncodeError};
use page_arrow_layout::constants::{UUID_WIDTH_BYTES, VIEW_INLINE_LEN};
use page_arrow_layout::raw::{BlockHeader, ColumnDesc};
use page_arrow_layout::{BlockRef, ByteView, TypeTag};
use pgrx_pg_sys as pg_sys;
use std::mem::size_of;
use std::ptr;

/// Result of trying to append a slot to the current block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendStatus {
    /// The row was written successfully.
    Appended,
    /// The current row did not fit and must be retried on a fresh block.
    Full,
}

/// Metadata returned after finalizing an encoded block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedBatch {
    /// Number of rows successfully written into the block.
    pub row_count: usize,
    /// Number of payload bytes occupied by the finalized block.
    pub payload_len: usize,
}

/// Direct writer from PostgreSQL `TupleTableSlot` rows into a
/// `page_arrow_layout` block.
///
/// The encoder assumes the payload already contains a valid initialized block
/// produced by `page_arrow_layout`. It validates that the block layout matches
/// the supplied PostgreSQL `TupleDesc`, then appends rows in-place without
/// building Rust-side column buffers.
#[derive(Debug)]
pub struct PageBatchEncoder<'payload> {
    tuple_desc: pg_sys::TupleDesc,
    attrs_ptr: *mut pg_sys::FormData_pg_attribute,
    col_count: usize,
    needed_attrs: i32,
    payload: &'payload mut [u8],
    block_ptr: *mut u8,
    descs_ptr: *mut ColumnDesc,
    header: BlockHeader,
}

impl<'payload> PageBatchEncoder<'payload> {
    /// Creates a new encoder over an initialized `page_arrow_layout` block.
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
        if tuple_desc.is_null() {
            return Err(ConfigError::NullTupleDesc);
        }

        let block = BlockRef::open(&*payload)?;
        let layout_cols = block.column_count();
        let tuple_desc_cols = unsafe { (*tuple_desc).natts as usize };
        if layout_cols != tuple_desc_cols {
            return Err(ConfigError::ColumnCountMismatch {
                layout_cols,
                tuple_desc_cols,
            });
        }

        let attrs_ptr = unsafe { (*tuple_desc).attrs.as_mut_ptr() };
        let block_ptr = payload.as_mut_ptr();
        let descs_ptr = unsafe { block_ptr.add(size_of::<BlockHeader>()).cast::<ColumnDesc>() };
        let header = unsafe { ptr::read_unaligned(block_ptr.cast::<BlockHeader>()) };
        let mut needs_utf8 = false;
        for index in 0..layout_cols {
            let attr = unsafe { &*attrs_ptr.add(index) };
            if attr.attisdropped {
                return Err(ConfigError::DroppedAttribute { index });
            }

            let desc = unsafe { ptr::read_unaligned(descs_ptr.add(index)) };
            let type_tag = desc.type_tag()?;
            validate_pg_layout_type(index, attr.atttypid, type_tag)?;
            if type_tag == TypeTag::Utf8View {
                needs_utf8 = true;
            }
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
            col_count: layout_cols,
            needed_attrs: i32::try_from(layout_cols)
                .map_err(|_| page_arrow_layout::LayoutError::SizeOverflow)?,
            payload,
            block_ptr,
            descs_ptr,
            header,
        })
    }

    /// Appends one row from a PostgreSQL `TupleTableSlot`.
    ///
    /// The slot may be undeformed or partially deformed; the encoder will ask
    /// PostgreSQL to deform enough attributes for the target layout when needed.
    ///
    /// Returns [`AppendStatus::Full`] when the row does not fit into the
    /// current block. In that case the caller should finalize the current block,
    /// allocate a fresh one, and retry the same slot there.
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
        if actual_tuple_desc != self.tuple_desc {
            return Err(EncodeError::SlotTupleDescMismatch);
        }

        let row_idx = self.header.row_count;
        if row_idx >= self.header.max_rows {
            return Ok(AppendStatus::Full);
        }

        let valid = unsafe { (*slot).tts_nvalid as usize };
        if valid < self.col_count {
            unsafe {
                pg_sys::slot_getsomeattrs_int(slot, self.needed_attrs);
            }
        }

        let values = unsafe { (*slot).tts_values };
        let isnulls = unsafe { (*slot).tts_isnull };
        if values.is_null() || isnulls.is_null() {
            return Err(EncodeError::InvalidSlotStorage);
        }

        let tail_before = self.header.tail_cursor;
        let mut processed_cols = 0usize;
        for col_idx in 0..self.col_count {
            let desc = self.desc(col_idx);
            let attr = unsafe { &*self.attrs_ptr.add(col_idx) };
            let is_null = unsafe { *isnulls.add(col_idx) };
            let datum = unsafe { *values.add(col_idx) };
            let result = if is_null
                || attr.atttypid == pg_sys::NAMEOID
                || !pg_oid_needs_detoast(attr.atttypid)
            {
                self.write_column_value(col_idx, row_idx, attr, desc, datum, is_null)
            } else {
                with_detoasted_slot_datum(datum, col_idx, |detoasted| {
                    self.write_column_value(col_idx, row_idx, attr, desc, detoasted, false)
                })
            };

            match result {
                Ok(CellWrite::Written) => {
                    processed_cols += 1;
                }
                Ok(CellWrite::Full) => {
                    self.rollback_row(row_idx, processed_cols, tail_before)?;
                    return Ok(AppendStatus::Full);
                }
                Err(error) => {
                    self.rollback_row(row_idx, processed_cols, tail_before)?;
                    return Err(error);
                }
            }
        }

        self.header.row_count = row_idx + 1;
        Ok(AppendStatus::Appended)
    }

    /// Finalizes the block and returns the written row count and payload length.
    ///
    /// This writes the encoder's local header state back into the payload and
    /// reopens the block through `page_arrow_layout` to validate the final
    /// structure.
    pub fn finish(mut self) -> Result<EncodedBatch, EncodeError> {
        self.write_header()?;
        BlockRef::open(&*self.payload)?;
        Ok(EncodedBatch {
            row_count: usize::try_from(self.header.row_count)
                .map_err(|_| page_arrow_layout::LayoutError::SizeOverflow)?,
            payload_len: usize::try_from(self.header.block_size)
                .map_err(|_| page_arrow_layout::LayoutError::SizeOverflow)?,
        })
    }

    fn write_column_value(
        &mut self,
        index: usize,
        row_idx: u32,
        attr: &pg_sys::FormData_pg_attribute,
        desc: ColumnDesc,
        datum: pg_sys::Datum,
        is_null: bool,
    ) -> Result<CellWrite, EncodeError> {
        if is_null {
            if !desc.flags().is_nullable() {
                return Err(EncodeError::NullInNonNullableColumn { index });
            }
            self.write_null(index, row_idx, desc)?;
            return Ok(CellWrite::Written);
        }

        match desc.type_tag {
            raw if raw == TypeTag::Boolean.to_raw() => {
                let value = unsafe { read_bool(datum, attr.attbyval) };
                self.write_bool(row_idx, desc, value);
                Ok(CellWrite::Written)
            }
            raw if raw == TypeTag::Int16.to_raw() => {
                let value = unsafe { read_i16(datum, attr.attbyval) };
                self.write_fixed(row_idx, desc, &value.to_le_bytes())
            }
            raw if raw == TypeTag::Int32.to_raw() => {
                let value = unsafe { read_i32(datum, attr.attbyval) };
                self.write_fixed(row_idx, desc, &value.to_le_bytes())
            }
            raw if raw == TypeTag::Int64.to_raw() => {
                let value = unsafe { read_i64(datum, attr.attbyval) };
                self.write_fixed(row_idx, desc, &value.to_le_bytes())
            }
            raw if raw == TypeTag::Float32.to_raw() => {
                let bits = unsafe { read_f32(datum, attr.attbyval) }.to_bits();
                self.write_fixed(row_idx, desc, &bits.to_le_bytes())
            }
            raw if raw == TypeTag::Float64.to_raw() => {
                let bits = unsafe { read_f64(datum, attr.attbyval) }.to_bits();
                self.write_fixed(row_idx, desc, &bits.to_le_bytes())
            }
            raw if raw == TypeTag::Uuid.to_raw() => {
                let bytes = unsafe { read_fixed_bytes(datum, UUID_WIDTH_BYTES as usize, index)? };
                self.write_fixed(row_idx, desc, bytes)
            }
            raw if raw == TypeTag::Utf8View.to_raw() => {
                let bytes = if attr.atttypid == pg_sys::NAMEOID {
                    unsafe { read_name_bytes(datum, index)? }
                } else {
                    unsafe { read_packed_varlena(datum, index)? }
                };
                self.write_view(index, row_idx, desc, bytes)
            }
            raw if raw == TypeTag::BinaryView.to_raw() => {
                let bytes = unsafe { read_packed_varlena(datum, index)? };
                self.write_view(index, row_idx, desc, bytes)
            }
            raw => Err(page_arrow_layout::LayoutError::InvalidTypeTag { raw }.into()),
        }
    }

    fn write_fixed(
        &mut self,
        row_idx: u32,
        desc: ColumnDesc,
        bytes: &[u8],
    ) -> Result<CellWrite, EncodeError> {
        self.write_validity(row_idx, desc, true);
        self.write_fixed_bytes(row_idx, desc, bytes)?;
        Ok(CellWrite::Written)
    }

    fn write_view(
        &mut self,
        index: usize,
        row_idx: u32,
        desc: ColumnDesc,
        bytes: &[u8],
    ) -> Result<CellWrite, EncodeError> {
        if bytes.len() > i32::MAX as usize {
            return Err(EncodeError::RowValueTooLarge {
                index,
                len: bytes.len(),
            });
        }
        if bytes.len() <= VIEW_INLINE_LEN {
            self.write_validity(row_idx, desc, true);
            self.write_view_slot(row_idx, desc, ByteView::new_inline(bytes)?)?;
            return Ok(CellWrite::Written);
        }

        let len =
            u32::try_from(bytes.len()).map_err(|_| page_arrow_layout::LayoutError::SizeOverflow)?;
        let Some(start) = self.tail_alloc(len)? else {
            return Ok(CellWrite::Full);
        };
        self.tail_bytes_mut(start, len).copy_from_slice(bytes);
        self.write_validity(row_idx, desc, true);
        self.write_view_slot(
            row_idx,
            desc,
            ByteView::new_outline(bytes, start - self.header.pool_base)?,
        )?;
        Ok(CellWrite::Written)
    }

    fn rollback_row(
        &mut self,
        row_idx: u32,
        processed_cols: usize,
        tail_before: u32,
    ) -> Result<(), EncodeError> {
        self.header.tail_cursor = tail_before;
        for index in 0..processed_cols {
            let desc = self.desc(index);
            if desc.flags().is_nullable() && !self.validity(row_idx, desc) {
                self.decrement_null_count(index, desc)?;
            }
        }
        Ok(())
    }

    fn write_null(
        &mut self,
        index: usize,
        row_idx: u32,
        desc: ColumnDesc,
    ) -> Result<(), EncodeError> {
        self.write_validity(row_idx, desc, false);
        match desc.type_tag {
            raw if raw == TypeTag::Boolean.to_raw() => self.write_bool_value(row_idx, desc, false),
            raw if raw == TypeTag::Int16.to_raw() => self.zero_value_slot(row_idx, desc, 2),
            raw if raw == TypeTag::Int32.to_raw() || raw == TypeTag::Float32.to_raw() => {
                self.zero_value_slot(row_idx, desc, 4)
            }
            raw if raw == TypeTag::Int64.to_raw() || raw == TypeTag::Float64.to_raw() => {
                self.zero_value_slot(row_idx, desc, 8)
            }
            raw if raw == TypeTag::Uuid.to_raw()
                || raw == TypeTag::Utf8View.to_raw()
                || raw == TypeTag::BinaryView.to_raw() =>
            {
                self.zero_value_slot(row_idx, desc, 16)
            }
            raw => return Err(page_arrow_layout::LayoutError::InvalidTypeTag { raw }.into()),
        }
        self.increment_null_count(index, desc)?;
        Ok(())
    }

    fn desc(&self, index: usize) -> ColumnDesc {
        unsafe { ptr::read_unaligned(self.descs_ptr.add(index)) }
    }

    fn write_desc(&mut self, index: usize, desc: ColumnDesc) {
        unsafe { ptr::write_unaligned(self.descs_ptr.add(index), desc) };
    }

    fn write_header(&mut self) -> Result<(), EncodeError> {
        unsafe { ptr::write_unaligned(self.block_ptr.cast::<BlockHeader>(), self.header) };
        Ok(())
    }

    fn increment_null_count(
        &mut self,
        index: usize,
        mut desc: ColumnDesc,
    ) -> Result<(), EncodeError> {
        desc.null_count = desc
            .null_count
            .checked_add(1)
            .ok_or(page_arrow_layout::LayoutError::SizeOverflow)?;
        self.write_desc(index, desc);
        Ok(())
    }

    fn decrement_null_count(
        &mut self,
        index: usize,
        mut desc: ColumnDesc,
    ) -> Result<(), EncodeError> {
        desc.null_count = desc
            .null_count
            .checked_sub(1)
            .ok_or(page_arrow_layout::LayoutError::SizeOverflow)?;
        self.write_desc(index, desc);
        Ok(())
    }

    fn tail_alloc(&mut self, len: u32) -> Result<Option<u32>, EncodeError> {
        if len == 0 {
            return Ok(Some(self.header.tail_cursor));
        }
        let next = self
            .header
            .tail_cursor
            .checked_sub(len)
            .ok_or(page_arrow_layout::LayoutError::SizeOverflow)?;
        if next < self.header.pool_base {
            return Ok(None);
        }
        self.header.tail_cursor = next;
        Ok(Some(next))
    }

    fn tail_bytes_mut(&mut self, start: u32, len: u32) -> &mut [u8] {
        let start = start as usize;
        let end = start + len as usize;
        &mut self.payload[start..end]
    }

    fn write_validity(&mut self, row_idx: u32, desc: ColumnDesc, valid: bool) {
        unsafe {
            bitmap_set_raw(
                self.block_ptr.add(desc.validity_off as usize),
                row_idx,
                valid,
            )
        };
    }

    fn validity(&self, row_idx: u32, desc: ColumnDesc) -> bool {
        unsafe {
            bitmap_get_raw(
                self.block_ptr.add(desc.validity_off as usize).cast_const(),
                row_idx,
            )
        }
    }

    fn write_bool(&mut self, row_idx: u32, desc: ColumnDesc, value: bool) {
        self.write_validity(row_idx, desc, true);
        self.write_bool_value(row_idx, desc, value);
    }

    fn write_bool_value(&mut self, row_idx: u32, desc: ColumnDesc, value: bool) {
        unsafe { bitmap_set_raw(self.block_ptr.add(desc.values_off as usize), row_idx, value) };
    }

    fn write_fixed_bytes(
        &mut self,
        row_idx: u32,
        desc: ColumnDesc,
        bytes: &[u8],
    ) -> Result<(), EncodeError> {
        let width = match desc.type_tag {
            raw if raw == TypeTag::Int16.to_raw() => 2usize,
            raw if raw == TypeTag::Int32.to_raw() || raw == TypeTag::Float32.to_raw() => 4usize,
            raw if raw == TypeTag::Int64.to_raw() || raw == TypeTag::Float64.to_raw() => 8usize,
            raw if raw == TypeTag::Uuid.to_raw() => 16usize,
            raw => return Err(page_arrow_layout::LayoutError::InvalidTypeTag { raw }.into()),
        };
        if bytes.len() != width {
            return Err(page_arrow_layout::LayoutError::InvalidHeaderBounds.into());
        }
        let start = desc.values_off as usize + (row_idx as usize * width);
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), self.block_ptr.add(start), width);
        }
        Ok(())
    }

    fn zero_value_slot(&mut self, row_idx: u32, desc: ColumnDesc, width: usize) {
        let start = desc.values_off as usize + (row_idx as usize * width);
        unsafe {
            ptr::write_bytes(self.block_ptr.add(start), 0, width);
        }
    }

    fn write_view_slot(
        &mut self,
        row_idx: u32,
        desc: ColumnDesc,
        view: ByteView,
    ) -> Result<(), EncodeError> {
        let start = desc.values_off as usize + (row_idx as usize * size_of::<ByteView>());
        unsafe {
            ptr::write_unaligned(self.block_ptr.add(start).cast::<ByteView>(), view);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn tail_cursor(&self) -> u32 {
        self.header.tail_cursor
    }
}

enum CellWrite {
    Written,
    Full,
}
