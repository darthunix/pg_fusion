use crate::bitmap::{bitmap_get_raw, bitmap_set_raw};
use crate::RowEncodeError;
use arrow_layout::constants::{UUID_WIDTH_BYTES, VIEW_INLINE_LEN};
use arrow_layout::raw::{BlockHeader, ColumnDesc};
use arrow_layout::{BlockRef, ByteView, LayoutError, TypeTag};
use std::mem::size_of;
use std::ptr;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellType {
    Null,
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Uuid,
    Utf8,
    Binary,
}

#[derive(Clone, Copy, Debug)]
pub enum CellRef<'a> {
    Null,
    Boolean(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Uuid(&'a [u8]),
    Utf8(&'a [u8]),
    Binary(&'a [u8]),
}

impl CellRef<'_> {
    fn cell_type(self) -> CellType {
        match self {
            Self::Null => CellType::Null,
            Self::Boolean(_) => CellType::Boolean,
            Self::Int16(_) => CellType::Int16,
            Self::Int32(_) => CellType::Int32,
            Self::Int64(_) => CellType::Int64,
            Self::Float32(_) => CellType::Float32,
            Self::Float64(_) => CellType::Float64,
            Self::Uuid(_) => CellType::Uuid,
            Self::Utf8(_) => CellType::Utf8,
            Self::Binary(_) => CellType::Binary,
        }
    }
}

pub trait RowSource {
    type Error: From<RowEncodeError>;

    fn with_cell<R>(
        &mut self,
        index: usize,
        f: impl FnOnce(CellRef<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error>;
}

#[derive(Debug)]
pub struct PageRowEncoder<'payload> {
    payload: &'payload mut [u8],
    block_ptr: *mut u8,
    descs_ptr: *mut ColumnDesc,
    header: BlockHeader,
    col_count: usize,
}

impl<'payload> PageRowEncoder<'payload> {
    pub fn new(payload: &'payload mut [u8]) -> Result<Self, LayoutError> {
        let block = BlockRef::open(&*payload)?;
        let col_count = block.column_count();
        let block_ptr = payload.as_mut_ptr();
        let descs_ptr = unsafe { block_ptr.add(size_of::<BlockHeader>()).cast::<ColumnDesc>() };
        let header = unsafe { ptr::read_unaligned(block_ptr.cast::<BlockHeader>()) };
        Ok(Self {
            payload,
            block_ptr,
            descs_ptr,
            header,
            col_count,
        })
    }

    pub fn column_count(&self) -> usize {
        self.col_count
    }

    pub fn column_type_tag(&self, index: usize) -> Result<TypeTag, LayoutError> {
        if index >= self.col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds {
                index,
                col_count: self.col_count,
            });
        }
        self.desc(index).type_tag()
    }

    pub fn append_row<S>(&mut self, source: &mut S) -> Result<AppendStatus, S::Error>
    where
        S: RowSource,
    {
        let row_idx = self.header.row_count;
        if row_idx >= self.header.max_rows {
            return Ok(AppendStatus::Full);
        }

        if let Some(status) = self.try_append_single_fixed_width_row(row_idx, source)? {
            return Ok(status);
        }

        let tail_before = self.header.tail_cursor;
        let mut processed_cols = 0usize;
        for col_idx in 0..self.col_count {
            let desc = self.desc(col_idx);
            let result = source.with_cell(col_idx, |cell| {
                self.write_cell(col_idx, row_idx, desc, cell)
                    .map_err(Into::into)
            })?;
            match result {
                CellWrite::Written => {
                    processed_cols += 1;
                }
                CellWrite::Full => {
                    self.rollback_row(row_idx, processed_cols, tail_before)?;
                    return Ok(AppendStatus::Full);
                }
            }
        }

        self.header.row_count = row_idx + 1;
        Ok(AppendStatus::Appended)
    }

    fn try_append_single_fixed_width_row<S>(
        &mut self,
        row_idx: u32,
        source: &mut S,
    ) -> Result<Option<AppendStatus>, S::Error>
    where
        S: RowSource,
    {
        if self.col_count != 1 {
            return Ok(None);
        }
        let desc = self.desc(0);
        let type_tag = match TypeTag::from_raw(desc.type_tag) {
            Ok(type_tag) => type_tag,
            Err(error) => return Err(RowEncodeError::Layout(error).into()),
        };
        if !matches!(
            type_tag,
            TypeTag::Boolean
                | TypeTag::Int16
                | TypeTag::Int32
                | TypeTag::Int64
                | TypeTag::Float32
                | TypeTag::Float64
        ) {
            return Ok(None);
        }

        source.with_cell(0, |cell| {
            if matches!(cell, CellRef::Null) {
                self.write_null(0, row_idx, desc)?;
                self.header.row_count = row_idx + 1;
                return Ok(Some(AppendStatus::Appended));
            }

            match (type_tag, cell) {
                (TypeTag::Boolean, CellRef::Boolean(value)) => {
                    self.write_bool(row_idx, desc, value);
                }
                (TypeTag::Int16, CellRef::Int16(value)) => {
                    self.write_validity(row_idx, desc, true);
                    self.write_fixed_bytes(row_idx, desc, &value.to_ne_bytes())?;
                }
                (TypeTag::Int32, CellRef::Int32(value)) => {
                    self.write_validity(row_idx, desc, true);
                    self.write_fixed_bytes(row_idx, desc, &value.to_ne_bytes())?;
                }
                (TypeTag::Int64, CellRef::Int64(value)) => {
                    self.write_validity(row_idx, desc, true);
                    self.write_fixed_bytes(row_idx, desc, &value.to_ne_bytes())?;
                }
                (TypeTag::Float32, CellRef::Float32(value)) => {
                    self.write_validity(row_idx, desc, true);
                    self.write_fixed_bytes(row_idx, desc, &value.to_bits().to_ne_bytes())?;
                }
                (TypeTag::Float64, CellRef::Float64(value)) => {
                    self.write_validity(row_idx, desc, true);
                    self.write_fixed_bytes(row_idx, desc, &value.to_bits().to_ne_bytes())?;
                }
                (expected, actual) => {
                    return Err(RowEncodeError::TypeMismatch {
                        index: 0,
                        expected,
                        actual: actual.cell_type(),
                    }
                    .into());
                }
            }

            self.header.row_count = row_idx + 1;
            Ok(Some(AppendStatus::Appended))
        })
    }

    pub fn finish(mut self) -> Result<EncodedBatch, RowEncodeError> {
        self.write_header();
        BlockRef::open(&*self.payload)?;
        Ok(EncodedBatch {
            row_count: usize::try_from(self.header.row_count)
                .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
            payload_len: usize::try_from(self.header.block_size)
                .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
        })
    }

    fn write_cell(
        &mut self,
        index: usize,
        row_idx: u32,
        desc: ColumnDesc,
        cell: CellRef<'_>,
    ) -> Result<CellWrite, RowEncodeError> {
        if matches!(cell, CellRef::Null) {
            self.write_null(index, row_idx, desc)?;
            return Ok(CellWrite::Written);
        }

        let type_tag = TypeTag::from_raw(desc.type_tag)?;
        match (type_tag, cell) {
            (TypeTag::Boolean, CellRef::Boolean(value)) => {
                self.write_bool(row_idx, desc, value);
                Ok(CellWrite::Written)
            }
            (TypeTag::Int16, CellRef::Int16(value)) => {
                self.write_fixed(row_idx, desc, &value.to_ne_bytes())
            }
            (TypeTag::Int32, CellRef::Int32(value)) => {
                self.write_fixed(row_idx, desc, &value.to_ne_bytes())
            }
            (TypeTag::Int64, CellRef::Int64(value)) => {
                self.write_fixed(row_idx, desc, &value.to_ne_bytes())
            }
            (TypeTag::Float32, CellRef::Float32(value)) => {
                self.write_fixed(row_idx, desc, &value.to_bits().to_ne_bytes())
            }
            (TypeTag::Float64, CellRef::Float64(value)) => {
                self.write_fixed(row_idx, desc, &value.to_bits().to_ne_bytes())
            }
            (TypeTag::Uuid, CellRef::Uuid(bytes)) => {
                if bytes.len() != UUID_WIDTH_BYTES as usize {
                    return Err(RowEncodeError::InvalidUuidWidth {
                        index,
                        len: bytes.len(),
                    });
                }
                self.write_fixed(row_idx, desc, bytes)
            }
            (TypeTag::Utf8View, CellRef::Utf8(bytes)) => {
                self.write_view(index, row_idx, desc, bytes)
            }
            (TypeTag::BinaryView, CellRef::Binary(bytes)) => {
                self.write_view(index, row_idx, desc, bytes)
            }
            (expected, actual) => Err(RowEncodeError::TypeMismatch {
                index,
                expected,
                actual: actual.cell_type(),
            }),
        }
    }

    fn write_fixed(
        &mut self,
        row_idx: u32,
        desc: ColumnDesc,
        bytes: &[u8],
    ) -> Result<CellWrite, RowEncodeError> {
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
    ) -> Result<CellWrite, RowEncodeError> {
        if bytes.len() > i32::MAX as usize {
            return Err(RowEncodeError::RowValueTooLarge {
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
            u32::try_from(bytes.len()).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?;
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
    ) -> Result<(), RowEncodeError> {
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
    ) -> Result<(), RowEncodeError> {
        if !desc.flags().is_nullable() {
            return Err(RowEncodeError::NullInNonNullableColumn { index });
        }
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
            raw => return Err(arrow_layout::LayoutError::InvalidTypeTag { raw }.into()),
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

    fn write_header(&mut self) {
        unsafe { ptr::write_unaligned(self.block_ptr.cast::<BlockHeader>(), self.header) };
    }

    fn increment_null_count(
        &mut self,
        index: usize,
        mut desc: ColumnDesc,
    ) -> Result<(), RowEncodeError> {
        desc.null_count = desc
            .null_count
            .checked_add(1)
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        self.write_desc(index, desc);
        Ok(())
    }

    fn decrement_null_count(
        &mut self,
        index: usize,
        mut desc: ColumnDesc,
    ) -> Result<(), RowEncodeError> {
        desc.null_count = desc
            .null_count
            .checked_sub(1)
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        self.write_desc(index, desc);
        Ok(())
    }

    fn tail_alloc(&mut self, len: u32) -> Result<Option<u32>, RowEncodeError> {
        if len == 0 {
            return Ok(Some(self.header.tail_cursor));
        }
        let next = self
            .header
            .tail_cursor
            .checked_sub(len)
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
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
        if !desc.flags().is_nullable() {
            return;
        }
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
    ) -> Result<(), RowEncodeError> {
        let width = match desc.type_tag {
            raw if raw == TypeTag::Int16.to_raw() => 2usize,
            raw if raw == TypeTag::Int32.to_raw() || raw == TypeTag::Float32.to_raw() => 4usize,
            raw if raw == TypeTag::Int64.to_raw() || raw == TypeTag::Float64.to_raw() => 8usize,
            raw if raw == TypeTag::Uuid.to_raw() => 16usize,
            raw => return Err(arrow_layout::LayoutError::InvalidTypeTag { raw }.into()),
        };
        if bytes.len() != width {
            return Err(arrow_layout::LayoutError::InvalidHeaderBounds.into());
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
    ) -> Result<(), RowEncodeError> {
        let start = desc.values_off as usize + (row_idx as usize * size_of::<ByteView>());
        unsafe {
            ptr::write_unaligned(self.block_ptr.add(start).cast::<ByteView>(), view);
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn tail_cursor_for_tests(&self) -> u32 {
        self.header.tail_cursor
    }
}

enum CellWrite {
    Written,
    Full,
}
