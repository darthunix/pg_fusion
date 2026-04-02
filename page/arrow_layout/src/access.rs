//! Zero-allocation read/write accessors over initialized blocks.

use crate::bitmap::{bitmap_get, bitmap_set};
use crate::constants::VIEW_INLINE_LEN;
use crate::internals::{
    block_range, block_range_mut, byte_range, column_layout_from_desc, desc_offset, read_struct,
    write_struct,
};
use crate::raw::{BlockHeader, ColumnDesc};
use crate::validate::{validate_block_prefix, validate_desc_layout_in_block};
use crate::{ByteView, ColumnLayout, LayoutError, LayoutPlan, TypeTag};
use std::mem::size_of;

/// Result of attempting to write a variable-width view value into a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewWriteStatus {
    /// The value was written successfully.
    Written,
    /// The value did not fit into the remaining shared tail arena.
    Full,
}

/// Zero-allocation read-only view over an initialized raw page block.
///
/// `BlockRef` validates the block on open and then provides copy-based access
/// to the on-page metadata, front-region buffers, and shared tail pool without
/// allocating.
#[derive(Debug)]
pub struct BlockRef<'a> {
    block: &'a [u8],
    header: BlockHeader,
}

impl<'a> BlockRef<'a> {
    /// Opens and validates an initialized block in-place.
    pub fn open(block: &'a [u8]) -> Result<Self, LayoutError> {
        let header = read_struct::<BlockHeader>(block, 0)?;
        let desc_count = usize::from(header.col_count);
        validate_block_prefix(block, &header, desc_count)?;
        validate_desc_layout_in_block(block, &header)?;
        Ok(Self { block, header })
    }

    /// Returns the total block size in bytes.
    pub fn block_size(&self) -> u32 {
        self.header.block_size
    }

    /// Returns the current row count.
    pub fn row_count(&self) -> u32 {
        self.header.row_count
    }

    /// Returns the maximum number of rows reserved in the block.
    pub fn max_rows(&self) -> u32 {
        self.header.max_rows
    }

    /// Returns the shared-pool base offset.
    pub fn pool_base(&self) -> u32 {
        self.header.pool_base
    }

    /// Returns the current shared-pool tail cursor.
    pub fn tail_cursor(&self) -> u32 {
        self.header.tail_cursor
    }

    /// Returns the number of columns described in the block.
    pub fn column_count(&self) -> usize {
        usize::from(self.header.col_count)
    }

    /// Returns the total shared-pool capacity `[pool_base, block_size)`.
    pub fn shared_pool_capacity(&self) -> Result<u32, LayoutError> {
        self.header.shared_pool_capacity()
    }

    /// Returns the allocated tail start relative to `pool_base`.
    pub fn allocated_shared_pool_offset(&self) -> Result<u32, LayoutError> {
        self.header
            .tail_cursor
            .checked_sub(self.header.pool_base)
            .ok_or(LayoutError::InvalidHeaderBounds)
    }

    /// Returns the null count stored in a column descriptor.
    pub fn null_count(&self, index: usize) -> Result<u32, LayoutError> {
        Ok(self.desc(index)?.null_count)
    }

    /// Resolves one column descriptor into its computed layout.
    pub fn column_layout(&self, index: usize) -> Result<ColumnLayout, LayoutError> {
        column_layout_from_desc(index, self.max_rows(), self.desc(index)?)
    }

    /// Reads one validity bit from a column bitmap.
    pub fn validity(&self, index: usize, row: u32) -> Result<bool, LayoutError> {
        let bytes = self.validity_bytes(index)?;
        Ok(bitmap_get(bytes, row))
    }

    /// Reads one boolean value from a boolean column.
    pub fn bool_value(&self, index: usize, row: u32) -> Result<bool, LayoutError> {
        let layout = self.column_layout(index)?;
        if layout.type_tag != TypeTag::Boolean {
            return Err(LayoutError::InvalidTypeTag {
                raw: layout.type_tag.to_raw(),
            });
        }
        let values = self.boolean_values(index)?;
        Ok(bitmap_get(values, row))
    }

    /// Borrows one fixed-width value slot from a non-boolean, non-view column.
    pub fn fixed_value(&self, index: usize, row: u32) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        let row_width = layout
            .type_tag
            .values_row_width()
            .ok_or(LayoutError::InvalidTypeTag {
                raw: layout.type_tag.to_raw(),
            })?;
        if layout.type_tag == TypeTag::Boolean || layout.type_tag.is_view() {
            return Err(LayoutError::InvalidTypeTag {
                raw: layout.type_tag.to_raw(),
            });
        }
        let row_width = usize::try_from(row_width).map_err(|_| LayoutError::SizeOverflow)?;
        let start = usize::try_from(
            row.checked_mul(u32::try_from(row_width).map_err(|_| LayoutError::SizeOverflow)?)
                .ok_or(LayoutError::SizeOverflow)?,
        )
        .map_err(|_| LayoutError::SizeOverflow)?;
        let end = start
            .checked_add(row_width)
            .ok_or(LayoutError::SizeOverflow)?;
        Ok(&self.fixed_values(index)?[start..end])
    }

    /// Reads one `ByteView` slot from a view column.
    pub fn view(&self, index: usize, row: u32) -> Result<ByteView, LayoutError> {
        let layout = self.column_layout(index)?;
        let type_tag = layout.type_tag;
        if !type_tag.is_view() {
            return Err(LayoutError::InconsistentViewFlag {
                index,
                type_tag,
                flags: layout.flags.bits(),
            });
        }
        if row >= self.max_rows() {
            return Err(LayoutError::RowCountExceedsMaxRows {
                row_count: row,
                max_rows: self.max_rows(),
            });
        }
        let slot_off = layout
            .values_off
            .checked_add(
                row.checked_mul(size_of::<ByteView>() as u32)
                    .ok_or(LayoutError::SizeOverflow)?,
            )
            .ok_or(LayoutError::SizeOverflow)?;
        let view = read_struct::<ByteView>(
            self.block,
            usize::try_from(slot_off).map_err(|_| LayoutError::SizeOverflow)?,
        )?;
        view.validate(self.header.shared_pool_capacity()?)?;
        Ok(view)
    }

    /// Borrows the full shared pool slice `[pool_base, block_size)`.
    pub fn shared_pool(&self) -> Result<&[u8], LayoutError> {
        block_range(
            self.block,
            byte_range(self.header.pool_base, self.header.shared_pool_capacity()?)?,
        )
    }

    /// Borrows only the allocated shared-pool suffix `[tail_cursor, block_size)`.
    pub fn allocated_shared_pool(&self) -> Result<&[u8], LayoutError> {
        block_range(
            self.block,
            byte_range(
                self.header.tail_cursor,
                self.header
                    .block_size
                    .checked_sub(self.header.tail_cursor)
                    .ok_or(LayoutError::InvalidHeaderBounds)?,
            )?,
        )
    }

    pub(crate) fn desc(&self, index: usize) -> Result<ColumnDesc, LayoutError> {
        let col_count = self.column_count();
        if index >= col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds { index, col_count });
        }
        read_struct(self.block, desc_offset(index))
    }

    pub(crate) fn validity_bytes(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.validity_off, layout.validity_len)?,
        )
    }

    fn boolean_values(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.values_off, layout.values_len)?,
        )
    }

    fn fixed_values(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.values_off, layout.values_len)?,
        )
    }
}

/// Zero-allocation mutable view over an initialized raw page block.
///
/// The wrapper keeps a local copy of the header for fast repeated access and
/// writes it back in-place whenever mutable block state changes.
#[derive(Debug)]
pub struct BlockMut<'a> {
    block: &'a mut [u8],
    header: BlockHeader,
}

impl<'a> BlockMut<'a> {
    /// Opens and validates an initialized block in-place.
    pub fn open(block: &'a mut [u8]) -> Result<Self, LayoutError> {
        let header = read_struct::<BlockHeader>(block, 0)?;
        let desc_count = usize::from(header.col_count);
        validate_block_prefix(block, &header, desc_count)?;
        validate_desc_layout_in_block(block, &header)?;
        Ok(Self { block, header })
    }

    /// Returns the total block size in bytes.
    pub fn block_size(&self) -> u32 {
        self.header.block_size
    }

    /// Returns the current row count.
    pub fn row_count(&self) -> u32 {
        self.header.row_count
    }

    /// Returns the maximum number of rows reserved in the block.
    pub fn max_rows(&self) -> u32 {
        self.header.max_rows
    }

    /// Returns the shared-pool base offset.
    pub fn pool_base(&self) -> u32 {
        self.header.pool_base
    }

    /// Returns the current shared-pool tail cursor.
    pub fn tail_cursor(&self) -> u32 {
        self.header.tail_cursor
    }

    /// Returns the number of columns described in the block.
    pub fn column_count(&self) -> usize {
        usize::from(self.header.col_count)
    }

    /// Returns the null count stored in a column descriptor.
    pub fn null_count(&self, index: usize) -> Result<u32, LayoutError> {
        Ok(self.desc(index)?.null_count)
    }

    /// Resolves one column descriptor into its computed layout.
    pub fn column_layout(&self, index: usize) -> Result<ColumnLayout, LayoutError> {
        column_layout_from_desc(index, self.max_rows(), self.desc(index)?)
    }

    /// Sets or clears one validity bit directly.
    ///
    /// This is a low-level escape hatch. Prefer higher-level helpers such as
    /// [`BlockMut::write_null`], [`BlockMut::write_bool`], and
    /// [`BlockMut::write_fixed`] in normal producer code.
    pub fn set_validity(&mut self, index: usize, row: u32, valid: bool) -> Result<(), LayoutError> {
        let bytes = self.validity_bytes_mut(index)?;
        bitmap_set(bytes, row, valid);
        Ok(())
    }

    /// Reads one validity bit.
    pub fn validity(&self, index: usize, row: u32) -> Result<bool, LayoutError> {
        let bytes = self.validity_bytes(index)?;
        Ok(bitmap_get(bytes, row))
    }

    /// Writes one nullable boolean value.
    pub fn write_bool(&mut self, index: usize, row: u32, value: bool) -> Result<(), LayoutError> {
        let layout = self.column_layout(index)?;
        if layout.type_tag != TypeTag::Boolean {
            return Err(LayoutError::InvalidTypeTag {
                raw: layout.type_tag.to_raw(),
            });
        }
        self.set_validity(index, row, true)?;
        let values = self.values_bytes_mut(index)?;
        bitmap_set(values, row, value);
        Ok(())
    }

    /// Writes one fixed-width value and marks the row valid.
    pub fn write_fixed(&mut self, index: usize, row: u32, bytes: &[u8]) -> Result<(), LayoutError> {
        self.set_validity(index, row, true)?;
        self.write_fixed_raw(index, row, bytes)
    }

    /// Writes a null slot for the column's layout type.
    pub fn write_null(&mut self, index: usize, row: u32) -> Result<(), LayoutError> {
        let layout = self.column_layout(index)?;
        self.set_validity(index, row, false)?;
        match layout.type_tag {
            TypeTag::Boolean => {
                let values = self.values_bytes_mut(index)?;
                bitmap_set(values, row, false);
            }
            TypeTag::Int16 => self.zero_value_slot(index, row, 2)?,
            TypeTag::Int32 | TypeTag::Float32 => self.zero_value_slot(index, row, 4)?,
            TypeTag::Int64 | TypeTag::Float64 => self.zero_value_slot(index, row, 8)?,
            TypeTag::Uuid | TypeTag::Utf8View | TypeTag::BinaryView => {
                self.zero_value_slot(index, row, 16)?
            }
        }
        Ok(())
    }

    /// Writes one string/binary view value, allocating tail space for long
    /// values automatically.
    pub fn write_view_bytes(
        &mut self,
        index: usize,
        row: u32,
        bytes: &[u8],
    ) -> Result<ViewWriteStatus, LayoutError> {
        if bytes.len() <= VIEW_INLINE_LEN {
            self.set_validity(index, row, true)?;
            self.write_view(index, row, ByteView::new_inline(bytes)?)?;
            return Ok(ViewWriteStatus::Written);
        }

        let len = u32::try_from(bytes.len()).map_err(|_| LayoutError::SizeOverflow)?;
        let Some(start) = self.tail_alloc(len)? else {
            return Ok(ViewWriteStatus::Full);
        };
        self.tail_bytes_mut(start, len)?.copy_from_slice(bytes);
        self.set_validity(index, row, true)?;
        self.write_view(
            index,
            row,
            ByteView::new_outline(bytes, start - self.pool_base())?,
        )?;
        Ok(ViewWriteStatus::Written)
    }

    /// Rolls the tail cursor back to a previous value.
    pub fn rollback_tail(&mut self, tail_cursor: u32) -> Result<(), LayoutError> {
        self.header.tail_cursor = tail_cursor;
        self.write_header()
    }

    /// Returns mutable access to one column's packed validity bitmap.
    ///
    /// This is a low-level bulk-write escape hatch for block-oriented encoders.
    /// Callers are responsible for keeping the bitmap and descriptor null counts
    /// consistent.
    pub fn column_validity_bytes_mut(&mut self, index: usize) -> Result<&mut [u8], LayoutError> {
        self.validity_bytes_mut(index)
    }

    /// Returns mutable access to one column's fixed-width values buffer or view slots.
    ///
    /// This is a low-level bulk-write escape hatch for block-oriented encoders.
    pub fn column_values_bytes_mut(&mut self, index: usize) -> Result<&mut [u8], LayoutError> {
        self.values_bytes_mut(index)
    }

    /// Reserves `len` bytes from the shared tail arena.
    ///
    /// Returns the absolute block offset of the reserved range on success, or
    /// `Ok(None)` when the tail is full.
    pub fn reserve_tail(&mut self, len: u32) -> Result<Option<u32>, LayoutError> {
        self.tail_alloc(len)
    }

    /// Returns a mutable slice for one tail allocation.
    pub fn tail_slice_mut(&mut self, start: u32, len: u32) -> Result<&mut [u8], LayoutError> {
        self.tail_bytes_mut(start, len)
    }

    /// Adds `delta` to one column descriptor's null count.
    pub fn add_null_count(&mut self, index: usize, delta: u32) -> Result<(), LayoutError> {
        if delta == 0 {
            return Ok(());
        }
        let mut desc = self.desc(index)?;
        desc.null_count = desc
            .null_count
            .checked_add(delta)
            .ok_or(LayoutError::SizeOverflow)?;
        self.write_desc(index, desc)
    }

    /// Advances the committed row count by `delta`.
    pub fn advance_row_count(&mut self, delta: u32) -> Result<(), LayoutError> {
        let next = self
            .header
            .row_count
            .checked_add(delta)
            .ok_or(LayoutError::SizeOverflow)?;
        if next > self.max_rows() {
            return Err(LayoutError::RowCountExceedsMaxRows {
                row_count: next,
                max_rows: self.max_rows(),
            });
        }
        self.header.row_count = next;
        self.write_header()
    }

    /// Commits the row currently being appended.
    ///
    /// This scans the current `row_count` slot across all columns, updates
    /// per-column `null_count`, and then increments `row_count`.
    pub fn commit_current_row(&mut self) -> Result<(), LayoutError> {
        let row = self.row_count();
        if row >= self.max_rows() {
            return Err(LayoutError::RowCountExceedsMaxRows {
                row_count: row,
                max_rows: self.max_rows(),
            });
        }
        for index in 0..self.column_count() {
            if !self.validity(index, row)? {
                self.increment_null_count(index)?;
            }
        }
        self.header.row_count = row + 1;
        self.write_header()
    }

    /// Writes one low-level `ByteView` slot directly.
    ///
    /// This is an escape hatch for tests or advanced producers that already
    /// built a validated slot themselves. Prefer [`BlockMut::write_view_bytes`]
    /// for normal producer code.
    pub fn write_view(
        &mut self,
        index: usize,
        row: u32,
        view: ByteView,
    ) -> Result<(), LayoutError> {
        let layout = self.column_layout(index)?;
        let type_tag = layout.type_tag;
        if !type_tag.is_view() {
            return Err(LayoutError::InconsistentViewFlag {
                index,
                type_tag,
                flags: layout.flags.bits(),
            });
        }
        if row >= self.max_rows() {
            return Err(LayoutError::RowCountExceedsMaxRows {
                row_count: row,
                max_rows: self.max_rows(),
            });
        }
        view.validate(self.header.shared_pool_capacity()?)?;
        let slot_off = layout
            .values_off
            .checked_add(
                row.checked_mul(size_of::<ByteView>() as u32)
                    .ok_or(LayoutError::SizeOverflow)?,
            )
            .ok_or(LayoutError::SizeOverflow)?;
        write_struct(
            self.block,
            usize::try_from(slot_off).map_err(|_| LayoutError::SizeOverflow)?,
            view,
        )
    }

    /// Validates the full mutated block.
    pub fn validate(&self) -> Result<(), LayoutError> {
        validate_block_prefix(self.block, &self.header, self.column_count())?;
        validate_desc_layout_in_block(self.block, &self.header)
    }

    pub(crate) fn desc(&self, index: usize) -> Result<ColumnDesc, LayoutError> {
        let col_count = self.column_count();
        if index >= col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds { index, col_count });
        }
        read_struct(self.block, desc_offset(index))
    }

    pub(crate) fn write_desc(&mut self, index: usize, desc: ColumnDesc) -> Result<(), LayoutError> {
        let col_count = self.column_count();
        if index >= col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds { index, col_count });
        }
        write_struct(self.block, desc_offset(index), desc)
    }

    pub(crate) fn validity_bytes_mut(&mut self, index: usize) -> Result<&mut [u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range_mut(
            self.block,
            byte_range(layout.validity_off, layout.validity_len)?,
        )
    }

    pub(crate) fn values_bytes_mut(&mut self, index: usize) -> Result<&mut [u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range_mut(
            self.block,
            byte_range(layout.values_off, layout.values_len)?,
        )
    }

    pub(crate) fn validity_bytes(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.validity_off, layout.validity_len)?,
        )
    }

    pub(crate) fn tail_alloc(&mut self, len: u32) -> Result<Option<u32>, LayoutError> {
        if len == 0 {
            return Ok(Some(self.header.tail_cursor));
        }
        let next = self
            .header
            .tail_cursor
            .checked_sub(len)
            .ok_or(LayoutError::SizeOverflow)?;
        if next < self.header.pool_base {
            return Ok(None);
        }
        self.header.tail_cursor = next;
        self.write_header()?;
        Ok(Some(next))
    }

    pub(crate) fn tail_bytes_mut(
        &mut self,
        start: u32,
        len: u32,
    ) -> Result<&mut [u8], LayoutError> {
        block_range_mut(self.block, byte_range(start, len)?)
    }

    fn increment_null_count(&mut self, index: usize) -> Result<(), LayoutError> {
        let mut desc = self.desc(index)?;
        desc.null_count = desc
            .null_count
            .checked_add(1)
            .ok_or(LayoutError::SizeOverflow)?;
        self.write_desc(index, desc)
    }

    fn write_fixed_raw(&mut self, index: usize, row: u32, bytes: &[u8]) -> Result<(), LayoutError> {
        let layout = self.column_layout(index)?;
        let row_width = layout
            .type_tag
            .values_row_width()
            .ok_or(LayoutError::InvalidTypeTag {
                raw: layout.type_tag.to_raw(),
            })?;
        let row_width = usize::try_from(row_width).map_err(|_| LayoutError::SizeOverflow)?;
        if bytes.len() != row_width {
            return Err(LayoutError::InvalidHeaderBounds);
        }
        let start = usize::try_from(
            row.checked_mul(u32::try_from(row_width).map_err(|_| LayoutError::SizeOverflow)?)
                .ok_or(LayoutError::SizeOverflow)?,
        )
        .map_err(|_| LayoutError::SizeOverflow)?;
        let end = start
            .checked_add(row_width)
            .ok_or(LayoutError::SizeOverflow)?;
        self.values_bytes_mut(index)?[start..end].copy_from_slice(bytes);
        Ok(())
    }

    fn zero_value_slot(
        &mut self,
        index: usize,
        row: u32,
        row_width: usize,
    ) -> Result<(), LayoutError> {
        let layout = self.column_layout(index)?;
        let width = layout
            .type_tag
            .values_row_width()
            .ok_or(LayoutError::InvalidTypeTag {
                raw: layout.type_tag.to_raw(),
            })?;
        let width = usize::try_from(width).map_err(|_| LayoutError::SizeOverflow)?;
        debug_assert_eq!(width, row_width);
        let start = usize::try_from(
            row.checked_mul(u32::try_from(row_width).map_err(|_| LayoutError::SizeOverflow)?)
                .ok_or(LayoutError::SizeOverflow)?,
        )
        .map_err(|_| LayoutError::SizeOverflow)?;
        let end = start
            .checked_add(row_width)
            .ok_or(LayoutError::SizeOverflow)?;
        self.values_bytes_mut(index)?[start..end].fill(0);
        Ok(())
    }

    fn write_header(&mut self) -> Result<(), LayoutError> {
        write_struct(self.block, 0, self.header)
    }
}

/// Initializes an empty raw block in-place from a precomputed layout plan.
///
/// The function zeroes the block contents up to `plan.block_size()` and writes
/// the v1 [`crate::raw::BlockHeader`] plus [`crate::raw::ColumnDesc`] array at
/// the front.
pub fn init_block(block: &mut [u8], plan: &LayoutPlan) -> Result<(), LayoutError> {
    let block_size = usize::try_from(plan.block_size()).map_err(|_| LayoutError::SizeOverflow)?;
    if block.len() < block_size {
        return Err(LayoutError::BlockSliceTooSmall {
            required: block_size,
            actual: block.len(),
        });
    }
    block[..block_size].fill(0);
    write_struct(block, 0, plan.block_header())?;
    for (index, desc) in plan.column_descs().enumerate() {
        write_struct(block, desc_offset(index), desc)?;
    }
    Ok(())
}
