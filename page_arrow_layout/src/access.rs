//! Zero-allocation read/write accessors over initialized blocks.

use crate::internals::{
    block_range, block_range_mut, byte_range, column_layout_from_desc, desc_offset, read_struct,
    write_struct,
};
use crate::validate::{validate_block_prefix, validate_desc_layout_in_block};
use crate::{
    bitmap_get, bitmap_set, BlockHeader, ByteView, ColumnDesc, ColumnLayout, LayoutError,
    LayoutPlan,
};
use std::mem::size_of;

/// Zero-allocation read-only view over an initialized raw page block.
///
/// `BlockRef` validates the block on open and then provides copy-based access
/// to the on-page header, column descriptors, front-region buffers, and shared
/// tail pool without allocating.
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

    /// Returns a copy of the raw block header.
    pub fn header(&self) -> BlockHeader {
        self.header
    }

    /// Reads one raw column descriptor by index.
    pub fn desc(&self, index: usize) -> Result<ColumnDesc, LayoutError> {
        let col_count = self.column_count();
        if index >= col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds { index, col_count });
        }
        read_struct(self.block, desc_offset(index))
    }

    /// Resolves one column descriptor into its computed layout.
    pub fn column_layout(&self, index: usize) -> Result<ColumnLayout, LayoutError> {
        column_layout_from_desc(index, self.max_rows(), self.desc(index)?)
    }

    /// Borrows the reserved validity region for one column.
    pub fn validity_bytes(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.validity_off, layout.validity_len)?,
        )
    }

    /// Borrows the reserved values region for one column.
    pub fn values_bytes(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.values_off, layout.values_len)?,
        )
    }

    /// Reads one validity bit from a column bitmap.
    pub fn validity(&self, index: usize, row: u32) -> Result<bool, LayoutError> {
        let bytes = self.validity_bytes(index)?;
        Ok(bitmap_get(bytes, row))
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

    /// Returns a copy of the raw block header.
    pub fn header(&self) -> BlockHeader {
        self.header
    }

    /// Reads one raw column descriptor by index.
    pub fn desc(&self, index: usize) -> Result<ColumnDesc, LayoutError> {
        let col_count = self.column_count();
        if index >= col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds { index, col_count });
        }
        read_struct(self.block, desc_offset(index))
    }

    /// Overwrites one raw column descriptor by index.
    pub fn write_desc(&mut self, index: usize, desc: ColumnDesc) -> Result<(), LayoutError> {
        let col_count = self.column_count();
        if index >= col_count {
            return Err(LayoutError::ColumnIndexOutOfBounds { index, col_count });
        }
        write_struct(self.block, desc_offset(index), desc)
    }

    /// Resolves one column descriptor into its computed layout.
    pub fn column_layout(&self, index: usize) -> Result<ColumnLayout, LayoutError> {
        column_layout_from_desc(index, self.max_rows(), self.desc(index)?)
    }

    /// Borrows the reserved validity region for one column mutably.
    pub fn validity_bytes_mut(&mut self, index: usize) -> Result<&mut [u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range_mut(
            self.block,
            byte_range(layout.validity_off, layout.validity_len)?,
        )
    }

    /// Borrows the reserved values region for one column mutably.
    pub fn values_bytes_mut(&mut self, index: usize) -> Result<&mut [u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range_mut(
            self.block,
            byte_range(layout.values_off, layout.values_len)?,
        )
    }

    /// Sets or clears one validity bit.
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

    /// Borrows the reserved validity region for one column.
    pub fn validity_bytes(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.validity_off, layout.validity_len)?,
        )
    }

    /// Borrows the reserved values region for one column.
    pub fn values_bytes(&self, index: usize) -> Result<&[u8], LayoutError> {
        let layout = self.column_layout(index)?;
        block_range(
            self.block,
            byte_range(layout.values_off, layout.values_len)?,
        )
    }

    /// Updates the current row count in the header.
    pub fn set_row_count(&mut self, row_count: u32) -> Result<(), LayoutError> {
        self.header.row_count = row_count;
        self.write_header()
    }

    /// Updates the current shared-pool tail cursor in the header.
    pub fn set_tail_cursor(&mut self, tail_cursor: u32) -> Result<(), LayoutError> {
        self.header.tail_cursor = tail_cursor;
        self.write_header()
    }

    /// Increments the stored null count for one column descriptor.
    pub fn increment_null_count(&mut self, index: usize) -> Result<(), LayoutError> {
        let mut desc = self.desc(index)?;
        desc.null_count = desc
            .null_count
            .checked_add(1)
            .ok_or(LayoutError::SizeOverflow)?;
        self.write_desc(index, desc)
    }

    /// Writes one validated `ByteView` slot into a view column.
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

    /// Allocates `len` bytes from the shared tail arena, returning the block
    /// offset of the allocated region when space remains.
    pub fn tail_alloc(&mut self, len: u32) -> Result<Option<u32>, LayoutError> {
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

    /// Borrows a mutable tail slice previously allocated from the shared pool.
    pub fn tail_bytes_mut(&mut self, start: u32, len: u32) -> Result<&mut [u8], LayoutError> {
        block_range_mut(self.block, byte_range(start, len)?)
    }

    /// Re-validates the full block after in-place mutations.
    pub fn validate(&self) -> Result<(), LayoutError> {
        validate_block_prefix(self.block, &self.header, self.column_count())?;
        validate_desc_layout_in_block(self.block, &self.header)
    }

    fn write_header(&mut self) -> Result<(), LayoutError> {
        write_struct(self.block, 0, self.header)
    }
}

/// Initializes an empty raw block in-place from a precomputed layout plan.
///
/// The function zeroes the block contents up to `plan.block_size()` and writes
/// the v1 [`BlockHeader`] plus [`ColumnDesc`] array at the front.
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
