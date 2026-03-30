//! Zero-copy Apache Arrow batch import over `page_transfer`.
//!
//! `page_arrow` consumes a [`page_transfer::ReceivedPage`] whose payload is a
//! raw [`page_arrow_layout`] block and returns a plain
//! [`arrow_array::RecordBatch`] backed directly by the shared-memory page
//! bytes.
//!
//! Ordinary imported batches extend page lifetime through Arrow buffer
//! ownership. Empty-schema batches contain no page-backed buffers and may
//! release the page before `import()` returns. Holding an imported batch does
//! not keep `page_transfer::PageRx` busy for later accepts.

mod error;

#[cfg(test)]
mod tests;

use arrow_array::types::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use arrow_array::{
    ArrayRef, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, PrimitiveArray, RecordBatch,
    RecordBatchOptions, StringViewArray,
};
use arrow_buffer::alloc::Allocation;
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
pub use error::{ConfigError, ImportError};
use page_arrow_layout::bitmap::bitmap_bytes;
use page_arrow_layout::constants::UUID_WIDTH_BYTES;
use page_arrow_layout::{BlockRef, ColumnLayout, LayoutError, TypeTag};
use page_transfer::{MessageKind, ReceivedPage};
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;

/// `page_transfer::MessageKind` for layout-backed Arrow pages imported by this crate.
pub const ARROW_LAYOUT_BATCH_KIND: MessageKind = 0x4152;

/// Importer for `page_arrow_layout` batches stored in `page_transfer` pages.
#[derive(Clone, Debug)]
pub struct ArrowPageDecoder {
    schema: SchemaRef,
    columns: Vec<ExpectedColumn>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExpectedColumn {
    type_tag: TypeTag,
    nullable: bool,
}

#[derive(Clone, Copy, Debug)]
struct SharedPoolImport {
    offset: usize,
    len: usize,
    allocated_tail_start: u32,
}

impl ArrowPageDecoder {
    /// Create a decoder for pages that should decode into `schema`.
    pub fn new(schema: SchemaRef) -> Result<Self, ConfigError> {
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            let type_tag =
                TypeTag::from_arrow_data_type(index, field.data_type()).map_err(|_| {
                    ConfigError::UnsupportedArrowType {
                        index,
                        data_type: field.data_type().to_string(),
                    }
                })?;
            columns.push(ExpectedColumn {
                type_tag,
                nullable: field.is_nullable(),
            });
        }

        Ok(Self { schema, columns })
    }

    /// Import one `ReceivedPage` as a zero-copy Arrow `RecordBatch`.
    pub fn import(&self, page: ReceivedPage) -> Result<RecordBatch, ImportError> {
        if page.kind() != ARROW_LAYOUT_BATCH_KIND {
            return Err(ImportError::WrongKind {
                expected: ARROW_LAYOUT_BATCH_KIND,
                actual: page.kind(),
            });
        }
        if page.flags() != 0 {
            return Err(ImportError::UnsupportedFlags {
                actual: page.flags(),
            });
        }

        let owner = Arc::new(PageAllocationOwner::new(page));
        let (row_count, shared_pool) = owner.inspect(|payload| {
            let block = BlockRef::open(payload)?;
            self.validate_schema(&block)?;
            Ok((
                block.row_count(),
                SharedPoolImport {
                    offset: usize::try_from(block.pool_base()).map_err(|_| LayoutError::SizeOverflow)?,
                    len: usize::try_from(block.shared_pool_capacity()?)
                        .map_err(|_| LayoutError::SizeOverflow)?,
                    allocated_tail_start: block.allocated_shared_pool_offset()?,
                },
            ))
        })?;
        let row_count_usize = usize::try_from(row_count).map_err(|_| LayoutError::SizeOverflow)?;

        if self.columns.is_empty() {
            return Ok(RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(row_count_usize)),
            )?);
        }

        let mut columns = Vec::with_capacity(self.columns.len());
        for index in 0..self.columns.len() {
            let (layout, null_count) = owner.inspect(|payload| {
                let block = BlockRef::open(payload)?;
                Ok((block.column_layout(index)?, block.null_count(index)?))
            })?;
            let nulls = self.import_nulls(&owner, index, &layout, row_count, null_count)?;
            let array: ArrayRef = match layout.type_tag {
                TypeTag::Boolean => {
                    Arc::new(self.import_boolean(&owner, &layout, row_count, nulls)?)
                }
                TypeTag::Int16 => {
                    Arc::new(self.import_primitive::<Int16Type>(&owner, &layout, row_count, nulls)?)
                }
                TypeTag::Int32 => {
                    Arc::new(self.import_primitive::<Int32Type>(&owner, &layout, row_count, nulls)?)
                }
                TypeTag::Int64 => {
                    Arc::new(self.import_primitive::<Int64Type>(&owner, &layout, row_count, nulls)?)
                }
                TypeTag::Float32 => Arc::new(
                    self.import_primitive::<Float32Type>(&owner, &layout, row_count, nulls)?,
                ),
                TypeTag::Float64 => Arc::new(
                    self.import_primitive::<Float64Type>(&owner, &layout, row_count, nulls)?,
                ),
                TypeTag::Uuid => Arc::new(self.import_uuid(&owner, &layout, row_count, nulls)?),
                TypeTag::Utf8View => Arc::new(self.import_utf8_view(
                    &owner,
                    index,
                    &layout,
                    row_count,
                    shared_pool,
                    nulls,
                )?),
                TypeTag::BinaryView => Arc::new(self.import_binary_view(
                    &owner,
                    index,
                    &layout,
                    row_count,
                    shared_pool,
                    nulls,
                )?),
            };
            columns.push(array);
        }

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    fn validate_schema(&self, block: &BlockRef<'_>) -> Result<(), ImportError> {
        if block.column_count() != self.columns.len() {
            return Err(ImportError::SchemaColumnCountMismatch {
                expected: self.columns.len(),
                actual: block.column_count(),
            });
        }

        for (index, expected) in self.columns.iter().copied().enumerate() {
            let layout = block.column_layout(index)?;
            if layout.type_tag != expected.type_tag {
                return Err(ImportError::SchemaTypeMismatch {
                    index,
                    expected: self.schema.field(index).data_type().to_string(),
                    actual: layout.type_tag,
                });
            }
            if layout.flags.is_nullable() != expected.nullable {
                return Err(ImportError::SchemaNullabilityMismatch {
                    index,
                    expected: expected.nullable,
                    actual: layout.flags.is_nullable(),
                });
            }
        }

        Ok(())
    }

    fn import_nulls(
        &self,
        owner: &Arc<PageAllocationOwner>,
        index: usize,
        layout: &ColumnLayout,
        row_count: u32,
        null_count: u32,
    ) -> Result<Option<NullBuffer>, ImportError> {
        if !layout.flags.is_nullable() {
            if null_count != 0 {
                return Err(ImportError::InvalidNullCount {
                    index,
                    row_count,
                    null_count,
                });
            }
            return Ok(None);
        }

        if null_count > row_count {
            return Err(ImportError::InvalidNullCount {
                index,
                row_count,
                null_count,
            });
        }

        let validity_len =
            usize::try_from(bitmap_bytes(row_count)).map_err(|_| LayoutError::SizeOverflow)?;
        let validity = owner.buffer_from_payload(
            usize::try_from(layout.validity_off).map_err(|_| LayoutError::SizeOverflow)?,
            validity_len,
        )?;
        let boolean = BooleanBuffer::new(
            validity,
            0,
            usize::try_from(row_count).map_err(|_| LayoutError::SizeOverflow)?,
        );
        let actual_null_count = u32::try_from(
            boolean
                .len()
                .checked_sub(boolean.count_set_bits())
                .ok_or(LayoutError::SizeOverflow)?,
        )
        .map_err(|_| LayoutError::SizeOverflow)?;
        if actual_null_count != null_count {
            return Err(ImportError::NullBitmapCountMismatch {
                index,
                expected: null_count,
                actual: actual_null_count,
            });
        }
        if actual_null_count == 0 {
            return Ok(None);
        }
        Ok(Some(NullBuffer::new(boolean)))
    }

    fn import_boolean(
        &self,
        owner: &Arc<PageAllocationOwner>,
        layout: &ColumnLayout,
        row_count: u32,
        nulls: Option<NullBuffer>,
    ) -> Result<BooleanArray, ImportError> {
        let values_len = usize::try_from(layout.type_tag.values_used_len(row_count)?)
            .map_err(|_| LayoutError::SizeOverflow)?;
        let values = owner.buffer_from_payload(
            usize::try_from(layout.values_off).map_err(|_| LayoutError::SizeOverflow)?,
            values_len,
        )?;
        Ok(BooleanArray::new(
            BooleanBuffer::new(
                values,
                0,
                usize::try_from(row_count).map_err(|_| LayoutError::SizeOverflow)?,
            ),
            nulls,
        ))
    }

    fn import_primitive<T: ArrowPrimitiveType>(
        &self,
        owner: &Arc<PageAllocationOwner>,
        layout: &ColumnLayout,
        row_count: u32,
        nulls: Option<NullBuffer>,
    ) -> Result<PrimitiveArray<T>, ImportError> {
        let values_len = usize::try_from(layout.type_tag.values_used_len(row_count)?)
            .map_err(|_| LayoutError::SizeOverflow)?;
        let values = owner.buffer_from_payload_aligned(
            usize::try_from(layout.values_off).map_err(|_| LayoutError::SizeOverflow)?,
            values_len,
            std::mem::align_of::<T::Native>(),
        )?;
        Ok(PrimitiveArray::new(
            ScalarBuffer::<T::Native>::new(
                values,
                0,
                usize::try_from(row_count).map_err(|_| LayoutError::SizeOverflow)?,
            ),
            nulls,
        ))
    }

    fn import_uuid(
        &self,
        owner: &Arc<PageAllocationOwner>,
        layout: &ColumnLayout,
        row_count: u32,
        nulls: Option<NullBuffer>,
    ) -> Result<FixedSizeBinaryArray, ImportError> {
        let values_len = usize::try_from(layout.type_tag.values_used_len(row_count)?)
            .map_err(|_| LayoutError::SizeOverflow)?;
        let values = owner.buffer_from_payload(
            usize::try_from(layout.values_off).map_err(|_| LayoutError::SizeOverflow)?,
            values_len,
        )?;
        Ok(FixedSizeBinaryArray::try_new(
            i32::try_from(UUID_WIDTH_BYTES).expect("uuid width fits into i32"),
            values,
            nulls,
        )?)
    }

    fn import_utf8_view(
        &self,
        owner: &Arc<PageAllocationOwner>,
        index: usize,
        layout: &ColumnLayout,
        row_count: u32,
        shared_pool: SharedPoolImport,
        nulls: Option<NullBuffer>,
    ) -> Result<StringViewArray, ImportError> {
        self.validate_view_tail(
            owner,
            index,
            layout,
            row_count,
            shared_pool.allocated_tail_start,
        )?;
        let views = self.import_view_slots(owner, layout, row_count)?;
        let shared_pool = owner.buffer_from_payload(shared_pool.offset, shared_pool.len)?;
        Ok(StringViewArray::try_new(views, vec![shared_pool], nulls)?)
    }

    fn import_binary_view(
        &self,
        owner: &Arc<PageAllocationOwner>,
        index: usize,
        layout: &ColumnLayout,
        row_count: u32,
        shared_pool: SharedPoolImport,
        nulls: Option<NullBuffer>,
    ) -> Result<BinaryViewArray, ImportError> {
        self.validate_view_tail(
            owner,
            index,
            layout,
            row_count,
            shared_pool.allocated_tail_start,
        )?;
        let views = self.import_view_slots(owner, layout, row_count)?;
        let shared_pool = owner.buffer_from_payload(shared_pool.offset, shared_pool.len)?;
        Ok(BinaryViewArray::try_new(views, vec![shared_pool], nulls)?)
    }

    fn import_view_slots(
        &self,
        owner: &Arc<PageAllocationOwner>,
        layout: &ColumnLayout,
        row_count: u32,
    ) -> Result<ScalarBuffer<u128>, ImportError> {
        let views_len = usize::try_from(layout.type_tag.values_used_len(row_count)?)
            .map_err(|_| LayoutError::SizeOverflow)?;
        let views = owner.buffer_from_payload_aligned(
            usize::try_from(layout.values_off).map_err(|_| LayoutError::SizeOverflow)?,
            views_len,
            std::mem::align_of::<u128>(),
        )?;
        Ok(ScalarBuffer::<u128>::new(
            views,
            0,
            usize::try_from(row_count).map_err(|_| LayoutError::SizeOverflow)?,
        ))
    }

    fn validate_view_tail(
        &self,
        owner: &Arc<PageAllocationOwner>,
        index: usize,
        layout: &ColumnLayout,
        row_count: u32,
        allocated_tail_start: u32,
    ) -> Result<(), ImportError> {
        owner.inspect(|payload| {
            let block = BlockRef::open(payload)?;
            for row in 0..row_count {
                if layout.flags.is_nullable() && !block.validity(index, row)? {
                    continue;
                }
                let view = block.view(index, row)?;
                if let Some(offset) = view.offset()? {
                    if offset < allocated_tail_start {
                        return Err(ImportError::ViewOffsetBeforeAllocatedTail {
                            index,
                            row,
                            offset,
                            allocated_tail_start,
                        });
                    }
                }
            }
            Ok(())
        })
    }
}

struct PageAllocationOwner {
    page: ReceivedPage,
    payload_addr: usize,
    payload_len: usize,
}

impl PageAllocationOwner {
    fn new(page: ReceivedPage) -> Self {
        let (payload_addr, payload_len) = {
            let payload = page.payload();
            (payload.as_ptr() as usize, payload.len())
        };
        Self {
            page,
            payload_addr,
            payload_len,
        }
    }

    fn inspect<R>(
        &self,
        f: impl FnOnce(&[u8]) -> Result<R, ImportError>,
    ) -> Result<R, ImportError> {
        f(self.payload())
    }

    fn buffer_from_payload(
        self: &Arc<Self>,
        offset: usize,
        len: usize,
    ) -> Result<Buffer, ImportError> {
        let ptr = self.slice_ptr(offset, len)?;

        let allocation: Arc<dyn Allocation> = self.clone();
        Ok(unsafe { Buffer::from_custom_allocation(ptr, len, allocation) })
    }

    fn buffer_from_payload_aligned(
        self: &Arc<Self>,
        offset: usize,
        len: usize,
        alignment: usize,
    ) -> Result<Buffer, ImportError> {
        if alignment <= 1 {
            return self.buffer_from_payload(offset, len);
        }

        let end = self.payload_end(offset, len)?;
        let ptr = self.slice_ptr(offset, len)?;
        let misalignment = (ptr.as_ptr() as usize) % alignment;
        let aligned_offset =
            offset
                .checked_sub(misalignment)
                .ok_or(ImportError::MissingAlignedHeadroom {
                    offset,
                    alignment,
                    required_headroom: misalignment,
                })?;
        let total_len = end - aligned_offset;
        let aligned_ptr = self.slice_ptr(aligned_offset, total_len)?;

        let allocation: Arc<dyn Allocation> = self.clone();
        let buffer = unsafe { Buffer::from_custom_allocation(aligned_ptr, total_len, allocation) };
        Ok(buffer.slice(misalignment))
    }

    fn payload(&self) -> &[u8] {
        let _keep_page_alive = &self.page;
        unsafe { slice::from_raw_parts(self.payload_addr as *const u8, self.payload_len) }
    }

    fn payload_end(&self, offset: usize, len: usize) -> Result<usize, ImportError> {
        let end = offset
            .checked_add(len)
            .ok_or(ImportError::PayloadRangeOverflow { offset, len })?;
        if end > self.payload_len {
            return Err(ImportError::PayloadOutOfBounds {
                payload_len: self.payload_len,
                offset,
                len,
            });
        }
        Ok(end)
    }

    fn slice_ptr(&self, offset: usize, len: usize) -> Result<NonNull<u8>, ImportError> {
        self.payload_end(offset, len)?;
        let addr = self.payload_addr.wrapping_add(offset) as *mut u8;
        Ok(NonNull::new(addr).expect("slice pointers are never null"))
    }
}
