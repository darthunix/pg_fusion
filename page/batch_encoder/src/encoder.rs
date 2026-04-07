use crate::{ConfigError, EncodeError};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    BinaryType, BinaryViewType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    StringViewType, Utf8Type,
};
use arrow_array::{
    Array, BooleanArray, FixedSizeBinaryArray, GenericByteArray, GenericByteViewArray,
    PrimitiveArray, RecordBatch,
};
use arrow_buffer::bit_mask::set_bits;
use arrow_layout::constants::{UUID_WIDTH_BYTES, VIEW_INLINE_LEN};
use arrow_layout::{BlockMut, ByteView, ColumnLayout, LayoutPlan, TypeTag};
use arrow_schema::{DataType, Schema};
use std::mem::size_of;
use std::slice;

/// Result of trying to append a batch prefix to the current block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendResult {
    /// Number of rows successfully appended from `start_row`.
    pub rows_written: usize,
    /// Whether the block filled up before the input batch prefix was exhausted.
    pub full: bool,
}

/// Metadata returned after finalizing an encoded block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedBatch {
    /// Number of rows committed into the block.
    pub row_count: usize,
    /// Number of bytes occupied by the finalized payload.
    pub payload_len: usize,
}

/// Direct writer from Arrow `RecordBatch` rows into one `arrow_layout` block.
#[derive(Debug)]
pub struct BatchPageEncoder<'schema, 'payload> {
    schema: &'schema Schema,
    block: BlockMut<'payload>,
}

impl<'schema, 'payload> BatchPageEncoder<'schema, 'payload> {
    /// Creates a new encoder over an initialized `arrow_layout` block.
    ///
    /// The supplied `schema` is the expected input schema for future
    /// [`RecordBatch`] writes. The target page layout is validated against both
    /// `schema` and `plan`.
    pub fn new(
        schema: &'schema Schema,
        plan: &LayoutPlan,
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        if schema.fields().len() != plan.column_count() {
            return Err(ConfigError::SchemaPlanColumnCountMismatch {
                schema_cols: schema.fields().len(),
                plan_cols: plan.column_count(),
            });
        }

        let block = BlockMut::open(payload)?;
        if block.column_count() != plan.column_count() {
            return Err(ConfigError::PlanBlockColumnCountMismatch {
                plan_cols: plan.column_count(),
                block_cols: block.column_count(),
            });
        }
        if block.block_size() != plan.block_size() {
            return Err(ConfigError::PlanBlockSizeMismatch {
                plan_block_size: plan.block_size(),
                block_block_size: block.block_size(),
            });
        }
        if block.max_rows() != plan.max_rows() {
            return Err(ConfigError::PlanMaxRowsMismatch {
                plan_max_rows: plan.max_rows(),
                block_max_rows: block.max_rows(),
            });
        }

        for (index, field) in schema.fields().iter().enumerate() {
            let plan_layout = plan.column_layout(index)?;
            let block_layout = block.column_layout(index)?;
            if block_layout != plan_layout {
                return Err(ConfigError::PlanBlockColumnLayoutMismatch { index });
            }
            validate_schema_column(index, field.data_type(), field.is_nullable(), plan_layout)?;
        }

        Ok(Self { schema, block })
    }

    /// Appends the largest fitting row prefix from `batch[start_row..]`.
    ///
    /// This method never partially commits a row. If the block fills, the
    /// caller should finalize it, initialize a fresh block, and retry from
    /// `start_row + rows_written`.
    ///
    /// On an empty page with variable-width data, `rows_written = 0` together
    /// with `full = true` can also mean the caller overestimated `max_rows` for
    /// this page shape. In that case the caller should retry the same input row
    /// on a fresh page with a smaller `LayoutPlan::max_rows()`.
    /// [`EncodeError::RowTooLargeForPage`] is reserved for the terminal case
    /// where the first row still does not fit on an empty page with
    /// `max_rows = 1`.
    pub fn append_batch(
        &mut self,
        batch: &RecordBatch,
        start_row: usize,
    ) -> Result<AppendResult, EncodeError> {
        if batch.schema().as_ref() != self.schema {
            return Err(EncodeError::BatchSchemaMismatch);
        }
        if start_row > batch.num_rows() {
            return Err(EncodeError::StartRowOutOfBounds {
                start_row,
                row_count: batch.num_rows(),
            });
        }

        let remaining_rows = batch.num_rows() - start_row;
        let free_rows = usize::try_from(self.block.max_rows() - self.block.row_count())
            .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?;
        let row_cap = remaining_rows.min(free_rows);
        if row_cap == 0 {
            return Ok(AppendResult {
                rows_written: 0,
                full: remaining_rows != 0,
            });
        }

        let rows_to_write = self.rows_that_fit(batch, start_row, row_cap)?;
        if rows_to_write == 0 {
            if self.block.row_count() == 0 && remaining_rows != 0 {
                let required_tail = self.row_tail_need(batch, start_row)?;
                let page_tail_capacity = self
                    .block
                    .block_size()
                    .checked_sub(self.block.pool_base())
                    .ok_or(arrow_layout::LayoutError::InvalidHeaderBounds)?;
                if required_tail > page_tail_capacity && self.block.max_rows() == 1 {
                    return Err(EncodeError::RowTooLargeForPage {
                        row: start_row,
                        required_tail,
                        page_tail_capacity,
                    });
                }
            }
            return Ok(AppendResult {
                rows_written: 0,
                full: remaining_rows != 0,
            });
        }

        let dest_start = self.block.row_count();
        for (index, field) in self.schema.fields().iter().enumerate() {
            let layout = self.block.column_layout(index)?;
            match field.data_type() {
                DataType::Boolean => {
                    self.write_boolean_column(
                        index,
                        layout,
                        batch.column(index).as_boolean(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Int16 => {
                    self.write_primitive_column::<Int16Type>(
                        index,
                        layout,
                        batch.column(index).as_primitive::<Int16Type>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Int32 => {
                    self.write_primitive_column::<Int32Type>(
                        index,
                        layout,
                        batch.column(index).as_primitive::<Int32Type>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Int64 => {
                    self.write_primitive_column::<Int64Type>(
                        index,
                        layout,
                        batch.column(index).as_primitive::<Int64Type>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Float32 => {
                    self.write_primitive_column::<Float32Type>(
                        index,
                        layout,
                        batch.column(index).as_primitive::<Float32Type>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Float64 => {
                    self.write_primitive_column::<Float64Type>(
                        index,
                        layout,
                        batch.column(index).as_primitive::<Float64Type>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::FixedSizeBinary(width) if *width == UUID_WIDTH_BYTES as i32 => {
                    self.write_uuid_column(
                        index,
                        layout,
                        batch.column(index).as_fixed_size_binary(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Utf8 => {
                    self.write_plain_bytes_column::<Utf8Type>(
                        index,
                        layout,
                        batch.column(index).as_string::<i32>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Binary => {
                    self.write_plain_bytes_column::<BinaryType>(
                        index,
                        layout,
                        batch.column(index).as_binary::<i32>(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::Utf8View => {
                    self.write_view_bytes_column::<StringViewType>(
                        index,
                        layout,
                        batch.column(index).as_string_view(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                DataType::BinaryView => {
                    self.write_view_bytes_column::<BinaryViewType>(
                        index,
                        layout,
                        batch.column(index).as_binary_view(),
                        start_row,
                        dest_start,
                        rows_to_write,
                    )?;
                }
                other => {
                    return Err(arrow_layout::LayoutError::UnsupportedArrowType {
                        index,
                        data_type: other.to_string(),
                    }
                    .into());
                }
            }
        }

        self.block.advance_row_count(
            u32::try_from(rows_to_write).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
        )?;

        Ok(AppendResult {
            rows_written: rows_to_write,
            full: rows_to_write < remaining_rows,
        })
    }

    /// Finalizes the block and returns committed row and payload counts.
    pub fn finish(self) -> Result<EncodedBatch, EncodeError> {
        self.block.validate()?;
        Ok(EncodedBatch {
            row_count: usize::try_from(self.block.row_count())
                .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
            payload_len: usize::try_from(self.block.block_size())
                .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
        })
    }

    fn rows_that_fit(
        &self,
        batch: &RecordBatch,
        start_row: usize,
        row_cap: usize,
    ) -> Result<usize, EncodeError> {
        let mut tail_remaining = self
            .block
            .tail_cursor()
            .checked_sub(self.block.pool_base())
            .ok_or(arrow_layout::LayoutError::InvalidHeaderBounds)?;

        let mut rows = 0usize;
        for row in start_row..start_row + row_cap {
            let row_tail_need = self.row_tail_need(batch, row)?;
            if row_tail_need > tail_remaining {
                break;
            }
            tail_remaining -= row_tail_need;
            rows += 1;
        }

        Ok(rows)
    }

    fn row_tail_need(&self, batch: &RecordBatch, row: usize) -> Result<u32, EncodeError> {
        let mut row_tail_need = 0u32;
        for (index, field) in self.schema.fields().iter().enumerate() {
            let Some(extra) =
                tail_need_for_row(index, batch.column(index).as_ref(), field.data_type(), row)?
            else {
                continue;
            };
            row_tail_need = row_tail_need
                .checked_add(extra)
                .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        }
        Ok(row_tail_need)
    }

    fn write_boolean_column(
        &mut self,
        index: usize,
        layout: ColumnLayout,
        array: &BooleanArray,
        start_row: usize,
        dest_start: u32,
        rows_to_write: usize,
    ) -> Result<(), EncodeError> {
        let null_count = self.copy_validity_bitmap(
            index,
            layout,
            array.nulls(),
            start_row,
            dest_start,
            rows_to_write,
        )?;
        let values = self.block.column_values_bytes_mut(index)?;
        let source = array.values();
        set_bits(
            values,
            source.values(),
            usize::try_from(dest_start).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
            source.offset() + start_row,
            rows_to_write,
        );
        self.block.add_null_count(index, null_count)?;
        Ok(())
    }

    fn write_primitive_column<T>(
        &mut self,
        index: usize,
        layout: ColumnLayout,
        array: &PrimitiveArray<T>,
        start_row: usize,
        dest_start: u32,
        rows_to_write: usize,
    ) -> Result<(), EncodeError>
    where
        T: arrow_array::types::ArrowPrimitiveType,
        T::Native: Copy,
    {
        let null_count = self.copy_validity_bitmap(
            index,
            layout,
            array.nulls(),
            start_row,
            dest_start,
            rows_to_write,
        )?;
        let src = &array.values()[start_row..start_row + rows_to_write];
        let src_bytes = native_slice_as_bytes(src);
        let row_width = size_of::<T::Native>();
        let dest_offset = usize::try_from(dest_start)
            .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?
            * row_width;
        let dest_end = dest_offset
            .checked_add(src_bytes.len())
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        self.block.column_values_bytes_mut(index)?[dest_offset..dest_end]
            .copy_from_slice(src_bytes);
        self.block.add_null_count(index, null_count)?;
        Ok(())
    }

    fn write_uuid_column(
        &mut self,
        index: usize,
        layout: ColumnLayout,
        array: &FixedSizeBinaryArray,
        start_row: usize,
        dest_start: u32,
        rows_to_write: usize,
    ) -> Result<(), EncodeError> {
        let null_count = self.copy_validity_bitmap(
            index,
            layout,
            array.nulls(),
            start_row,
            dest_start,
            rows_to_write,
        )?;
        let row_width = UUID_WIDTH_BYTES as usize;
        let src_start = start_row
            .checked_mul(row_width)
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        let src_end = src_start
            .checked_add(
                rows_to_write
                    .checked_mul(row_width)
                    .ok_or(arrow_layout::LayoutError::SizeOverflow)?,
            )
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        let dest_offset = usize::try_from(dest_start)
            .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?
            * row_width;
        let len = src_end
            .checked_sub(src_start)
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        let dest_end = dest_offset
            .checked_add(len)
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        self.block.column_values_bytes_mut(index)?[dest_offset..dest_end]
            .copy_from_slice(&array.value_data()[src_start..src_end]);
        self.block.add_null_count(index, null_count)?;
        Ok(())
    }

    fn write_plain_bytes_column<T>(
        &mut self,
        index: usize,
        layout: ColumnLayout,
        array: &GenericByteArray<T>,
        start_row: usize,
        dest_start: u32,
        rows_to_write: usize,
    ) -> Result<(), EncodeError>
    where
        T: arrow_array::types::ByteArrayType,
    {
        let null_count = self.copy_validity_bitmap(
            index,
            layout,
            array.nulls(),
            start_row,
            dest_start,
            rows_to_write,
        )?;
        for rel_row in 0..rows_to_write {
            let src_row = start_row + rel_row;
            if array.is_null(src_row) {
                continue;
            }
            let dest_row = dest_start
                .checked_add(
                    u32::try_from(rel_row).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
                )
                .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
            let bytes = array.value(src_row).as_ref();
            self.write_view_value(index, dest_row, bytes, src_row)?;
        }
        self.block.add_null_count(index, null_count)?;
        Ok(())
    }

    fn write_view_bytes_column<T>(
        &mut self,
        index: usize,
        layout: ColumnLayout,
        array: &GenericByteViewArray<T>,
        start_row: usize,
        dest_start: u32,
        rows_to_write: usize,
    ) -> Result<(), EncodeError>
    where
        T: arrow_array::types::ByteViewType + ?Sized,
    {
        let null_count = self.copy_validity_bitmap(
            index,
            layout,
            array.nulls(),
            start_row,
            dest_start,
            rows_to_write,
        )?;
        for rel_row in 0..rows_to_write {
            let src_row = start_row + rel_row;
            if array.is_null(src_row) {
                continue;
            }
            let dest_row = dest_start
                .checked_add(
                    u32::try_from(rel_row).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
                )
                .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
            let bytes = array.value(src_row).as_ref();
            self.write_view_value(index, dest_row, bytes, src_row)?;
        }
        self.block.add_null_count(index, null_count)?;
        Ok(())
    }

    fn copy_validity_bitmap(
        &mut self,
        index: usize,
        layout: ColumnLayout,
        nulls: Option<&arrow_buffer::NullBuffer>,
        start_row: usize,
        dest_start: u32,
        rows_to_write: usize,
    ) -> Result<u32, EncodeError> {
        if !layout.flags.is_nullable() {
            return Ok(0);
        }

        let dest = self.block.column_validity_bytes_mut(index)?;
        let dest_offset =
            usize::try_from(dest_start).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?;
        let null_count = match nulls {
            Some(nulls) => {
                let source = nulls.inner();
                set_bits(
                    dest,
                    source.values(),
                    dest_offset,
                    source.offset() + start_row,
                    rows_to_write,
                )
            }
            None => {
                fill_bits_true(dest, dest_offset, rows_to_write);
                0
            }
        };
        u32::try_from(null_count).map_err(|_| arrow_layout::LayoutError::SizeOverflow.into())
    }

    fn write_view_value(
        &mut self,
        index: usize,
        dest_row: u32,
        bytes: &[u8],
        src_row: usize,
    ) -> Result<(), EncodeError> {
        let view = if bytes.len() <= VIEW_INLINE_LEN {
            ByteView::new_inline(bytes)?
        } else {
            let len = u32::try_from(bytes.len()).map_err(|_| EncodeError::RowValueTooLarge {
                index,
                len: bytes.len(),
            })?;
            let Some(start) = self.block.reserve_tail(len)? else {
                return Err(EncodeError::UnexpectedFull {
                    index,
                    row: src_row,
                });
            };
            self.block
                .tail_slice_mut(start, len)?
                .copy_from_slice(bytes);
            ByteView::new_outline(bytes, start - self.block.pool_base())?
        };

        let row = usize::try_from(dest_row).map_err(|_| arrow_layout::LayoutError::SizeOverflow)?;
        let start = row
            .checked_mul(size_of::<ByteView>())
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        let end = start
            .checked_add(size_of::<ByteView>())
            .ok_or(arrow_layout::LayoutError::SizeOverflow)?;
        let bytes = unsafe {
            slice::from_raw_parts(
                (&view as *const ByteView).cast::<u8>(),
                size_of::<ByteView>(),
            )
        };
        self.block.column_values_bytes_mut(index)?[start..end].copy_from_slice(bytes);
        Ok(())
    }
}

fn validate_schema_column(
    index: usize,
    data_type: &DataType,
    nullable: bool,
    layout: ColumnLayout,
) -> Result<(), ConfigError> {
    if nullable != layout.flags.is_nullable() {
        return Err(ConfigError::SchemaPlanNullabilityMismatch {
            index,
            schema_nullable: nullable,
            layout_nullable: layout.flags.is_nullable(),
        });
    }

    let compatible = match data_type {
        DataType::Boolean => layout.type_tag == TypeTag::Boolean,
        DataType::Int16 => layout.type_tag == TypeTag::Int16,
        DataType::Int32 => layout.type_tag == TypeTag::Int32,
        DataType::Int64 => layout.type_tag == TypeTag::Int64,
        DataType::Float32 => layout.type_tag == TypeTag::Float32,
        DataType::Float64 => layout.type_tag == TypeTag::Float64,
        DataType::FixedSizeBinary(width) if *width == UUID_WIDTH_BYTES as i32 => {
            layout.type_tag == TypeTag::Uuid
        }
        DataType::Utf8 | DataType::Utf8View => layout.type_tag == TypeTag::Utf8View,
        DataType::Binary | DataType::BinaryView => layout.type_tag == TypeTag::BinaryView,
        other => {
            return Err(ConfigError::UnsupportedArrowType {
                index,
                data_type: other.clone(),
            });
        }
    };

    if compatible {
        Ok(())
    } else {
        Err(ConfigError::SchemaPlanTypeMismatch {
            index,
            data_type: data_type.to_string(),
            type_tag: layout.type_tag,
        })
    }
}

fn native_slice_as_bytes<T: Copy>(values: &[T]) -> &[u8] {
    unsafe { slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values)) }
}

fn fill_bits_true(dst: &mut [u8], offset: usize, len: usize) {
    if len == 0 {
        return;
    }

    let mut bit = offset;
    let mut remaining = len;

    let first_bit = bit % 8;
    if first_bit != 0 {
        let take = remaining.min(8 - first_bit);
        let mask = (((1u16 << take) - 1) << first_bit) as u8;
        dst[bit / 8] |= mask;
        bit += take;
        remaining -= take;
    }

    while remaining >= 8 {
        dst[bit / 8] = 0xFF;
        bit += 8;
        remaining -= 8;
    }

    if remaining != 0 {
        let mask = ((1u16 << remaining) - 1) as u8;
        dst[bit / 8] |= mask;
    }
}

fn tail_need_for_row(
    index: usize,
    array: &dyn Array,
    data_type: &DataType,
    row: usize,
) -> Result<Option<u32>, EncodeError> {
    let len = match data_type {
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            if array.is_null(row) {
                return Ok(None);
            }
            array.value(row).len()
        }
        DataType::Binary => {
            let array = array.as_binary::<i32>();
            if array.is_null(row) {
                return Ok(None);
            }
            array.value(row).len()
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            if array.is_null(row) {
                return Ok(None);
            }
            array.value(row).len()
        }
        DataType::BinaryView => {
            let array = array.as_binary_view();
            if array.is_null(row) {
                return Ok(None);
            }
            array.value(row).len()
        }
        _ => return Ok(None),
    };

    if len <= VIEW_INLINE_LEN {
        return Ok(None);
    }

    Ok(Some(u32::try_from(len).map_err(|_| {
        EncodeError::RowValueTooLarge { index, len }
    })?))
}
