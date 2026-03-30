//! Layout planning from logical column specs or Arrow schema.

use crate::bitmap::bitmap_bytes;
use crate::constants::{BLOCK_MAGIC, BLOCK_VERSION, BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS};
use crate::internals::{align_up_u32_with_bias, checked_u32};
use crate::raw::{BlockHeader, ColumnDesc};
#[cfg(test)]
use crate::validate::validate_header;
use crate::{BlockFlags, ColumnLayout, ColumnSpec, LayoutError, TypeTag};
use arrow_schema::Schema;
use std::mem::size_of;

/// Full planned front-and-tail page layout for one block shape.
///
/// A `LayoutPlan` describes:
///
/// - the fixed header and column-descriptor prefix
/// - the reserved front region for all fixed-size row-indexed buffers
/// - the start of the shared tail arena used by long `ByteView` payloads
///
/// The plan is deterministic for a given schema, `max_rows`, and `block_size`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayoutPlan {
    block_size: u32,
    max_rows: u32,
    front_base: u32,
    pool_base: u32,
    columns: Vec<ColumnLayout>,
}

impl LayoutPlan {
    /// Builds a deterministic layout plan from logical column specs.
    pub fn new(specs: &[ColumnSpec], max_rows: u32, block_size: u32) -> Result<Self, LayoutError> {
        u16::try_from(specs.len()).map_err(|_| LayoutError::TooManyColumns {
            actual: specs.len(),
        })?;
        let front_base = align_up_u32_with_bias(
            checked_u32(
                size_of::<BlockHeader>()
                    .checked_add(
                        specs
                            .len()
                            .checked_mul(size_of::<ColumnDesc>())
                            .ok_or(LayoutError::SizeOverflow)?,
                    )
                    .ok_or(LayoutError::SizeOverflow)?,
            )?,
            BUFFER_ALIGNMENT,
            BUFFER_ALIGNMENT_BIAS,
        )?;

        let mut cursor = front_base;
        let mut columns = Vec::with_capacity(specs.len());
        for spec in specs {
            let flags = spec.flags();
            let validity_len =
                crate::internals::align_up_u32(bitmap_bytes(max_rows), BUFFER_ALIGNMENT)?;
            let values_len = spec.type_tag.values_reserved_len(max_rows)?;

            let validity_off = cursor;
            cursor = cursor
                .checked_add(validity_len)
                .ok_or(LayoutError::SizeOverflow)?;
            let values_off = cursor;
            cursor = cursor
                .checked_add(values_len)
                .ok_or(LayoutError::SizeOverflow)?;

            columns.push(ColumnLayout {
                type_tag: spec.type_tag,
                flags,
                validity_off,
                values_off,
                validity_len,
                values_len,
            });
        }

        if cursor > block_size {
            return Err(LayoutError::LayoutDoesNotFit {
                block_size,
                required: cursor,
            });
        }

        Ok(Self {
            block_size,
            max_rows,
            front_base,
            pool_base: cursor,
            columns,
        })
    }

    /// Builds a layout plan from a supported Arrow schema.
    pub fn from_arrow_schema(
        schema: &Schema,
        max_rows: u32,
        block_size: u32,
    ) -> Result<Self, LayoutError> {
        let mut specs = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            specs.push(ColumnSpec::new(
                TypeTag::from_arrow_data_type(index, field.data_type())?,
                field.is_nullable(),
            ));
        }
        Self::new(&specs, max_rows, block_size)
    }

    /// Reconstructs and validates a plan from a raw header plus descriptors.
    #[cfg(test)]
    pub(crate) fn validate(header: &BlockHeader, descs: &[ColumnDesc]) -> Result<Self, LayoutError> {
        validate_header(header, descs.len())?;
        if usize::from(header.col_count) != descs.len() {
            return Err(LayoutError::ColumnCountMismatch {
                expected: usize::from(header.col_count),
                actual: descs.len(),
            });
        }

        let mut specs = Vec::with_capacity(descs.len());
        for (index, desc) in descs.iter().enumerate() {
            let type_tag = desc.type_tag()?;
            let flags = desc.flags();
            if flags.is_view() != type_tag.is_view() {
                return Err(LayoutError::InconsistentViewFlag {
                    index,
                    type_tag,
                    flags: flags.bits(),
                });
            }
            specs.push(ColumnSpec::new(type_tag, flags.is_nullable()));
        }

        let plan = Self::new(&specs, header.max_rows, header.block_size)?;
        if plan.front_base != header.front_base {
            return Err(LayoutError::FrontBaseMismatch {
                expected: plan.front_base,
                actual: header.front_base,
            });
        }
        if plan.pool_base != header.pool_base {
            return Err(LayoutError::PoolBaseMismatch {
                expected: plan.pool_base,
                actual: header.pool_base,
            });
        }

        for (index, (expected, actual)) in plan.columns.iter().zip(descs).enumerate() {
            let expected = expected.desc();
            if actual.type_tag != expected.type_tag
                || actual.flags != expected.flags
                || actual.validity_off != expected.validity_off
                || actual.values_off != expected.values_off
            {
                return Err(LayoutError::ColumnDescMismatch { index });
            }
        }

        Ok(plan)
    }

    /// Builds the initial raw block header for this plan.
    pub(crate) fn block_header(&self) -> BlockHeader {
        BlockHeader {
            magic: BLOCK_MAGIC,
            version: BLOCK_VERSION,
            flags: BlockFlags::NONE.bits(),
            block_size: self.block_size,
            max_rows: self.max_rows,
            row_count: 0,
            col_count: u16::try_from(self.columns.len()).expect("validated at construction"),
            reserved0: 0,
            front_base: self.front_base,
            pool_base: self.pool_base,
            tail_cursor: self.block_size,
            reserved1: 0,
        }
    }

    /// Returns the total block size in bytes.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Returns the maximum number of rows reserved in the front region.
    pub fn max_rows(&self) -> u32 {
        self.max_rows
    }

    /// Returns the byte offset where the front region begins.
    pub fn front_base(&self) -> u32 {
        self.front_base
    }

    /// Returns the byte offset where the shared tail pool begins.
    pub fn pool_base(&self) -> u32 {
        self.pool_base
    }

    /// Returns the total shared-pool capacity `[pool_base, block_size)`.
    pub fn shared_pool_capacity(&self) -> u32 {
        self.block_size - self.pool_base
    }

    /// Returns the planned per-column layouts.
    #[cfg(test)]
    pub(crate) fn columns(&self) -> &[ColumnLayout] {
        &self.columns
    }

    /// Returns the raw column descriptors that should be written to the page.
    pub(crate) fn column_descs(&self) -> impl ExactSizeIterator<Item = ColumnDesc> + '_ {
        self.columns.iter().copied().map(ColumnLayout::desc)
    }
}
