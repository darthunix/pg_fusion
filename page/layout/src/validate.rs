//! Public and internal layout validation helpers.

use crate::constants::{BLOCK_MAGIC, BLOCK_VERSION, BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS};
use crate::internals::{
    align_up_u32_with_bias, checked_u32, column_layout_from_desc, desc_offset, read_struct,
};
use crate::raw::{BlockHeader, ColumnDesc};
use crate::LayoutError;
use std::mem::size_of;

/// Validates the full block contract without allocating.
pub fn validate_block(header: &BlockHeader, descs: &[ColumnDesc]) -> Result<(), LayoutError> {
    validate_header(header, descs.len())?;
    validate_desc_layout(header, descs)
}

/// Validates the global header invariants for a block instance.
///
/// This checks the fixed block metadata only. It does not validate that the
/// provided [`ColumnDesc`] values match each other or a caller-side schema; use
/// [`crate::validate::validate_block`] or [`crate::BlockRef::open`] for full
/// block validation.
pub fn validate_header(header: &BlockHeader, desc_count: usize) -> Result<(), LayoutError> {
    if header.magic != BLOCK_MAGIC {
        return Err(LayoutError::InvalidMagic {
            expected: BLOCK_MAGIC,
            actual: header.magic,
        });
    }
    if header.version != BLOCK_VERSION {
        return Err(LayoutError::InvalidVersion {
            expected: BLOCK_VERSION,
            actual: header.version,
        });
    }
    if header.row_count > header.max_rows {
        return Err(LayoutError::RowCountExceedsMaxRows {
            row_count: header.row_count,
            max_rows: header.max_rows,
        });
    }
    if usize::from(header.col_count) != desc_count {
        return Err(LayoutError::ColumnCountMismatch {
            expected: usize::from(header.col_count),
            actual: desc_count,
        });
    }
    let expected_front_base = align_up_u32_with_bias(
        checked_u32(
            size_of::<BlockHeader>()
                .checked_add(
                    desc_count
                        .checked_mul(size_of::<ColumnDesc>())
                        .ok_or(LayoutError::SizeOverflow)?,
                )
                .ok_or(LayoutError::SizeOverflow)?,
        )?,
        BUFFER_ALIGNMENT,
        BUFFER_ALIGNMENT_BIAS,
    )?;
    if header.front_base != expected_front_base {
        return Err(LayoutError::FrontBaseMismatch {
            expected: expected_front_base,
            actual: header.front_base,
        });
    }
    if header.front_base > header.pool_base
        || header.pool_base > header.tail_cursor
        || header.tail_cursor > header.block_size
    {
        return Err(LayoutError::InvalidHeaderBounds);
    }
    if header.front_base % BUFFER_ALIGNMENT != BUFFER_ALIGNMENT_BIAS
        || header.pool_base % BUFFER_ALIGNMENT != BUFFER_ALIGNMENT_BIAS
    {
        return Err(LayoutError::MisalignedFrontRegion {
            front_base: header.front_base,
            pool_base: header.pool_base,
            alignment: BUFFER_ALIGNMENT,
        });
    }
    Ok(())
}

pub(crate) fn validate_block_prefix(
    block: &[u8],
    header: &BlockHeader,
    desc_count: usize,
) -> Result<(), LayoutError> {
    let block_size = usize::try_from(header.block_size).map_err(|_| LayoutError::SizeOverflow)?;
    if block.len() < block_size {
        return Err(LayoutError::BlockSliceTooSmall {
            required: block_size,
            actual: block.len(),
        });
    }

    let prefix_len = desc_offset(desc_count);
    if block.len() < prefix_len {
        return Err(LayoutError::BlockSliceTooSmall {
            required: prefix_len,
            actual: block.len(),
        });
    }

    validate_header(header, desc_count)
}

pub(crate) fn validate_desc_layout(
    header: &BlockHeader,
    descs: &[ColumnDesc],
) -> Result<(), LayoutError> {
    let mut cursor = header.front_base;
    for (index, desc) in descs.iter().copied().enumerate() {
        let layout = column_layout_from_desc(index, header.max_rows, desc)?;
        if desc.validity_off != cursor {
            return Err(LayoutError::ColumnDescMismatch { index });
        }
        cursor = cursor
            .checked_add(layout.validity_len)
            .ok_or(LayoutError::SizeOverflow)?;
        if desc.values_off != cursor {
            return Err(LayoutError::ColumnDescMismatch { index });
        }
        cursor = cursor
            .checked_add(layout.values_len)
            .ok_or(LayoutError::SizeOverflow)?;
        if desc.reserved0 != 0 {
            return Err(LayoutError::ColumnDescMismatch { index });
        }
    }
    if cursor != header.pool_base {
        return Err(LayoutError::PoolBaseMismatch {
            expected: cursor,
            actual: header.pool_base,
        });
    }
    Ok(())
}

pub(crate) fn validate_desc_layout_in_block(
    block: &[u8],
    header: &BlockHeader,
) -> Result<(), LayoutError> {
    let mut cursor = header.front_base;
    for index in 0..usize::from(header.col_count) {
        let desc = read_struct::<ColumnDesc>(block, desc_offset(index))?;
        let layout = column_layout_from_desc(index, header.max_rows, desc)?;
        if desc.validity_off != cursor {
            return Err(LayoutError::ColumnDescMismatch { index });
        }
        cursor = cursor
            .checked_add(layout.validity_len)
            .ok_or(LayoutError::SizeOverflow)?;
        if desc.values_off != cursor {
            return Err(LayoutError::ColumnDescMismatch { index });
        }
        cursor = cursor
            .checked_add(layout.values_len)
            .ok_or(LayoutError::SizeOverflow)?;
        if desc.reserved0 != 0 {
            return Err(LayoutError::ColumnDescMismatch { index });
        }
    }
    if cursor != header.pool_base {
        return Err(LayoutError::PoolBaseMismatch {
            expected: cursor,
            actual: header.pool_base,
        });
    }
    Ok(())
}
