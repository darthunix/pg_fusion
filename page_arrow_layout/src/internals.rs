//! Internal low-level helpers shared by the public modules.

use crate::bitmap::bitmap_bytes;
use crate::constants::BUFFER_ALIGNMENT;
use crate::raw::{BlockHeader, ColumnDesc};
use crate::{ColumnLayout, LayoutError};
use std::mem::size_of;
use std::ops::Range;
use std::ptr;

pub(crate) fn column_layout_from_desc(
    index: usize,
    max_rows: u32,
    desc: ColumnDesc,
) -> Result<ColumnLayout, LayoutError> {
    let type_tag = desc.type_tag()?;
    let flags = desc.flags();
    if flags.is_view() != type_tag.is_view() {
        return Err(LayoutError::InconsistentViewFlag {
            index,
            type_tag,
            flags: flags.bits(),
        });
    }
    let validity_len = align_up_u32(bitmap_bytes(max_rows), BUFFER_ALIGNMENT)?;
    let values_len = type_tag.values_reserved_len(max_rows)?;
    Ok(ColumnLayout {
        type_tag,
        flags,
        validity_off: desc.validity_off,
        values_off: desc.values_off,
        validity_len,
        values_len,
    })
}

pub(crate) fn desc_offset(index: usize) -> usize {
    size_of::<BlockHeader>() + (index * size_of::<ColumnDesc>())
}

pub(crate) fn byte_range(offset: u32, len: u32) -> Result<Range<usize>, LayoutError> {
    let start = usize::try_from(offset).map_err(|_| LayoutError::SizeOverflow)?;
    let len = usize::try_from(len).map_err(|_| LayoutError::SizeOverflow)?;
    let end = start.checked_add(len).ok_or(LayoutError::SizeOverflow)?;
    Ok(start..end)
}

pub(crate) fn block_range(block: &[u8], range: Range<usize>) -> Result<&[u8], LayoutError> {
    block.get(range).ok_or(LayoutError::InvalidHeaderBounds)
}

pub(crate) fn block_range_mut(
    block: &mut [u8],
    range: Range<usize>,
) -> Result<&mut [u8], LayoutError> {
    block.get_mut(range).ok_or(LayoutError::InvalidHeaderBounds)
}

pub(crate) fn read_struct<T: Copy>(block: &[u8], offset: usize) -> Result<T, LayoutError> {
    let end = offset
        .checked_add(size_of::<T>())
        .ok_or(LayoutError::SizeOverflow)?;
    let bytes = block
        .get(offset..end)
        .ok_or(LayoutError::BlockSliceTooSmall {
            required: end,
            actual: block.len(),
        })?;
    let ptr = bytes.as_ptr().cast::<T>();
    Ok(unsafe { ptr::read_unaligned(ptr) })
}

pub(crate) fn write_struct<T: Copy>(
    block: &mut [u8],
    offset: usize,
    value: T,
) -> Result<(), LayoutError> {
    let actual = block.len();
    let end = offset
        .checked_add(size_of::<T>())
        .ok_or(LayoutError::SizeOverflow)?;
    let bytes = block
        .get_mut(offset..end)
        .ok_or(LayoutError::BlockSliceTooSmall {
            required: end,
            actual,
        })?;
    let ptr = bytes.as_mut_ptr().cast::<T>();
    unsafe { ptr::write_unaligned(ptr, value) };
    Ok(())
}

pub(crate) fn align_up_u32(value: u32, alignment: u32) -> Result<u32, LayoutError> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(LayoutError::InvalidAlignment { alignment });
    }
    let mask = alignment - 1;
    value
        .checked_add(mask)
        .map(|v| v & !mask)
        .ok_or(LayoutError::SizeOverflow)
}

pub(crate) fn align_up_u32_with_bias(
    value: u32,
    alignment: u32,
    bias: u32,
) -> Result<u32, LayoutError> {
    if bias >= alignment {
        return Err(LayoutError::InvalidAlignment { alignment });
    }
    let delta = alignment - bias;
    let adjusted = value.checked_add(delta).ok_or(LayoutError::SizeOverflow)?;
    align_up_u32(adjusted, alignment)?
        .checked_sub(delta)
        .ok_or(LayoutError::SizeOverflow)
}

pub(crate) fn checked_u32(value: usize) -> Result<u32, LayoutError> {
    u32::try_from(value).map_err(|_| LayoutError::SizeOverflow)
}

pub(crate) fn aligned_mul(lhs: u32, rhs: u32) -> Result<u32, LayoutError> {
    align_up_u32(
        lhs.checked_mul(rhs).ok_or(LayoutError::SizeOverflow)?,
        BUFFER_ALIGNMENT,
    )
}
