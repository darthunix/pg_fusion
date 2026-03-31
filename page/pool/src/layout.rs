use crate::error::ConfigError;
use crate::shm::{PoolHeader, SharedMetrics, PAGE_ALIGN};
use crate::types::PagePoolConfig;
use lockfree::treiber_stack_layout;
use std::alloc::{Layout, LayoutError};
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;

#[derive(Clone, Copy)]
pub(crate) struct ComputedLayout {
    pub(crate) region: Layout,
    pub(crate) metrics_offset: usize,
    pub(crate) states_offset: usize,
    pub(crate) freelist_offset: usize,
    pub(crate) freelist_next_offset: usize,
    pub(crate) data_offset: usize,
    pub(crate) data_size: usize,
}

pub(crate) fn compute_layout_from_config(
    cfg: PagePoolConfig,
) -> Result<ComputedLayout, ConfigError> {
    compute_layout_from_values(cfg.page_size.get(), cfg.page_count.get())
}

pub(crate) fn compute_layout_from_values(
    page_size: usize,
    page_count: u32,
) -> Result<ComputedLayout, ConfigError> {
    validate_page_size(page_size)?;
    if page_count == 0 {
        return Err(ConfigError::ZeroPageCount);
    }

    let header = Layout::new::<PoolHeader>();
    let metrics = Layout::new::<SharedMetrics>();
    let states =
        Layout::array::<AtomicU64>(page_count as usize).map_err(|_| ConfigError::LayoutOverflow)?;
    let freelist = treiber_stack_layout(page_count as usize)
        .map_err(|_: LayoutError| ConfigError::LayoutOverflow)?;
    let data_size = page_size
        .checked_mul(page_count as usize)
        .ok_or(ConfigError::LayoutOverflow)?;
    let data =
        Layout::from_size_align(data_size, PAGE_ALIGN).map_err(|_| ConfigError::LayoutOverflow)?;

    let (layout, metrics_offset) = header
        .extend(metrics)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (layout, states_offset) = layout
        .extend(states)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (layout, freelist_offset) = layout
        .extend(freelist.layout)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (layout, data_offset) = layout
        .extend(data)
        .map_err(|_| ConfigError::LayoutOverflow)?;

    Ok(ComputedLayout {
        region: layout.pad_to_align(),
        metrics_offset,
        states_offset,
        freelist_offset,
        freelist_next_offset: freelist_offset + freelist.next_offset,
        data_offset,
        data_size,
    })
}

pub(crate) fn validate_region(
    base: NonNull<u8>,
    len: usize,
    expected_align: usize,
    expected_size: usize,
) -> Result<(), (usize, usize, bool)> {
    let address = base.as_ptr() as usize;
    if address % expected_align != 0 {
        return Err((expected_align, address, false));
    }
    if len < expected_size {
        return Err((expected_size, len, true));
    }
    Ok(())
}

fn validate_page_size(page_size: usize) -> Result<(), ConfigError> {
    if page_size == 0 {
        return Err(ConfigError::ZeroPageSize);
    }
    if page_size < PAGE_ALIGN {
        return Err(ConfigError::PageSizeTooSmall {
            minimum: PAGE_ALIGN,
            actual: page_size,
        });
    }
    if page_size % PAGE_ALIGN != 0 {
        return Err(ConfigError::PageSizeNotAligned {
            align: PAGE_ALIGN,
            actual: page_size,
        });
    }
    Ok(())
}
