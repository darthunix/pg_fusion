pub(super) use crate::shm::{pack_state, LeaseState, MAX_GENERATION};
pub(super) use crate::{
    AccessError, AcquireError, PageDescriptor, PagePool, PagePoolConfig, RegionLayout, ReleaseError,
};
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

pub(super) fn cfg(page_size: usize, page_count: u32) -> PagePoolConfig {
    PagePoolConfig::new(page_size, page_count).expect("valid page pool config")
}

pub(super) struct OwnedRegion {
    pub(super) base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    pub(super) fn new(region: RegionLayout) -> Self {
        let layout = Layout::from_size_align(region.size, region.align).expect("test layout");
        let base = unsafe { alloc(layout) };
        let base = NonNull::new(base).expect("allocation failed");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

pub(super) fn init_pool(config: PagePoolConfig) -> (OwnedRegion, PagePool) {
    let layout = PagePool::layout(config).expect("pool layout");
    let region = OwnedRegion::new(layout);
    let pool =
        unsafe { PagePool::init_in_place(region.base, layout.size, config) }.expect("pool init");
    (region, pool)
}

mod basic;
mod concurrency;
mod exhaustion;
mod ownership;

#[cfg(unix)]
mod unix;
