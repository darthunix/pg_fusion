pub(super) use crate::codec::{encode_frame, FrameDecoder};
pub(super) use crate::page::{
    decode_page_header, encode_page_header, PageHeader, PAGE_HEADER_LEN,
    PAGE_HEADER_LEN as HEADER_LEN,
};
pub(super) use crate::rx::ReceiveEvent;
pub(super) use crate::tx::validate_page_size;
pub(super) use crate::wire::{decode_owned_frame, OwnedFrame, PageFrame, TRANSPORT_HEADER_LEN};
pub(super) use crate::{
    InvalidPageError, MessageKind, OutboundPage, PageRx, PageTx, PageWriter, ReceivedPage, RxError,
    TxError,
};
use page_pool::{PagePool, PagePoolConfig, RegionLayout};
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

pub(super) const TEST_KIND: MessageKind = 7;
pub(super) const TEST_FLAGS: u16 = 11;

pub(super) fn cfg(page_size: usize, page_count: u32) -> PagePoolConfig {
    PagePoolConfig::new(page_size, page_count).expect("valid pool config")
}

pub(super) struct OwnedRegion {
    pub(super) base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    pub(super) fn new(region: RegionLayout) -> Self {
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
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

mod codec;
mod tx_rx;

#[cfg(unix)]
mod unix;
