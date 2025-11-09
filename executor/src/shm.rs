use crate::layout::{slot_block_ptr as slot_ptr_calc, slot_block_vis_ptr as vis_ptr_calc, SlotBlocksLayout};
use std::sync::{OnceLock, atomic::{AtomicPtr, Ordering}};

static SLOT_BLOCKS_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
static SLOT_BLOCKS_LAYOUT: OnceLock<SlotBlocksLayout> = OnceLock::new();
static RESULT_RING_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
static RESULT_RING_LAYOUT: OnceLock<crate::layout::BufferLayout> = OnceLock::new();

pub fn set_slot_blocks(base: *mut u8, layout: SlotBlocksLayout) {
    SLOT_BLOCKS_BASE.store(base, Ordering::Release);
    let _ = SLOT_BLOCKS_LAYOUT.set(layout);
}

fn get() -> (*mut u8, SlotBlocksLayout) {
    let base = SLOT_BLOCKS_BASE.load(Ordering::Acquire);
    assert!(!base.is_null(), "slot blocks base not set");
    let layout = *SLOT_BLOCKS_LAYOUT.get().expect("slot blocks layout not set");
    (base, layout)
}

/// Copy the heap page bytes from shared memory for the given `slot` into an owned Vec.
pub fn copy_block(slot: usize) -> Vec<u8> {
    let (base, layout) = get();
    let ptr = unsafe { slot_ptr_calc(base, layout, slot, 0) };
    let len = layout.block_len;
    unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
}

/// Copy the visibility bitmap bytes from shared memory for the given `slot` into an owned Vec.
pub fn copy_vis(slot: usize, vis_len: usize) -> Vec<u8> {
    let (base, layout) = get();
    let ptr = unsafe { vis_ptr_calc(base, layout, slot, 0) };
    let len = vis_len.min(layout.vis_bytes_per_block);
    unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
}

/// Borrow the heap page bytes from shared memory for the given `slot`.
/// Lifetime is 'static since the region lives for the process lifetime; caller
/// must ensure the producer won't mutate the slot while reading.
pub unsafe fn block_slice(slot: usize) -> &'static [u8] {
    let (base, layout) = get();
    let ptr = slot_ptr_calc(base, layout, slot, 0);
    std::slice::from_raw_parts(ptr, layout.block_len)
}

/// Borrow the visibility bitmap bytes from shared memory for the given `slot`.
/// Lifetime is 'static; see notes in `block_slice`.
pub unsafe fn vis_slice(slot: usize, vis_len: usize) -> &'static [u8] {
    let (base, layout) = get();
    let ptr = vis_ptr_calc(base, layout, slot, 0);
    let len = vis_len.min(layout.vis_bytes_per_block);
    std::slice::from_raw_parts(ptr, len)
}

pub fn set_result_ring(base: *mut u8, layout: crate::layout::BufferLayout) {
    RESULT_RING_BASE.store(base, Ordering::Release);
    let _ = RESULT_RING_LAYOUT.set(layout);
}

pub fn result_ring_writer_for(conn_offset: usize) -> crate::buffer::LockFreeBuffer<'static> {
    let base = RESULT_RING_BASE.load(Ordering::Acquire);
    assert!(!base.is_null(), "result ring base not set");
    let layout = *RESULT_RING_LAYOUT.get().expect("result ring layout not set");
    let per = layout.layout.size();
    let ptr = unsafe { base.add(conn_offset * per) };
    unsafe { crate::buffer::LockFreeBuffer::from_layout(ptr, layout) }
}
