use crate::stack::TreiberStack;
use std::alloc::{Layout, LayoutError};
use std::sync::atomic::AtomicU32;

/// Memory layout helpers for placing the Treiber stack in a single
/// contiguous shared memory region.
///
/// The region contains, in order:
/// - a `TreiberStack` header (struct with atomics and metadata)
/// - an array `[AtomicU32; capacity]` used as the per-node `next` links
///
/// The returned `Layout` can be used to allocate/map a shared block,
/// and `next_offset` indicates where the `next` array begins relative
/// to the base pointer.
#[derive(Clone, Copy, Debug)]
pub struct StackLayout {
    pub layout: Layout,
    pub next_offset: usize,
}

/// Compute the memory layout for a Treiber stack with the given capacity.
pub fn treiber_stack_layout(capacity: usize) -> Result<StackLayout, LayoutError> {
    let header = Layout::new::<TreiberStack>();
    let next_arr = Layout::array::<AtomicU32>(capacity)?;
    let (combined, next_offset) = header.extend(next_arr)?;
    Ok(StackLayout {
        layout: combined.pad_to_align(),
        next_offset,
    })
}

/// Given a base pointer to an allocated region described by `treiber_stack_layout`,
/// return typed pointers to the header and the `next` array.
///
/// # Safety
/// - `base` must be non-null and point to a region of at least `layout.layout.size()` bytes
///   with the alignment `layout.layout.align()`.
/// - The memory must be appropriately mapped in shared memory if used across processes.
pub unsafe fn treiber_stack_ptrs(
    base: *mut u8,
    layout: StackLayout,
) -> (*mut TreiberStack, *mut AtomicU32) {
    let header_ptr = base as *mut TreiberStack;
    let next_ptr = base.add(layout.next_offset) as *mut AtomicU32;
    // The caller is responsible for constructing/initializing the header in place
    // and initializing the `next` array (or calling `TreiberStack::new`).
    (header_ptr, next_ptr)
}
