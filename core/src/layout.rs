use crate::stack::TreiberStack;
use std::alloc::{Layout, LayoutError};
use std::sync::atomic::{AtomicBool, AtomicU32};

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

/// Memory layout for the lock-free byte buffer (ring) used by `LockFreeBuffer`.
/// Region contains two `AtomicU32` (head, tail) followed by a byte array of size `capacity`.
#[derive(Clone, Copy, Debug)]
pub struct BufferLayout {
    pub layout: Layout,
    pub head_offset: usize,
    pub tail_offset: usize,
    pub data_offset: usize,
    pub data_len: usize,
}

/// Compute the memory layout for a lock-free ring buffer of `capacity` bytes.
pub fn lockfree_buffer_layout(capacity: usize) -> Result<BufferLayout, LayoutError> {
    // LockFreeBuffer uses u32 indices; keep capacity within u32 range.
    assert!(capacity <= (u32::MAX as usize), "capacity exceeds u32 range");

    let head = Layout::new::<AtomicU32>();
    let tail = Layout::new::<AtomicU32>();
    let (ht, tail_offset) = head.extend(tail)?;
    let data = Layout::array::<u8>(capacity)?;
    let (combined, data_offset) = ht.extend(data)?;
    Ok(BufferLayout {
        layout: combined.pad_to_align(),
        head_offset: 0,
        tail_offset,
        data_offset,
        data_len: capacity,
    })
}

/// Given a base pointer and `BufferLayout`, return typed pointers to head, tail and data.
///
/// # Safety
/// - `base` must point to a region of at least `layout.layout.size()` bytes
///   with alignment `layout.layout.align()`.
pub unsafe fn lockfree_buffer_ptrs(
    base: *mut u8,
    layout: BufferLayout,
) -> (*mut AtomicU32, *mut AtomicU32, *mut u8) {
    let head = base.add(layout.head_offset) as *mut AtomicU32;
    let tail = base.add(layout.tail_offset) as *mut AtomicU32;
    let data = base.add(layout.data_offset) as *mut u8;
    (head, tail, data)
}

/// Layout for a Socket: one `AtomicBool` signal flag and an embedded lock-free buffer region.
#[derive(Clone, Copy, Debug)]
pub struct SocketLayout {
    pub layout: Layout,
    pub flag_offset: usize,
    pub buffer_offset: usize,
    pub buffer_layout: BufferLayout,
}

/// Compute the layout for a single socket with a buffer of `capacity` bytes.
pub fn socket_layout(capacity: usize) -> Result<SocketLayout, LayoutError> {
    let flag = Layout::new::<AtomicBool>();
    let buf = lockfree_buffer_layout(capacity)?;
    let (combined, buffer_offset) = flag.extend(buf.layout)?;
    Ok(SocketLayout {
        layout: combined.pad_to_align(),
        flag_offset: 0,
        buffer_offset,
        buffer_layout: buf,
    })
}

/// Given the base pointer and computed `SocketLayout`, return pointers to:
/// - the `AtomicBool` flag
/// - the start of the buffer region
///
/// The caller can form a `&mut [u8]` of length `layout.buffer_layout.layout.size()`
/// starting at `buffer_base` and pass it to `LockFreeBuffer::new`.
///
/// # Safety
/// - `base` must be non-null and properly aligned for `layout.layout`.
/// - Memory must be at least `layout.layout.size()` bytes long.
pub unsafe fn socket_ptrs(
    base: *mut u8,
    layout: SocketLayout,
) -> (*mut AtomicBool, *mut u8) {
    let flag_ptr = base.add(layout.flag_offset) as *mut AtomicBool;
    let buffer_base = base.add(layout.buffer_offset);
    (flag_ptr, buffer_base)
}
