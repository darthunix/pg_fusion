use crate::stack::TreiberStack;
use std::alloc::{Layout, LayoutError};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32};

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
    assert!(
        capacity <= (u32::MAX as usize),
        "capacity exceeds u32 range"
    );

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

/// Layout for a per-connection result ring (reuses BufferLayout semantics).
#[inline]
pub fn result_ring_layout(capacity: usize) -> Result<BufferLayout, LayoutError> {
    lockfree_buffer_layout(capacity)
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
    let data = base.add(layout.data_offset);
    (head, tail, data)
}

/// Layout for SharedState flags array kept in shared memory.
#[derive(Clone, Copy, Debug)]
pub struct SharedStateLayout {
    pub layout: Layout,
}

/// Compute the layout for a SharedState with `num_flags` sockets.
pub fn shared_state_layout(num_flags: usize) -> Result<SharedStateLayout, LayoutError> {
    let flags = Layout::array::<AtomicBool>(num_flags)?;
    Ok(SharedStateLayout {
        layout: flags.pad_to_align(),
    })
}

/// Return a pointer to the first flag for a SharedState region.
/// The caller can create `&[AtomicBool]` of length `num_flags` from it.
/// # Safety: `base` must be valid for at least `layout.layout.size()` bytes.
pub unsafe fn shared_state_flags_ptr(base: *mut u8, _layout: SharedStateLayout) -> *mut AtomicBool {
    base as *mut AtomicBool
}

/// Layout for a Socket memory region: contains only the embedded lock-free buffer region.
#[derive(Clone, Copy, Debug)]
pub struct SocketLayout {
    pub layout: Layout,
    pub buffer_offset: usize,
    pub buffer_layout: BufferLayout,
}

/// Compute the layout for a single socket's memory region with a buffer of `capacity` bytes.
/// This function requires a `SharedStateLayout` to make the relationship explicit,
/// but the socket region itself does not include flags.
pub fn socket_layout(capacity: usize) -> Result<SocketLayout, LayoutError> {
    let buf = lockfree_buffer_layout(capacity)?;
    Ok(SocketLayout {
        layout: buf.layout,
        buffer_offset: 0,
        buffer_layout: buf,
    })
}

/// Given the base pointer and computed `SocketLayout`, return the start of the buffer region.
/// # Safety: `base` must be non-null and properly aligned; memory must be at least `layout.layout.size()` bytes long.
pub unsafe fn socket_ptrs(base: *mut u8, layout: SocketLayout) -> *mut u8 {
    base.add(layout.buffer_offset)
}

/// Layout for a `Connection` memory region: contains
/// - receive `Socket`'s embedded buffer region
/// - send `LockFreeBuffer` region
/// - client PID stored as `AtomicI32`
#[derive(Clone, Copy, Debug)]
pub struct ConnectionLayout {
    pub layout: Layout,
    pub recv_offset: usize,
    pub recv_socket_layout: SocketLayout,
    pub send_offset: usize,
    pub send_buffer_layout: BufferLayout,
    pub client_offset: usize,
}

/// Compute a layout for a connection with given receive/send buffer capacities.
pub fn connection_layout(
    recv_capacity: usize,
    send_capacity: usize,
) -> Result<ConnectionLayout, LayoutError> {
    let recv = socket_layout(recv_capacity)?;
    let send = lockfree_buffer_layout(send_capacity)?;
    let pid = Layout::new::<AtomicI32>();

    let (rs, send_offset) = recv.layout.extend(send.layout)?;
    let (combined, client_offset) = rs.extend(pid)?;

    Ok(ConnectionLayout {
        layout: combined.pad_to_align(),
        recv_offset: 0,
        recv_socket_layout: recv,
        send_offset,
        send_buffer_layout: send,
        client_offset,
    })
}

/// Return pointers to the receive buffer base, send buffer base, and client PID atomic.
/// # Safety
/// - `base` must be valid for at least `layout.layout.size()` bytes with proper alignment.
pub unsafe fn connection_ptrs(
    base: *mut u8,
    layout: ConnectionLayout,
) -> (*mut u8, *mut u8, *mut AtomicI32) {
    let recv_base = base.add(layout.recv_offset);
    let send_base = base.add(layout.send_offset);
    let client_ptr = base.add(layout.client_offset) as *mut AtomicI32;
    (recv_base, send_base, client_ptr)
}

/// Layout for a single shared server PID cell.
#[derive(Clone, Copy, Debug)]
pub struct ServerPidLayout {
    pub layout: Layout,
}

/// Compute the layout for a shared server PID cell stored as `AtomicI32`.
pub fn server_pid_layout() -> Result<ServerPidLayout, LayoutError> {
    Ok(ServerPidLayout {
        layout: Layout::new::<AtomicI32>(),
    })
}

/// Return a pointer to the shared server PID atomic cell.
/// # Safety
/// - `base` must be valid for at least `layout.layout.size()` bytes with proper alignment.
pub unsafe fn server_pid_ptr(base: *mut u8, _layout: ServerPidLayout) -> *mut AtomicI32 {
    base as *mut AtomicI32
}

/// Layout for per-slot buffers used to stage heap blocks (e.g., BLCKSZ bytes each).
/// Each slot reserves `blocks_per_slot` contiguous byte arrays of length `block_len`.
#[derive(Clone, Copy, Debug)]
pub struct SlotBlocksLayout {
    /// Total layout for the entire region holding all slots.
    pub layout: Layout,
    /// Number of slots in this region.
    pub slot_count: usize,
    /// Length of a single heap block buffer (typically BLCKSZ).
    pub block_len: usize,
    /// Number of block-sized buffers reserved per slot (e.g., 2 for double-buffering).
    pub blocks_per_slot: usize,
    /// Reserved bytes per block for the visibility bitmap stored adjacent to the block.
    pub vis_bytes_per_block: usize,
}

/// Compute a memory layout for `slot_count` slots, each with `blocks_per_slot` buffers of
/// `block_len` bytes. The memory region is a flat byte array of size
/// `slot_count * blocks_per_slot * block_len`.
pub fn slot_blocks_layout(
    slot_count: usize,
    block_len: usize,
    blocks_per_slot: usize,
) -> Result<SlotBlocksLayout, LayoutError> {
    assert!(slot_count > 0, "slot_count must be > 0");
    assert!(block_len > 0, "block_len must be > 0");
    assert!(blocks_per_slot > 0, "blocks_per_slot must be > 0");
    // Reserve visibility bitmap per block. Upper bound approximation:
    // max_offsets ~= block_len / size_of::<ItemIdData>() (ignoring header/special),
    // bitmap bytes ~= ceil(max_offsets / 8). Use block_len / 32 as a safe upper bound.
    let vis_bytes_per_block = (block_len + 31) / 32; // ceil(block_len / 32)

    // Total bytes required for all slots and buffers per slot,
    // accounting for visibility bitmap region after each block.
    let per_block = block_len
        .checked_add(vis_bytes_per_block)
        .expect("per-block size overflow");
    let per_slot = per_block
        .checked_mul(blocks_per_slot)
        .expect("per-slot size overflow");
    let total = slot_count
        .checked_mul(per_slot)
        .expect("slot buffers size overflow");
    let region = Layout::array::<u8>(total)?;
    Ok(SlotBlocksLayout {
        layout: region.pad_to_align(),
        slot_count,
        block_len,
        blocks_per_slot,
        vis_bytes_per_block,
    })
}

/// Return pointers to the first two buffers backing the given `slot` index (for double-buffering).
///
/// # Safety
/// - `base` must be valid for at least `layout.layout.size()` bytes with proper alignment.
/// - `slot` must be in `0..layout.slot_count`.
pub unsafe fn slot_blocks_ptrs(
    base: *mut u8,
    layout: SlotBlocksLayout,
    slot: usize,
) -> (*mut u8, *mut u8) {
    assert!(slot < layout.slot_count, "slot index out of range");
    assert!(layout.blocks_per_slot >= 2, "need at least 2 blocks per slot to return a pair");
    let per_block = layout.block_len + layout.vis_bytes_per_block;
    let stride = layout.blocks_per_slot * per_block;
    let slot_base = base.add(slot * stride);
    let buf0 = slot_base;
    let buf1 = slot_base.add(per_block);
    (buf0, buf1)
}

/// Return a pointer to a specific block buffer within a slot.
/// `block_idx` must be in `0..layout.blocks_per_slot`.
///
/// # Safety
/// - Same safety requirements as `slot_blocks_ptrs`.
pub unsafe fn slot_block_ptr(
    base: *mut u8,
    layout: SlotBlocksLayout,
    slot: usize,
    block_idx: usize,
) -> *mut u8 {
    assert!(slot < layout.slot_count, "slot index out of range");
    assert!(
        block_idx < layout.blocks_per_slot,
        "block index out of range"
    );
    let per_block = layout.block_len + layout.vis_bytes_per_block;
    let stride = layout.blocks_per_slot * per_block;
    let slot_base = base.add(slot * stride);
    slot_base.add(block_idx * per_block)
}

/// Return a pointer to the visibility bitmap buffer associated with `block_idx` within `slot`.
/// The caller must ensure it only reads/writes the first `layout.vis_bytes_per_block` bytes.
///
/// # Safety
/// - Same safety requirements as `slot_blocks_ptrs`.
pub unsafe fn slot_block_vis_ptr(
    base: *mut u8,
    layout: SlotBlocksLayout,
    slot: usize,
    block_idx: usize,
) -> *mut u8 {
    let block_ptr = slot_block_ptr(base, layout, slot, block_idx);
    block_ptr.add(layout.block_len)
}
