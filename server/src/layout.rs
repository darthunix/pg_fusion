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

/// Layout for SharedState flags array kept in shared memory.
#[derive(Clone, Copy, Debug)]
pub struct SharedStateLayout {
    pub layout: Layout,
}

/// Compute the layout for a SharedState with `num_flags` sockets.
pub fn shared_state_layout(num_flags: usize) -> Result<SharedStateLayout, LayoutError> {
    let flags = Layout::array::<AtomicBool>(num_flags)?;
    Ok(SharedStateLayout { layout: flags.pad_to_align() })
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
