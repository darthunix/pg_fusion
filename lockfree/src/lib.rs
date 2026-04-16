use std::alloc::{Layout, LayoutError};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

/// Shared-memory header for a fixed-size Treiber stack.
///
/// The header is address-independent and may reside inside a shared region that
/// is mapped at different virtual addresses in different processes.
#[repr(C)]
pub struct TreiberStackHeader {
    head: AtomicU64,
    capacity: usize,
    len: AtomicUsize,
}

/// Lock-free fixed-size Treiber stack over a pre-allocated shared memory pool.
///
/// The handle is process-local: it stores raw pointers to the shared header and
/// the shared `next` array, but those pointers are not written into shared
/// memory.
pub struct TreiberStack {
    header: NonNull<TreiberStackHeader>,
    next: NonNull<AtomicU32>,
    capacity: usize,
}

// Safety: the handle only contains pointers to shared atomics and exposes only
// lock-free operations over them.
unsafe impl Send for TreiberStack {}
unsafe impl Sync for TreiberStack {}

#[derive(Debug)]
pub enum StackError {
    Empty,
    Full,
}

#[derive(Clone, Copy, Debug)]
pub struct StackLayout {
    pub layout: Layout,
    pub next_offset: usize,
}

/// Compute the memory layout for a Treiber stack with the given capacity.
pub fn treiber_stack_layout(capacity: usize) -> Result<StackLayout, LayoutError> {
    let header = Layout::new::<TreiberStackHeader>();
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
) -> (*mut TreiberStackHeader, *mut AtomicU32) {
    let header_ptr = base as *mut TreiberStackHeader;
    let next_ptr = base.add(layout.next_offset) as *mut AtomicU32;
    (header_ptr, next_ptr)
}

impl TreiberStack {
    const NONE: u32 = u32::MAX;

    #[inline]
    fn pack_head(idx: u32, tag: u32) -> u64 {
        ((tag as u64) << 32) | (idx as u64)
    }

    #[inline]
    fn unpack_head(word: u64) -> (u32, u32) {
        let idx = (word & 0xFFFF_FFFF) as u32;
        let tag = (word >> 32) as u32;
        (idx, tag)
    }

    #[inline]
    fn header(&self) -> &TreiberStackHeader {
        unsafe { self.header.as_ref() }
    }

    /// Initialize a stack in-place in shared memory and return a process-local handle.
    ///
    /// # Safety
    /// Caller must ensure that:
    /// - `header_ptr` and `next_ptr` point into a valid region described by
    ///   `treiber_stack_layout(capacity)`.
    /// - initialization happens exactly once before concurrent access.
    pub unsafe fn init_in_place(
        header_ptr: *mut TreiberStackHeader,
        next_ptr: *mut AtomicU32,
        capacity: usize,
    ) -> Self {
        assert!(
            capacity <= (u32::MAX as usize),
            "capacity exceeds u32 range"
        );

        for i in 0..capacity {
            let next = if i == 0 { Self::NONE } else { (i as u32) - 1 };
            (*next_ptr.add(i)).store(next, Ordering::Relaxed);
        }

        let head_idx = if capacity == 0 {
            Self::NONE
        } else {
            (capacity - 1) as u32
        };
        let head = Self::pack_head(head_idx, 0);

        std::ptr::write(
            header_ptr,
            TreiberStackHeader {
                head: AtomicU64::new(head),
                capacity,
                len: AtomicUsize::new(capacity),
            },
        );

        Self::attach(header_ptr, next_ptr)
    }

    /// Initialize an empty stack in-place in shared memory and return a
    /// process-local handle.
    ///
    /// # Safety
    /// Caller must ensure that:
    /// - `header_ptr` and `next_ptr` point into a valid region described by
    ///   `treiber_stack_layout(capacity)`.
    /// - initialization happens exactly once before concurrent access.
    pub unsafe fn init_empty_in_place(
        header_ptr: *mut TreiberStackHeader,
        next_ptr: *mut AtomicU32,
        capacity: usize,
    ) -> Self {
        assert!(
            capacity <= (u32::MAX as usize),
            "capacity exceeds u32 range"
        );

        for i in 0..capacity {
            (*next_ptr.add(i)).store(Self::NONE, Ordering::Relaxed);
        }

        std::ptr::write(
            header_ptr,
            TreiberStackHeader {
                head: AtomicU64::new(Self::pack_head(Self::NONE, 0)),
                capacity,
                len: AtomicUsize::new(0),
            },
        );

        Self::attach(header_ptr, next_ptr)
    }

    /// Attach to an already initialized stack in shared memory.
    ///
    /// # Safety
    /// Caller must ensure the shared-memory region stays valid for the returned
    /// handle and was previously initialized with `init_in_place`.
    pub unsafe fn attach(header_ptr: *mut TreiberStackHeader, next_ptr: *mut AtomicU32) -> Self {
        let header = NonNull::new(header_ptr).expect("null header_ptr");
        let capacity = header.as_ref().capacity;
        Self {
            header,
            next: NonNull::new(next_ptr).expect("null next_ptr"),
            capacity,
        }
    }

    /// Pop an index from the stack.
    pub fn allocate(&self) -> Result<u32, StackError> {
        loop {
            let cur = self.header().head.load(Ordering::Acquire);
            let (idx, tag) = Self::unpack_head(cur);
            if idx == Self::NONE {
                return Err(StackError::Empty);
            }

            let next_ptr = unsafe { self.next.as_ptr().add(idx as usize) };
            let next = unsafe { (*next_ptr).load(Ordering::Relaxed) };
            let new = Self::pack_head(next, tag.wrapping_add(1));

            if self
                .header()
                .head
                .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.header().len.fetch_sub(1, Ordering::Relaxed);
                return Ok(idx);
            }
        }
    }

    /// Push an index back to the stack.
    pub fn release(&self, val: u32) -> Result<(), StackError> {
        if (val as usize) >= self.capacity {
            return Err(StackError::Full);
        }

        if self.header().len.load(Ordering::Relaxed) >= self.capacity {
            return Err(StackError::Full);
        }

        loop {
            let cur = self.header().head.load(Ordering::Acquire);
            let (idx, tag) = Self::unpack_head(cur);

            let node_ptr = unsafe { self.next.as_ptr().add(val as usize) };
            unsafe { (*node_ptr).store(idx, Ordering::Relaxed) };

            let new = Self::pack_head(val, tag.wrapping_add(1));
            if self
                .header()
                .head
                .compare_exchange(cur, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                self.header().len.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
    }

    /// Atomically reset the stack to empty without reinitializing the shared
    /// header object in place.
    pub fn reset_empty(&self) {
        let cur = self.header().head.load(Ordering::Acquire);
        let (_, tag) = Self::unpack_head(cur);
        self.header().head.store(
            Self::pack_head(Self::NONE, tag.wrapping_add(1)),
            Ordering::Release,
        );
        self.header().len.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::{treiber_stack_layout, treiber_stack_ptrs, StackError, TreiberStack};
    use std::alloc::{alloc, dealloc};

    #[test]
    fn treiber_stack_basic_allocate_release() {
        let capacity = 8usize;
        let layout = treiber_stack_layout(capacity).expect("layout");

        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null(), "allocation failed");

            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base, layout);
            let stack = TreiberStack::init_in_place(hdr_ptr, next_ptr, capacity);

            for expected in (0..capacity).rev() {
                let v = stack.allocate().expect("pop");
                assert_eq!(v as usize, expected);
            }
            assert!(matches!(stack.allocate(), Err(StackError::Empty)));

            for i in 0..capacity {
                stack.release(i as u32).expect("push");
            }

            for expected in (0..capacity).rev() {
                let v = stack.allocate().expect("pop after push");
                assert_eq!(v as usize, expected);
            }

            assert!(matches!(
                stack.release(capacity as u32),
                Err(StackError::Full)
            ));

            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn treiber_stack_underflow_overflow_paths() {
        let capacity = 1usize;
        let layout = treiber_stack_layout(capacity).expect("layout");

        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());

            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base, layout);
            let s = TreiberStack::init_in_place(hdr_ptr, next_ptr, capacity);

            assert!(matches!(s.release(0), Err(StackError::Full)));
            assert!(s.allocate().is_ok());
            assert!(matches!(s.allocate(), Err(StackError::Empty)));
            assert!(s.release(0).is_ok());
            assert!(matches!(s.release(0), Err(StackError::Full)));

            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn treiber_stack_attach_roundtrip() {
        let capacity = 4usize;
        let layout = treiber_stack_layout(capacity).expect("layout");

        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());

            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base, layout);
            let stack = TreiberStack::init_in_place(hdr_ptr, next_ptr, capacity);

            let v = stack.allocate().expect("pop");
            assert_eq!(v, 3);

            let attached = TreiberStack::attach(hdr_ptr, next_ptr);
            attached.release(v).expect("push from attached handle");

            let v = stack.allocate().expect("pop after attach");
            assert_eq!(v, 3);

            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn treiber_stack_init_empty_starts_empty_then_accepts_pushes() {
        let capacity = 3usize;
        let layout = treiber_stack_layout(capacity).expect("layout");

        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());

            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base, layout);
            let stack = TreiberStack::init_empty_in_place(hdr_ptr, next_ptr, capacity);

            assert!(matches!(stack.allocate(), Err(StackError::Empty)));

            stack.release(1).expect("push");
            stack.release(2).expect("push");

            assert_eq!(stack.allocate().expect("pop"), 2);
            assert_eq!(stack.allocate().expect("pop"), 1);
            assert!(matches!(stack.allocate(), Err(StackError::Empty)));

            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn treiber_stack_reset_empty_discards_all_members() {
        let capacity = 3usize;
        let layout = treiber_stack_layout(capacity).expect("layout");

        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());

            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base, layout);
            let stack = TreiberStack::init_in_place(hdr_ptr, next_ptr, capacity);

            assert_eq!(stack.allocate().expect("pop"), 2);
            stack.reset_empty();
            assert!(matches!(stack.allocate(), Err(StackError::Empty)));

            stack.release(1).expect("push after reset");
            assert_eq!(stack.allocate().expect("pop after reset"), 1);

            dealloc(base, layout.layout);
        }
    }
}
