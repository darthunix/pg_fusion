use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

/// Lock-free fixed-size Treiber stack over a pre-allocated shared memory pool.
///
/// Memory layout/contract:
/// - `next_ptr` must point to an array of `AtomicU32` of length `capacity` located
///   in shared memory. Each entry stores the index of the next node in the stack
///   (or `u32::MAX` for none).
/// - The stack itself only stores the head pointer (index + ABA tag) and a
///   best-effort length counter.
pub struct TreiberStack {
    // head packs: lower 32 bits = index, upper 32 bits = ABA tag
    head: AtomicU64,
    next: NonNull<AtomicU32>,
    capacity: usize,
    // Best-effort size to detect obvious Full/Empty misuse without scanning.
    len: AtomicUsize,
}

#[derive(Debug)]
pub enum StackError {
    Empty,
    Full,
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

    /// # Safety
    /// Caller must ensure that `next_ptr` points to a valid array of
    /// `AtomicU32` with at least `capacity` elements, all in shared memory.
    /// The stack initialization must be performed once, before other threads
    /// or processes can access it.
    pub unsafe fn new(next_ptr: *mut AtomicU32, capacity: usize) -> Self {
        assert!(
            capacity <= (u32::MAX as usize),
            "capacity exceeds u32 range"
        );

        // Build an initial stack containing all nodes [0..capacity-1]
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

        Self {
            head: AtomicU64::new(head),
            next: NonNull::new(next_ptr).expect("null next_ptr"),
            capacity,
            len: AtomicUsize::new(capacity),
        }
    }

    /// Pop an index from the stack.
    pub fn allocate(&self) -> Result<u32, StackError> {
        loop {
            let cur = self.head.load(Ordering::Acquire);
            let (idx, tag) = Self::unpack_head(cur);
            if idx == Self::NONE {
                return Err(StackError::Empty);
            }

            let next_ptr = unsafe { self.next.as_ptr().add(idx as usize) };
            let next = unsafe { (*next_ptr).load(Ordering::Relaxed) };
            let new = Self::pack_head(next, tag.wrapping_add(1));

            if self
                .head
                .compare_exchange(cur, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                // Best-effort size book-keeping
                self.len.fetch_sub(1, Ordering::Relaxed);
                return Ok(idx);
            }
        }
    }

    /// Push an index back to the stack.
    pub fn release(&self, val: u32) -> Result<(), StackError> {
        if (val as usize) >= self.capacity {
            return Err(StackError::Full);
        }

        // Fast-path capacity check (best-effort)
        if self.len.load(Ordering::Relaxed) >= self.capacity {
            return Err(StackError::Full);
        }

        loop {
            let cur = self.head.load(Ordering::Acquire);
            let (idx, tag) = Self::unpack_head(cur);

            let node_ptr = unsafe { self.next.as_ptr().add(val as usize) };
            // Link new node to current head before publishing
            unsafe { (*node_ptr).store(idx, Ordering::Relaxed) };

            let new = Self::pack_head(val, tag.wrapping_add(1));
            if self
                .head
                .compare_exchange(cur, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                // Best-effort size book-keeping
                self.len.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StackError, TreiberStack};
    use crate::layout::{treiber_stack_layout, treiber_stack_ptrs};
    use std::alloc::{alloc, dealloc};
    use std::ptr;

    #[test]
    fn treiber_stack_basic_allocate_release() {
        let capacity = 8usize;
        let layout = treiber_stack_layout(capacity).expect("layout");

        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null(), "allocation failed");

            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base, layout);

            // Construct the stack in-place in the allocated region
            let stack = TreiberStack::new(next_ptr, capacity);
            ptr::write(hdr_ptr, stack);
            let stack_ref: &TreiberStack = &*hdr_ptr;

            // Initially, the stack contains indices [0..capacity-1], top at capacity-1
            for expected in (0..capacity).rev() {
                let v = stack_ref.allocate().expect("pop");
                assert_eq!(v as usize, expected);
            }
            assert!(matches!(stack_ref.allocate(), Err(StackError::Empty)));

            // Push them back in order 0..capacity-1
            for i in 0..capacity {
                stack_ref.release(i as u32).expect("push");
            }

            // Now pop should return capacity-1..0
            for expected in (0..capacity).rev() {
                let v = stack_ref.allocate().expect("pop after push");
                assert_eq!(v as usize, expected);
            }

            // Releasing an out-of-range index must error
            assert!(matches!(
                stack_ref.release(capacity as u32),
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
            let stack = TreiberStack::new(next_ptr, capacity);
            ptr::write(hdr_ptr, stack);
            let s: &TreiberStack = &*hdr_ptr;

            // On fresh stack, push should fail with Full (already contains 1 element)
            assert!(matches!(s.release(0), Err(StackError::Full)));

            // Pop once ok, second pop is Empty
            assert!(s.allocate().is_ok());
            assert!(matches!(s.allocate(), Err(StackError::Empty)));

            // Push back ok now, then another push becomes Full
            assert!(s.release(0).is_ok());
            assert!(matches!(s.release(0), Err(StackError::Full)));

            dealloc(base, layout.layout);
        }
    }
}
