use libc::c_void;
use pgrx::pg_sys::ShmemAlloc;
use pgrx::prelude::*;
use std::cell::OnceCell;
use std::mem::{align_of, size_of};
use std::ops::Range;
use std::ptr::write;
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) const MOCKED_MAX_BACKENDS: u32 = 10;
static mut PROC_FREE_LIST_PTR: OnceCell<*mut c_void> = OnceCell::new();

/// The change of MaxBackends value requires cluster restart.
/// So, it is safe to use it as a constant on startup.
#[inline]
fn max_backends() -> u32 {
    #[cfg(not(any(test, feature = "pg_test")))]
    unsafe {
        pgrx::pg_sys::MaxBackends as u32
    }
    #[cfg(any(test, feature = "pg_test"))]
    MOCKED_MAX_BACKENDS
}

pub(crate) type SlotNumber = u32;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct SlotFreeList {
    locked: *mut AtomicBool,
    len: *mut u32,
    list: *mut [SlotNumber],
    buffer: *mut u8,
    size: usize,
}

impl SlotFreeList {
    pub(crate) fn estimated_size() -> usize {
        let Range { start, end: _ } = Self::range1();
        let Range { start: _, end } = Self::range2();
        end - start + max_backends() as usize * size_of::<SlotNumber>()
    }

    #[cfg(any(test, feature = "pg_test"))]
    pub(crate) fn buffer(&self) -> &[u8] {
        unsafe { from_raw_parts_mut(self.buffer, self.size) }
    }

    fn from_bytes(ptr: *mut u8, size: usize) -> Self {
        assert!(size >= Self::estimated_size());
        let buffer = unsafe { from_raw_parts_mut(ptr, size) };
        let locked = buffer[Self::range1()].as_mut_ptr() as *mut AtomicBool;
        let range2 = Self::range2();
        let len = buffer[range2.clone()].as_mut_ptr() as *mut u32;
        let list = unsafe {
            from_raw_parts_mut(
                buffer[range2.end..].as_ptr() as *mut SlotNumber,
                max_backends() as usize * size_of::<SlotNumber>(),
            )
        };
        Self {
            locked,
            len,
            list,
            buffer: ptr,
            size,
        }
    }

    pub(crate) fn new(ptr: *mut u8, size: usize) -> Self {
        unsafe {
            let buffer = from_raw_parts_mut(ptr, size);
            write(
                buffer[Self::range1()].as_mut_ptr() as *mut AtomicBool,
                AtomicBool::new(false),
            );
            let range2 = Self::range2();
            let len = max_backends();
            write(buffer[range2.clone()].as_mut_ptr() as *mut u32, len);
            for i in 0..len as usize {
                write(
                    buffer[range2.end + i * size_of::<SlotNumber>()
                        ..range2.end + (i + 1) * size_of::<SlotNumber>()]
                        .as_mut_ptr() as *mut SlotNumber,
                    i as u32,
                );
            }
        }
        Self::from_bytes(ptr, size)
    }

    fn range1() -> Range<usize> {
        aligned_offsets::<AtomicBool>(0)
    }

    fn range2() -> Range<usize> {
        let Range { start: _, end } = Self::range1();
        aligned_offsets::<u32>(end)
    }

    pub(crate) fn pop(&mut self) -> Option<SlotNumber> {
        unsafe {
            loop {
                if let Ok(_) = (*self.locked).compare_exchange(
                    false,
                    true,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    let len = *self.len;
                    if len == 0 {
                        (*self.locked).store(false, Ordering::Relaxed);
                        return None;
                    }

                    let list_ptr = self.list as *mut SlotNumber;
                    let slot = *list_ptr.add(len as usize - 1);
                    *self.len -= 1;
                    (*self.locked).store(false, Ordering::Relaxed);
                    return Some(slot);
                }
            }
        }
    }

    pub(crate) fn push(&mut self, slot: SlotNumber) {
        unsafe {
            loop {
                if let Ok(_) = (*self.locked).compare_exchange(
                    false,
                    true,
                    // TODO: recheck ordering
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    let len = *self.len;
                    if len >= max_backends() {
                        (*self.locked).store(false, Ordering::Release);
                        panic!("SlotFreeList is full");
                    }
                    let list_ptr = self.list as *mut SlotNumber;
                    *list_ptr.add(len as usize) = slot;
                    *self.len += 1;
                    (*self.locked).store(false, Ordering::Release);
                    break;
                }
            }
        }
    }
}

fn aligned_offsets<T>(ptr: usize) -> Range<usize> {
    let align = align_of::<T>();
    let lpad = (align - ptr % align) % align;
    ptr + lpad..ptr + lpad + size_of::<T>()
}

pub(crate) fn slot_free_list() -> SlotFreeList {
    // We initialize this pointer only once on postmaster shared
    // memory initialization, when no backends exist yet. That is
    // why no read-write concurrency problems can happen and it is
    // safe to read without any locks.
    let ptr = unsafe { *PROC_FREE_LIST_PTR.get().unwrap() };
    SlotFreeList::new(ptr as *mut u8, SlotFreeList::estimated_size())
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn init_shmem() {
    PROC_FREE_LIST_PTR
        .set(ShmemAlloc(SlotFreeList::estimated_size()))
        .unwrap();
    let free_list = slot_free_list();
    log!("DataFusion worker has initialized shared memory: {free_list:?}");
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::ptr::addr_of_mut;

    use super::*;

    static mut BUFFER: [u8; 50] = [1; 50];

    fn set_bytes(buf: &mut [u8], positions: &[usize], value: u8) {
        for &pos in positions {
            buf[pos] = value;
        }
    }

    #[pg_test]
    fn test_slot_free_list() {
        assert_eq!(SlotFreeList::estimated_size(), 48);
        let mut list = unsafe { SlotFreeList::new(addr_of_mut!(BUFFER) as *mut u8, BUFFER.len()) };
        let mut expected_buf = [0; 50];
        set_bytes(&mut expected_buf, &[1, 2, 3, 48, 49], 1);
        set_bytes(&mut expected_buf, &[4], 10);
        set_bytes(&mut expected_buf, &[8], 0);
        set_bytes(&mut expected_buf, &[12], 1);
        set_bytes(&mut expected_buf, &[16], 2);
        set_bytes(&mut expected_buf, &[20], 3);
        set_bytes(&mut expected_buf, &[24], 4);
        set_bytes(&mut expected_buf, &[28], 5);
        set_bytes(&mut expected_buf, &[32], 6);
        set_bytes(&mut expected_buf, &[36], 7);
        set_bytes(&mut expected_buf, &[40], 8);
        set_bytes(&mut expected_buf, &[44], 9);
        assert_eq!(list.buffer(), &expected_buf);
        for i in (0..max_backends()).rev() {
            assert_eq!(list.pop(), Some(i));
        }
        assert_eq!(list.pop(), None);
        list.push(1);
        assert_eq!(list.pop(), Some(1));
    }
}
