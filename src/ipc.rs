use libc::c_void;
use pgrx::pg_sys::{on_proc_exit, Datum, ShmemAlloc};
use pgrx::prelude::*;
use std::cell::OnceCell;
use std::mem::{align_of, size_of};
use std::ops::Range;
use std::ptr::write;
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicBool, Ordering};

const DATA_SIZE: usize = 8 * 1024;
static mut PROC_FREE_LIST_PTR: OnceCell<*mut c_void> = OnceCell::new();
pub(crate) static mut CURRENT_SLOT: OnceCell<SlotHandler> = OnceCell::new();

/// The change of MaxBackends value requires cluster restart.
/// So, it is safe to use it as a constant on startup.
#[inline]
fn max_backends() -> u32 {
    #[cfg(not(any(test, feature = "pg_test")))]
    unsafe {
        pgrx::pg_sys::MaxBackends as u32
    }
    #[cfg(any(test, feature = "pg_test"))]
    10
}

pub(crate) type SlotNumber = u32;

#[repr(C)]
pub(crate) struct SlotFreeList {
    locked: *mut AtomicBool,
    len: *mut u32,
    list: *mut [SlotNumber],
}

impl SlotFreeList {
    pub(crate) fn estimated_size() -> usize {
        let Range { start, end: _ } = Self::range1();
        let Range { start: _, end } = Self::range2();
        end - start + max_backends() as usize * size_of::<SlotNumber>()
    }

    pub(crate) fn from_bytes(ptr: *mut u8, size: usize) -> Self {
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
        Self { locked, len, list }
    }

    fn init(ptr: *mut u8, size: usize) {
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
    // We initialize this pointer exactly once on postmaster shared
    // memory initialization, when no backends exist yet. That is
    // why no read-write concurrency problems can happen and it is
    // safe to read without any locks.
    let ptr = unsafe { *PROC_FREE_LIST_PTR.get().unwrap() };
    SlotFreeList::from_bytes(ptr as *mut u8, SlotFreeList::estimated_size())
}

pub(crate) struct SlotHandler(SlotNumber);

impl SlotHandler {
    pub(crate) fn new() -> Self {
        let mut free_slots = slot_free_list();
        let id = free_slots.pop().unwrap();
        debug1!("Slot {} is allocated", id);
        unsafe { on_proc_exit(Some(backend_cleanup), Datum::null()) };
        SlotHandler(id)
    }

    pub(crate) fn id(&self) -> SlotNumber {
        self.0
    }
}

impl Drop for SlotHandler {
    fn drop(&mut self) {
        let id = self.id();
        let mut free_slots = slot_free_list();
        free_slots.push(id);
        debug1!("Slot {} is freed", id);
    }
}

#[repr(C)]
pub(crate) struct Slot {
    locked: *mut AtomicBool,
    data: *mut [u8; DATA_SIZE],
}

impl Slot {
    fn range1() -> Range<usize> {
        aligned_offsets::<AtomicBool>(0)
    }

    fn range2() -> Range<usize> {
        let Range { start: _, end } = Self::range1();
        aligned_offsets::<[u8; DATA_SIZE]>(end)
    }

    pub(crate) fn estimated_size() -> usize {
        let Range { start, end: _ } = Self::range1();
        let Range { start: _, end } = Self::range2();
        end - start
    }

    pub(crate) fn from_bytes(ptr: *mut u8, size: usize) -> Self {
        assert!(size >= Self::estimated_size());
        let buffer = unsafe { from_raw_parts_mut(ptr, size) };
        let locked = buffer[Self::range1()].as_mut_ptr() as *mut AtomicBool;
        let data = buffer[Self::range2()].as_mut_ptr() as *mut [u8; DATA_SIZE];
        Self { locked, data }
    }

    pub(crate) fn init(ptr: *mut u8, size: usize) {
        unsafe {
            let buffer = from_raw_parts_mut(ptr, size);
            write(
                buffer[Self::range1()].as_mut_ptr() as *mut AtomicBool,
                AtomicBool::new(false),
            );
        }
    }

    pub(crate) fn lock(&self) -> bool {
        unsafe {
            if let Ok(_) =
                (*self.locked).compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            {
                true
            } else {
                false
            }
        }
    }

    pub(crate) fn unlock(&self) {
        unsafe {
            (*self.locked).store(false, Ordering::Release);
        }
    }

    pub(crate) fn data(&self) -> &[u8; DATA_SIZE] {
        unsafe { &*self.data }
    }

    pub(crate) fn data_mut(&mut self) -> &mut [u8; DATA_SIZE] {
        unsafe { &mut *self.data }
    }
}

pub(crate) struct Bus {
    slots: *mut Slot,
}

impl Bus {
    pub(crate) fn estimated_size() -> usize {
        Slot::estimated_size() * max_backends() as usize
    }

    pub(crate) fn init(ptr: *mut u8, size: usize) -> Self {
        assert!(size >= Self::estimated_size());
        for i in 0..max_backends() as usize {
            Slot::init(
                unsafe { ptr.add(i * Slot::estimated_size()) },
                Slot::estimated_size(),
            );
        }
        let slots = ptr as *mut Slot;
        Self { slots }
    }

    pub(crate) fn from_bytes(ptr: *mut u8, size: usize) -> Self {
        let buffer = unsafe { from_raw_parts_mut(ptr, size) };
        let slots = buffer.as_mut_ptr() as *mut Slot;
        Self { slots }
    }

    pub(crate) fn slot(&mut self, id: SlotNumber) -> Option<Slot> {
        assert!(id < max_backends());
        let slot_ptr = unsafe {
            let ptr = self.slots as *mut u8;
            ptr.add(id as usize * Slot::estimated_size())
        };
        let slot = Slot::from_bytes(slot_ptr, Slot::estimated_size());
        if slot.lock() {
            Some(slot)
        } else {
            None
        }
    }
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn backend_cleanup(_code: i32, _args: Datum) {
    if let Some(slot) = CURRENT_SLOT.take() {
        drop(slot);
    }
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn init_shmem() {
    PROC_FREE_LIST_PTR
        .set(ShmemAlloc(SlotFreeList::estimated_size()))
        .unwrap();
    let ptr = unsafe { *PROC_FREE_LIST_PTR.get().unwrap() };
    SlotFreeList::init(ptr as *mut u8, SlotFreeList::estimated_size());
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::ptr::addr_of_mut;

    use super::*;

    static mut FREE_LIST_BUFFER: [u8; 50] = [1; 50];
    static mut SLOT_BUFFER: [u8; 8193] = [1; 8193];
    static mut BUS_BUFFER: [u8; 8193 * 10] = [1; 8193 * 10];

    fn set_bytes(buf: &mut [u8], positions: &[usize], value: u8) {
        for &pos in positions {
            buf[pos] = value;
        }
    }

    #[pg_test]
    fn test_slot_free_list() {
        assert_eq!(SlotFreeList::estimated_size(), 48);
        let ptr = addr_of_mut!(FREE_LIST_BUFFER) as *mut u8;
        let len = unsafe { FREE_LIST_BUFFER.len() };
        SlotFreeList::init(ptr, len);
        let mut list = SlotFreeList::from_bytes(ptr, len);
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
        unsafe {
            assert_eq!(FREE_LIST_BUFFER, expected_buf);
        }
        for i in (0..max_backends()).rev() {
            assert_eq!(list.pop(), Some(i));
        }
        assert_eq!(list.pop(), None);
        list.push(1);
        assert_eq!(list.pop(), Some(1));
    }

    #[pg_test]
    fn test_slot() {
        assert_eq!(Slot::estimated_size(), 8193);
        let ptr = addr_of_mut!(SLOT_BUFFER) as *mut u8;
        let len = unsafe { SLOT_BUFFER.len() };
        Slot::init(ptr, len);
        let mut slot = Slot::from_bytes(ptr, len);
        assert_eq!(slot.lock(), true);
        assert_eq!(slot.lock(), false);
        unsafe {
            assert_eq!(&SLOT_BUFFER[0..1], &[1]);
            write(slot.data_mut(), [1; DATA_SIZE]);
            assert_eq!(&SLOT_BUFFER[1..], [1; DATA_SIZE]);
        }
        slot.unlock();
        unsafe {
            assert_eq!(&SLOT_BUFFER[0..1], &[0]);
        }
    }

    #[pg_test]
    fn test_bus() {
        let ptr = addr_of_mut!(BUS_BUFFER) as *mut u8;
        let len = unsafe { BUS_BUFFER.len() };
        Bus::init(ptr, len);
        let slot_size = Slot::estimated_size();
        assert_eq!(Bus::estimated_size(), slot_size * 10);
        for i in 0..max_backends() as usize {
            unsafe {
                let slot_ptr = BUS_BUFFER.as_ptr().add(i * slot_size);
                assert_eq!(slot_ptr, &BUS_BUFFER[i * slot_size] as *const u8);
                assert_eq!(BUS_BUFFER[i * slot_size], 0);
            }
        }
        let mut bus = Bus::from_bytes(ptr, len);
        for i in 0..max_backends() {
            let slot = bus.slot(i);
            assert_eq!(slot.is_some(), true);
            let mut slot = slot.unwrap();
            assert_eq!(slot.lock(), false);
            slot.unlock();
            unsafe {
                assert_eq!(BUS_BUFFER[i as usize * slot_size], 0);
            }
        }
    }
}
