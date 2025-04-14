use libc::c_void;
use pgrx::pg_sys::{on_proc_exit, Datum, MyProcNumber, ProcNumber, ShmemAlloc};
use pgrx::prelude::*;
use std::cell::OnceCell;
use std::cmp::min;
use std::io::{Error, ErrorKind, Read, Result, Write};
use std::mem::{align_of, size_of};
use std::ops::Range;
use std::ptr::write;
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

static mut SLOT_FREE_LIST_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut BUS_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut WORKER_PID_PTR: OnceCell<*mut c_void> = OnceCell::new();
pub(crate) const INVALID_SLOT_NUMBER: SlotNumber = u32::MAX;
pub(crate) const INVALID_PROC_NUMBER: i32 = -1;
pub(crate) const DATA_SIZE: usize = 8 * 1024;
pub(crate) static mut CURRENT_SLOT: OnceCell<SlotHandler> = OnceCell::new();

/// The change of MaxBackends value requires cluster restart.
/// So, it is safe to use it as a constant on startup.
#[inline]
pub(crate) fn max_backends() -> u32 {
    #[cfg(not(any(test, feature = "pg_test")))]
    unsafe {
        pgrx::pg_sys::MaxBackends as u32
    }
    #[cfg(any(test, feature = "pg_test"))]
    10
}

#[inline]
pub(crate) fn my_slot() -> SlotNumber {
    unsafe { CURRENT_SLOT.get_or_init(SlotHandler::new).id() }
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

    #[allow(clippy::size_of_in_element_count)]
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

pub(crate) fn aligned_offsets<T>(ptr: usize) -> Range<usize> {
    let align = align_of::<T>();
    let lpad = (align - ptr % align) % align;
    ptr + lpad..ptr + lpad + size_of::<T>()
}

pub(crate) fn slot_free_list() -> SlotFreeList {
    // We initialize this pointer exactly once on postmaster shared
    // memory initialization, when no backends exist yet. That is
    // why no read-write concurrency problems can happen and it is
    // safe to read without any locks.
    let ptr = unsafe { *SLOT_FREE_LIST_PTR.get().unwrap() };
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
    holder: *mut AtomicI32,
    owner: *mut AtomicI32,
    data: *mut [u8; DATA_SIZE],
}

impl From<SlotStream> for Slot {
    fn from(value: SlotStream) -> Self {
        value.inner
    }
}

impl Slot {
    pub(crate) fn range1() -> Range<usize> {
        aligned_offsets::<AtomicBool>(0)
    }

    pub(crate) fn range2() -> Range<usize> {
        let Range { start: _, end } = Self::range1();
        aligned_offsets::<AtomicI32>(end)
    }

    pub(crate) fn range3() -> Range<usize> {
        let Range { start: _, end } = Self::range2();
        aligned_offsets::<AtomicI32>(end)
    }

    pub(crate) fn range4() -> Range<usize> {
        let Range { start: _, end } = Self::range3();
        aligned_offsets::<[u8; DATA_SIZE]>(end)
    }

    pub(crate) fn estimated_size() -> usize {
        let Range { start, end: _ } = Self::range1();
        let Range { start: _, end } = Self::range4();
        end - start
    }

    pub(crate) fn from_bytes(ptr: *mut u8, size: usize) -> Self {
        assert!(size >= Self::estimated_size());
        let buffer = unsafe { from_raw_parts_mut(ptr, size) };
        let locked = buffer[Self::range1()].as_mut_ptr() as *mut AtomicBool;
        let holder = buffer[Self::range2()].as_mut_ptr() as *mut AtomicI32;
        let owner = buffer[Self::range3()].as_mut_ptr() as *mut AtomicI32;
        let data = buffer[Self::range4()].as_mut_ptr() as *mut [u8; DATA_SIZE];
        Self {
            locked,
            holder,
            owner,
            data,
        }
    }

    pub(crate) fn init(ptr: *mut u8, size: usize) {
        unsafe {
            let buffer = from_raw_parts_mut(ptr, size);
            write(
                buffer[Self::range1()].as_mut_ptr() as *mut AtomicBool,
                AtomicBool::new(false),
            );
            write(
                buffer[Self::range2()].as_mut_ptr() as *mut AtomicI32,
                AtomicI32::new(INVALID_PROC_NUMBER),
            );
        }
    }

    pub(crate) fn holder(&self) -> i32 {
        unsafe { (*self.holder).load(Ordering::Relaxed) }
    }

    pub(crate) fn owner(&self) -> i32 {
        unsafe { (*self.owner).load(Ordering::Relaxed) }
    }

    pub(crate) fn is_locked(&self) -> bool {
        unsafe { (*self.locked).load(Ordering::Relaxed) }
    }

    pub(crate) fn lock(&self) -> bool {
        unsafe {
            if let Ok(_) =
                (*self.locked).compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            {
                (*self.holder).store(MyProcNumber, Ordering::Relaxed);
                true
            } else {
                false
            }
        }
    }

    pub(crate) fn unlock(&self) {
        unsafe {
            (*self.holder).store(INVALID_PROC_NUMBER, Ordering::Release);
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

impl Drop for Slot {
    fn drop(&mut self) {
        if self.is_locked() {
            self.unlock();
        }
    }
}

pub(crate) struct SlotStream {
    pos: usize,
    inner: Slot,
}

impl SlotStream {
    pub(crate) fn rewind(&mut self, len: usize) -> Result<()> {
        if len > DATA_SIZE - self.pos {
            return Err(Error::new(ErrorKind::InvalidInput, "Cannot rewind"));
        }
        self.pos += len;
        Ok(())
    }

    pub(crate) fn look_ahead(&self, len: usize) -> Result<&[u8]> {
        if len > DATA_SIZE - self.pos {
            return Err(Error::new(ErrorKind::InvalidInput, "Cannot look ahead"));
        }
        Ok(&self.inner.data()[self.pos..self.pos + len])
    }

    pub(crate) fn position(&self) -> usize {
        self.pos
    }

    pub(crate) fn reset(&mut self) {
        self.pos = 0;
    }
}

impl From<Slot> for SlotStream {
    fn from(value: Slot) -> Self {
        SlotStream {
            pos: 0,
            inner: value,
        }
    }
}

impl Write for SlotStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let len = buf.len();
        if len > DATA_SIZE - self.pos {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Data size exceeds slot capacity",
            ));
        }
        let slot_data = &mut self.inner.data_mut()[self.pos..self.pos + len];
        slot_data.copy_from_slice(buf);
        self.pos += len;
        Ok(len)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Read for SlotStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let remaining = DATA_SIZE - self.pos;
        let len = min(remaining, buf.len());
        let slot_data = &self.inner.data()[self.pos..self.pos + len];
        buf.copy_from_slice(slot_data);
        self.pos += len;
        Ok(len)
    }
}

unsafe impl Send for SlotStream {}

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

    pub(crate) fn slot(&mut self, id: SlotNumber) -> Slot {
        assert!(id < max_backends());
        let slot_ptr = unsafe {
            let ptr = self.slots as *mut u8;
            ptr.add(id as usize * Slot::estimated_size())
        };
        Slot::from_bytes(slot_ptr, Slot::estimated_size())
    }

    pub(crate) fn slot_locked(&mut self, id: SlotNumber) -> Option<Slot> {
        let slot = self.slot(id);
        if slot.lock() {
            Some(slot)
        } else {
            None
        }
    }

    pub(crate) fn new() -> Self {
        let ptr = unsafe { *BUS_PTR.get().unwrap() };
        Self::from_bytes(ptr as *mut u8, Self::estimated_size())
    }

    pub(crate) fn into_iter(self) -> BusIter {
        BusIter::from(self)
    }
}

pub(crate) struct BusIter {
    pos: SlotNumber,
    inner: Bus,
}

impl From<Bus> for BusIter {
    fn from(value: Bus) -> Self {
        Self {
            pos: 0,
            inner: value,
        }
    }
}

impl Iterator for BusIter {
    type Item = Option<Slot>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= max_backends() {
            return None;
        }
        let res = self.inner.slot_locked(self.pos);
        self.pos += 1;
        Some(res)
    }
}

pub(crate) fn worker_id() -> ProcNumber {
    let ptr = unsafe { *WORKER_PID_PTR.get().unwrap() as *mut AtomicI32 };
    unsafe { (*ptr).load(Ordering::Relaxed) }
}

pub(crate) fn set_worker_id(id: ProcNumber) {
    let ptr = unsafe { *WORKER_PID_PTR.get().unwrap() as *mut AtomicI32 };
    unsafe { (*ptr).store(id, Ordering::Relaxed) }
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn backend_cleanup(_code: i32, _args: Datum) {
    let mut bus = Bus::new();
    if let Some(handler) = CURRENT_SLOT.take() {
        let slot = bus.slot(handler.id());
        if slot.owner() == slot.holder() {
            slot.unlock();
        }

        drop(handler);
    }
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn init_shmem() {
    SLOT_FREE_LIST_PTR
        .set(ShmemAlloc(SlotFreeList::estimated_size()))
        .unwrap();
    let list_ptr = unsafe { *SLOT_FREE_LIST_PTR.get().unwrap() };
    SlotFreeList::init(list_ptr as *mut u8, SlotFreeList::estimated_size());

    BUS_PTR.set(ShmemAlloc(Bus::estimated_size())).unwrap();
    let bus_ptr = unsafe { *BUS_PTR.get().unwrap() };
    Bus::init(bus_ptr as *mut u8, Bus::estimated_size());

    WORKER_PID_PTR
        .set(ShmemAlloc(size_of::<AtomicI32>()))
        .unwrap();
    set_worker_id(INVALID_PROC_NUMBER);
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
pub(crate) mod tests {
    use pgrx::prelude::*;
    use std::ptr::addr_of_mut;

    use super::*;

    pub const SLOT_SIZE: usize = 8204;
    static mut FREE_LIST_BUFFER: [u8; 50] = [1; 50];

    fn set_bytes(buf: &mut [u8], positions: &[usize], value: u8) {
        for &pos in positions {
            buf[pos] = value;
        }
    }

    #[inline(always)]
    pub(crate) fn make_slot(bytes: &mut [u8]) -> Slot {
        let ptr = addr_of_mut!(*bytes) as *mut u8;
        Slot::init(ptr, bytes.len());
        Slot::from_bytes(ptr, bytes.len())
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
        assert_eq!(Slot::estimated_size(), SLOT_SIZE);
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut slot = make_slot(&mut slot_buf);
        assert!(slot.lock());
        assert!(!slot.lock());
        unsafe {
            assert_eq!(&slot_buf[Slot::range1()], &[1]);
            write(slot.data_mut(), [1; DATA_SIZE]);
            assert_eq!(&slot_buf[Slot::range4()], [1; DATA_SIZE]);
        }
        slot.unlock();
        assert_eq!(&slot_buf[Slot::range1()], &[0]);
    }

    #[pg_test]
    fn test_slot_stream() {
        let mut buffer: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let slot = make_slot(&mut buffer);
        slot.lock();
        let mut stream = SlotStream::from(slot);
        let data = [42; 10];
        let len = stream.write(&data).unwrap();
        assert_eq!(len, 10);
        let len = stream.write(&data).unwrap();
        assert_eq!(len, 10);
        let slot = Slot::from(stream);
        assert!(slot.is_locked());
        assert_eq!(slot.data()[0..10], data);
        assert_eq!(slot.data()[10..20], data);
        assert_eq!(slot.data()[20..30], [1; 10]);
        let mut stream = SlotStream::from(slot);
        let mut consumed: [u8; 10] = [0; 10];
        let len = stream.read(&mut consumed).unwrap();
        assert_eq!(len, 10);
        assert_eq!(data, consumed);
        let len = stream.read(&mut consumed).unwrap();
        assert_eq!(len, 10);
        assert_eq!(data, consumed);
        let len = stream.read(&mut consumed).unwrap();
        assert_eq!(len, 10);
        assert_eq!([1; 10], consumed);

        // Test slot is unlocked on drop
        {
            let slot = Slot::from(stream);
            assert!(slot.is_locked());
        }
        let ptr = addr_of_mut!(buffer) as *mut u8;
        let slot = Slot::from_bytes(ptr, buffer.len());
        assert!(!slot.is_locked());
    }

    #[pg_test]
    fn test_bus() {
        let mut buffer: [u8; SLOT_SIZE * 10] = [1; SLOT_SIZE * 10];
        let ptr = addr_of_mut!(buffer) as *mut u8;
        let len = buffer.len();
        Bus::init(ptr, len);
        let slot_size = Slot::estimated_size();
        assert_eq!(Bus::estimated_size(), slot_size * 10);
        for i in 0..max_backends() as usize {
            unsafe {
                let slot_ptr = buffer.as_ptr().add(i * slot_size);
                assert_eq!(slot_ptr, &buffer[i * slot_size] as *const u8);
                assert_eq!(buffer[i * slot_size], 0);
            }
        }
        let mut bus = Bus::from_bytes(ptr, len);
        for i in 0..max_backends() {
            let slot = bus.slot_locked(i);
            assert!(slot.is_some());
            let slot = slot.unwrap();
            assert!(!slot.lock());
            slot.unlock();
            assert_eq!(buffer[i as usize * slot_size], 0);
        }
    }
}
