use super::*;
use std::alloc::{alloc, alloc_zeroed, dealloc, GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Once;

struct CountingAllocator;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        TRACK_ALLOCATIONS.with(|tracking| {
            if tracking.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        TRACK_ALLOCATIONS.with(|tracking| {
            if tracking.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        TRACK_ALLOCATIONS.with(|tracking| {
            if tracking.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

struct AllocationTrackingGuard;

impl AllocationTrackingGuard {
    fn start() -> Self {
        TRACK_ALLOCATIONS.with(|tracking| assert!(!tracking.get(), "nested allocation tracking"));
        TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
        ALLOCATION_COUNT.with(|count| count.set(0));
        Self
    }
}

impl Drop for AllocationTrackingGuard {
    fn drop(&mut self) {
        TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    }
}

fn count_thread_allocations<F, T>(f: F) -> (usize, T)
where
    F: FnOnce() -> T,
{
    let _guard = AllocationTrackingGuard::start();
    let result = f();
    let allocations = ALLOCATION_COUNT.with(|count| count.get());
    (allocations, result)
}

fn assert_commit_published(outcome: CommitOutcome) {
    match outcome {
        CommitOutcome::Notified | CommitOutcome::PeerMissing => {}
        CommitOutcome::NotifyFailed(err) => {
            panic!("commit published but notify unexpectedly failed: {err}")
        }
    }
}

fn current_pid() -> i32 {
    #[cfg(unix)]
    {
        unsafe { libc::getpid() as i32 }
    }

    #[cfg(not(unix))]
    {
        1
    }
}

fn ignore_sigusr1_for_tests() {
    #[cfg(unix)]
    {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| unsafe {
            libc::signal(libc::SIGUSR1, libc::SIG_IGN);
        });
    }
}

struct TestRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl TestRegion {
    fn new_inactive(region: ControlRegionLayout) -> (Self, ControlRegion) {
        ignore_sigusr1_for_tests();
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("alloc");
        let handle =
            unsafe { ControlRegion::init_in_place(base, region.size, region) }.expect("init");
        (Self { base, layout }, handle)
    }

    fn new(region: ControlRegionLayout) -> (Self, ControlRegion) {
        let (mem, handle) = Self::new_inactive(region);
        assert_eq!(handle.activate_worker_generation(current_pid()), 1);
        (mem, handle)
    }

    fn new_inactive_filled(region: ControlRegionLayout, fill: u8) -> (Self, ControlRegion) {
        ignore_sigusr1_for_tests();
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = NonNull::new(unsafe { alloc(layout) }).expect("alloc");
        unsafe {
            std::ptr::write_bytes(base.as_ptr(), fill, layout.size());
        }
        let handle =
            unsafe { ControlRegion::init_in_place(base, region.size, region) }.expect("init");
        (Self { base, layout }, handle)
    }
}

impl Drop for TestRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

#[test]
fn init_and_attach_round_trip() {
    let region_layout = ControlRegionLayout::new(4, 64, 96).expect("layout");
    let (region_mem, _region) = TestRegion::new(region_layout);
    let wrong_base = NonNull::new(unsafe { region_mem.base.as_ptr().add(1) }).expect("base");
    let attached = unsafe { ControlRegion::attach(wrong_base, region_layout.size) };
    assert!(attached.is_err(), "wrong base pointer must not attach");

    let (region_mem, _region) = TestRegion::new(region_layout);
    let attached =
        unsafe { ControlRegion::attach(region_mem.base, region_layout.size) }.expect("attach");
    assert_eq!(attached.slot_count(), 4);
    assert_eq!(attached.backend_to_worker_capacity(), 64);
    assert_eq!(attached.worker_to_backend_capacity(), 96);
    assert_eq!(attached.region_generation(), 1);
}

#[test]
fn init_in_place_over_nonzero_memory_initializes_rings_to_known_empty_state() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new_inactive_filled(layout, 0xa5);
    assert_eq!(region.activate_worker_generation(current_pid()), 1);

    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    assert_eq!(backend.begin_session().expect("session"), 1);

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve");
        writer.payload_mut().copy_from_slice(b"init");
        assert_commit_published(writer.commit().expect("commit"));
    }

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek"), Some(&b"init"[..]));
    rx.consume_frame().expect("consume");
    assert_eq!(rx.peek_frame().expect("empty"), None);
}

#[test]
fn backend_acquire_requires_active_worker_generation() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new_inactive(layout);
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));
}

#[test]
fn backend_slot_reuse_and_release_returns_to_freelist() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);

    {
        let mut lease = BackendSlotLease::acquire(&region).expect("lease");
        assert_eq!(lease.slot_id(), 0);
        assert_eq!(lease.generation(), 1);
        assert!(lease.backend_pid() > 0);
        assert_eq!(lease.begin_session().expect("session 1"), 1);
        assert_eq!(lease.begin_session().expect("session 2"), 2);
        lease.release();
    }

    let mut lease = BackendSlotLease::acquire(&region).expect("lease after release");
    assert_eq!(lease.slot_id(), 0);
    assert_eq!(lease.session_epoch(), 2);
    assert_eq!(lease.begin_session().expect("session 3"), 3);
}

#[test]
fn reacquired_slot_has_no_active_session_until_begin_session() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);

    let mut first = BackendSlotLease::acquire(&region).expect("first lease");
    assert_eq!(first.begin_session().expect("session 1"), 1);
    first.release();

    let mut second = BackendSlotLease::acquire(&region).expect("second lease");
    assert_eq!(second.slot_id(), 0);
    assert!(matches!(
        worker.slot(second.slot_id()),
        Err(SlotError::NoActiveSession { slot_id: 0 })
    ));

    {
        let mut tx = second.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve before session");
        writer.payload_mut().copy_from_slice(b"pre!");
        assert_commit_published(writer.commit().expect("commit before session"));
    }
    assert!(matches!(
        worker.slot(second.slot_id()),
        Err(SlotError::NoActiveSession { slot_id: 0 })
    ));

    assert_eq!(second.begin_session().expect("session 2"), 2);
    let mut slot = worker.slot(second.slot_id()).expect("slot after session");
    let rx = slot.from_backend_rx().expect("rx after session");
    assert_eq!(rx.peek_frame().expect("peek after reset"), None);
}

#[test]
fn acquiring_all_slots_reports_empty() {
    let layout = ControlRegionLayout::new(2, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);

    let lease_a = BackendSlotLease::acquire(&region).expect("lease a");
    let lease_b = BackendSlotLease::acquire(&region).expect("lease b");
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    drop(lease_a);
    drop(lease_b);
}

#[test]
fn worker_rejects_bad_slot_ids() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);

    assert!(matches!(
        worker.slot(1),
        Err(SlotError::BadSlotId {
            slot_id: 1,
            slot_count: 1
        })
    ));
}

#[test]
fn worker_rejects_duplicate_slot_claims_in_same_epoch() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker_a = WorkerControl::attach(&region);
    let worker_b = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let epoch = backend.begin_session().expect("session");

    let _slot = worker_a.slot(backend.slot_id()).expect("first claim");
    assert!(matches!(
        worker_b.slot(backend.slot_id()),
        Err(SlotError::Busy {
            slot_id: 0,
            session_epoch
        }) if session_epoch == epoch
    ));
}

#[test]
fn worker_claim_retries_if_session_epoch_changes_during_claim() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    assert_eq!(backend.begin_session().expect("session 1"), 1);

    let (generation, bank_index, claimed_epoch) = region
        .claim_worker_slot_epoch_with_post_epoch_load_hook_for_test(backend.slot_id(), || {
            assert_eq!(backend.begin_session().expect("session 2"), 2);
        })
        .expect("claim after epoch rollover");

    assert_eq!(generation, 1);
    assert_eq!(bank_index, 1);
    assert_eq!(claimed_epoch, 2);

    assert!(matches!(
        backend.begin_session(),
        Err(BeginSessionError::WorkerClaimed {
            slot_id: 0,
            claimed_epoch: 2
        })
    ));
}

#[test]
fn worker_slot_drop_releases_claim_for_same_epoch() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let epoch = backend.begin_session().expect("session");

    let slot = worker.slot(backend.slot_id()).expect("first claim");
    assert_eq!(slot.session_epoch(), epoch);
    drop(slot);

    let slot = worker.slot(backend.slot_id()).expect("reclaim same epoch");
    assert_eq!(slot.session_epoch(), epoch);
}

#[test]
fn begin_session_clears_stale_frames_and_flags() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let first_epoch = backend.begin_session().expect("session 1");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(5).expect("reserve");
        writer.payload_mut().copy_from_slice(b"hello");
        assert_commit_published(writer.commit().expect("commit"));
    }

    let worker = WorkerControl::attach(&region);
    {
        let mut slot = worker.slot(backend.slot_id()).expect("worker slot");
        assert_eq!(slot.session_epoch(), first_epoch);
        let rx = slot.from_backend_rx().expect("rx");
        assert_eq!(rx.peek_frame().expect("peek"), Some(&b"hello"[..]));
    }

    let second_epoch = backend.begin_session().expect("session 2");
    assert_eq!(second_epoch, first_epoch + 1);
    let mut slot = worker.slot(backend.slot_id()).expect("worker slot");
    let rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek after reset"), None);
    assert_eq!(worker.ready_slots().next(), None);
}

#[test]
fn begin_session_rejects_live_worker_claims() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let first_epoch = backend.begin_session().expect("session 1");

    let mut slot = worker.slot(backend.slot_id()).expect("slot epoch 1");
    assert!(matches!(
        backend.begin_session(),
        Err(BeginSessionError::WorkerClaimed {
            slot_id: 0,
            claimed_epoch
        }) if claimed_epoch == first_epoch
    ));

    {
        let rx = slot.from_backend_rx().expect("rx");
        assert_eq!(rx.peek_frame().expect("empty"), None);
    }
    drop(slot);

    let second_epoch = backend.begin_session().expect("session 2");
    assert_eq!(second_epoch, first_epoch + 1);
}

#[test]
fn begin_session_rejects_live_worker_tx_and_writer() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let first_epoch = backend.begin_session().expect("session 1");

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    {
        let mut tx = slot.to_backend_tx().expect("tx");
        assert!(matches!(
            backend.begin_session(),
            Err(BeginSessionError::WorkerClaimed {
                slot_id: 0,
                claimed_epoch
            }) if claimed_epoch == first_epoch
        ));

        let writer = tx.try_reserve(0).expect("reserve");
        assert!(matches!(
            backend.begin_session(),
            Err(BeginSessionError::WorkerClaimed {
                slot_id: 0,
                claimed_epoch
            }) if claimed_epoch == first_epoch
        ));
        drop(writer);
    }
    drop(slot);

    let second_epoch = backend.begin_session().expect("session 2");
    assert_eq!(second_epoch, first_epoch + 1);
}

#[test]
fn worker_rejects_released_slot_even_if_historical_epoch_is_nonzero() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);

    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    assert_eq!(backend.begin_session().expect("session"), 1);
    backend.release();

    assert!(matches!(
        worker.slot(0),
        Err(SlotError::NoActiveSession { slot_id: 0 })
    ));
}

#[test]
fn ready_flags_and_multiple_frames_round_trip() {
    let layout = ControlRegionLayout::new(2, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(3).expect("reserve first");
        writer.payload_mut().copy_from_slice(b"one");
        assert_commit_published(writer.commit().expect("commit 1"));
    }
    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(3).expect("reserve second");
        writer.payload_mut().copy_from_slice(b"two");
        assert_commit_published(writer.commit().expect("commit 2"));
    }

    assert_eq!(
        worker.ready_slots().collect::<Vec<_>>(),
        vec![backend.slot_id()]
    );

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    {
        let mut rx = slot.from_backend_rx().expect("rx");
        assert_eq!(rx.peek_frame().expect("peek 1"), Some(&b"one"[..]));
        rx.consume_frame().expect("consume 1");
    }
    assert_eq!(
        worker.ready_slots().collect::<Vec<_>>(),
        vec![backend.slot_id()]
    );
    {
        let mut rx = slot.from_backend_rx().expect("rx");
        assert_eq!(rx.peek_frame().expect("peek 2"), Some(&b"two"[..]));
        rx.consume_frame().expect("consume 2");
        assert_eq!(rx.peek_frame().expect("empty"), None);
    }
    assert_eq!(worker.ready_slots().next(), None);
}

#[test]
fn wrap_sentinel_path_round_trips() {
    let layout = ControlRegionLayout::new(1, 32, 32).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(16).expect("reserve a");
        writer.payload_mut().copy_from_slice(b"aaaaaaaaaaaaaaaa");
        assert_commit_published(writer.commit().expect("commit a"));
    }
    {
        let mut tx = backend.to_worker_tx();
        let writer = tx.try_reserve(0).expect("reserve b");
        assert_commit_published(writer.commit().expect("commit b"));
    }

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    {
        let mut rx = slot.from_backend_rx().expect("rx");
        assert_eq!(
            rx.peek_frame().expect("peek a"),
            Some(&b"aaaaaaaaaaaaaaaa"[..])
        );
        rx.consume_frame().expect("consume a");
        assert_eq!(rx.peek_frame().expect("peek b"), Some(&b""[..]));
        rx.consume_frame().expect("consume b");
    }

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(8).expect("reserve c");
        writer.payload_mut().copy_from_slice(b"cccccccc");
        assert_commit_published(writer.commit().expect("commit c"));
    }

    let mut rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek c"), Some(&b"cccccccc"[..]));
    rx.consume_frame().expect("consume c");
}

#[test]
fn implicit_short_tail_wrap_round_trips() {
    let layout = ControlRegionLayout::new(1, 32, 32).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(16).expect("reserve a");
        writer.payload_mut().copy_from_slice(b"aaaaaaaaaaaaaaaa");
        assert_commit_published(writer.commit().expect("commit a"));
    }
    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(5).expect("reserve b");
        writer.payload_mut().copy_from_slice(b"bbbbb");
        assert_commit_published(writer.commit().expect("commit b"));
    }

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    {
        let mut rx = slot.from_backend_rx().expect("rx");
        assert_eq!(
            rx.peek_frame().expect("peek a"),
            Some(&b"aaaaaaaaaaaaaaaa"[..])
        );
        rx.consume_frame().expect("consume a");
        assert_eq!(rx.peek_frame().expect("peek b"), Some(&b"bbbbb"[..]));
        rx.consume_frame().expect("consume b");
    }

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve c");
        writer.payload_mut().copy_from_slice(b"cccc");
        assert_commit_published(writer.commit().expect("commit c"));
    }

    let mut rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek c"), Some(&b"cccc"[..]));
    rx.consume_frame().expect("consume c");
}

#[test]
fn uncommitted_writer_rolls_back() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve");
        writer.payload_mut().copy_from_slice(b"drop");
    }

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    let rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek"), None);
    assert_eq!(worker.ready_slots().next(), None);
}

#[test]
fn steady_state_operations_are_allocation_free() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let region_layout = Layout::from_size_align(layout.size, layout.align).expect("layout");
    let base = NonNull::new(unsafe { alloc_zeroed(region_layout) }).expect("alloc");
    let region = unsafe { ControlRegion::init_in_place(base, layout.size, layout) }.expect("init");
    region.activate_worker_generation(current_pid());
    let worker = WorkerControl::attach(&region);

    let (allocations, ()) = count_thread_allocations(|| {
        let mut backend = BackendSlotLease::acquire(&region).expect("backend");
        backend.begin_session().expect("session");
        {
            let mut tx = backend.to_worker_tx();
            let mut writer = tx.try_reserve(4).expect("reserve");
            writer.payload_mut().copy_from_slice(b"ping");
            assert_commit_published(writer.commit().expect("commit"));
        }
        let mut slot = worker.slot(backend.slot_id()).expect("slot");
        {
            let mut rx = slot.from_backend_rx().expect("rx");
            assert_eq!(rx.peek_frame().expect("peek"), Some(&b"ping"[..]));
            rx.consume_frame().expect("consume");
        }
        backend.release();
    });

    assert_eq!(allocations, 0);
    unsafe { dealloc(base.as_ptr(), region_layout) };
}

#[test]
fn ready_slots_reports_queued_frames_even_if_ready_flag_is_cleared() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve");
        writer.payload_mut().copy_from_slice(b"ping");
        assert_commit_published(writer.commit().expect("commit"));
    }

    region.clear_to_worker_ready_for_test(backend.slot_id());

    assert_eq!(
        worker.ready_slots().collect::<Vec<_>>(),
        vec![backend.slot_id()]
    );
}

#[test]
fn clear_does_not_publish_transient_backlog_from_offset_zero() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(8).expect("reserve first");
        writer.payload_mut().copy_from_slice(b"aaaaaaaa");
        assert_commit_published(writer.commit().expect("commit first"));
    }
    {
        let mut slot = worker.slot(backend.slot_id()).expect("slot");
        let mut rx = slot.from_backend_rx().expect("rx");
        assert_eq!(rx.peek_frame().expect("peek first"), Some(&b"aaaaaaaa"[..]));
        rx.consume_frame().expect("consume first");
    }
    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve second");
        writer.payload_mut().copy_from_slice(b"bbbb");
        assert_commit_published(writer.commit().expect("commit second"));
    }

    region.clear_backend_to_worker_with_hook_for_test(backend.slot_id(), || {
        let mut slot = worker.slot(backend.slot_id()).expect("slot during clear");
        let rx = slot.from_backend_rx().expect("rx");
        assert_eq!(rx.peek_frame().expect("peek during clear"), None);
    });
}

#[test]
fn ready_slots_ignores_worker_to_backend_backlog() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");
    WorkerControl::attach(&region).clear_worker_pid();

    {
        let mut slot = worker.slot(backend.slot_id()).expect("slot");
        let mut tx = slot.to_backend_tx().expect("tx");
        let mut writer = tx.try_reserve(4).expect("reserve");
        writer.payload_mut().copy_from_slice(b"pong");
        assert_commit_published(writer.commit().expect("commit"));
    }

    region.force_to_backend_ready_for_test(backend.slot_id(), true);
    assert_eq!(worker.ready_slots().next(), None);
}

#[test]
fn commit_notify_failed_still_publishes_frame() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let mut worker = WorkerControl::attach(&region);
    worker.set_worker_pid(i32::MAX);

    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    let outcome = {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve");
        writer.payload_mut().copy_from_slice(b"ping");
        writer.commit().expect("commit")
    };

    assert!(matches!(outcome, CommitOutcome::NotifyFailed(_)));

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek"), Some(&b"ping"[..]));
    rx.consume_frame().expect("consume");
}

#[test]
fn consume_frame_restores_ready_flag_if_writer_races_after_clear() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    backend.begin_session().expect("session");

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve first");
        writer.payload_mut().copy_from_slice(b"one!");
        assert_commit_published(writer.commit().expect("commit first"));
    }

    let mut slot = worker.slot(backend.slot_id()).expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    assert_eq!(rx.peek_frame().expect("peek first"), Some(&b"one!"[..]));
    rx.consume_frame_with_post_clear_hook(|| {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve second");
        writer.payload_mut().copy_from_slice(b"two!");
        assert_commit_published(writer.commit().expect("commit second"));
    })
    .expect("consume with hook");

    assert_eq!(
        worker.ready_slots().collect::<Vec<_>>(),
        vec![backend.slot_id()]
    );
    assert_eq!(rx.peek_frame().expect("peek second"), Some(&b"two!"[..]));
}

#[test]
fn worker_restart_switches_generation_and_invalidates_old_handles() {
    let layout = ControlRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerControl::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    assert_eq!(backend.generation(), 1);
    assert_eq!(backend.begin_session().expect("session 1"), 1);

    {
        let mut tx = backend.to_worker_tx();
        let mut writer = tx.try_reserve(4).expect("reserve old");
        writer.payload_mut().copy_from_slice(b"old!");
        assert_commit_published(writer.commit().expect("commit old"));
    }

    let mut slot = worker.slot(backend.slot_id()).expect("worker slot");
    let stale_rx = slot.from_backend_rx().expect("stale rx");
    assert_eq!(stale_rx.peek_frame().expect("peek old"), Some(&b"old!"[..]));

    let new_generation = region.activate_worker_generation(current_pid());
    assert_eq!(new_generation, 2);
    assert_eq!(worker.ready_slots().next(), None);

    assert!(matches!(
        stale_rx.peek_frame(),
        Err(WorkerRxError::Slot(SlotError::StaleGeneration {
            slot_id: 0,
            claimed_generation: 1,
            current_generation: 2
        }))
    ));
    assert!(matches!(
        backend.begin_session(),
        Err(BeginSessionError::Lease(LeaseError::StaleGeneration {
            slot_id: 0,
            claimed_generation: 1,
            current_generation: 2
        }))
    ));
    {
        let mut tx = backend.to_worker_tx();
        assert!(matches!(
            tx.try_reserve(4),
            Err(BackendTxError::Lease(LeaseError::StaleGeneration {
                slot_id: 0,
                claimed_generation: 1,
                current_generation: 2
            }))
        ));
    }

    let mut fresh_backend = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh_backend.generation(), 2);
    assert_eq!(fresh_backend.slot_id(), 0);
    assert_eq!(fresh_backend.session_epoch(), 0);
    assert_eq!(fresh_backend.begin_session().expect("fresh session"), 1);
}
