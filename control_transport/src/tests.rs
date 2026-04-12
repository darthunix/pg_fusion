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
    fn new_inactive(region: TransportRegionLayout) -> (Self, TransportRegion) {
        ignore_sigusr1_for_tests();
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("alloc");
        let handle =
            unsafe { TransportRegion::init_in_place(base, region.size, region) }.expect("init");
        (Self { base, layout }, handle)
    }

    fn new(region: TransportRegionLayout) -> (Self, TransportRegion) {
        let (mem, handle) = Self::new_inactive(region);
        assert_eq!(
            handle
                .activate_worker_generation(current_pid())
                .expect("activate"),
            1
        );
        (mem, handle)
    }

    fn new_inactive_filled(region: TransportRegionLayout, fill: u8) -> (Self, TransportRegion) {
        ignore_sigusr1_for_tests();
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = NonNull::new(unsafe { alloc(layout) }).expect("alloc");
        unsafe {
            std::ptr::write_bytes(base.as_ptr(), fill, layout.size());
        }
        let handle =
            unsafe { TransportRegion::init_in_place(base, region.size, region) }.expect("init");
        (Self { base, layout }, handle)
    }
}

impl Drop for TestRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

fn recv_exact<const N: usize, E>(result: Result<Option<usize>, E>, buf: &[u8; N], expected: &[u8])
where
    E: std::fmt::Debug,
{
    let len = result.expect("recv").expect("frame");
    assert_eq!(&buf[..len], expected);
}

#[test]
fn init_and_attach_round_trip() {
    let region_layout = TransportRegionLayout::new(4, 64, 96).expect("layout");
    let (region_mem, _region) = TestRegion::new(region_layout);
    let wrong_base = NonNull::new(unsafe { region_mem.base.as_ptr().add(1) }).expect("base");
    let attached = unsafe { TransportRegion::attach(wrong_base, region_layout.size) };
    assert!(attached.is_err(), "wrong base pointer must not attach");

    let (region_mem, _region) = TestRegion::new(region_layout);
    let attached =
        unsafe { TransportRegion::attach(region_mem.base, region_layout.size) }.expect("attach");
    assert_eq!(attached.slot_count(), 4);
    assert_eq!(attached.backend_to_worker_capacity(), 64);
    assert_eq!(attached.worker_to_backend_capacity(), 96);
    assert_eq!(attached.region_generation(), 1);
}

#[test]
fn init_in_place_over_nonzero_memory_initializes_rings_to_known_empty_state() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new_inactive_filled(layout, 0xa5);
    assert_eq!(
        region
            .activate_worker_generation(current_pid())
            .expect("activate"),
        1
    );

    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let mut tx = backend.to_worker_tx();
    assert_commit_published(tx.send_frame(b"init").expect("send"));

    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    let mut buf = [0u8; 8];
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"init");
    assert_eq!(rx.recv_frame_into(&mut buf).expect("empty"), None);
}

#[test]
fn backend_acquire_requires_active_worker_generation() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new_inactive(layout);
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));
}

#[test]
fn backend_slot_reuse_and_release_returns_to_freelist() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);

    {
        let mut lease = BackendSlotLease::acquire(&region).expect("lease");
        assert_eq!(lease.slot_id(), 0);
        assert_eq!(lease.generation(), 1);
        assert!(lease.backend_pid() > 0);
        lease.release();
    }

    let lease = BackendSlotLease::acquire(&region).expect("lease after release");
    assert_eq!(lease.slot_id(), 0);
}

#[test]
fn worker_attach_is_exclusive_and_release_is_deferred_until_worker_drop() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");

    let slot_id = backend.slot_id();
    let mut worker_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("worker slot");
    let second_err = match unsafe { worker.slot_unchecked(slot_id) } {
        Ok(_) => panic!("second claim must fail"),
        Err(err) => err,
    };
    assert!(matches!(
        second_err,
        SlotAccessError::Busy {
            slot_id: 0,
            claimed_generation: 1
        }
    ));

    backend.release();
    assert!(matches!(
        worker_slot.clear_transport(),
        Err(SlotAccessError::Released {
            slot_id: 0,
            claimed_generation: 1
        })
    ));
    assert!(matches!(
        worker_slot.from_backend_rx(),
        Err(SlotAccessError::Released {
            slot_id: 0,
            claimed_generation: 1
        })
    ));
    assert!(matches!(
        worker_slot.to_backend_tx(),
        Err(SlotAccessError::Released {
            slot_id: 0,
            claimed_generation: 1
        })
    ));
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    drop(worker_slot);
    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
}

#[test]
fn existing_worker_handles_fail_once_backend_detaches() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);

    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    assert_commit_published(backend.to_worker_tx().send_frame(b"hello").expect("send"));

    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    {
        let mut tx = slot.to_backend_tx().expect("tx");
        backend.release();
        let err = tx
            .send_frame(b"pong")
            .expect_err("tx must fail after backend release");
        assert!(matches!(
            err,
            WorkerTxError::Slot(SlotAccessError::Released {
                slot_id: 0,
                claimed_generation: 1
            })
        ));
    }

    drop(slot);

    let mut backend = BackendSlotLease::acquire(&region).expect("backend again");
    assert_commit_published(backend.to_worker_tx().send_frame(b"next").expect("send"));
    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    backend.release();
    let mut buf = [0u8; 8];
    let err = rx
        .recv_frame_into(&mut buf)
        .expect_err("rx must fail after backend release");
    assert!(matches!(
        err,
        WorkerRxError::Slot(SlotAccessError::Released {
            slot_id: 0,
            claimed_generation: 1
        })
    ));
}

#[test]
fn worker_claim_fails_if_backend_releases_during_attach() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();

    region.set_worker_claim_hook_for_tests(move || {
        backend.release();
    });

    let err = match unsafe { worker.slot_unchecked(slot_id) } {
        Ok(_) => panic!("worker attach must fail once backend detaches"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        SlotAccessError::Released {
            slot_id: 0,
            claimed_generation: 1
        }
    ));

    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
}

#[test]
fn backend_to_worker_round_trip_and_ready_slots() {
    let layout = TransportRegionLayout::new(2, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");

    let mut tx = backend.to_worker_tx();
    assert_commit_published(tx.send_frame(b"hello").expect("send"));

    assert_eq!(
        worker.ready_slots().collect::<Vec<_>>(),
        vec![backend.slot_id()]
    );

    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    assert_eq!(worker.ready_slots().next(), None);

    let mut rx = slot.from_backend_rx().expect("rx");
    let mut buf = [0u8; 8];
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"hello");
    assert_eq!(worker.ready_slots().next(), None);
}

#[test]
fn worker_to_backend_round_trip_does_not_mark_ready_slots() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");

    let mut tx = slot.to_backend_tx().expect("tx");
    assert_commit_published(tx.send_frame(b"pong").expect("send"));

    assert_eq!(worker.ready_slots().next(), None);

    let mut rx = backend.from_worker_rx();
    let mut buf = [0u8; 8];
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"pong");
}

#[test]
fn generation_switch_invalidates_old_handles_and_keeps_live_old_slots_unavailable() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");

    assert_commit_published(
        backend
            .to_worker_tx()
            .send_frame(b"stale")
            .expect("send before restart"),
    );

    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        2
    );
    assert_eq!(worker.ready_slots().next(), None);

    let mut backend_tx = backend.to_worker_tx();
    let send_err = match backend_tx.send_frame(b"x") {
        Ok(_) => panic!("stale backend unexpectedly sent"),
        Err(err) => err,
    };
    assert!(matches!(
        send_err,
        BackendTxError::Lease(LeaseError::StaleGeneration { .. })
    ));

    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    backend.release();
    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), 0);
    assert_eq!(fresh.generation(), 2);
}

#[test]
fn deactivate_generation_makes_transport_offline_until_reactivated() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    assert_eq!(worker.deactivate_generation().expect("deactivate"), 2);

    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));

    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        3
    );
    let lease = BackendSlotLease::acquire(&region).expect("lease");
    assert_eq!(lease.slot_id(), 0);
}

#[test]
fn backend_acquire_stops_once_worker_leaves_online_even_before_generation_bump() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);

    worker.set_worker_state_for_tests(crate::region::WORKER_STATE_OFFLINE);
    assert_eq!(region.region_generation(), 1);

    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));
}

#[test]
fn backend_acquire_rolls_back_if_generation_switch_happens_after_publish() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let region_for_hook = region;

    region.set_backend_acquire_publish_hook_for_tests(move || {
        region_for_hook
            .deactivate_worker_generation()
            .expect("deactivate from hook");
    });

    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));
    assert_eq!(region.region_generation(), 2);

    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        3
    );
    let lease = BackendSlotLease::acquire(&region).expect("lease after rollback");
    assert_eq!(lease.slot_id(), 0);
    assert_eq!(lease.generation(), 3);
}

#[test]
fn generation_switch_requires_local_worker_slots_to_be_detached() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");

    let activate_err = worker
        .activate_generation(current_pid())
        .expect_err("activate must fail while slot is alive");
    assert!(matches!(
        activate_err,
        WorkerLifecycleError::HandlesAlive { live_slots: 1 }
    ));

    let deactivate_err = worker
        .deactivate_generation()
        .expect_err("deactivate must fail while slot is alive");
    assert!(matches!(
        deactivate_err,
        WorkerLifecycleError::HandlesAlive { live_slots: 1 }
    ));

    drop(slot);
    backend.release();
    assert_eq!(worker.deactivate_generation().expect("deactivate"), 2);
    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        3
    );
}

#[test]
fn worker_exit_helper_clears_owned_slots_before_deactivate() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    let slot = unsafe { worker.slot_unchecked(slot_id) }.expect("slot");

    backend.release();
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    worker.release_owned_slots_for_exit();
    assert_eq!(worker.deactivate_generation().expect("deactivate"), 2);
    drop(slot);
    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        3
    );

    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
}

#[test]
fn activate_generation_sweeps_stale_worker_owners_from_dead_process() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let slot_id = backend.slot_id();

    worker.forget_local_worker_owners_for_tests();
    std::mem::forget(slot);
    backend.release();

    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        2
    );
    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), 2);
}

#[test]
fn commit_peer_missing_still_publishes_frame() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let mut worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    worker.clear_worker_pid();

    let outcome = backend.to_worker_tx().send_frame(b"hey").expect("send");
    assert!(matches!(outcome, CommitOutcome::PeerMissing));

    worker.set_worker_pid(current_pid());
    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    let mut buf = [0u8; 8];
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"hey");
}

#[test]
fn recv_frame_into_requires_large_enough_buffer_without_consuming_frame() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    assert_commit_published(backend.to_worker_tx().send_frame(b"hello").expect("send"));

    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    let mut small = [0u8; 3];
    let err = rx
        .recv_frame_into(&mut small)
        .expect_err("small buffer must fail");
    assert!(matches!(
        err,
        WorkerRxError::Ring(RxError::BufferTooSmall {
            required: 5,
            available: 3
        })
    ));

    let mut buf = [0u8; 8];
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"hello");
}

#[test]
fn steady_state_transport_ops_do_not_allocate() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = WorkerTransport::attach(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");

    let (allocations, ()) = count_thread_allocations(|| {
        let mut tx = backend.to_worker_tx();
        let _ = tx.send_frame(b"noop").expect("send");

        let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
        let mut rx = slot.from_backend_rx().expect("rx");
        let mut buf = [0u8; 8];
        recv_exact(rx.recv_frame_into(&mut buf), &buf, b"noop");
    });

    assert_eq!(allocations, 0);
}
