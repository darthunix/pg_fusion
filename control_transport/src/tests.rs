use super::*;
use crate::process::{
    clear_probe_hook_for_tests, clear_signal_hook_for_tests, set_probe_hook_for_tests,
    set_signal_hook_for_tests,
};
use crate::ring::{framed_ring_layout, FramedRing, FramedRingLayout};
use std::alloc::{alloc, alloc_zeroed, dealloc, GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32};
use std::sync::{Mutex, MutexGuard, Once, OnceLock};

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

fn attach_worker(region: &TransportRegion) -> WorkerTransport {
    WorkerTransport::attach(region).expect("worker attach")
}

fn test_region_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

thread_local! {
    static TEST_REGION_GUARD_DEPTH: Cell<usize> = const { Cell::new(0) };
    static TEST_REGION_GUARD: RefCell<Option<MutexGuard<'static, ()>>> = const { RefCell::new(None) };
}

struct TestRegionGuard;

impl TestRegionGuard {
    fn acquire() -> Self {
        TEST_REGION_GUARD_DEPTH.with(|depth| {
            if depth.get() == 0 {
                let guard = test_region_mutex()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                TEST_REGION_GUARD.with(|slot| {
                    *slot.borrow_mut() = Some(guard);
                });
            }
            depth.set(depth.get() + 1);
        });
        Self
    }
}

impl Drop for TestRegionGuard {
    fn drop(&mut self) {
        TEST_REGION_GUARD_DEPTH.with(|depth| {
            let current = depth.get();
            assert!(current > 0, "test region guard underflow");
            let next = current - 1;
            depth.set(next);
            if next == 0 {
                TransportRegion::clear_all_worker_owner_registry_for_tests();
                TransportRegion::clear_current_process_pid_for_tests();
                TEST_REGION_GUARD.with(|slot| {
                    let _ = slot.borrow_mut().take();
                });
            }
        });
    }
}

struct TestRegion {
    base: NonNull<u8>,
    layout: Layout,
    _guard: TestRegionGuard,
}

impl TestRegion {
    fn new_inactive(region: TransportRegionLayout) -> (Self, TransportRegion) {
        ignore_sigusr1_for_tests();
        let guard = TestRegionGuard::acquire();
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("alloc");
        let handle =
            unsafe { TransportRegion::init_in_place(base, region.size, region) }.expect("init");
        (
            Self {
                base,
                layout,
                _guard: guard,
            },
            handle,
        )
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
        let guard = TestRegionGuard::acquire();
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = NonNull::new(unsafe { alloc(layout) }).expect("alloc");
        unsafe {
            std::ptr::write_bytes(base.as_ptr(), fill, layout.size());
        }
        let handle =
            unsafe { TransportRegion::init_in_place(base, region.size, region) }.expect("init");
        (
            Self {
                base,
                layout,
                _guard: guard,
            },
            handle,
        )
    }
}

impl Drop for TestRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

struct TestFramedRing {
    base: NonNull<u8>,
    layout: Layout,
    ring_layout: FramedRingLayout,
    ready: AtomicBool,
    peer_pid: AtomicI32,
}

impl TestFramedRing {
    fn new(capacity: usize) -> Self {
        let ring_layout = framed_ring_layout(capacity).expect("ring layout");
        let base = NonNull::new(unsafe { alloc_zeroed(ring_layout.layout) }).expect("ring alloc");
        unsafe {
            FramedRing::init_empty_in_place(base.as_ptr(), ring_layout);
        }
        Self {
            base,
            layout: ring_layout.layout,
            ring_layout,
            ready: AtomicBool::new(false),
            peer_pid: AtomicI32::new(0),
        }
    }

    fn ring(&self) -> FramedRing<'_> {
        unsafe { FramedRing::from_layout(self.base.as_ptr(), self.ring_layout) }
    }
}

impl Drop for TestFramedRing {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

struct ProcessHookGuard;

impl ProcessHookGuard {
    fn with_probe<F>(hook: F) -> Self
    where
        F: Fn(i32) -> io::Result<bool> + 'static,
    {
        clear_probe_hook_for_tests();
        clear_signal_hook_for_tests();
        set_probe_hook_for_tests(hook);
        Self
    }

    fn with_signal<F>(hook: F) -> Self
    where
        F: Fn(i32) -> Result<bool, NotifyError> + 'static,
    {
        clear_probe_hook_for_tests();
        clear_signal_hook_for_tests();
        set_signal_hook_for_tests(hook);
        Self
    }
}

impl Drop for ProcessHookGuard {
    fn drop(&mut self) {
        clear_probe_hook_for_tests();
        clear_signal_hook_for_tests();
    }
}

struct PidOverrideGuard;

impl PidOverrideGuard {
    fn new() -> Self {
        TransportRegion::clear_current_process_pid_for_tests();
        Self
    }

    fn set(&self, pid: i32) {
        TransportRegion::set_current_process_pid_for_tests(pid);
    }
}

impl Drop for PidOverrideGuard {
    fn drop(&mut self) {
        TransportRegion::clear_current_process_pid_for_tests();
    }
}

#[cfg(debug_assertions)]
struct LifecycleTransitionHookGuard {
    region: TransportRegion,
}

#[cfg(debug_assertions)]
impl LifecycleTransitionHookGuard {
    fn new<F>(region: &TransportRegion, hook: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        region.clear_lifecycle_transition_hook_for_tests();
        region.set_lifecycle_transition_hook_for_tests(hook);
        Self { region: *region }
    }
}

#[cfg(debug_assertions)]
impl Drop for LifecycleTransitionHookGuard {
    fn drop(&mut self) {
        self.region.clear_lifecycle_transition_hook_for_tests();
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
fn init_in_place_rejects_already_initialized_region() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, _region) = TestRegion::new_inactive(layout);

    let err = match unsafe { TransportRegion::init_in_place(mem.base, layout.size, layout) } {
        Ok(_) => panic!("second init must be rejected"),
        Err(err) => err,
    };
    assert!(matches!(err, InitError::AlreadyInitialized));
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

    let worker = attach_worker(&region);
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
fn worker_attach_allows_same_region_multiple_times() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);

    let first = WorkerTransport::attach(&region).expect("first attach");
    let second = WorkerTransport::attach(&region).expect("second attach");

    assert_eq!(first.ready_slots().next(), None);
    assert_eq!(second.ready_slots().next(), None);
}

#[test]
fn worker_attach_rejects_different_region_in_same_process() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem1, region1) = TestRegion::new(layout);
    let (mem2, region2) = TestRegion::new(layout);

    let _attached = WorkerTransport::attach(&region1).expect("first attach");
    let err = match WorkerTransport::attach(&region2) {
        Ok(_) => panic!("different region must be rejected"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        WorkerAttachError::RegionAlreadyAttached {
            existing_region_key,
            requested_region_key,
        } if existing_region_key == mem1.base.as_ptr() as usize
            && requested_region_key == mem2.base.as_ptr() as usize
    ));
}

#[test]
fn worker_attach_resets_registry_after_pid_change() {
    let pid = PidOverrideGuard::new();

    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem1, region1) = TestRegion::new(layout);
    let (_mem2, region2) = TestRegion::new(layout);

    let _attached = WorkerTransport::attach(&region1).expect("first attach");

    pid.set(2002);
    let attached = WorkerTransport::attach(&region2).expect("attach after simulated fork");
    assert_eq!(attached.ready_slots().next(), None);
}

#[test]
fn pid_change_clears_inherited_local_worker_owners() {
    let pid = PidOverrideGuard::new();

    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");

    backend.release();
    std::mem::forget(slot);

    pid.set(2002);
    let worker = WorkerTransport::attach(&region).expect("attach after simulated fork");
    assert_eq!(
        worker
            .activate_generation(current_pid())
            .expect("activate after simulated fork"),
        2
    );
}

#[test]
fn reinit_in_place_rejects_different_layout() {
    let _guard = TestRegionGuard::acquire();
    ignore_sigusr1_for_tests();

    let small = TransportRegionLayout::new(1, 64, 64).expect("small layout");
    let large = TransportRegionLayout::new(2, 64, 64).expect("large layout");
    let layout = Layout::from_size_align(large.size, large.align).expect("alloc layout");
    let base = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("alloc");

    let region_small =
        unsafe { TransportRegion::init_in_place(base, large.size, small) }.expect("init small");
    assert_eq!(region_small.slot_count(), 1);

    let err = match unsafe { TransportRegion::reinit_in_place(base, large.size, large) } {
        Ok(_) => panic!("different layout reinit must fail"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        ReinitError::LayoutMismatch {
            existing,
            requested,
        } if existing == small && requested == large
    ));

    TransportRegion::clear_all_worker_owner_registry_for_tests();
    unsafe { dealloc(base.as_ptr(), layout) };
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
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);

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
    let worker = attach_worker(&region);
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
fn worker_claim_pending_blocks_same_generation_reuse_until_rollback() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    let region_for_hook = region;

    region.set_worker_claim_hook_for_tests(move || {
        backend.release();
        assert!(matches!(
            BackendSlotLease::acquire(&region_for_hook),
            Err(AcquireError::Empty)
        ));
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
fn worker_claim_surfaces_unexpected_backend_probe_error() {
    let probe_err = io::Error::from_raw_os_error(libc::EIO);
    let expected_kind = probe_err.kind();
    let _hooks = ProcessHookGuard::with_probe(|_pid| Err(io::Error::from_raw_os_error(libc::EIO)));
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let backend = BackendSlotLease::acquire(&region).expect("backend");

    let err = match unsafe { worker.slot_unchecked(backend.slot_id()) } {
        Ok(_) => panic!("worker attach must fail when backend liveness probe fails"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        SlotAccessError::BackendProbeFailed {
            slot_id: 0,
            claimed_generation: 1,
            error_kind,
            raw_os_error: Some(code),
        } if error_kind == expected_kind && code == libc::EIO
    ));
}

#[test]
fn backend_to_worker_round_trip_and_ready_slots() {
    let layout = TransportRegionLayout::new(2, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);

    worker.set_worker_state_for_tests(crate::region::WORKER_STATE_OFFLINE);
    assert_eq!(region.region_generation(), 1);

    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));
}

#[test]
fn backend_acquire_requires_online_generation_not_restarting() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);

    worker.set_worker_state_for_tests(crate::region::WORKER_STATE_RESTARTING);
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
    let worker = attach_worker(&region);
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
fn backend_acquire_rolls_back_if_activate_happens_after_publish() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let region_for_hook = region;

    region.set_backend_acquire_publish_hook_for_tests(move || {
        region_for_hook
            .activate_worker_generation(current_pid())
            .expect("activate from hook");
    });

    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline)
    ));
    assert_eq!(region.region_generation(), 2);

    let lease = BackendSlotLease::acquire(&region).expect("lease after activate-side rollback");
    assert_eq!(lease.slot_id(), 0);
    assert_eq!(lease.generation(), 2);
}

#[test]
fn activate_generation_reaps_dead_old_generation_backend_owner() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();

    assert_eq!(
        worker.activate_generation(current_pid()).expect("activate"),
        2
    );
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    // Simulate a backend from the old generation that died after restart
    // without running its release hook.
    std::mem::forget(backend);
    let _hooks = ProcessHookGuard::with_probe(|_pid| Ok(false));

    assert_eq!(
        worker
            .activate_generation(current_pid())
            .expect("activate again"),
        3
    );

    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), 3);
}

#[test]
fn generation_switch_requires_local_worker_slots_to_be_detached() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);
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
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let slot_id = backend.slot_id();

    worker.forget_local_worker_owners_for_tests();
    // Simulate a dead worker process that leaked the slot attachment after its
    // process-local registry was already lost.
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
    let mut worker = attach_worker(&region);
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
fn commit_esrch_still_publishes_frame() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let mut worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    worker.set_worker_pid(i32::MAX);

    let outcome = backend.to_worker_tx().send_frame(b"hey").expect("send");
    assert!(matches!(outcome, CommitOutcome::PeerMissing));

    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    let mut buf = [0u8; 8];
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"hey");
}

#[test]
fn signal_hook_can_force_peer_missing() {
    let _hooks = ProcessHookGuard::with_signal(|_pid| Ok(false));
    let pid = std::sync::atomic::AtomicI32::new(current_pid());
    assert!(!crate::ring::signal_peer(&pid).expect("hooked signal"));
}

#[test]
fn signal_hooks_are_thread_local() {
    ignore_sigusr1_for_tests();
    let _main_hooks = ProcessHookGuard::with_signal(|_pid| Ok(true));
    let pid = current_pid();

    let worker = std::thread::spawn(move || {
        let _thread_hooks = ProcessHookGuard::with_signal(|_pid| Ok(false));
        let pid = std::sync::atomic::AtomicI32::new(pid);
        crate::ring::signal_peer(&pid).expect("hooked signal from spawned thread")
    });

    assert!(
        !worker.join().expect("spawned signal thread"),
        "spawned thread must observe only its local hook"
    );

    let pid = std::sync::atomic::AtomicI32::new(current_pid());
    assert!(
        crate::ring::signal_peer(&pid).expect("hooked signal from main thread"),
        "main thread hook must remain installed"
    );
}

#[test]
fn backend_acquire_reaps_dead_backend_owner_before_reporting_empty() {
    let _hooks = ProcessHookGuard::with_probe(|_pid| Ok(false));
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);

    let backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    // Simulate a dead backend process that never reached its explicit release
    // hook, leaving only shared-memory ownership behind.
    std::mem::forget(backend);

    let fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), 1);
}

#[test]
fn backend_acquire_surfaces_probe_failure_during_reap() {
    let _hooks = ProcessHookGuard::with_probe(|_pid| Err(io::Error::from_raw_os_error(libc::EIO)));
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);

    let backend = BackendSlotLease::acquire(&region).expect("backend");
    std::mem::forget(backend);

    let err = match BackendSlotLease::acquire(&region) {
        Ok(_) => panic!("probe failure must surface"),
        Err(err) => err,
    };
    match err {
        AcquireError::BackendProbeFailed {
            slot_id,
            raw_os_error,
            ..
        } => {
            assert_eq!(slot_id, 0);
            assert_eq!(raw_os_error, Some(libc::EIO));
        }
        other => panic!("unexpected acquire error: {other:?}"),
    }
}

#[test]
fn worker_claim_reaps_dead_backend_owner_before_attach() {
    let _hooks = ProcessHookGuard::with_probe(|_pid| Ok(false));
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);

    let backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    // Simulate a dead backend process that leaked the shared-memory owner bit.
    std::mem::forget(backend);

    let err = match unsafe { worker.slot_unchecked(slot_id) } {
        Ok(_) => panic!("stale backend must not attach"),
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
fn reinit_in_place_retains_old_backend_owned_slot_until_release() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let mut old_backend = BackendSlotLease::acquire(&region).expect("old backend");
    let old_generation = old_backend.generation();
    let old_epoch = old_backend.lease_epoch_for_tests();
    let slot_id = old_backend.slot_id();

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let worker = attach_worker(&reinitialized);
    let fresh_generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");
    assert!(fresh_generation > old_generation);
    assert!(matches!(
        BackendSlotLease::acquire(&reinitialized),
        Err(AcquireError::Empty)
    ));

    old_backend.release();

    let fresh_backend = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh_backend.slot_id(), slot_id);
    assert_eq!(fresh_backend.generation(), fresh_generation);
    assert!(
        fresh_backend.lease_epoch_for_tests() > old_epoch,
        "reinit must preserve monotonic lease identity",
    );
}

#[test]
fn reinit_in_place_makes_old_worker_handle_stale_but_does_not_free_slot() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut old_backend = BackendSlotLease::acquire(&region).expect("old backend");
    let slot_id = old_backend.slot_id();
    let mut old_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("old slot");

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let fresh_worker = attach_worker(&reinitialized);
    let fresh_generation = fresh_worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let err = match old_slot.from_backend_rx() {
        Ok(_) => panic!("old worker handle must go stale after reinit"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        SlotAccessError::StaleGeneration {
            slot_id: 0,
            claimed_generation: 1,
            current_generation,
        } if current_generation == fresh_generation
    ));
    assert!(matches!(
        BackendSlotLease::acquire(&reinitialized),
        Err(AcquireError::Empty)
    ));

    old_backend.release();
    let fresh_backend = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh_backend.generation(), fresh_generation);
}

#[test]
fn stale_worker_drop_after_reinit_is_harmless_with_retention() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut old_backend = BackendSlotLease::acquire(&region).expect("old backend");
    let slot_id = old_backend.slot_id();
    let old_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("old slot");

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let fresh_worker = attach_worker(&reinitialized);
    let fresh_generation = fresh_worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    drop(old_slot);

    assert!(matches!(
        BackendSlotLease::acquire(&reinitialized),
        Err(AcquireError::Empty)
    ));

    old_backend.release();
    let reacquired = BackendSlotLease::acquire(&reinitialized).expect("reacquire");
    assert_eq!(reacquired.generation(), fresh_generation);
}

#[test]
fn reinit_keeps_already_free_slots_reusable() {
    let layout = TransportRegionLayout::new(2, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let mut old_backend = BackendSlotLease::acquire(&region).expect("old backend");
    let retained_slot = old_backend.slot_id();

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let worker = attach_worker(&reinitialized);
    let fresh_generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let first_fresh = BackendSlotLease::acquire(&reinitialized).expect("first fresh backend");
    assert_eq!(first_fresh.generation(), fresh_generation);
    assert_ne!(first_fresh.slot_id(), retained_slot);
    assert!(matches!(
        BackendSlotLease::acquire(&reinitialized),
        Err(AcquireError::Empty)
    ));

    old_backend.release();

    let second_fresh = BackendSlotLease::acquire(&reinitialized).expect("second fresh backend");
    assert_eq!(second_fresh.slot_id(), retained_slot);
    assert_eq!(second_fresh.generation(), fresh_generation);
}

#[test]
fn reinit_plus_activate_reaps_dead_old_backend_owner() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let backend = BackendSlotLease::acquire(&region).expect("old backend");
    let slot_id = backend.slot_id();

    std::mem::forget(backend);
    let _hooks = ProcessHookGuard::with_probe(|_pid| Ok(false));

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let worker = attach_worker(&reinitialized);
    let fresh_generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let fresh = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), fresh_generation);
}

#[test]
fn reinit_recovers_slot_popped_from_freelist_before_lease_publish() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);

    let slot_id = region.acquire_slot_without_publish_for_tests();
    assert_eq!(slot_id, 0);
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let worker = attach_worker(&reinitialized);
    let generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let fresh = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), generation);
}

#[test]
fn reinit_recovers_slot_finalized_before_freelist_push() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    std::mem::forget(backend);

    region.force_slot_free_pending_without_publish_for_tests(slot_id);
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::Empty)
    ));

    let reinitialized =
        unsafe { TransportRegion::reinit_in_place(mem.base, layout.size, layout) }.expect("reinit");
    let worker = attach_worker(&reinitialized);
    let generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let fresh = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), generation);
}

#[test]
fn reinit_waits_for_live_popped_token_and_stale_acquire_cannot_claim_rebuilt_slot() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let actor_alive = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let backend_pid = current_pid();

    let region_for_acquire = region;
    let entered_for_acquire = entered.clone();
    let release_for_acquire = release.clone();
    let acquire_thread = std::thread::spawn(move || {
        region_for_acquire.set_backend_acquire_popped_hook_for_tests(move || {
            entered_for_acquire.wait();
            release_for_acquire.wait();
        });
        BackendSlotLease::acquire(&region_for_acquire)
    });

    entered.wait();

    let base = mem.base.as_ptr() as usize;
    let actor_alive_for_reinit = actor_alive.clone();
    let reinit_thread = std::thread::spawn(move || {
        set_probe_hook_for_tests(move |pid| {
            if pid == backend_pid {
                return Ok(actor_alive_for_reinit.load(std::sync::atomic::Ordering::Acquire));
            }
            Ok(false)
        });
        let result = unsafe {
            TransportRegion::reinit_in_place(
                NonNull::new(base as *mut u8).expect("nonnull base"),
                layout.size,
                layout,
            )
        };
        clear_probe_hook_for_tests();
        result
    });

    while !region.is_reiniting_for_tests() {
        std::thread::yield_now();
    }

    actor_alive.store(false, std::sync::atomic::Ordering::Release);
    release.wait();

    let acquire_result = acquire_thread.join().expect("acquire thread");
    assert!(matches!(acquire_result, Err(AcquireError::WorkerOffline)));

    let reinitialized = reinit_thread
        .join()
        .expect("reinit thread")
        .expect("reinit");
    let worker = attach_worker(&reinitialized);
    let generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let fresh = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh.slot_id(), 0);
    assert_eq!(fresh.generation(), generation);
    assert!(matches!(
        BackendSlotLease::acquire(&reinitialized),
        Err(AcquireError::Empty)
    ));
}

#[test]
fn reinit_republishes_free_pending_created_after_rebuild_pass() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));

    let region_for_reinit = region;
    let entered_for_reinit = entered.clone();
    let release_for_reinit = release.clone();
    let base = mem.base.as_ptr() as usize;
    let reinit_thread = std::thread::spawn(move || {
        region_for_reinit.set_reinit_rebuild_pass_hook_for_tests(move || {
            entered_for_reinit.wait();
            release_for_reinit.wait();
        });
        unsafe {
            TransportRegion::reinit_in_place(
                NonNull::new(base as *mut u8).expect("nonnull base"),
                layout.size,
                layout,
            )
        }
    });

    entered.wait();
    assert!(region.is_reiniting_for_tests());

    backend.release();
    assert!(matches!(
        BackendSlotLease::acquire(&region),
        Err(AcquireError::WorkerOffline | AcquireError::Empty)
    ));

    release.wait();

    let reinitialized = reinit_thread
        .join()
        .expect("reinit thread")
        .expect("reinit");
    let worker = attach_worker(&reinitialized);
    let generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let fresh = BackendSlotLease::acquire(&reinitialized).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    assert_eq!(fresh.generation(), generation);
}

#[test]
fn reinit_waits_for_live_post_push_publication_before_resetting_freelist() {
    let layout = TransportRegionLayout::new(2, 64, 64).expect("layout");
    let (mem, region) = TestRegion::new(layout);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let backend_pid = current_pid();

    let region_for_release = region;
    let entered_for_release = entered.clone();
    let release_for_release = release.clone();
    let release_thread = std::thread::spawn(move || {
        region_for_release.set_free_slot_pushed_hook_for_tests(move || {
            entered_for_release.wait();
            release_for_release.wait();
        });
        backend.release();
    });

    entered.wait();

    let base = mem.base.as_ptr() as usize;
    let reinit_thread = std::thread::spawn(move || {
        set_probe_hook_for_tests(move |pid| Ok(pid == backend_pid));
        let result = unsafe {
            TransportRegion::reinit_in_place(
                NonNull::new(base as *mut u8).expect("nonnull base"),
                layout.size,
                layout,
            )
        };
        clear_probe_hook_for_tests();
        result
    });

    while !region.is_reiniting_for_tests() {
        std::thread::yield_now();
    }

    release.wait();
    release_thread.join().expect("release thread");

    let reinitialized = reinit_thread
        .join()
        .expect("reinit thread")
        .expect("reinit");
    let worker = attach_worker(&reinitialized);
    let generation = worker
        .activate_generation(current_pid())
        .expect("reactivate after reinit");

    let first = BackendSlotLease::acquire(&reinitialized).expect("first fresh backend");
    let second = BackendSlotLease::acquire(&reinitialized).expect("second fresh backend");
    let mut slots = [first.slot_id(), second.slot_id()];
    slots.sort_unstable();
    assert_eq!(slots, [0, 1]);
    assert_eq!(first.generation(), generation);
    assert_eq!(second.generation(), generation);
    assert!(matches!(
        BackendSlotLease::acquire(&reinitialized),
        Err(AcquireError::Empty)
    ));
}

#[test]
fn same_generation_reuse_makes_old_worker_handle_stale_epoch() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();

    let mut old_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("old slot");
    backend.release();
    worker.release_owned_slots_for_exit();

    let fresh_backend = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh_backend.slot_id(), slot_id);
    assert_eq!(fresh_backend.generation(), 1);

    let err = match old_slot.from_backend_rx() {
        Ok(_) => panic!("old worker handle must stale out on same-generation reuse"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        SlotAccessError::StaleLeaseEpoch {
            slot_id: 0,
            claimed_generation: 1,
            ..
        }
    ));

    let mut fresh_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("fresh slot");
    let mut rx = fresh_slot.from_backend_rx().expect("fresh rx");
    let mut buf = [0u8; 8];
    assert_eq!(rx.recv_frame_into(&mut buf).expect("empty"), None);
}

#[test]
fn old_worker_drop_does_not_detach_new_same_generation_incarnation() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();

    let old_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("old slot");
    backend.release();
    worker.release_owned_slots_for_exit();

    let mut fresh_backend = BackendSlotLease::acquire(&region).expect("fresh backend");
    let fresh_slot = unsafe { worker.slot_unchecked(slot_id) }.expect("fresh slot");

    drop(old_slot);
    fresh_backend.release();
    drop(fresh_slot);

    let reacquired = BackendSlotLease::acquire(&region).expect("reacquire after fresh drop");
    assert_eq!(reacquired.slot_id(), slot_id);
}

#[test]
fn recv_frame_into_requires_large_enough_buffer_without_consuming_frame() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
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
fn framed_ring_empty_nonzero_offset_resends_max_frame_capacity_10() {
    let ring = TestFramedRing::new(10);
    let mut tx = ring.ring();
    let mut rx = ring.ring();
    let mut buf = [0u8; 8];

    assert_commit_published(
        tx.send_frame(&ring.ready, &ring.peer_pid, b"abcde")
            .expect("send"),
    );
    recv_exact(rx.recv_frame_into(&ring.ready, &mut buf), &buf, b"abcde");
    assert_eq!(
        rx.recv_frame_into(&ring.ready, &mut buf).expect("empty"),
        None
    );

    assert_commit_published(
        tx.send_frame(&ring.ready, &ring.peer_pid, b"abcde")
            .expect("resend after drain"),
    );
    recv_exact(rx.recv_frame_into(&ring.ready, &mut buf), &buf, b"abcde");
    assert_eq!(
        rx.recv_frame_into(&ring.ready, &mut buf).expect("empty"),
        None
    );
}

#[test]
fn framed_ring_empty_nonzero_offset_resends_max_frame_capacity_9() {
    let ring = TestFramedRing::new(9);
    let mut tx = ring.ring();
    let mut rx = ring.ring();
    let mut buf = [0u8; 8];

    assert_commit_published(
        tx.send_frame(&ring.ready, &ring.peer_pid, b"pong")
            .expect("send"),
    );
    recv_exact(rx.recv_frame_into(&ring.ready, &mut buf), &buf, b"pong");
    assert_eq!(
        rx.recv_frame_into(&ring.ready, &mut buf).expect("empty"),
        None
    );

    assert_commit_published(
        tx.send_frame(&ring.ready, &ring.peer_pid, b"pong")
            .expect("resend after drain"),
    );
    recv_exact(rx.recv_frame_into(&ring.ready, &mut buf), &buf, b"pong");
    assert_eq!(
        rx.recv_frame_into(&ring.ready, &mut buf).expect("empty"),
        None
    );
}

#[test]
fn backend_to_worker_split_frame_wrap_round_trips_capacity_10() {
    let layout = TransportRegionLayout::new(1, 10, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    let mut buf = [0u8; 8];

    assert_commit_published(backend.to_worker_tx().send_frame(b"hi").expect("send"));
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"hi");

    assert_commit_published(
        backend
            .to_worker_tx()
            .send_frame(b"x")
            .expect("split-frame wrap send"),
    );
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"x");
    assert_eq!(rx.recv_frame_into(&mut buf).expect("empty"), None);
}

#[test]
fn backend_to_worker_split_frame_wrap_round_trips_capacity_9() {
    let layout = TransportRegionLayout::new(1, 9, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let mut slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("slot");
    let mut rx = slot.from_backend_rx().expect("rx");
    let mut buf = [0u8; 8];

    assert_commit_published(backend.to_worker_tx().send_frame(b"hi").expect("send"));
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"hi");

    assert_commit_published(
        backend
            .to_worker_tx()
            .send_frame(b"x")
            .expect("split-frame wrap send"),
    );
    recv_exact(rx.recv_frame_into(&mut buf), &buf, b"x");
    assert_eq!(rx.recv_frame_into(&mut buf).expect("empty"), None);
}

#[test]
fn slot_reuse_after_wrapped_drain_keeps_ring_sendable() {
    let layout = TransportRegionLayout::new(1, 10, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);

    let mut backend = BackendSlotLease::acquire(&region).expect("backend");
    let slot_id = backend.slot_id();
    {
        let mut slot = unsafe { worker.slot_unchecked(slot_id) }.expect("slot");
        let mut rx = slot.from_backend_rx().expect("rx");
        let mut buf = [0u8; 8];

        assert_commit_published(backend.to_worker_tx().send_frame(b"abcde").expect("send"));
        recv_exact(rx.recv_frame_into(&mut buf), &buf, b"abcde");
        assert_eq!(rx.recv_frame_into(&mut buf).expect("empty"), None);
    }
    backend.release();

    let mut fresh = BackendSlotLease::acquire(&region).expect("fresh backend");
    assert_eq!(fresh.slot_id(), slot_id);
    let mut fresh_slot = unsafe { worker.slot_unchecked(fresh.slot_id()) }.expect("fresh slot");
    let mut fresh_rx = fresh_slot.from_backend_rx().expect("fresh rx");
    let mut buf = [0u8; 8];

    assert_commit_published(
        fresh
            .to_worker_tx()
            .send_frame(b"abcde")
            .expect("resend after reuse"),
    );
    recv_exact(fresh_rx.recv_frame_into(&mut buf), &buf, b"abcde");
    assert_eq!(fresh_rx.recv_frame_into(&mut buf).expect("empty"), None);
}

#[cfg(debug_assertions)]
#[test]
fn lifecycle_generation_switch_is_not_reentrant_for_same_region() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new_inactive(layout);
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let _hook = LifecycleTransitionHookGuard::new(&region, {
        let entered = entered.clone();
        let release = release.clone();
        move || {
            entered.wait();
            release.wait();
        }
    });

    let region_for_activate = region;
    let activate =
        std::thread::spawn(move || region_for_activate.activate_worker_generation(current_pid()));
    entered.wait();

    let region_for_deactivate = region;
    let deactivate = std::thread::spawn(move || {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = region_for_deactivate.deactivate_worker_generation();
        }))
    });

    let deactivate = deactivate.join().expect("deactivate thread");
    assert!(
        deactivate.is_err(),
        "concurrent lifecycle transition must panic in debug builds"
    );

    release.wait();
    assert_eq!(
        activate.join().expect("activate thread").expect("activate"),
        1
    );
}

#[test]
fn steady_state_transport_ops_do_not_allocate() {
    let layout = TransportRegionLayout::new(1, 64, 64).expect("layout");
    let (_mem, region) = TestRegion::new(layout);
    let worker = attach_worker(&region);
    let mut backend = BackendSlotLease::acquire(&region).expect("backend");

    {
        let warm_slot = unsafe { worker.slot_unchecked(backend.slot_id()) }.expect("warm slot");
        drop(warm_slot);
    }

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
