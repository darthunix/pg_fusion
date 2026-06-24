use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use pgrx::pg_sys::AsPgCStr;

const TEST_GATE_SHMEM_NAME: &str = "pg_fusion:test_execution_gate";
const TEST_GATE_TIMEOUT: Duration = Duration::from_secs(10);

#[repr(C)]
struct TestExecutionGate {
    enabled: AtomicU32,
    claimed: AtomicU32,
    entered: AtomicU32,
    release: AtomicU32,
}

impl TestExecutionGate {
    fn new() -> Self {
        Self {
            enabled: AtomicU32::new(0),
            claimed: AtomicU32::new(0),
            entered: AtomicU32::new(0),
            release: AtomicU32::new(0),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TestExecutionGateHandle {
    ptr: NonNull<TestExecutionGate>,
}

unsafe impl Send for TestExecutionGateHandle {}
unsafe impl Sync for TestExecutionGateHandle {}

pub(crate) fn request_shmem_space() {
    unsafe {
        pgrx::pg_sys::RequestAddinShmemSpace(std::mem::size_of::<TestExecutionGate>());
    }
}

pub(crate) unsafe fn init_shmem() {
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(
            TEST_GATE_SHMEM_NAME.as_pg_cstr(),
            std::mem::size_of::<TestExecutionGate>(),
            &mut found,
        ) as *mut TestExecutionGate
    };
    let base = NonNull::new(base).expect("test execution gate shmem");
    if !found {
        unsafe {
            base.as_ptr().write(TestExecutionGate::new());
        }
    }
}

pub(crate) fn attach() -> TestExecutionGateHandle {
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(
            TEST_GATE_SHMEM_NAME.as_pg_cstr(),
            std::mem::size_of::<TestExecutionGate>(),
            &mut found,
        ) as *mut TestExecutionGate
    };
    assert!(
        found,
        "test execution gate shmem must already be initialized"
    );
    TestExecutionGateHandle {
        ptr: NonNull::new(base).expect("test execution gate shmem base"),
    }
}

impl TestExecutionGateHandle {
    fn gate(self) -> &'static TestExecutionGate {
        unsafe { self.ptr.as_ref() }
    }

    pub(crate) fn reset(self) {
        let gate = self.gate();
        gate.release.store(0, Ordering::Release);
        gate.entered.store(0, Ordering::Release);
        gate.claimed.store(0, Ordering::Release);
        gate.enabled.store(1, Ordering::Release);
    }

    pub(crate) fn release(self) {
        let gate = self.gate();
        gate.release.store(1, Ordering::Release);
        gate.enabled.store(0, Ordering::Release);
    }

    pub(crate) fn disable(self) {
        let gate = self.gate();
        gate.release.store(1, Ordering::Release);
        gate.enabled.store(0, Ordering::Release);
        gate.claimed.store(0, Ordering::Release);
        gate.entered.store(0, Ordering::Release);
    }

    pub(crate) fn entered(self) -> i32 {
        self.gate().entered.load(Ordering::Acquire) as i32
    }

    pub(crate) async fn wait_at_execution_start(self) {
        let gate = self.gate();
        if gate.enabled.load(Ordering::Acquire) == 0 {
            return;
        }
        if gate
            .claimed
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        gate.entered.fetch_add(1, Ordering::AcqRel);
        let deadline = Instant::now() + TEST_GATE_TIMEOUT;
        while gate.release.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
    }
}
