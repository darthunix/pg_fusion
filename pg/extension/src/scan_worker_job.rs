use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use backend_service::CtidBlockRange;
use control_transport::{BackendLeaseId, BackendLeaseSlot};

pub(crate) const SCAN_WORKER_JOB_CAPACITY: usize = 64;
pub(crate) const SCAN_WORKER_SQL_CAPACITY: usize = 64 * 1024;
const SCAN_WORKER_ERROR_CAPACITY: usize = 256;
const REGISTRY_MAGIC: u64 = 0x5046_5343_414e_4a42;

const STATE_FREE: u32 = 0;
const STATE_RESERVED: u32 = 1;
const STATE_STARTING: u32 = 2;
const STATE_READY: u32 = 3;
const STATE_RUNNING: u32 = 4;
const STATE_DONE: u32 = 5;
const STATE_FAILED: u32 = 6;
const STATE_FAILING: u32 = 7;

#[repr(C, align(8))]
pub(crate) struct ScanWorkerJobRegistry {
    magic: AtomicU64,
    jobs: [ScanWorkerJob; SCAN_WORKER_JOB_CAPACITY],
}

#[repr(C, align(8))]
struct ScanWorkerJob {
    state: AtomicU32,
    db_oid: AtomicU32,
    user_oid: AtomicU32,
    session_epoch: AtomicU64,
    scan_id: AtomicU64,
    producer_id: AtomicU32,
    producer_count: AtomicU32,
    start_block: AtomicU64,
    end_block: AtomicU64,
    peer_slot_id: AtomicU32,
    peer_generation: AtomicU64,
    peer_lease_epoch: AtomicU64,
    sql_len: AtomicU32,
    error_len: AtomicU32,
    sql: [u8; SCAN_WORKER_SQL_CAPACITY],
    error: [u8; SCAN_WORKER_ERROR_CAPACITY],
}

#[derive(Clone, Copy)]
pub(crate) struct ScanWorkerJobRegistryHandle {
    ptr: NonNull<ScanWorkerJobRegistry>,
}

unsafe impl Send for ScanWorkerJobRegistryHandle {}
unsafe impl Sync for ScanWorkerJobRegistryHandle {}

pub(crate) struct ScanWorkerJobSpec<'a> {
    pub(crate) sql: &'a str,
    pub(crate) db_oid: u32,
    pub(crate) user_oid: u32,
    pub(crate) session_epoch: u64,
    pub(crate) scan_id: u64,
    pub(crate) producer_id: u16,
    pub(crate) producer_count: u16,
    pub(crate) ctid_range: CtidBlockRange,
}

#[derive(Debug)]
pub(crate) struct ScanWorkerJobSnapshot {
    pub(crate) sql: String,
    pub(crate) db_oid: u32,
    pub(crate) user_oid: u32,
    pub(crate) session_epoch: u64,
    pub(crate) scan_id: u64,
    pub(crate) producer_id: u16,
    pub(crate) producer_count: u16,
    pub(crate) ctid_range: CtidBlockRange,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanWorkerJobError {
    #[error("scan worker SQL is too large: {actual} bytes > {capacity} bytes")]
    SqlTooLarge { actual: usize, capacity: usize },
    #[error("no free scan worker job slots")]
    NoFreeJobSlots,
    #[error("invalid scan worker job id {job_id}")]
    InvalidJobId { job_id: usize },
    #[error("scan worker job {job_id} failed before ready: {message}")]
    FailedBeforeReady { job_id: usize, message: String },
    #[error("timed out waiting for scan worker job {job_id} to become ready")]
    ReadyTimeout { job_id: usize },
    #[error("scan worker job {job_id} is not startable; state={state}")]
    NotStartable { job_id: usize, state: u32 },
    #[error("scan worker job {job_id} contains invalid UTF-8 SQL")]
    InvalidSqlUtf8 { job_id: usize },
}

impl ScanWorkerJobRegistry {
    pub(crate) fn layout() -> Layout {
        Layout::new::<Self>()
    }
}

impl ScanWorkerJobRegistryHandle {
    pub(crate) unsafe fn init_or_attach(
        base: NonNull<u8>,
        found: bool,
    ) -> ScanWorkerJobRegistryHandle {
        let ptr = base.cast::<ScanWorkerJobRegistry>();
        let registry = unsafe { ptr.as_ref() };
        if !found {
            registry.magic.store(REGISTRY_MAGIC, Ordering::Release);
        }
        ScanWorkerJobRegistryHandle { ptr }
    }

    pub(crate) unsafe fn attach(base: NonNull<u8>) -> ScanWorkerJobRegistryHandle {
        let ptr = base.cast::<ScanWorkerJobRegistry>();
        ScanWorkerJobRegistryHandle { ptr }
    }

    pub(crate) fn allocate(
        &self,
        spec: ScanWorkerJobSpec<'_>,
    ) -> Result<usize, ScanWorkerJobError> {
        let sql = spec.sql.as_bytes();
        if sql.len() > SCAN_WORKER_SQL_CAPACITY {
            return Err(ScanWorkerJobError::SqlTooLarge {
                actual: sql.len(),
                capacity: SCAN_WORKER_SQL_CAPACITY,
            });
        }

        for job_id in 0..SCAN_WORKER_JOB_CAPACITY {
            let job = self.job(job_id);
            if !try_reserve_job(job) {
                continue;
            }

            job.db_oid.store(spec.db_oid, Ordering::Relaxed);
            job.user_oid.store(spec.user_oid, Ordering::Relaxed);
            job.session_epoch
                .store(spec.session_epoch, Ordering::Relaxed);
            job.scan_id.store(spec.scan_id, Ordering::Relaxed);
            job.producer_id
                .store(u32::from(spec.producer_id), Ordering::Relaxed);
            job.producer_count
                .store(u32::from(spec.producer_count), Ordering::Relaxed);
            job.start_block
                .store(spec.ctid_range.start_block, Ordering::Relaxed);
            job.end_block
                .store(spec.ctid_range.end_block, Ordering::Relaxed);
            job.peer_slot_id.store(u32::MAX, Ordering::Relaxed);
            job.peer_generation.store(0, Ordering::Relaxed);
            job.peer_lease_epoch.store(0, Ordering::Relaxed);
            job.error_len.store(0, Ordering::Relaxed);
            unsafe {
                let dst = job.sql.as_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(sql.as_ptr(), dst, sql.len());
            }
            job.sql_len
                .store(sql.len().try_into().unwrap(), Ordering::Release);
            job.state.store(STATE_STARTING, Ordering::Release);
            return Ok(job_id);
        }

        Err(ScanWorkerJobError::NoFreeJobSlots)
    }

    pub(crate) fn snapshot(
        &self,
        job_id: usize,
    ) -> Result<ScanWorkerJobSnapshot, ScanWorkerJobError> {
        let job = self.checked_job(job_id)?;
        let state = job.state.load(Ordering::Acquire);
        if state == STATE_FAILED {
            return Err(ScanWorkerJobError::FailedBeforeReady {
                job_id,
                message: job_error_message(job),
            });
        }
        if state != STATE_STARTING && state != STATE_READY && state != STATE_RUNNING {
            return Err(ScanWorkerJobError::NotStartable { job_id, state });
        }
        let sql_len = job.sql_len.load(Ordering::Acquire) as usize;
        let sql = std::str::from_utf8(&job.sql[..sql_len])
            .map_err(|_| ScanWorkerJobError::InvalidSqlUtf8 { job_id })?
            .to_string();
        Ok(ScanWorkerJobSnapshot {
            sql,
            db_oid: job.db_oid.load(Ordering::Relaxed),
            user_oid: job.user_oid.load(Ordering::Relaxed),
            session_epoch: job.session_epoch.load(Ordering::Relaxed),
            scan_id: job.scan_id.load(Ordering::Relaxed),
            producer_id: job.producer_id.load(Ordering::Relaxed) as u16,
            producer_count: job.producer_count.load(Ordering::Relaxed) as u16,
            ctid_range: CtidBlockRange {
                start_block: job.start_block.load(Ordering::Relaxed),
                end_block: job.end_block.load(Ordering::Relaxed),
            },
        })
    }

    pub(crate) fn publish_ready(
        &self,
        job_id: usize,
        peer: BackendLeaseSlot,
    ) -> Result<(), ScanWorkerJobError> {
        let job = self.checked_job(job_id)?;
        job.peer_slot_id.store(peer.slot_id(), Ordering::Relaxed);
        job.peer_generation
            .store(peer.lease_id().generation(), Ordering::Relaxed);
        job.peer_lease_epoch
            .store(peer.lease_id().lease_epoch(), Ordering::Relaxed);
        transition_job_state(job, job_id, STATE_STARTING, STATE_READY)
    }

    pub(crate) fn mark_running(&self, job_id: usize) -> Result<(), ScanWorkerJobError> {
        let job = self.checked_job(job_id)?;
        transition_job_state(job, job_id, STATE_READY, STATE_RUNNING)
    }

    pub(crate) fn mark_done(&self, job_id: usize) -> Result<(), ScanWorkerJobError> {
        let job = self.checked_job(job_id)?;
        transition_job_state(job, job_id, STATE_RUNNING, STATE_DONE)
    }

    pub(crate) fn mark_failed(
        &self,
        job_id: usize,
        message: &str,
    ) -> Result<(), ScanWorkerJobError> {
        let job = self.checked_job(job_id)?;
        loop {
            let state = job.state.load(Ordering::Acquire);
            match state {
                STATE_STARTING | STATE_READY | STATE_RUNNING => {
                    if job
                        .state
                        .compare_exchange(state, STATE_FAILING, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        continue;
                    }
                    write_job_error(job, message);
                    job.state.store(STATE_FAILED, Ordering::Release);
                    return Ok(());
                }
                STATE_FAILING => std::thread::yield_now(),
                STATE_FAILED => {
                    return Err(ScanWorkerJobError::FailedBeforeReady {
                        job_id,
                        message: job_error_message(job),
                    });
                }
                state => return Err(ScanWorkerJobError::NotStartable { job_id, state }),
            }
        }
    }

    pub(crate) fn wait_ready(
        &self,
        job_id: usize,
        timeout: Duration,
    ) -> Result<BackendLeaseSlot, ScanWorkerJobError> {
        let job = self.checked_job(job_id)?;
        let deadline = Instant::now() + timeout;
        loop {
            match job.state.load(Ordering::Acquire) {
                STATE_READY | STATE_RUNNING | STATE_DONE => {
                    return Ok(BackendLeaseSlot::new(
                        job.peer_slot_id.load(Ordering::Acquire),
                        BackendLeaseId::new(
                            job.peer_generation.load(Ordering::Acquire),
                            job.peer_lease_epoch.load(Ordering::Acquire),
                        ),
                    ));
                }
                STATE_FAILED => {
                    return Err(ScanWorkerJobError::FailedBeforeReady {
                        job_id,
                        message: job_error_message(job),
                    });
                }
                STATE_FAILING if Instant::now() >= deadline => {
                    return Err(ScanWorkerJobError::ReadyTimeout { job_id });
                }
                STATE_FAILING => std::thread::sleep(Duration::from_millis(1)),
                _ if Instant::now() >= deadline => {
                    return Err(ScanWorkerJobError::ReadyTimeout { job_id });
                }
                _ => std::thread::sleep(Duration::from_millis(1)),
            }
        }
    }

    fn checked_job(&self, job_id: usize) -> Result<&ScanWorkerJob, ScanWorkerJobError> {
        if job_id >= SCAN_WORKER_JOB_CAPACITY {
            return Err(ScanWorkerJobError::InvalidJobId { job_id });
        }
        Ok(self.job(job_id))
    }

    fn job(&self, job_id: usize) -> &ScanWorkerJob {
        unsafe { &self.ptr.as_ref().jobs[job_id] }
    }
}

fn try_reserve_job(job: &ScanWorkerJob) -> bool {
    for expected in [STATE_FREE, STATE_DONE, STATE_FAILED] {
        if job
            .state
            .compare_exchange(
                expected,
                STATE_RESERVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            return true;
        }
    }
    false
}

fn transition_job_state(
    job: &ScanWorkerJob,
    job_id: usize,
    expected: u32,
    next: u32,
) -> Result<(), ScanWorkerJobError> {
    match job
        .state
        .compare_exchange(expected, next, Ordering::AcqRel, Ordering::Acquire)
    {
        Ok(_) => Ok(()),
        Err(STATE_FAILED) => Err(ScanWorkerJobError::FailedBeforeReady {
            job_id,
            message: job_error_message(job),
        }),
        Err(state) => Err(ScanWorkerJobError::NotStartable { job_id, state }),
    }
}

fn write_job_error(job: &ScanWorkerJob, message: &str) {
    let bytes = message.as_bytes();
    let len = bytes.len().min(SCAN_WORKER_ERROR_CAPACITY);
    unsafe {
        let dst = job.error.as_ptr() as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, len);
    }
    job.error_len.store(len as u32, Ordering::Release);
}

fn job_error_message(job: &ScanWorkerJob) -> String {
    let len = job.error_len.load(Ordering::Acquire) as usize;
    String::from_utf8_lossy(&job.error[..len]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{alloc_zeroed, dealloc};

    struct TestRegistry {
        base: NonNull<u8>,
        handle: ScanWorkerJobRegistryHandle,
    }

    impl TestRegistry {
        fn new() -> Self {
            let layout = ScanWorkerJobRegistry::layout();
            let base = unsafe { NonNull::new(alloc_zeroed(layout)).expect("registry memory") };
            let handle = unsafe { ScanWorkerJobRegistryHandle::init_or_attach(base, false) };
            Self { base, handle }
        }

        fn handle(&self) -> ScanWorkerJobRegistryHandle {
            self.handle
        }
    }

    impl Drop for TestRegistry {
        fn drop(&mut self) {
            unsafe {
                dealloc(self.base.as_ptr(), ScanWorkerJobRegistry::layout());
            }
        }
    }

    fn spec(sql: &str) -> ScanWorkerJobSpec<'_> {
        ScanWorkerJobSpec {
            sql,
            db_oid: 1,
            user_oid: 2,
            session_epoch: 3,
            scan_id: 4,
            producer_id: 1,
            producer_count: 2,
            ctid_range: CtidBlockRange {
                start_block: 10,
                end_block: 20,
            },
        }
    }

    fn peer() -> BackendLeaseSlot {
        BackendLeaseSlot::new(7, BackendLeaseId::new(11, 13))
    }

    #[test]
    fn failed_starting_job_is_reused() {
        let registry = TestRegistry::new();
        let jobs = registry.handle();

        let first = jobs.allocate(spec("select 1")).expect("first job");
        assert_eq!(first, 0);
        jobs.mark_failed(first, "launch failed")
            .expect("mark failed");

        let second = jobs.allocate(spec("select 2")).expect("second job");
        assert_eq!(second, first);
        let snapshot = jobs.snapshot(second).expect("snapshot");
        assert_eq!(snapshot.sql, "select 2");
    }

    #[test]
    fn publish_ready_after_failure_does_not_resurrect_job() {
        let registry = TestRegistry::new();
        let jobs = registry.handle();
        let job_id = jobs.allocate(spec("select 1")).expect("job");

        jobs.mark_failed(job_id, "launch failed")
            .expect("mark failed");

        let err = jobs
            .publish_ready(job_id, peer())
            .expect_err("publish must fail");
        assert!(matches!(err, ScanWorkerJobError::FailedBeforeReady { .. }));
        let err = jobs
            .wait_ready(job_id, Duration::from_millis(0))
            .expect_err("wait must fail");
        match err {
            ScanWorkerJobError::FailedBeforeReady { message, .. } => {
                assert_eq!(message, "launch failed");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn running_and_done_require_ordered_state_transitions() {
        let registry = TestRegistry::new();
        let jobs = registry.handle();
        let job_id = jobs.allocate(spec("select 1")).expect("job");

        assert!(matches!(
            jobs.mark_running(job_id).expect_err("not ready"),
            ScanWorkerJobError::NotStartable {
                state: STATE_STARTING,
                ..
            }
        ));

        jobs.publish_ready(job_id, peer()).expect("ready");
        jobs.mark_running(job_id).expect("running");
        jobs.mark_done(job_id).expect("done");

        assert!(matches!(
            jobs.mark_done(job_id).expect_err("already done"),
            ScanWorkerJobError::NotStartable {
                state: STATE_DONE,
                ..
            }
        ));
        assert!(matches!(
            jobs.mark_failed(job_id, "late failure")
                .expect_err("done is terminal"),
            ScanWorkerJobError::NotStartable {
                state: STATE_DONE,
                ..
            }
        ));
    }
}
