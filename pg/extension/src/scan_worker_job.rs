use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use backend_service::{StandaloneScanDescriptor, StandaloneScanField};
use control_transport::{BackendLeaseId, BackendLeaseSlot};

pub(crate) const SCAN_WORKER_JOB_CAPACITY: usize = 64;
pub(crate) const SCAN_WORKER_PAYLOAD_CAPACITY: usize = 64 * 1024;
const SCAN_WORKER_ERROR_CAPACITY: usize = 256;
const REGISTRY_MAGIC: u64 = 0x5046_5343_414e_4a42;
const DESCRIPTOR_MAGIC: &[u8; 4] = b"PFSD";
const DESCRIPTOR_VERSION: u8 = 1;
const NONE_U64: u64 = u64::MAX;

const STATE_FREE: u32 = 0;
const STATE_RESERVED: u32 = 1;
const STATE_STARTING: u32 = 2;
const STATE_READY: u32 = 3;
const STATE_RUNNING: u32 = 4;
const STATE_DONE: u32 = 5;
const STATE_FAILED: u32 = 6;
const STATE_FAILING: u32 = 7;
const CONFIG_SCAN_TIMING_DETAIL: u32 = 1 << 0;

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
    peer_slot_id: AtomicU32,
    peer_generation: AtomicU64,
    peer_lease_epoch: AtomicU64,
    payload_len: AtomicU32,
    config_flags: AtomicU32,
    error_len: AtomicU32,
    payload: [u8; SCAN_WORKER_PAYLOAD_CAPACITY],
    error: [u8; SCAN_WORKER_ERROR_CAPACITY],
}

#[derive(Clone, Copy)]
pub(crate) struct ScanWorkerJobRegistryHandle {
    ptr: NonNull<ScanWorkerJobRegistry>,
}

unsafe impl Send for ScanWorkerJobRegistryHandle {}
unsafe impl Sync for ScanWorkerJobRegistryHandle {}

pub(crate) struct ScanWorkerJobSpec<'a> {
    pub(crate) payload: &'a [u8],
    pub(crate) db_oid: u32,
    pub(crate) user_oid: u32,
    pub(crate) session_epoch: u64,
    pub(crate) scan_id: u64,
    pub(crate) producer_id: u16,
    pub(crate) producer_count: u16,
    pub(crate) scan_timing_detail: bool,
}

#[derive(Debug)]
pub(crate) struct ScanWorkerJobSnapshot {
    pub(crate) descriptor: StandaloneScanDescriptor,
    pub(crate) db_oid: u32,
    pub(crate) user_oid: u32,
    pub(crate) session_epoch: u64,
    pub(crate) scan_id: u64,
    pub(crate) producer_id: u16,
    pub(crate) producer_count: u16,
    pub(crate) scan_timing_detail: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanWorkerJobError {
    #[error("scan worker payload is too large: {actual} bytes > {capacity} bytes")]
    PayloadTooLarge { actual: usize, capacity: usize },
    #[error("scan worker descriptor encode failed: {0}")]
    DescriptorEncode(String),
    #[error("scan worker descriptor decode failed: {0}")]
    DescriptorDecode(String),
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
        if spec.payload.len() > SCAN_WORKER_PAYLOAD_CAPACITY {
            return Err(ScanWorkerJobError::PayloadTooLarge {
                actual: spec.payload.len(),
                capacity: SCAN_WORKER_PAYLOAD_CAPACITY,
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
            job.peer_slot_id.store(u32::MAX, Ordering::Relaxed);
            job.peer_generation.store(0, Ordering::Relaxed);
            job.peer_lease_epoch.store(0, Ordering::Relaxed);
            job.config_flags
                .store(config_flags(&spec), Ordering::Relaxed);
            job.error_len.store(0, Ordering::Relaxed);
            unsafe {
                let dst = job.payload.as_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(spec.payload.as_ptr(), dst, spec.payload.len());
            }
            job.payload_len
                .store(spec.payload.len().try_into().unwrap(), Ordering::Release);
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
        let payload_len = job.payload_len.load(Ordering::Acquire) as usize;
        let descriptor = decode_scan_worker_descriptor(&job.payload[..payload_len])?;
        Ok(ScanWorkerJobSnapshot {
            descriptor,
            db_oid: job.db_oid.load(Ordering::Relaxed),
            user_oid: job.user_oid.load(Ordering::Relaxed),
            session_epoch: job.session_epoch.load(Ordering::Relaxed),
            scan_id: job.scan_id.load(Ordering::Relaxed),
            producer_id: job.producer_id.load(Ordering::Relaxed) as u16,
            producer_count: job.producer_count.load(Ordering::Relaxed) as u16,
            scan_timing_detail: job.config_flags.load(Ordering::Relaxed)
                & CONFIG_SCAN_TIMING_DETAIL
                != 0,
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

fn config_flags(spec: &ScanWorkerJobSpec<'_>) -> u32 {
    if spec.scan_timing_detail {
        CONFIG_SCAN_TIMING_DETAIL
    } else {
        0
    }
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

pub(crate) fn encode_scan_worker_descriptor(
    descriptor: &StandaloneScanDescriptor,
    out: &mut Vec<u8>,
) -> Result<(), ScanWorkerJobError> {
    out.clear();
    out.extend_from_slice(DESCRIPTOR_MAGIC);
    out.push(DESCRIPTOR_VERSION);
    put_u32(out, descriptor.table_oid);
    put_option_usize(out, descriptor.planner_fetch_hint)?;
    put_option_usize(out, descriptor.local_row_cap)?;
    put_bytes(out, descriptor.sql.as_bytes(), "scan SQL")?;
    put_len(out, descriptor.fields.len(), "field count")?;
    for field in &descriptor.fields {
        put_bytes(out, field.name.as_bytes(), "field name")?;
        put_u16(out, field.type_tag);
        out.push(u8::from(field.nullable));
    }
    put_len(
        out,
        descriptor.source_projection.len(),
        "source projection length",
    )?;
    for &index in &descriptor.source_projection {
        put_usize(out, index)?;
    }
    if out.len() > SCAN_WORKER_PAYLOAD_CAPACITY {
        return Err(ScanWorkerJobError::PayloadTooLarge {
            actual: out.len(),
            capacity: SCAN_WORKER_PAYLOAD_CAPACITY,
        });
    }
    Ok(())
}

fn decode_scan_worker_descriptor(
    payload: &[u8],
) -> Result<StandaloneScanDescriptor, ScanWorkerJobError> {
    DescriptorReader::new(payload).read_descriptor()
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_len(out: &mut Vec<u8>, value: usize, label: &'static str) -> Result<(), ScanWorkerJobError> {
    let value = u32::try_from(value).map_err(|_| {
        ScanWorkerJobError::DescriptorEncode(format!("{label} does not fit into u32"))
    })?;
    put_u32(out, value);
    Ok(())
}

fn put_usize(out: &mut Vec<u8>, value: usize) -> Result<(), ScanWorkerJobError> {
    let value = u64::try_from(value).map_err(|_| {
        ScanWorkerJobError::DescriptorEncode("usize value does not fit into u64".into())
    })?;
    put_u64(out, value);
    Ok(())
}

fn put_option_usize(out: &mut Vec<u8>, value: Option<usize>) -> Result<(), ScanWorkerJobError> {
    match value {
        Some(value) => put_usize(out, value),
        None => {
            put_u64(out, NONE_U64);
            Ok(())
        }
    }
}

fn put_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    label: &'static str,
) -> Result<(), ScanWorkerJobError> {
    put_len(out, bytes.len(), label)?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct DescriptorReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> DescriptorReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_descriptor(&mut self) -> Result<StandaloneScanDescriptor, ScanWorkerJobError> {
        let magic = self.read_exact(DESCRIPTOR_MAGIC.len(), "magic")?;
        if magic != DESCRIPTOR_MAGIC {
            return Err(ScanWorkerJobError::DescriptorDecode(
                "invalid descriptor magic".into(),
            ));
        }
        let version = self.read_u8("version")?;
        if version != DESCRIPTOR_VERSION {
            return Err(ScanWorkerJobError::DescriptorDecode(format!(
                "unsupported descriptor version {version}"
            )));
        }

        let table_oid = self.read_u32("table oid")?;
        let planner_fetch_hint = self.read_option_usize("planner fetch hint")?;
        let local_row_cap = self.read_option_usize("local row cap")?;
        let sql = self.read_string("scan SQL")?;
        let field_count = self.read_len("field count")?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let name = self.read_string("field name")?;
            let type_tag = self.read_u16("field type tag")?;
            let nullable = match self.read_u8("field nullable")? {
                0 => false,
                1 => true,
                other => {
                    return Err(ScanWorkerJobError::DescriptorDecode(format!(
                        "invalid nullable flag {other}"
                    )));
                }
            };
            fields.push(StandaloneScanField {
                name,
                type_tag,
                nullable,
            });
        }
        let projection_len = self.read_len("source projection length")?;
        let mut source_projection = Vec::with_capacity(projection_len);
        for _ in 0..projection_len {
            source_projection.push(self.read_usize("source projection")?);
        }
        if self.offset != self.payload.len() {
            return Err(ScanWorkerJobError::DescriptorDecode(format!(
                "descriptor has {} trailing bytes",
                self.payload.len() - self.offset
            )));
        }
        Ok(StandaloneScanDescriptor {
            sql,
            table_oid,
            fields,
            source_projection,
            planner_fetch_hint,
            local_row_cap,
        })
    }

    fn read_u8(&mut self, label: &'static str) -> Result<u8, ScanWorkerJobError> {
        Ok(self.read_exact(1, label)?[0])
    }

    fn read_u16(&mut self, label: &'static str) -> Result<u16, ScanWorkerJobError> {
        let bytes = self.read_exact(2, label)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self, label: &'static str) -> Result<u32, ScanWorkerJobError> {
        let bytes = self.read_exact(4, label)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self, label: &'static str) -> Result<u64, ScanWorkerJobError> {
        let bytes = self.read_exact(8, label)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_len(&mut self, label: &'static str) -> Result<usize, ScanWorkerJobError> {
        usize::try_from(self.read_u32(label)?).map_err(|_| {
            ScanWorkerJobError::DescriptorDecode(format!("{label} does not fit into usize"))
        })
    }

    fn read_usize(&mut self, label: &'static str) -> Result<usize, ScanWorkerJobError> {
        let value = self.read_u64(label)?;
        usize::try_from(value).map_err(|_| {
            ScanWorkerJobError::DescriptorDecode(format!("{label} does not fit into usize"))
        })
    }

    fn read_option_usize(
        &mut self,
        label: &'static str,
    ) -> Result<Option<usize>, ScanWorkerJobError> {
        match self.read_u64(label)? {
            NONE_U64 => Ok(None),
            value => usize::try_from(value).map(Some).map_err(|_| {
                ScanWorkerJobError::DescriptorDecode(format!("{label} does not fit into usize"))
            }),
        }
    }

    fn read_string(&mut self, label: &'static str) -> Result<String, ScanWorkerJobError> {
        let len = self.read_len(label)?;
        let bytes = self.read_exact(len, label)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|err| ScanWorkerJobError::DescriptorDecode(format!("{label}: {err}")))
    }

    fn read_exact(
        &mut self,
        len: usize,
        label: &'static str,
    ) -> Result<&'a [u8], ScanWorkerJobError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            ScanWorkerJobError::DescriptorDecode(format!("{label} length overflow"))
        })?;
        if end > self.payload.len() {
            return Err(ScanWorkerJobError::DescriptorDecode(format!(
                "descriptor ended while reading {label}"
            )));
        }
        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
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

    fn descriptor(sql: &str) -> StandaloneScanDescriptor {
        StandaloneScanDescriptor {
            sql: sql.to_owned(),
            table_oid: 42,
            fields: vec![StandaloneScanField {
                name: "id".into(),
                type_tag: 4,
                nullable: false,
            }],
            source_projection: vec![0],
            planner_fetch_hint: Some(128),
            local_row_cap: None,
        }
    }

    fn encoded_descriptor(sql: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        encode_scan_worker_descriptor(&descriptor(sql), &mut payload).expect("encode descriptor");
        payload
    }

    fn spec(payload: &[u8]) -> ScanWorkerJobSpec<'_> {
        ScanWorkerJobSpec {
            payload,
            db_oid: 1,
            user_oid: 2,
            session_epoch: 3,
            scan_id: 4,
            producer_id: 1,
            producer_count: 2,
            scan_timing_detail: false,
        }
    }

    fn spec_with_scan_timing_detail(payload: &[u8]) -> ScanWorkerJobSpec<'_> {
        ScanWorkerJobSpec {
            scan_timing_detail: true,
            ..spec(payload)
        }
    }

    fn peer() -> BackendLeaseSlot {
        BackendLeaseSlot::new(7, BackendLeaseId::new(11, 13))
    }

    #[test]
    fn failed_starting_job_is_reused() {
        let registry = TestRegistry::new();
        let jobs = registry.handle();
        let first_payload = encoded_descriptor("select 1");
        let second_payload = encoded_descriptor("select 2");

        let first = jobs.allocate(spec(&first_payload)).expect("first job");
        assert_eq!(first, 0);
        jobs.mark_failed(first, "launch failed")
            .expect("mark failed");

        let second = jobs.allocate(spec(&second_payload)).expect("second job");
        assert_eq!(second, first);
        let snapshot = jobs.snapshot(second).expect("snapshot");
        assert_eq!(snapshot.descriptor.sql, "select 2");
        assert_eq!(snapshot.descriptor.fields[0].name, "id");
        assert_eq!(snapshot.descriptor.planner_fetch_hint, Some(128));
        assert!(!snapshot.scan_timing_detail);
    }

    #[test]
    fn scan_timing_detail_is_snapshotted_and_overwritten_on_reuse() {
        let registry = TestRegistry::new();
        let jobs = registry.handle();
        let first_payload = encoded_descriptor("select 1");
        let second_payload = encoded_descriptor("select 2");

        let first = jobs
            .allocate(spec_with_scan_timing_detail(&first_payload))
            .expect("first job");
        let snapshot = jobs.snapshot(first).expect("first snapshot");
        assert!(snapshot.scan_timing_detail);

        jobs.mark_failed(first, "launch failed")
            .expect("mark failed");
        let second = jobs.allocate(spec(&second_payload)).expect("second job");
        assert_eq!(second, first);
        let snapshot = jobs.snapshot(second).expect("second snapshot");
        assert!(!snapshot.scan_timing_detail);
    }

    #[test]
    fn publish_ready_after_failure_does_not_resurrect_job() {
        let registry = TestRegistry::new();
        let jobs = registry.handle();
        let payload = encoded_descriptor("select 1");
        let job_id = jobs.allocate(spec(&payload)).expect("job");

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
        let payload = encoded_descriptor("select 1");
        let job_id = jobs.allocate(spec(&payload)).expect("job");

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
