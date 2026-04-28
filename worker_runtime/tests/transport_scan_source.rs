use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use control_transport::{
    BackendRxError, BackendSlotLease, RxError, TransportRegion, TransportRegionLayout,
    WorkerTransport,
};
use futures::{FutureExt, StreamExt};
use issuance::{IssuanceConfig, IssuancePool, IssuedRx};
use pool::PagePoolConfig;
use runtime_protocol::{
    decode_worker_scan_to_backend, encode_backend_scan_to_worker_into,
    encoded_len_backend_scan_to_worker, ProducerRole, WorkerScanToBackendRef,
    MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
};
use scan_flow::ProducerRoleKind;
use scan_node::PgScanId;
use transfer::PageRx;
use worker_runtime::{
    OpenScanRequest, ScanBatchSource, ScanIngressProvider, ScanProducerPeer,
    TransportScanBatchSource, WorkerRuntimeError, WorkerScanTuning,
};

struct OwnedRegion {
    base: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
}

impl OwnedRegion {
    fn new(size: usize, align: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, align).expect("layout");
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        let base = std::ptr::NonNull::new(base).expect("allocation");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.base.as_ptr(), self.layout) };
    }
}

struct TestIngress {
    entries: Mutex<BTreeMap<(u64, u64, u16), IssuedRx>>,
}

impl std::fmt::Debug for TestIngress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestIngress").finish_non_exhaustive()
    }
}

impl TestIngress {
    fn with_entry(session_epoch: u64, scan_id: u64, rx: IssuedRx) -> Self {
        let mut entries = BTreeMap::new();
        entries.insert((session_epoch, scan_id, 0), rx);
        Self {
            entries: Mutex::new(entries),
        }
    }

    fn with_producers(
        session_epoch: u64,
        scan_id: u64,
        producer_ids: impl IntoIterator<Item = u16>,
        page_pool: pool::PagePool,
        issuance_pool: IssuancePool,
    ) -> Self {
        let mut entries = BTreeMap::new();
        for producer_id in producer_ids {
            entries.insert(
                (session_epoch, scan_id, producer_id),
                IssuedRx::new(PageRx::new(page_pool), issuance_pool),
            );
        }
        Self {
            entries: Mutex::new(entries),
        }
    }
}

impl ScanIngressProvider for TestIngress {
    fn issued_rx(
        &self,
        session_epoch: u64,
        scan_id: u64,
        producer_id: u16,
    ) -> Result<IssuedRx, WorkerRuntimeError> {
        self.entries
            .lock()
            .unwrap()
            .get(&(session_epoch, scan_id, producer_id))
            .cloned()
            .ok_or(WorkerRuntimeError::MissingScanIngress {
                session_epoch,
                scan_id,
            })
    }
}

fn init_page_pool(page_size: usize, page_count: u32) -> (OwnedRegion, pool::PagePool) {
    let cfg = PagePoolConfig::new(page_size, page_count).expect("pool config");
    let layout = pool::PagePool::layout(cfg).expect("pool layout");
    let region = OwnedRegion::new(layout.size, layout.align);
    let pool =
        unsafe { pool::PagePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
    (region, pool)
}

fn init_issuance_pool(permit_count: u32) -> (OwnedRegion, issuance::IssuancePool) {
    let cfg = IssuanceConfig::new(permit_count).expect("issuance config");
    let layout = IssuancePool::layout(cfg).expect("issuance layout");
    let region = OwnedRegion::new(layout.size, layout.align);
    let pool = unsafe { IssuancePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
    (region, pool)
}

fn init_transport_region(
    slot_count: u32,
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
) -> (OwnedRegion, TransportRegion) {
    let layout =
        TransportRegionLayout::new(slot_count, backend_to_worker_cap, worker_to_backend_cap)
            .expect("transport layout");
    let region_mem = OwnedRegion::new(layout.size, layout.align);
    let region = unsafe { TransportRegion::init_in_place(region_mem.base, layout.size, layout) }
        .expect("transport region");
    (region_mem, region)
}

fn test_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn shared_transport_region() -> TransportRegion {
    static REGION: OnceLock<TransportRegion> = OnceLock::new();
    *REGION.get_or_init(|| {
        let (region_mem, region) = init_transport_region(1, 256, 256);
        std::mem::forget(region_mem);
        activate_generation(&region);
        region
    })
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn open_request(peer: control_transport::BackendLeaseSlot) -> OpenScanRequest {
    OpenScanRequest {
        producers: vec![ScanProducerPeer {
            producer_id: 0,
            role: ProducerRoleKind::Leader,
            peer,
        }],
        session_epoch: 7,
        scan_id: PgScanId::new(11),
        output_schema: output_schema(),
        page_kind: import::ARROW_LAYOUT_BATCH_KIND,
        page_flags: 0,
        tuning: WorkerScanTuning::default(),
    }
}

fn open_request_with_producers(producers: Vec<ScanProducerPeer>) -> OpenScanRequest {
    OpenScanRequest {
        producers,
        session_epoch: 7,
        scan_id: PgScanId::new(11),
        output_schema: output_schema(),
        page_kind: import::ARROW_LAYOUT_BATCH_KIND,
        page_flags: 0,
        tuning: WorkerScanTuning::default(),
    }
}

fn activate_generation(region: &TransportRegion) {
    let mut worker = WorkerTransport::attach(region).expect("worker attach");
    worker
        .activate_generation(std::process::id() as i32)
        .expect("activate generation");
    worker.clear_worker_pid();
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

fn wait_for_worker_frame(backend: &mut BackendSlotLease) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut buf = vec![0_u8; 1024];
    loop {
        let received = {
            let mut rx = backend.from_worker_rx();
            rx.recv_frame_into(&mut buf)
        };
        match received {
            Ok(Some(len)) => return buf[..len].to_vec(),
            Ok(None) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for worker frame"
                );
                thread::sleep(Duration::from_millis(1));
            }
            Err(BackendRxError::Ring(RxError::BufferTooSmall { .. })) => {
                panic!("test scratch buffer too small")
            }
            Err(err) => panic!("backend recv failed: {err:?}"),
        }
    }
}

#[test]
fn transport_scan_source_rejects_inbound_ring_below_minimum() {
    let (_region_mem, region) = init_transport_region(
        1,
        MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY - 1,
        MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
    );
    let ingress = Arc::new(TestIngress {
        entries: Mutex::new(BTreeMap::new()),
    });

    let err =
        TransportScanBatchSource::new(region, MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, ingress)
            .expect_err("small inbound ring must be rejected");
    assert!(matches!(
        err,
        WorkerRuntimeError::ScanTransportRingTooSmall {
            direction: "backend_to_worker",
            required,
            actual,
        } if required == MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY
            && actual == MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY - 1
    ));
}

#[test]
fn transport_scan_source_rejects_outbound_ring_below_minimum() {
    let (_region_mem, region) = init_transport_region(
        1,
        MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
        MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY - 1,
    );
    let ingress = Arc::new(TestIngress {
        entries: Mutex::new(BTreeMap::new()),
    });

    let err =
        TransportScanBatchSource::new(region, MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, ingress)
            .expect_err("small outbound ring must be rejected");
    assert!(matches!(
        err,
        WorkerRuntimeError::ScanTransportRingTooSmall {
            direction: "worker_to_backend",
            required,
            actual,
        } if required == MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY
            && actual == MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY - 1
    ));
}

#[test]
fn transport_scan_source_accepts_minimum_asymmetric_scan_rings() {
    let (_region_mem, region) = init_transport_region(
        1,
        MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
        MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
    );
    let ingress = Arc::new(TestIngress {
        entries: Mutex::new(BTreeMap::new()),
    });

    TransportScanBatchSource::new(region, MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, ingress)
        .expect("minimum asymmetric scan rings should be accepted");
}

#[test]
fn transport_scan_source_sends_open_scan_on_dedicated_peer_and_finishes() {
    ignore_sigusr1_for_tests();
    let _guard = test_mutex().lock().unwrap();
    let region = shared_transport_region();
    let mut backend = BackendSlotLease::acquire(&region).expect("backend lease");

    let (_page_mem, page_pool) = init_page_pool(128, 1);
    let (_issuance_mem, issuance_pool) = init_issuance_pool(1);
    let ingress = Arc::new(TestIngress::with_entry(
        7,
        11,
        IssuedRx::new(PageRx::new(page_pool), issuance_pool),
    ));
    let source = TransportScanBatchSource::new(region, 256, ingress).expect("source");

    let mut stream = source
        .open_scan(open_request(backend.backend_lease_slot()))
        .unwrap();

    let open_frame = wait_for_worker_frame(&mut backend);
    let open = decode_worker_scan_to_backend(&open_frame).expect("decode open");
    let WorkerScanToBackendRef::OpenScan {
        session_epoch,
        scan_id,
        scan,
    } = open
    else {
        panic!("expected OpenScan, got {open:?}");
    };
    assert_eq!(session_epoch, 7);
    assert_eq!(scan_id, 11);
    assert_eq!(scan.page_kind, import::ARROW_LAYOUT_BATCH_KIND);
    assert_eq!(scan.page_flags, 0);
    let producers: Vec<_> = scan.producers().iter().collect();
    assert_eq!(producers.len(), 1);
    assert_eq!(producers[0].producer_id, 0);
    assert_eq!(producers[0].role, ProducerRole::Leader);

    let message = runtime_protocol::BackendScanToWorker::ScanFinished {
        session_epoch: 7,
        scan_id: 11,
        producer_id: 0,
    };
    let mut encoded = vec![0_u8; encoded_len_backend_scan_to_worker(message)];
    let written = encode_backend_scan_to_worker_into(message, &mut encoded).expect("encode");
    backend
        .to_worker_tx()
        .send_frame(&encoded[..written])
        .expect("send scan finished");

    let next = futures::executor::block_on(stream.next());
    assert!(next.is_none());
}

#[test]
fn transport_scan_source_surfaces_scan_failure() {
    ignore_sigusr1_for_tests();
    let _guard = test_mutex().lock().unwrap();
    let region = shared_transport_region();
    let mut backend = BackendSlotLease::acquire(&region).expect("backend lease");

    let (_page_mem, page_pool) = init_page_pool(128, 1);
    let (_issuance_mem, issuance_pool) = init_issuance_pool(1);
    let ingress = Arc::new(TestIngress::with_entry(
        7,
        11,
        IssuedRx::new(PageRx::new(page_pool), issuance_pool),
    ));
    let source = TransportScanBatchSource::new(region, 256, ingress).expect("source");

    let mut stream = source
        .open_scan(open_request(backend.backend_lease_slot()))
        .unwrap();
    let _open = wait_for_worker_frame(&mut backend);

    let message = runtime_protocol::BackendScanToWorker::ScanFailed {
        session_epoch: 7,
        scan_id: 11,
        producer_id: 0,
        message: "boom",
    };
    let mut encoded = vec![0_u8; encoded_len_backend_scan_to_worker(message)];
    let written = encode_backend_scan_to_worker_into(message, &mut encoded).expect("encode");
    backend
        .to_worker_tx()
        .send_frame(&encoded[..written])
        .expect("send scan failure");

    let next = futures::executor::block_on(stream.next()).expect("failure item");
    let err = next.expect_err("scan failure");
    assert!(err.to_string().contains("boom"));
    assert!(futures::executor::block_on(stream.next()).is_none());
}

#[test]
fn dropping_transport_scan_stream_sends_cancel_scan() {
    ignore_sigusr1_for_tests();
    let _guard = test_mutex().lock().unwrap();
    let region = shared_transport_region();
    let mut backend = BackendSlotLease::acquire(&region).expect("backend lease");

    let (_page_mem, page_pool) = init_page_pool(128, 1);
    let (_issuance_mem, issuance_pool) = init_issuance_pool(1);
    let ingress = Arc::new(TestIngress::with_entry(
        7,
        11,
        IssuedRx::new(PageRx::new(page_pool), issuance_pool),
    ));
    let source = TransportScanBatchSource::new(region, 256, ingress).expect("source");

    let stream = source
        .open_scan(open_request(backend.backend_lease_slot()))
        .unwrap();
    let _open = wait_for_worker_frame(&mut backend);
    drop(stream);

    let cancel_frame = wait_for_worker_frame(&mut backend);
    let cancel = decode_worker_scan_to_backend(&cancel_frame).expect("decode cancel");
    assert_eq!(
        cancel,
        WorkerScanToBackendRef::CancelScan {
            session_epoch: 7,
            scan_id: 11,
        }
    );
}

#[test]
fn transport_scan_source_waits_for_all_declared_producers() {
    ignore_sigusr1_for_tests();
    let _guard = test_mutex().lock().unwrap();
    let (_region_mem, region) = init_transport_region(2, 256, 256);
    activate_generation(&region);
    let mut leader = BackendSlotLease::acquire(&region).expect("leader lease");
    let mut worker = BackendSlotLease::acquire(&region).expect("worker lease");

    let (_page_mem, page_pool) = init_page_pool(128, 1);
    let (_issuance_mem, issuance_pool) = init_issuance_pool(1);
    let ingress = Arc::new(TestIngress::with_producers(
        7,
        11,
        [0, 1],
        page_pool,
        issuance_pool,
    ));
    let source = TransportScanBatchSource::new(region, 256, ingress).expect("source");
    let request = open_request_with_producers(vec![
        ScanProducerPeer {
            producer_id: 0,
            role: ProducerRoleKind::Leader,
            peer: leader.backend_lease_slot(),
        },
        ScanProducerPeer {
            producer_id: 1,
            role: ProducerRoleKind::Worker,
            peer: worker.backend_lease_slot(),
        },
    ]);

    let mut stream = source.open_scan(request).unwrap();
    let leader_open = wait_for_worker_frame(&mut leader);
    let worker_open = wait_for_worker_frame(&mut worker);
    for frame in [&leader_open, &worker_open] {
        let open = decode_worker_scan_to_backend(frame).expect("decode open");
        let WorkerScanToBackendRef::OpenScan { scan, .. } = open else {
            panic!("expected OpenScan, got {open:?}");
        };
        let producers: Vec<_> = scan.producers().iter().collect();
        assert_eq!(producers.len(), 2);
        assert_eq!(producers[0].producer_id, 0);
        assert_eq!(producers[0].role, ProducerRole::Leader);
        assert_eq!(producers[1].producer_id, 1);
        assert_eq!(producers[1].role, ProducerRole::Worker);
    }

    let leader_done = runtime_protocol::BackendScanToWorker::ScanFinished {
        session_epoch: 7,
        scan_id: 11,
        producer_id: 0,
    };
    let mut encoded = vec![0_u8; encoded_len_backend_scan_to_worker(leader_done)];
    let written = encode_backend_scan_to_worker_into(leader_done, &mut encoded).expect("encode");
    leader
        .to_worker_tx()
        .send_frame(&encoded[..written])
        .expect("send leader finished");
    thread::sleep(Duration::from_millis(10));
    assert!(stream.next().now_or_never().is_none());

    let worker_done = runtime_protocol::BackendScanToWorker::ScanFinished {
        session_epoch: 7,
        scan_id: 11,
        producer_id: 1,
    };
    let mut encoded = vec![0_u8; encoded_len_backend_scan_to_worker(worker_done)];
    let written = encode_backend_scan_to_worker_into(worker_done, &mut encoded).expect("encode");
    worker
        .to_worker_tx()
        .send_frame(&encoded[..written])
        .expect("send worker finished");

    let next = futures::executor::block_on(stream.next());
    assert!(next.is_none());
}
