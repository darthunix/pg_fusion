use crate::{
    BackendPageSource, BackendProducerRole, BackendProducerStep, BackendScanCoordinator, FlowId,
    LogicalTerminal, ProducerDescriptor, ScanOpen, SourcePageStatus, WorkerRoleError,
    WorkerScanRole, WorkerStep,
};
use issuance::{IssuanceConfig, IssuancePool, IssuedRx, IssuedTx};
use pool::{PagePool, PagePoolConfig};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use transfer::{PageRx, PageTx};

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn new(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).expect("layout");
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("allocation");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

fn init_page_pool(page_size: usize, page_count: u32) -> (OwnedRegion, PagePool) {
    let cfg = PagePoolConfig::new(page_size, page_count).expect("pool config");
    let layout = PagePool::layout(cfg).expect("pool layout");
    let region = OwnedRegion::new(layout.size, layout.align);
    let pool = unsafe { PagePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
    (region, pool)
}

fn init_issuance_pool(permit_count: u32) -> (OwnedRegion, IssuancePool) {
    let cfg = IssuanceConfig::new(permit_count).expect("issuance config");
    let layout = IssuancePool::layout(cfg).expect("issuance layout");
    let region = OwnedRegion::new(layout.size, layout.align);
    let pool =
        unsafe { IssuancePool::init_in_place(region.base, layout.size, cfg) }.expect("issuance");
    (region, pool)
}

#[derive(Clone, Copy)]
enum ScriptStep {
    Page { byte: u8, payload_len: usize },
    Eof,
    Error(&'static str),
}

struct StaticSource {
    script: Vec<ScriptStep>,
    next: usize,
    opened: Option<FlowId>,
    closed: bool,
}

impl StaticSource {
    fn new(script: Vec<ScriptStep>) -> Self {
        Self {
            script,
            next: 0,
            opened: None,
            closed: false,
        }
    }
}

impl BackendPageSource for StaticSource {
    type Error = &'static str;

    fn open(&mut self, flow: FlowId) -> Result<(), Self::Error> {
        self.opened = Some(flow);
        Ok(())
    }

    fn fill_next_page(&mut self, payload: &mut [u8]) -> Result<SourcePageStatus, Self::Error> {
        let step = self
            .script
            .get(self.next)
            .copied()
            .expect("script must not be exhausted");
        self.next += 1;
        match step {
            ScriptStep::Page { byte, payload_len } => {
                payload.fill(byte);
                Ok(SourcePageStatus::Page { payload_len })
            }
            ScriptStep::Eof => Ok(SourcePageStatus::Eof),
            ScriptStep::Error(err) => Err(err),
        }
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        self.closed = true;
        Ok(())
    }
}

fn scan_open() -> ScanOpen {
    scan_open_with_id(42)
}

fn scan_open_with_id(scan_id: u64) -> ScanOpen {
    ScanOpen::new(
        FlowId {
            session_epoch: 7,
            scan_id,
        },
        0x4152,
        0x0003,
        vec![ProducerDescriptor::leader(0), ProducerDescriptor::worker(1)],
    )
    .expect("scan open")
}

struct CloseTrackingSource {
    close_calls: Arc<AtomicUsize>,
}

impl BackendPageSource for CloseTrackingSource {
    type Error = &'static str;

    fn open(&mut self, _flow: FlowId) -> Result<(), Self::Error> {
        Ok(())
    }

    fn fill_next_page(&mut self, payload: &mut [u8]) -> Result<SourcePageStatus, Self::Error> {
        payload[..4].copy_from_slice(b"late");
        Ok(SourcePageStatus::Page { payload_len: 4 })
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        self.close_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn two_producer_happy_path_reaches_logical_eof() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 4);
    let (_issuance_region, issuance_pool) = init_issuance_pool(4);

    let tx0 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let tx1 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx0 = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let rx1 = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut coordinator = BackendScanCoordinator::new();
    coordinator.open(scan.clone()).expect("coordinator open");
    let mut worker = WorkerScanRole::new();
    worker.open(scan.clone()).expect("worker open");

    let mut producer0 = BackendProducerRole::new(tx0);
    let mut producer1 = BackendProducerRole::new(tx1);
    producer0
        .open(
            &scan,
            0,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0x11,
                    payload_len: 4096 - 20,
                },
                ScriptStep::Eof,
            ]),
        )
        .expect("producer0 open");
    producer1
        .open(
            &scan,
            1,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0x22,
                    payload_len: 4096 - 20,
                },
                ScriptStep::Eof,
            ]),
        )
        .expect("producer1 open");

    let frame1 = match producer1.step().expect("producer1 page") {
        BackendProducerStep::OutboundPage {
            flow,
            producer_id,
            outbound,
        } => {
            assert_eq!(flow, scan.flow);
            assert_eq!(producer_id, 1);
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected producer1 step: {other:?}"),
    };
    match worker
        .accept_page_frame(scan.flow, 1, &rx1, &frame1)
        .expect("worker page1")
    {
        WorkerStep::Page {
            flow,
            producer_id,
            page,
        } => {
            assert_eq!(flow, scan.flow);
            assert_eq!(producer_id, 1);
            assert_eq!(page.kind(), scan.page_kind);
            assert_eq!(page.flags(), scan.page_flags);
            assert_eq!(page.payload()[0], 0x22);
            drop(page);
        }
        _ => panic!("expected worker page"),
    }

    let frame0 = match producer0.step().expect("producer0 page") {
        BackendProducerStep::OutboundPage {
            flow,
            producer_id,
            outbound,
        } => {
            assert_eq!(flow, scan.flow);
            assert_eq!(producer_id, 0);
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected producer0 step: {other:?}"),
    };
    match worker
        .accept_page_frame(scan.flow, 0, &rx0, &frame0)
        .expect("worker page0")
    {
        WorkerStep::Page {
            flow,
            producer_id,
            page,
        } => {
            assert_eq!(flow, scan.flow);
            assert_eq!(producer_id, 0);
            assert_eq!(page.payload()[0], 0x11);
            drop(page);
        }
        _ => panic!("expected worker page"),
    }

    match producer1.step().expect("producer1 eof") {
        BackendProducerStep::ProducerEof { flow, producer_id } => {
            assert_eq!(
                coordinator
                    .accept_producer_eof(flow, producer_id)
                    .expect("coordinator eof1"),
                None
            );
            assert!(matches!(
                worker
                    .accept_producer_eof(flow, producer_id)
                    .expect("worker eof1"),
                WorkerStep::Idle
            ));
        }
        other => panic!("unexpected producer1 eof step: {other:?}"),
    }

    match producer0.step().expect("producer0 eof") {
        BackendProducerStep::ProducerEof { flow, producer_id } => {
            assert_eq!(
                coordinator
                    .accept_producer_eof(flow, producer_id)
                    .expect("coordinator eof0"),
                Some(LogicalTerminal::LogicalEof { flow })
            );
            assert!(matches!(
                worker
                    .accept_producer_eof(flow, producer_id)
                    .expect("worker eof0"),
                WorkerStep::LogicalEof { flow: eof_flow } if eof_flow == flow
            ));
        }
        other => panic!("unexpected producer0 eof step: {other:?}"),
    }

    producer0.close().expect("producer0 close");
    producer1.close().expect("producer1 close");
    coordinator.close().expect("coordinator close");
    worker.close().expect("worker close");
}

#[test]
fn blocked_producer_retries_after_worker_drops_page() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);

    let tx0 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let tx1 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx0 = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let rx1 = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut worker = WorkerScanRole::new();
    worker.open(scan.clone()).expect("worker open");

    let mut producer0 = BackendProducerRole::new(tx0);
    let mut producer1 = BackendProducerRole::new(tx1);
    producer0
        .open(
            &scan,
            0,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0x33,
                    payload_len: 4096 - 20,
                },
                ScriptStep::Eof,
            ]),
        )
        .expect("producer0 open");
    producer1
        .open(
            &scan,
            1,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0x44,
                    payload_len: 4096 - 20,
                },
                ScriptStep::Eof,
            ]),
        )
        .expect("producer1 open");

    let frame0 = match producer0.step().expect("producer0 page") {
        BackendProducerStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected step: {other:?}"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    assert!(matches!(
        producer1.step().expect("producer1 blocked"),
        BackendProducerStep::Blocked { .. }
    ));

    let held_page = match worker
        .accept_page_frame(scan.flow, 0, &rx0, &frame0)
        .expect("worker page0")
    {
        WorkerStep::Page { page, .. } => page,
        _ => panic!("expected page"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    assert!(matches!(
        producer1.step().expect("producer1 still blocked"),
        BackendProducerStep::Blocked { .. }
    ));

    drop(held_page);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    let frame1 = match producer1.step().expect("producer1 page") {
        BackendProducerStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected step: {other:?}"),
    };
    let page1 = match worker
        .accept_page_frame(scan.flow, 1, &rx1, &frame1)
        .expect("worker page1")
    {
        WorkerStep::Page { page, .. } => page,
        _ => panic!("expected page"),
    };
    drop(page1);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    assert!(matches!(
        producer0.step().expect("producer0 eof"),
        BackendProducerStep::ProducerEof { .. }
    ));
    assert!(matches!(
        worker
            .accept_producer_eof(scan.flow, 0)
            .expect("worker eof0"),
        WorkerStep::Idle
    ));
    producer0.close().expect("producer0 close");

    assert!(matches!(
        producer1.step().expect("producer1 eof"),
        BackendProducerStep::ProducerEof { .. }
    ));
    assert!(matches!(
        worker
            .accept_producer_eof(scan.flow, 1)
            .expect("worker eof1"),
        WorkerStep::LogicalEof { .. }
    ));
    producer1.close().expect("producer1 close");
    worker.close().expect("worker close");
}

#[test]
fn first_producer_error_fails_logical_scan_and_late_page_is_drained() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 2);
    let (_issuance_region, issuance_pool) = init_issuance_pool(2);

    let tx0 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let tx1 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx1 = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut coordinator = BackendScanCoordinator::new();
    coordinator.open(scan.clone()).expect("coordinator open");
    let mut worker = WorkerScanRole::new();
    worker.open(scan.clone()).expect("worker open");

    let mut producer0 = BackendProducerRole::new(tx0);
    let mut producer1 = BackendProducerRole::new(tx1);
    producer0
        .open(
            &scan,
            0,
            StaticSource::new(vec![ScriptStep::Error("boom from producer0")]),
        )
        .expect("producer0 open");
    producer1
        .open(
            &scan,
            1,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0x55,
                    payload_len: 4096 - 20,
                },
                ScriptStep::Eof,
            ]),
        )
        .expect("producer1 open");

    let (flow, producer_id, message) = match producer0.step().expect("producer0 error") {
        BackendProducerStep::ProducerError {
            flow,
            producer_id,
            error,
        } => (flow, producer_id, error.to_string()),
        other => panic!("unexpected producer0 step: {other:?}"),
    };
    assert!(matches!(
        coordinator
            .accept_producer_error(flow, producer_id, message.clone())
            .expect("coordinator error"),
        LogicalTerminal::LogicalError {
            flow: err_flow,
            producer_id: err_producer,
            ..
        } if err_flow == flow && err_producer == producer_id
    ));
    assert!(matches!(
        worker
            .accept_producer_error(flow, producer_id, message)
            .expect("worker error"),
        WorkerStep::LogicalError {
            flow: err_flow,
            producer_id: err_producer,
            ..
        } if err_flow == flow && err_producer == producer_id
    ));

    let late_frame = match producer1.step().expect("producer1 late page") {
        BackendProducerStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected producer1 step: {other:?}"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    assert!(matches!(
        worker.accept_page_frame(scan.flow, 1, &rx1, &late_frame),
        Err(WorkerRoleError::InvalidState { .. })
    ));
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    producer0.close().expect("producer0 close");
    producer1.abort().expect("producer1 abort");
    worker.close().expect("worker close");
    coordinator.close().expect("coordinator close");
}

#[test]
fn producer_page_preserves_partial_payload_len() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);

    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut worker = WorkerScanRole::new();
    worker.open(scan.clone()).expect("worker open");

    let mut producer = BackendProducerRole::new(tx);
    producer
        .open(
            &scan,
            0,
            StaticSource::new(vec![ScriptStep::Page {
                byte: 0x77,
                payload_len: 8,
            }]),
        )
        .expect("producer open");

    let frame = match producer.step().expect("producer page") {
        BackendProducerStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected producer step: {other:?}"),
    };

    match worker
        .accept_page_frame(scan.flow, 0, &rx, &frame)
        .expect("worker page")
    {
        WorkerStep::Page { page, .. } => {
            assert_eq!(page.payload().len(), 8);
            assert_eq!(page.payload(), &[0x77; 8]);
            drop(page);
        }
        _ => panic!("expected page"),
    }
}

#[test]
fn producer_abort_closes_live_source() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);

    let close_calls = Arc::new(AtomicUsize::new(0));
    let source = CloseTrackingSource {
        close_calls: Arc::clone(&close_calls),
    };

    let mut producer = BackendProducerRole::new(tx);
    producer.open(&scan, 0, source).expect("producer open");
    producer.abort().expect("abort");

    assert_eq!(close_calls.load(Ordering::Relaxed), 1);
    assert_eq!(producer.state(), crate::BackendProducerState::Closed);
}

#[test]
fn producer_outbound_page_rolls_back_when_not_marked_sent() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);

    let mut producer = BackendProducerRole::new(tx);
    producer
        .open(
            &scan,
            0,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0x88,
                    payload_len: 16,
                },
                ScriptStep::Eof,
            ]),
        )
        .expect("producer open");

    let outbound = match producer.step().expect("producer page") {
        BackendProducerStep::OutboundPage { outbound, .. } => outbound,
        other => panic!("unexpected producer step: {other:?}"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);
    drop(outbound);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    assert!(matches!(
        producer.step().expect("producer eof"),
        BackendProducerStep::ProducerEof { .. }
    ));
}

#[test]
fn producer_step_blocks_while_previous_outbound_page_is_still_pending() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 2);
    let (_issuance_region, issuance_pool) = init_issuance_pool(2);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);

    let mut producer = BackendProducerRole::new(tx);
    producer
        .open(
            &scan,
            0,
            StaticSource::new(vec![
                ScriptStep::Page {
                    byte: 0xb1,
                    payload_len: 16,
                },
                ScriptStep::Page {
                    byte: 0xb2,
                    payload_len: 16,
                },
            ]),
        )
        .expect("producer open");

    let outbound = match producer.step().expect("producer first page") {
        BackendProducerStep::OutboundPage { outbound, .. } => outbound,
        other => panic!("unexpected producer step: {other:?}"),
    };
    assert_eq!(producer.state(), crate::BackendProducerState::Streaming);

    assert!(matches!(
        producer
            .step()
            .expect("producer blocked on pending outbound"),
        BackendProducerStep::Blocked { .. }
    ));
    assert_eq!(producer.state(), crate::BackendProducerState::Streaming);

    drop(outbound);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn producer_step_blocks_when_page_pool_is_temporarily_empty() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(2);

    let tx0 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let tx1 = IssuedTx::new(PageTx::new(page_pool), issuance_pool);

    let mut producer0 = BackendProducerRole::new(tx0);
    let mut producer1 = BackendProducerRole::new(tx1);
    producer0
        .open(
            &scan,
            0,
            StaticSource::new(vec![ScriptStep::Page {
                byte: 0xc1,
                payload_len: 16,
            }]),
        )
        .expect("producer0 open");
    producer1
        .open(
            &scan,
            1,
            StaticSource::new(vec![ScriptStep::Page {
                byte: 0xc2,
                payload_len: 16,
            }]),
        )
        .expect("producer1 open");

    let outbound0 = match producer0.step().expect("producer0 first page") {
        BackendProducerStep::OutboundPage { outbound, .. } => outbound,
        other => panic!("unexpected producer0 step: {other:?}"),
    };
    assert!(matches!(
        producer1.step().expect("producer1 blocked on page pool"),
        BackendProducerStep::Blocked { .. }
    ));
    assert_eq!(producer1.state(), crate::BackendProducerState::Opened);

    drop(outbound0);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn stale_frame_from_previous_flow_is_drained_before_unexpected_flow() {
    let scan_a = scan_open_with_id(42);
    let scan_b = scan_open_with_id(43);
    let (_page_region, page_pool) = init_page_pool(4096, 2);
    let (_issuance_region, issuance_pool) = init_issuance_pool(2);

    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut worker = WorkerScanRole::new();
    worker.open(scan_a.clone()).expect("worker open scan a");
    assert!(matches!(
        worker
            .accept_producer_error(scan_a.flow, 0, "close scan a".to_string())
            .expect("worker terminal error"),
        WorkerStep::LogicalError { .. }
    ));
    worker.close().expect("worker close scan a");
    worker.open(scan_b.clone()).expect("worker open scan b");

    let mut producer_a = BackendProducerRole::new(tx.clone());
    producer_a
        .open(
            &scan_a,
            0,
            StaticSource::new(vec![ScriptStep::Page {
                byte: 0x99,
                payload_len: 12,
            }]),
        )
        .expect("producer a open");
    let stale_frame = match producer_a.step().expect("producer a page") {
        BackendProducerStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected producer a step: {other:?}"),
    };

    assert!(matches!(
        worker.accept_page_frame(scan_a.flow, 0, &rx, &stale_frame),
        Err(WorkerRoleError::UnexpectedFlow { expected, actual })
            if expected == scan_b.flow && actual == scan_a.flow
    ));
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    let mut producer_b = BackendProducerRole::new(tx);
    producer_b
        .open(
            &scan_b,
            0,
            StaticSource::new(vec![ScriptStep::Page {
                byte: 0xaa,
                payload_len: 12,
            }]),
        )
        .expect("producer b open");
    let frame_b = match producer_b.step().expect("producer b page") {
        BackendProducerStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            frame
        }
        other => panic!("unexpected producer b step: {other:?}"),
    };

    match worker
        .accept_page_frame(scan_b.flow, 0, &rx, &frame_b)
        .expect("worker page b")
    {
        WorkerStep::Page { page, .. } => {
            assert_eq!(page.payload().len(), 12);
            assert_eq!(page.payload(), &[0xaa; 12]);
            drop(page);
        }
        _ => panic!("expected page"),
    }
}

#[test]
fn wrong_page_envelope_fails_worker_and_releases_page() {
    let scan = scan_open();
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut worker = WorkerScanRole::new();
    worker.open(scan.clone()).expect("worker open");

    let mut writer = tx
        .begin(scan.page_kind + 1, scan.page_flags)
        .expect("begin wrong kind");
    writer.payload_mut().fill(0x66);
    let payload_len = writer.payload_mut().len();
    let outbound = writer.finish_with_payload_len(payload_len).expect("finish");
    let frame = outbound.frame();
    outbound.mark_sent();
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    assert!(matches!(
        worker
            .accept_page_frame(scan.flow, 0, &rx, &frame)
            .expect("logical error"),
        WorkerStep::LogicalError {
            flow,
            producer_id,
            ..
        } if flow == scan.flow && producer_id == 0
    ));
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
    worker.close().expect("worker close");
}
