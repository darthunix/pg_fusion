use arrow_array::{Int32Array, StringViewArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use backend_service::{
    ActiveScanDriver, BackendService, BackendServiceConfig, BackendServiceError,
    BeginExecutionOutput, ExecutionKey, ExplainInput, OpenScanInput, ScanStreamStep,
    ScanYieldReason, StartExecutionInput,
};
use control_transport::{BackendLeaseId, BackendLeaseSlot, TransportRegion, TransportRegionLayout};
use datafusion_common::ScalarValue;
use import::ArrowPageDecoder;
use issuance::{IssuanceConfig, IssuancePool, IssueEvent, IssuedRx, IssuedTx};
use pgrx::prelude::*;
use plan_builder::{PlanBuildInput, PlanBuilder};
use plan_flow::{FlowId as PlanFlowId, PlanOpen, WorkerPlanRole, WorkerStep};
use pool::{PagePool, PagePoolConfig};
use runtime_protocol::{
    decode_worker_scan_to_backend, encode_worker_scan_to_backend_into,
    encoded_len_worker_scan_to_backend, BackendExecutionToWorker as BackendToWorker,
    ExecutionFailureCode, ProducerDescriptorWire, ProducerRole, ScanChannelDescriptorWire,
    ScanFlowDescriptor, WorkerScanToBackend, WorkerScanToBackendRef,
};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;
use transfer::{PageRx, PageTx};

const BACKEND_SERVICE_TABLE: &str = "pg_temp.backend_service_runtime_test";
const TEST_SLOT_ID: u32 = 17;
const TEST_PAGE_SIZE: usize = 8192;
const TEST_PAGE_COUNT: u32 = 32;
const TEST_PERMIT_COUNT: u32 = 32;

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn from_size_align(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).expect("region layout");
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("region allocation");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

struct IssuedTransportHarness {
    _page_region: OwnedRegion,
    _issuance_region: OwnedRegion,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
}

struct ControlTransportHarness {
    _region: OwnedRegion,
    region: TransportRegion,
}

impl ControlTransportHarness {
    fn new(slot_count: u32) -> Self {
        let layout = TransportRegionLayout::new(slot_count, 256, 256).expect("transport layout");
        let region_mem = OwnedRegion::from_size_align(layout.size, layout.align);
        let region =
            unsafe { TransportRegion::init_in_place(region_mem.base, layout.size, layout) }
                .expect("transport init");
        assert_eq!(
            region
                .activate_worker_generation(std::process::id() as i32)
                .expect("activate worker generation"),
            1
        );
        Self {
            _region: region_mem,
            region,
        }
    }

    fn region(&self) -> &TransportRegion {
        &self.region
    }
}

impl IssuedTransportHarness {
    fn new() -> Self {
        Self::with_counts(TEST_PAGE_COUNT, TEST_PERMIT_COUNT)
    }

    fn with_counts(page_count: u32, permit_count: u32) -> Self {
        let page_cfg = PagePoolConfig::new(TEST_PAGE_SIZE, page_count).expect("page config");
        let page_layout = PagePool::layout(page_cfg).expect("page layout");
        let page_region = OwnedRegion::from_size_align(page_layout.size, page_layout.align);
        let page_pool =
            unsafe { PagePool::init_in_place(page_region.base, page_layout.size, page_cfg) }
                .expect("page pool");

        let issuance_cfg = IssuanceConfig::new(permit_count).expect("issuance config");
        let issuance_layout = IssuancePool::layout(issuance_cfg).expect("issuance layout");
        let issuance_region =
            OwnedRegion::from_size_align(issuance_layout.size, issuance_layout.align);
        let issuance_pool = unsafe {
            IssuancePool::init_in_place(issuance_region.base, issuance_layout.size, issuance_cfg)
        }
        .expect("issuance pool");

        Self {
            _page_region: page_region,
            _issuance_region: issuance_region,
            page_pool,
            issuance_pool,
        }
    }

    fn tx(&self) -> IssuedTx {
        IssuedTx::new(PageTx::new(self.page_pool), self.issuance_pool)
    }

    fn rx(&self) -> IssuedRx {
        IssuedRx::new(PageRx::new(self.page_pool), self.issuance_pool)
    }

    fn assert_drained(&self) {
        assert_eq!(self.page_pool.snapshot().leased_pages, 0);
        assert_eq!(self.issuance_pool.snapshot().leased_permits, 0);
    }
}

struct LatestSnapshotGuard;

impl LatestSnapshotGuard {
    unsafe fn acquire() -> Self {
        let snapshot = pg_sys::GetLatestSnapshot();
        pg_sys::PushActiveSnapshot(snapshot);
        Self
    }
}

impl Drop for LatestSnapshotGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::PopActiveSnapshot();
        }
    }
}

struct ActiveExecutionGuard {
    key: Option<ExecutionKey>,
}

impl ActiveExecutionGuard {
    fn new(key: ExecutionKey) -> Self {
        Self { key: Some(key) }
    }

    fn disarm(&mut self) {
        self.key = None;
    }
}

impl Drop for ActiveExecutionGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let _ = BackendService::accept_cancel_execution(key.slot_id, key.session_epoch);
        }
    }
}

fn reset_backend_service_table() {
    Spi::run(&format!("DROP TABLE IF EXISTS {BACKEND_SERVICE_TABLE}")).unwrap();
    Spi::run(&format!(
        "CREATE TEMP TABLE {BACKEND_SERVICE_TABLE} (id int4 NOT NULL, payload text NOT NULL)"
    ))
    .unwrap();
    Spi::run(&format!(
        "INSERT INTO {BACKEND_SERVICE_TABLE} VALUES \
         (1, 'one'), \
         (2, 'two'), \
         (3, 'three')"
    ))
    .unwrap();
}

pub fn backend_service_render_explain_uses_physical_plan_and_pg_leaf() {
    reset_backend_service_table();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let sql = format!("SELECT id FROM {BACKEND_SERVICE_TABLE} WHERE id > 0 LIMIT 1");

    let rendered = BackendService::render_explain(ExplainInput {
        sql: &sql,
        params: Vec::new(),
        options: Default::default(),
        config: BackendServiceConfig::default(),
    })
    .expect("render physical explain");

    assert!(
        rendered.contains("PostgreSQL Scan:"),
        "expected PostgreSQL scan exec in physical plan: {rendered}"
    );
    assert!(
        !rendered.contains("PgFusionPgScanExec"),
        "internal scan exec name should not be rendered in default explain: {rendered}"
    );
    assert!(
        !rendered.contains("scan_id="),
        "scan id should not be rendered in default explain: {rendered}"
    );
    assert!(
        !rendered.contains("planner_fetch_hint="),
        "planner fetch hint should not be rendered in default explain: {rendered}"
    );
    assert!(
        !rendered.contains("PgScan:"),
        "logical PgScan should not be rendered in backend explain: {rendered}"
    );
    assert!(
        !rendered.contains("pg_plan=\""),
        "PostgreSQL leaf explain should not be rendered inline: {rendered}"
    );
    assert!(
        !rendered.contains("PostgreSQL Plan:"),
        "PostgreSQL scan block should not render a redundant header: {rendered}"
    );
    assert!(
        rendered.contains("Seq Scan") || rendered.contains("Index Scan"),
        "expected PostgreSQL scan node in leaf explain: {rendered}"
    );
    let pg_scan_block = rendered
        .lines()
        .skip_while(|line| !line.contains("PostgreSQL Scan:"))
        .skip(1);
    assert!(
        pg_scan_block
            .take(4)
            .any(|line| line.contains("Seq Scan") || line.contains("Index Scan")),
        "expected PostgreSQL scan node on a separate line: {rendered}"
    );
    assert!(
        rendered.contains("PostgreSQL Scan:")
            && rendered.contains("(soft_limit=1, local_row_cap=1)"),
        "expected scan limits in parentheses in leaf explain: {rendered}"
    );
}

pub fn backend_service_render_explain_materializes_retaining_sort_input() {
    reset_backend_service_table();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let sql = format!("SELECT id, payload FROM {BACKEND_SERVICE_TABLE} ORDER BY payload");

    let rendered = BackendService::render_explain(ExplainInput {
        sql: &sql,
        params: Vec::new(),
        options: Default::default(),
        config: BackendServiceConfig::default(),
    })
    .expect("render physical explain");

    assert!(
        rendered.contains("SortExec:"),
        "expected a DataFusion sort in physical explain: {rendered}"
    );
    assert!(
        rendered.contains("PageMaterializeExec"),
        "retaining sort input should materialize page-backed batches: {rendered}"
    );
    let materialize_block = rendered
        .lines()
        .skip_while(|line| !line.contains("PageMaterializeExec"))
        .skip(1);
    assert!(
        materialize_block
            .take(4)
            .any(|line| line.contains("PostgreSQL Scan:")),
        "PageMaterializeExec should sit directly above the PostgreSQL scan subtree: {rendered}"
    );
}

pub fn backend_service_render_explain_keeps_aggregate_scan_zero_copy() {
    reset_backend_service_table();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let sql = format!("SELECT avg(id) FROM {BACKEND_SERVICE_TABLE}");

    let rendered = BackendService::render_explain(ExplainInput {
        sql: &sql,
        params: Vec::new(),
        options: Default::default(),
        config: BackendServiceConfig::default(),
    })
    .expect("render physical explain");

    assert!(
        rendered.contains("AggregateExec:"),
        "expected a DataFusion aggregate in physical explain: {rendered}"
    );
    assert!(
        !rendered.contains("PageMaterializeExec"),
        "plain aggregate scan should stay zero-copy in the v1 policy: {rendered}"
    );
}

fn test_query() -> String {
    format!("SELECT id, payload FROM {BACKEND_SERVICE_TABLE} WHERE id > 0")
}

fn self_join_query() -> String {
    format!(
        "SELECT l.id, r.payload \
         FROM {BACKEND_SERVICE_TABLE} AS l \
         JOIN {BACKEND_SERVICE_TABLE} AS r ON l.id = r.id"
    )
}

fn transport_schema(schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Utf8 => Field::new(field.name(), DataType::Utf8View, field.is_nullable()),
                DataType::Binary => {
                    Field::new(field.name(), DataType::BinaryView, field.is_nullable())
                }
                _ => field.as_ref().clone(),
            })
            .collect::<Vec<_>>(),
    ))
}

fn begin_and_finalize_execution(
    slot_id: u32,
    sql: &str,
    config: &BackendServiceConfig,
    scan_slots: &TransportRegion,
) -> BeginExecutionOutput {
    let plan_transport = IssuedTransportHarness::new();
    let plan_rx = plan_transport.rx();
    let begin = BackendService::begin_execution(StartExecutionInput {
        slot_id,
        sql,
        params: Vec::<ScalarValue>::new(),
        plan_tx: plan_transport.tx(),
        scan_slot_region: scan_slots,
        config: config.clone(),
        scan_worker_launcher: None,
    })
    .expect("begin execution");

    let BackendToWorker::StartExecution {
        session_epoch,
        plan,
        scans,
    } = begin.control()
    else {
        panic!("begin execution must emit StartExecution");
    };
    assert_eq!(scans.len(), begin.scan_channels.len());

    let open = PlanOpen::new(
        PlanFlowId {
            session_epoch,
            plan_id: plan.plan_id,
        },
        plan.page_kind,
        plan.page_flags,
    );
    let mut worker = WorkerPlanRole::new();
    worker.open(open).expect("worker plan open");

    loop {
        match BackendService::step_execution_start().expect("step execution start") {
            plan_flow::BackendPlanStep::OutboundPage { flow, outbound } => {
                let frame = outbound.frame();
                outbound.mark_sent();
                assert!(matches!(
                    worker
                        .accept_frame(flow, &plan_rx, &frame)
                        .expect("accept plan page"),
                    WorkerStep::Idle
                ));
            }
            plan_flow::BackendPlanStep::CloseFrame { flow, frame } => {
                match worker
                    .accept_frame(flow, &plan_rx, &frame)
                    .expect("accept plan close")
                {
                    WorkerStep::Plan {
                        flow: decoded,
                        plan,
                    } => {
                        assert_eq!(decoded, flow);
                        assert!(
                            !plan.display_indent().to_string().is_empty(),
                            "decoded plan should not be empty"
                        );
                    }
                    other => panic!("unexpected worker step after close: {other:?}"),
                }
                break;
            }
            plan_flow::BackendPlanStep::Blocked { .. } => {
                panic!("unexpected plan-flow backpressure in backend_service test")
            }
            plan_flow::BackendPlanStep::LogicalError { message, .. } => {
                panic!("unexpected logical plan publication error: {message}")
            }
        }
    }

    let finalized = BackendService::finalize_execution_start().expect("finalize execution start");
    assert_eq!(finalized, begin.key);
    worker.close().expect("worker plan close");
    plan_transport.assert_drained();
    begin
}

fn try_open_scan_with_runtime_protocol<'a>(
    peer: BackendLeaseSlot,
    session_epoch: u64,
    scan_id: u64,
    config: &BackendServiceConfig,
    scan_tx: IssuedTx,
) -> Result<Option<ActiveScanDriver>, BackendServiceError> {
    let producers = [ProducerDescriptorWire {
        producer_id: 0,
        role: ProducerRole::Leader,
    }];
    let descriptor =
        ScanFlowDescriptor::new(config.scan_page_kind, config.scan_page_flags, &producers)
            .expect("scan descriptor");
    let message = WorkerScanToBackend::OpenScan {
        session_epoch,
        scan_id,
        scan: descriptor,
    };
    let mut encoded = vec![0u8; encoded_len_worker_scan_to_backend(message)];
    let written =
        encode_worker_scan_to_backend_into(message, &mut encoded).expect("encode open scan");
    let decoded = decode_worker_scan_to_backend(&encoded[..written]).expect("decode open scan");

    let WorkerScanToBackendRef::OpenScan {
        session_epoch: decoded_epoch,
        scan_id: decoded_scan_id,
        scan,
    } = decoded
    else {
        panic!("expected open-scan message");
    };
    assert_eq!(decoded_epoch, session_epoch);
    assert_eq!(decoded_scan_id, scan_id);

    BackendService::open_scan(OpenScanInput {
        peer,
        session_epoch,
        scan_id,
        scan,
        scan_tx,
    })
}

fn open_scan_with_runtime_protocol<'a>(
    peer: BackendLeaseSlot,
    session_epoch: u64,
    scan_id: u64,
    config: &BackendServiceConfig,
    scan_tx: IssuedTx,
) -> ActiveScanDriver {
    let opened = try_open_scan_with_runtime_protocol(peer, session_epoch, scan_id, config, scan_tx)
        .expect("open scan");
    opened.expect("fresh scan open must be accepted")
}

fn step_scan_and_drain_page(driver: &mut ActiveScanDriver, scan_rx: &IssuedRx) -> bool {
    match driver.step().expect("step scan") {
        ScanStreamStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            match scan_rx.accept(&frame).expect("accept scan frame") {
                IssueEvent::Page(page) => drop(page),
                IssueEvent::Closed => panic!("scan transport should not emit close frame"),
            }
            false
        }
        ScanStreamStep::YieldForControl { reason } => {
            panic!("unexpected control yield in multi-scan happy path: {reason:?}")
        }
        ScanStreamStep::Finished { .. } => true,
        ScanStreamStep::Failed { message, .. } => {
            panic!("unexpected scan failure in multi-scan happy path: {message}")
        }
    }
}

fn peer_from_scan_channels(begin: &BeginExecutionOutput, scan_id: u64) -> BackendLeaseSlot {
    let descriptor = begin
        .scan_channels
        .iter()
        .find(|descriptor| descriptor.scan_id == scan_id)
        .unwrap_or_else(|| panic!("missing scan channel for scan_id {scan_id}"));
    backend_lease_slot_from_wire(*descriptor)
}

fn backend_lease_slot_from_wire(channel: ScanChannelDescriptorWire) -> BackendLeaseSlot {
    BackendLeaseSlot::new(
        channel.peer.slot_id(),
        BackendLeaseId::new(channel.peer.generation(), channel.peer.lease_epoch()),
    )
}

pub fn backend_service_streams_scan_under_saved_snapshot() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = test_query();
    let scan_transport = IssuedTransportHarness::new();
    let scan_slots = ControlTransportHarness::new(8);
    let mut config = BackendServiceConfig::default();
    config.scan_fetch_batch_rows = 2;
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    Spi::run(&format!(
        "INSERT INTO {BACKEND_SERVICE_TABLE} VALUES (4, 'late')"
    ))
    .unwrap();

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build scan metadata");
    assert_eq!(built.scans.len(), 1, "expected exactly one leaf scan");
    let spec = &built.scans[0];
    let schema = transport_schema(spec.arrow_schema());
    let scan_peer = peer_from_scan_channels(&begin, spec.scan_id.get());

    let scan_rx = scan_transport.rx();
    let mut driver = open_scan_with_runtime_protocol(
        scan_peer,
        begin.key.session_epoch,
        spec.scan_id.get(),
        &config,
        scan_transport.tx(),
    );

    let decoder = ArrowPageDecoder::new(schema).expect("scan decoder");
    let mut rows = Vec::<(i32, String)>::new();
    loop {
        match driver.step().expect("step scan") {
            ScanStreamStep::OutboundPage { outbound, .. } => {
                let frame = outbound.frame();
                outbound.mark_sent();
                let page = match scan_rx.accept(&frame).expect("accept scan frame") {
                    IssueEvent::Page(page) => page,
                    IssueEvent::Closed => panic!("scan transport should not emit close frame"),
                };
                let batch = decoder.import_owned(page).expect("import scan page");
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id column must be Int32");
                let payloads = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("payload column must be Utf8View");
                for index in 0..batch.num_rows() {
                    rows.push((ids.value(index), payloads.value(index).to_string()));
                }
            }
            ScanStreamStep::YieldForControl { reason } => {
                panic!("unexpected scan yield during happy path: {reason:?}")
            }
            ScanStreamStep::Finished { .. } => break,
            ScanStreamStep::Failed { message, .. } => {
                panic!("unexpected scan failure: {message}")
            }
        }
    }

    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ]
    );
    scan_transport.assert_drained();

    assert!(
        driver.complete_execution().expect("complete execution"),
        "fresh complete message must be accepted"
    );
    execution_guard.disarm();
}

pub fn backend_service_yields_for_control_on_permit_backpressure() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = test_query();
    let scan_transport = IssuedTransportHarness::with_counts(1, 1);
    let scan_slots = ControlTransportHarness::new(8);
    let config = BackendServiceConfig::default();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build scan metadata");
    assert_eq!(built.scans.len(), 1, "expected exactly one leaf scan");
    let spec = &built.scans[0];
    let scan_peer = peer_from_scan_channels(&begin, spec.scan_id.get());

    let scan_rx = scan_transport.rx();
    let mut driver = open_scan_with_runtime_protocol(
        scan_peer,
        begin.key.session_epoch,
        spec.scan_id.get(),
        &config,
        scan_transport.tx(),
    );

    let held_page = match driver.step().expect("first scan step") {
        ScanStreamStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            match scan_rx.accept(&frame).expect("accept first scan frame") {
                IssueEvent::Page(page) => page,
                IssueEvent::Closed => panic!("unexpected close frame"),
            }
        }
        other => panic!("expected first outbound page, got {other:?}"),
    };

    match driver.step().expect("step scan while permit is held") {
        ScanStreamStep::YieldForControl {
            reason: ScanYieldReason::PermitBackpressure,
        } => {}
        other => panic!("expected control yield on held permit, got {other:?}"),
    }

    assert!(matches!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch),
        Err(BackendServiceError::ExecutionScansNotTerminal { .. })
    ));
    assert!(matches!(
        driver.complete_execution(),
        Err(BackendServiceError::ProtocolViolation(message))
            if message.contains("before scan stream reaches Finished")
    ));

    drop(held_page);

    loop {
        match driver.step().expect("step scan after control yield") {
            ScanStreamStep::OutboundPage { outbound, .. } => {
                let frame = outbound.frame();
                outbound.mark_sent();
                match scan_rx
                    .accept(&frame)
                    .expect("accept scan frame after yield")
                {
                    IssueEvent::Page(page) => drop(page),
                    IssueEvent::Closed => panic!("unexpected close frame"),
                }
            }
            ScanStreamStep::YieldForControl { reason } => {
                panic!("unexpected repeated control yield after permit release: {reason:?}")
            }
            ScanStreamStep::Finished { .. } => break,
            ScanStreamStep::Failed { message, .. } => {
                panic!("unexpected scan failure after yield: {message}")
            }
        }
    }

    scan_transport.assert_drained();
    assert!(
        driver
            .complete_execution()
            .expect("complete execution after control yield"),
        "fresh complete message must be accepted"
    );
    execution_guard.disarm();
}

pub fn backend_service_driver_fail_execution_from_control_yield() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = test_query();
    let scan_transport = IssuedTransportHarness::with_counts(1, 1);
    let scan_slots = ControlTransportHarness::new(8);
    let config = BackendServiceConfig::default();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build scan metadata");
    assert_eq!(built.scans.len(), 1, "expected exactly one leaf scan");
    let spec = &built.scans[0];
    let scan_peer = peer_from_scan_channels(&begin, spec.scan_id.get());

    let scan_rx = scan_transport.rx();
    let mut driver = open_scan_with_runtime_protocol(
        scan_peer,
        begin.key.session_epoch,
        spec.scan_id.get(),
        &config,
        scan_transport.tx(),
    );

    let held_page = match driver.step().expect("first scan step") {
        ScanStreamStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            match scan_rx.accept(&frame).expect("accept first scan frame") {
                IssueEvent::Page(page) => page,
                IssueEvent::Closed => panic!("unexpected close frame"),
            }
        }
        other => panic!("expected first outbound page, got {other:?}"),
    };

    match driver.step().expect("step scan while permit is held") {
        ScanStreamStep::YieldForControl {
            reason: ScanYieldReason::PermitBackpressure,
        } => {}
        other => panic!("expected control yield on held permit, got {other:?}"),
    }

    assert!(
        driver
            .fail_execution(ExecutionFailureCode::Internal, Some(7))
            .expect("driver fail execution from control yield"),
        "driver fail should terminate the active execution"
    );

    drop(held_page);
    scan_transport.assert_drained();
    assert!(
        !BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch)
            .expect("late complete should be ignored after driver fail"),
        "late complete must not revive an execution failed by the driver"
    );
    execution_guard.disarm();
}

pub fn backend_service_wait_interrupt_cleans_up_active_execution() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = test_query();
    let scan_transport = IssuedTransportHarness::with_counts(1, 1);
    let scan_slots = ControlTransportHarness::new(8);
    let config = BackendServiceConfig::default();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build scan metadata");
    assert_eq!(built.scans.len(), 1, "expected exactly one leaf scan");
    let spec = &built.scans[0];
    let scan_peer = peer_from_scan_channels(&begin, spec.scan_id.get());

    let scan_rx = scan_transport.rx();
    let mut driver = open_scan_with_runtime_protocol(
        scan_peer,
        begin.key.session_epoch,
        spec.scan_id.get(),
        &config,
        scan_transport.tx(),
    );

    let held_page = match driver.step().expect("first scan step") {
        ScanStreamStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            match scan_rx.accept(&frame).expect("accept first scan frame") {
                IssueEvent::Page(page) => page,
                IssueEvent::Closed => panic!("unexpected close frame"),
            }
        }
        other => panic!("expected first outbound page, got {other:?}"),
    };

    BackendService::inject_wait_for_scan_backpressure_error_for_tests("synthetic wait interrupt");
    let err = driver
        .step()
        .expect_err("synthetic wait interrupt must fail the active execution");
    assert!(
        matches!(err, BackendServiceError::Postgres(message) if message.contains("synthetic wait interrupt"))
    );
    BackendService::clear_wait_for_scan_backpressure_error_for_tests();

    drop(held_page);
    scan_transport.assert_drained();

    assert!(
        !BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch)
            .expect("late complete should be ignored after wait interrupt cleanup"),
        "late complete must not revive an execution cleaned up after wait interrupt",
    );
    execution_guard.disarm();

    let restarted =
        begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    assert!(
        restarted.key.session_epoch > begin.key.session_epoch,
        "a fresh execution should start after wait-interrupt cleanup"
    );
    assert!(
        BackendService::accept_cancel_execution(TEST_SLOT_ID, restarted.key.session_epoch)
            .expect("cancel restarted execution"),
        "restarted execution should be cancellable after cleanup"
    );
}

pub fn backend_service_stale_cancel_is_ignored_after_new_execution() {
    reset_backend_service_table();
    let query = "SELECT 1".to_string();
    let config = BackendServiceConfig::default();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let scan_slots = ControlTransportHarness::new(8);

    let first = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut first_guard = ActiveExecutionGuard::new(first.key);
    assert!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, first.key.session_epoch)
            .expect("complete first execution")
    );
    first_guard.disarm();

    let second = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut second_guard = ActiveExecutionGuard::new(second.key);

    assert!(
        !BackendService::accept_cancel_execution(TEST_SLOT_ID, first.key.session_epoch)
            .expect("stale cancel should be ignored")
    );
    assert!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, second.key.session_epoch)
            .expect("complete second execution"),
        "fresh execution must remain active after stale cancel"
    );
    second_guard.disarm();
}

pub fn backend_service_cancel_during_stream_marks_scan_used() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = test_query();
    let scan_transport = IssuedTransportHarness::new();
    let scan_slots = ControlTransportHarness::new(8);
    let config = BackendServiceConfig::default();
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build scan metadata");
    assert_eq!(built.scans.len(), 1, "expected exactly one leaf scan");
    let spec = &built.scans[0];
    let scan_peer = peer_from_scan_channels(&begin, spec.scan_id.get());

    let scan_rx = scan_transport.rx();
    let mut driver = open_scan_with_runtime_protocol(
        scan_peer,
        begin.key.session_epoch,
        spec.scan_id.get(),
        &config,
        scan_transport.tx(),
    );

    match driver.step().expect("step scan before cancel") {
        ScanStreamStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            outbound.mark_sent();
            let page = match scan_rx.accept(&frame).expect("accept scan frame") {
                IssueEvent::Page(page) => page,
                IssueEvent::Closed => panic!("scan transport should not emit close frame"),
            };
            drop(page);
        }
        other => panic!("expected outbound page before cancel, got {other:?}"),
    }

    assert!(
        driver.cancel_scan().expect("cancel active scan"),
        "cancel should be accepted for the active scan"
    );
    assert!(matches!(
        try_open_scan_with_runtime_protocol(
            scan_peer,
            begin.key.session_epoch,
            spec.scan_id.get(),
            &config,
            scan_transport.tx(),
        ),
        Err(BackendServiceError::ScanAlreadyUsed { scan_id })
            if scan_id == spec.scan_id.get()
    ));
    scan_transport.assert_drained();

    assert!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch)
            .expect("complete execution after cancel"),
        "execution should still complete after scan cancellation"
    );
    execution_guard.disarm();
}

pub fn backend_service_rejects_descriptor_mismatch_without_poisoning_execution() {
    reset_backend_service_table();

    let query = test_query();
    let config = BackendServiceConfig::default();
    let scan_transport = IssuedTransportHarness::new();
    let scan_slots = ControlTransportHarness::new(8);
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build scan metadata");
    assert_eq!(built.scans.len(), 1, "expected exactly one leaf scan");
    let spec = &built.scans[0];
    let scan_peer = peer_from_scan_channels(&begin, spec.scan_id.get());

    let mut mismatched = config.clone();
    mismatched.scan_page_flags ^= 0x0001;
    assert!(matches!(
        try_open_scan_with_runtime_protocol(
            scan_peer,
            begin.key.session_epoch,
            spec.scan_id.get(),
            &mismatched,
            scan_transport.tx(),
        ),
        Err(BackendServiceError::ProtocolViolation(message))
            if message.contains("scan descriptor mismatch")
    ));

    let mut driver = open_scan_with_runtime_protocol(
        scan_peer,
        begin.key.session_epoch,
        spec.scan_id.get(),
        &config,
        scan_transport.tx(),
    );
    assert!(driver
        .cancel_scan()
        .expect("cancel scan after successful retry"));
    scan_transport.assert_drained();

    assert!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch)
            .expect("complete execution after descriptor mismatch"),
        "fresh execution should remain usable after descriptor mismatch"
    );
    execution_guard.disarm();
}

pub fn backend_service_interleaves_two_scan_portals_under_shared_spi() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = self_join_query();
    let scan_transport_left = IssuedTransportHarness::new();
    let scan_transport_right = IssuedTransportHarness::new();
    let scan_slots = ControlTransportHarness::new(16);
    let mut config = BackendServiceConfig::default();
    config.scan_fetch_batch_rows = 1;
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build multi-scan metadata");
    assert_eq!(built.scans.len(), 2, "expected exactly two leaf scans");
    let left_spec = &built.scans[0];
    let right_spec = &built.scans[1];
    let left_peer = peer_from_scan_channels(&begin, left_spec.scan_id.get());
    let right_peer = peer_from_scan_channels(&begin, right_spec.scan_id.get());

    let left_rx = scan_transport_left.rx();
    let right_rx = scan_transport_right.rx();
    let mut left_driver = open_scan_with_runtime_protocol(
        left_peer,
        begin.key.session_epoch,
        left_spec.scan_id.get(),
        &config,
        scan_transport_left.tx(),
    );
    let mut right_driver = open_scan_with_runtime_protocol(
        right_peer,
        begin.key.session_epoch,
        right_spec.scan_id.get(),
        &config,
        scan_transport_right.tx(),
    );

    let mut left_finished = false;
    let mut right_finished = false;
    while !left_finished || !right_finished {
        if !left_finished {
            left_finished = step_scan_and_drain_page(&mut left_driver, &left_rx);
        }
        if !right_finished {
            right_finished = step_scan_and_drain_page(&mut right_driver, &right_rx);
        }
    }

    scan_transport_left.assert_drained();
    scan_transport_right.assert_drained();
    assert!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch)
            .expect("complete execution after interleaved multi-scan"),
        "execution should complete after both scan portals finish"
    );
    execution_guard.disarm();
}

pub fn backend_service_drop_finished_driver_does_not_cancel_sibling_scan() {
    reset_backend_service_table();
    Spi::run("SET LOCAL max_parallel_workers_per_gather = 0").unwrap();

    let query = self_join_query();
    let scan_transport_left = IssuedTransportHarness::new();
    let scan_transport_right = IssuedTransportHarness::new();
    let scan_slots = ControlTransportHarness::new(16);
    let mut config = BackendServiceConfig::default();
    config.scan_fetch_batch_rows = 1;
    let _snapshot = unsafe { LatestSnapshotGuard::acquire() };
    let begin = begin_and_finalize_execution(TEST_SLOT_ID, &query, &config, scan_slots.region());
    let mut execution_guard = ActiveExecutionGuard::new(begin.key);

    let built = PlanBuilder::new()
        .build(PlanBuildInput {
            sql: &query,
            params: Vec::<ScalarValue>::new(),
        })
        .expect("build multi-scan metadata");
    assert_eq!(built.scans.len(), 2, "expected exactly two leaf scans");
    let left_spec = &built.scans[0];
    let right_spec = &built.scans[1];
    let left_peer = peer_from_scan_channels(&begin, left_spec.scan_id.get());
    let right_peer = peer_from_scan_channels(&begin, right_spec.scan_id.get());

    let left_rx = scan_transport_left.rx();
    let right_rx = scan_transport_right.rx();
    let mut left_driver = open_scan_with_runtime_protocol(
        left_peer,
        begin.key.session_epoch,
        left_spec.scan_id.get(),
        &config,
        scan_transport_left.tx(),
    );
    let mut right_driver = open_scan_with_runtime_protocol(
        right_peer,
        begin.key.session_epoch,
        right_spec.scan_id.get(),
        &config,
        scan_transport_right.tx(),
    );

    while !step_scan_and_drain_page(&mut left_driver, &left_rx) {}

    assert!(matches!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch),
        Err(BackendServiceError::ExecutionScansNotTerminal {
            unfinished_count: 1,
            ..
        })
    ));

    drop(left_driver);

    while !step_scan_and_drain_page(&mut right_driver, &right_rx) {}

    scan_transport_left.assert_drained();
    scan_transport_right.assert_drained();
    assert!(
        BackendService::accept_complete_execution(TEST_SLOT_ID, begin.key.session_epoch)
            .expect("complete execution after sibling scan finishes"),
        "dropping a finished driver must not cancel the still-active sibling scan"
    );
    execution_guard.disarm();
}
