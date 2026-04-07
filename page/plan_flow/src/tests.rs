use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DFSchema;
use datafusion_expr::logical_plan::{Filter, LogicalPlan};
use datafusion_expr::{lit, Expr};
use issuance::{IssuanceConfig, IssuancePool, IssuedOwnedFrame, IssuedRx, IssuedTx};
use pool::{PagePool, PagePoolConfig};
use scan_node::{PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{compile_scan, CompileScanInput, LimitLowering, PgRelation};
use transfer::{PageRx, PageTx};

use crate::{
    BackendPlanRole, BackendPlanState, BackendPlanStep, FlowId, PlanOpen, WorkerPlanError,
    WorkerPlanRole, WorkerPlanState, WorkerStep,
};

const TEST_IDENTIFIER_MAX_BYTES: usize = 63;
const TEST_PAGE_KIND: transfer::MessageKind = 0x504c;
const TEST_PAGE_FLAGS: u16 = 0x0007;

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

fn plan_open(plan_id: u64) -> PlanOpen {
    PlanOpen::new(
        FlowId {
            session_epoch: 9,
            plan_id,
        },
        TEST_PAGE_KIND,
        TEST_PAGE_FLAGS,
    )
}

fn test_plan(scan_id: u64) -> LogicalPlan {
    let relation = PgRelation::new(Some("public"), "users");
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let source_schema = DFSchema::try_from_qualified_schema(
        datafusion_common::TableReference::partial("public", "users"),
        arrow_schema.as_ref(),
    )
    .expect("df schema");
    let filter = Expr::IsNotNull(Box::new(Expr::Column(
        datafusion_common::Column::from_name("name"),
    )));
    let compiled = compile_scan(CompileScanInput {
        relation: &relation,
        schema: arrow_schema.as_ref(),
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0, 1, 2]),
        filters: std::slice::from_ref(&filter),
        requested_limit: Some(25),
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan");
    let spec = Arc::new(
        PgScanSpec::try_new(
            PgScanId::new(scan_id),
            42,
            relation,
            &source_schema,
            compiled,
        )
        .expect("scan spec"),
    );
    let mut plan = PgScanNode::new(spec).into_logical_plan();
    for threshold in 1..=6_i64 {
        let (qualifier, field) = plan.schema().qualified_field(0);
        let expr =
            Expr::Column(datafusion_common::Column::from((qualifier, field))).gt(lit(threshold));
        plan = LogicalPlan::Filter(Filter::try_new(expr, Arc::new(plan)).expect("filter plan"));
    }
    plan
}

fn mark_and_frame(outbound: issuance::IssuedOutboundPage) -> IssuedOwnedFrame {
    let frame = outbound.frame();
    outbound.mark_sent();
    frame
}

#[test]
fn multi_page_happy_path_decodes_plan_and_closes_cleanly() {
    let open = plan_open(1);
    let plan = test_plan(11);
    let (_page_region, page_pool) = init_page_pool(128, 8);
    let (_issuance_region, issuance_pool) = init_issuance_pool(8);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut backend = BackendPlanRole::new(tx);
    let mut worker = WorkerPlanRole::new();

    backend.open(open, &plan).expect("backend open");
    worker.open(open).expect("worker open");

    let mut page_count = 0usize;
    let decoded = loop {
        match backend.step().expect("backend step") {
            BackendPlanStep::OutboundPage { flow, outbound } => {
                assert_eq!(flow, open.flow);
                let frame = mark_and_frame(outbound);
                page_count += 1;
                assert!(matches!(
                    worker.accept_frame(flow, &rx, &frame).expect("worker page"),
                    WorkerStep::Idle
                ));
            }
            BackendPlanStep::CloseFrame { flow, frame } => {
                assert_eq!(flow, open.flow);
                match worker
                    .accept_frame(flow, &rx, &frame)
                    .expect("worker close")
                {
                    WorkerStep::Plan {
                        flow: result_flow,
                        plan,
                    } => {
                        assert_eq!(result_flow, flow);
                        break plan;
                    }
                    other => panic!("unexpected worker close step: {other:?}"),
                }
            }
            other => panic!("unexpected backend step: {other:?}"),
        }
    };

    assert!(page_count > 1, "plan should span multiple pages");
    assert_eq!(
        decoded.display_indent().to_string(),
        plan.display_indent().to_string()
    );
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    backend.close().expect("backend close");
    worker.close().expect("worker close");
    assert_eq!(backend.state(), BackendPlanState::Exhausted);
    assert_eq!(worker.state(), WorkerPlanState::Closed);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn sender_blocks_while_previous_outbound_page_is_pending() {
    let open = plan_open(2);
    let plan = test_plan(12);
    let (_page_region, page_pool) = init_page_pool(128, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let mut backend = BackendPlanRole::new(tx);

    backend.open(open, &plan).expect("backend open");
    let outbound = match backend.step().expect("first page") {
        BackendPlanStep::OutboundPage { flow, outbound } => {
            assert_eq!(flow, open.flow);
            outbound
        }
        other => panic!("unexpected first step: {other:?}"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);
    assert!(matches!(
        backend.step().expect("blocked"),
        BackendPlanStep::Blocked { flow } if flow == open.flow
    ));
    drop(outbound);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
    backend.abort();
    assert_eq!(backend.state(), BackendPlanState::Closed);
}

#[test]
fn sender_blocks_until_worker_accepts_and_releases_detached_page() {
    let open = plan_open(3);
    let plan = test_plan(13);
    let (_page_region, page_pool) = init_page_pool(128, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut backend = BackendPlanRole::new(tx);
    let mut worker = WorkerPlanRole::new();

    backend.open(open, &plan).expect("backend open");
    worker.open(open).expect("worker open");

    let outbound = match backend.step().expect("first page") {
        BackendPlanStep::OutboundPage { outbound, .. } => outbound,
        other => panic!("unexpected first step: {other:?}"),
    };
    let frame = outbound.frame();
    outbound.mark_sent();
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    assert!(matches!(
        backend.step().expect("blocked before accept"),
        BackendPlanStep::Blocked { flow } if flow == open.flow
    ));

    match worker
        .accept_frame(open.flow, &rx, &frame)
        .expect("worker page")
    {
        WorkerStep::Idle => {}
        other => panic!("unexpected worker step: {other:?}"),
    }
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);

    assert!(!matches!(
        backend
            .step()
            .expect("backend makes progress after release"),
        BackendPlanStep::Blocked { .. }
    ));
}

#[test]
fn backend_abort_plus_unsent_drop_returns_page_and_permit() {
    let open = plan_open(4);
    let plan = test_plan(14);
    let (_page_region, page_pool) = init_page_pool(128, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let mut backend = BackendPlanRole::new(tx);

    backend.open(open, &plan).expect("backend open");
    let outbound = match backend.step().expect("first page") {
        BackendPlanStep::OutboundPage { outbound, .. } => outbound,
        other => panic!("unexpected step: {other:?}"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);
    backend.abort();
    drop(outbound);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
    assert_eq!(backend.state(), BackendPlanState::Closed);
}

#[test]
fn single_page_budget_still_completes_multi_page_plan() {
    let open = plan_open(5);
    let plan = test_plan(15);
    let (_page_region, page_pool) = init_page_pool(128, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut backend = BackendPlanRole::new(tx);
    let mut worker = WorkerPlanRole::new();

    backend.open(open, &plan).expect("backend open");
    worker.open(open).expect("worker open");

    let mut saw_multiple_pages = false;
    for _ in 0..64 {
        match backend.step().expect("backend step") {
            BackendPlanStep::OutboundPage { flow, outbound } => {
                let frame = mark_and_frame(outbound);
                assert!(matches!(
                    worker.accept_frame(flow, &rx, &frame).expect("worker page"),
                    WorkerStep::Idle
                ));
                assert_eq!(issuance_pool.snapshot().leased_permits, 0);
                saw_multiple_pages = true;
            }
            BackendPlanStep::Blocked { .. } => {
                panic!("single-page budget must still make progress")
            }
            BackendPlanStep::CloseFrame { flow, frame } => {
                match worker
                    .accept_frame(flow, &rx, &frame)
                    .expect("worker close")
                {
                    WorkerStep::Plan {
                        flow: result_flow, ..
                    } => {
                        assert_eq!(result_flow, flow);
                        break;
                    }
                    other => panic!("unexpected worker close step: {other:?}"),
                }
            }
            BackendPlanStep::LogicalError { message, .. } => panic!("unexpected error: {message}"),
        }
    }

    assert!(saw_multiple_pages, "expected plan to span multiple pages");
    backend.close().expect("backend close");
    worker.close().expect("worker close");
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
    assert_eq!(backend.state(), BackendPlanState::Exhausted);
    assert_eq!(worker.state(), WorkerPlanState::Closed);
}

#[test]
fn close_frame_after_truncated_payload_fails_decode() {
    let open = plan_open(6);
    let plan = test_plan(16);
    let (_page_region, page_pool) = init_page_pool(128, 8);
    let (_issuance_region, issuance_pool) = init_issuance_pool(8);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut worker = WorkerPlanRole::new();
    let mut encoder = plan_codec::PlanEncodeSession::new(&plan).expect("encoder");

    worker.open(open).expect("worker open");

    let mut writer = tx.begin(TEST_PAGE_KIND, TEST_PAGE_FLAGS).expect("writer");
    let written = match encoder.write_chunk(writer.payload_mut()).expect("encode") {
        plan_codec::EncodeProgress::NeedMoreOutput { written }
        | plan_codec::EncodeProgress::Done { written } => written,
    };
    let outbound = writer.finish_with_payload_len(written).expect("outbound");
    let frame = outbound.frame();
    outbound.mark_sent();

    assert!(matches!(
        worker
            .accept_frame(open.flow, &rx, &frame)
            .expect("worker page"),
        WorkerStep::Idle
    ));

    let close = tx.close().expect("close");
    match worker
        .accept_frame(open.flow, &rx, &close)
        .expect("worker close")
    {
        WorkerStep::LogicalError { flow, message } => {
            assert_eq!(flow, open.flow);
            assert!(message.contains("UnexpectedEof") || message.contains("unexpected EOF"));
        }
        other => panic!("unexpected worker step: {other:?}"),
    }
    assert_eq!(worker.state(), WorkerPlanState::Failed);
}

#[test]
fn wrong_page_envelope_fails_logical_flow_and_releases_page() {
    let open = plan_open(7);
    let (_page_region, page_pool) = init_page_pool(128, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut worker = WorkerPlanRole::new();

    worker.open(open).expect("worker open");

    let mut writer = tx
        .begin(TEST_PAGE_KIND + 1, TEST_PAGE_FLAGS)
        .expect("writer");
    writer.payload_mut()[..4].copy_from_slice(b"oops");
    let outbound = writer.finish_with_payload_len(4).expect("outbound");
    let frame = outbound.frame();
    outbound.mark_sent();

    match worker
        .accept_frame(open.flow, &rx, &frame)
        .expect("worker wrong page")
    {
        WorkerStep::LogicalError { flow, message } => {
            assert_eq!(flow, open.flow);
            assert!(message.contains("unexpected page envelope"));
        }
        other => panic!("unexpected worker step: {other:?}"),
    }
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
    assert_eq!(worker.state(), WorkerPlanState::Failed);
}

#[test]
fn stale_page_from_previous_flow_is_drained_before_unexpected_flow() {
    let open = plan_open(8);
    let stale = plan_open(9);
    let (_page_region, page_pool) = init_page_pool(128, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut worker = WorkerPlanRole::new();

    worker.open(open).expect("worker open");

    let mut writer = tx.begin(TEST_PAGE_KIND, TEST_PAGE_FLAGS).expect("writer");
    writer.payload_mut()[..4].copy_from_slice(b"late");
    let outbound = writer.finish_with_payload_len(4).expect("outbound");
    let frame = outbound.frame();
    outbound.mark_sent();

    let err = worker
        .accept_frame(stale.flow, &rx, &frame)
        .expect_err("unexpected flow must fail");
    assert!(matches!(
        err,
        WorkerPlanError::UnexpectedFlow {
            expected,
            actual
        } if expected == open.flow && actual == stale.flow
    ));
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn explicit_sender_error_reaches_worker_failure_terminal() {
    let open = plan_open(10);
    let mut worker = WorkerPlanRole::new();
    worker.open(open).expect("worker open");

    match worker
        .accept_sender_error(open.flow, "backend aborted".into())
        .expect("sender error")
    {
        WorkerStep::LogicalError { flow, message } => {
            assert_eq!(flow, open.flow);
            assert_eq!(message, "backend aborted");
        }
        other => panic!("unexpected worker step: {other:?}"),
    }
    assert_eq!(worker.state(), WorkerPlanState::Failed);
}

#[test]
fn backend_role_cannot_reopen_after_successful_close() {
    let open = plan_open(11);
    let plan = test_plan(21);
    let (_page_region, page_pool) = init_page_pool(128, 8);
    let (_issuance_region, issuance_pool) = init_issuance_pool(8);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
    let mut backend = BackendPlanRole::new(tx);
    let mut worker = WorkerPlanRole::new();

    backend.open(open, &plan).expect("backend open");
    worker.open(open).expect("worker open");

    loop {
        match backend.step().expect("backend step") {
            BackendPlanStep::OutboundPage { flow, outbound } => {
                let frame = mark_and_frame(outbound);
                assert!(matches!(
                    worker.accept_frame(flow, &rx, &frame).expect("worker page"),
                    WorkerStep::Idle
                ));
            }
            BackendPlanStep::CloseFrame { flow, frame } => {
                assert!(matches!(
                    worker.accept_frame(flow, &rx, &frame).expect("worker close"),
                    WorkerStep::Plan { flow: result_flow, .. } if result_flow == flow
                ));
                break;
            }
            other => panic!("unexpected backend step: {other:?}"),
        }
    }

    backend.close().expect("backend close");
    worker.close().expect("worker close");
    assert_eq!(backend.state(), BackendPlanState::Exhausted);

    let err = backend
        .open(plan_open(12), &plan)
        .expect_err("reopen must fail");
    assert!(matches!(
        err,
        crate::BackendPlanError::InvalidState {
            action: "open",
            state: BackendPlanState::Exhausted,
        }
    ));
}
