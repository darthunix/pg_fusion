use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{Field, Schema, SchemaRef};
use backend_service::{
    build_standalone_scan_descriptor, ActiveScanDriver, BackendService, BackendServiceError,
    BeginExecutionOutput, CtidBlockRange, DiagnosticLogLevel, ExecutionKey, ExplainInput,
    ExplainRenderOptions, OpenScanInput, ScanStreamStep, ScanWorkerLaunchInput,
    ScanWorkerLaunchOutput, ScanWorkerLauncher, ScanWorkerProducer, ScanWorkerQueryInput,
    StartExecutionInput,
};
use control_transport::{
    BackendLeaseSlot, BackendSlotLease, BackendTxError, TxError, WorkerTransport,
};
use issuance::{
    decode_issued_frame, encode_issued_frame, IssuancePool, IssuedOwnedFrame, IssuedTx,
};
use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::pg_sys::{
    self, CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, ExecutorEnd_hook_type,
    List, MyLatch, Node, QueryDesc, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
};
use pgrx::prelude::*;
use pgrx::{check_for_interrupts, pg_guard, PgRelation as PgrxRelation, PgTryBuilder};
use pool::PagePool;
use runtime_metrics::{MetricId, PageDirection, RuntimeMetrics};
use runtime_protocol::{
    decode_runtime_message_family, decode_worker_execution_to_backend,
    decode_worker_scan_to_backend, encode_backend_execution_to_worker_into,
    encode_backend_scan_to_worker_into, encode_worker_scan_to_backend_into,
    encoded_len_backend_execution_to_worker, encoded_len_backend_scan_to_worker,
    encoded_len_worker_scan_to_backend, BackendExecutionToWorker, BackendScanToWorker,
    RuntimeMessageFamily, WorkerExecutionToBackend, WorkerScanToBackend, WorkerScanToBackendRef,
};
use transfer::PageTx;
use worker_runtime::normalize_result_transport_schema;

use crate::diag;
use crate::guc::host_config;
use crate::logging;
use crate::result_ingress::{AcceptedResultFrame, ResultIngress};
use crate::scan_worker_job::{
    encode_scan_worker_descriptor, ScanWorkerJobError, ScanWorkerJobRegistryHandle,
    ScanWorkerJobSpec,
};
use crate::shmem::{
    attach_control_region, attach_issuance_pool, attach_page_pool, attach_runtime_filters,
    attach_runtime_metrics, attach_scan_region, attach_scan_worker_jobs,
};
use crate::utility_hook::PlannerBypassGuard;

thread_local! {
    static SCAN_METHODS: CustomScanMethods = CustomScanMethods {
        CustomName: c"PgFusionScan".as_ptr(),
        CreateCustomScanState: Some(create_pg_fusion_scan_state),
    };
    static EXEC_METHODS: CustomExecMethods = CustomExecMethods {
        CustomName: c"PgFusionScan".as_ptr(),
        BeginCustomScan: Some(begin_pg_fusion_scan),
        ExecCustomScan: Some(exec_pg_fusion_scan),
        EndCustomScan: Some(end_pg_fusion_scan),
        ReScanCustomScan: None,
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(explain_pg_fusion_scan),
    };
}

static mut PREV_EXECUTOR_END_HOOK: ExecutorEnd_hook_type = None;

#[repr(C)]
struct PgFusionScanState {
    css: CustomScanState,
    state: *mut HostScanState,
}

struct HostScanState {
    sql: String,
    control_lease: Option<BackendSlotLease>,
    execution_key: Option<ExecutionKey>,
    scan_peers: BTreeMap<u64, BackendLeaseSlot>,
    active_drivers: BTreeMap<u64, ActiveScanDriver>,
    pending_complete_session_epoch: Option<u64>,
    page_pool: Option<PagePool>,
    issuance_pool: Option<IssuancePool>,
    result_ingress: Option<ResultIngress>,
    primary_scratch: Vec<u8>,
    scan_scratch: Vec<u8>,
    terminal_error: Option<String>,
    owns_result_slot: bool,
    metrics: RuntimeMetrics,
    query_start_ns: u64,
    query_total_recorded: bool,
}

enum PrimaryInbound {
    Control(WorkerExecutionToBackend),
    Issued(IssuedOwnedFrame),
}

pub(crate) fn register_methods() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(scan_methods());
        PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;
        pg_sys::ExecutorEnd_hook = Some(pg_fusion_executor_end_hook);
    }
}

pub(crate) fn scan_methods() -> *const CustomScanMethods {
    SCAN_METHODS.with(|methods| methods as *const CustomScanMethods)
}

fn exec_methods() -> *const CustomExecMethods {
    EXEC_METHODS.with(|methods| methods as *const CustomExecMethods)
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_fusion_executor_end_hook(query_desc: *mut QueryDesc) {
    let estate = if query_desc.is_null() {
        std::ptr::null_mut()
    } else {
        (*query_desc).estate
    };
    let planstate = if query_desc.is_null() {
        std::ptr::null_mut()
    } else {
        (*query_desc).planstate
    };
    let custom_scan =
        if !planstate.is_null() && (*planstate).type_ == pg_sys::NodeTag::T_CustomScanState {
            planstate.cast::<CustomScanState>()
        } else {
            std::ptr::null_mut()
        };

    diag::update_executor_watch(query_desc, estate, custom_scan);
    diag::backend_diag(|| {
        format!(
            "pg_fusion ExecutorEnd hook entry query_desc={:p} estate={:p} planstate={:p} custom_scan={:p}",
            query_desc, estate, planstate, custom_scan
        )
    });
    if !custom_scan.is_null() {
        diag::log_live_watch("pg_fusion ExecutorEnd hook entry live watch");
    }

    if let Some(prev) = PREV_EXECUTOR_END_HOOK {
        prev(query_desc);
    } else {
        pg_sys::standard_ExecutorEnd(query_desc);
    }

    diag::backend_diag(|| {
        format!(
            "pg_fusion ExecutorEnd hook exit query_desc={:p} estate={:p} custom_scan={:p} {}",
            query_desc,
            estate,
            custom_scan,
            diag::watch_snapshot()
        )
    });
    diag::clear_watch();
}

#[pg_guard]
unsafe extern "C-unwind" fn create_pg_fusion_scan_state(cscan: *mut CustomScan) -> *mut Node {
    let sql = sql_from_custom_private((*cscan).custom_private);
    let host_state = Box::new(HostScanState {
        sql,
        control_lease: None,
        execution_key: None,
        scan_peers: BTreeMap::new(),
        active_drivers: BTreeMap::new(),
        pending_complete_session_epoch: None,
        page_pool: None,
        issuance_pool: None,
        result_ingress: None,
        primary_scratch: Vec::new(),
        scan_scratch: Vec::new(),
        terminal_error: None,
        owns_result_slot: false,
        metrics: RuntimeMetrics::default(),
        query_start_ns: 0,
        query_total_recorded: false,
    });

    let state_ptr =
        pg_sys::palloc0(std::mem::size_of::<PgFusionScanState>()) as *mut PgFusionScanState;
    let mut state = PgFusionScanState {
        css: CustomScanState {
            methods: exec_methods(),
            ..Default::default()
        },
        state: Box::into_raw(host_state),
    };
    state.css.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    std::ptr::write(state_ptr, state);
    state_ptr.cast()
}

unsafe fn with_query_context<T>(estate: *mut pg_sys::EState, f: impl FnOnce() -> T) -> T {
    if estate.is_null() || (*estate).es_query_cxt.is_null() {
        error!("pg_fusion expected non-null estate->es_query_cxt for slot allocation");
    }
    let previous = pg_sys::MemoryContextSwitchTo((*estate).es_query_cxt);
    let result = f();
    pg_sys::MemoryContextSwitchTo(previous);
    result
}

unsafe fn ensure_slot_query_context(
    slot: *mut pg_sys::TupleTableSlot,
    estate: *mut pg_sys::EState,
    slot_name: &str,
) {
    if slot.is_null() {
        error!("pg_fusion expected non-null {slot_name}");
    }
    let query_cxt = (*estate).es_query_cxt;
    if (*slot).tts_mcxt != query_cxt {
        error!(
            "pg_fusion {slot_name} was allocated in wrong context: slot_mcxt={:p} es_query_cxt={:p}",
            (*slot).tts_mcxt,
            query_cxt
        );
    }
}

unsafe fn validate_core_scan_slot(node: *mut CustomScanState, estate: *mut pg_sys::EState) {
    let scan_slot = (*node).ss.ss_ScanTupleSlot;
    ensure_slot_query_context(scan_slot, estate, "ss_ScanTupleSlot");
    if (*scan_slot).tts_ops != &raw const pg_sys::TTSOpsVirtual {
        error!(
            "pg_fusion expected core ss_ScanTupleSlot to use TTSOpsVirtual: slot={}",
            tuple_slot_snapshot(scan_slot)
        );
    }
}

unsafe fn validate_core_slots(
    node: *mut CustomScanState,
    estate: *mut pg_sys::EState,
    state: &mut HostScanState,
) {
    validate_core_scan_slot(node, estate);
    drop_owned_result_slot(node, state);
    ensure_slot_query_context(
        (*node).ss.ps.ps_ResultTupleSlot,
        estate,
        "ps_ResultTupleSlot",
    );
}

unsafe fn drop_owned_result_slot(node: *mut CustomScanState, state: &mut HostScanState) {
    if !state.owns_result_slot {
        return;
    }
    let result_slot = (*node).ss.ps.ps_ResultTupleSlot;
    if !result_slot.is_null() {
        pg_sys::ExecDropSingleTupleTableSlot(result_slot);
        (*node).ss.ps.ps_ResultTupleSlot = std::ptr::null_mut();
    }
    state.owns_result_slot = false;
}

unsafe fn refresh_debug_watch(node: *mut CustomScanState, state: &HostScanState) {
    if !logging::backend_log_enabled(DiagnosticLogLevel::Trace) {
        return;
    }
    let estate = (*node).ss.ps.state;
    diag::update_executor_watch(std::ptr::null_mut(), estate, node);
    diag::update_slot_watch(
        (*node).ss.ss_ScanTupleSlot,
        (*node).ss.ps.ps_ResultTupleSlot,
    );
    if let Some(ingress) = state.result_ingress.as_ref() {
        let (per_tuple_cxt, queue_cxt) = ingress.debug_contexts();
        diag::update_result_ingress_watch(
            ingress.debug_project_slot(),
            ingress.debug_front_queued_tuple(),
            per_tuple_cxt,
            queue_cxt,
        );
    } else {
        diag::update_result_ingress_watch(
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }
}

unsafe fn register_debug_context_callbacks(estate: *mut pg_sys::EState, state: &HostScanState) {
    if estate.is_null() {
        return;
    }
    diag::register_context_callback("es_query_cxt", (*estate).es_query_cxt);
    if let Some(ingress) = state.result_ingress.as_ref() {
        let (per_tuple_cxt, queue_cxt) = ingress.debug_contexts();
        diag::register_context_callback("result_ingress_per_tuple", per_tuple_cxt);
        diag::register_context_callback("result_ingress_queue", queue_cxt);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn begin_pg_fusion_scan(
    node: *mut CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: i32,
) {
    let state = host_state_mut(node);
    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        validate_core_slots(node, estate, state);
        state.control_lease = None;
        state.execution_key = None;
        state.scan_peers.clear();
        state.active_drivers.clear();
        state.pending_complete_session_epoch = None;
        state.result_ingress = None;
        state.terminal_error = None;
        refresh_debug_watch(node, state);
        register_debug_context_callbacks(estate, state);
        diag::log_live_watch("pg_fusion explain-only begin live watch");
        return;
    }

    let config = host_config().unwrap_or_else(|err| error!("pg_fusion config error: {err}"));
    let metrics = attach_runtime_metrics();
    state.metrics = metrics;
    state.query_start_ns = metrics.now_ns();
    state.query_total_recorded = false;
    let backend_start = metrics.now_ns();
    let mut backend_config = config.backend_service_config();
    backend_config.metrics = metrics;
    backend_config.runtime_filters = attach_runtime_filters();
    let control_region = attach_control_region();
    let scan_region = attach_scan_region();
    let scan_worker_jobs = attach_scan_worker_jobs();
    let page_pool = attach_page_pool();
    let issuance_pool = attach_issuance_pool();
    let transport_schema = build_transport_schema(&state.sql)
        .unwrap_or_else(|err| error!("pg_fusion schema preparation failed: {err}"));
    let control_lease = BackendSlotLease::acquire(&control_region)
        .unwrap_or_else(|err| error!("pg_fusion failed to acquire primary control slot: {err}"));
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion acquired primary control lease {} state={}",
            control_lease_snapshot(&control_lease),
            host_state_snapshot(state)
        )
    });

    let plan_tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let mut scan_worker_launcher = DynamicScanWorkerLauncher {
        jobs: scan_worker_jobs,
        budgets: BTreeMap::new(),
        capacity_exhausted: false,
    };
    let begin = {
        let _planner_bypass = PlannerBypassGuard::enter();
        BackendService::begin_execution(StartExecutionInput {
            slot_id: control_lease.slot_id(),
            sql: &state.sql,
            params: Vec::new(),
            plan_tx,
            scan_slot_region: &scan_region,
            config: backend_config,
            scan_worker_launcher: Some(&mut scan_worker_launcher),
        })
    }
    .unwrap_or_else(|err| error!("pg_fusion begin execution failed: {err}"));
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion begin_execution returned key={:?} scan_channel_count={} primary_peer={} state={}",
            begin.key,
            begin.scan_channels.len(),
            control_lease_snapshot(&control_lease),
            host_state_snapshot(state)
        )
    });

    let mut control_lease = control_lease;
    send_backend_execution(&mut control_lease, begin.control(), &mut Vec::new()).unwrap_or_else(
        |err| {
            let _ = BackendService::abort_execution_start();
            error!("pg_fusion failed to send StartExecution: {err}");
        },
    );
    publish_plan_to_worker(&mut control_lease).unwrap_or_else(|err| {
        let _ = BackendService::abort_execution_start();
        error!("pg_fusion failed to publish logical plan: {err}");
    });
    let key = BackendService::finalize_execution_start()
        .unwrap_or_else(|err| error!("pg_fusion finalize execution failed: {err}"));
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion finalized execution start slot_id={} session_epoch={} primary_peer={} state={}",
            key.slot_id,
            key.session_epoch,
            control_lease_snapshot(&control_lease),
            host_state_snapshot(state)
        )
    });
    let tuple_desc = tuple_desc_for_slots(node);

    validate_core_slots(node, estate, state);

    state.result_ingress = Some(
        with_query_context(estate, || {
            ResultIngress::new(transport_schema, tuple_desc, page_pool, issuance_pool)
        })
        .unwrap_or_else(|err| error!("pg_fusion result ingress init failed: {err}")),
    );
    state.control_lease = Some(control_lease);
    state.execution_key = Some(key);
    state.page_pool = Some(page_pool);
    state.issuance_pool = Some(issuance_pool);
    state.scan_peers = scan_peers_from_begin(&begin);
    state.pending_complete_session_epoch = None;
    state.primary_scratch = vec![
        0_u8;
        config
            .control_backend_to_worker_capacity
            .max(config.control_worker_to_backend_capacity)
    ];
    state.scan_scratch = vec![
        0_u8;
        config
            .scan_backend_to_worker_capacity
            .max(config.scan_worker_to_backend_capacity)
    ];
    state.active_drivers.clear();
    state.terminal_error = None;
    refresh_debug_watch(node, state);
    register_debug_context_callbacks(estate, state);
    diag::log_live_watch("pg_fusion begin scan live watch");
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion begin scan installed execution slot_id={} session_epoch={} scan_peers={:?} state={}",
            key.slot_id,
            key.session_epoch,
            scan_peer_keys(state),
            host_state_snapshot(state)
        )
    });
    metrics.add_elapsed(MetricId::BackendTotalNs, backend_start);
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_pg_fusion_scan(
    node: *mut CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let state = host_state_mut(node);
    state.metrics.increment(MetricId::BackendExecCallsTotal);
    let backend_start = state.metrics.now_ns();
    let scan_slot = (*node).ss.ss_ScanTupleSlot;
    refresh_debug_watch(node, state);
    diag::log_live_watch("pg_fusion exec entry live watch");
    host_diag(DiagnosticLogLevel::Trace, || {
        format!(
            "pg_fusion exec entry slots scan_slot={} result_slot={} state={}",
            tuple_slot_snapshot((*node).ss.ss_ScanTupleSlot),
            tuple_slot_snapshot((*node).ss.ps.ps_ResultTupleSlot),
            host_state_snapshot(state)
        )
    });
    if scan_slot.is_null() {
        error!("pg_fusion expected non-null core ss_ScanTupleSlot in ExecCustomScan");
    }
    if (*scan_slot).tts_ops != &raw const pg_sys::TTSOpsVirtual {
        error!(
            "pg_fusion expected core ss_ScanTupleSlot to use TTSOpsVirtual in ExecCustomScan: slot={}",
            tuple_slot_snapshot(scan_slot)
        );
    }

    loop {
        if let Some(err) = state.terminal_error.take() {
            error!("pg_fusion execution failed: {err}");
        }

        if let Some(result) = state
            .result_ingress
            .as_mut()
            .map(|ingress| ingress.store_next_into(scan_slot))
            .transpose()
            .unwrap_or_else(|err| {
                error!("pg_fusion result ingress projection failed: {err}");
            })
            .flatten()
        {
            refresh_debug_watch(node, state);
            diag::log_live_watch("pg_fusion exec returning row live watch");
            host_diag(DiagnosticLogLevel::Trace, || {
                format!(
                    "pg_fusion exec returning row from scan_slot={} result_slot={} state={}",
                    tuple_slot_snapshot((*node).ss.ss_ScanTupleSlot),
                    tuple_slot_snapshot((*node).ss.ps.ps_ResultTupleSlot),
                    host_state_snapshot(state)
                )
            });
            record_backend_row_return(state, backend_start);
            return result;
        }

        let mut progressed = false;
        progressed |= poll_primary_peer(state).unwrap_or_else(|err| {
            error!("pg_fusion primary peer poll failed: {err}");
        });
        if let Some(result) = state
            .result_ingress
            .as_mut()
            .map(|ingress| ingress.store_next_into(scan_slot))
            .transpose()
            .unwrap_or_else(|err| {
                error!("pg_fusion result ingress projection failed: {err}");
            })
            .flatten()
        {
            refresh_debug_watch(node, state);
            diag::log_live_watch("pg_fusion exec returning row after primary poll live watch");
            host_diag(DiagnosticLogLevel::Trace, || {
                format!(
                    "pg_fusion exec returning row after primary poll scan_slot={} result_slot={} state={}",
                    tuple_slot_snapshot((*node).ss.ss_ScanTupleSlot),
                    tuple_slot_snapshot((*node).ss.ps.ps_ResultTupleSlot),
                    host_state_snapshot(state)
                )
            });
            record_backend_row_return(state, backend_start);
            return result;
        }

        progressed |= poll_scan_peers(state).unwrap_or_else(|err| {
            error!("pg_fusion scan peer poll failed: {err}");
        });
        progressed |= drive_active_scans(
            state,
            (*node).ss.ss_ScanTupleSlot,
            (*node).ss.ps.ps_ResultTupleSlot,
        )
        .unwrap_or_else(|err| {
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion scan driver failure snapshot before raising: {}",
                    host_state_snapshot(state)
                )
            });
            error!("pg_fusion scan driver failed: {err}");
        });

        if let Some(result) = state
            .result_ingress
            .as_mut()
            .map(|ingress| ingress.store_next_into(scan_slot))
            .transpose()
            .unwrap_or_else(|err| {
                error!("pg_fusion result ingress projection failed: {err}");
            })
            .flatten()
        {
            refresh_debug_watch(node, state);
            diag::log_live_watch("pg_fusion exec returning row after scan drive live watch");
            host_diag(DiagnosticLogLevel::Trace, || {
                format!(
                    "pg_fusion exec returning row after scan drive scan_slot={} result_slot={} state={}",
                    tuple_slot_snapshot((*node).ss.ss_ScanTupleSlot),
                    tuple_slot_snapshot((*node).ss.ps.ps_ResultTupleSlot),
                    host_state_snapshot(state)
                )
            });
            record_backend_row_return(state, backend_start);
            return result;
        }

        if state
            .result_ingress
            .as_ref()
            .is_some_and(ResultIngress::is_complete)
        {
            refresh_debug_watch(node, state);
            diag::log_live_watch("pg_fusion returning EOF live watch");
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion exec returning EOF scan_slot={} result_slot={} state={}",
                    tuple_slot_snapshot((*node).ss.ss_ScanTupleSlot),
                    tuple_slot_snapshot((*node).ss.ps.ps_ResultTupleSlot),
                    host_state_snapshot(state)
                )
            });
            record_backend_eof(state, backend_start);
            return std::ptr::null_mut();
        }

        if !progressed {
            let wait_start = state.metrics.now_ns();
            wait_latch(Some(Duration::from_millis(1)));
            state
                .metrics
                .add_elapsed(MetricId::BackendWaitLatchNs, wait_start);
            state.metrics.increment(MetricId::BackendWaitLatchTotal);
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn end_pg_fusion_scan(node: *mut CustomScanState) {
    let state = host_state_mut(node);
    refresh_debug_watch(node, state);
    diag::log_live_watch("pg_fusion EndCustomScan entry");
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion ending custom scan with state {}",
            host_state_snapshot(state)
        )
    });
    if let Some(key) = state.execution_key.take() {
        let _ = BackendService::accept_cancel_execution(key.slot_id, key.session_epoch);
    }
    record_query_total(state);
    state.active_drivers.clear();
    state.pending_complete_session_epoch = None;
    let scan_slot = (*node).ss.ss_ScanTupleSlot;
    if !scan_slot.is_null() {
        pg_sys::ExecClearTuple(scan_slot);
    }
    state.result_ingress.take();
    state.control_lease.take();
    drop_owned_result_slot(node, state);

    let host_state = std::mem::replace(&mut state_from_node(node).state, std::ptr::null_mut());
    if !host_state.is_null() {
        drop(Box::from_raw(host_state));
    }
    diag::clear_watch();
    host_diag(DiagnosticLogLevel::Basic, || {
        "pg_fusion finished custom scan cleanup".to_string()
    });
}

#[pg_guard]
unsafe extern "C-unwind" fn explain_pg_fusion_scan(
    node: *mut CustomScanState,
    _ancestors: *mut List,
    es: *mut pg_sys::ExplainState,
) {
    let state = host_state_ref(node);
    let config = host_config().unwrap_or_else(|err| error!("pg_fusion config error: {err}"));
    let rendered = {
        let _planner_bypass = PlannerBypassGuard::enter();
        BackendService::render_explain(ExplainInput {
            sql: &state.sql,
            params: Vec::new(),
            options: explain_render_options(es),
            config: config.backend_service_config(),
        })
    }
    .unwrap_or_else(|err| error!("pg_fusion explain failed: {err}"));
    emit_pg_fusion_explain(rendered, es);
}

unsafe fn explain_render_options(es: *mut pg_sys::ExplainState) -> ExplainRenderOptions {
    if es.is_null() {
        ExplainRenderOptions::default()
    } else {
        ExplainRenderOptions {
            verbose: (*es).verbose,
            costs: (*es).costs,
        }
    }
}

unsafe fn emit_pg_fusion_explain(rendered: String, es: *mut pg_sys::ExplainState) {
    if !es.is_null() && (*es).format == pg_sys::ExplainFormat::EXPLAIN_FORMAT_TEXT {
        emit_text_explain_lines(&rendered, es);
    } else {
        let rendered = CString::new(rendered).expect("explain text must not contain NUL bytes");
        pg_sys::ExplainPropertyText(c"pg_fusion".as_ptr(), rendered.as_ptr(), es);
    }
}

unsafe fn emit_text_explain_lines(rendered: &str, es: *mut pg_sys::ExplainState) {
    if es.is_null() || (*es).str_.is_null() {
        return;
    }

    for line in rendered.lines() {
        pg_sys::appendStringInfoSpaces((*es).str_, (*es).indent * 2);
        let line = CString::new(line).expect("explain text must not contain NUL bytes");
        pg_sys::appendStringInfoString((*es).str_, line.as_ptr());
        pg_sys::appendStringInfoChar((*es).str_, b'\n' as std::ffi::c_char);
    }
}

fn poll_primary_peer(state: &mut HostScanState) -> Result<bool, BackendServiceError> {
    if state.control_lease.is_none() {
        return Ok(false);
    }
    let mut progressed = false;
    loop {
        let len = {
            let lease = state.control_lease.as_mut().expect("checked above");
            let mut rx = lease.from_worker_rx();
            rx.recv_frame_into(&mut state.primary_scratch)?
        };
        let Some(len) = len else {
            break;
        };
        progressed = true;
        match decode_primary_inbound(&state.primary_scratch[..len])
            .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?
        {
            PrimaryInbound::Control(message) => {
                handle_primary_control(state, message)?;
            }
            PrimaryInbound::Issued(frame) => {
                if let Some(descriptor) = issued_page_descriptor(&frame) {
                    if let Some(observation) = state
                        .metrics
                        .observe_page(PageDirection::WorkerToBackend, descriptor)
                    {
                        state
                            .metrics
                            .add(MetricId::ResultW2bWaitNs, observation.wait_ns);
                        state.metrics.increment(MetricId::ResultW2bWaitTotal);
                    }
                }
                let ingress = state.result_ingress.as_mut().ok_or_else(|| {
                    BackendServiceError::ProtocolViolation(
                        "result ingress is not initialized".into(),
                    )
                })?;
                let read_start = state.metrics.now_ns();
                let accepted = ingress
                    .accept_frame(&frame)
                    .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
                if accepted == AcceptedResultFrame::Page {
                    state
                        .metrics
                        .add_elapsed(MetricId::ResultPageReadNs, read_start);
                    state.metrics.increment(MetricId::ResultPagesReadTotal);
                    break;
                }
            }
        }
    }
    Ok(progressed)
}

fn issued_page_descriptor(frame: &IssuedOwnedFrame) -> Option<pool::PageDescriptor> {
    match frame {
        IssuedOwnedFrame::Page(frame) => Some(frame.inner.descriptor),
        IssuedOwnedFrame::Close(_) => None,
    }
}

fn handle_primary_control(
    state: &mut HostScanState,
    message: WorkerExecutionToBackend,
) -> Result<(), BackendServiceError> {
    let slot_id = state
        .control_lease
        .as_ref()
        .map(|lease| lease.slot_id())
        .ok_or(BackendServiceError::NoActiveExecution)?;
    match message {
        WorkerExecutionToBackend::CompleteExecution { session_epoch } => {
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion backend received CompleteExecution session_epoch={} state={}",
                    session_epoch,
                    host_state_snapshot(state)
                )
            });
            if let Some(ingress) = state.result_ingress.as_mut() {
                ingress.mark_execution_complete();
            }
            state.pending_complete_session_epoch = Some(session_epoch);
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion backend stored pending CompleteExecution session_epoch={} state_after={}",
                    session_epoch,
                    host_state_snapshot(state)
                )
            });
        }
        WorkerExecutionToBackend::FailExecution {
            session_epoch,
            code,
            detail,
        } => {
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion backend received FailExecution session_epoch={} code={:?} detail={:?} state={}",
                    session_epoch,
                    code,
                    detail,
                    host_state_snapshot(state)
                )
            });
            let _ = BackendService::accept_fail_execution(slot_id, session_epoch, code, detail)?;
            state.execution_key = None;
            state.active_drivers.clear();
            state.pending_complete_session_epoch = None;
            state.terminal_error = Some(format!(
                "worker failed execution session_epoch={session_epoch} code={code:?} detail={detail:?}"
            ));
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion backend applied FailExecution session_epoch={} state_after={}",
                    session_epoch,
                    host_state_snapshot(state)
                )
            });
        }
    }
    Ok(())
}

fn poll_scan_peers(state: &mut HostScanState) -> Result<bool, BackendServiceError> {
    let peers = match BackendService::scan_peers() {
        Ok(peers) => peers,
        Err(BackendServiceError::NoActiveExecution) => return Ok(false),
        Err(err) => return Err(err),
    };
    let mut progressed = false;
    for peer in peers.iter().copied() {
        while let Some(len) = BackendService::recv_scan_peer_frame(peer, &mut state.scan_scratch)? {
            progressed = true;
            match decode_worker_scan_to_backend(&state.scan_scratch[..len]).map_err(|err| {
                BackendServiceError::ProtocolViolation(format!(
                    "failed to decode scan control on {peer:?}: {err}"
                ))
            })? {
                WorkerScanToBackendRef::OpenScan {
                    session_epoch,
                    scan_id,
                    scan,
                } => {
                    host_diag(DiagnosticLogLevel::Basic, || {
                        format!(
                            "pg_fusion backend received OpenScan session_epoch={} scan_id={} peer={} active_drivers={:?} state={}",
                            session_epoch,
                            scan_id,
                            peer_snapshot(peer),
                            active_driver_keys(state),
                            host_state_snapshot(state)
                        )
                    });
                    let page_pool = state.page_pool.expect("page pool");
                    let issuance_pool = state.issuance_pool.expect("issuance pool");
                    let opened = {
                        let _planner_bypass = PlannerBypassGuard::enter();
                        BackendService::open_scan(OpenScanInput {
                            peer,
                            session_epoch,
                            scan_id,
                            scan,
                            scan_tx: IssuedTx::new(PageTx::new(page_pool), issuance_pool),
                        })
                    }?;
                    if let Some(driver) = opened {
                        state.active_drivers.insert(scan_id, driver);
                        host_diag(DiagnosticLogLevel::Basic, || {
                            format!(
                                "pg_fusion backend installed active scan driver scan_id={} peer={} active_drivers={:?} state={}",
                                scan_id,
                                peer_snapshot(peer),
                                active_driver_keys(state),
                                host_state_snapshot(state)
                            )
                        });
                    } else {
                        host_diag(DiagnosticLogLevel::Basic, || {
                            format!(
                                "pg_fusion backend ignored OpenScan session_epoch={} scan_id={} peer={} state={}",
                                session_epoch,
                                scan_id,
                                peer_snapshot(peer),
                                host_state_snapshot(state)
                            )
                        });
                    }
                }
                WorkerScanToBackendRef::CancelScan {
                    session_epoch: _,
                    scan_id,
                } => {
                    host_diag(DiagnosticLogLevel::Basic, || {
                        format!(
                            "pg_fusion backend received CancelScan scan_id={} active_drivers_before={:?} state={}",
                            scan_id,
                            active_driver_keys(state),
                            host_state_snapshot(state)
                        )
                    });
                    if let Some(mut driver) = state.active_drivers.remove(&scan_id) {
                        let _ = driver.cancel_scan()?;
                        host_diag(DiagnosticLogLevel::Basic, || {
                            format!(
                                "pg_fusion backend cancelled scan driver scan_id={} active_drivers_after={:?} state={}",
                                scan_id,
                                active_driver_keys(state),
                                host_state_snapshot(state)
                            )
                        });
                    } else {
                        host_diag(DiagnosticLogLevel::Basic, || {
                            format!(
                                "pg_fusion backend ignored CancelScan for missing driver scan_id={} state={}",
                                scan_id,
                                host_state_snapshot(state)
                            )
                        });
                    }
                }
            }
        }
    }
    Ok(progressed)
}

fn drive_active_scans(
    state: &mut HostScanState,
    scan_slot: *mut pg_sys::TupleTableSlot,
    result_slot: *mut pg_sys::TupleTableSlot,
) -> Result<bool, BackendServiceError> {
    let scan_ids = state.active_drivers.keys().copied().collect::<Vec<_>>();
    let mut progressed = false;
    for scan_id in scan_ids {
        host_diag(DiagnosticLogLevel::Trace, || {
            format!(
                "pg_fusion preparing to detach active scan driver scan_id={} state_before_remove={}",
                scan_id,
                host_state_snapshot(state)
            )
        });
        let Some(mut driver) = state.active_drivers.remove(&scan_id) else {
            continue;
        };
        host_diag(DiagnosticLogLevel::Trace, || {
            format!(
                "pg_fusion detached active scan driver scan_id={} state_after_remove={}",
                scan_id,
                host_state_snapshot(state)
            )
        });
        let peer = state.scan_peers.get(&scan_id).copied().ok_or_else(|| {
            BackendServiceError::ProtocolViolation(format!(
                "missing dedicated peer for active scan {scan_id}"
            ))
        })?;
        host_diag(DiagnosticLogLevel::Trace, || {
            format!(
                "pg_fusion calling driver.step() scan_id={} peer={} state_before_step={}",
                scan_id,
                peer_snapshot(peer),
                host_state_snapshot(state)
            )
        });
        let step = match driver.step() {
            Ok(step) => step,
            Err(err) => {
                host_diag(DiagnosticLogLevel::Basic, || {
                    format!(
                        "pg_fusion driver.step() returned error scan_id={} peer={} state_on_error={} error={}",
                        scan_id,
                        peer_snapshot(peer),
                        host_state_snapshot(state),
                        err
                    )
                });
                return Err(err);
            }
        };
        match step {
            ScanStreamStep::OutboundPage {
                flow,
                producer_id,
                outbound,
            } => {
                host_diag(DiagnosticLogLevel::Trace, || {
                    format!(
                        "pg_fusion active scan scan_id={} produced one outbound page peer={} state_before_reinsert={}",
                        scan_id,
                        peer_snapshot(peer),
                        host_state_snapshot(state)
                    )
                });
                let descriptor = outbound.descriptor();
                let payload_len = outbound.payload_len();
                let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
                    BackendServiceError::ProtocolViolation(format!(
                        "failed to encode scan page header: {err}"
                    ))
                })?;
                if !try_send_scan_peer_bytes(peer, &frame)? {
                    host_diag(DiagnosticLogLevel::Trace, || {
                        format!(
                            "pg_fusion active scan scan_id={} scan control ring is full; deferring outbound page peer={} state_before_reinsert={}",
                            scan_id,
                            peer_snapshot(peer),
                            host_state_snapshot(state)
                        )
                    });
                    driver.defer_outbound_step(ScanStreamStep::OutboundPage {
                        flow,
                        producer_id,
                        outbound,
                    })?;
                    state.active_drivers.insert(scan_id, driver);
                    continue;
                }
                state
                    .metrics
                    .stamp_page(PageDirection::BackendToWorker, descriptor, payload_len);
                state.metrics.increment(MetricId::ScanPagesSentTotal);
                state
                    .metrics
                    .add(MetricId::ScanBytesSentTotal, payload_len as u64);
                outbound.mark_sent();
                state.active_drivers.insert(scan_id, driver);
                host_diag(DiagnosticLogLevel::Trace, || {
                    format!(
                        "pg_fusion reinserted active scan driver after outbound page scan_id={} state_after_reinsert={}",
                        scan_id,
                        host_state_snapshot(state)
                    )
                });
                progressed = true;
            }
            ScanStreamStep::YieldForControl { reason } => {
                host_diag(DiagnosticLogLevel::Trace, || {
                    format!(
                        "pg_fusion active scan scan_id={} yielded for control reason={:?} peer={} state_before_reinsert={}",
                        scan_id,
                        reason,
                        peer_snapshot(peer),
                        host_state_snapshot(state)
                    )
                });
                state.active_drivers.insert(scan_id, driver);
                host_diag(DiagnosticLogLevel::Trace, || {
                    format!(
                        "pg_fusion reinserted active scan driver after yield scan_id={} state_after_reinsert={}",
                        scan_id,
                        host_state_snapshot(state)
                    )
                });
            }
            ScanStreamStep::Finished { flow } => {
                host_diag(DiagnosticLogLevel::Basic, || {
                    format!(
                        "pg_fusion active scan scan_id={} finished flow={:?} peer={} state={}",
                        scan_id,
                        flow,
                        peer_snapshot(peer),
                        host_state_snapshot(state)
                    )
                });
                if !try_send_scan_terminal(
                    peer,
                    BackendScanToWorker::ScanFinished {
                        session_epoch: flow.session_epoch,
                        scan_id: flow.scan_id,
                        producer_id: 0,
                    },
                )? {
                    host_diag(DiagnosticLogLevel::Trace, || {
                        format!(
                            "pg_fusion active scan scan_id={} scan terminal ring is full; deferring finished terminal peer={} state_before_reinsert={}",
                            scan_id,
                            peer_snapshot(peer),
                            host_state_snapshot(state)
                        )
                    });
                    driver.defer_terminal_step(ScanStreamStep::Finished { flow })?;
                    state.active_drivers.insert(scan_id, driver);
                    continue;
                }
                progressed = true;
            }
            ScanStreamStep::Failed {
                flow,
                producer_id,
                message,
            } => {
                host_diag(DiagnosticLogLevel::Basic, || {
                    format!(
                        "pg_fusion active scan scan_id={} failed flow={:?} producer_id={} message={}",
                        scan_id,
                        flow,
                        producer_id,
                        message
                    )
                });
                let message = truncate_scan_failure_message(&message);
                if !try_send_scan_terminal(
                    peer,
                    BackendScanToWorker::ScanFailed {
                        session_epoch: flow.session_epoch,
                        scan_id: flow.scan_id,
                        producer_id,
                        message: &message,
                    },
                )? {
                    host_diag(DiagnosticLogLevel::Trace, || {
                        format!(
                            "pg_fusion active scan scan_id={} scan terminal ring is full; deferring failure terminal peer={} state_before_reinsert={}",
                            scan_id,
                            peer_snapshot(peer),
                            host_state_snapshot(state)
                        )
                    });
                    driver.defer_terminal_step(ScanStreamStep::Failed {
                        flow,
                        producer_id,
                        message,
                    })?;
                    state.active_drivers.clear();
                    state.active_drivers.insert(scan_id, driver);
                    continue;
                }
                state.active_drivers.clear();
                host_diag(DiagnosticLogLevel::Basic, || {
                    format!(
                        "pg_fusion cleared active drivers after scan failure scan_id={} state_after={}",
                        scan_id,
                        host_state_snapshot(state)
                    )
                });
                progressed = true;
            }
        }
    }
    if state.active_drivers.is_empty() && result_ingress_complete(state) {
        progressed |= flush_pending_complete(state, scan_slot, result_slot)?;
    }
    Ok(progressed)
}

fn try_send_scan_peer_bytes(
    peer: BackendLeaseSlot,
    frame: &[u8],
) -> Result<bool, BackendServiceError> {
    match BackendService::send_scan_peer_bytes(peer, frame) {
        Ok(_) => Ok(true),
        Err(BackendServiceError::ScanControlTx(BackendTxError::Ring(TxError::Full { .. }))) => {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

fn result_ingress_complete(state: &HostScanState) -> bool {
    state
        .result_ingress
        .as_ref()
        .is_none_or(ResultIngress::is_complete)
}

fn record_backend_row_return(state: &mut HostScanState, backend_start: u64) {
    state
        .metrics
        .add_elapsed(MetricId::BackendTotalNs, backend_start);
    state.metrics.increment(MetricId::BackendRowsReturnedTotal);
}

fn record_backend_eof(state: &mut HostScanState, backend_start: u64) {
    record_query_total(state);
    state
        .metrics
        .add_elapsed(MetricId::BackendTotalNs, backend_start);
}

fn record_query_total(state: &mut HostScanState) {
    if state.query_total_recorded || state.query_start_ns == 0 {
        return;
    }
    state
        .metrics
        .add_elapsed(MetricId::QueryTotalNs, state.query_start_ns);
    state.query_total_recorded = true;
}

fn flush_pending_complete(
    state: &mut HostScanState,
    scan_slot: *mut pg_sys::TupleTableSlot,
    result_slot: *mut pg_sys::TupleTableSlot,
) -> Result<bool, BackendServiceError> {
    let Some(session_epoch) = state.pending_complete_session_epoch.take() else {
        return Ok(false);
    };
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion flushing pending CompleteExecution session_epoch={} scan_slot={} result_slot={} current_mcxt={:p} state={}",
            session_epoch,
            tuple_slot_snapshot(scan_slot),
            tuple_slot_snapshot(result_slot),
            unsafe { pg_sys::CurrentMemoryContext },
            host_state_snapshot(state)
        )
    });
    if logging::backend_log_enabled(DiagnosticLogLevel::Trace) {
        unsafe {
            diag::update_slot_watch(scan_slot, result_slot);
            if let Some(ingress) = state.result_ingress.as_ref() {
                let (per_tuple_cxt, queue_cxt) = ingress.debug_contexts();
                diag::update_result_ingress_watch(
                    ingress.debug_project_slot(),
                    ingress.debug_front_queued_tuple(),
                    per_tuple_cxt,
                    queue_cxt,
                );
            } else {
                diag::update_result_ingress_watch(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
            }
            diag::log_live_watch("pg_fusion before accept_complete_execution");
        }
    }
    let slot_id = state
        .control_lease
        .as_ref()
        .map(|lease| lease.slot_id())
        .ok_or(BackendServiceError::NoActiveExecution)?;
    let _ = BackendService::accept_complete_execution(slot_id, session_epoch)?;
    state.execution_key = None;
    if logging::backend_log_enabled(DiagnosticLogLevel::Trace) {
        unsafe {
            diag::update_slot_watch(scan_slot, result_slot);
            if let Some(ingress) = state.result_ingress.as_ref() {
                let (per_tuple_cxt, queue_cxt) = ingress.debug_contexts();
                diag::update_result_ingress_watch(
                    ingress.debug_project_slot(),
                    ingress.debug_front_queued_tuple(),
                    per_tuple_cxt,
                    queue_cxt,
                );
            } else {
                diag::update_result_ingress_watch(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                );
            }
            diag::log_live_watch("pg_fusion after accept_complete_execution");
        }
    }
    host_diag(DiagnosticLogLevel::Basic, || {
        format!(
            "pg_fusion accepted backend CompleteExecution session_epoch={} scan_slot={} result_slot={} current_mcxt={:p} state_after={}",
            session_epoch,
            tuple_slot_snapshot(scan_slot),
            tuple_slot_snapshot(result_slot),
            unsafe { pg_sys::CurrentMemoryContext },
            host_state_snapshot(state)
        )
    });
    Ok(true)
}

fn try_send_scan_terminal(
    peer: BackendLeaseSlot,
    message: BackendScanToWorker<'_>,
) -> Result<bool, BackendServiceError> {
    let mut buf = vec![0_u8; encoded_len_backend_scan_to_worker(message)];
    let written = encode_backend_scan_to_worker_into(message, &mut buf)
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    try_send_scan_peer_bytes(peer, &buf[..written])
}

fn publish_plan_to_worker(lease: &mut BackendSlotLease) -> Result<(), BackendServiceError> {
    loop {
        match BackendService::step_execution_start()? {
            plan_flow::BackendPlanStep::OutboundPage { outbound, .. } => {
                let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
                    BackendServiceError::ProtocolViolation(format!(
                        "failed to encode plan page header: {err}"
                    ))
                })?;
                let mut tx = lease.to_worker_tx();
                let _ = tx.send_frame(&frame)?;
                outbound.mark_sent();
            }
            plan_flow::BackendPlanStep::CloseFrame { frame, .. } => {
                let frame = encode_issued_frame(frame).map_err(|err| {
                    BackendServiceError::ProtocolViolation(format!(
                        "failed to encode plan close header: {err}"
                    ))
                })?;
                let mut tx = lease.to_worker_tx();
                let _ = tx.send_frame(&frame)?;
                break;
            }
            plan_flow::BackendPlanStep::Blocked { .. } => {
                wait_latch(Some(Duration::from_millis(1)))
            }
            plan_flow::BackendPlanStep::LogicalError { message, .. } => {
                return Err(BackendServiceError::ProtocolViolation(message));
            }
        }
    }
    Ok(())
}

fn send_backend_execution(
    lease: &mut BackendSlotLease,
    message: BackendExecutionToWorker<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), BackendServiceError> {
    let needed = encoded_len_backend_execution_to_worker(message);
    if scratch.len() < needed {
        scratch.resize(needed, 0);
    }
    let written = encode_backend_execution_to_worker_into(message, scratch)
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    let mut tx = lease.to_worker_tx();
    let _ = tx.send_frame(&scratch[..written])?;
    Ok(())
}

fn decode_primary_inbound(bytes: &[u8]) -> Result<PrimaryInbound, Box<dyn std::error::Error>> {
    match decode_runtime_message_family(bytes) {
        Ok(RuntimeMessageFamily::WorkerExecutionToBackend) => Ok(PrimaryInbound::Control(
            decode_worker_execution_to_backend(bytes)?,
        )),
        Ok(other) => Err(format!("unexpected primary message family {other:?}").into()),
        Err(runtime_error)
            if matches!(
                runtime_error,
                runtime_protocol::DecodeError::InvalidMagic { .. }
                    | runtime_protocol::DecodeError::UnsupportedVersion { .. }
                    | runtime_protocol::DecodeError::TruncatedEnvelope { .. }
            ) =>
        {
            Ok(PrimaryInbound::Issued(decode_issued_frame(bytes)?))
        }
        Err(err) => Err(Box::new(err)),
    }
}

fn build_transport_schema(sql: &str) -> Result<SchemaRef, String> {
    let config = crate::host_config().map_err(|err| err.to_string())?;
    let built = plan_builder::PlanBuilder::new()
        .with_config(config.plan_builder_config())
        .build(plan_builder::PlanBuildInput {
            sql,
            params: Vec::new(),
        })
        .map_err(|err| err.to_string())?;
    let output_schema = Arc::new(Schema::new(
        built
            .logical_plan
            .schema()
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
            .collect::<Vec<_>>(),
    ));
    let (schema, _) =
        normalize_result_transport_schema(&output_schema).map_err(|err| err.to_string())?;
    Ok(schema)
}

struct DynamicScanWorkerLauncher {
    jobs: ScanWorkerJobRegistryHandle,
    budgets: BTreeMap<u64, ScanWorkerBudget>,
    capacity_exhausted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ScanWorkerBudget {
    worker_count: u16,
    block_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScanWorkerBudgetCandidate {
    scan_id: u64,
    block_count: u64,
    max_workers: u16,
    assigned_workers: u16,
}

impl ScanWorkerLauncher for DynamicScanWorkerLauncher {
    fn prepare_query(
        &mut self,
        input: ScanWorkerQueryInput<'_>,
    ) -> Result<(), BackendServiceError> {
        self.budgets.clear();
        self.capacity_exhausted = false;

        let query_budget = postgres_dynamic_scan_worker_budget();
        if query_budget == 0 {
            return Ok(());
        }

        let mut candidates = Vec::new();
        for spec in input.scans {
            if spec.compiled_scan.uses_dummy_projection
                || spec.compiled_scan.output_columns.is_empty()
            {
                continue;
            }

            let storage = relation_storage_info(spec.table_oid)?;
            if !storage.is_cross_backend_visible
                || !storage.has_no_dropped_attributes
                || storage.block_count <= 1
            {
                continue;
            }

            candidates.push(ScanWorkerBudgetCandidate {
                scan_id: spec.scan_id.get(),
                block_count: storage.block_count,
                max_workers: storage
                    .block_count
                    .saturating_sub(1)
                    .min(u64::from(u16::MAX - 1)) as u16,
                assigned_workers: 0,
            });
        }

        self.budgets = assign_scan_worker_budgets(candidates, query_budget);
        Ok(())
    }

    fn launch_scan_workers(
        &mut self,
        input: ScanWorkerLaunchInput<'_>,
    ) -> Result<ScanWorkerLaunchOutput, BackendServiceError> {
        if self.capacity_exhausted {
            return Ok(ScanWorkerLaunchOutput::default());
        }

        let Some(budget) = self.budgets.get(&input.scan_id).copied() else {
            return Ok(ScanWorkerLaunchOutput::default());
        };
        if budget.worker_count == 0 || budget.block_count <= 1 {
            return Ok(ScanWorkerLaunchOutput::default());
        }
        let total_producers = (u64::from(budget.worker_count) + 1)
            .min(budget.block_count)
            .min(u64::from(u16::MAX)) as u16;
        if total_producers <= 1 {
            return Ok(ScanWorkerLaunchOutput::default());
        }

        let db_oid: u32 = unsafe { pg_sys::MyDatabaseId.into() };
        let user_oid: u32 = unsafe { pg_sys::GetUserId().into() };
        let mut workers = Vec::with_capacity(total_producers.saturating_sub(1) as usize);
        let mut job_payload = Vec::new();
        for producer_id in 1..total_producers {
            let range = producer_block_range(budget.block_count, total_producers, producer_id);
            let descriptor = match build_standalone_scan_descriptor(input.spec, Some(range)) {
                Ok(descriptor) => descriptor,
                Err(err) => {
                    cancel_launched_scan_workers(&workers, input.session_epoch, input.scan_id);
                    return Err(err);
                }
            };
            if let Err(err) = encode_scan_worker_descriptor(&descriptor, &mut job_payload) {
                cancel_launched_scan_workers(&workers, input.session_epoch, input.scan_id);
                return Err(BackendServiceError::ProtocolViolation(err.to_string()));
            }
            let job_id = match self.jobs.allocate(ScanWorkerJobSpec {
                payload: &job_payload,
                db_oid,
                user_oid,
                session_epoch: input.session_epoch,
                scan_id: input.scan_id,
                producer_id,
                producer_count: total_producers,
                scan_timing_detail: input.scan_timing_detail,
            }) {
                Ok(job_id) => job_id,
                Err(ScanWorkerJobError::NoFreeJobSlots) => {
                    return Ok(self.handle_capacity_exhausted(
                        &workers,
                        input.session_epoch,
                        input.scan_id,
                        "no free scan worker job slots",
                    ));
                }
                Err(err) => {
                    cancel_launched_scan_workers(&workers, input.session_epoch, input.scan_id);
                    return Err(BackendServiceError::ProtocolViolation(err.to_string()));
                }
            };
            if let Err(err) = BackgroundWorkerBuilder::new("pg_fusion scan worker")
                .set_function("scan_worker_main")
                .set_library("pg_fusion")
                .enable_spi_access()
                .set_argument((job_id as i32).into_datum())
                .set_notify_pid(unsafe { pg_sys::MyProcPid })
                .load_dynamic()
            {
                let message = format!("failed to launch scan worker job {job_id}: {err:?}");
                mark_scan_worker_job_failed(self.jobs, job_id, &message);
                return Ok(self.handle_capacity_exhausted(
                    &workers,
                    input.session_epoch,
                    input.scan_id,
                    &message,
                ));
            }
            let peer = match self.jobs.wait_ready(job_id, Duration::from_secs(5)) {
                Ok(peer) => peer,
                Err(err) => {
                    let message = err.to_string();
                    mark_scan_worker_job_failed(self.jobs, job_id, &message);
                    cancel_launched_scan_workers(&workers, input.session_epoch, input.scan_id);
                    return Err(BackendServiceError::ProtocolViolation(message));
                }
            };
            workers.push(ScanWorkerProducer::worker(producer_id, peer));
        }

        Ok(ScanWorkerLaunchOutput {
            leader_ctid_range: Some(producer_block_range(budget.block_count, total_producers, 0)),
            workers,
        })
    }
}

impl DynamicScanWorkerLauncher {
    fn handle_capacity_exhausted(
        &mut self,
        workers: &[ScanWorkerProducer],
        session_epoch: u64,
        scan_id: u64,
        message: &str,
    ) -> ScanWorkerLaunchOutput {
        self.capacity_exhausted = true;
        cancel_launched_scan_workers(workers, session_epoch, scan_id);
        host_diag(DiagnosticLogLevel::Basic, || {
            format!(
                "pg_fusion dynamic scan worker capacity exhausted for scan_id={scan_id}: {message}; continuing leader-only"
            )
        });
        ScanWorkerLaunchOutput::default()
    }
}

fn postgres_dynamic_scan_worker_budget() -> u16 {
    let requested = postgres_max_parallel_workers_per_gather().min(32);
    let worker_process_budget = postgres_max_worker_processes().saturating_sub(1);
    requested
        .min(worker_process_budget)
        .min(u32::from(u16::MAX)) as u16
}

fn postgres_max_parallel_workers_per_gather() -> u32 {
    unsafe { pg_sys::max_parallel_workers_per_gather.max(0) as u32 }
}

fn postgres_max_worker_processes() -> u32 {
    unsafe { pg_sys::max_worker_processes.max(0) as u32 }
}

fn assign_scan_worker_budgets(
    mut candidates: Vec<ScanWorkerBudgetCandidate>,
    query_budget: u16,
) -> BTreeMap<u64, ScanWorkerBudget> {
    candidates.sort_by(|left, right| {
        right
            .block_count
            .cmp(&left.block_count)
            .then_with(|| left.scan_id.cmp(&right.scan_id))
    });

    let mut remaining = query_budget;
    while remaining > 0 {
        let mut assigned_this_round = false;
        for candidate in &mut candidates {
            if remaining == 0 {
                break;
            }
            if candidate.assigned_workers >= candidate.max_workers {
                continue;
            }
            candidate.assigned_workers += 1;
            remaining -= 1;
            assigned_this_round = true;
        }
        if !assigned_this_round {
            break;
        }
    }

    candidates
        .into_iter()
        .filter(|candidate| candidate.assigned_workers > 0)
        .map(|candidate| {
            (
                candidate.scan_id,
                ScanWorkerBudget {
                    worker_count: candidate.assigned_workers,
                    block_count: candidate.block_count,
                },
            )
        })
        .collect()
}

fn mark_scan_worker_job_failed(jobs: ScanWorkerJobRegistryHandle, job_id: usize, message: &str) {
    if let Err(err) = jobs.mark_failed(job_id, message) {
        host_diag(DiagnosticLogLevel::Basic, || {
            format!("pg_fusion failed to mark scan worker job {job_id} failed during launch cleanup: {err}")
        });
    }
}

fn cancel_launched_scan_workers(workers: &[ScanWorkerProducer], session_epoch: u64, scan_id: u64) {
    if workers.is_empty() {
        return;
    }

    let scan_region = attach_scan_region();
    let transport = match WorkerTransport::attach(&scan_region) {
        Ok(transport) => transport,
        Err(err) => {
            host_diag(DiagnosticLogLevel::Basic, || {
                format!("pg_fusion failed to attach scan transport for launch cleanup: {err}")
            });
            return;
        }
    };
    let mut scratch = Vec::new();
    for worker in workers {
        if let Err(err) = send_scan_worker_cancel(
            &transport,
            worker.peer,
            session_epoch,
            scan_id,
            &mut scratch,
        ) {
            host_diag(DiagnosticLogLevel::Basic, || {
                format!(
                    "pg_fusion failed to cancel scan worker producer_id={} peer={} during launch cleanup: {err}",
                    worker.producer_id,
                    peer_snapshot(worker.peer)
                )
            });
        }
    }
}

fn send_scan_worker_cancel(
    transport: &WorkerTransport,
    peer: BackendLeaseSlot,
    session_epoch: u64,
    scan_id: u64,
    scratch: &mut Vec<u8>,
) -> Result<(), BackendServiceError> {
    let message = WorkerScanToBackend::CancelScan {
        session_epoch,
        scan_id,
    };
    let needed = encoded_len_worker_scan_to_backend(message);
    if scratch.len() < needed {
        scratch.resize(needed, 0);
    }
    let written = encode_worker_scan_to_backend_into(message, scratch)
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    let mut slot = transport
        .slot_for_backend_lease(peer)
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    let mut tx = slot
        .to_backend_tx()
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    let _ = tx
        .send_frame(&scratch[..written])
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    Ok(())
}

struct RelationStorageInfo {
    block_count: u64,
    is_cross_backend_visible: bool,
    has_no_dropped_attributes: bool,
}

fn relation_storage_info(table_oid: u32) -> Result<RelationStorageInfo, BackendServiceError> {
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let relation = PgrxRelation::with_lock(table_oid.into(), pg_sys::AccessShareLock as _);
        let blocks = pg_sys::RelationGetNumberOfBlocksInFork(
            relation.as_ptr(),
            pg_sys::ForkNumber::MAIN_FORKNUM,
        );
        let has_no_dropped_attributes = relation
            .tuple_desc()
            .iter()
            .all(|attribute| !attribute.is_dropped());
        let relpersistence = (*(*relation.as_ptr()).rd_rel).relpersistence as u8;
        Ok(RelationStorageInfo {
            block_count: u64::from(blocks),
            is_cross_backend_visible: relpersistence != pg_sys::RELPERSISTENCE_TEMP,
            has_no_dropped_attributes,
        })
    }))
    .catch_others(|error| {
        Err(BackendServiceError::Postgres(format!(
            "failed to read relation block count: {error:?}"
        )))
    })
    .execute()
}

fn producer_block_range(
    block_count: u64,
    total_producers: u16,
    producer_id: u16,
) -> CtidBlockRange {
    let total = u64::from(total_producers);
    let id = u64::from(producer_id);
    CtidBlockRange {
        start_block: block_count.saturating_mul(id) / total,
        end_block: block_count.saturating_mul(id + 1) / total,
    }
}

fn scan_peers_from_begin(begin: &BeginExecutionOutput) -> BTreeMap<u64, BackendLeaseSlot> {
    begin
        .scan_channels
        .iter()
        .filter(|descriptor| descriptor.role == runtime_protocol::ProducerRole::Leader)
        .map(|descriptor| {
            (
                descriptor.scan_id,
                BackendLeaseSlot::new(
                    descriptor.peer.slot_id(),
                    control_transport::BackendLeaseId::new(
                        descriptor.peer.generation(),
                        descriptor.peer.lease_epoch(),
                    ),
                ),
            )
        })
        .collect()
}

fn peer_snapshot(peer: BackendLeaseSlot) -> String {
    format!(
        "slot_id={} generation={} lease_epoch={}",
        peer.slot_id(),
        peer.lease_id().generation(),
        peer.lease_id().lease_epoch()
    )
}

fn control_lease_snapshot(lease: &BackendSlotLease) -> String {
    peer_snapshot(lease.backend_lease_slot())
}

fn scan_peer_keys(state: &HostScanState) -> Vec<u64> {
    state.scan_peers.keys().copied().collect()
}

fn active_driver_keys(state: &HostScanState) -> Vec<u64> {
    state.active_drivers.keys().copied().collect()
}

fn host_state_snapshot(state: &HostScanState) -> String {
    format!(
        "execution_key={:?} pending_complete={:?} active_drivers={:?} scan_peers={:?} result_complete={:?} owns_result_slot={}",
        state.execution_key,
        state.pending_complete_session_epoch,
        active_driver_keys(state),
        scan_peer_keys(state),
        state.result_ingress.as_ref().map(ResultIngress::is_complete),
        state.owns_result_slot,
    )
}

fn tuple_slot_snapshot(slot: *mut pg_sys::TupleTableSlot) -> String {
    if slot.is_null() {
        return "slot=null".to_string();
    }

    unsafe {
        let flags = (*slot).tts_flags as u32;
        let ops = if (*slot).tts_ops == &raw const pg_sys::TTSOpsMinimalTuple {
            "minimal"
        } else if (*slot).tts_ops == &raw const pg_sys::TTSOpsVirtual {
            "virtual"
        } else {
            "other"
        };
        format!(
            "slot={:p} ops={} flags=0x{:x} tupdesc={:p} mcxt={:p}",
            slot,
            ops,
            flags,
            (*slot).tts_tupleDescriptor,
            (*slot).tts_mcxt,
        )
    }
}

fn host_diag(level: DiagnosticLogLevel, message: impl FnOnce() -> String) {
    logging::write_backend_log(level, "backend", "extension::custom_scan", message);
}

unsafe fn tuple_desc_from_scan(node: *mut CustomScanState) -> pg_sys::TupleDesc {
    let plan = (*node).ss.ps.plan as *mut CustomScan;
    pg_sys::ExecTypeFromTL((*plan).custom_scan_tlist)
}

unsafe fn tuple_desc_for_slots(node: *mut CustomScanState) -> pg_sys::TupleDesc {
    let scan_slot = (*node).ss.ss_ScanTupleSlot;
    if !scan_slot.is_null() && !(*scan_slot).tts_tupleDescriptor.is_null() {
        return (*scan_slot).tts_tupleDescriptor;
    }

    let result_slot = (*node).ss.ps.ps_ResultTupleSlot;
    if !result_slot.is_null() && !(*result_slot).tts_tupleDescriptor.is_null() {
        return (*result_slot).tts_tupleDescriptor;
    }

    tuple_desc_from_scan(node)
}

fn truncate_scan_failure_message(message: &str) -> String {
    if message.len() <= runtime_protocol::MAX_SCAN_FAILURE_MESSAGE_LEN {
        return message.to_string();
    }

    let mut cutoff = runtime_protocol::MAX_SCAN_FAILURE_MESSAGE_LEN;
    while !message.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    message[..cutoff].to_string()
}

unsafe fn sql_from_custom_private(list: *mut List) -> String {
    let cell = list_nth(list, 0);
    let node = (*cell).ptr_value as *const pg_sys::String;
    assert!(!node.is_null(), "custom private SQL node must be present");
    let ptr = (*node).sval as *const i8;
    CStr::from_ptr(ptr)
        .to_str()
        .expect("custom private SQL must be valid UTF-8")
        .to_string()
}

unsafe fn list_nth(list: *mut List, n: i32) -> *mut pg_sys::ListCell {
    assert!(!list.is_null());
    assert!(n >= 0 && n < (*list).length);
    (*list).elements.offset(n as isize)
}

unsafe fn state_from_node<'a>(node: *mut CustomScanState) -> &'a mut PgFusionScanState {
    &mut *(node as *mut PgFusionScanState)
}

unsafe fn host_state_mut<'a>(node: *mut CustomScanState) -> &'a mut HostScanState {
    &mut *state_from_node(node).state
}

unsafe fn host_state_ref<'a>(node: *mut CustomScanState) -> &'a HostScanState {
    &*state_from_node(node).state
}

fn wait_latch(timeout: Option<Duration>) {
    let timeout_ms = timeout
        .map(|value| value.as_millis().try_into().expect("timeout fits c_long"))
        .unwrap_or(-1);
    let events = if timeout.is_some() {
        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH
    } else {
        WL_LATCH_SET | WL_POSTMASTER_DEATH
    };
    let rc = unsafe {
        let rc = pg_sys::WaitLatch(
            MyLatch,
            events as i32,
            timeout_ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(MyLatch);
        rc
    };
    check_for_interrupts!();
    if rc & WL_POSTMASTER_DEATH as i32 != 0 {
        panic!("postmaster died");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(scan_id: u64, block_count: u64, max_workers: u16) -> ScanWorkerBudgetCandidate {
        ScanWorkerBudgetCandidate {
            scan_id,
            block_count,
            max_workers,
            assigned_workers: 0,
        }
    }

    fn worker_count(budgets: &BTreeMap<u64, ScanWorkerBudget>, scan_id: u64) -> u16 {
        budgets
            .get(&scan_id)
            .map(|budget| budget.worker_count)
            .unwrap_or(0)
    }

    #[test]
    fn scan_worker_budget_assigns_single_scan_up_to_capacity() {
        let budgets = assign_scan_worker_budgets(vec![candidate(7, 10, 9)], 4);

        assert_eq!(worker_count(&budgets, 7), 4);
        assert_eq!(budgets.get(&7).expect("budget").block_count, 10);
    }

    #[test]
    fn scan_worker_budget_round_robins_largest_scans_first() {
        let budgets = assign_scan_worker_budgets(
            vec![
                candidate(3, 20, 19),
                candidate(1, 100, 99),
                candidate(2, 100, 99),
            ],
            5,
        );

        assert_eq!(worker_count(&budgets, 1), 2);
        assert_eq!(worker_count(&budgets, 2), 2);
        assert_eq!(worker_count(&budgets, 3), 1);
    }

    #[test]
    fn scan_worker_budget_skips_saturated_candidates() {
        let budgets =
            assign_scan_worker_budgets(vec![candidate(1, 100, 1), candidate(2, 90, 5)], 4);

        assert_eq!(worker_count(&budgets, 1), 1);
        assert_eq!(worker_count(&budgets, 2), 3);
    }
}
