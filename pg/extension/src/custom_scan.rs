use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{Field, Schema, SchemaRef};
use backend_service::{
    ActiveScanDriver, BackendService, BackendServiceError, BeginExecutionOutput,
    DiagnosticLogLevel, ExecutionKey, ExplainInput, ExplainRenderOptions, OpenScanInput,
    ScanStreamStep, StartExecutionInput,
};
use control_transport::{BackendLeaseSlot, BackendSlotLease};
use issuance::{
    decode_issued_frame, encode_issued_frame, IssuancePool, IssuedOwnedFrame, IssuedTx,
};
use pgrx::pg_sys::{
    self, CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, ExecutorEnd_hook_type,
    List, MyLatch, Node, QueryDesc, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
};
use pgrx::prelude::*;
use pgrx::{check_for_interrupts, pg_guard};
use pool::PagePool;
use runtime_protocol::{
    decode_runtime_message_family, decode_worker_execution_to_backend,
    decode_worker_scan_to_backend, encode_backend_execution_to_worker_into,
    encode_backend_scan_to_worker_into, encoded_len_backend_execution_to_worker,
    encoded_len_backend_scan_to_worker, BackendExecutionToWorker, BackendScanToWorker,
    RuntimeMessageFamily, WorkerExecutionToBackend, WorkerScanToBackendRef,
};
use transfer::PageTx;
use worker_runtime::normalize_result_transport_schema;

use crate::diag;
use crate::guc::host_config;
use crate::logging;
use crate::result_ingress::{AcceptedResultFrame, ResultIngress};
use crate::shmem::{
    attach_control_region, attach_issuance_pool, attach_page_pool, attach_scan_region,
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
    let backend_config = config.backend_service_config();
    let control_region = attach_control_region();
    let scan_region = attach_scan_region();
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
    let begin = {
        let _planner_bypass = PlannerBypassGuard::enter();
        BackendService::begin_execution(StartExecutionInput {
            slot_id: control_lease.slot_id(),
            sql: &state.sql,
            params: Vec::new(),
            plan_tx,
            scan_slot_region: &scan_region,
            config: backend_config,
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
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_pg_fusion_scan(
    node: *mut CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let state = host_state_mut(node);
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
            return std::ptr::null_mut();
        }

        if !progressed {
            wait_latch(Some(Duration::from_millis(1)));
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
                let ingress = state.result_ingress.as_mut().ok_or_else(|| {
                    BackendServiceError::ProtocolViolation(
                        "result ingress is not initialized".into(),
                    )
                })?;
                let accepted = ingress
                    .accept_frame(&frame)
                    .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
                if accepted == AcceptedResultFrame::Page {
                    break;
                }
            }
        }
    }
    Ok(progressed)
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
            ScanStreamStep::OutboundPage { outbound, .. } => {
                host_diag(DiagnosticLogLevel::Trace, || {
                    format!(
                        "pg_fusion active scan scan_id={} produced one outbound page peer={} state_before_reinsert={}",
                        scan_id,
                        peer_snapshot(peer),
                        host_state_snapshot(state)
                    )
                });
                let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
                    BackendServiceError::ProtocolViolation(format!(
                        "failed to encode scan page header: {err}"
                    ))
                })?;
                let _ = BackendService::send_scan_peer_bytes(peer, &frame)?;
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
                send_scan_terminal(
                    peer,
                    BackendScanToWorker::ScanFinished {
                        session_epoch: flow.session_epoch,
                        scan_id: flow.scan_id,
                        producer_id: 0,
                    },
                )?;
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
                send_scan_terminal(
                    peer,
                    BackendScanToWorker::ScanFailed {
                        session_epoch: flow.session_epoch,
                        scan_id: flow.scan_id,
                        producer_id,
                        message: &message,
                    },
                )?;
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

fn result_ingress_complete(state: &HostScanState) -> bool {
    state
        .result_ingress
        .as_ref()
        .is_none_or(ResultIngress::is_complete)
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

fn send_scan_terminal(
    peer: BackendLeaseSlot,
    message: BackendScanToWorker<'_>,
) -> Result<(), BackendServiceError> {
    let mut buf = vec![0_u8; encoded_len_backend_scan_to_worker(message)];
    let written = encode_backend_scan_to_worker_into(message, &mut buf)
        .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
    let _ = BackendService::send_scan_peer_bytes(peer, &buf[..written])?;
    Ok(())
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
    let built = plan_builder::PlanBuilder::new()
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

fn scan_peers_from_begin(begin: &BeginExecutionOutput) -> BTreeMap<u64, BackendLeaseSlot> {
    begin
        .scan_channels
        .iter()
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
