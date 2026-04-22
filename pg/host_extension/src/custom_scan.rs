use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{Field, Schema, SchemaRef};
use backend_service::{
    ActiveScanDriver, BackendService, BackendServiceError, BeginExecutionOutput, ExecutionKey,
    ExplainInput, OpenScanInput, ScanStreamStep, StartExecutionInput,
};
use control_transport::{BackendLeaseSlot, BackendSlotLease};
use issuance::{
    decode_issued_frame, encode_issued_frame, IssuancePool, IssuedOwnedFrame, IssuedTx,
};
use pgrx::pg_sys::{
    self, CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, List, MyLatch, Node,
    WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
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

use crate::guc::host_config;
use crate::result_ingress::ResultIngress;
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
}

enum PrimaryInbound {
    Control(WorkerExecutionToBackend),
    Issued(IssuedOwnedFrame),
}

pub(crate) fn register_methods() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(scan_methods());
    }
}

pub(crate) fn scan_methods() -> *const CustomScanMethods {
    SCAN_METHODS.with(|methods| methods as *const CustomScanMethods)
}

fn exec_methods() -> *const CustomExecMethods {
    EXEC_METHODS.with(|methods| methods as *const CustomExecMethods)
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

#[pg_guard]
unsafe extern "C-unwind" fn begin_pg_fusion_scan(
    node: *mut CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: i32,
) {
    let state = host_state_mut(node);
    if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        let tuple_desc = tuple_desc_from_scan(node);
        pg_sys::ExecInitScanTupleSlot(
            estate,
            &mut (*node).ss,
            tuple_desc,
            &raw const pg_sys::TTSOpsMinimalTuple,
        );
        (*node).ss.ps.ps_ResultTupleSlot =
            pg_sys::MakeSingleTupleTableSlot(tuple_desc, &raw const pg_sys::TTSOpsMinimalTuple);
        state.control_lease = None;
        state.execution_key = None;
        state.scan_peers.clear();
        state.active_drivers.clear();
        state.pending_complete_session_epoch = None;
        state.result_ingress = None;
        state.terminal_error = None;
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
    let tuple_desc = tuple_desc_from_scan(node);

    pg_sys::ExecInitScanTupleSlot(
        estate,
        &mut (*node).ss,
        tuple_desc,
        &raw const pg_sys::TTSOpsMinimalTuple,
    );
    (*node).ss.ps.ps_ResultTupleSlot =
        pg_sys::MakeSingleTupleTableSlot(tuple_desc, &raw const pg_sys::TTSOpsMinimalTuple);

    state.result_ingress = Some(
        ResultIngress::new(transport_schema, tuple_desc, page_pool, issuance_pool)
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
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_pg_fusion_scan(
    node: *mut CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let state = host_state_mut(node);
    let scan_slot = (*node).ss.ss_ScanTupleSlot;
    if scan_slot.is_null() {
        return std::ptr::null_mut();
    }

    loop {
        if let Some(err) = state.terminal_error.take() {
            error!("pg_fusion execution failed: {err}");
        }

        if let Some(result) = state
            .result_ingress
            .as_mut()
            .and_then(|ingress| ingress.store_next_into(scan_slot))
        {
            return result;
        }

        let mut progressed = false;
        progressed |= poll_primary_peer(state).unwrap_or_else(|err| {
            error!("pg_fusion primary peer poll failed: {err}");
        });
        if let Some(result) = state
            .result_ingress
            .as_mut()
            .and_then(|ingress| ingress.store_next_into(scan_slot))
        {
            return result;
        }

        progressed |= poll_scan_peers(state).unwrap_or_else(|err| {
            error!("pg_fusion scan peer poll failed: {err}");
        });
        progressed |= drive_active_scans(state).unwrap_or_else(|err| {
            error!("pg_fusion scan driver failed: {err}");
        });

        if let Some(result) = state
            .result_ingress
            .as_mut()
            .and_then(|ingress| ingress.store_next_into(scan_slot))
        {
            return result;
        }

        if state
            .result_ingress
            .as_ref()
            .is_some_and(ResultIngress::is_complete)
        {
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
    if let Some(key) = state.execution_key.take() {
        let _ = BackendService::accept_cancel_execution(key.slot_id, key.session_epoch);
    }
    state.active_drivers.clear();
    state.pending_complete_session_epoch = None;
    state.result_ingress.take();
    state.control_lease.take();
    let result_slot = (*node).ss.ps.ps_ResultTupleSlot;
    if !result_slot.is_null() {
        pg_sys::ExecDropSingleTupleTableSlot(result_slot);
        (*node).ss.ps.ps_ResultTupleSlot = std::ptr::null_mut();
    }

    let host_state = std::mem::replace(&mut state_from_node(node).state, std::ptr::null_mut());
    if !host_state.is_null() {
        drop(Box::from_raw(host_state));
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn explain_pg_fusion_scan(
    node: *mut CustomScanState,
    _ancestors: *mut List,
    es: *mut pg_sys::ExplainState,
) {
    let state = host_state_ref(node);
    let rendered = {
        let _planner_bypass = PlannerBypassGuard::enter();
        BackendService::render_explain(ExplainInput {
            sql: &state.sql,
            params: Vec::new(),
        })
    }
    .unwrap_or_else(|err| error!("pg_fusion explain failed: {err}"));
    let rendered = CString::new(rendered).expect("explain text must not contain NUL bytes");
    pg_sys::ExplainPropertyText(c"pg_fusion".as_ptr(), rendered.as_ptr(), es);
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
                ingress
                    .accept_frame(&frame)
                    .map_err(|err| BackendServiceError::ProtocolViolation(err.to_string()))?;
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
            if let Some(ingress) = state.result_ingress.as_mut() {
                ingress.mark_execution_complete();
            }
            state.pending_complete_session_epoch = Some(session_epoch);
        }
        WorkerExecutionToBackend::FailExecution {
            session_epoch,
            code,
            detail,
        } => {
            let _ = BackendService::accept_fail_execution(slot_id, session_epoch, code, detail)?;
            state.execution_key = None;
            state.active_drivers.clear();
            state.pending_complete_session_epoch = None;
            state.terminal_error = Some(format!(
                "worker failed execution session_epoch={session_epoch} code={code:?} detail={detail:?}"
            ));
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
                    }
                }
                WorkerScanToBackendRef::CancelScan {
                    session_epoch: _,
                    scan_id,
                } => {
                    if let Some(mut driver) = state.active_drivers.remove(&scan_id) {
                        let _ = driver.cancel_scan()?;
                    }
                }
            }
        }
    }
    Ok(progressed)
}

fn drive_active_scans(state: &mut HostScanState) -> Result<bool, BackendServiceError> {
    let scan_ids = state.active_drivers.keys().copied().collect::<Vec<_>>();
    let mut progressed = false;
    for scan_id in scan_ids {
        let Some(mut driver) = state.active_drivers.remove(&scan_id) else {
            continue;
        };
        let peer = state.scan_peers.get(&scan_id).copied().ok_or_else(|| {
            BackendServiceError::ProtocolViolation(format!(
                "missing dedicated peer for active scan {scan_id}"
            ))
        })?;
        match driver.step()? {
            ScanStreamStep::OutboundPage { outbound, .. } => {
                let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
                    BackendServiceError::ProtocolViolation(format!(
                        "failed to encode scan page header: {err}"
                    ))
                })?;
                let _ = BackendService::send_scan_peer_bytes(peer, &frame)?;
                outbound.mark_sent();
                state.active_drivers.insert(scan_id, driver);
                progressed = true;
            }
            ScanStreamStep::YieldForControl { .. } => {
                state.active_drivers.insert(scan_id, driver);
            }
            ScanStreamStep::Finished { flow } => {
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
                progressed = true;
            }
        }
    }
    if state.active_drivers.is_empty() && result_ingress_complete(state) {
        progressed |= flush_pending_complete(state)?;
    }
    Ok(progressed)
}

fn result_ingress_complete(state: &HostScanState) -> bool {
    state
        .result_ingress
        .as_ref()
        .is_none_or(ResultIngress::is_complete)
}

fn flush_pending_complete(state: &mut HostScanState) -> Result<bool, BackendServiceError> {
    let Some(session_epoch) = state.pending_complete_session_epoch.take() else {
        return Ok(false);
    };
    let slot_id = state
        .control_lease
        .as_ref()
        .map(|lease| lease.slot_id())
        .ok_or(BackendServiceError::NoActiveExecution)?;
    let _ = BackendService::accept_complete_execution(slot_id, session_epoch)?;
    state.execution_key = None;
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

unsafe fn tuple_desc_from_scan(node: *mut CustomScanState) -> pg_sys::TupleDesc {
    let plan = (*node).ss.ps.plan as *mut CustomScan;
    pg_sys::ExecTypeFromTL((*plan).custom_scan_tlist)
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
