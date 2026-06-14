use std::collections::VecDeque;
use std::ffi::CStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ::metrics::{MetricId, PageDirection, RuntimeMetrics};
use ::worker::{
    record_datafusion_spill_leaks, record_datafusion_spill_metrics, DecodedInbound,
    ExecutionSpillDir, ResultPageEmitter, ResultPageProducerConfig, ResultPageStep,
    ScanIngressProvider, TransportScanBatchSource, TransportWorkerRuntime, WorkerRuntimeCore,
    WorkerRuntimeError, WorkerRuntimeStep, WorkerSpillRuntime,
};
use backend_service::{BackendService, StandaloneScanProducerInput};
use control_transport::WorkerTransport;
use control_transport::{BackendLeaseSlot, BackendSlotLease};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use issuance::{encode_issued_frame, IssuancePool, IssuedRx, IssuedTx};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use pool::PagePool;
use protocol::{
    ExecutionFailureCode, WorkerExecutionToBackend, MAX_EXECUTION_FAILURE_DETAIL_LEN,
    RUNTIME_ENVELOPE_HEADER_LEN,
};
use tracing::{debug, info, trace, warn, Level};
use transfer::PageTx;

use crate::guc::host_config;
use crate::logging::init_tracing_file_logger;
use crate::shmem::{
    attach_control_region, attach_issuance_pool, attach_page_pool, attach_runtime_filters,
    attach_runtime_metrics, attach_scan_region, attach_scan_worker_jobs,
};

const POLL_INTERVAL: Duration = Duration::from_millis(5);

pub(crate) fn register_background_worker() {
    BackgroundWorkerBuilder::new("pg_fusion")
        .set_function("worker_main")
        .set_library("pg_fusion")
        .enable_shmem_access(Some(crate::shmem::init_shmem))
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn worker_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    if let Err(err) = run_worker_main() {
        init_tracing_file_logger("/tmp/pg_fusion.log", "warn");
        warn!(
            component = "worker",
            error = %err,
            "pg_fusion worker exited with error"
        );
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn scan_worker_main(arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let job_id = arg.value();
    if let Err(err) = run_scan_worker_main(job_id) {
        init_tracing_file_logger("/tmp/pg_fusion.log", "warn");
        warn!(
            component = "scan_worker",
            job_id,
            error = %err,
            "pg_fusion scan worker exited with error"
        );
    }
}

fn run_scan_worker_main(job_id: usize) -> Result<(), String> {
    let config = host_config().map_err(|err| format!("invalid host configuration: {err}"))?;
    init_tracing_file_logger(&config.log_path, &config.worker_log_filter);
    let jobs = attach_scan_worker_jobs();
    let job = jobs.snapshot(job_id).map_err(|err| err.to_string())?;
    BackgroundWorker::connect_worker_to_spi_by_oid(
        Some(job.db_oid.into()),
        Some(job.user_oid.into()),
    );

    let scan_region = attach_scan_region();
    let page_pool = attach_page_pool();
    let issuance_pool = attach_issuance_pool();
    let metrics = attach_runtime_metrics();
    let runtime_filters = attach_runtime_filters();
    let scan_lease = BackendSlotLease::acquire(&scan_region).map_err(|err| err.to_string())?;
    let peer = scan_lease.backend_lease_slot();
    jobs.publish_ready(job_id, peer)
        .map_err(|err| err.to_string())?;
    jobs.mark_running(job_id).map_err(|err| err.to_string())?;

    let mut backend_config = config.backend_service_config();
    backend_config.metrics = metrics;
    backend_config.runtime_filters = runtime_filters;
    let run_result = BackgroundWorker::transaction(|| {
        BackendService::run_standalone_scan_producer(StandaloneScanProducerInput {
            descriptor: job.descriptor,
            session_epoch: job.session_epoch,
            scan_id: job.scan_id,
            producer_id: job.producer_id,
            producer_count: job.producer_count,
            scan_lease,
            scan_tx: IssuedTx::new(transfer::PageTx::new(page_pool), issuance_pool),
            config: backend_config,
        })
    });

    match run_result {
        Ok(()) => {
            jobs.mark_done(job_id).map_err(|err| err.to_string())?;
            Ok(())
        }
        Err(err) => {
            let message = err.to_string();
            let _ = jobs.mark_failed(job_id, &message);
            Err(message)
        }
    }
}

fn run_worker_main() -> Result<(), WorkerRuntimeError> {
    let config = host_config().map_err(|err| {
        WorkerRuntimeError::ProtocolViolation(format!("invalid host configuration: {err}"))
    })?;
    init_tracing_file_logger(&config.log_path, &config.worker_log_filter);
    info!(
        component = "worker",
        worker_pid = std::process::id(),
        control_slots = config.control_slot_count,
        scan_slots = config.scan_slot_count,
        control_b2w = config.control_backend_to_worker_capacity,
        control_w2b = config.control_worker_to_backend_capacity,
        scan_b2w = config.scan_backend_to_worker_capacity,
        scan_w2b = config.scan_worker_to_backend_capacity,
        "pg_fusion worker starting"
    );
    let control_region = attach_control_region();
    let scan_region = attach_scan_region();
    let page_pool = attach_page_pool();
    let issuance_pool = attach_issuance_pool();
    let metrics = attach_runtime_metrics();
    let runtime_filters = attach_runtime_filters();

    let spill_cluster_id = worker_spill_cluster_id();
    let mut worker_config = config.worker_runtime_config();
    worker_config.spill = worker_config
        .spill
        .with_cluster_namespace(&spill_cluster_id);
    worker_config.metrics = metrics;
    worker_config.runtime_filter_pool = runtime_filters;
    let scan_transport = WorkerTransport::attach(&scan_region)?;
    let worker_pid = std::process::id() as i32;
    debug!(
        component = "worker",
        worker_pid, "attached dedicated scan transport region"
    );
    let scan_generation = scan_transport.activate_generation(worker_pid)?;
    debug!(
        component = "worker",
        worker_pid, scan_generation, "activated dedicated scan transport generation"
    );
    let run_result = (|| -> Result<(), WorkerRuntimeError> {
        let mut transport = TransportWorkerRuntime::attach(&control_region, &worker_config)?;
        debug!(
            component = "worker",
            worker_pid, "attached primary control transport region"
        );
        let control_generation = transport.activate_generation(worker_pid)?;
        debug!(
            component = "worker",
            worker_pid, control_generation, "activated primary control transport generation"
        );

        let control_result = (|| -> Result<(), WorkerRuntimeError> {
            let mut spill_runtime = WorkerSpillRuntime::new(
                worker_config.spill.clone(),
                worker_pid,
                control_generation,
            )?;
            if let (Some(memory_limit_bytes), Some(active_dir)) = (
                spill_runtime.config().memory_limit_bytes,
                spill_runtime.active_dir(),
            ) {
                info!(
                    component = "worker",
                    worker_pid,
                    control_generation,
                    memory_limit_bytes,
                    spill_cluster = %spill_cluster_id,
                    spill_dir = %active_dir.display(),
                    "enabled DataFusion worker spill"
                );
            }

            let scan_source = Arc::new(TransportScanBatchSource::new_with_metrics(
                scan_region,
                config.scan_backend_to_worker_capacity,
                Arc::new(SharedScanIngress {
                    page_pool,
                    issuance_pool,
                }),
                metrics,
            )?);
            let mut runtime = WorkerRuntimeCore::new(worker_config, scan_source);
            let mut plan_rx: Option<IssuedRx> = None;
            let df_runtime_plan = resolve_datafusion_runtime_plan(config.worker_threads);
            let df_runtime = build_datafusion_runtime(df_runtime_plan)?;
            info!(
                component = "worker",
                requested_worker_threads = ?df_runtime_plan.requested_worker_threads,
                datafusion_worker_threads = df_runtime_plan.worker_threads,
                datafusion_runtime = df_runtime_plan.mode.as_str(),
                "configured DataFusion Tokio runtime"
            );
            debug!(component = "worker", "worker entering main poll loop");

            while BackgroundWorker::wait_latch(Some(POLL_INTERVAL)) {
                if let Some(peer) = runtime.active_peer() {
                    trace!(
                        component = "worker",
                        peer = ?peer,
                        state = ?runtime.state(),
                        "worker probing active backend peer"
                    );
                    let steps = recv_backend_peer_steps(
                        &mut transport,
                        &mut runtime,
                        peer,
                        page_pool,
                        issuance_pool,
                        &mut plan_rx,
                    )?;
                    handle_steps(
                        &mut transport,
                        &mut runtime,
                        &df_runtime,
                        &mut spill_runtime,
                        &config,
                        page_pool,
                        issuance_pool,
                        &mut plan_rx,
                        metrics,
                        steps,
                    )?;
                }

                let mut ready_cursor = 0;
                while let Some(peer) = transport.next_ready_backend_lease(&mut ready_cursor) {
                    if let Some(active_peer) = runtime.active_peer() {
                        if active_peer != peer {
                            trace!(
                                component = "worker",
                                peer = ?active_peer,
                                incoming_peer = ?peer,
                                state = ?runtime.state(),
                                "worker probing active backend peer before non-active peer"
                            );
                            let steps = recv_backend_peer_steps(
                                &mut transport,
                                &mut runtime,
                                active_peer,
                                page_pool,
                                issuance_pool,
                                &mut plan_rx,
                            )?;
                            handle_steps(
                                &mut transport,
                                &mut runtime,
                                &df_runtime,
                                &mut spill_runtime,
                                &config,
                                page_pool,
                                issuance_pool,
                                &mut plan_rx,
                                metrics,
                                steps,
                            )?;
                        }
                    }
                    if tracing::enabled!(Level::TRACE) {
                        trace!(
                            component = "worker",
                            peer = ?peer,
                            state = ?runtime.state(),
                            "worker polling ready backend peer"
                        );
                    }
                    let steps = recv_backend_peer_steps(
                        &mut transport,
                        &mut runtime,
                        peer,
                        page_pool,
                        issuance_pool,
                        &mut plan_rx,
                    )?;
                    handle_steps(
                        &mut transport,
                        &mut runtime,
                        &df_runtime,
                        &mut spill_runtime,
                        &config,
                        page_pool,
                        issuance_pool,
                        &mut plan_rx,
                        metrics,
                        steps,
                    )?;
                }
            }

            Ok(())
        })();

        finish_with_deactivation(
            control_result,
            transport.deactivate_generation(),
            "primary control transport",
        )
    })();

    finish_with_deactivation(
        run_result,
        scan_transport.deactivate_generation().map_err(Into::into),
        "dedicated scan transport",
    )?;
    info!(component = "worker", "worker stopped cleanly");
    Ok(())
}

fn recv_backend_peer_steps(
    transport: &mut TransportWorkerRuntime,
    runtime: &mut WorkerRuntimeCore,
    peer: BackendLeaseSlot,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    plan_rx: &mut Option<IssuedRx>,
) -> Result<VecDeque<WorkerRuntimeStep>, WorkerRuntimeError> {
    let mut steps = VecDeque::new();
    let recv_result = transport.recv_peer_frames(peer, |bytes| {
        let decoded = WorkerRuntimeCore::decode_inbound(bytes)?;
        let step = match decoded {
            DecodedInbound::Control(message) => runtime.accept_backend_control(peer, message)?,
            DecodedInbound::IssuedFrame(frame) => {
                let rx = plan_rx.as_ref().ok_or_else(|| {
                    WorkerRuntimeError::ProtocolViolation(
                        "received a plan frame before opening plan ingress".into(),
                    )
                })?;
                runtime.accept_issued_plan_frame(peer, rx, &frame)?
            }
        };
        if matches!(step, WorkerRuntimeStep::PlanOpened { .. }) {
            *plan_rx = Some(IssuedRx::new(
                transfer::PageRx::new(page_pool),
                issuance_pool,
            ));
        }
        steps.push_back(step);
        Ok(())
    });

    match recv_result {
        Ok(()) => Ok(steps),
        Err(err) if is_detached_backend_peer_error(&err) => {
            warn!(
                component = "worker",
                peer = ?peer,
                error = %err,
                "worker observed detached backend peer while receiving frames"
            );
            steps.clear();
            if let Some(step) = runtime.cancel_detached_backend_peer(peer)? {
                steps.push_back(step);
            }
            Ok(steps)
        }
        Err(err) => Err(err),
    }
}

fn finish_with_deactivation(
    result: Result<(), WorkerRuntimeError>,
    deactivate: Result<u64, WorkerRuntimeError>,
    transport: &'static str,
) -> Result<(), WorkerRuntimeError> {
    match (result, deactivate) {
        (Ok(()), Ok(_)) => Ok(()),
        (Ok(()), Err(err)) => Err(err),
        (Err(err), Ok(_)) => Err(err),
        (Err(err), Err(deactivate_err)) => {
            warn!(
                component = "worker",
                transport,
                error = %deactivate_err,
                "failed to deactivate worker transport after error"
            );
            Err(err)
        }
    }
}

fn is_detached_backend_peer_error(err: &WorkerRuntimeError) -> bool {
    match err {
        WorkerRuntimeError::SlotAccess(err) => is_detached_slot_access_error(err),
        WorkerRuntimeError::WorkerTx(control_transport::WorkerTxError::Slot(err))
        | WorkerRuntimeError::WorkerRx(control_transport::WorkerRxError::Slot(err)) => {
            is_detached_slot_access_error(err)
        }
        _ => false,
    }
}

fn is_detached_slot_access_error(err: &control_transport::SlotAccessError) -> bool {
    matches!(
        err,
        control_transport::SlotAccessError::Released { .. }
            | control_transport::SlotAccessError::StaleLeaseEpoch { .. }
    )
}

fn worker_spill_cluster_id() -> String {
    let data_dir = postgres_data_dir_path().unwrap_or_else(|| PathBuf::from("unknown"));
    let normalized = std::fs::canonicalize(&data_dir).unwrap_or(data_dir);
    format!("{:016x}", fnv1a64(normalized.to_string_lossy().as_bytes()))
}

fn postgres_data_dir_path() -> Option<PathBuf> {
    let data_dir = unsafe { pgrx::pg_sys::DataDir };
    if data_dir.is_null() {
        return None;
    }
    let path = unsafe { CStr::from_ptr(data_dir) }
        .to_string_lossy()
        .into_owned();
    if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(path))
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}

fn worker_execution_failure_detail(
    err: &WorkerRuntimeError,
    control_frame_capacity: usize,
) -> Option<String> {
    let max_len = control_frame_capacity
        .saturating_sub(worker_failure_detail_fixed_overhead())
        .min(MAX_EXECUTION_FAILURE_DETAIL_LEN);
    if max_len == 0 {
        return None;
    }
    Some(truncate_utf8(&err.to_string(), max_len))
}

fn worker_failure_detail_fixed_overhead() -> usize {
    RUNTIME_ENVELOPE_HEADER_LEN + 32
}

fn truncate_utf8(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_string();
    }

    const SUFFIX: &str = "... [truncated]";
    if max_len <= SUFFIX.len() {
        return value
            .char_indices()
            .map(|(idx, ch)| (idx, ch.len_utf8()))
            .take_while(|(idx, len)| idx + len <= max_len)
            .map(|(idx, len)| &value[idx..idx + len])
            .collect();
    }

    let prefix_len = max_len - SUFFIX.len();
    let mut end = 0;
    for (idx, ch) in value.char_indices() {
        let next = idx + ch.len_utf8();
        if next > prefix_len {
            break;
        }
        end = next;
    }
    format!("{}{}", &value[..end], SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deactivation_helper_preserves_primary_failure() {
        let err = finish_with_deactivation(
            Err(protocol_error("startup failed")),
            Err(protocol_error("deactivation failed")),
            "test transport",
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "runtime protocol violation: startup failed"
        );
    }

    #[test]
    fn deactivation_helper_reports_cleanup_failure_on_clean_run() {
        let err = finish_with_deactivation(
            Ok(()),
            Err(protocol_error("deactivation failed")),
            "test transport",
        )
        .unwrap_err();

        assert_eq!(
            err.to_string(),
            "runtime protocol violation: deactivation failed"
        );
    }

    #[test]
    fn detached_backend_peer_error_classifies_released_and_stale_lease() {
        let released = WorkerRuntimeError::WorkerTx(control_transport::WorkerTxError::Slot(
            control_transport::SlotAccessError::Released {
                slot_id: 7,
                claimed_generation: 1,
            },
        ));
        let stale_lease = WorkerRuntimeError::WorkerRx(control_transport::WorkerRxError::Slot(
            control_transport::SlotAccessError::StaleLeaseEpoch {
                slot_id: 7,
                claimed_generation: 1,
                claimed_lease_epoch: 2,
                current_lease_epoch: 3,
            },
        ));

        assert!(is_detached_backend_peer_error(&released));
        assert!(is_detached_backend_peer_error(&stale_lease));
    }

    #[test]
    fn detached_backend_peer_error_keeps_unrelated_errors_fatal() {
        let worker_offline =
            WorkerRuntimeError::SlotAccess(control_transport::SlotAccessError::WorkerOffline);
        let ring_full = WorkerRuntimeError::WorkerTx(control_transport::WorkerTxError::Ring(
            control_transport::TxError::Full {
                required: 16,
                available: 8,
            },
        ));

        assert!(!is_detached_backend_peer_error(&worker_offline));
        assert!(!is_detached_backend_peer_error(&ring_full));
    }

    #[test]
    fn datafusion_runtime_plan_uses_explicit_current_thread() {
        let plan = resolve_datafusion_runtime_plan_with(Some(1), || 8);

        assert_eq!(plan.requested_worker_threads, Some(1));
        assert_eq!(plan.worker_threads, 1);
        assert_eq!(plan.mode, DataFusionRuntimeMode::CurrentThread);
        assert_eq!(plan.mode.as_str(), "current-thread");
    }

    #[test]
    fn datafusion_runtime_plan_uses_explicit_multi_thread() {
        let plan = resolve_datafusion_runtime_plan_with(Some(4), || 1);

        assert_eq!(plan.requested_worker_threads, Some(4));
        assert_eq!(plan.worker_threads, 4);
        assert_eq!(plan.mode, DataFusionRuntimeMode::MultiThread);
        assert_eq!(plan.mode.as_str(), "multi-thread");
    }

    #[test]
    fn datafusion_runtime_plan_uses_auto_thread_count() {
        let plan = resolve_datafusion_runtime_plan_with(None, || 6);

        assert_eq!(plan.requested_worker_threads, None);
        assert_eq!(plan.worker_threads, 6);
        assert_eq!(plan.mode, DataFusionRuntimeMode::MultiThread);
    }

    #[test]
    fn datafusion_runtime_plan_clamps_auto_to_one_thread() {
        let plan = resolve_datafusion_runtime_plan_with(None, || 0);

        assert_eq!(plan.worker_threads, 1);
        assert_eq!(plan.mode, DataFusionRuntimeMode::CurrentThread);
    }

    fn protocol_error(message: &str) -> WorkerRuntimeError {
        WorkerRuntimeError::ProtocolViolation(message.into())
    }

    #[test]
    fn worker_failure_detail_truncates_to_frame_budget() {
        let err = protocol_error("abcdefghijklmnopqrstuvwxyz");
        let detail =
            worker_execution_failure_detail(&err, worker_failure_detail_fixed_overhead() + 20)
                .unwrap();

        assert!(detail.len() <= 20);
        assert!(detail.ends_with("... [truncated]"));
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_steps(
    transport: &mut TransportWorkerRuntime,
    runtime: &mut WorkerRuntimeCore,
    df_runtime: &tokio::runtime::Runtime,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    plan_rx: &mut Option<IssuedRx>,
    metrics: RuntimeMetrics,
    mut steps: VecDeque<WorkerRuntimeStep>,
) -> Result<(), WorkerRuntimeError> {
    while let Some(step) = steps.pop_front() {
        match step {
            WorkerRuntimeStep::Idle
            | WorkerRuntimeStep::StaleControlIgnored { .. }
            | WorkerRuntimeStep::PlanFrameAccepted { .. }
            | WorkerRuntimeStep::PlanningResultIgnored { .. } => {}
            WorkerRuntimeStep::PlanOpened {
                session_epoch,
                plan_id,
            } => {
                debug!(
                    component = "worker",
                    session_epoch, plan_id, "worker opened logical plan ingress"
                );
            }
            WorkerRuntimeStep::PlanningStarted(pending) => {
                let peer = pending.peer();
                let flow = pending.flow();
                debug!(
                    component = "worker",
                    peer = ?peer,
                    flow = ?flow,
                    "worker starting physical planning"
                );
                let plan_start = metrics.now_ns();
                let result = df_runtime.block_on(pending.plan());
                metrics.add_elapsed(MetricId::WorkerPhysicalPlanNs, plan_start);
                metrics.increment(MetricId::WorkerPhysicalPlanTotal);
                steps.push_back(runtime.finish_physical_planning(peer, flow, result)?);
            }
            WorkerRuntimeStep::PhysicalPlanReady(result) => {
                let peer = runtime.active_peer().expect("peer");
                let worker_start = metrics.now_ns();
                info!(
                    component = "worker",
                    session_epoch = result.session_epoch,
                    peer = ?peer,
                    "worker received physical plan and is starting execution"
                );
                let execution_result: Result<(), WorkerRuntimeError> = runtime
                    .take_physical_plan()
                    .ok_or(WorkerRuntimeError::MissingPhysicalPlan)
                    .and_then(|plan| {
                        df_runtime.block_on(execute_physical_plan(
                            transport,
                            spill_runtime,
                            config,
                            page_pool,
                            issuance_pool,
                            metrics,
                            peer,
                            result.session_epoch,
                            plan,
                        ))
                    });

                match execution_result {
                    Ok(()) => {
                        info!(
                            component = "worker",
                            session_epoch = result.session_epoch,
                            peer = ?peer,
                            "worker finished execution successfully and is sending CompleteExecution"
                        );
                        if let Err(err) = transport.send_peer_message(
                            peer,
                            WorkerExecutionToBackend::CompleteExecution {
                                session_epoch: result.session_epoch,
                            },
                        ) {
                            if is_detached_backend_peer_error(&err) {
                                warn!(
                                    component = "worker",
                                    session_epoch = result.session_epoch,
                                    peer = ?peer,
                                    error = %err,
                                    "worker could not send CompleteExecution because backend peer detached"
                                );
                            } else {
                                return Err(err);
                            }
                        }
                        metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
                        steps.push_back(runtime.mark_execution_complete()?);
                    }
                    Err(err) => {
                        let detail = worker_execution_failure_detail(
                            &err,
                            config.control_backend_to_worker_capacity,
                        );
                        warn!(
                            component = "worker",
                            session_epoch = result.session_epoch,
                            peer = ?peer,
                            error = %err,
                            "worker execution failed locally; sending FailExecution"
                        );
                        if let Err(send_err) = transport.send_peer_message(
                            peer,
                            WorkerExecutionToBackend::FailExecution {
                                session_epoch: result.session_epoch,
                                code: ExecutionFailureCode::Internal,
                                detail,
                            },
                        ) {
                            if is_detached_backend_peer_error(&send_err) {
                                warn!(
                                    component = "worker",
                                    session_epoch = result.session_epoch,
                                    peer = ?peer,
                                    error = %send_err,
                                    "worker could not send FailExecution because backend peer detached"
                                );
                            } else {
                                return Err(send_err);
                            }
                        }
                        metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
                        steps.push_back(
                            runtime.fail_execution_locally(ExecutionFailureCode::Internal, None)?,
                        );
                    }
                }
            }
            WorkerRuntimeStep::ExecutionCancelled { session_epoch } => {
                info!(
                    component = "worker",
                    session_epoch, "worker observed execution cancel"
                );
                plan_rx.take();
                if runtime.state() == ::worker::fsm::WorkerExecutionState::Terminal {
                    runtime.cleanup()?;
                    info!(
                        component = "worker",
                        session_epoch, "worker cleaned up terminal execution after cancel"
                    );
                }
            }
            WorkerRuntimeStep::ExecutionFailed {
                session_epoch,
                code,
                detail,
            } => {
                warn!(
                    component = "worker",
                    session_epoch,
                    code = ?code,
                    detail = ?detail,
                    "worker observed execution failure transition"
                );
                plan_rx.take();
                if runtime.state() == ::worker::fsm::WorkerExecutionState::Terminal {
                    runtime.cleanup()?;
                    info!(
                        component = "worker",
                        session_epoch, "worker cleaned up terminal execution after failure"
                    );
                }
            }
            WorkerRuntimeStep::ExecutionCompleted { session_epoch } => {
                info!(
                    component = "worker",
                    session_epoch, "worker observed execution complete transition"
                );
                plan_rx.take();
                if runtime.state() == ::worker::fsm::WorkerExecutionState::Terminal {
                    runtime.cleanup()?;
                    info!(
                        component = "worker",
                        session_epoch, "worker cleaned up terminal execution after completion"
                    );
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DataFusionRuntimePlan {
    requested_worker_threads: Option<usize>,
    worker_threads: usize,
    mode: DataFusionRuntimeMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DataFusionRuntimeMode {
    CurrentThread,
    MultiThread,
}

impl DataFusionRuntimeMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::CurrentThread => "current-thread",
            Self::MultiThread => "multi-thread",
        }
    }
}

fn resolve_datafusion_runtime_plan(worker_threads: Option<usize>) -> DataFusionRuntimePlan {
    resolve_datafusion_runtime_plan_with(worker_threads, default_datafusion_worker_threads)
}

fn resolve_datafusion_runtime_plan_with(
    worker_threads: Option<usize>,
    default_worker_threads: impl FnOnce() -> usize,
) -> DataFusionRuntimePlan {
    let effective_worker_threads = worker_threads.unwrap_or_else(default_worker_threads).max(1);
    let mode = if effective_worker_threads == 1 {
        DataFusionRuntimeMode::CurrentThread
    } else {
        DataFusionRuntimeMode::MultiThread
    };

    DataFusionRuntimePlan {
        requested_worker_threads: worker_threads,
        worker_threads: effective_worker_threads,
        mode,
    }
}

fn default_datafusion_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(1)
}

fn build_datafusion_runtime(
    plan: DataFusionRuntimePlan,
) -> Result<tokio::runtime::Runtime, WorkerRuntimeError> {
    let result = match plan.mode {
        DataFusionRuntimeMode::CurrentThread => tokio::runtime::Builder::new_current_thread()
            .thread_name("pg_fusion-df")
            .build(),
        DataFusionRuntimeMode::MultiThread => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(plan.worker_threads)
            .thread_name("pg_fusion-df")
            .build(),
    };

    result.map_err(|err| {
        WorkerRuntimeError::ProtocolViolation(format!(
            "failed to build DataFusion Tokio runtime: {err}"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_physical_plan(
    transport: &mut TransportWorkerRuntime,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    metrics: RuntimeMetrics,
    peer: BackendLeaseSlot,
    session_epoch: u64,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(), WorkerRuntimeError> {
    let spill_dir = spill_runtime.execution_dir(peer, session_epoch)?;
    let spill_dir_created = spill_dir.path().is_some();
    if spill_dir_created {
        metrics.increment(MetricId::WorkerSpillDirsCreatedTotal);
    }
    let task_ctx = match spill_runtime.task_context(&spill_dir) {
        Ok(task_ctx) => task_ctx,
        Err(err) => {
            let cleanup_result = cleanup_execution_spill_dir(
                spill_dir,
                spill_dir_created,
                metrics,
                peer,
                session_epoch,
            );
            if let Err(cleanup_err) = cleanup_result {
                warn!(
                    component = "worker",
                    session_epoch,
                    peer = ?peer,
                    error = %cleanup_err,
                    "worker failed to clean execution spill directory after task context failure"
                );
            }
            return Err(err);
        }
    };
    let execution_result: Result<(), WorkerRuntimeError> = async {
        let stream = execute_stream(Arc::clone(&plan), Arc::clone(&task_ctx))?;
        let page_tx = PageTx::new(page_pool);
        let payload_capacity = u32::try_from(page_tx.payload_capacity()).map_err(|_| {
            WorkerRuntimeError::ProtocolViolation("result payload capacity exceeds u32".into())
        })?;
        let mut producer = ResultPageEmitter::new(
            stream,
            IssuedTx::new(page_tx, issuance_pool),
            payload_capacity,
            ResultPageProducerConfig {
                estimator: row_estimator::EstimatorConfig {
                    initial_tail_bytes_per_row: config.estimator_initial_tail_bytes_per_row,
                },
                metrics,
                ..ResultPageProducerConfig::default()
            },
        )?;

        loop {
            match producer.next_step_async().await? {
                Some(ResultPageStep::OutboundPage(outbound)) => {
                    trace!(
                        component = "worker",
                        session_epoch,
                        peer = ?peer,
                        "worker produced one result page"
                    );
                    let descriptor = outbound.descriptor();
                    let payload_len = outbound.payload_len();
                    let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
                        WorkerRuntimeError::ProtocolViolation(format!(
                            "failed to encode result page frame: {err}"
                        ))
                    })?;
                    transport.send_peer_bytes(peer, &frame)?;
                    metrics.stamp_page(PageDirection::WorkerToBackend, descriptor, payload_len);
                    metrics.increment(MetricId::WorkerResultPagesTotal);
                    metrics.add(MetricId::WorkerResultBytesSentTotal, payload_len as u64);
                    outbound.mark_sent();
                }
                Some(ResultPageStep::CloseFrame(frame)) => {
                    debug!(
                        component = "worker",
                        session_epoch,
                        peer = ?peer,
                        "worker produced terminal result close frame"
                    );
                    let frame = encode_issued_frame(frame).map_err(|err| {
                        WorkerRuntimeError::ProtocolViolation(format!(
                            "failed to encode result close frame: {err}"
                        ))
                    })?;
                    transport.send_peer_bytes(peer, &frame)?;
                }
                None => break,
            }
        }
        Ok(())
    }
    .await;
    record_datafusion_spill_metrics(plan.as_ref(), metrics);
    record_datafusion_spill_leaks(task_ctx.as_ref(), metrics);

    let cleanup_result =
        cleanup_execution_spill_dir(spill_dir, spill_dir_created, metrics, peer, session_epoch);

    execution_result?;
    cleanup_result?;

    Ok(())
}

fn cleanup_execution_spill_dir(
    spill_dir: ExecutionSpillDir,
    spill_dir_created: bool,
    metrics: RuntimeMetrics,
    peer: BackendLeaseSlot,
    session_epoch: u64,
) -> Result<(), WorkerRuntimeError> {
    let cleanup_result = spill_dir.cleanup();
    if spill_dir_created {
        match &cleanup_result {
            Ok(()) => metrics.increment(MetricId::WorkerSpillDirsRemovedTotal),
            Err(err) => {
                metrics.increment(MetricId::WorkerSpillCleanupErrorsTotal);
                warn!(
                    component = "worker",
                    session_epoch,
                    peer = ?peer,
                    error = %err,
                    "worker failed to clean execution spill directory"
                );
            }
        }
    }
    cleanup_result
}

#[derive(Clone, Copy)]
struct SharedScanIngress {
    page_pool: PagePool,
    issuance_pool: IssuancePool,
}

impl std::fmt::Debug for SharedScanIngress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedScanIngress { .. }")
    }
}

impl ScanIngressProvider for SharedScanIngress {
    fn issued_rx(
        &self,
        _session_epoch: u64,
        _scan_id: u64,
        _producer_id: u16,
    ) -> Result<IssuedRx, WorkerRuntimeError> {
        Ok(IssuedRx::new(
            transfer::PageRx::new(self.page_pool),
            self.issuance_pool,
        ))
    }
}
