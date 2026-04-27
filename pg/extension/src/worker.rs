use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use backend_service::{BackendService, StandaloneScanProducerInput};
use control_transport::BackendSlotLease;
use control_transport::WorkerTransport;
use datafusion_execution::TaskContext;
use futures::executor::block_on;
use issuance::{encode_issued_frame, IssuancePool, IssuedRx, IssuedTx};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use pool::PagePool;
use runtime_metrics::{MetricId, PageDirection, RuntimeMetrics};
use runtime_protocol::{ExecutionFailureCode, WorkerExecutionToBackend};
use tracing::{debug, info, trace, warn, Level};
use transfer::PageTx;
use worker_runtime::{
    DecodedInbound, ResultPageProducer, ResultPageProducerConfig, ResultPageStep,
    ScanIngressProvider, TransportScanBatchSource, TransportWorkerRuntime, WorkerRuntimeCore,
    WorkerRuntimeError, WorkerRuntimeStep,
};

use crate::guc::host_config;
use crate::logging::init_tracing_file_logger;
use crate::shmem::{
    attach_control_region, attach_issuance_pool, attach_page_pool, attach_runtime_metrics,
    attach_scan_region, attach_scan_worker_jobs,
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
    let job_id = arg.value() as usize;
    if let Err(err) = run_scan_worker_main(job_id) {
        let _ = init_tracing_file_logger("/tmp/pg_fusion.log", "warn");
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
    let scan_lease = BackendSlotLease::acquire(&scan_region).map_err(|err| err.to_string())?;
    let peer = scan_lease.backend_lease_slot();
    jobs.publish_ready(job_id, peer)
        .map_err(|err| err.to_string())?;
    jobs.mark_running(job_id).map_err(|err| err.to_string())?;

    let mut backend_config = config.backend_service_config();
    backend_config.metrics = metrics;
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

    let worker_config = config.worker_runtime_config();
    let scan_transport = WorkerTransport::attach(&scan_region)?;
    let worker_pid = std::process::id() as i32;
    debug!(
        component = "worker",
        worker_pid, "attached dedicated scan transport region"
    );
    scan_transport.activate_generation(worker_pid)?;
    debug!(
        component = "worker",
        worker_pid, "activated dedicated scan transport generation"
    );
    let mut transport = TransportWorkerRuntime::attach(&control_region, &worker_config)?;
    debug!(
        component = "worker",
        worker_pid, "attached primary control transport region"
    );
    transport.activate_generation(worker_pid)?;
    debug!(
        component = "worker",
        worker_pid, "activated primary control transport generation"
    );

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
    debug!(component = "worker", "worker entering main poll loop");

    while BackgroundWorker::wait_latch(Some(POLL_INTERVAL)) {
        let mut ready_cursor = 0;
        while let Some(peer) = transport.next_ready_backend_lease(&mut ready_cursor) {
            if tracing::enabled!(Level::TRACE) {
                trace!(
                    component = "worker",
                    peer = ?peer,
                    state = ?runtime.state(),
                    "worker polling ready backend peer"
                );
            }
            let mut steps = VecDeque::new();
            transport.recv_peer_frames(peer, |bytes| {
                let decoded = WorkerRuntimeCore::decode_inbound(bytes)?;
                let step = match decoded {
                    DecodedInbound::Control(message) => {
                        runtime.accept_backend_control(peer, message)?
                    }
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
                    plan_rx = Some(IssuedRx::new(
                        transfer::PageRx::new(page_pool),
                        issuance_pool,
                    ));
                }
                steps.push_back(step);
                Ok(())
            })?;

            handle_steps(
                &mut transport,
                &mut runtime,
                &config,
                page_pool,
                issuance_pool,
                &mut plan_rx,
                metrics,
                steps,
            )?;
        }
    }

    transport.deactivate_generation()?;
    scan_transport.deactivate_generation()?;
    info!(component = "worker", "worker stopped cleanly");
    Ok(())
}

fn handle_steps(
    transport: &mut TransportWorkerRuntime,
    runtime: &mut WorkerRuntimeCore,
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
                let result = block_on(pending.plan());
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
                let execution_result: Result<(), WorkerRuntimeError> = (|| {
                    let plan = runtime
                        .take_physical_plan()
                        .ok_or(WorkerRuntimeError::MissingPhysicalPlan)?;
                    let stream = plan.execute(0, Arc::new(TaskContext::default()))?;
                    let page_tx = PageTx::new(page_pool);
                    let payload_capacity =
                        u32::try_from(page_tx.payload_capacity()).map_err(|_| {
                            WorkerRuntimeError::ProtocolViolation(
                                "result payload capacity exceeds u32".into(),
                            )
                        })?;
                    let mut producer = ResultPageProducer::new(
                        stream,
                        IssuedTx::new(page_tx, issuance_pool),
                        payload_capacity,
                        ResultPageProducerConfig {
                            estimator: row_estimator::EstimatorConfig {
                                initial_tail_bytes_per_row: config
                                    .estimator_initial_tail_bytes_per_row,
                            },
                            metrics,
                            ..ResultPageProducerConfig::default()
                        },
                    )?;
                    loop {
                        match producer.next_step()? {
                            Some(ResultPageStep::OutboundPage(outbound)) => {
                                trace!(
                                    component = "worker",
                                    session_epoch = result.session_epoch,
                                    peer = ?peer,
                                    "worker produced one result page"
                                );
                                let descriptor = outbound.descriptor();
                                let payload_len = outbound.payload_len();
                                let frame =
                                    encode_issued_frame(outbound.frame()).map_err(|err| {
                                        WorkerRuntimeError::ProtocolViolation(format!(
                                            "failed to encode result page frame: {err}"
                                        ))
                                    })?;
                                transport.send_peer_bytes(peer, &frame)?;
                                metrics.stamp_page(
                                    PageDirection::WorkerToBackend,
                                    descriptor,
                                    payload_len,
                                );
                                metrics.increment(MetricId::WorkerResultPagesTotal);
                                metrics
                                    .add(MetricId::WorkerResultBytesSentTotal, payload_len as u64);
                                outbound.mark_sent();
                            }
                            Some(ResultPageStep::CloseFrame(frame)) => {
                                debug!(
                                    component = "worker",
                                    session_epoch = result.session_epoch,
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
                })();

                match execution_result {
                    Ok(()) => {
                        info!(
                            component = "worker",
                            session_epoch = result.session_epoch,
                            peer = ?peer,
                            "worker finished execution successfully and is sending CompleteExecution"
                        );
                        transport.send_peer_message(
                            peer,
                            WorkerExecutionToBackend::CompleteExecution {
                                session_epoch: result.session_epoch,
                            },
                        )?;
                        metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
                        steps.push_back(runtime.mark_execution_complete()?);
                    }
                    Err(err) => {
                        warn!(
                            component = "worker",
                            session_epoch = result.session_epoch,
                            peer = ?peer,
                            error = %err,
                            "worker execution failed locally; sending FailExecution"
                        );
                        transport.send_peer_message(
                            peer,
                            WorkerExecutionToBackend::FailExecution {
                                session_epoch: result.session_epoch,
                                code: ExecutionFailureCode::Internal,
                                detail: None,
                            },
                        )?;
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
                if runtime.state() == worker_runtime::fsm::WorkerExecutionState::Terminal {
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
                if runtime.state() == worker_runtime::fsm::WorkerExecutionState::Terminal {
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
                if runtime.state() == worker_runtime::fsm::WorkerExecutionState::Terminal {
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
