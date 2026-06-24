use std::collections::{HashMap, VecDeque};
use std::ffi::CStr;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::c_long;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ::metrics::{MetricId, PageDirection, RuntimeMetrics};
use ::worker::{
    record_datafusion_spill_leaks, record_datafusion_spill_metrics, DecodedInbound,
    ExecutionSpillDir, ResultPageEmitter, ResultPageProducerConfig, ResultPageStep,
    ScanIngressProvider, TransportScanBatchSource, TransportWorkerRuntime, WorkerRuntimeConfig,
    WorkerRuntimeCore, WorkerRuntimeError, WorkerRuntimeStep, WorkerSpillRuntime,
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
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn, Level};
use transfer::PageTx;

use crate::guc::host_config;
use crate::logging::init_tracing_file_logger;
use crate::shmem::{
    attach_control_region, attach_issuance_pool, attach_page_pool, attach_runtime_filters,
    attach_runtime_metrics, attach_scan_region, attach_scan_worker_jobs,
};

const POLL_INTERVAL: Duration = Duration::from_millis(5);

type WorkerTaskSender = mpsc::Sender<WorkerTaskEvent>;
type WorkerTaskReceiver = mpsc::Receiver<WorkerTaskEvent>;
type WorkerTaskAck = oneshot::Sender<Result<(), WorkerRuntimeError>>;

struct ActiveWorkerExecution {
    runtime: WorkerRuntimeCore,
    plan_rx: Option<IssuedRx>,
    task: Option<JoinHandle<()>>,
    worker_start_ns: Option<u64>,
}

impl ActiveWorkerExecution {
    fn new(config: WorkerRuntimeConfig, scan_source: Arc<dyn ::worker::ScanBatchSource>) -> Self {
        Self {
            runtime: WorkerRuntimeCore::new(config, scan_source),
            plan_rx: None,
            task: None,
            worker_start_ns: None,
        }
    }

    fn abort_task(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

enum WorkerTaskEvent {
    PhysicalPlanningFinished {
        peer: BackendLeaseSlot,
        flow: plan_flow::FlowId,
        result: Result<Arc<dyn ExecutionPlan>, WorkerRuntimeError>,
    },
    ResultPage {
        peer: BackendLeaseSlot,
        session_epoch: u64,
        outbound: issuance::IssuedOutboundPage,
        ack: WorkerTaskAck,
    },
    ResultClose {
        peer: BackendLeaseSlot,
        session_epoch: u64,
        frame: issuance::IssuedOwnedFrame,
        ack: WorkerTaskAck,
    },
    ExecutionFinished {
        peer: BackendLeaseSlot,
        session_epoch: u64,
        result: Result<(), WorkerRuntimeError>,
    },
}

struct ExecutionTaskInput {
    task_tx: WorkerTaskNotifier,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    metrics: RuntimeMetrics,
    peer: BackendLeaseSlot,
    session_epoch: u64,
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<datafusion_execution::TaskContext>,
    spill_dir: ExecutionSpillDir,
    spill_dir_created: bool,
    estimator_initial_tail_bytes_per_row: u32,
    #[cfg(feature = "pg_test")]
    test_execution_gate: crate::test_gate::TestExecutionGateHandle,
}

struct WorkerSchedulerWake {
    reader: UnixStream,
    sender: WorkerSchedulerWakeSender,
}

impl WorkerSchedulerWake {
    fn new() -> Result<Self, WorkerRuntimeError> {
        let (reader, writer) = UnixStream::pair().map_err(|err| {
            WorkerRuntimeError::ProtocolViolation(format!(
                "failed to create worker scheduler wake socket: {err}"
            ))
        })?;
        reader.set_nonblocking(true).map_err(|err| {
            WorkerRuntimeError::ProtocolViolation(format!(
                "failed to configure worker scheduler wake reader: {err}"
            ))
        })?;
        writer.set_nonblocking(true).map_err(|err| {
            WorkerRuntimeError::ProtocolViolation(format!(
                "failed to configure worker scheduler wake writer: {err}"
            ))
        })?;
        Ok(Self {
            reader,
            sender: WorkerSchedulerWakeSender {
                writer: Arc::new(writer),
            },
        })
    }

    fn sender(&self) -> WorkerSchedulerWakeSender {
        self.sender.clone()
    }

    fn read_fd(&self) -> RawFd {
        self.reader.as_raw_fd()
    }

    fn drain(&mut self) -> usize {
        let mut buf = [0_u8; 64];
        let mut drained = 0;
        loop {
            match self.reader.read(&mut buf) {
                Ok(0) => return drained,
                Ok(bytes) => drained += bytes,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return drained,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => {
                    warn!(
                        component = "worker",
                        error = %err,
                        "failed to drain worker scheduler wake socket"
                    );
                    return drained;
                }
            }
        }
    }
}

#[derive(Clone)]
struct WorkerSchedulerWakeSender {
    writer: Arc<UnixStream>,
}

impl WorkerSchedulerWakeSender {
    fn wake(&self) -> io::Result<()> {
        let mut writer = self.writer.as_ref();
        loop {
            match writer.write(&[1]) {
                Ok(_) => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
    }
}

#[derive(Clone)]
struct WorkerTaskNotifier {
    tx: WorkerTaskSender,
    wake: WorkerSchedulerWakeSender,
}

impl WorkerTaskNotifier {
    fn new(tx: WorkerTaskSender, wake: WorkerSchedulerWakeSender) -> Self {
        Self { tx, wake }
    }

    async fn send(
        &self,
        event: WorkerTaskEvent,
    ) -> Result<(), mpsc::error::SendError<WorkerTaskEvent>> {
        self.tx.send(event).await?;
        if let Err(err) = self.wake.wake() {
            warn!(
                component = "worker",
                error = %err,
                "failed to wake worker scheduler after task event"
            );
        }
        Ok(())
    }
}

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

            let scan_source: Arc<dyn ::worker::ScanBatchSource> =
                Arc::new(TransportScanBatchSource::new_with_metrics(
                    scan_region,
                    config.scan_backend_to_worker_capacity,
                    Arc::new(SharedScanIngress {
                        page_pool,
                        issuance_pool,
                    }),
                    metrics,
                )?);
            let mut scheduler_wake = WorkerSchedulerWake::new()?;
            let (task_event_tx, mut task_rx) =
                mpsc::channel(config.max_fusion_tasks.saturating_mul(4).max(1));
            let task_tx = WorkerTaskNotifier::new(task_event_tx, scheduler_wake.sender());
            let mut executions: HashMap<BackendLeaseSlot, ActiveWorkerExecution> = HashMap::new();
            #[cfg(feature = "pg_test")]
            let test_execution_gate = crate::test_gate::attach();
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

            while wait_worker_scheduler(&mut scheduler_wake, POLL_INTERVAL) {
                drain_task_events(
                    &mut executions,
                    &mut transport,
                    &df_runtime,
                    &mut spill_runtime,
                    &config,
                    page_pool,
                    issuance_pool,
                    metrics,
                    &mut task_rx,
                    &task_tx,
                    #[cfg(feature = "pg_test")]
                    test_execution_gate,
                )?;

                let active_peers = executions.keys().copied().collect::<Vec<_>>();
                for peer in active_peers {
                    trace!(
                        component = "worker",
                        peer = ?peer,
                        "worker probing active backend peer"
                    );
                    poll_execution_peer(
                        &mut executions,
                        &mut transport,
                        &df_runtime,
                        &mut spill_runtime,
                        &config,
                        &worker_config,
                        &scan_source,
                        &task_tx,
                        #[cfg(feature = "pg_test")]
                        test_execution_gate,
                        peer,
                        page_pool,
                        issuance_pool,
                        metrics,
                    )?;
                }

                let mut ready_cursor = 0;
                while let Some(peer) = transport.next_ready_backend_lease(&mut ready_cursor) {
                    if executions.contains_key(&peer) {
                        continue;
                    }
                    if executions.len() >= config.max_fusion_tasks {
                        trace!(
                            component = "worker",
                            peer = ?peer,
                            active_executions = executions.len(),
                            max_fusion_tasks = config.max_fusion_tasks,
                            "worker deferring new backend peer because fusion task limit is reached"
                        );
                        continue;
                    }
                    if tracing::enabled!(Level::TRACE) {
                        trace!(
                            component = "worker",
                            peer = ?peer,
                            "worker polling ready backend peer"
                        );
                    }
                    poll_execution_peer(
                        &mut executions,
                        &mut transport,
                        &df_runtime,
                        &mut spill_runtime,
                        &config,
                        &worker_config,
                        &scan_source,
                        &task_tx,
                        #[cfg(feature = "pg_test")]
                        test_execution_gate,
                        peer,
                        page_pool,
                        issuance_pool,
                        metrics,
                    )?;
                }

                drain_task_events(
                    &mut executions,
                    &mut transport,
                    &df_runtime,
                    &mut spill_runtime,
                    &config,
                    page_pool,
                    issuance_pool,
                    metrics,
                    &mut task_rx,
                    &task_tx,
                    #[cfg(feature = "pg_test")]
                    test_execution_gate,
                )?;
            }

            for execution in executions.values_mut() {
                execution.abort_task();
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

fn wait_worker_scheduler(wake: &mut WorkerSchedulerWake, timeout: Duration) -> bool {
    let timeout_ms = timeout.as_millis().try_into().unwrap_or(c_long::MAX);
    let wake_events = (pgrx::pg_sys::WL_LATCH_SET
        | pgrx::pg_sys::WL_SOCKET_READABLE
        | pgrx::pg_sys::WL_TIMEOUT
        | pgrx::pg_sys::WL_POSTMASTER_DEATH) as i32;

    let wakeup_flags = unsafe {
        let flags = pgrx::pg_sys::WaitLatchOrSocket(
            pgrx::pg_sys::MyLatch,
            wake_events,
            wake.read_fd() as pgrx::pg_sys::pgsocket,
            timeout_ms,
            pgrx::pg_sys::PG_WAIT_EXTENSION,
        );
        pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch);
        pgrx::pg_sys::check_for_interrupts!();
        flags
    };

    if wakeup_flags & pgrx::pg_sys::WL_SOCKET_READABLE as i32 != 0 {
        wake.drain();
    }

    let postmaster_died = wakeup_flags & pgrx::pg_sys::WL_POSTMASTER_DEATH as i32 != 0;
    !BackgroundWorker::sigterm_received() && !postmaster_died
}

#[allow(clippy::too_many_arguments)]
fn poll_execution_peer(
    executions: &mut HashMap<BackendLeaseSlot, ActiveWorkerExecution>,
    transport: &mut TransportWorkerRuntime,
    df_runtime: &tokio::runtime::Runtime,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    worker_config: &WorkerRuntimeConfig,
    scan_source: &Arc<dyn ::worker::ScanBatchSource>,
    task_tx: &WorkerTaskNotifier,
    #[cfg(feature = "pg_test")] test_execution_gate: crate::test_gate::TestExecutionGateHandle,
    peer: BackendLeaseSlot,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    metrics: RuntimeMetrics,
) -> Result<(), WorkerRuntimeError> {
    let mut execution = executions.remove(&peer).unwrap_or_else(|| {
        ActiveWorkerExecution::new(worker_config.clone(), Arc::clone(scan_source))
    });
    let steps = recv_backend_peer_steps(
        transport,
        &mut execution.runtime,
        peer,
        page_pool,
        issuance_pool,
        &mut execution.plan_rx,
    )?;
    handle_steps(
        transport,
        &mut execution,
        df_runtime,
        spill_runtime,
        config,
        page_pool,
        issuance_pool,
        metrics,
        task_tx,
        #[cfg(feature = "pg_test")]
        test_execution_gate,
        steps,
    )?;
    if execution.runtime.active_peer().is_some() {
        executions.insert(peer, execution);
    } else {
        execution.abort_task();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn drain_task_events(
    executions: &mut HashMap<BackendLeaseSlot, ActiveWorkerExecution>,
    transport: &mut TransportWorkerRuntime,
    df_runtime: &tokio::runtime::Runtime,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    metrics: RuntimeMetrics,
    task_rx: &mut WorkerTaskReceiver,
    task_tx: &WorkerTaskNotifier,
    #[cfg(feature = "pg_test")] test_execution_gate: crate::test_gate::TestExecutionGateHandle,
) -> Result<(), WorkerRuntimeError> {
    while let Ok(event) = task_rx.try_recv() {
        handle_task_event(
            executions,
            transport,
            df_runtime,
            spill_runtime,
            config,
            page_pool,
            issuance_pool,
            metrics,
            task_tx,
            #[cfg(feature = "pg_test")]
            test_execution_gate,
            event,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_task_event(
    executions: &mut HashMap<BackendLeaseSlot, ActiveWorkerExecution>,
    transport: &mut TransportWorkerRuntime,
    df_runtime: &tokio::runtime::Runtime,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    metrics: RuntimeMetrics,
    task_tx: &WorkerTaskNotifier,
    #[cfg(feature = "pg_test")] test_execution_gate: crate::test_gate::TestExecutionGateHandle,
    event: WorkerTaskEvent,
) -> Result<(), WorkerRuntimeError> {
    match event {
        WorkerTaskEvent::PhysicalPlanningFinished { peer, flow, result } => {
            let Some(mut execution) = executions.remove(&peer) else {
                return Ok(());
            };
            execution.task = None;
            let step = execution
                .runtime
                .finish_physical_planning(peer, flow, result)?;
            handle_steps(
                transport,
                &mut execution,
                df_runtime,
                spill_runtime,
                config,
                page_pool,
                issuance_pool,
                metrics,
                task_tx,
                #[cfg(feature = "pg_test")]
                test_execution_gate,
                VecDeque::from([step]),
            )?;
            if execution.runtime.active_peer().is_some() {
                executions.insert(peer, execution);
            } else {
                execution.abort_task();
            }
        }
        WorkerTaskEvent::ResultPage {
            peer,
            session_epoch,
            outbound,
            ack,
        } => {
            let result = if active_execution_session_matches(executions, peer, session_epoch) {
                send_result_page_from_task(transport, metrics, peer, session_epoch, outbound)
            } else {
                Err(stale_execution_task_error(peer, session_epoch))
            };
            let _ = ack.send(result);
        }
        WorkerTaskEvent::ResultClose {
            peer,
            session_epoch,
            frame,
            ack,
        } => {
            let result = if active_execution_session_matches(executions, peer, session_epoch) {
                send_result_close_from_task(transport, peer, session_epoch, frame)
            } else {
                Err(stale_execution_task_error(peer, session_epoch))
            };
            let _ = ack.send(result);
        }
        WorkerTaskEvent::ExecutionFinished {
            peer,
            session_epoch,
            result,
        } => {
            let Some(mut execution) = executions.remove(&peer) else {
                return Ok(());
            };
            execution.task = None;
            finish_execution_task(
                transport,
                &mut execution,
                config,
                metrics,
                peer,
                session_epoch,
                result,
            )?;
            if execution.runtime.active_peer().is_some() {
                executions.insert(peer, execution);
            } else {
                execution.abort_task();
            }
        }
    }
    Ok(())
}

fn active_execution_session_matches(
    executions: &HashMap<BackendLeaseSlot, ActiveWorkerExecution>,
    peer: BackendLeaseSlot,
    session_epoch: u64,
) -> bool {
    executions
        .get(&peer)
        .and_then(|execution| execution.runtime.session_epoch())
        == Some(session_epoch)
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

fn send_result_page_from_task(
    transport: &mut TransportWorkerRuntime,
    metrics: RuntimeMetrics,
    peer: BackendLeaseSlot,
    session_epoch: u64,
    outbound: issuance::IssuedOutboundPage,
) -> Result<(), WorkerRuntimeError> {
    trace!(
        component = "worker",
        session_epoch,
        peer = ?peer,
        "worker produced one result page"
    );
    let descriptor = outbound.descriptor();
    let payload_len = outbound.payload_len();
    let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
        WorkerRuntimeError::ProtocolViolation(format!("failed to encode result page frame: {err}"))
    })?;
    transport.send_peer_bytes(peer, &frame)?;
    metrics.stamp_page(PageDirection::WorkerToBackend, descriptor, payload_len);
    metrics.increment(MetricId::WorkerResultPagesTotal);
    metrics.add(MetricId::WorkerResultBytesSentTotal, payload_len as u64);
    outbound.mark_sent();
    Ok(())
}

fn send_result_close_from_task(
    transport: &mut TransportWorkerRuntime,
    peer: BackendLeaseSlot,
    session_epoch: u64,
    frame: issuance::IssuedOwnedFrame,
) -> Result<(), WorkerRuntimeError> {
    debug!(
        component = "worker",
        session_epoch,
        peer = ?peer,
        "worker produced terminal result close frame"
    );
    let frame = encode_issued_frame(frame).map_err(|err| {
        WorkerRuntimeError::ProtocolViolation(format!("failed to encode result close frame: {err}"))
    })?;
    transport.send_peer_bytes(peer, &frame)?;
    Ok(())
}

fn finish_execution_task(
    transport: &mut TransportWorkerRuntime,
    execution: &mut ActiveWorkerExecution,
    config: &crate::HostConfig,
    metrics: RuntimeMetrics,
    peer: BackendLeaseSlot,
    session_epoch: u64,
    result: Result<(), WorkerRuntimeError>,
) -> Result<(), WorkerRuntimeError> {
    let worker_start = execution.worker_start_ns.take();
    match result {
        Ok(()) => {
            info!(
                component = "worker",
                session_epoch,
                peer = ?peer,
                "worker finished execution successfully and is sending CompleteExecution"
            );
            if let Err(err) = transport.send_peer_message(
                peer,
                WorkerExecutionToBackend::CompleteExecution { session_epoch },
            ) {
                if is_detached_backend_peer_error(&err) {
                    warn!(
                        component = "worker",
                        session_epoch,
                        peer = ?peer,
                        error = %err,
                        "worker could not send CompleteExecution because backend peer detached"
                    );
                } else {
                    return Err(err);
                }
            }
            if let Some(worker_start) = worker_start {
                metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
            }
            let step = execution.runtime.mark_execution_complete()?;
            handle_terminal_step(execution, step)?;
        }
        Err(err) => {
            let detail =
                worker_execution_failure_detail(&err, config.control_backend_to_worker_capacity);
            warn!(
                component = "worker",
                session_epoch,
                peer = ?peer,
                error = %err,
                "worker execution failed locally; sending FailExecution"
            );
            if let Err(send_err) = transport.send_peer_message(
                peer,
                WorkerExecutionToBackend::FailExecution {
                    session_epoch,
                    code: ExecutionFailureCode::Internal,
                    detail,
                },
            ) {
                if is_detached_backend_peer_error(&send_err) {
                    warn!(
                        component = "worker",
                        session_epoch,
                        peer = ?peer,
                        error = %send_err,
                        "worker could not send FailExecution because backend peer detached"
                    );
                } else {
                    return Err(send_err);
                }
            }
            if let Some(worker_start) = worker_start {
                metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
            }
            let step = execution
                .runtime
                .fail_execution_locally(ExecutionFailureCode::Internal, None)?;
            handle_terminal_step(execution, step)?;
        }
    }
    Ok(())
}

fn handle_terminal_step(
    execution: &mut ActiveWorkerExecution,
    step: WorkerRuntimeStep,
) -> Result<(), WorkerRuntimeError> {
    match step {
        WorkerRuntimeStep::ExecutionCancelled { session_epoch } => {
            info!(
                component = "worker",
                session_epoch, "worker observed execution cancel"
            );
            execution.plan_rx.take();
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
            execution.plan_rx.take();
        }
        WorkerRuntimeStep::ExecutionCompleted { session_epoch } => {
            info!(
                component = "worker",
                session_epoch, "worker observed execution complete transition"
            );
            execution.plan_rx.take();
        }
        _ => return Ok(()),
    }

    if execution.runtime.state() == ::worker::fsm::WorkerExecutionState::Terminal {
        execution.runtime.cleanup()?;
    }
    Ok(())
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
    fn datafusion_runtime_plan_uses_explicit_single_worker_thread() {
        let plan = resolve_datafusion_runtime_plan_with(Some(1), || 8);

        assert_eq!(plan.requested_worker_threads, Some(1));
        assert_eq!(plan.worker_threads, 1);
        assert_eq!(plan.mode, DataFusionRuntimeMode::MultiThread);
        assert_eq!(plan.mode.as_str(), "multi-thread");
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
        assert_eq!(plan.mode, DataFusionRuntimeMode::MultiThread);
    }

    #[test]
    fn scheduler_wake_drains_nonblocking_notifications() {
        let mut wake = WorkerSchedulerWake::new().expect("create scheduler wake");
        let sender = wake.sender();

        sender.wake().expect("wake scheduler");
        sender.wake().expect("coalesce second scheduler wake");
        assert!(wake.drain() >= 1);
        assert_eq!(wake.drain(), 0);
        sender.wake().expect("wake after drain");
        assert_eq!(wake.drain(), 1);
    }

    #[test]
    fn task_notifier_wakes_after_event_send() {
        let mut wake = WorkerSchedulerWake::new().expect("create scheduler wake");
        let (tx, mut rx) = mpsc::channel(1);
        let notifier = WorkerTaskNotifier::new(tx, wake.sender());
        let peer = BackendLeaseSlot::new(0, control_transport::BackendLeaseId::new(1, 1));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create test runtime");

        runtime.block_on(async {
            notifier
                .send(WorkerTaskEvent::ExecutionFinished {
                    peer,
                    session_epoch: 42,
                    result: Ok(()),
                })
                .await
                .expect("send task event");
        });

        assert_eq!(wake.drain(), 1);
        match rx.try_recv().expect("receive task event") {
            WorkerTaskEvent::ExecutionFinished {
                peer: received_peer,
                session_epoch,
                result,
            } => {
                assert_eq!(received_peer, peer);
                assert_eq!(session_epoch, 42);
                assert!(result.is_ok());
            }
            _ => panic!("unexpected task event"),
        }
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
    execution: &mut ActiveWorkerExecution,
    df_runtime: &tokio::runtime::Runtime,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    metrics: RuntimeMetrics,
    task_tx: &WorkerTaskNotifier,
    #[cfg(feature = "pg_test")] test_execution_gate: crate::test_gate::TestExecutionGateHandle,
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
                let task_tx = task_tx.clone();
                execution.abort_task();
                execution.task = Some(df_runtime.spawn(async move {
                    let plan_start = metrics.now_ns();
                    let result = pending.plan().await;
                    metrics.add_elapsed(MetricId::WorkerPhysicalPlanNs, plan_start);
                    metrics.increment(MetricId::WorkerPhysicalPlanTotal);
                    let _ = task_tx
                        .send(WorkerTaskEvent::PhysicalPlanningFinished { peer, flow, result })
                        .await;
                }));
            }
            WorkerRuntimeStep::PhysicalPlanReady(result) => {
                let peer = execution.runtime.active_peer().expect("peer");
                let worker_start = metrics.now_ns();
                info!(
                    component = "worker",
                    session_epoch = result.session_epoch,
                    peer = ?peer,
                    "worker received physical plan and is starting execution"
                );
                let plan = execution
                    .runtime
                    .take_physical_plan()
                    .ok_or(WorkerRuntimeError::MissingPhysicalPlan)?;
                let spill_dir = spill_runtime.execution_dir(peer, result.session_epoch)?;
                execution.worker_start_ns = Some(worker_start);
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
                            result.session_epoch,
                        );
                        if let Err(cleanup_err) = cleanup_result {
                            warn!(
                                component = "worker",
                                session_epoch = result.session_epoch,
                                peer = ?peer,
                                error = %cleanup_err,
                                "worker failed to clean execution spill directory after task context failure"
                            );
                        }
                        finish_execution_task(
                            transport,
                            execution,
                            config,
                            metrics,
                            peer,
                            result.session_epoch,
                            Err(err),
                        )?;
                        continue;
                    }
                };

                let task_input = ExecutionTaskInput {
                    task_tx: task_tx.clone(),
                    page_pool,
                    issuance_pool,
                    metrics,
                    peer,
                    session_epoch: result.session_epoch,
                    plan,
                    task_ctx,
                    spill_dir,
                    spill_dir_created,
                    estimator_initial_tail_bytes_per_row: config
                        .estimator_initial_tail_bytes_per_row,
                    #[cfg(feature = "pg_test")]
                    test_execution_gate,
                };
                execution.abort_task();
                execution.task = Some(df_runtime.spawn(async move {
                    let peer = task_input.peer;
                    let session_epoch = task_input.session_epoch;
                    let task_tx = task_input.task_tx.clone();
                    let result = execute_physical_plan_task(task_input).await;
                    let _ = task_tx
                        .send(WorkerTaskEvent::ExecutionFinished {
                            peer,
                            session_epoch,
                            result,
                        })
                        .await;
                }));
            }
            step @ WorkerRuntimeStep::ExecutionCancelled { .. } => {
                execution.abort_task();
                handle_terminal_step(execution, step)?;
            }
            step @ WorkerRuntimeStep::ExecutionFailed { .. } => {
                execution.abort_task();
                handle_terminal_step(execution, step)?;
            }
            step @ WorkerRuntimeStep::ExecutionCompleted { .. } => {
                handle_terminal_step(execution, step)?;
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
    MultiThread,
}

impl DataFusionRuntimeMode {
    fn as_str(self) -> &'static str {
        match self {
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

    DataFusionRuntimePlan {
        requested_worker_threads: worker_threads,
        worker_threads: effective_worker_threads,
        mode: DataFusionRuntimeMode::MultiThread,
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
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(plan.worker_threads)
        .thread_name("pg_fusion-df")
        .build();

    result.map_err(|err| {
        WorkerRuntimeError::ProtocolViolation(format!(
            "failed to build DataFusion Tokio runtime: {err}"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_physical_plan_task(input: ExecutionTaskInput) -> Result<(), WorkerRuntimeError> {
    let ExecutionTaskInput {
        task_tx,
        page_pool,
        issuance_pool,
        metrics,
        peer,
        session_epoch,
        plan,
        task_ctx,
        spill_dir,
        spill_dir_created,
        estimator_initial_tail_bytes_per_row,
        #[cfg(feature = "pg_test")]
        test_execution_gate,
    } = input;
    #[cfg(feature = "pg_test")]
    test_execution_gate.wait_at_execution_start().await;

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
                    initial_tail_bytes_per_row: estimator_initial_tail_bytes_per_row,
                },
                metrics,
                ..ResultPageProducerConfig::default()
            },
        )?;

        loop {
            match producer.next_step_async().await? {
                Some(ResultPageStep::OutboundPage(outbound)) => {
                    let (ack, ack_rx) = oneshot::channel();
                    task_tx
                        .send(WorkerTaskEvent::ResultPage {
                            peer,
                            session_epoch,
                            outbound,
                            ack,
                        })
                        .await
                        .map_err(|_| worker_scheduler_gone_error())?;
                    ack_rx.await.map_err(|_| worker_scheduler_gone_error())??;
                }
                Some(ResultPageStep::CloseFrame(frame)) => {
                    let (ack, ack_rx) = oneshot::channel();
                    task_tx
                        .send(WorkerTaskEvent::ResultClose {
                            peer,
                            session_epoch,
                            frame,
                            ack,
                        })
                        .await
                        .map_err(|_| worker_scheduler_gone_error())?;
                    ack_rx.await.map_err(|_| worker_scheduler_gone_error())??;
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

fn worker_scheduler_gone_error() -> WorkerRuntimeError {
    WorkerRuntimeError::ProtocolViolation(
        "worker scheduler stopped while execution task ran".into(),
    )
}

fn stale_execution_task_error(peer: BackendLeaseSlot, session_epoch: u64) -> WorkerRuntimeError {
    WorkerRuntimeError::ProtocolViolation(format!(
        "stale execution task result ignored for peer {peer:?} session {session_epoch}"
    ))
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
