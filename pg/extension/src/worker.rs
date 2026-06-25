use std::collections::{HashMap, HashSet, VecDeque};
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingTerminalControl {
    Complete {
        session_epoch: u64,
    },
    Fail {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<String>,
    },
}

impl PendingTerminalControl {
    fn session_epoch(&self) -> u64 {
        match self {
            Self::Complete { session_epoch } | Self::Fail { session_epoch, .. } => *session_epoch,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Complete { .. } => "CompleteExecution",
            Self::Fail { .. } => "FailExecution",
        }
    }

    fn to_message(&self) -> WorkerExecutionToBackend {
        match self {
            Self::Complete { session_epoch } => WorkerExecutionToBackend::CompleteExecution {
                session_epoch: *session_epoch,
            },
            Self::Fail {
                session_epoch,
                code,
                detail,
            } => WorkerExecutionToBackend::FailExecution {
                session_epoch: *session_epoch,
                code: *code,
                detail: detail.clone(),
            },
        }
    }
}

enum PeerPollOutcome {
    Steps(VecDeque<WorkerRuntimeStep>),
    DetachedIdle,
    ReservationCancelled,
    ActiveReservationCancelViolation(WorkerRuntimeError),
}

#[derive(Debug)]
enum AdmissionPollOutcome {
    Idle,
    Requested,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerSchedulerState {
    Active,
    Reserved,
    Retained,
    Queued,
    Unknown,
}

struct ActiveWorkerExecution {
    runtime: WorkerRuntimeCore,
    plan_rx: Option<IssuedRx>,
    task: Option<JoinHandle<()>>,
    worker_start_ns: Option<u64>,
    pending_terminal: Option<PendingTerminalControl>,
}

impl ActiveWorkerExecution {
    fn new(config: WorkerRuntimeConfig, scan_source: Arc<dyn ::worker::ScanBatchSource>) -> Self {
        Self {
            runtime: WorkerRuntimeCore::new(config, scan_source),
            plan_rx: None,
            task: None,
            worker_start_ns: None,
            pending_terminal: None,
        }
    }

    fn abort_task(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct ExecutionRegistry {
    entries: HashMap<BackendLeaseSlot, ActiveWorkerExecution>,
    active_peers: HashSet<BackendLeaseSlot>,
    reserved_peers: HashSet<BackendLeaseSlot>,
    queued_reservations: VecDeque<BackendLeaseSlot>,
    queued_peers: HashSet<BackendLeaseSlot>,
    retained_by_slot: HashMap<u32, BackendLeaseSlot>,
}

impl ExecutionRegistry {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            active_peers: HashSet::with_capacity(capacity),
            reserved_peers: HashSet::with_capacity(capacity),
            queued_reservations: VecDeque::with_capacity(capacity),
            queued_peers: HashSet::with_capacity(capacity),
            retained_by_slot: HashMap::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    fn active_len(&self) -> usize {
        self.active_peers.len()
    }

    fn admitted_len(&self) -> usize {
        self.active_peers.len() + self.reserved_peers.len()
    }

    fn has_admission_capacity(&self, max_fusion_tasks: usize) -> bool {
        self.admitted_len() < max_fusion_tasks
    }

    fn active_peers(&self) -> Vec<BackendLeaseSlot> {
        self.active_peers.iter().copied().collect()
    }

    fn reserved_peers(&self) -> Vec<BackendLeaseSlot> {
        self.reserved_peers.iter().copied().collect()
    }

    fn pending_terminal_peers(&self) -> Vec<BackendLeaseSlot> {
        self.entries
            .iter()
            .filter_map(|(peer, execution)| execution.pending_terminal.as_ref().map(|_| *peer))
            .collect()
    }

    fn contains_active(&self, peer: BackendLeaseSlot) -> bool {
        self.active_peers.contains(&peer)
    }

    fn contains_reserved(&self, peer: BackendLeaseSlot) -> bool {
        self.reserved_peers.contains(&peer)
    }

    fn contains_queued(&self, peer: BackendLeaseSlot) -> bool {
        self.queued_peers.contains(&peer)
    }

    fn peer_scheduler_state(&self, peer: BackendLeaseSlot) -> PeerSchedulerState {
        if self.active_peers.contains(&peer) {
            PeerSchedulerState::Active
        } else if self.reserved_peers.contains(&peer) {
            PeerSchedulerState::Reserved
        } else if self.retained_by_slot.get(&peer.slot_id()) == Some(&peer)
            && self.entries.contains_key(&peer)
        {
            PeerSchedulerState::Retained
        } else if self.queued_peers.contains(&peer) {
            PeerSchedulerState::Queued
        } else {
            PeerSchedulerState::Unknown
        }
    }

    fn has_queued_reservations(&self) -> bool {
        !self.queued_peers.is_empty()
    }

    fn reserve(&mut self, peer: BackendLeaseSlot) {
        self.remove_queued(peer);
        self.evict_retained_for_slot(peer.slot_id(), None);
        self.reserved_peers.insert(peer);
    }

    fn queue_reservation(&mut self, peer: BackendLeaseSlot) {
        if self.reserved_peers.contains(&peer) || !self.queued_peers.insert(peer) {
            return;
        }
        self.queued_reservations.push_back(peer);
    }

    fn cancel_reservation(&mut self, peer: BackendLeaseSlot) {
        self.reserved_peers.remove(&peer);
        self.remove_queued(peer);
    }

    fn remove_queued(&mut self, peer: BackendLeaseSlot) {
        if !self.queued_peers.remove(&peer) {
            return;
        }
        self.queued_reservations
            .retain(|queued_peer| *queued_peer != peer);
    }

    fn pop_next_queued_reservation(&mut self) -> Option<BackendLeaseSlot> {
        while let Some(peer) = self.queued_reservations.pop_front() {
            if self.queued_peers.remove(&peer) {
                return Some(peer);
            }
        }
        None
    }

    fn remove(&mut self, peer: BackendLeaseSlot) -> Option<ActiveWorkerExecution> {
        let execution = self.entries.remove(&peer);
        if execution.is_some() {
            self.active_peers.remove(&peer);
            if self.retained_by_slot.get(&peer.slot_id()) == Some(&peer) {
                self.retained_by_slot.remove(&peer.slot_id());
            }
        }
        execution
    }

    fn retain_after_poll(&mut self, peer: BackendLeaseSlot, mut execution: ActiveWorkerExecution) {
        if execution.runtime.active_peer() == Some(peer) {
            self.reserved_peers.remove(&peer);
            self.evict_retained_for_slot(peer.slot_id(), Some(peer));
            self.active_peers.insert(peer);
            self.entries.insert(peer, execution);
        } else if execution.runtime.retained_peer() == Some(peer) {
            self.reserved_peers.remove(&peer);
            self.evict_retained_for_slot(peer.slot_id(), Some(peer));
            execution.abort_task();
            execution.plan_rx.take();
            self.active_peers.remove(&peer);
            self.retained_by_slot.insert(peer.slot_id(), peer);
            self.entries.insert(peer, execution);
        } else {
            execution.abort_task();
        }
    }

    fn evict_retained_for_slot(&mut self, slot_id: u32, keep: Option<BackendLeaseSlot>) {
        let Some(retained_peer) = self.retained_by_slot.get(&slot_id).copied() else {
            return;
        };
        if Some(retained_peer) == keep {
            return;
        }
        self.retained_by_slot.remove(&slot_id);
        if let Some(mut execution) = self.entries.remove(&retained_peer) {
            self.active_peers.remove(&retained_peer);
            execution.abort_task();
        }
    }

    fn active_session_matches(&self, peer: BackendLeaseSlot, session_epoch: u64) -> bool {
        self.entries
            .get(&peer)
            .and_then(|execution| execution.runtime.session_epoch())
            == Some(session_epoch)
    }

    fn remove_active_session(
        &mut self,
        peer: BackendLeaseSlot,
        session_epoch: u64,
    ) -> Option<ActiveWorkerExecution> {
        if self.active_session_matches(peer, session_epoch) {
            self.remove(peer)
        } else {
            None
        }
    }

    fn abort_all(&mut self) {
        for execution in self.entries.values_mut() {
            execution.abort_task();
        }
        self.reserved_peers.clear();
        self.queued_reservations.clear();
        self.queued_peers.clear();
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
            runtime_filter_exec_id: job.runtime_filter_exec_id,
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
            let mut executions =
                ExecutionRegistry::with_capacity(config.control_slot_count as usize);
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

                retry_pending_terminal_controls(&mut executions, &mut transport)?;

                let active_peers = executions.active_peers();
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

                let reserved_peers = executions.reserved_peers();
                for peer in reserved_peers {
                    trace!(
                        component = "worker",
                        peer = ?peer,
                        "worker probing reserved backend peer"
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
                    match executions.peer_scheduler_state(peer) {
                        PeerSchedulerState::Active => continue,
                        PeerSchedulerState::Reserved | PeerSchedulerState::Retained => {
                            if tracing::enabled!(Level::TRACE) {
                                trace!(
                                    component = "worker",
                                    peer = ?peer,
                                    state = ?executions.peer_scheduler_state(peer),
                                    "worker polling known backend peer"
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
                        PeerSchedulerState::Queued | PeerSchedulerState::Unknown => {
                            let admission = recv_backend_admission_request(&mut transport, peer)?;
                            match admission {
                                AdmissionPollOutcome::Idle => {}
                                AdmissionPollOutcome::Cancelled => {
                                    executions.cancel_reservation(peer);
                                }
                                AdmissionPollOutcome::Requested => {
                                    handle_requested_execution_reservation(
                                        &mut executions,
                                        &mut transport,
                                        peer,
                                        config.max_fusion_tasks,
                                    )?;
                                }
                            }
                        }
                    }
                }

                grant_queued_execution_reservations(
                    &mut executions,
                    &mut transport,
                    config.max_fusion_tasks,
                )?;

                let mut ready_cursor = 0;
                while let Some(peer) = transport.next_ready_backend_lease(&mut ready_cursor) {
                    if executions.contains_active(peer) || !executions.contains_reserved(peer) {
                        continue;
                    }
                    if tracing::enabled!(Level::TRACE) {
                        trace!(
                            component = "worker",
                            peer = ?peer,
                            "worker polling admitted backend peer after queued grants"
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

                retry_pending_terminal_controls(&mut executions, &mut transport)?;

                grant_queued_execution_reservations(
                    &mut executions,
                    &mut transport,
                    config.max_fusion_tasks,
                )?;
            }

            executions.abort_all();
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

fn recv_backend_admission_request(
    transport: &mut TransportWorkerRuntime,
    peer: BackendLeaseSlot,
) -> Result<AdmissionPollOutcome, WorkerRuntimeError> {
    let mut requested = false;
    let mut cancelled = false;
    let recv_result = transport.recv_peer_frames(peer, |bytes| {
        match WorkerRuntimeCore::decode_inbound(bytes)? {
            DecodedInbound::Control(
                protocol::BackendExecutionToWorkerRef::ReserveExecutionSlot,
            ) => {
                requested = true;
            }
            DecodedInbound::Control(
                protocol::BackendExecutionToWorkerRef::CancelExecutionReservation,
            ) => {
                cancelled = true;
            }
            DecodedInbound::Control(message) => {
                return Err(WorkerRuntimeError::ProtocolViolation(format!(
                    "backend sent {message:?} before execution reservation was granted"
                )));
            }
            DecodedInbound::IssuedFrame(_) => {
                return Err(WorkerRuntimeError::ProtocolViolation(
                    "backend sent plan frame before execution reservation was granted".into(),
                ));
            }
        }
        Ok(())
    });

    match recv_result {
        Ok(()) if cancelled => Ok(AdmissionPollOutcome::Cancelled),
        Ok(()) if requested => Ok(AdmissionPollOutcome::Requested),
        Ok(()) => Ok(AdmissionPollOutcome::Idle),
        Err(err) if is_detached_backend_peer_error(&err) => {
            warn!(
                component = "worker",
                peer = ?peer,
                error = %err,
                "worker could not read admission request because backend peer detached"
            );
            Ok(AdmissionPollOutcome::Cancelled)
        }
        Err(err) => Err(err),
    }
}

fn grant_execution_reservation(
    executions: &mut ExecutionRegistry,
    transport: &mut TransportWorkerRuntime,
    peer: BackendLeaseSlot,
) -> Result<bool, WorkerRuntimeError> {
    executions.reserve(peer);
    match transport.send_peer_message(peer, WorkerExecutionToBackend::ExecutionSlotReserved) {
        Ok(_) => Ok(true),
        Err(err) if is_detached_backend_peer_error(&err) => {
            executions.cancel_reservation(peer);
            warn!(
                component = "worker",
                peer = ?peer,
                error = %err,
                "worker could not grant execution reservation because backend peer detached"
            );
            Ok(false)
        }
        Err(err) => {
            executions.cancel_reservation(peer);
            Err(err)
        }
    }
}

fn handle_requested_execution_reservation(
    executions: &mut ExecutionRegistry,
    transport: &mut TransportWorkerRuntime,
    peer: BackendLeaseSlot,
    max_fusion_tasks: usize,
) -> Result<(), WorkerRuntimeError> {
    if executions.contains_queued(peer) {
        trace!(
            component = "worker",
            peer = ?peer,
            "worker observed duplicate queued backend execution reservation"
        );
    } else if executions.has_queued_reservations() {
        executions.queue_reservation(peer);
        trace!(
            component = "worker",
            peer = ?peer,
            admitted_executions = executions.admitted_len(),
            max_fusion_tasks,
            "worker queued backend execution reservation behind older reservations"
        );
    } else if executions.has_admission_capacity(max_fusion_tasks) {
        let granted = grant_execution_reservation(executions, transport, peer)?;
        if granted {
            trace!(
                component = "worker",
                peer = ?peer,
                admitted_executions = executions.admitted_len(),
                max_fusion_tasks,
                "worker granted backend execution reservation"
            );
        }
    } else {
        executions.queue_reservation(peer);
        trace!(
            component = "worker",
            peer = ?peer,
            admitted_executions = executions.admitted_len(),
            max_fusion_tasks,
            "worker queued backend execution reservation because fusion task limit is reached"
        );
    }
    Ok(())
}

fn grant_queued_execution_reservations(
    executions: &mut ExecutionRegistry,
    transport: &mut TransportWorkerRuntime,
    max_fusion_tasks: usize,
) -> Result<(), WorkerRuntimeError> {
    while executions.has_admission_capacity(max_fusion_tasks) {
        let Some(peer) = executions.pop_next_queued_reservation() else {
            break;
        };
        let granted = grant_execution_reservation(executions, transport, peer)?;
        if granted {
            trace!(
                component = "worker",
                peer = ?peer,
                admitted_executions = executions.admitted_len(),
                max_fusion_tasks,
                "worker granted queued backend execution reservation"
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn poll_execution_peer(
    executions: &mut ExecutionRegistry,
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
    let execution = executions.remove(peer);
    if execution.is_none() && !executions.contains_reserved(peer) {
        return Err(WorkerRuntimeError::ProtocolViolation(format!(
            "backend peer {peer:?} sent execution frames without a granted reservation"
        )));
    }
    let mut execution = execution.unwrap_or_else(|| {
        ActiveWorkerExecution::new(worker_config.clone(), Arc::clone(scan_source))
    });
    let poll = recv_backend_peer_steps(
        transport,
        &mut execution.runtime,
        peer,
        page_pool,
        issuance_pool,
        &mut execution.plan_rx,
    )?;
    match poll {
        PeerPollOutcome::Steps(steps) => {
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
            executions.retain_after_poll(peer, execution);
        }
        PeerPollOutcome::DetachedIdle => {
            execution.abort_task();
            executions.cancel_reservation(peer);
        }
        PeerPollOutcome::ReservationCancelled => {
            execution.abort_task();
            executions.cancel_reservation(peer);
        }
        PeerPollOutcome::ActiveReservationCancelViolation(err) => {
            let session_epoch = execution
                .runtime
                .session_epoch()
                .ok_or(WorkerRuntimeError::NoActiveExecution)?;
            execution.abort_task();
            finish_execution_task(
                transport,
                &mut execution,
                config,
                metrics,
                peer,
                session_epoch,
                Err(err),
            )?;
            executions.retain_after_poll(peer, execution);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn drain_task_events(
    executions: &mut ExecutionRegistry,
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
    executions: &mut ExecutionRegistry,
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
            let Some(mut execution) = executions.remove(peer) else {
                return Ok(());
            };
            let (step, planning_err) = execution
                .runtime
                .finish_physical_planning_step(peer, flow, result)?;
            if planning_err.is_some()
                || !matches!(step, WorkerRuntimeStep::PlanningResultIgnored { .. })
            {
                execution.task = None;
            }
            if let Some(err) = planning_err {
                finish_failed_terminal_step(transport, &mut execution, config, peer, err, step)?;
            } else {
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
            }
            executions.retain_after_poll(peer, execution);
        }
        WorkerTaskEvent::ResultPage {
            peer,
            session_epoch,
            outbound,
            ack,
        } => {
            let result = if executions.active_session_matches(peer, session_epoch) {
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
            let result = if executions.active_session_matches(peer, session_epoch) {
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
            let Some(mut execution) = executions.remove_active_session(peer, session_epoch) else {
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
            executions.retain_after_poll(peer, execution);
        }
    }
    Ok(())
}

fn recv_backend_peer_steps(
    transport: &mut TransportWorkerRuntime,
    runtime: &mut WorkerRuntimeCore,
    peer: BackendLeaseSlot,
    page_pool: PagePool,
    issuance_pool: IssuancePool,
    plan_rx: &mut Option<IssuedRx>,
) -> Result<PeerPollOutcome, WorkerRuntimeError> {
    let mut steps = VecDeque::new();
    let mut reservation_cancelled = false;
    let mut active_reservation_cancel_violation = None;
    let recv_result = transport.recv_peer_frames(peer, |bytes| {
        if active_reservation_cancel_violation.is_some() {
            return Ok(());
        }
        let decoded = WorkerRuntimeCore::decode_inbound(bytes)?;
        let step = match decoded {
            DecodedInbound::Control(
                protocol::BackendExecutionToWorkerRef::ReserveExecutionSlot,
            ) => {
                return Ok(());
            }
            DecodedInbound::Control(
                protocol::BackendExecutionToWorkerRef::CancelExecutionReservation,
            ) => {
                if runtime.active_peer() == Some(peer) {
                    active_reservation_cancel_violation =
                        Some(WorkerRuntimeError::ProtocolViolation(format!(
                            "backend peer {peer:?} sent CancelExecutionReservation after execution started"
                        )));
                } else if runtime.retained_peer() == Some(peer) {
                    // Reservation cancellation is only meaningful before
                    // StartExecution. Once the runtime is retained, treat it
                    // like stale pre-session traffic so pending terminal
                    // delivery is not discarded.
                } else {
                    reservation_cancelled = true;
                }
                return Ok(());
            }
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
        Ok(()) if active_reservation_cancel_violation.is_some() => {
            Ok(PeerPollOutcome::ActiveReservationCancelViolation(
                active_reservation_cancel_violation.expect("violation error"),
            ))
        }
        Ok(()) if reservation_cancelled && steps.is_empty() => {
            Ok(PeerPollOutcome::ReservationCancelled)
        }
        Ok(()) => Ok(PeerPollOutcome::Steps(steps)),
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
                return Ok(PeerPollOutcome::Steps(steps));
            }
            Ok(PeerPollOutcome::DetachedIdle)
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
            if let Some(worker_start) = worker_start {
                metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
            }
            let step = execution.runtime.mark_execution_complete()?;
            handle_terminal_step(execution, step)?;
            queue_and_try_send_terminal_control(
                transport,
                execution,
                peer,
                PendingTerminalControl::Complete { session_epoch },
            )?;
        }
        Err(err) => {
            let detail =
                worker_execution_failure_detail(&err, config.control_worker_to_backend_capacity);
            warn!(
                component = "worker",
                session_epoch,
                peer = ?peer,
                error = %err,
                "worker execution failed locally; sending FailExecution"
            );
            if let Some(worker_start) = worker_start {
                metrics.add_elapsed(MetricId::WorkerTotalNs, worker_start);
            }
            let step = execution
                .runtime
                .fail_execution_locally(ExecutionFailureCode::Internal, None)?;
            handle_terminal_step(execution, step)?;
            queue_and_try_send_terminal_control(
                transport,
                execution,
                peer,
                PendingTerminalControl::Fail {
                    session_epoch,
                    code: ExecutionFailureCode::Internal,
                    detail,
                },
            )?;
        }
    }
    Ok(())
}

fn finish_failed_terminal_step(
    transport: &mut TransportWorkerRuntime,
    execution: &mut ActiveWorkerExecution,
    config: &crate::HostConfig,
    peer: BackendLeaseSlot,
    err: WorkerRuntimeError,
    step: WorkerRuntimeStep,
) -> Result<(), WorkerRuntimeError> {
    let WorkerRuntimeStep::ExecutionFailed {
        session_epoch,
        code,
        ..
    } = step
    else {
        return Err(WorkerRuntimeError::ProtocolViolation(format!(
            "planning failure produced non-failure runtime step: {step:?}"
        )));
    };
    let detail = worker_execution_failure_detail(&err, config.control_worker_to_backend_capacity);
    warn!(
        component = "worker",
        session_epoch,
        peer = ?peer,
        error = %err,
        "worker physical planning failed locally; sending FailExecution"
    );
    handle_terminal_step(
        execution,
        WorkerRuntimeStep::ExecutionFailed {
            session_epoch,
            code,
            detail: None,
        },
    )?;
    queue_and_try_send_terminal_control(
        transport,
        execution,
        peer,
        PendingTerminalControl::Fail {
            session_epoch,
            code,
            detail,
        },
    )
}

fn queue_and_try_send_terminal_control(
    transport: &mut TransportWorkerRuntime,
    execution: &mut ActiveWorkerExecution,
    peer: BackendLeaseSlot,
    terminal: PendingTerminalControl,
) -> Result<(), WorkerRuntimeError> {
    execution.pending_terminal = Some(terminal);
    retry_pending_terminal_control(transport, execution, peer)
}

fn retry_pending_terminal_controls(
    executions: &mut ExecutionRegistry,
    transport: &mut TransportWorkerRuntime,
) -> Result<(), WorkerRuntimeError> {
    for peer in executions.pending_terminal_peers() {
        let Some(mut execution) = executions.remove(peer) else {
            continue;
        };
        let retry = retry_pending_terminal_control(transport, &mut execution, peer);
        executions.retain_after_poll(peer, execution);
        retry?;
    }
    Ok(())
}

fn retry_pending_terminal_control(
    transport: &mut TransportWorkerRuntime,
    execution: &mut ActiveWorkerExecution,
    peer: BackendLeaseSlot,
) -> Result<(), WorkerRuntimeError> {
    let Some(terminal) = execution.pending_terminal.clone() else {
        return Ok(());
    };
    let session_epoch = terminal.session_epoch();
    let kind = terminal.kind();
    match transport.send_peer_message(peer, terminal.to_message()) {
        Ok(_) => {
            execution.pending_terminal = None;
            Ok(())
        }
        Err(err) if is_detached_backend_peer_error(&err) => {
            warn!(
                component = "worker",
                session_epoch,
                peer = ?peer,
                error = %err,
                terminal = kind,
                "worker could not send terminal control because backend peer detached"
            );
            execution.pending_terminal = None;
            Ok(())
        }
        Err(err) if is_worker_to_backend_ring_full_error(&err) => {
            warn!(
                component = "worker",
                session_epoch,
                peer = ?peer,
                error = %err,
                terminal = kind,
                "worker terminal control ring is full; deferring terminal control"
            );
            Ok(())
        }
        Err(err) => Err(err),
    }
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

fn is_worker_to_backend_ring_full_error(err: &WorkerRuntimeError) -> bool {
    matches!(
        err,
        WorkerRuntimeError::WorkerTx(control_transport::WorkerTxError::Ring(
            control_transport::TxError::Full { .. }
        ))
    )
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
    use std::alloc::{alloc_zeroed, dealloc, Layout};
    use std::ptr::NonNull;
    use std::sync::Once;
    use std::time::{SystemTime, UNIX_EPOCH};

    use control_transport::{TransportRegion, TransportRegionLayout};

    #[derive(Debug)]
    struct NoopScanSource;

    impl ::worker::ScanBatchSource for NoopScanSource {
        fn open_scan(
            &self,
            _request: ::worker::OpenScanRequest,
        ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            Err(datafusion_common::DataFusionError::Execution(
                "test scan source is unused".into(),
            ))
        }
    }

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

    struct TestTransport {
        worker: TransportWorkerRuntime,
        region: TransportRegion,
        backend: BackendSlotLease,
        _region_mem: OwnedRegion,
    }

    impl TestTransport {
        fn new(worker_to_backend_capacity: usize) -> Self {
            Self::with_slots(1, worker_to_backend_capacity)
        }

        fn with_slots(slot_count: u32, worker_to_backend_capacity: usize) -> Self {
            ignore_sigusr1_for_tests();
            let layout = TransportRegionLayout::new(slot_count, 256, worker_to_backend_capacity)
                .expect("transport layout");
            let worker_config = WorkerRuntimeConfig {
                control_frame_capacity: worker_to_backend_capacity,
                ..WorkerRuntimeConfig::default()
            };
            Self::with_layout_and_worker_config(layout, worker_config)
        }

        fn with_host_config(config: &crate::HostConfig) -> Self {
            ignore_sigusr1_for_tests();
            let layout = config.control_transport_layout().expect("transport layout");
            Self::with_layout_and_worker_config(layout, config.worker_runtime_config())
        }

        fn with_layout_and_worker_config(
            layout: TransportRegionLayout,
            worker_config: WorkerRuntimeConfig,
        ) -> Self {
            let region_mem = OwnedRegion::new(layout.size, layout.align);
            let region =
                unsafe { TransportRegion::init_in_place(region_mem.base, layout.size, layout) }
                    .expect("transport region");
            let worker = TransportWorkerRuntime::attach(&region, &worker_config)
                .expect("attach worker transport");
            worker
                .activate_generation(std::process::id() as i32)
                .expect("activate worker generation");
            let backend = BackendSlotLease::acquire(&region).expect("acquire backend lease");

            Self {
                worker,
                region,
                backend,
                _region_mem: region_mem,
            }
        }

        fn acquire_backend(&self) -> BackendSlotLease {
            BackendSlotLease::acquire(&self.region).expect("acquire backend lease")
        }

        fn peer(&self) -> BackendLeaseSlot {
            self.backend.backend_lease_slot()
        }

        fn recv_worker_execution(&mut self) -> protocol::WorkerExecutionToBackend {
            recv_worker_execution_from(&mut self.backend)
        }
    }

    fn send_backend_execution_frame(
        backend: &mut BackendSlotLease,
        message: protocol::BackendExecutionToWorker<'_>,
    ) {
        let mut buf = vec![0_u8; protocol::encoded_len_backend_execution_to_worker(message)];
        let len = protocol::encode_backend_execution_to_worker_into(message, &mut buf)
            .expect("encode backend execution frame");
        let mut tx = backend.to_worker_tx();
        tx.send_frame(&buf[..len]).expect("send backend frame");
    }

    fn recv_worker_execution_from(
        backend: &mut BackendSlotLease,
    ) -> protocol::WorkerExecutionToBackend {
        let mut rx = backend.from_worker_rx();
        let mut buf = vec![0_u8; 4096];
        let len = rx
            .recv_frame_into(&mut buf)
            .expect("receive worker frame")
            .expect("worker frame");
        protocol::decode_worker_execution_to_backend(&buf[..len]).expect("decode worker frame")
    }

    fn drain_worker_execution_frames(
        backend: &mut BackendSlotLease,
    ) -> Vec<protocol::WorkerExecutionToBackend> {
        let mut rx = backend.from_worker_rx();
        let mut buf = vec![0_u8; 4096];
        let mut frames = Vec::new();
        while let Some(len) = rx.recv_frame_into(&mut buf).expect("receive worker frame") {
            frames.push(
                protocol::decode_worker_execution_to_backend(&buf[..len])
                    .expect("decode worker frame"),
            );
        }
        frames
    }

    fn fill_worker_to_backend_ring_until_full(
        worker: &mut TransportWorkerRuntime,
        peer: BackendLeaseSlot,
    ) {
        loop {
            match worker.send_peer_message(peer, WorkerExecutionToBackend::ExecutionSlotReserved) {
                Ok(_) => {}
                Err(err) if is_worker_to_backend_ring_full_error(&err) => return,
                Err(err) => panic!("unexpected worker-to-backend fill error: {err}"),
            }
        }
    }

    fn assert_fail_execution_frame(
        frame: protocol::WorkerExecutionToBackend,
        session_epoch: u64,
        expected_detail: &str,
    ) {
        match frame {
            protocol::WorkerExecutionToBackend::FailExecution {
                session_epoch: actual_session_epoch,
                code,
                detail,
            } => {
                assert_eq!(actual_session_epoch, session_epoch);
                assert_eq!(code, ExecutionFailureCode::Internal);
                assert!(detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains(expected_detail)));
            }
            other => panic!("unexpected worker frame: {other:?}"),
        }
    }

    fn init_page_pool_for_test() -> (OwnedRegion, PagePool) {
        let config = pool::PagePoolConfig::new(4096, 1).expect("page pool config");
        let layout = PagePool::layout(config).expect("page pool layout");
        let region = OwnedRegion::new(layout.size, layout.align);
        let page_pool = unsafe { PagePool::init_in_place(region.base, layout.size, config) }
            .expect("page pool init");
        (region, page_pool)
    }

    fn init_issuance_pool_for_test() -> (OwnedRegion, IssuancePool) {
        let config = issuance::IssuanceConfig::new(1).expect("issuance config");
        let layout = IssuancePool::layout(config).expect("issuance layout");
        let region = OwnedRegion::new(layout.size, layout.align);
        let issuance_pool =
            unsafe { IssuancePool::init_in_place(region.base, layout.size, config) }
                .expect("issuance pool init");
        (region, issuance_pool)
    }

    fn poll_test_execution_peer(
        registry: &mut ExecutionRegistry,
        worker: &mut TransportWorkerRuntime,
        peer: BackendLeaseSlot,
    ) {
        let (_page_region, page_pool) = init_page_pool_for_test();
        let (_issuance_region, issuance_pool) = init_issuance_pool_for_test();
        let df_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create datafusion runtime");
        let mut spill_runtime =
            WorkerSpillRuntime::new(::worker::WorkerSpillConfig::default(), 123, 456)
                .expect("create spill runtime");
        let worker_config = WorkerRuntimeConfig::default();
        let scan_source: Arc<dyn ::worker::ScanBatchSource> = Arc::new(NoopScanSource);
        let wake = WorkerSchedulerWake::new().expect("create scheduler wake");
        let (task_event_tx, _task_rx) = mpsc::channel(1);
        let task_tx = WorkerTaskNotifier::new(task_event_tx, wake.sender());

        poll_execution_peer(
            registry,
            worker,
            &df_runtime,
            &mut spill_runtime,
            &test_host_config(4096),
            &worker_config,
            &scan_source,
            &task_tx,
            #[cfg(feature = "pg_test")]
            crate::test_gate::attach(),
            peer,
            page_pool,
            issuance_pool,
            RuntimeMetrics::default(),
        )
        .expect("poll execution peer");
    }

    fn ignore_sigusr1_for_tests() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| unsafe {
            libc::signal(libc::SIGUSR1, libc::SIG_IGN);
        });
    }

    struct TestTempDir {
        path: PathBuf,
    }

    impl TestTempDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "pg_fusion_worker_test_{}_{}",
                std::process::id(),
                unique
            ));
            std::fs::create_dir(&path).expect("create test temp dir");
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn test_host_config(control_worker_to_backend_capacity: usize) -> crate::HostConfig {
        crate::HostConfig {
            enable: true,
            worker_threads: Some(1),
            log_path: "/tmp/pg_fusion.log".into(),
            worker_log_filter: "warn".into(),
            backend_log_level: backend_service::DiagnosticLogLevel::Basic,
            worker_memory_limit_bytes: None,
            worker_spill_directory: None,
            max_fusion_tasks: 1,
            control_slot_count: 1,
            control_backend_to_worker_capacity: 256,
            control_worker_to_backend_capacity,
            scan_slot_count: 1,
            scan_backend_to_worker_capacity: protocol::MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
            scan_worker_to_backend_capacity: protocol::MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
            page_size: 4096,
            page_count: 4,
            scan_fetch_batch_rows: 16,
            scan_batch_channel_capacity: 1,
            scan_idle_poll_interval_us: 50,
            estimator_initial_tail_bytes_per_row: 64,
            join_reordering: false,
            runtime_filter_enable: true,
            runtime_filter_count: 1,
            runtime_filter_bits: 1024,
            runtime_filter_hashes: 1,
        }
    }

    fn test_peer(slot_id: u32) -> BackendLeaseSlot {
        test_peer_incarnation(slot_id, u64::from(slot_id) + 1)
    }

    fn test_peer_incarnation(slot_id: u32, lease_epoch: u64) -> BackendLeaseSlot {
        BackendLeaseSlot::new(
            slot_id,
            control_transport::BackendLeaseId::new(1, lease_epoch),
        )
    }

    fn test_execution() -> ActiveWorkerExecution {
        ActiveWorkerExecution::new(
            WorkerRuntimeConfig::default(),
            std::sync::Arc::new(NoopScanSource),
        )
    }

    fn test_plan_descriptor(plan_id: u64) -> protocol::PlanFlowDescriptor {
        protocol::PlanFlowDescriptor {
            plan_id,
            page_kind: 0x504c,
            page_flags: 0,
        }
    }

    fn start_test_execution(
        execution: &mut ActiveWorkerExecution,
        peer: BackendLeaseSlot,
        session_epoch: u64,
    ) {
        execution
            .runtime
            .accept_backend_control(
                peer,
                protocol::BackendExecutionToWorkerRef::StartExecution {
                    session_epoch,
                    plan: test_plan_descriptor(1),
                    options: protocol::ExecutionOptionsWire::default(),
                    scans: protocol::ScanChannelSetRef::empty(),
                },
            )
            .expect("start test execution");
    }

    fn test_empty_logical_plan() -> datafusion::logical_expr::LogicalPlan {
        datafusion::logical_expr::LogicalPlan::EmptyRelation(
            datafusion::logical_expr::logical_plan::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(datafusion_common::DFSchema::empty()),
            },
        )
    }

    fn start_planning_test_execution(
        execution: &mut ActiveWorkerExecution,
        peer: BackendLeaseSlot,
        session_epoch: u64,
    ) -> Box<::worker::PendingPhysicalPlanning> {
        start_test_execution(execution, peer, session_epoch);

        let (_page_region, page_pool) = init_page_pool_for_test();
        let (_issuance_region, issuance_pool) = init_issuance_pool_for_test();
        let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
        let rx = IssuedRx::new(transfer::PageRx::new(page_pool), issuance_pool);
        let plan_descriptor = test_plan_descriptor(1);
        let flow = plan_flow::FlowId {
            session_epoch,
            plan_id: plan_descriptor.plan_id,
        };
        let open =
            plan_flow::PlanOpen::new(flow, plan_descriptor.page_kind, plan_descriptor.page_flags);
        let plan = test_empty_logical_plan();
        let mut backend = plan_flow::BackendPlanRole::new(tx);
        backend.open(open, &plan).expect("open test plan flow");

        loop {
            match backend.step().expect("step test plan flow") {
                plan_flow::BackendPlanStep::OutboundPage { flow, outbound } => {
                    let frame = outbound.frame();
                    outbound.mark_sent();
                    assert!(matches!(
                        execution
                            .runtime
                            .accept_issued_plan_frame(peer, &rx, &frame)
                            .expect("accept plan page"),
                        WorkerRuntimeStep::PlanFrameAccepted { .. }
                    ));
                    assert_eq!(flow.session_epoch, session_epoch);
                }
                plan_flow::BackendPlanStep::CloseFrame { flow, frame } => {
                    let step = execution
                        .runtime
                        .accept_issued_plan_frame(peer, &rx, &frame)
                        .expect("accept plan close");
                    let WorkerRuntimeStep::PlanningStarted(pending) = step else {
                        panic!("expected planning start, got {step:?}");
                    };
                    assert_eq!(pending.flow(), flow);
                    backend.close().expect("close test plan flow");
                    return pending;
                }
                plan_flow::BackendPlanStep::Blocked { .. } => {
                    panic!("test plan flow unexpectedly blocked")
                }
                plan_flow::BackendPlanStep::LogicalError { message, .. } => {
                    panic!("test plan flow failed: {message}")
                }
            }
        }
    }

    fn start_running_test_execution(
        execution: &mut ActiveWorkerExecution,
        peer: BackendLeaseSlot,
        session_epoch: u64,
    ) {
        let pending = start_planning_test_execution(execution, peer, session_epoch);
        let flow = pending.flow();
        let plan = futures::executor::block_on(pending.plan()).expect("build test physical plan");
        let (step, planning_err) = execution
            .runtime
            .finish_physical_planning_step(peer, flow, Ok(plan))
            .expect("finish test physical planning");
        assert!(planning_err.is_none());
        assert!(matches!(step, WorkerRuntimeStep::PhysicalPlanReady(_)));
        assert_eq!(
            execution.runtime.state(),
            ::worker::fsm::WorkerExecutionState::Running
        );
    }

    fn retained_idle_execution(peer: BackendLeaseSlot) -> ActiveWorkerExecution {
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);
        let step = execution
            .runtime
            .accept_backend_control(
                peer,
                protocol::BackendExecutionToWorkerRef::CancelExecution { session_epoch: 10 },
            )
            .expect("cancel test execution");
        handle_terminal_step(&mut execution, step).expect("cleanup terminal execution");
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        execution
    }

    #[test]
    fn execution_registry_counts_only_active_entries() {
        let peer = test_peer(1);
        let mut registry = ExecutionRegistry::with_capacity(4);
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);

        registry.retain_after_poll(peer, execution);
        assert_eq!(registry.active_len(), 1);
        assert!(registry.contains_active(peer));
        assert!(registry.entries.capacity() >= 4);
        assert!(registry.active_peers.capacity() >= 4);

        let mut execution = registry.remove(peer).expect("active entry");
        assert_eq!(registry.active_len(), 0);
        let step = execution
            .runtime
            .accept_backend_control(
                peer,
                protocol::BackendExecutionToWorkerRef::CancelExecution { session_epoch: 10 },
            )
            .expect("cancel active execution");
        handle_terminal_step(&mut execution, step).expect("cleanup terminal execution");

        registry.retain_after_poll(peer, execution);
        assert_eq!(registry.active_len(), 0);
        assert!(!registry.contains_active(peer));
        assert!(registry.entries.contains_key(&peer));
    }

    #[test]
    fn execution_registry_retained_entries_do_not_consume_active_capacity() {
        let retained_peer = test_peer(1);
        let active_peer = test_peer(2);
        let mut registry = ExecutionRegistry::with_capacity(2);

        registry.retain_after_poll(retained_peer, retained_idle_execution(retained_peer));
        assert_eq!(registry.active_len(), 0);
        assert!(!registry.contains_active(retained_peer));

        let mut active = test_execution();
        start_test_execution(&mut active, active_peer, 20);
        registry.retain_after_poll(active_peer, active);

        assert_eq!(registry.active_len(), 1);
        assert!(registry.contains_active(active_peer));
        assert!(registry.entries.contains_key(&retained_peer));
    }

    #[test]
    fn execution_registry_can_evict_retained_idle_entries() {
        let peer = test_peer(1);
        let mut registry = ExecutionRegistry::with_capacity(1);
        registry.retain_after_poll(peer, retained_idle_execution(peer));

        let execution = registry.remove(peer).expect("retained entry");
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert_eq!(registry.active_len(), 0);
        assert!(!registry.entries.contains_key(&peer));
    }

    #[test]
    fn execution_registry_bounds_retained_entries_by_slot_id() {
        let old_peer = test_peer_incarnation(1, 2);
        let new_peer = test_peer_incarnation(1, 3);
        let mut registry = ExecutionRegistry::with_capacity(1);

        registry.retain_after_poll(old_peer, retained_idle_execution(old_peer));
        registry.retain_after_poll(new_peer, retained_idle_execution(new_peer));

        assert_eq!(registry.active_len(), 0);
        assert_eq!(registry.entries.len(), 1);
        assert!(!registry.entries.contains_key(&old_peer));
        assert!(registry.entries.contains_key(&new_peer));
        assert_eq!(registry.retained_by_slot.get(&1), Some(&new_peer));
    }

    #[test]
    fn execution_registry_active_peer_evicts_retained_entry_for_same_slot() {
        let retained_peer = test_peer_incarnation(1, 2);
        let active_peer = test_peer_incarnation(1, 3);
        let mut registry = ExecutionRegistry::with_capacity(1);
        registry.retain_after_poll(retained_peer, retained_idle_execution(retained_peer));

        let mut active = test_execution();
        start_test_execution(&mut active, active_peer, 20);
        registry.retain_after_poll(active_peer, active);

        assert_eq!(registry.active_len(), 1);
        assert!(registry.contains_active(active_peer));
        assert!(!registry.entries.contains_key(&retained_peer));
        assert!(!registry.retained_by_slot.contains_key(&1));
    }

    #[test]
    fn retained_cancel_routes_through_runtime_polling() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        registry.retain_after_poll(peer, retained_idle_execution(peer));
        assert_eq!(
            registry.peer_scheduler_state(peer),
            PeerSchedulerState::Retained
        );

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecution { session_epoch: 10 },
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        assert_eq!(
            registry.peer_scheduler_state(peer),
            PeerSchedulerState::Retained
        );
        assert!(registry.entries.contains_key(&peer));
    }

    #[test]
    fn retained_fail_routes_through_runtime_polling() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        registry.retain_after_poll(peer, retained_idle_execution(peer));
        assert_eq!(
            registry.peer_scheduler_state(peer),
            PeerSchedulerState::Retained
        );

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::FailExecution {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                detail: None,
            },
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        assert_eq!(
            registry.peer_scheduler_state(peer),
            PeerSchedulerState::Retained
        );
        assert!(registry.entries.contains_key(&peer));
    }

    #[test]
    fn retained_cancel_reservation_preserves_pending_terminal() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        let mut execution = retained_idle_execution(peer);
        execution.pending_terminal = Some(PendingTerminalControl::Fail {
            session_epoch: 10,
            code: ExecutionFailureCode::Internal,
            detail: Some("deferred failure".into()),
        });
        registry.retain_after_poll(peer, execution);
        assert_eq!(
            registry.peer_scheduler_state(peer),
            PeerSchedulerState::Retained
        );

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecutionReservation,
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        let execution = registry.entries.get(&peer).expect("retained execution");
        assert_eq!(
            registry.peer_scheduler_state(peer),
            PeerSchedulerState::Retained
        );
        assert_eq!(
            execution.pending_terminal,
            Some(PendingTerminalControl::Fail {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                detail: Some("deferred failure".into()),
            })
        );
        assert!(drain_worker_execution_frames(&mut transport.backend).is_empty());
    }

    #[test]
    fn execution_reservation_counts_until_start_becomes_active() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);

        assert!(
            grant_execution_reservation(&mut registry, &mut transport.worker, peer)
                .expect("grant reservation")
        );
        assert!(registry.contains_reserved(peer));
        assert_eq!(registry.admitted_len(), 1);
        assert_eq!(
            transport.recv_worker_execution(),
            protocol::WorkerExecutionToBackend::ExecutionSlotReserved
        );

        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);
        registry.retain_after_poll(peer, execution);

        assert!(!registry.contains_reserved(peer));
        assert!(registry.contains_active(peer));
        assert_eq!(registry.admitted_len(), 1);
    }

    #[test]
    fn queued_execution_reservation_grants_after_active_slot_frees() {
        let mut transport = TestTransport::with_slots(2, 4096);
        let active_peer = transport.peer();
        let mut queued_backend = transport.acquire_backend();
        let queued_peer = queued_backend.backend_lease_slot();
        let mut registry = ExecutionRegistry::with_capacity(2);

        let mut active = test_execution();
        start_test_execution(&mut active, active_peer, 10);
        registry.retain_after_poll(active_peer, active);
        registry.queue_reservation(queued_peer);

        grant_queued_execution_reservations(&mut registry, &mut transport.worker, 1)
            .expect("capacity full keeps reservation queued");
        assert!(registry.contains_queued(queued_peer));
        assert_eq!(registry.admitted_len(), 1);

        let mut active = registry.remove(active_peer).expect("active execution");
        let step = active
            .runtime
            .accept_backend_control(
                active_peer,
                protocol::BackendExecutionToWorkerRef::CancelExecution { session_epoch: 10 },
            )
            .expect("cancel active execution");
        handle_terminal_step(&mut active, step).expect("cleanup active execution");
        registry.retain_after_poll(active_peer, active);

        grant_queued_execution_reservations(&mut registry, &mut transport.worker, 1)
            .expect("free capacity grants queued reservation");
        assert!(!registry.contains_queued(queued_peer));
        assert!(registry.contains_reserved(queued_peer));
        assert_eq!(
            recv_worker_execution_from(&mut queued_backend),
            protocol::WorkerExecutionToBackend::ExecutionSlotReserved
        );
    }

    #[test]
    fn new_reservation_request_waits_behind_existing_queue() {
        let mut transport = TestTransport::with_slots(2, 4096);
        let mut newer_backend = transport.acquire_backend();
        let mut older_backend = transport.backend;
        let older_peer = older_backend.backend_lease_slot();
        let newer_peer = newer_backend.backend_lease_slot();
        let mut registry = ExecutionRegistry::with_capacity(2);

        registry.queue_reservation(older_peer);
        handle_requested_execution_reservation(&mut registry, &mut transport.worker, newer_peer, 1)
            .expect("new request is admitted into queue");

        assert!(registry.contains_queued(older_peer));
        assert!(registry.contains_queued(newer_peer));
        assert!(!registry.contains_reserved(newer_peer));

        grant_queued_execution_reservations(&mut registry, &mut transport.worker, 1)
            .expect("grant queued reservation");

        assert!(registry.contains_reserved(older_peer));
        assert!(registry.contains_queued(newer_peer));
        assert!(!registry.contains_reserved(newer_peer));
        assert_eq!(
            recv_worker_execution_from(&mut older_backend),
            protocol::WorkerExecutionToBackend::ExecutionSlotReserved
        );
        let mut scratch = vec![0_u8; 4096];
        assert!(newer_backend
            .from_worker_rx()
            .recv_frame_into(&mut scratch)
            .unwrap()
            .is_none());
    }

    #[test]
    fn detached_reserved_peer_releases_admission_slot() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);

        assert!(
            grant_execution_reservation(&mut registry, &mut transport.worker, peer)
                .expect("grant reservation")
        );
        assert_eq!(
            transport.recv_worker_execution(),
            protocol::WorkerExecutionToBackend::ExecutionSlotReserved
        );
        assert!(registry.contains_reserved(peer));

        let TestTransport {
            mut worker,
            backend,
            region: _region,
            _region_mem,
        } = transport;
        drop(backend);

        let (_page_region, page_pool) = init_page_pool_for_test();
        let (_issuance_region, issuance_pool) = init_issuance_pool_for_test();
        let df_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create datafusion runtime");
        let mut spill_runtime =
            WorkerSpillRuntime::new(::worker::WorkerSpillConfig::default(), 123, 456)
                .expect("create spill runtime");
        let worker_config = WorkerRuntimeConfig::default();
        let scan_source: Arc<dyn ::worker::ScanBatchSource> = Arc::new(NoopScanSource);
        let wake = WorkerSchedulerWake::new().expect("create scheduler wake");
        let (task_event_tx, _task_rx) = mpsc::channel(1);
        let task_tx = WorkerTaskNotifier::new(task_event_tx, wake.sender());

        poll_execution_peer(
            &mut registry,
            &mut worker,
            &df_runtime,
            &mut spill_runtime,
            &test_host_config(4096),
            &worker_config,
            &scan_source,
            &task_tx,
            #[cfg(feature = "pg_test")]
            crate::test_gate::attach(),
            peer,
            page_pool,
            issuance_pool,
            RuntimeMetrics::default(),
        )
        .expect("poll detached reserved peer");

        assert!(!registry.contains_reserved(peer));
        assert_eq!(registry.admitted_len(), 0);
    }

    #[test]
    fn admission_request_reads_reserve_and_cancel_messages() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::ReserveExecutionSlot,
        );
        assert!(matches!(
            recv_backend_admission_request(&mut transport.worker, peer),
            Ok(AdmissionPollOutcome::Requested)
        ));

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecutionReservation,
        );
        assert!(matches!(
            recv_backend_admission_request(&mut transport.worker, peer),
            Ok(AdmissionPollOutcome::Cancelled)
        ));
    }

    #[test]
    fn admission_request_rejects_start_before_reservation_grant() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::StartExecution {
                session_epoch: 10,
                plan: test_plan_descriptor(1),
                options: protocol::ExecutionOptionsWire::default(),
                scans: protocol::ScanChannelSet::empty(),
            },
        );

        let err = recv_backend_admission_request(&mut transport.worker, peer)
            .expect_err("start before grant must fail");
        assert!(
            err.to_string()
                .contains("before execution reservation was granted"),
            "{err}"
        );
    }

    #[test]
    fn reserved_peer_cancel_reservation_before_start_releases_admission_slot() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);

        assert!(
            grant_execution_reservation(&mut registry, &mut transport.worker, peer)
                .expect("grant reservation")
        );
        assert_eq!(
            transport.recv_worker_execution(),
            protocol::WorkerExecutionToBackend::ExecutionSlotReserved
        );
        assert!(registry.contains_reserved(peer));

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecutionReservation,
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        assert!(!registry.contains_reserved(peer));
        assert_eq!(registry.admitted_len(), 0);
        assert!(!registry.entries.contains_key(&peer));
    }

    #[test]
    fn active_receiving_plan_cancel_reservation_reports_failed_execution() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);
        registry.retain_after_poll(peer, execution);

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecutionReservation,
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        let execution = registry.entries.get(&peer).expect("retained execution");
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert!(execution.pending_terminal.is_none());
        assert!(!registry.contains_active(peer));
        assert_fail_execution_frame(
            transport.recv_worker_execution(),
            10,
            "CancelExecutionReservation after execution started",
        );
    }

    #[test]
    fn active_running_cancel_reservation_aborts_task_and_reports_failure() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        let df_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create datafusion runtime");
        let mut execution = test_execution();
        start_running_test_execution(&mut execution, peer, 10);
        execution.task = Some(df_runtime.spawn(async {
            std::future::pending::<()>().await;
        }));
        registry.retain_after_poll(peer, execution);

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecutionReservation,
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        let execution = registry.entries.get(&peer).expect("retained execution");
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert!(execution.task.is_none());
        assert!(execution.pending_terminal.is_none());
        assert!(!registry.contains_active(peer));
        assert_fail_execution_frame(
            transport.recv_worker_execution(),
            10,
            "CancelExecutionReservation after execution started",
        );
    }

    #[test]
    fn active_cancel_reservation_failure_defers_when_terminal_ring_is_full() {
        let mut transport = TestTransport::new(256);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        let mut execution = test_execution();
        start_running_test_execution(&mut execution, peer, 10);
        registry.retain_after_poll(peer, execution);
        fill_worker_to_backend_ring_until_full(&mut transport.worker, peer);

        send_backend_execution_frame(
            &mut transport.backend,
            protocol::BackendExecutionToWorker::CancelExecutionReservation,
        );
        poll_test_execution_peer(&mut registry, &mut transport.worker, peer);

        let execution = registry.entries.get(&peer).expect("retained execution");
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert!(!registry.contains_active(peer));
        assert!(matches!(
            execution.pending_terminal,
            Some(PendingTerminalControl::Fail {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                ..
            })
        ));

        let filled = drain_worker_execution_frames(&mut transport.backend);
        assert!(!filled.is_empty());
        assert!(filled
            .iter()
            .all(|frame| matches!(frame, WorkerExecutionToBackend::ExecutionSlotReserved)));

        let mut execution = registry.remove(peer).expect("retained execution");
        retry_pending_terminal_control(&mut transport.worker, &mut execution, peer)
            .expect("retry pending failure after drain");
        assert!(execution.pending_terminal.is_none());
        registry.retain_after_poll(peer, execution);

        assert_fail_execution_frame(
            transport.recv_worker_execution(),
            10,
            "CancelExecutionReservation after execution started",
        );
    }

    #[test]
    fn execution_registry_removes_only_matching_active_terminal_task_event() {
        let peer = test_peer(1);
        let mut registry = ExecutionRegistry::with_capacity(1);
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);
        registry.retain_after_poll(peer, execution);

        assert!(registry.remove_active_session(peer, 11).is_none());
        assert_eq!(registry.active_len(), 1);
        assert!(registry.entries.contains_key(&peer));

        let removed = registry
            .remove_active_session(peer, 10)
            .expect("matching active session");
        assert_eq!(removed.runtime.active_peer(), Some(peer));
        assert_eq!(registry.active_len(), 0);
        assert!(!registry.entries.contains_key(&peer));
    }

    #[test]
    fn execution_registry_ignores_retained_terminal_task_event() {
        let peer = test_peer(1);
        let mut registry = ExecutionRegistry::with_capacity(1);
        registry.retain_after_poll(peer, retained_idle_execution(peer));

        assert!(registry.remove_active_session(peer, 10).is_none());
        assert_eq!(registry.active_len(), 0);
        assert!(registry.entries.contains_key(&peer));
        assert_eq!(registry.retained_by_slot.get(&peer.slot_id()), Some(&peer));
    }

    #[test]
    fn stale_physical_planning_result_preserves_current_task_handle() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        let df_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create datafusion runtime");
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 11);
        execution.task = Some(df_runtime.spawn(async {
            std::future::pending::<()>().await;
        }));
        registry.retain_after_poll(peer, execution);

        let (_page_region, page_pool) = init_page_pool_for_test();
        let (_issuance_region, issuance_pool) = init_issuance_pool_for_test();
        let mut spill_runtime =
            WorkerSpillRuntime::new(::worker::WorkerSpillConfig::default(), 123, 456)
                .expect("create spill runtime");
        let wake = WorkerSchedulerWake::new().expect("create scheduler wake");
        let (task_event_tx, _task_rx) = mpsc::channel(1);
        let task_tx = WorkerTaskNotifier::new(task_event_tx, wake.sender());

        handle_task_event(
            &mut registry,
            &mut transport.worker,
            &df_runtime,
            &mut spill_runtime,
            &test_host_config(4096),
            page_pool,
            issuance_pool,
            RuntimeMetrics::default(),
            &task_tx,
            #[cfg(feature = "pg_test")]
            crate::test_gate::attach(),
            WorkerTaskEvent::PhysicalPlanningFinished {
                peer,
                flow: plan_flow::FlowId {
                    session_epoch: 10,
                    plan_id: 1,
                },
                result: Err(protocol_error("stale planning result")),
            },
        )
        .expect("stale planning result is ignored");

        let execution = registry.entries.get(&peer).expect("current execution");
        assert!(registry.contains_active(peer));
        assert!(execution.task.is_some());

        let mut execution = registry.remove(peer).expect("current execution");
        execution.abort_task();
    }

    #[test]
    fn active_physical_planning_failure_reports_failed_execution() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut registry = ExecutionRegistry::with_capacity(1);
        let df_runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create datafusion runtime");
        let mut execution = test_execution();
        let pending = start_planning_test_execution(&mut execution, peer, 10);
        let flow = pending.flow();
        drop(pending);
        registry.retain_after_poll(peer, execution);

        let (_page_region, page_pool) = init_page_pool_for_test();
        let (_issuance_region, issuance_pool) = init_issuance_pool_for_test();
        let mut spill_runtime =
            WorkerSpillRuntime::new(::worker::WorkerSpillConfig::default(), 123, 456)
                .expect("create spill runtime");
        let wake = WorkerSchedulerWake::new().expect("create scheduler wake");
        let (task_event_tx, _task_rx) = mpsc::channel(1);
        let task_tx = WorkerTaskNotifier::new(task_event_tx, wake.sender());

        handle_task_event(
            &mut registry,
            &mut transport.worker,
            &df_runtime,
            &mut spill_runtime,
            &test_host_config(4096),
            page_pool,
            issuance_pool,
            RuntimeMetrics::default(),
            &task_tx,
            #[cfg(feature = "pg_test")]
            crate::test_gate::attach(),
            WorkerTaskEvent::PhysicalPlanningFinished {
                peer,
                flow,
                result: Err(protocol_error("physical planning failed for test")),
            },
        )
        .expect("planning failure is reported to backend");

        let execution = registry.entries.get(&peer).expect("retained execution");
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert!(execution.pending_terminal.is_none());
        assert!(!registry.contains_active(peer));

        match transport.recv_worker_execution() {
            WorkerExecutionToBackend::FailExecution {
                session_epoch,
                code,
                detail,
            } => {
                assert_eq!(session_epoch, 10);
                assert_eq!(code, ExecutionFailureCode::Internal);
                assert!(detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("physical planning failed for test")));
            }
            other => panic!("unexpected worker frame: {other:?}"),
        }
    }

    #[test]
    fn execution_spill_dir_failure_reports_failed_execution() {
        let mut transport = TestTransport::new(4096);
        let peer = transport.peer();
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);
        execution.worker_start_ns = Some(RuntimeMetrics::default().now_ns());

        let tmp = TestTempDir::new();
        let spill_config =
            ::worker::WorkerSpillConfig::new(Some(1024 * 1024), Some(tmp.path().to_path_buf()));
        let mut spill_runtime =
            WorkerSpillRuntime::new(spill_config, 123, 456).expect("create spill runtime");
        let active_dir = spill_runtime
            .active_dir()
            .expect("active spill dir")
            .to_path_buf();
        std::fs::remove_dir_all(&active_dir).expect("remove active spill dir");
        std::fs::write(&active_dir, b"not a directory").expect("replace active dir with file");

        let result = execution_spill_dir_or_fail(
            &mut transport.worker,
            &mut execution,
            &mut spill_runtime,
            &test_host_config(4096),
            RuntimeMetrics::default(),
            peer,
            10,
        )
        .expect("spill dir failure is contained");

        assert!(result.is_none());
        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        let frame = transport.recv_worker_execution();
        match frame {
            protocol::WorkerExecutionToBackend::FailExecution {
                session_epoch,
                code,
                detail,
            } => {
                assert_eq!(session_epoch, 10);
                assert_eq!(code, ExecutionFailureCode::Internal);
                assert!(detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("create execution spill directory")));
            }
            other => panic!("unexpected worker frame: {other:?}"),
        }
    }

    #[test]
    fn complete_execution_terminal_send_full_defers_and_retries() {
        let mut transport = TestTransport::new(128);
        let peer = transport.peer();
        fill_worker_to_backend_ring_until_full(&mut transport.worker, peer);

        let mut execution = test_execution();
        start_running_test_execution(&mut execution, peer, 10);
        finish_execution_task(
            &mut transport.worker,
            &mut execution,
            &test_host_config(128),
            RuntimeMetrics::default(),
            peer,
            10,
            Ok(()),
        )
        .expect("full terminal ring should defer complete");

        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert_eq!(
            execution.pending_terminal,
            Some(PendingTerminalControl::Complete { session_epoch: 10 })
        );

        let filled = drain_worker_execution_frames(&mut transport.backend);
        assert!(!filled.is_empty());
        assert!(filled
            .iter()
            .all(|frame| matches!(frame, WorkerExecutionToBackend::ExecutionSlotReserved)));

        retry_pending_terminal_control(&mut transport.worker, &mut execution, peer)
            .expect("retry complete after drain");
        assert!(execution.pending_terminal.is_none());
        assert_eq!(
            transport.recv_worker_execution(),
            WorkerExecutionToBackend::CompleteExecution { session_epoch: 10 }
        );
    }

    #[test]
    fn fail_execution_terminal_send_full_defers_and_retries_with_detail() {
        let mut transport = TestTransport::new(256);
        let peer = transport.peer();
        fill_worker_to_backend_ring_until_full(&mut transport.worker, peer);

        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);
        finish_execution_task(
            &mut transport.worker,
            &mut execution,
            &test_host_config(256),
            RuntimeMetrics::default(),
            peer,
            10,
            Err(protocol_error("deferred terminal failure")),
        )
        .expect("full terminal ring should defer failure");

        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert!(matches!(
            execution.pending_terminal,
            Some(PendingTerminalControl::Fail {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                ..
            })
        ));

        let filled = drain_worker_execution_frames(&mut transport.backend);
        assert!(!filled.is_empty());
        retry_pending_terminal_control(&mut transport.worker, &mut execution, peer)
            .expect("retry failure after drain");
        assert!(execution.pending_terminal.is_none());

        match transport.recv_worker_execution() {
            WorkerExecutionToBackend::FailExecution {
                session_epoch,
                code,
                detail,
            } => {
                assert_eq!(session_epoch, 10);
                assert_eq!(code, ExecutionFailureCode::Internal);
                assert!(detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("deferred terminal failure")));
            }
            other => panic!("unexpected worker frame: {other:?}"),
        }
    }

    #[test]
    fn worker_failure_detail_fits_asymmetric_primary_control_scratch() {
        let config = test_host_config(4096);
        assert!(
            config.control_backend_to_worker_capacity < config.control_worker_to_backend_capacity
        );
        let mut transport = TestTransport::with_host_config(&config);
        let peer = transport.peer();
        let mut execution = test_execution();
        start_test_execution(&mut execution, peer, 10);

        let detail = "asymmetric worker scratch execution failure ".repeat(128);
        finish_execution_task(
            &mut transport.worker,
            &mut execution,
            &config,
            RuntimeMetrics::default(),
            peer,
            10,
            Err(protocol_error(&detail)),
        )
        .expect("worker failure detail should fit resized scratch");

        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert_fail_execution_frame(
            transport.recv_worker_execution(),
            10,
            "asymmetric worker scratch execution failure",
        );
    }

    #[test]
    fn planning_failure_detail_fits_asymmetric_primary_control_scratch() {
        let config = test_host_config(4096);
        assert!(
            config.control_backend_to_worker_capacity < config.control_worker_to_backend_capacity
        );
        let mut transport = TestTransport::with_host_config(&config);
        let peer = transport.peer();
        let mut execution = test_execution();
        let pending = start_planning_test_execution(&mut execution, peer, 10);
        let flow = pending.flow();
        drop(pending);

        let detail = "asymmetric worker scratch planning failure ".repeat(128);
        let (step, planning_err) = execution
            .runtime
            .finish_physical_planning_step(peer, flow, Err(protocol_error(&detail)))
            .expect("finish failed physical planning");
        let err = planning_err.expect("planning error");

        finish_failed_terminal_step(
            &mut transport.worker,
            &mut execution,
            &config,
            peer,
            err,
            step,
        )
        .expect("planning failure detail should fit resized scratch");

        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert_fail_execution_frame(
            transport.recv_worker_execution(),
            10,
            "asymmetric worker scratch planning failure",
        );
    }

    #[test]
    fn detached_terminal_send_cleans_without_pending_retry() {
        let mut transport = TestTransport::new(128);
        let peer = transport.peer();
        transport.backend.release();

        let mut execution = test_execution();
        start_running_test_execution(&mut execution, peer, 10);
        finish_execution_task(
            &mut transport.worker,
            &mut execution,
            &test_host_config(128),
            RuntimeMetrics::default(),
            peer,
            10,
            Ok(()),
        )
        .expect("detached terminal peer should be contained");

        assert_eq!(execution.runtime.active_peer(), None);
        assert_eq!(execution.runtime.retained_peer(), Some(peer));
        assert!(execution.pending_terminal.is_none());
    }

    #[test]
    fn terminal_send_config_error_remains_fatal() {
        let mut transport = TestTransport::new(5);
        let peer = transport.peer();
        let mut execution = test_execution();
        start_running_test_execution(&mut execution, peer, 10);

        let err = finish_execution_task(
            &mut transport.worker,
            &mut execution,
            &test_host_config(5),
            RuntimeMetrics::default(),
            peer,
            10,
            Ok(()),
        )
        .expect_err("terminal frame larger than scratch remains fatal");

        assert!(matches!(err, WorkerRuntimeError::ControlFrameTooLarge));
        assert!(matches!(
            execution.pending_terminal,
            Some(PendingTerminalControl::Complete { session_epoch: 10 })
        ));
    }

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
                let Some(spill_dir) = execution_spill_dir_or_fail(
                    transport,
                    execution,
                    spill_runtime,
                    config,
                    metrics,
                    peer,
                    result.session_epoch,
                )?
                else {
                    continue;
                };
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

fn execution_spill_dir_or_fail(
    transport: &mut TransportWorkerRuntime,
    execution: &mut ActiveWorkerExecution,
    spill_runtime: &mut WorkerSpillRuntime,
    config: &crate::HostConfig,
    metrics: RuntimeMetrics,
    peer: BackendLeaseSlot,
    session_epoch: u64,
) -> Result<Option<ExecutionSpillDir>, WorkerRuntimeError> {
    match spill_runtime.execution_dir(peer, session_epoch) {
        Ok(spill_dir) => Ok(Some(spill_dir)),
        Err(err) => {
            finish_execution_task(
                transport,
                execution,
                config,
                metrics,
                peer,
                session_epoch,
                Err(err),
            )?;
            Ok(None)
        }
    }
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
