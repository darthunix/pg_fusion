#![doc = include_str!("../README.md")]

mod error;
mod explain;
mod fsm;
mod source;

use arrow_layout::{ColumnSpec, TypeTag};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use control_transport::{BackendLeaseSlot, BackendSlotLease, CommitOutcome, TransportRegion};
use datafusion_common::ScalarValue;
use datafusion_expr::logical_plan::LogicalPlan;
use fsm::backend_execution_flow::StateMachine as BackendExecutionMachine;
pub use fsm::{BackendExecutionAction, BackendExecutionEvent, BackendExecutionState};
use issuance::{encode_issued_frame, IssuedTx};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::{PgRelation as PgrxRelation, PgTryBuilder};
use plan_builder::{PlanBuildInput, PlanBuilder};
use plan_flow::{BackendPlanRole, BackendPlanStep, PlanOpen};
use row_estimator::{EstimatorConfig, PageRowEstimator};
use row_estimator_seed::{seed_estimator_config, ProjectedColumnRef};
use runtime_metrics::{MetricId, PageDirection, RuntimeMetrics};
use runtime_protocol::{
    decode_worker_scan_to_backend, encode_backend_scan_to_worker_into, BackendExecutionToWorker,
    BackendLeaseSlotWire, BackendScanToWorker, ExecutionFailureCode, PlanFlowDescriptor,
    ProducerRole, ScanChannelDescriptorWire, ScanChannelSet, ScanFlowDescriptorRef,
    WorkerScanToBackendRef,
};
use scan_flow::{
    BackendProducerRole, BackendProducerStep, BackendScanCoordinator, FlowId as ScanFlowId,
    LogicalTerminal, ProducerDescriptor, ProducerRoleKind, ScanOpen,
};
use scan_node::PgScanSpec;
use scan_sql::{render_unprojected_ctid_block_scan_sql, render_unprojected_scan_sql};
use slot_scan::{prepare_scan, ExecutionSpiContext, PreparedScan, ScanOptions};
pub use slot_scan::{DiagnosticLogLevel, DiagnosticsConfig};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
#[cfg(not(test))]
use std::fs::{create_dir_all, File, OpenOptions};
#[cfg(not(test))]
use std::io::Write;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
#[cfg(not(test))]
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use error::BackendServiceError;

const PLAN_ID: u64 = 1;
const SINGLE_SCAN_PRODUCER_ID: u16 = 0;
const STANDALONE_OPEN_SCAN_TIMEOUT: Duration = Duration::from_secs(30);
const STANDALONE_TERMINAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

thread_local! {
    static CURRENT_SESSION_EPOCH: Cell<u64> = const { Cell::new(0) };
    static ACTIVE_EXECUTION: RefCell<Option<ActiveExecution>> = const { RefCell::new(None) };
    static ACTIVE_DIAGNOSTICS: RefCell<DiagnosticsConfig> = RefCell::new(DiagnosticsConfig::default());
}

#[cfg(not(test))]
thread_local! {
    static LOG_FILE: RefCell<Option<CachedLogFile>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "pg_test"))]
thread_local! {
    static WAIT_FOR_SCAN_BACKPRESSURE_ERROR_FOR_TESTS: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendServiceConfig {
    pub scan_fetch_batch_rows: u32,
    pub scan_batch_channel_capacity: u32,
    pub scan_idle_poll_interval_us: u32,
    pub estimator_default: EstimatorConfig,
    pub join_reordering_enabled: bool,
    pub plan_page_kind: u16,
    pub plan_page_flags: u16,
    pub scan_page_kind: u16,
    pub scan_page_flags: u16,
    pub diagnostics: DiagnosticsConfig,
    pub metrics: RuntimeMetrics,
    pub scan_timing_detail: bool,
}

impl Default for BackendServiceConfig {
    fn default() -> Self {
        Self {
            scan_fetch_batch_rows: 1024,
            scan_batch_channel_capacity: 8,
            scan_idle_poll_interval_us: 100,
            estimator_default: EstimatorConfig::default(),
            join_reordering_enabled: true,
            plan_page_kind: 0x504c,
            plan_page_flags: 0,
            scan_page_kind: import::ARROW_LAYOUT_BATCH_KIND,
            scan_page_flags: 0,
            diagnostics: DiagnosticsConfig::default(),
            metrics: RuntimeMetrics::default(),
            scan_timing_detail: false,
        }
    }
}

impl BackendServiceConfig {
    pub fn plan_builder_config(&self) -> plan_builder::PlanBuilderConfig {
        plan_builder::PlanBuilderConfig {
            join_reordering_enabled: self.join_reordering_enabled,
            ..plan_builder::PlanBuilderConfig::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExecutionKey {
    pub slot_id: u32,
    pub session_epoch: u64,
}

#[derive(Debug)]
pub struct ExecutionSnapshot {
    pub snapshot: pg_sys::Snapshot,
    pub owner: pg_sys::ResourceOwner,
}

impl ExecutionSnapshot {
    fn capture_current() -> Result<Self, BackendServiceError> {
        unsafe {
            let snapshot = pg_sys::GetActiveSnapshot();
            if snapshot.is_null() {
                return Err(BackendServiceError::MissingActiveSnapshot);
            }
            let owner = pg_sys::CurrentResourceOwner;
            if owner.is_null() {
                return Err(BackendServiceError::MissingResourceOwner);
            }
            let registered = pg_sys::RegisterSnapshotOnOwner(snapshot, owner);
            Ok(Self {
                snapshot: registered,
                owner,
            })
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    fn unregistered_for_tests() -> Self {
        Self {
            snapshot: std::ptr::null_mut(),
            owner: std::ptr::null_mut(),
        }
    }
}

impl Drop for ExecutionSnapshot {
    fn drop(&mut self) {
        backend_diag_trace(|| {
            format!(
                "backend_service drop ExecutionSnapshot snapshot={:p} owner={:p} current_mcxt={:p}",
                self.snapshot,
                self.owner,
                diagnostic_current_memory_context()
            )
        });
        unsafe {
            if !self.snapshot.is_null() && !self.owner.is_null() {
                pg_sys::UnregisterSnapshotFromOwner(self.snapshot, self.owner);
            }
        }
    }
}

pub struct StartExecutionInput<'a> {
    pub slot_id: u32,
    pub sql: &'a str,
    pub params: Vec<ScalarValue>,
    pub plan_tx: IssuedTx,
    pub scan_slot_region: &'a TransportRegion,
    pub config: BackendServiceConfig,
    pub scan_worker_launcher: Option<&'a mut dyn ScanWorkerLauncher>,
}

pub trait ScanWorkerLauncher {
    fn prepare_query(
        &mut self,
        input: ScanWorkerQueryInput<'_>,
    ) -> Result<(), BackendServiceError> {
        let _ = input;
        Ok(())
    }

    fn launch_scan_workers(
        &mut self,
        input: ScanWorkerLaunchInput<'_>,
    ) -> Result<ScanWorkerLaunchOutput, BackendServiceError>;
}

pub struct ScanWorkerQueryInput<'a> {
    pub scans: &'a [Arc<PgScanSpec>],
}

pub struct ScanWorkerLaunchInput<'a> {
    pub session_epoch: u64,
    pub scan_id: u64,
    pub spec: &'a PgScanSpec,
    pub leader_peer: BackendLeaseSlot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanWorkerProducer {
    pub producer_id: u16,
    pub role: ProducerRoleKind,
    pub peer: BackendLeaseSlot,
}

impl ScanWorkerProducer {
    pub fn leader(producer_id: u16, peer: BackendLeaseSlot) -> Self {
        Self {
            producer_id,
            role: ProducerRoleKind::Leader,
            peer,
        }
    }

    pub fn worker(producer_id: u16, peer: BackendLeaseSlot) -> Self {
        Self {
            producer_id,
            role: ProducerRoleKind::Worker,
            peer,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanWorkerLaunchOutput {
    pub leader_ctid_range: Option<CtidBlockRange>,
    pub workers: Vec<ScanWorkerProducer>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BeginExecutionOutput {
    pub key: ExecutionKey,
    pub plan: PlanFlowDescriptor,
    pub options: runtime_protocol::ExecutionOptionsWire,
    pub scan_channels: Box<[ScanChannelDescriptorWire]>,
}

impl BeginExecutionOutput {
    pub fn control(&self) -> BackendExecutionToWorker<'_> {
        BackendExecutionToWorker::StartExecution {
            session_epoch: self.key.session_epoch,
            plan: self.plan,
            options: self.options,
            scans: ScanChannelSet::new(&self.scan_channels)
                .expect("begin_execution scan channels must remain unique"),
        }
    }
}

pub type ExecutionStartStep = BackendPlanStep;

pub struct OpenScanInput<'a> {
    pub peer: BackendLeaseSlot,
    pub session_epoch: u64,
    pub scan_id: u64,
    pub scan: ScanFlowDescriptorRef<'a>,
    pub scan_tx: IssuedTx,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneScanField {
    pub name: String,
    pub type_tag: u16,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneScanDescriptor {
    pub sql: String,
    pub table_oid: u32,
    pub fields: Vec<StandaloneScanField>,
    pub source_projection: Vec<usize>,
    pub planner_fetch_hint: Option<usize>,
    pub local_row_cap: Option<usize>,
}

pub struct StandaloneScanProducerInput {
    pub descriptor: StandaloneScanDescriptor,
    pub session_epoch: u64,
    pub scan_id: u64,
    pub producer_id: u16,
    pub producer_count: u16,
    pub scan_lease: BackendSlotLease,
    pub scan_tx: IssuedTx,
    pub config: BackendServiceConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CtidBlockRange {
    pub start_block: u64,
    pub end_block: u64,
}

#[derive(Debug)]
pub enum ScanStreamStep {
    OutboundPage {
        flow: ScanFlowId,
        producer_id: u16,
        outbound: issuance::IssuedOutboundPage,
    },
    YieldForControl {
        reason: ScanYieldReason,
    },
    Finished {
        flow: ScanFlowId,
    },
    Failed {
        flow: ScanFlowId,
        producer_id: u16,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanYieldReason {
    PermitBackpressure,
}

pub struct ExplainInput<'a> {
    pub sql: &'a str,
    pub params: Vec<ScalarValue>,
    pub options: ExplainRenderOptions,
    pub config: BackendServiceConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExplainRenderOptions {
    pub verbose: bool,
    pub costs: bool,
}

impl Default for ExplainRenderOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            costs: true,
        }
    }
}

#[derive(Default)]
pub struct BackendService;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActiveScanDriverState {
    Streaming,
    AwaitingExecutionTerminal,
    Released,
}

#[derive(Debug)]
pub struct ActiveScanDriver {
    key: ExecutionKey,
    scan_id: u64,
    state: ActiveScanDriverState,
    // This handle drives process-local PostgreSQL backend state stored in
    // `ACTIVE_EXECUTION`. Keep it on the creating backend thread so `step()`
    // and `Drop` cannot accidentally consult another thread's empty TLS slot.
    _thread_bound: PhantomData<Rc<()>>,
}

struct StartingRuntime {
    plan_role: BackendPlanRole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanEntryState {
    Prepared,
    Streaming,
    Finished,
    Cancelled,
}

struct PreparedScanEntry {
    scan_id: u64,
    _spec: Arc<PgScanSpec>,
    schema: arrow_schema::SchemaRef,
    prepared_scan: PreparedScan,
    physical_columns: Vec<ColumnSpec>,
    source_projection: Vec<usize>,
    estimator_config: EstimatorConfig,
    canonical_open: ScanOpen,
    producers: Vec<ScanWorkerProducer>,
    leader_peer: BackendLeaseSlot,
    leader_lease: Option<BackendSlotLease>,
    state: ScanEntryState,
}

struct ActiveScanStream {
    scan_id: u64,
    producer: BackendProducerRole<source::SlotScanPageSource>,
    coordinator: BackendScanCoordinator,
}

enum ScanDriveOutcome {
    Continue(ScanStreamStep),
    Blocked,
    Terminal(ScanStreamStep),
    FatalExecution(ScanStreamStep),
}

struct ActiveExecution {
    key: ExecutionKey,
    snapshot: ExecutionSnapshot,
    _logical_plan: LogicalPlan,
    machine: BackendExecutionMachine,
    config: BackendServiceConfig,
    starting: Option<StartingRuntime>,
    scan_spi: Option<ExecutionSpiContext>,
    scans: BTreeMap<u64, PreparedScanEntry>,
    active_scans: BTreeMap<u64, ActiveScanStream>,
}

impl ActiveScanDriver {
    pub fn key(&self) -> ExecutionKey {
        self.key
    }

    pub fn scan_id(&self) -> u64 {
        self.scan_id
    }

    pub fn step(&mut self) -> Result<ScanStreamStep, BackendServiceError> {
        match self.state {
            ActiveScanDriverState::Streaming => {
                let step = step_scan_with_driver(self.key, self.scan_id)?;
                match step {
                    ScanStreamStep::Finished { .. } => {
                        self.state = if execution_ready_for_completion(self.key)? {
                            ActiveScanDriverState::AwaitingExecutionTerminal
                        } else {
                            ActiveScanDriverState::Released
                        };
                    }
                    ScanStreamStep::Failed { .. } => {
                        self.state = ActiveScanDriverState::Released;
                    }
                    ScanStreamStep::OutboundPage { .. }
                    | ScanStreamStep::YieldForControl { .. } => {}
                }
                Ok(step)
            }
            ActiveScanDriverState::AwaitingExecutionTerminal => {
                Err(BackendServiceError::ProtocolViolation(
                    "scan driver is awaiting execution terminal control after scan completion"
                        .into(),
                ))
            }
            ActiveScanDriverState::Released => Err(BackendServiceError::ProtocolViolation(
                "scan driver has already been released".into(),
            )),
        }
    }

    pub fn cancel_scan(&mut self) -> Result<bool, BackendServiceError> {
        if self.state != ActiveScanDriverState::Streaming {
            return Err(BackendServiceError::ProtocolViolation(
                "scan driver cannot cancel a scan after the stream has already terminated".into(),
            ));
        }
        let handled = cancel_scan_from_driver(self.key, self.scan_id)?;
        self.state = ActiveScanDriverState::Released;
        Ok(handled)
    }

    pub fn complete_execution(&mut self) -> Result<bool, BackendServiceError> {
        match self.state {
            ActiveScanDriverState::AwaitingExecutionTerminal => {}
            ActiveScanDriverState::Streaming => {
                return Err(BackendServiceError::ProtocolViolation(
                    "scan driver cannot complete execution before scan stream reaches Finished"
                        .into(),
                ));
            }
            ActiveScanDriverState::Released => {
                return Err(BackendServiceError::ProtocolViolation(
                    "scan driver has already been released".into(),
                ));
            }
        }
        let handled = terminate_current_execution_from_driver(
            self.key,
            self.scan_id,
            BackendExecutionEvent::CompleteExecution,
        )?;
        self.state = ActiveScanDriverState::Released;
        Ok(handled)
    }

    pub fn fail_execution(
        &mut self,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    ) -> Result<bool, BackendServiceError> {
        if self.state == ActiveScanDriverState::Released {
            return Err(BackendServiceError::ProtocolViolation(
                "scan driver has already been released".into(),
            ));
        }
        let _ = (code, detail);
        let handled = terminate_current_execution_from_driver(
            self.key,
            self.scan_id,
            BackendExecutionEvent::FailExecution,
        )?;
        self.state = ActiveScanDriverState::Released;
        Ok(handled)
    }

    pub fn cancel_execution(&mut self) -> Result<bool, BackendServiceError> {
        if self.state == ActiveScanDriverState::Released {
            return Err(BackendServiceError::ProtocolViolation(
                "scan driver has already been released".into(),
            ));
        }
        let handled = terminate_current_execution_from_driver(
            self.key,
            self.scan_id,
            BackendExecutionEvent::CancelExecution,
        )?;
        self.state = ActiveScanDriverState::Released;
        Ok(handled)
    }
}

impl Drop for ActiveScanDriver {
    fn drop(&mut self) {
        match self.state {
            ActiveScanDriverState::Streaming => {
                let _ = terminate_current_execution_from_driver(
                    self.key,
                    self.scan_id,
                    BackendExecutionEvent::CancelExecution,
                );
                self.state = ActiveScanDriverState::Released;
            }
            ActiveScanDriverState::AwaitingExecutionTerminal | ActiveScanDriverState::Released => {
                self.state = ActiveScanDriverState::Released;
            }
        }
    }
}

impl BackendService {
    pub fn begin_execution(
        input: StartExecutionInput<'_>,
    ) -> Result<BeginExecutionOutput, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            if slot.borrow().is_some() {
                return Err(BackendServiceError::ExecutionAlreadyActive);
            }
            let config = input.config.clone();
            set_backend_diagnostics(config.diagnostics.clone());

            let session_epoch = bump_current_session_epoch();
            let key = ExecutionKey {
                slot_id: input.slot_id,
                session_epoch,
            };

            let built = PlanBuilder::new()
                .with_config(config.plan_builder_config())
                .build(PlanBuildInput {
                    sql: input.sql,
                    params: input.params,
                })?;

            let mut scan_worker_launcher = input.scan_worker_launcher;
            if let Some(launcher) = scan_worker_launcher.as_deref_mut() {
                launcher.prepare_query(ScanWorkerQueryInput {
                    scans: &built.scans,
                })?;
            }

            let plan_open = PlanOpen::new(
                plan_flow::FlowId {
                    session_epoch,
                    plan_id: PLAN_ID,
                },
                config.plan_page_kind,
                config.plan_page_flags,
            );

            let mut plan_role = BackendPlanRole::new(input.plan_tx);
            plan_role.open(plan_open, &built.logical_plan)?;

            let mut scans = BTreeMap::new();
            let mut scan_channels = Vec::new();
            for spec in built.scans.iter().cloned() {
                let scan_lease = BackendSlotLease::acquire(input.scan_slot_region)?;
                let scan_peer = scan_lease.backend_lease_slot();
                backend_diag_warning(|| {
                    format!(
                    "backend_service begin_execution acquired scan lease slot_id={} session_epoch={} scan_id={} peer_slot_id={} generation={} lease_epoch={} backend_pid={}",
                    key.slot_id,
                    key.session_epoch,
                    spec.scan_id.get(),
                    scan_peer.slot_id(),
                    scan_peer.lease_id().generation(),
                    scan_peer.lease_id().lease_epoch(),
                    std::process::id()
                    )
                });
                let scan_id = spec.scan_id.get();
                let mut producers = vec![ScanWorkerProducer::leader(
                    SINGLE_SCAN_PRODUCER_ID,
                    scan_peer,
                )];
                let mut leader_ctid_range = None;
                if let Some(launcher) = scan_worker_launcher.as_deref_mut() {
                    let mut output = launcher.launch_scan_workers(ScanWorkerLaunchInput {
                        session_epoch,
                        scan_id,
                        spec: spec.as_ref(),
                        leader_peer: scan_peer,
                    })?;
                    leader_ctid_range = output.leader_ctid_range.take();
                    producers.append(&mut output.workers);
                    producers.sort_by_key(|producer| producer.producer_id);
                }
                let entry = prepare_scan_entry(
                    session_epoch,
                    &config,
                    spec,
                    producers,
                    leader_ctid_range,
                    scan_lease,
                )?;
                scan_channels.extend(entry.producers.iter().map(|producer| {
                    ScanChannelDescriptorWire {
                        scan_id: entry.scan_id,
                        producer_id: producer.producer_id,
                        role: match producer.role {
                            ProducerRoleKind::Leader => ProducerRole::Leader,
                            ProducerRoleKind::Worker => ProducerRole::Worker,
                        },
                        peer: BackendLeaseSlotWire::new(
                            producer.peer.slot_id(),
                            producer.peer.lease_id().generation(),
                            producer.peer.lease_id().lease_epoch(),
                        ),
                    }
                }));
                scans.insert(entry.scan_id, entry);
            }

            scan_channels.sort_by_key(|channel| (channel.scan_id, channel.producer_id));
            let scan_channels = scan_channels.into_boxed_slice();

            let snapshot = ExecutionSnapshot::capture_current()?;
            let mut machine = BackendExecutionMachine::new();
            consume_execution_event(&mut machine, BackendExecutionEvent::BeginExecution)?;
            let scan_spi = if scans.is_empty() {
                None
            } else {
                Some(ExecutionSpiContext::connect(config.diagnostics.clone())?)
            };
            let plan = PlanFlowDescriptor {
                plan_id: PLAN_ID,
                page_kind: config.plan_page_kind,
                page_flags: config.plan_page_flags,
            };
            let options = runtime_protocol::ExecutionOptionsWire {
                scan_batch_channel_capacity: config.scan_batch_channel_capacity,
                scan_idle_poll_interval_us: config.scan_idle_poll_interval_us,
            };

            *slot.borrow_mut() = Some(ActiveExecution {
                key,
                snapshot,
                _logical_plan: built.logical_plan,
                machine,
                config,
                starting: Some(StartingRuntime { plan_role }),
                scan_spi,
                scans,
                active_scans: BTreeMap::new(),
            });
            backend_diag_info(|| {
                format!(
                "backend_service begin_execution installed slot_id={} session_epoch={} scans={}",
                key.slot_id,
                key.session_epoch,
                scan_channels.len()
                )
            });

            Ok(BeginExecutionOutput {
                key,
                plan,
                options,
                scan_channels,
            })
        })
    }

    pub fn step_execution_start() -> Result<ExecutionStartStep, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let execution = active
                .as_mut()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            ensure_execution_state(
                execution,
                BackendExecutionState::Starting,
                "step execution start",
            )?;
            let starting = execution
                .starting
                .as_mut()
                .ok_or_else(|| missing_starting_runtime_error("step execution start"))?;
            starting
                .plan_role
                .step()
                .map_err(BackendServiceError::PlanFlow)
        })
    }

    pub fn finalize_execution_start() -> Result<ExecutionKey, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let execution = active
                .as_mut()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            ensure_execution_state(
                execution,
                BackendExecutionState::Starting,
                "finalize execution start",
            )?;

            let starting = execution
                .starting
                .as_mut()
                .ok_or_else(|| missing_starting_runtime_error("finalize execution start"))?;
            starting.plan_role.close()?;
            consume_execution_event(&mut execution.machine, BackendExecutionEvent::PlanPublished)?;
            execution.starting = None;
            Ok(execution.key)
        })
    }

    pub fn abort_execution_start() -> Result<(), BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let execution = active
                .take()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            if execution.machine.state() != &BackendExecutionState::Starting {
                *active = Some(execution);
                return Err(BackendServiceError::InvalidExecutionState {
                    action: "abort execution start",
                    state: *active.as_ref().unwrap().machine.state(),
                });
            }
            backend_diag_warning(|| {
                format!(
                    "backend_service abort_execution_start slot_id={} session_epoch={} state={:?}",
                    execution.key.slot_id,
                    execution.key.session_epoch,
                    execution.machine.state()
                )
            });
            cleanup_execution(execution, Some(BackendExecutionEvent::CancelExecution))
        })
    }

    pub fn open_scan(
        input: OpenScanInput<'_>,
    ) -> Result<Option<ActiveScanDriver>, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let Some(execution) = active.as_mut() else {
                return classify_missing_execution(0, input.session_epoch).map(|_| None);
            };
            backend_diag_info(|| {
                format!(
                    "backend_service open_scan requested slot_id={} session_epoch={} scan_id={} peer={:?} state={:?} active_scans={} unfinished_scans={}",
                    execution.key.slot_id,
                    input.session_epoch,
                    input.scan_id,
                    input.peer,
                    execution.machine.state(),
                    execution.active_scans.len(),
                    execution_unfinished_scan_count(execution)
                )
            });

            if classify_session_epoch_for_scan_open(input.session_epoch)?
                == SessionEpochMatch::Stale
            {
                return Ok(None);
            }
            ensure_execution_state(execution, BackendExecutionState::Running, "open scan")?;
            let snapshot = execution.snapshot.snapshot;
            let block_size = u32::try_from(input.scan_tx.payload_capacity()).map_err(|_| {
                BackendServiceError::ProtocolViolation(
                    "scan payload capacity exceeds u32".into(),
                )
            })?;
            let fetch_batch_rows =
                normalize_scan_fetch_batch_rows(execution.config.scan_fetch_batch_rows);
            let (
                prepared_scan,
                schema,
                physical_columns,
                source_projection,
                estimator_config,
                canonical_open,
            ) = {
                let entry = execution.scans.get_mut(&input.scan_id).ok_or(
                    BackendServiceError::UnknownScanId {
                        scan_id: input.scan_id,
                    },
                )?;
                if entry.leader_peer != input.peer {
                    return Err(BackendServiceError::ScanPeerMismatch {
                        scan_id: input.scan_id,
                        expected: entry.leader_peer,
                        incoming: input.peer,
                    });
                }
                match entry.state {
                    ScanEntryState::Prepared => {}
                    ScanEntryState::Streaming => {
                        return Err(BackendServiceError::ScanAlreadyStreaming {
                            scan_id: input.scan_id,
                        });
                    }
                    ScanEntryState::Finished | ScanEntryState::Cancelled => {
                        return Err(BackendServiceError::ScanAlreadyUsed {
                            scan_id: input.scan_id,
                        });
                    }
                }

                if !scan_descriptor_matches(&entry.canonical_open, input.scan) {
                    return Err(BackendServiceError::ProtocolViolation(format!(
                        "scan descriptor mismatch for scan_id {}",
                        input.scan_id
                    )));
                }

                (
                    entry.prepared_scan.clone(),
                    Arc::clone(&entry.schema),
                    entry.physical_columns.clone(),
                    entry.source_projection.clone(),
                    entry.estimator_config,
                    entry.canonical_open.clone(),
                )
            };
            let spi = execution.scan_spi.clone().ok_or_else(|| {
                BackendServiceError::ProtocolViolation(
                    "execution has no installed shared scan SPI context".into(),
                )
            })?;
            let estimator = PageRowEstimator::new(&physical_columns, block_size, estimator_config)?;

            let source = source::SlotScanPageSource::new(
                snapshot,
                spi,
                prepared_scan,
                schema,
                source_projection,
                block_size,
                fetch_batch_rows,
                estimator,
                execution.config.metrics,
                execution.config.scan_timing_detail,
            );

            let mut coordinator = BackendScanCoordinator::new();
            coordinator.open(ScanOpen::new(
                canonical_open.flow,
                canonical_open.page_kind,
                canonical_open.page_flags,
                vec![ProducerDescriptor::leader(SINGLE_SCAN_PRODUCER_ID)],
            )?)?;

            let mut producer = BackendProducerRole::new(input.scan_tx);
            producer.open(&canonical_open, SINGLE_SCAN_PRODUCER_ID, source)?;

            consume_execution_event(&mut execution.machine, BackendExecutionEvent::OpenScan)?;
            execution
                .scans
                .get_mut(&input.scan_id)
                .expect("scan entry must remain installed while opening")
                .state = ScanEntryState::Streaming;
            execution.active_scans.insert(
                input.scan_id,
                ActiveScanStream {
                    scan_id: input.scan_id,
                    producer,
                    coordinator,
                },
            );
            backend_diag_info(|| {
                format!(
                    "backend_service open_scan installed driver slot_id={} session_epoch={} scan_id={} active_scans={} unfinished_scans={}",
                    execution.key.slot_id,
                    execution.key.session_epoch,
                    input.scan_id,
                    execution.active_scans.len(),
                    execution_unfinished_scan_count(execution)
                )
            });

            Ok(Some(ActiveScanDriver {
                key: execution.key,
                scan_id: input.scan_id,
                state: ActiveScanDriverState::Streaming,
                _thread_bound: PhantomData,
            }))
        })
    }

    pub fn run_standalone_scan_producer(
        input: StandaloneScanProducerInput,
    ) -> Result<(), BackendServiceError> {
        run_standalone_scan_producer(input)
    }

    pub fn accept_complete_execution(
        slot_id: u32,
        session_epoch: u64,
    ) -> Result<bool, BackendServiceError> {
        terminate_current_execution(
            slot_id,
            session_epoch,
            BackendExecutionEvent::CompleteExecution,
        )
    }

    pub fn accept_fail_execution(
        slot_id: u32,
        session_epoch: u64,
        _code: ExecutionFailureCode,
        _detail: Option<u64>,
    ) -> Result<bool, BackendServiceError> {
        let _ = (_code, _detail);
        // TODO: preserve worker failure code/detail for backend-side telemetry or debug state.
        terminate_current_execution(slot_id, session_epoch, BackendExecutionEvent::FailExecution)
    }

    pub fn accept_cancel_execution(
        slot_id: u32,
        session_epoch: u64,
    ) -> Result<bool, BackendServiceError> {
        terminate_current_execution(
            slot_id,
            session_epoch,
            BackendExecutionEvent::CancelExecution,
        )
    }

    pub fn render_explain(input: ExplainInput<'_>) -> Result<String, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            if slot.borrow().is_some() {
                return Err(BackendServiceError::ExecutionAlreadyActive);
            }
            Ok(())
        })?;
        explain::render_physical_explain(input)
    }

    pub fn scan_peers() -> Result<Box<[BackendLeaseSlot]>, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let active = slot.borrow();
            let execution = active
                .as_ref()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            Ok(execution
                .scans
                .values()
                .map(|entry| entry.leader_peer)
                .collect::<Vec<_>>()
                .into_boxed_slice())
        })
    }

    pub fn recv_scan_peer_frame(
        peer: BackendLeaseSlot,
        scratch: &mut [u8],
    ) -> Result<Option<usize>, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let execution = active
                .as_mut()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            let entry = execution
                .scans
                .values_mut()
                .find(|entry| entry.leader_peer == peer)
                .ok_or_else(|| {
                    BackendServiceError::ProtocolViolation(format!(
                        "unknown dedicated scan peer {:?}",
                        peer
                    ))
                })?;
            let lease = entry.leader_lease.as_mut().ok_or_else(|| {
                BackendServiceError::ProtocolViolation(format!(
                    "scan peer {:?} has no live backend lease",
                    peer
                ))
            })?;
            let mut rx = lease.from_worker_rx();
            Ok(rx.recv_frame_into(scratch)?)
        })
    }

    pub fn send_scan_peer_bytes(
        peer: BackendLeaseSlot,
        payload: &[u8],
    ) -> Result<CommitOutcome, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let execution = active
                .as_mut()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            let entry = execution
                .scans
                .values_mut()
                .find(|entry| entry.leader_peer == peer)
                .ok_or_else(|| {
                    BackendServiceError::ProtocolViolation(format!(
                        "unknown dedicated scan peer {:?}",
                        peer
                    ))
                })?;
            let lease = entry.leader_lease.as_mut().ok_or_else(|| {
                BackendServiceError::ProtocolViolation(format!(
                    "scan peer {:?} has no live backend lease",
                    peer
                ))
            })?;
            let mut tx = lease.to_worker_tx();
            Ok(tx.send_frame(payload)?)
        })
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[doc(hidden)]
    pub fn inject_wait_for_scan_backpressure_error_for_tests(message: impl Into<String>) {
        WAIT_FOR_SCAN_BACKPRESSURE_ERROR_FOR_TESTS.with(|slot| {
            *slot.borrow_mut() = Some(message.into());
        });
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[doc(hidden)]
    pub fn clear_wait_for_scan_backpressure_error_for_tests() {
        WAIT_FOR_SCAN_BACKPRESSURE_ERROR_FOR_TESTS.with(|slot| {
            slot.borrow_mut().take();
        });
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[doc(hidden)]
    pub fn current_session_epoch_for_tests() -> u64 {
        current_session_epoch()
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[doc(hidden)]
    pub fn reset_for_tests() {
        CURRENT_SESSION_EPOCH.with(|epoch| epoch.set(0));
        ACTIVE_EXECUTION.with(|slot| {
            slot.borrow_mut().take();
        });
        BackendService::clear_wait_for_scan_backpressure_error_for_tests();
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[doc(hidden)]
    pub fn install_fake_execution_for_tests(
        slot_id: u32,
        session_epoch: u64,
        state: BackendExecutionState,
    ) {
        CURRENT_SESSION_EPOCH.with(|epoch| epoch.set(session_epoch));
        ACTIVE_EXECUTION.with(|slot| {
            let mut machine = BackendExecutionMachine::new();
            if state != BackendExecutionState::Idle {
                let _ = machine.consume(&BackendExecutionEvent::BeginExecution);
            }
            if state == BackendExecutionState::Running {
                let _ = machine.consume(&BackendExecutionEvent::PlanPublished);
            }
            if state == BackendExecutionState::Terminal {
                let _ = machine.consume(&BackendExecutionEvent::PlanPublished);
                let _ = machine.consume(&BackendExecutionEvent::CompleteExecution);
            }
            *slot.borrow_mut() = Some(ActiveExecution {
                key: ExecutionKey {
                    slot_id,
                    session_epoch,
                },
                snapshot: ExecutionSnapshot::unregistered_for_tests(),
                _logical_plan: datafusion_expr::logical_plan::LogicalPlan::EmptyRelation(
                    datafusion_expr::logical_plan::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(datafusion_common::DFSchema::empty()),
                    },
                ),
                machine,
                config: BackendServiceConfig::default(),
                starting: None,
                scan_spi: None,
                scans: BTreeMap::new(),
                active_scans: BTreeMap::new(),
            });
        });
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[doc(hidden)]
    pub fn install_starting_execution_with_plan_role_for_tests(
        slot_id: u32,
        session_epoch: u64,
        plan_role: BackendPlanRole,
    ) {
        CURRENT_SESSION_EPOCH.with(|epoch| epoch.set(session_epoch));
        ACTIVE_EXECUTION.with(|slot| {
            let mut machine = BackendExecutionMachine::new();
            let _ = machine.consume(&BackendExecutionEvent::BeginExecution);
            *slot.borrow_mut() = Some(ActiveExecution {
                key: ExecutionKey {
                    slot_id,
                    session_epoch,
                },
                snapshot: ExecutionSnapshot::unregistered_for_tests(),
                _logical_plan: datafusion_expr::logical_plan::LogicalPlan::EmptyRelation(
                    datafusion_expr::logical_plan::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(datafusion_common::DFSchema::empty()),
                    },
                ),
                machine,
                config: BackendServiceConfig::default(),
                starting: Some(StartingRuntime { plan_role }),
                scan_spi: None,
                scans: BTreeMap::new(),
                active_scans: BTreeMap::new(),
            });
        });
    }
}

pub(crate) fn with_registered_snapshot<T>(
    snapshot: pg_sys::Snapshot,
    f: impl FnOnce() -> Result<T, BackendServiceError>,
) -> Result<T, BackendServiceError> {
    if snapshot.is_null() {
        return f();
    }

    let pushed = Cell::new(false);
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        pg_sys::PushActiveSnapshot(snapshot);
        pushed.set(true);
        f()
    }))
    .catch_others(|error| Err(backend_error_from_caught_error(error)))
    .finally(|| unsafe {
        if pushed.get() {
            pg_sys::PopActiveSnapshot();
        }
    })
    .execute()
}

fn current_session_epoch() -> u64 {
    CURRENT_SESSION_EPOCH.with(Cell::get)
}

fn bump_current_session_epoch() -> u64 {
    CURRENT_SESSION_EPOCH.with(|epoch| {
        let next = epoch.get().saturating_add(1);
        epoch.set(next);
        next
    })
}

fn wait_for_scan_backpressure(
    blocked_loops: u32,
    metrics: RuntimeMetrics,
) -> Result<bool, BackendServiceError> {
    #[cfg(any(test, feature = "pg_test"))]
    if let Some(err) = take_wait_for_scan_backpressure_error_for_tests() {
        return Err(err);
    }

    PgTryBuilder::new(AssertUnwindSafe(|| {
        if blocked_loops < 8 {
            for _ in 0..64 {
                std::hint::spin_loop();
            }
            pgrx::pg_sys::check_for_interrupts!();
            return Ok(true);
        }

        if blocked_loops == 8 {
            let wait_start = metrics.now_ns();
            wait_latch(Some(Duration::from_millis(1)));
            metrics.add_elapsed(MetricId::BackendWaitLatchNs, wait_start);
            metrics.increment(MetricId::BackendWaitLatchTotal);
            return Ok(true);
        }

        Ok(false)
    }))
    .catch_others(|error| Err(backend_error_from_caught_error(error)))
    .execute()
}

fn wait_latch(timeout: Option<Duration>) {
    let timeout_ms: std::ffi::c_long = timeout
        .map(|t| t.as_millis().try_into().unwrap())
        .unwrap_or(-1);
    let events = if timeout.is_some() {
        pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH
    } else {
        pg_sys::WL_LATCH_SET | pg_sys::WL_POSTMASTER_DEATH
    };

    let rc = unsafe {
        let rc = pg_sys::WaitLatch(
            pg_sys::MyLatch,
            events as i32,
            timeout_ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(pg_sys::MyLatch);
        rc
    };
    pgrx::pg_sys::check_for_interrupts!();
    if rc & pg_sys::WL_POSTMASTER_DEATH as i32 != 0 {
        panic!("postmaster is dead");
    }
}

fn classify_missing_execution(
    _slot_id: u32,
    session_epoch: u64,
) -> Result<bool, BackendServiceError> {
    match session_epoch.cmp(&current_session_epoch()) {
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal => Ok(false),
        std::cmp::Ordering::Greater => Err(BackendServiceError::FutureSession {
            current: current_session_epoch(),
            incoming: session_epoch,
        }),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionEpochMatch {
    Current,
    Stale,
}

fn classify_session_epoch_for_scan_open(
    session_epoch: u64,
) -> Result<SessionEpochMatch, BackendServiceError> {
    let current = current_session_epoch();
    match session_epoch.cmp(&current) {
        std::cmp::Ordering::Less => Ok(SessionEpochMatch::Stale),
        std::cmp::Ordering::Equal => Ok(SessionEpochMatch::Current),
        std::cmp::Ordering::Greater => Err(BackendServiceError::FutureSession {
            current,
            incoming: session_epoch,
        }),
    }
}

fn should_ignore_message(
    execution: &ActiveExecution,
    slot_id: u32,
    session_epoch: u64,
) -> Result<bool, BackendServiceError> {
    let current = current_session_epoch();
    match session_epoch.cmp(&current) {
        std::cmp::Ordering::Less => Ok(true),
        std::cmp::Ordering::Greater => Err(BackendServiceError::FutureSession {
            current,
            incoming: session_epoch,
        }),
        std::cmp::Ordering::Equal => Ok(slot_id != execution.key.slot_id),
    }
}

fn ensure_driver_matches_execution(
    execution: &ActiveExecution,
    key: ExecutionKey,
    scan_id: u64,
    action: &'static str,
) -> Result<(), BackendServiceError> {
    if execution.key != key {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "scan driver key mismatch during {}: driver {:?}, execution {:?}",
            action, key, execution.key
        )));
    }
    if !execution.active_scans.contains_key(&scan_id) {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "scan driver for scan_id {} is not active during {}",
            scan_id, action
        )));
    }
    Ok(())
}

fn ensure_driver_belongs_to_execution(
    execution: &ActiveExecution,
    key: ExecutionKey,
    scan_id: u64,
    action: &'static str,
) -> Result<(), BackendServiceError> {
    if execution.key != key {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "scan driver key mismatch during {}: driver {:?}, execution {:?}",
            action, key, execution.key
        )));
    }
    if !execution.scans.contains_key(&scan_id) {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "scan driver for scan_id {} is not installed during {}",
            scan_id, action
        )));
    }
    Ok(())
}

fn ensure_execution_state(
    execution: &ActiveExecution,
    expected: BackendExecutionState,
    action: &'static str,
) -> Result<(), BackendServiceError> {
    let state = *execution.machine.state();
    if state == expected {
        Ok(())
    } else {
        Err(BackendServiceError::InvalidExecutionState { action, state })
    }
}

fn scan_state_is_terminal(state: ScanEntryState) -> bool {
    matches!(state, ScanEntryState::Finished | ScanEntryState::Cancelled)
}

fn execution_unfinished_scan_count(execution: &ActiveExecution) -> usize {
    execution
        .scans
        .values()
        .filter(|entry| !scan_state_is_terminal(entry.state))
        .count()
}

fn execution_ready_for_completion(key: ExecutionKey) -> Result<bool, BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let active = slot.borrow();
        let execution = active.as_ref().ok_or(BackendServiceError::NoActiveExecution)?;
        if execution.key != key {
            return Err(BackendServiceError::ProtocolViolation(format!(
                "execution key mismatch while checking completion readiness: expected {:?}, found {:?}",
                key, execution.key
            )));
        }
        Ok(execution.active_scans.is_empty() && execution_unfinished_scan_count(execution) == 0)
    })
}

fn step_scan_with_driver(
    key: ExecutionKey,
    scan_id: u64,
) -> Result<ScanStreamStep, BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let mut active = slot.borrow_mut();
        let (mut active_scan, metrics) = {
            let execution = match active.as_mut() {
                Some(execution) => execution,
                None => {
                    backend_diag_warning(|| {
                        format!(
                            "backend_service step_scan_with_driver missing execution slot_id={} session_epoch={} scan_id={} current_session_epoch={}",
                            key.slot_id,
                            key.session_epoch,
                            scan_id,
                            current_session_epoch()
                        )
                    });
                    return Err(BackendServiceError::NoActiveExecution);
                }
            };
            ensure_driver_matches_execution(execution, key, scan_id, "step scan")?;
            ensure_execution_state(execution, BackendExecutionState::Running, "step scan")?;
            let active_scan = execution.active_scans.remove(&scan_id).ok_or_else(|| {
                BackendServiceError::ProtocolViolation(format!(
                    "scan driver for scan_id {} lost its active scan stream during step scan",
                    scan_id
                ))
            })?;
            (active_scan, execution.config.metrics)
        };

        let mut blocked_loops = 0u32;
        loop {
            let outcome = {
                let execution = active
                    .as_mut()
                    .expect("active execution must remain installed during step scan");
                drive_scan_step(execution, &mut active_scan)
            };

            match outcome {
                Ok(ScanDriveOutcome::Continue(step)) => {
                    active
                        .as_mut()
                        .expect("active execution must exist for continuing scan")
                        .active_scans
                        .insert(scan_id, active_scan);
                    return Ok(step);
                }
                Ok(ScanDriveOutcome::Blocked) => {
                    // During the bounded internal wait, the stackful scan
                    // session stays detached in `active_scan`. It must be
                    // reinstalled only when we yield control back to the host
                    // loop, or handed straight to fatal cleanup on error.
                    match wait_for_scan_backpressure(blocked_loops, metrics) {
                        Ok(true) => {
                            blocked_loops = blocked_loops.saturating_add(1);
                        }
                        Ok(false) => {
                            active
                                .as_mut()
                                .expect("active execution must exist while yielding scan control")
                                .active_scans
                                .insert(scan_id, active_scan);
                            return Ok(ScanStreamStep::YieldForControl {
                                reason: ScanYieldReason::PermitBackpressure,
                            });
                        }
                        Err(err) => {
                            return Err(fail_current_execution_after_scan_error(
                                &mut active,
                                active_scan,
                                err,
                            ));
                        }
                    }
                }
                Ok(ScanDriveOutcome::Terminal(step)) => return Ok(step),
                Ok(ScanDriveOutcome::FatalExecution(step)) => {
                    return finish_current_execution_after_fatal_scan_step(
                        &mut active,
                        active_scan,
                        step,
                    );
                }
                Err(err) => {
                    return Err(fail_current_execution_after_scan_error(
                        &mut active,
                        active_scan,
                        err,
                    ));
                }
            }
        }
    })
}

#[cfg(any(test, feature = "pg_test"))]
fn take_wait_for_scan_backpressure_error_for_tests() -> Option<BackendServiceError> {
    WAIT_FOR_SCAN_BACKPRESSURE_ERROR_FOR_TESTS
        .with(|slot| slot.borrow_mut().take().map(BackendServiceError::Postgres))
}

fn prepare_scan_entry(
    session_epoch: u64,
    config: &BackendServiceConfig,
    spec: Arc<PgScanSpec>,
    producers: Vec<ScanWorkerProducer>,
    leader_ctid_range: Option<CtidBlockRange>,
    scan_lease: BackendSlotLease,
) -> Result<PreparedScanEntry, BackendServiceError> {
    let scan_id = spec.scan_id.get();
    if spec.compiled_scan.uses_dummy_projection {
        return Err(BackendServiceError::UnsupportedDummyProjection { scan_id });
    }

    let source_schema = spec.arrow_schema();
    let mut normalized_fields = Vec::with_capacity(source_schema.fields().len());
    let mut physical_columns = Vec::with_capacity(source_schema.fields().len());
    let mut projected_columns = Vec::with_capacity(source_schema.fields().len());
    for (index, field) in source_schema.fields().iter().enumerate() {
        let (normalized_field, type_tag) =
            normalize_transport_field(index, field.as_ref(), scan_id)?;
        normalized_fields.push(normalized_field);
        physical_columns.push(ColumnSpec::new(type_tag, field.is_nullable()));
        projected_columns.push(ProjectedColumnRef::relation_attribute(
            field.name(),
            type_tag,
        ));
    }
    let schema: SchemaRef = Arc::new(Schema::new(normalized_fields));

    let seeded_config = seed_estimator_config(
        spec.table_oid.into(),
        &projected_columns,
        config.estimator_default,
    )?;
    let execution_shape = match leader_ctid_range {
        Some(range) => scan_execution_shape_for_ctid_range(&spec, range)?,
        None => scan_execution_shape(&spec)?,
    };
    let prepared_scan = prepare_scan(
        &execution_shape.sql,
        ScanOptions {
            planner_fetch_hint: spec.fetch_hints.planner_fetch_hint,
            local_row_cap: spec.fetch_hints.local_row_cap,
            diagnostics: config.diagnostics.clone(),
        },
    )?;
    let canonical_open = ScanOpen::new(
        ScanFlowId {
            session_epoch,
            scan_id,
        },
        config.scan_page_kind,
        config.scan_page_flags,
        producers
            .iter()
            .map(|producer| ProducerDescriptor {
                producer_id: producer.producer_id,
                role: producer.role,
            })
            .collect(),
    )?;
    let leader_peer = producers
        .iter()
        .find(|producer| producer.role == ProducerRoleKind::Leader)
        .map(|producer| producer.peer)
        .ok_or_else(|| {
            BackendServiceError::ProtocolViolation(format!(
                "scan_id {scan_id} has no leader producer"
            ))
        })?;

    Ok(PreparedScanEntry {
        scan_id,
        _spec: spec,
        schema,
        prepared_scan,
        physical_columns,
        source_projection: execution_shape.source_projection,
        estimator_config: seeded_config,
        canonical_open,
        producers,
        leader_peer,
        leader_lease: Some(scan_lease),
        state: ScanEntryState::Prepared,
    })
}

#[derive(Debug)]
struct ScanExecutionShape {
    sql: String,
    source_projection: Vec<usize>,
}

fn scan_execution_shape(spec: &PgScanSpec) -> Result<ScanExecutionShape, BackendServiceError> {
    if !spec.compiled_scan.output_columns.is_empty()
        && can_use_unprojected_relation_scan(spec.table_oid.into())?
    {
        return Ok(ScanExecutionShape {
            sql: render_unprojected_scan_sql(&spec.relation, &spec.compiled_scan),
            source_projection: spec.compiled_scan.output_columns.clone(),
        });
    }

    Ok(ScanExecutionShape {
        sql: spec.compiled_scan.sql.clone(),
        source_projection: (0..spec.compiled_scan.output_columns.len()).collect(),
    })
}

fn scan_execution_sql(spec: &PgScanSpec) -> Result<String, BackendServiceError> {
    scan_execution_shape(spec).map(|shape| shape.sql)
}

fn scan_execution_shape_for_ctid_range(
    spec: &PgScanSpec,
    range: CtidBlockRange,
) -> Result<ScanExecutionShape, BackendServiceError> {
    if spec.compiled_scan.output_columns.is_empty() {
        return Err(BackendServiceError::UnsupportedDummyProjection {
            scan_id: spec.scan_id.get(),
        });
    }
    if !can_use_unprojected_relation_scan(spec.table_oid.into())? {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "scan_id {} cannot use CTID chunking because the relation has dropped attributes",
            spec.scan_id.get()
        )));
    }
    Ok(ScanExecutionShape {
        sql: render_unprojected_ctid_block_scan_sql(
            &spec.relation,
            &spec.compiled_scan,
            range.start_block,
            range.end_block,
        ),
        source_projection: spec.compiled_scan.output_columns.clone(),
    })
}

pub fn build_standalone_scan_descriptor(
    spec: &PgScanSpec,
    ctid_range: Option<CtidBlockRange>,
) -> Result<StandaloneScanDescriptor, BackendServiceError> {
    let scan_id = spec.scan_id.get();
    if spec.compiled_scan.uses_dummy_projection {
        return Err(BackendServiceError::UnsupportedDummyProjection { scan_id });
    }

    let source_schema = spec.arrow_schema();
    let mut fields = Vec::with_capacity(source_schema.fields().len());
    for (index, field) in source_schema.fields().iter().enumerate() {
        let (normalized_field, type_tag) =
            normalize_transport_field(index, field.as_ref(), scan_id)?;
        fields.push(StandaloneScanField {
            name: normalized_field.name().to_owned(),
            type_tag: type_tag.to_raw(),
            nullable: normalized_field.is_nullable(),
        });
    }

    let execution_shape = match ctid_range {
        Some(range) => scan_execution_shape_for_ctid_range(spec, range)?,
        None => scan_execution_shape(spec)?,
    };

    Ok(StandaloneScanDescriptor {
        sql: execution_shape.sql,
        table_oid: spec.table_oid,
        fields,
        source_projection: execution_shape.source_projection,
        planner_fetch_hint: spec.fetch_hints.planner_fetch_hint,
        local_row_cap: spec.fetch_hints.local_row_cap,
    })
}

fn run_standalone_scan_producer(
    input: StandaloneScanProducerInput,
) -> Result<(), BackendServiceError> {
    if input.producer_count == 0 || input.producer_id >= input.producer_count {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "invalid standalone scan producer_id={} producer_count={}",
            input.producer_id, input.producer_count
        )));
    }

    let canonical_open = standalone_scan_open(
        input.session_epoch,
        input.scan_id,
        input.producer_count,
        input.config.scan_page_kind,
        input.config.scan_page_flags,
    )?;
    let mut scan_lease = input.scan_lease;
    wait_for_standalone_open_scan(
        &mut scan_lease,
        input.session_epoch,
        input.scan_id,
        &canonical_open,
    )?;

    let source = match standalone_page_source(
        input.scan_id,
        input.scan_tx.payload_capacity(),
        &input.config,
        &input.descriptor,
    ) {
        Ok(source) => source,
        Err(err) => {
            send_standalone_scan_failed(
                &mut scan_lease,
                input.session_epoch,
                input.scan_id,
                input.producer_id,
                &err,
            );
            return Err(err);
        }
    };

    let mut producer = BackendProducerRole::new(input.scan_tx);
    if let Err(err) = producer.open(&canonical_open, input.producer_id, source) {
        let err = BackendServiceError::ScanProducer(err);
        send_standalone_scan_failed(
            &mut scan_lease,
            input.session_epoch,
            input.scan_id,
            input.producer_id,
            &err,
        );
        return Err(err);
    }
    drive_standalone_producer(scan_lease, producer, input.config.metrics)
}

fn standalone_page_source(
    scan_id: u64,
    payload_capacity: usize,
    config: &BackendServiceConfig,
    descriptor: &StandaloneScanDescriptor,
) -> Result<source::SlotScanPageSource, BackendServiceError> {
    let field_count = descriptor.fields.len();
    if descriptor.source_projection.len() != field_count {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "standalone scan descriptor for scan_id {scan_id} has {} fields but {} source projection entries",
            field_count,
            descriptor.source_projection.len()
        )));
    }

    let mut fields = Vec::with_capacity(field_count);
    let mut physical_columns = Vec::with_capacity(field_count);
    let mut projected_columns = Vec::with_capacity(field_count);
    for (index, field) in descriptor.fields.iter().enumerate() {
        let type_tag = TypeTag::from_raw(field.type_tag).map_err(|_| {
            BackendServiceError::ProtocolViolation(format!(
                "standalone scan descriptor for scan_id {scan_id} has invalid type tag {} at field {index}",
                field.type_tag
            ))
        })?;
        fields.push(Field::new(
            field.name.clone(),
            arrow_data_type_for_type_tag(type_tag),
            field.nullable,
        ));
        physical_columns.push(ColumnSpec::new(type_tag, field.nullable));
        projected_columns.push(ProjectedColumnRef::relation_attribute(
            &field.name,
            type_tag,
        ));
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let estimator_config = seed_estimator_config(
        descriptor.table_oid.into(),
        &projected_columns,
        config.estimator_default,
    )?;
    let prepared_scan = prepare_scan(
        &descriptor.sql,
        ScanOptions {
            planner_fetch_hint: descriptor.planner_fetch_hint,
            local_row_cap: descriptor.local_row_cap,
            diagnostics: config.diagnostics.clone(),
        },
    )?;
    let block_size = u32::try_from(payload_capacity).map_err(|_| {
        BackendServiceError::ProtocolViolation("scan payload capacity exceeds u32".into())
    })?;
    let estimator = PageRowEstimator::new(&physical_columns, block_size, estimator_config)?;
    let spi = ExecutionSpiContext::connect(config.diagnostics.clone())?;
    Ok(source::SlotScanPageSource::new(
        std::ptr::null_mut(),
        spi,
        prepared_scan,
        schema,
        descriptor.source_projection.clone(),
        block_size,
        normalize_scan_fetch_batch_rows(config.scan_fetch_batch_rows),
        estimator,
        config.metrics,
        config.scan_timing_detail,
    ))
}

fn arrow_data_type_for_type_tag(type_tag: TypeTag) -> DataType {
    match type_tag {
        TypeTag::Boolean => DataType::Boolean,
        TypeTag::Int16 => DataType::Int16,
        TypeTag::Int32 => DataType::Int32,
        TypeTag::Int64 => DataType::Int64,
        TypeTag::Float32 => DataType::Float32,
        TypeTag::Float64 => DataType::Float64,
        TypeTag::Uuid => DataType::FixedSizeBinary(16),
        TypeTag::Utf8View => DataType::Utf8View,
        TypeTag::BinaryView => DataType::BinaryView,
    }
}

fn standalone_scan_open(
    session_epoch: u64,
    scan_id: u64,
    producer_count: u16,
    page_kind: u16,
    page_flags: u16,
) -> Result<ScanOpen, BackendServiceError> {
    let mut producers = Vec::with_capacity(producer_count as usize);
    producers.push(ProducerDescriptor::leader(SINGLE_SCAN_PRODUCER_ID));
    for producer_id in 1..producer_count {
        producers.push(ProducerDescriptor::worker(producer_id));
    }
    Ok(ScanOpen::new(
        ScanFlowId {
            session_epoch,
            scan_id,
        },
        page_kind,
        page_flags,
        producers,
    )?)
}

fn wait_for_standalone_open_scan(
    scan_lease: &mut BackendSlotLease,
    session_epoch: u64,
    scan_id: u64,
    canonical_open: &ScanOpen,
) -> Result<(), BackendServiceError> {
    let mut scratch = vec![0_u8; 1024];
    let deadline = Instant::now() + STANDALONE_OPEN_SCAN_TIMEOUT;
    loop {
        let received = {
            let mut rx = scan_lease.from_worker_rx();
            rx.recv_frame_into(&mut scratch)?
        };
        let Some(len) = received else {
            if Instant::now() >= deadline {
                return Err(BackendServiceError::ProtocolViolation(format!(
                    "timed out waiting for OpenScan for standalone scan_id {scan_id}"
                )));
            }
            wait_latch(Some(Duration::from_millis(1)));
            continue;
        };
        match decode_worker_scan_to_backend(&scratch[..len]).map_err(|err| {
            BackendServiceError::ProtocolViolation(format!(
                "failed to decode standalone scan control: {err}"
            ))
        })? {
            WorkerScanToBackendRef::OpenScan {
                session_epoch: incoming_epoch,
                scan_id: incoming_scan_id,
                scan,
            } => {
                if incoming_epoch != session_epoch || incoming_scan_id != scan_id {
                    return Err(BackendServiceError::ProtocolViolation(format!(
                        "standalone scan open targeted session_epoch={incoming_epoch}, scan_id={incoming_scan_id}; expected session_epoch={session_epoch}, scan_id={scan_id}"
                    )));
                }
                if !scan_descriptor_matches(canonical_open, scan) {
                    return Err(BackendServiceError::ProtocolViolation(format!(
                        "standalone scan descriptor mismatch for scan_id {scan_id}"
                    )));
                }
                return Ok(());
            }
            WorkerScanToBackendRef::CancelScan { .. } => {
                return Err(BackendServiceError::ProtocolViolation(format!(
                    "standalone scan_id {scan_id} was cancelled before OpenScan"
                )));
            }
        }
    }
}

fn drive_standalone_producer(
    mut scan_lease: BackendSlotLease,
    mut producer: BackendProducerRole<source::SlotScanPageSource>,
    metrics: RuntimeMetrics,
) -> Result<(), BackendServiceError> {
    loop {
        match producer.step()? {
            BackendProducerStep::OutboundPage {
                outbound,
                flow: _,
                producer_id: _,
            } => {
                let descriptor = outbound.descriptor();
                let payload_len = outbound.payload_len();
                let frame = encode_issued_frame(outbound.frame()).map_err(|err| {
                    BackendServiceError::ProtocolViolation(format!(
                        "failed to encode standalone scan page frame: {err}"
                    ))
                })?;
                let _ = scan_lease.to_worker_tx().send_frame(&frame)?;
                metrics.stamp_page(PageDirection::BackendToWorker, descriptor, payload_len);
                metrics.increment(MetricId::ScanPagesSentTotal);
                metrics.add(MetricId::ScanBytesSentTotal, payload_len as u64);
                outbound.mark_sent();
            }
            BackendProducerStep::Blocked { .. } => {
                wait_latch(Some(Duration::from_millis(1)));
            }
            BackendProducerStep::ProducerEof { flow, producer_id } => {
                producer.close()?;
                send_standalone_scan_terminal(
                    &mut scan_lease,
                    BackendScanToWorker::ScanFinished {
                        session_epoch: flow.session_epoch,
                        scan_id: flow.scan_id,
                        producer_id,
                    },
                )?;
                return Ok(());
            }
            BackendProducerStep::ProducerError {
                flow,
                producer_id,
                error,
            } => {
                let message = error.to_string();
                let truncated = truncate_scan_failure_message(&message);
                let _ = producer.abort();
                send_standalone_scan_terminal(
                    &mut scan_lease,
                    BackendScanToWorker::ScanFailed {
                        session_epoch: flow.session_epoch,
                        scan_id: flow.scan_id,
                        producer_id,
                        message: &truncated,
                    },
                )?;
                return Err(BackendServiceError::ProtocolViolation(message));
            }
        }
    }
}

fn send_standalone_scan_terminal(
    scan_lease: &mut BackendSlotLease,
    message: BackendScanToWorker<'_>,
) -> Result<(), BackendServiceError> {
    let mut encoded = vec![0_u8; runtime_protocol::encoded_len_backend_scan_to_worker(message)];
    let written = encode_backend_scan_to_worker_into(message, &mut encoded).map_err(|err| {
        BackendServiceError::ProtocolViolation(format!(
            "failed to encode standalone scan terminal: {err}"
        ))
    })?;
    let _ = scan_lease.to_worker_tx().send_frame(&encoded[..written])?;
    wait_for_standalone_worker_detach(scan_lease)?;
    Ok(())
}

fn wait_for_standalone_worker_detach(
    scan_lease: &BackendSlotLease,
) -> Result<(), BackendServiceError> {
    let deadline = Instant::now() + STANDALONE_TERMINAL_DRAIN_TIMEOUT;
    while scan_lease.worker_attached() {
        if Instant::now() >= deadline {
            return Err(BackendServiceError::ProtocolViolation(format!(
                "timed out waiting for worker to detach standalone scan slot {}",
                scan_lease.slot_id()
            )));
        }
        wait_latch(Some(Duration::from_millis(1)));
    }
    Ok(())
}

fn send_standalone_scan_failed(
    scan_lease: &mut BackendSlotLease,
    session_epoch: u64,
    scan_id: u64,
    producer_id: u16,
    error: &dyn std::fmt::Display,
) {
    let message = truncate_scan_failure_message(&error.to_string());
    let _ = send_standalone_scan_terminal(
        scan_lease,
        BackendScanToWorker::ScanFailed {
            session_epoch,
            scan_id,
            producer_id,
            message: &message,
        },
    );
}

fn truncate_scan_failure_message(message: &str) -> String {
    const MAX_SCAN_FAILURE_BYTES: usize = runtime_protocol::MAX_SCAN_FAILURE_MESSAGE_LEN;
    if message.len() <= MAX_SCAN_FAILURE_BYTES {
        return message.to_string();
    }

    let mut end = MAX_SCAN_FAILURE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
}

fn can_use_unprojected_relation_scan(
    relation_oid: pg_sys::Oid,
) -> Result<bool, BackendServiceError> {
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let relation = PgrxRelation::with_lock(relation_oid, pg_sys::AccessShareLock as _);
        let has_no_dropped_attributes = relation
            .tuple_desc()
            .iter()
            .all(|attribute| !attribute.is_dropped());
        Ok(has_no_dropped_attributes)
    }))
    .catch_others(|error| Err(backend_error_from_caught_error(error)))
    .execute()
}

fn normalize_transport_field(
    index: usize,
    field: &Field,
    scan_id: u64,
) -> Result<(Field, TypeTag), BackendServiceError> {
    let (data_type, type_tag) = match field.data_type() {
        DataType::Utf8 => (DataType::Utf8View, TypeTag::Utf8View),
        DataType::Binary => (DataType::BinaryView, TypeTag::BinaryView),
        other => {
            let type_tag = TypeTag::from_arrow_data_type(index, other).map_err(|_| {
                BackendServiceError::UnsupportedArrowType {
                    scan_id,
                    index,
                    data_type: other.to_string(),
                }
            })?;
            return Ok((field.clone(), type_tag));
        }
    };

    Ok((
        Field::new(field.name(), data_type, field.is_nullable()),
        type_tag,
    ))
}

fn normalize_scan_fetch_batch_rows(fetch_batch_rows: u32) -> usize {
    usize::try_from(fetch_batch_rows.max(1)).expect("scan fetch batch size must fit into usize")
}

fn drive_scan_step(
    execution: &mut ActiveExecution,
    active_scan: &mut ActiveScanStream,
) -> Result<ScanDriveOutcome, BackendServiceError> {
    match active_scan.producer.step()? {
        BackendProducerStep::OutboundPage {
            flow,
            producer_id,
            outbound,
        } => Ok(ScanDriveOutcome::Continue(ScanStreamStep::OutboundPage {
            flow,
            producer_id,
            outbound,
        })),
        BackendProducerStep::Blocked { .. } => Ok(ScanDriveOutcome::Blocked),
        BackendProducerStep::ProducerEof { flow, producer_id } => {
            let terminal = active_scan
                .coordinator
                .accept_producer_eof(flow, producer_id)?
                .ok_or(BackendServiceError::MissingLogicalTerminal {
                    scan_id: active_scan.scan_id,
                })?;
            let step = match terminal {
                LogicalTerminal::LogicalEof { flow } => ScanStreamStep::Finished { flow },
                LogicalTerminal::LogicalError {
                    flow,
                    producer_id,
                    message,
                } => {
                    return Err(BackendServiceError::ProtocolViolation(format!(
                        "scan coordinator returned logical error on producer EOF for scan_id {} flow {:?} producer_id {}: {}",
                        active_scan.scan_id, flow, producer_id, message
                    )));
                }
            };
            finalize_detached_scan(execution, active_scan, ScanEntryState::Finished)?;
            Ok(ScanDriveOutcome::Terminal(step))
        }
        BackendProducerStep::ProducerError {
            flow,
            producer_id,
            error,
        } => {
            let terminal = active_scan.coordinator.accept_producer_error(
                flow,
                producer_id,
                error.to_string(),
            )?;
            let step = match terminal {
                LogicalTerminal::LogicalError {
                    flow,
                    producer_id,
                    message,
                } => ScanStreamStep::Failed {
                    flow,
                    producer_id,
                    message,
                },
                LogicalTerminal::LogicalEof { flow } => {
                    return Err(BackendServiceError::ProtocolViolation(format!(
                        "scan coordinator returned logical EOF on producer error for scan_id {} flow {:?}",
                        active_scan.scan_id, flow
                    )));
                }
            };
            Ok(ScanDriveOutcome::FatalExecution(step))
        }
    }
}

fn finalize_detached_scan(
    execution: &mut ActiveExecution,
    active_scan: &mut ActiveScanStream,
    state: ScanEntryState,
) -> Result<(), BackendServiceError> {
    let mut final_error = None;

    if let Err(err) = active_scan.producer.close() {
        final_error.get_or_insert(BackendServiceError::ScanProducer(err));
    }
    if let Err(err) = active_scan.coordinator.close() {
        final_error.get_or_insert(BackendServiceError::ScanCoordinator(err));
    }
    if let Err(err) = mark_scan_terminal(execution, active_scan.scan_id, state) {
        final_error.get_or_insert(err);
    }

    match final_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn cancel_detached_scan(
    execution: &mut ActiveExecution,
    active_scan: &mut ActiveScanStream,
) -> Result<(), BackendServiceError> {
    let mut final_error = None;

    if let Err(err) = active_scan.producer.abort() {
        final_error.get_or_insert(BackendServiceError::ScanProducer(err));
    }
    active_scan.coordinator.abort();
    if let Err(err) = mark_scan_terminal(execution, active_scan.scan_id, ScanEntryState::Cancelled)
    {
        final_error.get_or_insert(err);
    }

    match final_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn scan_descriptor_matches(canonical: &ScanOpen, incoming: ScanFlowDescriptorRef<'_>) -> bool {
    if canonical.page_kind != incoming.page_kind || canonical.page_flags != incoming.page_flags {
        return false;
    }

    // Producer order is part of the canonical scan contract because producer_id
    // assignment is positional within the declared producer set.
    let mut expected = canonical.producers.iter();
    for producer in incoming.producers().iter() {
        let Some(expected_producer) = expected.next() else {
            return false;
        };
        if expected_producer.producer_id != producer.producer_id {
            return false;
        }
        let expected_role = match expected_producer.role {
            ProducerRoleKind::Leader => ProducerRole::Leader,
            ProducerRoleKind::Worker => ProducerRole::Worker,
        };
        if expected_role != producer.role {
            return false;
        }
    }

    expected.next().is_none()
}

#[cfg(any(test, feature = "pg_test"))]
#[doc(hidden)]
pub fn scan_descriptor_matches_for_tests(
    canonical: &ScanOpen,
    incoming: ScanFlowDescriptorRef<'_>,
) -> bool {
    scan_descriptor_matches(canonical, incoming)
}

fn mark_scan_terminal(
    execution: &mut ActiveExecution,
    scan_id: u64,
    state: ScanEntryState,
) -> Result<(), BackendServiceError> {
    let entry = execution
        .scans
        .get_mut(&scan_id)
        .ok_or(BackendServiceError::UnknownScanId { scan_id })?;
    backend_diag_warning(|| {
        format!(
            "backend_service mark_scan_terminal slot_id={} session_epoch={} scan_id={} terminal_state={:?} peer_slot_id={} generation={} lease_epoch={} scan_lease_live={}",
            execution.key.slot_id,
            execution.key.session_epoch,
            scan_id,
            state,
            entry.leader_peer.slot_id(),
            entry.leader_peer.lease_id().generation(),
            entry.leader_peer.lease_id().lease_epoch(),
            entry.leader_lease.is_some()
        )
    });
    entry.state = state;
    // Keep the dedicated scan lease alive until execution cleanup so the host
    // can still publish ScanFinished/ScanFailed on the same peer after the
    // scan runtime transitions to a terminal state.
    Ok(())
}

fn cancel_scan_from_driver(key: ExecutionKey, scan_id: u64) -> Result<bool, BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let mut active = slot.borrow_mut();
        {
            let Some(execution) = active.as_mut() else {
                return classify_missing_execution(key.slot_id, key.session_epoch);
            };

            if should_ignore_message(execution, key.slot_id, key.session_epoch)? {
                return Ok(false);
            }
            ensure_driver_matches_execution(execution, key, scan_id, "cancel scan")?;
            ensure_execution_state(execution, BackendExecutionState::Running, "cancel scan")?;
        }

        let mut active_scan = active
            .as_mut()
            .expect("active execution must remain installed during cancel_scan")
            .active_scans
            .remove(&scan_id)
            .ok_or_else(|| {
                BackendServiceError::ProtocolViolation(format!(
                    "scan driver for scan_id {} lost its active scan stream during cancel scan",
                    scan_id
                ))
            })?;

        let cancel_result = {
            let execution = active
                .as_mut()
                .expect("active execution must remain installed during cancel_scan");
            cancel_detached_scan(execution, &mut active_scan)
        };

        match cancel_result {
            Ok(()) => Ok(true),
            Err(err) => Err(fail_current_execution_after_scan_error(
                &mut active,
                active_scan,
                err,
            )),
        }
    })
}

fn terminate_current_execution(
    slot_id: u32,
    session_epoch: u64,
    event: BackendExecutionEvent,
) -> Result<bool, BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(current) = active.as_ref() else {
            return classify_missing_execution(slot_id, session_epoch);
        };

        if should_ignore_message(current, slot_id, session_epoch)? {
            return Ok(false);
        }
        if event == BackendExecutionEvent::CompleteExecution {
            let unfinished_count = execution_unfinished_scan_count(current);
            if unfinished_count != 0 {
                return Err(BackendServiceError::ExecutionScansNotTerminal {
                    action: terminal_event_action(event),
                    active_count: current.active_scans.len(),
                    unfinished_count,
                });
            }
        }
        if !terminal_event_allowed(*current.machine.state(), event) {
            return Err(BackendServiceError::InvalidExecutionState {
                action: terminal_event_action(event),
                state: *current.machine.state(),
            });
        }
        backend_diag_info(|| {
            format!(
                "backend_service terminate_current_execution slot_id={} session_epoch={} event={:?} state={:?} active_scans={} unfinished_scans={}",
                slot_id,
                session_epoch,
                event,
                current.machine.state(),
                current.active_scans.len(),
                execution_unfinished_scan_count(current)
            )
        });

        let execution = active.take().expect("checked above");
        cleanup_execution(execution, Some(event))?;
        Ok(true)
    })
}

fn terminate_current_execution_from_driver(
    key: ExecutionKey,
    scan_id: u64,
    event: BackendExecutionEvent,
) -> Result<bool, BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(current) = active.as_ref() else {
            return classify_missing_execution(key.slot_id, key.session_epoch);
        };

        if should_ignore_message(current, key.slot_id, key.session_epoch)? {
            return Ok(false);
        }
        ensure_driver_belongs_to_execution(current, key, scan_id, terminal_event_action(event))?;
        if event == BackendExecutionEvent::CompleteExecution {
            let unfinished_count = execution_unfinished_scan_count(current);
            if unfinished_count != 0 {
                return Err(BackendServiceError::ExecutionScansNotTerminal {
                    action: terminal_event_action(event),
                    active_count: current.active_scans.len(),
                    unfinished_count,
                });
            }
        }
        if !terminal_event_allowed(*current.machine.state(), event) {
            return Err(BackendServiceError::InvalidExecutionState {
                action: terminal_event_action(event),
                state: *current.machine.state(),
            });
        }
        backend_diag_info(|| {
            format!(
                "backend_service terminate_current_execution_from_driver slot_id={} session_epoch={} scan_id={} event={:?} state={:?} active_scans={} unfinished_scans={}",
                key.slot_id,
                key.session_epoch,
                scan_id,
                event,
                current.machine.state(),
                current.active_scans.len(),
                execution_unfinished_scan_count(current)
            )
        });

        let execution = active.take().expect("checked above");
        cleanup_execution(execution, Some(event))?;
        Ok(true)
    })
}

fn missing_starting_runtime_error(action: &'static str) -> BackendServiceError {
    BackendServiceError::ProtocolViolation(format!(
        "backend execution is in Starting without an installed plan publication runtime during {}",
        action
    ))
}

fn terminal_event_allowed(state: BackendExecutionState, event: BackendExecutionEvent) -> bool {
    match event {
        BackendExecutionEvent::CompleteExecution => state == BackendExecutionState::Running,
        BackendExecutionEvent::FailExecution | BackendExecutionEvent::CancelExecution => {
            matches!(
                state,
                BackendExecutionState::Starting | BackendExecutionState::Running
            )
        }
        _ => false,
    }
}

fn terminal_event_action(event: BackendExecutionEvent) -> &'static str {
    match event {
        BackendExecutionEvent::CompleteExecution => "complete execution",
        BackendExecutionEvent::FailExecution => "fail execution",
        BackendExecutionEvent::CancelExecution => "cancel execution",
        _ => "terminate execution",
    }
}

fn diagnostic_current_memory_context() -> pg_sys::MemoryContext {
    #[cfg(test)]
    {
        std::ptr::null_mut()
    }
    #[cfg(not(test))]
    unsafe {
        pg_sys::CurrentMemoryContext
    }
}

#[cfg(not(test))]
struct CachedLogFile {
    path: Arc<str>,
    file: File,
}

fn set_backend_diagnostics(diagnostics: DiagnosticsConfig) {
    ACTIVE_DIAGNOSTICS.with(|slot| *slot.borrow_mut() = diagnostics);
}

fn reset_backend_diagnostics() {
    ACTIVE_DIAGNOSTICS.with(|slot| *slot.borrow_mut() = DiagnosticsConfig::default());
}

fn backend_diag_info(message: impl FnOnce() -> String) {
    backend_diag_write_file("info", DiagnosticLogLevel::Basic, message);
}

fn backend_diag_warning(message: impl FnOnce() -> String) {
    backend_diag_write_file("warn", DiagnosticLogLevel::Basic, message);
}

fn backend_diag_trace(message: impl FnOnce() -> String) {
    backend_diag_write_file("trace", DiagnosticLogLevel::Trace, message);
}

fn backend_diag_write_file(
    severity: &str,
    required: DiagnosticLogLevel,
    message: impl FnOnce() -> String,
) {
    let diagnostics = ACTIVE_DIAGNOSTICS.with(|slot| slot.borrow().clone());
    if !diagnostics_enabled(&diagnostics, required) {
        return;
    }
    #[cfg(test)]
    {
        let _ = (severity, message);
    }
    #[cfg(not(test))]
    {
        write_diag_line(&diagnostics, severity, required, &message());
    }
}

fn diagnostics_enabled(diagnostics: &DiagnosticsConfig, required: DiagnosticLogLevel) -> bool {
    diagnostics.level != DiagnosticLogLevel::Off && diagnostics.level >= required
}

#[cfg(not(test))]
fn write_diag_line(
    diagnostics: &DiagnosticsConfig,
    severity: &str,
    level: DiagnosticLogLevel,
    message: &str,
) {
    LOG_FILE.with(|slot| {
        let mut cached = slot.borrow_mut();
        if cached
            .as_ref()
            .is_none_or(|cached| cached.path.as_ref() != diagnostics.log_path.as_ref())
        {
            *cached = open_log_file(Arc::clone(&diagnostics.log_path));
        }

        let Some(cached) = cached.as_mut() else {
            return;
        };
        let _ = writeln!(
            cached.file,
            "pid={} component=backend_service severity={} level={:?} target=backend_service {}",
            std::process::id(),
            severity,
            level,
            message
        );
    });
}

#[cfg(not(test))]
fn open_log_file(path: Arc<str>) -> Option<CachedLogFile> {
    let path_ref = Path::new(path.as_ref());
    if let Some(parent) = path_ref.parent() {
        let _ = create_dir_all(parent);
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path_ref)
        .ok()?;
    Some(CachedLogFile { path, file })
}

fn cleanup_execution(
    mut execution: ActiveExecution,
    terminal_event: Option<BackendExecutionEvent>,
) -> Result<(), BackendServiceError> {
    backend_diag_info(|| {
        format!(
            "backend_service cleanup_execution slot_id={} session_epoch={} terminal_event={:?} state_before={:?} active_scans={} unfinished_scans={}",
            execution.key.slot_id,
            execution.key.session_epoch,
            terminal_event,
            execution.machine.state(),
            execution.active_scans.len(),
            execution_unfinished_scan_count(&execution)
        )
    });
    let mut cleanup_error = None;

    if let Some(event) = terminal_event {
        if let Err(err) = consume_execution_event(&mut execution.machine, event) {
            cleanup_error.get_or_insert(err);
        }
    }

    if let Some(mut starting) = execution.starting.take() {
        starting.plan_role.abort();
    }

    for (scan_id, mut active_scan) in std::mem::take(&mut execution.active_scans) {
        if let Err(err) = active_scan.producer.abort() {
            cleanup_error.get_or_insert(BackendServiceError::ScanProducer(err));
        }
        active_scan.coordinator.abort();
        let _ = mark_scan_terminal(&mut execution, scan_id, ScanEntryState::Cancelled);
    }

    for entry in execution.scans.values_mut() {
        if entry.leader_lease.is_some() {
            backend_diag_warning(|| {
                format!(
                    "backend_service cleanup_execution releasing remaining scan lease slot_id={} session_epoch={} scan_id={} peer_slot_id={} generation={} lease_epoch={} scan_state={:?}",
                    execution.key.slot_id,
                    execution.key.session_epoch,
                    entry.scan_id,
                    entry.leader_peer.slot_id(),
                    entry.leader_peer.lease_id().generation(),
                    entry.leader_peer.lease_id().lease_epoch(),
                    entry.state
                )
            });
        }
        entry.leader_lease.take();
    }
    if execution.scan_spi.is_some() {
        backend_diag_trace(|| {
            format!(
                "backend_service cleanup_execution will drop shared scan SPI after scan plans slot_id={} session_epoch={} current_mcxt={:p}",
                execution.key.slot_id,
                execution.key.session_epoch,
                diagnostic_current_memory_context()
            )
        });
    }

    if execution.machine.state() == &BackendExecutionState::Terminal {
        if let Err(err) =
            consume_execution_event(&mut execution.machine, BackendExecutionEvent::Cleanup)
        {
            cleanup_error.get_or_insert(err);
        }
    }

    let ActiveExecution {
        key,
        snapshot,
        _logical_plan,
        machine,
        config: _,
        starting,
        scan_spi,
        scans,
        active_scans,
    } = execution;

    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution drop sequence start slot_id={} session_epoch={} scans={} active_scans={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            scans.len(),
            active_scans.len(),
            diagnostic_current_memory_context()
        )
    });

    drop(active_scans);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped active_scans slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });
    drop(scans);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped scans slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });
    drop(scan_spi);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped scan_spi slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });
    drop(starting);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped starting runtime slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });
    drop(machine);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped machine slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });
    drop(_logical_plan);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped logical_plan slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });
    drop(snapshot);
    backend_diag_trace(|| {
        format!(
            "backend_service cleanup_execution dropped snapshot slot_id={} session_epoch={} current_mcxt={:p}",
            key.slot_id,
            key.session_epoch,
            diagnostic_current_memory_context()
        )
    });

    if let Some(err) = cleanup_error {
        reset_backend_diagnostics();
        return Err(err);
    }
    reset_backend_diagnostics();
    Ok(())
}

fn fail_current_execution_after_scan_error(
    active: &mut Option<ActiveExecution>,
    active_scan: ActiveScanStream,
    err: BackendServiceError,
) -> BackendServiceError {
    backend_diag_warning(|| {
        format!(
            "backend_service fatal scan cleanup after error scan_id={} error={}",
            active_scan.scan_id, err
        )
    });
    if let Some(execution) = active.as_mut() {
        execution
            .active_scans
            .insert(active_scan.scan_id, active_scan);
    } else {
        return err;
    }

    let execution = active
        .take()
        .expect("fatal scan cleanup requires an installed execution");
    match cleanup_execution(execution, Some(BackendExecutionEvent::FailExecution)) {
        Ok(()) => err,
        Err(cleanup_err) => BackendServiceError::ProtocolViolation(format!(
            "fatal scan error: {}; cleanup also failed: {}",
            err, cleanup_err
        )),
    }
}

fn finish_current_execution_after_fatal_scan_step(
    active: &mut Option<ActiveExecution>,
    active_scan: ActiveScanStream,
    step: ScanStreamStep,
) -> Result<ScanStreamStep, BackendServiceError> {
    let detail = fatal_scan_step_detail(&step);
    backend_diag_warning(|| {
        format!(
            "backend_service fatal scan step cleanup scan_id={} detail={}",
            active_scan.scan_id, detail
        )
    });
    if let Some(execution) = active.as_mut() {
        execution
            .active_scans
            .insert(active_scan.scan_id, active_scan);
    } else {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "fatal scan step lost active execution before cleanup: {}",
            detail
        )));
    }

    let execution = active
        .take()
        .expect("fatal scan step cleanup requires an installed execution");
    match cleanup_execution(execution, Some(BackendExecutionEvent::FailExecution)) {
        Ok(()) => Ok(step),
        Err(cleanup_err) => Err(BackendServiceError::ProtocolViolation(format!(
            "fatal scan failure: {}; cleanup also failed: {}",
            detail, cleanup_err
        ))),
    }
}

fn fatal_scan_step_detail(step: &ScanStreamStep) -> String {
    match step {
        ScanStreamStep::Failed {
            flow,
            producer_id,
            message,
        } => format!(
            "scan flow {:?} producer_id {} failed: {}",
            flow, producer_id, message
        ),
        ScanStreamStep::Finished { flow } => {
            format!("scan flow {:?} reached EOF on fatal path", flow)
        }
        ScanStreamStep::OutboundPage {
            flow, producer_id, ..
        } => format!(
            "scan flow {:?} producer_id {} emitted a page on fatal path",
            flow, producer_id
        ),
        ScanStreamStep::YieldForControl { reason } => {
            format!("scan yielded for control on fatal path: {:?}", reason)
        }
    }
}

fn consume_execution_event(
    machine: &mut BackendExecutionMachine,
    event: BackendExecutionEvent,
) -> Result<(), BackendServiceError> {
    machine
        .consume(&event)
        .map(|_| ())
        .map_err(|err| BackendServiceError::StateMachine(err.to_string()))
}

fn backend_error_from_caught_error(error: CaughtError) -> BackendServiceError {
    let message = match error {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report.message().to_owned(),
    };
    BackendServiceError::Postgres(message)
}
