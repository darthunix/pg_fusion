#![doc = include_str!("../README.md")]

mod error;
mod fsm;
mod source;

#[cfg(test)]
mod tests;

use arrow_layout::{ColumnSpec, TypeTag};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::ScalarValue;
use datafusion_expr::logical_plan::LogicalPlan;
use fsm::backend_execution_flow::StateMachine as BackendExecutionMachine;
pub use fsm::{BackendExecutionAction, BackendExecutionEvent, BackendExecutionState};
use issuance::IssuedTx;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;
use plan_builder::{PlanBuildInput, PlanBuilder};
use plan_flow::{BackendPlanRole, BackendPlanStep, PlanOpen};
use row_estimator::{EstimatorConfig, PageRowEstimator};
use row_estimator_seed::{seed_estimator_config, ProjectedColumnRef};
use runtime_protocol::{
    BackendToWorker, ExecutionFailureCode, PlanFlowDescriptor, ProducerRole, ScanFlowDescriptorRef,
};
use scan_flow::{
    BackendProducerRole, BackendProducerStep, BackendScanCoordinator, FlowId as ScanFlowId,
    LogicalTerminal, ProducerDescriptor, ProducerRoleKind, ScanOpen,
};
use scan_node::PgScanSpec;
use slot_scan::{prepare_scan, PreparedScan, ScanOptions};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

pub use error::BackendServiceError;

const PLAN_ID: u64 = 1;
const SINGLE_SCAN_PRODUCER_ID: u16 = 0;

thread_local! {
    static CURRENT_SESSION_EPOCH: Cell<u64> = const { Cell::new(0) };
    static ACTIVE_EXECUTION: RefCell<Option<ActiveExecution>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "pg_test"))]
thread_local! {
    static WAIT_FOR_SCAN_BACKPRESSURE_ERROR_FOR_TESTS: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendServiceConfig {
    pub scan_payload_block_size: u32,
    pub scan_fetch_batch_rows: u32,
    pub estimator_default: EstimatorConfig,
    pub plan_page_kind: u16,
    pub plan_page_flags: u16,
    pub scan_page_kind: u16,
    pub scan_page_flags: u16,
}

impl Default for BackendServiceConfig {
    fn default() -> Self {
        Self {
            scan_payload_block_size: 4096,
            scan_fetch_batch_rows: 1024,
            estimator_default: EstimatorConfig::default(),
            plan_page_kind: 0x504c,
            plan_page_flags: 0,
            scan_page_kind: import::ARROW_LAYOUT_BATCH_KIND,
            scan_page_flags: 0,
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

    #[cfg(test)]
    fn unregistered_for_tests() -> Self {
        Self {
            snapshot: std::ptr::null_mut(),
            owner: std::ptr::null_mut(),
        }
    }
}

impl Drop for ExecutionSnapshot {
    fn drop(&mut self) {
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
    pub config: BackendServiceConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BeginExecutionOutput {
    pub key: ExecutionKey,
    pub control: BackendToWorker,
}

pub type ExecutionStartStep = BackendPlanStep;

pub struct OpenScanInput<'a> {
    pub slot_id: u32,
    pub session_epoch: u64,
    pub scan_id: u64,
    pub scan: ScanFlowDescriptorRef<'a>,
    pub scan_tx: IssuedTx,
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
    estimator: PageRowEstimator,
    canonical_open: ScanOpen,
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
    scans: Vec<PreparedScanEntry>,
    active_scan: Option<ActiveScanStream>,
    driver_scan_id: Option<u64>,
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
                        self.state = ActiveScanDriverState::AwaitingExecutionTerminal;
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
        if self.state == ActiveScanDriverState::Released {
            return Err(BackendServiceError::ProtocolViolation(
                "scan driver has already been released".into(),
            ));
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
        if self.state == ActiveScanDriverState::Released {
            return;
        }
        let _ = terminate_current_execution_from_driver(
            self.key,
            self.scan_id,
            BackendExecutionEvent::CancelExecution,
        );
        self.state = ActiveScanDriverState::Released;
    }
}

impl BackendService {
    pub fn begin_execution(
        input: StartExecutionInput<'_>,
    ) -> Result<BeginExecutionOutput, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            if let Some(execution) = slot.borrow().as_ref() {
                if let Some(scan_id) = execution.driver_scan_id {
                    return Err(BackendServiceError::ScanDriverActive {
                        action: "begin execution",
                        scan_id,
                    });
                }
                return Err(BackendServiceError::ExecutionAlreadyActive);
            }

            let session_epoch = bump_current_session_epoch();
            let key = ExecutionKey {
                slot_id: input.slot_id,
                session_epoch,
            };

            let built = PlanBuilder::new().build(PlanBuildInput {
                sql: input.sql,
                params: input.params,
            })?;

            let plan_open = PlanOpen::new(
                plan_flow::FlowId {
                    session_epoch,
                    plan_id: PLAN_ID,
                },
                input.config.plan_page_kind,
                input.config.plan_page_flags,
            );

            let mut plan_role = BackendPlanRole::new(input.plan_tx);
            plan_role.open(plan_open, &built.logical_plan)?;

            let scans = built
                .scans
                .iter()
                .cloned()
                .map(|spec| prepare_scan_entry(session_epoch, input.config, spec))
                .collect::<Result<Vec<_>, _>>()?;

            let snapshot = ExecutionSnapshot::capture_current()?;
            let mut machine = BackendExecutionMachine::new();
            consume_execution_event(&mut machine, BackendExecutionEvent::BeginExecution)?;

            let control = BackendToWorker::StartExecution {
                session_epoch,
                plan: PlanFlowDescriptor {
                    plan_id: PLAN_ID,
                    page_kind: input.config.plan_page_kind,
                    page_flags: input.config.plan_page_flags,
                },
            };

            *slot.borrow_mut() = Some(ActiveExecution {
                key,
                snapshot,
                _logical_plan: built.logical_plan,
                machine,
                config: input.config,
                starting: Some(StartingRuntime { plan_role }),
                scans,
                active_scan: None,
                driver_scan_id: None,
            });

            Ok(BeginExecutionOutput { key, control })
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
            cleanup_execution(execution, Some(BackendExecutionEvent::CancelExecution))
        })
    }

    pub fn open_scan(
        input: OpenScanInput<'_>,
    ) -> Result<Option<ActiveScanDriver>, BackendServiceError> {
        ACTIVE_EXECUTION.with(|slot| {
            let mut active = slot.borrow_mut();
            let Some(execution) = active.as_mut() else {
                return classify_missing_execution(input.slot_id, input.session_epoch)
                    .map(|_| None);
            };

            if should_ignore_message(execution, input.slot_id, input.session_epoch)? {
                return Ok(None);
            }
            ensure_execution_state(execution, BackendExecutionState::Running, "open scan")?;
            if let Some(scan_id) = execution.driver_scan_id {
                return Err(BackendServiceError::ScanDriverActive {
                    action: "open scan",
                    scan_id,
                });
            }
            if execution.active_scan.is_some() {
                return Err(BackendServiceError::ProtocolViolation(
                    "only one backend scan may stream at a time".into(),
                ));
            }

            let scan_index = execution
                .scans
                .iter()
                .position(|entry| entry.scan_id == input.scan_id)
                .ok_or(BackendServiceError::UnknownScanId {
                    scan_id: input.scan_id,
                })?;
            let snapshot = execution.snapshot.snapshot;
            let block_size = execution.config.scan_payload_block_size;
            let fetch_batch_rows =
                normalize_scan_fetch_batch_rows(execution.config.scan_fetch_batch_rows);
            let (prepared_scan, schema, estimator, canonical_open) = {
                let entry = &mut execution.scans[scan_index];
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
                    entry.estimator.clone(),
                    entry.canonical_open.clone(),
                )
            };

            let source = source::SlotScanPageSource::new(
                snapshot,
                prepared_scan,
                schema,
                block_size,
                fetch_batch_rows,
                estimator,
            );

            let mut coordinator = BackendScanCoordinator::new();
            coordinator.open(canonical_open.clone())?;

            let mut producer = BackendProducerRole::new(input.scan_tx);
            producer.open(&canonical_open, SINGLE_SCAN_PRODUCER_ID, source)?;

            consume_execution_event(&mut execution.machine, BackendExecutionEvent::OpenScan)?;
            execution.scans[scan_index].state = ScanEntryState::Streaming;
            execution.active_scan = Some(ActiveScanStream {
                scan_id: input.scan_id,
                producer,
                coordinator,
            });
            execution.driver_scan_id = Some(input.scan_id);

            Ok(Some(ActiveScanDriver {
                key: execution.key,
                scan_id: input.scan_id,
                state: ActiveScanDriverState::Streaming,
                _thread_bound: PhantomData,
            }))
        })
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
        ensure_no_scan_driver_active("render explain")?;
        let built = PlanBuilder::new().build(PlanBuildInput {
            sql: input.sql,
            params: input.params,
        })?;
        let rendered = built.logical_plan.display_indent().to_string();
        Ok(rendered)
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

    #[cfg(test)]
    pub(crate) fn current_session_epoch_for_tests() -> u64 {
        current_session_epoch()
    }

    #[cfg(test)]
    pub(crate) fn reset_for_tests() {
        CURRENT_SESSION_EPOCH.with(|epoch| epoch.set(0));
        ACTIVE_EXECUTION.with(|slot| {
            slot.borrow_mut().take();
        });
        BackendService::clear_wait_for_scan_backpressure_error_for_tests();
    }

    #[cfg(test)]
    pub(crate) fn install_fake_execution_for_tests(
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
                scans: Vec::new(),
                active_scan: None,
                driver_scan_id: None,
            });
        });
    }

    #[cfg(test)]
    pub(crate) fn install_starting_execution_with_plan_role_for_tests(
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
                scans: Vec::new(),
                active_scan: None,
                driver_scan_id: None,
            });
        });
    }

    #[cfg(test)]
    pub(crate) fn install_fake_execution_with_driver_for_tests(
        slot_id: u32,
        session_epoch: u64,
        state: BackendExecutionState,
        scan_id: u64,
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
                scans: Vec::new(),
                active_scan: None,
                driver_scan_id: Some(scan_id),
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

fn wait_for_scan_backpressure(blocked_loops: u32) -> Result<bool, BackendServiceError> {
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
            wait_latch(Some(Duration::from_millis(1)));
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

fn ensure_no_scan_driver_active(action: &'static str) -> Result<(), BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let active = slot.borrow();
        if let Some(execution) = active.as_ref() {
            if let Some(scan_id) = execution.driver_scan_id {
                return Err(BackendServiceError::ScanDriverActive { action, scan_id });
            }
        }
        Ok(())
    })
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
    if execution.driver_scan_id != Some(scan_id) {
        return Err(BackendServiceError::ProtocolViolation(format!(
            "scan driver for scan_id {} is not active during {}",
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

fn step_scan_with_driver(
    key: ExecutionKey,
    scan_id: u64,
) -> Result<ScanStreamStep, BackendServiceError> {
    ACTIVE_EXECUTION.with(|slot| {
        let mut active = slot.borrow_mut();
        let mut active_scan = {
            let execution = active
                .as_mut()
                .ok_or(BackendServiceError::NoActiveExecution)?;
            ensure_driver_matches_execution(execution, key, scan_id, "step scan")?;
            ensure_execution_state(execution, BackendExecutionState::Running, "step scan")?;
            execution.active_scan.take().ok_or_else(|| {
                BackendServiceError::ProtocolViolation(format!(
                    "scan driver for scan_id {} lost its active scan stream during step scan",
                    scan_id
                ))
            })?
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
                        .active_scan = Some(active_scan);
                    return Ok(step);
                }
                Ok(ScanDriveOutcome::Blocked) => {
                    // During the bounded internal wait, the stackful scan
                    // session stays detached in `active_scan`. It must be
                    // reinstalled only when we yield control back to the host
                    // loop, or handed straight to fatal cleanup on error.
                    match wait_for_scan_backpressure(blocked_loops) {
                        Ok(true) => {
                            blocked_loops = blocked_loops.saturating_add(1);
                        }
                        Ok(false) => {
                            active
                                .as_mut()
                                .expect("active execution must exist while yielding scan control")
                                .active_scan = Some(active_scan);
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
    config: BackendServiceConfig,
    spec: Arc<PgScanSpec>,
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
    let estimator = PageRowEstimator::new(
        &physical_columns,
        config.scan_payload_block_size,
        seeded_config,
    )?;
    let prepared_scan = prepare_scan(
        &spec.compiled_scan.sql,
        ScanOptions {
            planner_fetch_hint: spec.fetch_hints.planner_fetch_hint,
            local_row_cap: spec.fetch_hints.local_row_cap,
        },
    )?;
    let canonical_open = ScanOpen::new(
        ScanFlowId {
            session_epoch,
            scan_id,
        },
        config.scan_page_kind,
        config.scan_page_flags,
        vec![ProducerDescriptor::leader(SINGLE_SCAN_PRODUCER_ID)],
    )?;

    Ok(PreparedScanEntry {
        scan_id,
        _spec: spec,
        schema,
        prepared_scan,
        estimator,
        canonical_open,
        state: ScanEntryState::Prepared,
    })
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

fn mark_scan_terminal(
    execution: &mut ActiveExecution,
    scan_id: u64,
    state: ScanEntryState,
) -> Result<(), BackendServiceError> {
    let entry = execution
        .scans
        .iter_mut()
        .find(|entry| entry.scan_id == scan_id)
        .ok_or(BackendServiceError::UnknownScanId { scan_id })?;
    entry.state = state;
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
            .active_scan
            .take()
            .ok_or_else(|| {
                BackendServiceError::ProtocolViolation(format!(
                    "scan driver for scan_id {} lost its active scan stream during cancel scan",
                    scan_id
                ))
            })?;
        if active_scan.scan_id != scan_id {
            active
                .as_mut()
                .expect("active execution must exist for mismatched cancel")
                .active_scan = Some(active_scan);
            return Err(BackendServiceError::ScanNotActive { scan_id });
        }

        let cancel_result = {
            let execution = active
                .as_mut()
                .expect("active execution must remain installed during cancel_scan");
            cancel_detached_scan(execution, &mut active_scan)
        };

        match cancel_result {
            Ok(()) => {
                let execution = active
                    .as_mut()
                    .expect("active execution must remain installed after cancel_scan");
                execution.driver_scan_id = None;
                Ok(true)
            }
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
        if let Some(scan_id) = current.driver_scan_id {
            return Err(BackendServiceError::ScanDriverActive {
                action: terminal_event_action(event),
                scan_id,
            });
        }
        if !terminal_event_allowed(*current.machine.state(), event) {
            return Err(BackendServiceError::InvalidExecutionState {
                action: terminal_event_action(event),
                state: *current.machine.state(),
            });
        }

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
        ensure_driver_matches_execution(current, key, scan_id, terminal_event_action(event))?;
        if !terminal_event_allowed(*current.machine.state(), event) {
            return Err(BackendServiceError::InvalidExecutionState {
                action: terminal_event_action(event),
                state: *current.machine.state(),
            });
        }

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

fn cleanup_execution(
    mut execution: ActiveExecution,
    terminal_event: Option<BackendExecutionEvent>,
) -> Result<(), BackendServiceError> {
    let mut cleanup_error = None;
    execution.driver_scan_id = None;

    if let Some(event) = terminal_event {
        if let Err(err) = consume_execution_event(&mut execution.machine, event) {
            cleanup_error.get_or_insert(err);
        }
    }

    if let Some(mut starting) = execution.starting.take() {
        starting.plan_role.abort();
    }

    if let Some(mut active_scan) = execution.active_scan.take() {
        if let Err(err) = active_scan.producer.abort() {
            cleanup_error.get_or_insert(BackendServiceError::ScanProducer(err));
        }
        active_scan.coordinator.abort();
        let _ = mark_scan_terminal(
            &mut execution,
            active_scan.scan_id,
            ScanEntryState::Cancelled,
        );
    }

    if execution.machine.state() == &BackendExecutionState::Terminal {
        if let Err(err) =
            consume_execution_event(&mut execution.machine, BackendExecutionEvent::Cleanup)
        {
            cleanup_error.get_or_insert(err);
        }
    }

    drop(execution);

    if let Some(err) = cleanup_error {
        return Err(err);
    }
    Ok(())
}

fn fail_current_execution_after_scan_error(
    active: &mut Option<ActiveExecution>,
    active_scan: ActiveScanStream,
    err: BackendServiceError,
) -> BackendServiceError {
    if let Some(execution) = active.as_mut() {
        execution.active_scan = Some(active_scan);
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
    if let Some(execution) = active.as_mut() {
        execution.active_scan = Some(active_scan);
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
