use std::collections::BTreeMap;
use std::sync::Arc;

use control_transport::{
    BackendLeaseId, BackendLeaseSlot, CommitOutcome, TransportRegion, WorkerTransport,
};
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionState;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_expr::logical_plan::LogicalPlan;
use issuance::{decode_issued_frame, IssuedOwnedFrame, IssuedRx};
use plan_flow::{FlowId as PlanFlowId, PlanOpen, WorkerPlanRole, WorkerStep as WorkerPlanStep};
use runtime_protocol::{
    classify_session, decode_backend_execution_to_worker, decode_runtime_message_family,
    encode_worker_execution_to_backend_into, encoded_len_worker_execution_to_backend,
    BackendExecutionToWorkerRef as BackendToWorkerRef, ExecutionFailureCode, RuntimeMessageFamily,
    SessionDisposition, WorkerExecutionToBackend as WorkerToBackend,
};
use scan_node::PgScanExtensionPlanner;

use crate::error::WorkerRuntimeError;
use crate::fsm::worker_execution_flow::StateMachine as WorkerExecutionMachine;
use crate::fsm::{WorkerExecutionEvent, WorkerExecutionState};
use crate::scan_exec::{ScanBatchSource, ScanProducerPeer, WorkerPgScanExecFactory};

/// Static configuration for one worker-side runtime instance.
#[derive(Clone, Debug)]
pub struct WorkerRuntimeConfig {
    /// Maximum single control frame payload read from `control_transport`.
    pub control_frame_capacity: usize,
    /// Page kind expected for backend scan result pages.
    pub scan_page_kind: transfer::MessageKind,
    /// Page flags expected for backend scan result pages.
    pub scan_page_flags: u16,
}

impl Default for WorkerRuntimeConfig {
    fn default() -> Self {
        Self {
            control_frame_capacity: 8192,
            scan_page_kind: import::ARROW_LAYOUT_BATCH_KIND,
            scan_page_flags: 0,
        }
    }
}

/// One decoded inbound payload delivered to [`WorkerRuntimeCore`].
#[derive(Debug)]
pub enum DecodedInbound<'a> {
    /// One worker control-plane message decoded from `runtime_protocol`.
    Control(BackendToWorkerRef<'a>),
    /// One issued frame decoded from the fixed-size `issuance` header.
    IssuedFrame(IssuedOwnedFrame),
}

/// Physical plan returned after logical plan decoding and lowering succeed.
#[derive(Debug, Clone)]
pub struct PhysicalPlanResult {
    pub session_epoch: u64,
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Immutable planning work extracted from the runtime while the FSM is in `Planning`.
///
/// Callers run [`Self::plan`] outside the `WorkerRuntimeCore` borrow, then
/// report the result back through `WorkerRuntimeCore::finish_physical_planning`.
#[derive(Debug, Clone)]
pub struct PendingPhysicalPlanning {
    peer: BackendLeaseSlot,
    flow: PlanFlowId,
    config: WorkerRuntimeConfig,
    scan_source: Arc<dyn ScanBatchSource>,
    scan_peers: BTreeMap<u64, Vec<ScanProducerPeer>>,
    logical_plan: LogicalPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetainedSession {
    peer: BackendLeaseSlot,
    session_epoch: u64,
}

/// One externally visible worker-runtime lifecycle step.
#[derive(Debug, Clone)]
pub enum WorkerRuntimeStep {
    Idle,
    StaleControlIgnored {
        current: u64,
        incoming: u64,
    },
    PlanOpened {
        session_epoch: u64,
        plan_id: u64,
    },
    PlanFrameAccepted {
        session_epoch: u64,
    },
    PlanningStarted(PendingPhysicalPlanning),
    PhysicalPlanReady(PhysicalPlanResult),
    PlanningResultIgnored {
        session_epoch: u64,
        plan_id: u64,
    },
    ExecutionCancelled {
        session_epoch: u64,
    },
    ExecutionFailed {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
    ExecutionCompleted {
        session_epoch: u64,
    },
}

/// Sans-IO worker execution runtime for one active backend slot.
///
/// Transport polling is intentionally outside this type. It receives decoded
/// control messages and issued plan frames, owns the worker execution FSM, and
/// performs local physical planning with `PgScanExtensionPlanner`.
pub struct WorkerRuntimeCore {
    config: WorkerRuntimeConfig,
    machine: WorkerExecutionMachine,
    active_peer: Option<BackendLeaseSlot>,
    active_session_epoch: Option<u64>,
    latest_session: Option<RetainedSession>,
    active_plan_flow: Option<PlanFlowId>,
    scan_peers: BTreeMap<u64, Vec<ScanProducerPeer>>,
    plan_role: WorkerPlanRole,
    scan_source: Arc<dyn ScanBatchSource>,
    physical_plan: Option<Arc<dyn ExecutionPlan>>,
}

impl std::fmt::Debug for WorkerRuntimeCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerRuntimeCore")
            .field("state", &self.state())
            .field("active_peer", &self.active_peer)
            .field("active_session_epoch", &self.active_session_epoch)
            .field("latest_session", &self.latest_session)
            .field("active_plan_flow", &self.active_plan_flow)
            .field("scan_peer_count", &self.scan_peers.len())
            .field("has_physical_plan", &self.physical_plan.is_some())
            .finish()
    }
}

impl WorkerRuntimeCore {
    /// Create one fresh worker runtime with no active execution.
    pub fn new(config: WorkerRuntimeConfig, scan_source: Arc<dyn ScanBatchSource>) -> Self {
        Self {
            config,
            machine: WorkerExecutionMachine::new(),
            active_peer: None,
            active_session_epoch: None,
            latest_session: None,
            active_plan_flow: None,
            scan_peers: BTreeMap::new(),
            plan_role: WorkerPlanRole::new(),
            scan_source,
            physical_plan: None,
        }
    }

    /// Current worker execution FSM state.
    pub fn state(&self) -> WorkerExecutionState {
        *self.machine.state()
    }

    /// Epoch of the currently active execution, if any.
    ///
    /// This does not expose the internally retained latest-seen epoch used for
    /// stale control classification after cleanup.
    pub fn session_epoch(&self) -> Option<u64> {
        self.active_session_epoch
    }

    /// Peer key of the currently active execution, if any.
    pub fn active_peer(&self) -> Option<BackendLeaseSlot> {
        self.active_peer
    }

    /// Borrow the currently prepared physical plan, if planning reached `Running`.
    pub fn physical_plan(&self) -> Option<Arc<dyn ExecutionPlan>> {
        self.physical_plan.as_ref().map(Arc::clone)
    }

    /// Take ownership of the currently prepared physical plan.
    pub fn take_physical_plan(&mut self) -> Option<Arc<dyn ExecutionPlan>> {
        self.physical_plan.take()
    }

    /// Decode one already framed inbound control payload.
    ///
    /// `worker_runtime` only multiplexes two framed payload families here:
    /// backend runtime control messages and fixed-size `issuance` headers.
    /// The demux therefore uses wire-shape facts instead of a callback:
    ///
    /// - exact `issuance::ISSUED_HEADER_LEN` => decode as issuance
    /// - shorter framed payloads within the runtime bound => decode as runtime
    /// - longer malformed payloads => reject as issuance traffic
    pub fn decode_inbound(bytes: &[u8]) -> Result<DecodedInbound<'_>, WorkerRuntimeError> {
        match decode_runtime_message_family(bytes) {
            Ok(RuntimeMessageFamily::BackendExecutionToWorker) => {
                let message = decode_backend_execution_to_worker(bytes)?;
                Ok(DecodedInbound::Control(message))
            }
            Ok(other) => Err(runtime_protocol::DecodeError::UnexpectedMessageFamily {
                actual: other as u8,
            }
            .into()),
            Err(runtime_error)
                if matches!(
                    runtime_error,
                    runtime_protocol::DecodeError::InvalidMagic { .. }
                        | runtime_protocol::DecodeError::UnsupportedVersion { .. }
                        | runtime_protocol::DecodeError::TruncatedEnvelope { .. }
                ) =>
            {
                match decode_issued_frame(bytes) {
                    Ok(frame) => Ok(DecodedInbound::IssuedFrame(frame)),
                    Err(_) => Err(runtime_error.into()),
                }
            }
            Err(runtime_error) => Err(runtime_error.into()),
        }
    }

    /// Accept one decoded backend control message from the current backend peer.
    pub fn accept_backend_control(
        &mut self,
        peer: BackendLeaseSlot,
        message: BackendToWorkerRef<'_>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        match message {
            BackendToWorkerRef::StartExecution {
                session_epoch,
                plan,
                scans,
            } => self.start_execution(peer, session_epoch, plan, scans),
            BackendToWorkerRef::CancelExecution { session_epoch } => {
                self.cancel_execution(peer, session_epoch)
            }
            BackendToWorkerRef::FailExecution {
                session_epoch,
                code,
                detail,
            } => self.fail_execution(peer, session_epoch, code, detail),
        }
    }

    /// Accept one issued plan frame while receiving a logical plan.
    pub fn accept_issued_plan_frame(
        &mut self,
        peer: BackendLeaseSlot,
        rx: &IssuedRx,
        frame: &IssuedOwnedFrame,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        self.ensure_state("accept plan frame", WorkerExecutionState::ReceivingPlan)?;
        let active_peer = self
            .active_peer
            .ok_or(WorkerRuntimeError::NoActiveExecution)?;
        if peer != active_peer {
            return Err(WorkerRuntimeError::BackendPeerMismatch {
                active_peer,
                incoming_peer: peer,
            });
        }
        self.consume_event(WorkerExecutionEvent::PlanFrame)?;

        let flow = self
            .active_plan_flow
            .ok_or(WorkerRuntimeError::NoActiveExecution)?;
        match self.plan_role.accept_frame(flow, rx, frame)? {
            WorkerPlanStep::Idle => Ok(WorkerRuntimeStep::PlanFrameAccepted {
                session_epoch: flow.session_epoch,
            }),
            WorkerPlanStep::Plan { flow, plan } => {
                self.consume_event(WorkerExecutionEvent::PlanDecoded)?;
                Ok(WorkerRuntimeStep::PlanningStarted(
                    self.begin_physical_planning(flow, *plan),
                ))
            }
            WorkerPlanStep::LogicalError { flow, message } => {
                self.fail_logical_plan_decode(flow, message)
            }
        }
    }

    /// Commit one previously-started physical planning result back into the runtime.
    ///
    /// If the execution has already been cancelled, failed, or restarted, the
    /// completion is ignored and the runtime state is left unchanged.
    pub fn finish_physical_planning(
        &mut self,
        peer: BackendLeaseSlot,
        flow: PlanFlowId,
        result: Result<Arc<dyn ExecutionPlan>, WorkerRuntimeError>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        if !self.matches_active_planning(peer, flow) {
            return Ok(WorkerRuntimeStep::PlanningResultIgnored {
                session_epoch: flow.session_epoch,
                plan_id: flow.plan_id,
            });
        }

        let physical_plan = match result {
            Ok(physical_plan) => physical_plan,
            Err(err) => return Err(self.fail_planning_stage(err)),
        };

        if let Err(err) = self.plan_role.close() {
            return Err(self.fail_planning_stage(err.into()));
        }

        if let Err(err) = self.consume_event(WorkerExecutionEvent::PhysicalPlanReady) {
            return Err(self.fail_planning_stage(err));
        }

        self.physical_plan = Some(Arc::clone(&physical_plan));
        Ok(WorkerRuntimeStep::PhysicalPlanReady(PhysicalPlanResult {
            session_epoch: flow.session_epoch,
            plan: physical_plan,
        }))
    }

    /// Mark the active execution complete after the physical plan finished.
    pub fn mark_execution_complete(&mut self) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        let session_epoch = self
            .active_session_epoch
            .ok_or(WorkerRuntimeError::NoActiveExecution)?;
        self.ensure_state("complete execution", WorkerExecutionState::Running)?;
        self.consume_event(WorkerExecutionEvent::CompleteExecution)?;
        self.clear_active_execution_state();
        Ok(WorkerRuntimeStep::ExecutionCompleted { session_epoch })
    }

    /// Fail the current local execution and move the runtime into `Terminal`.
    ///
    /// This is the public terminal path for worker-side execution errors after
    /// planning started, such as scan-open, page import, or local DataFusion
    /// execution failures.
    pub fn fail_execution_locally(
        &mut self,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        let session_epoch = self
            .active_session_epoch
            .ok_or(WorkerRuntimeError::NoActiveExecution)?;
        match self.state() {
            WorkerExecutionState::ReceivingPlan
            | WorkerExecutionState::Planning
            | WorkerExecutionState::Running => {}
            state => {
                return Err(WorkerRuntimeError::InvalidState {
                    action: "fail execution locally",
                    state,
                })
            }
        }
        self.consume_event(WorkerExecutionEvent::FailExecution)?;
        self.clear_active_execution_state();
        Ok(WorkerRuntimeStep::ExecutionFailed {
            session_epoch,
            code,
            detail,
        })
    }

    /// Reset transient execution state after the FSM reaches `Terminal`.
    pub fn cleanup(&mut self) -> Result<(), WorkerRuntimeError> {
        self.consume_event(WorkerExecutionEvent::Cleanup)?;
        self.reset_runtime_state();
        Ok(())
    }

    /// Abort the current execution because the underlying transport generation restarted.
    pub fn abort_for_transport_restart(&mut self) -> Result<(), WorkerRuntimeError> {
        if self.state() != WorkerExecutionState::Terminal {
            self.consume_event(WorkerExecutionEvent::TransportRestart)?;
        }
        self.clear_active_execution_state();
        Ok(())
    }

    fn begin_physical_planning(
        &self,
        flow: PlanFlowId,
        logical_plan: LogicalPlan,
    ) -> PendingPhysicalPlanning {
        PendingPhysicalPlanning {
            peer: self
                .active_peer
                .expect("physical planning only starts with an active peer"),
            flow,
            config: self.config.clone(),
            scan_source: Arc::clone(&self.scan_source),
            scan_peers: self.scan_peers.clone(),
            logical_plan,
        }
    }

    fn fail_planning_stage(&mut self, err: WorkerRuntimeError) -> WorkerRuntimeError {
        self.plan_role.abort();
        self.physical_plan = None;
        match self.consume_event(WorkerExecutionEvent::FailExecution) {
            Ok(()) => {
                self.clear_active_execution_state();
                err
            }
            Err(terminal_err) => WorkerRuntimeError::StateMachine(format!(
                "planning failed: {}; failed to transition worker runtime to Terminal: {}",
                err, terminal_err
            )),
        }
    }

    fn fail_logical_plan_decode(
        &mut self,
        flow: PlanFlowId,
        _message: String,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        let step = self.fail_execution_locally(ExecutionFailureCode::Internal, None)?;
        debug_assert!(matches!(
            step,
            WorkerRuntimeStep::ExecutionFailed {
                session_epoch,
                code: ExecutionFailureCode::Internal,
                detail: None
            } if session_epoch == flow.session_epoch
        ));
        Ok(step)
    }

    fn start_execution(
        &mut self,
        peer: BackendLeaseSlot,
        session_epoch: u64,
        plan: runtime_protocol::PlanFlowDescriptor,
        scans: runtime_protocol::ScanChannelSetRef<'_>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        if self.state() != WorkerExecutionState::Idle {
            return Err(WorkerRuntimeError::InvalidState {
                action: "start execution",
                state: self.state(),
            });
        }

        if let Some(current) = self.latest_session {
            if current.peer == peer && session_epoch <= current.session_epoch {
                return Ok(WorkerRuntimeStep::StaleControlIgnored {
                    current: current.session_epoch,
                    incoming: session_epoch,
                });
            }
        }

        self.consume_event(WorkerExecutionEvent::StartExecution)?;
        let flow = PlanFlowId {
            session_epoch,
            plan_id: plan.plan_id,
        };
        let scan_peers = materialize_scan_peer_map(scans)?;
        self.plan_role
            .open(PlanOpen::new(flow, plan.page_kind, plan.page_flags))?;
        self.active_peer = Some(peer);
        self.active_session_epoch = Some(session_epoch);
        self.latest_session = Some(RetainedSession {
            peer,
            session_epoch,
        });
        self.active_plan_flow = Some(flow);
        self.scan_peers = scan_peers;
        self.physical_plan = None;
        Ok(WorkerRuntimeStep::PlanOpened {
            session_epoch,
            plan_id: plan.plan_id,
        })
    }

    fn cancel_execution(
        &mut self,
        peer: BackendLeaseSlot,
        session_epoch: u64,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        match self.classify_incoming_control_session(peer, session_epoch)? {
            SessionDisposition::Stale => {
                let current = self
                    .latest_session
                    .map(|session| session.session_epoch)
                    .unwrap_or_default();
                Ok(WorkerRuntimeStep::StaleControlIgnored {
                    current,
                    incoming: session_epoch,
                })
            }
            SessionDisposition::Future => Err(WorkerRuntimeError::FutureSession {
                current: self
                    .latest_session
                    .map(|session| session.session_epoch)
                    .unwrap_or_default(),
                incoming: session_epoch,
            }),
            SessionDisposition::Current => {
                self.consume_event(WorkerExecutionEvent::CancelExecution)?;
                self.clear_active_execution_state();
                Ok(WorkerRuntimeStep::ExecutionCancelled { session_epoch })
            }
        }
    }

    fn fail_execution(
        &mut self,
        peer: BackendLeaseSlot,
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        match self.classify_incoming_control_session(peer, session_epoch)? {
            SessionDisposition::Stale => {
                let current = self
                    .latest_session
                    .map(|session| session.session_epoch)
                    .unwrap_or_default();
                Ok(WorkerRuntimeStep::StaleControlIgnored {
                    current,
                    incoming: session_epoch,
                })
            }
            SessionDisposition::Future => Err(WorkerRuntimeError::FutureSession {
                current: self
                    .latest_session
                    .map(|session| session.session_epoch)
                    .unwrap_or_default(),
                incoming: session_epoch,
            }),
            SessionDisposition::Current => {
                self.consume_event(WorkerExecutionEvent::FailExecution)?;
                self.clear_active_execution_state();
                Ok(WorkerRuntimeStep::ExecutionFailed {
                    session_epoch,
                    code,
                    detail,
                })
            }
        }
    }

    fn classify_incoming_control_session(
        &self,
        peer: BackendLeaseSlot,
        incoming: u64,
    ) -> Result<SessionDisposition, WorkerRuntimeError> {
        if let Some(active_peer) = self.active_peer {
            if peer != active_peer {
                return Err(WorkerRuntimeError::BackendPeerMismatch {
                    active_peer,
                    incoming_peer: peer,
                });
            }
            let current = self
                .active_session_epoch
                .ok_or(WorkerRuntimeError::NoActiveExecution)?;
            return Ok(classify_session(current, incoming));
        }

        let current = self
            .latest_session
            .ok_or(WorkerRuntimeError::NoActiveExecution)?;
        if peer != current.peer {
            return Ok(SessionDisposition::Stale);
        }
        Ok(match classify_session(current.session_epoch, incoming) {
            SessionDisposition::Future => SessionDisposition::Future,
            SessionDisposition::Current | SessionDisposition::Stale => SessionDisposition::Stale,
        })
    }

    fn clear_active_execution_state(&mut self) {
        self.active_peer = None;
        self.active_session_epoch = None;
        self.active_plan_flow = None;
        self.scan_peers.clear();
        self.plan_role.abort();
        self.physical_plan = None;
    }

    fn matches_active_planning(&self, peer: BackendLeaseSlot, flow: PlanFlowId) -> bool {
        self.state() == WorkerExecutionState::Planning
            && self.active_peer == Some(peer)
            && self.active_session_epoch == Some(flow.session_epoch)
            && self.active_plan_flow == Some(flow)
    }

    fn ensure_state(
        &self,
        action: &'static str,
        expected: WorkerExecutionState,
    ) -> Result<(), WorkerRuntimeError> {
        if self.state() == expected {
            Ok(())
        } else {
            Err(WorkerRuntimeError::InvalidState {
                action,
                state: self.state(),
            })
        }
    }

    fn consume_event(&mut self, event: WorkerExecutionEvent) -> Result<(), WorkerRuntimeError> {
        self.machine
            .consume(&event)
            .map(|_| ())
            .map_err(|err| WorkerRuntimeError::StateMachine(err.to_string()))
    }

    fn reset_runtime_state(&mut self) {
        self.clear_active_execution_state();
    }
}

impl PendingPhysicalPlanning {
    /// Backend peer this planning task still belongs to.
    pub fn peer(&self) -> BackendLeaseSlot {
        self.peer
    }

    /// Logical plan flow identity this planning task belongs to.
    pub fn flow(&self) -> PlanFlowId {
        self.flow
    }

    /// Run worker-side physical lowering outside the runtime borrow.
    pub async fn plan(self) -> Result<Arc<dyn ExecutionPlan>, WorkerRuntimeError> {
        let factory = WorkerPgScanExecFactory::new(
            self.flow.session_epoch,
            Arc::clone(&self.scan_source),
            self.scan_peers,
            self.config.scan_page_kind,
            self.config.scan_page_flags,
        );
        let pg_scan_planner = PgScanExtensionPlanner::new(Arc::new(factory));
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(pg_scan_planner)]);
        let session_state = build_worker_planning_session_state();
        Ok(physical_planner
            .create_physical_plan(&self.logical_plan, &session_state)
            .await?)
    }
}

fn materialize_scan_peer_map(
    scans: runtime_protocol::ScanChannelSetRef<'_>,
) -> Result<BTreeMap<u64, Vec<ScanProducerPeer>>, WorkerRuntimeError> {
    let mut scan_peers: BTreeMap<u64, Vec<ScanProducerPeer>> = BTreeMap::new();
    for channel in scans.iter() {
        let role = match channel.role {
            runtime_protocol::ProducerRole::Leader => scan_flow::ProducerRoleKind::Leader,
            runtime_protocol::ProducerRole::Worker => scan_flow::ProducerRoleKind::Worker,
        };
        scan_peers
            .entry(channel.scan_id)
            .or_default()
            .push(ScanProducerPeer {
                producer_id: channel.producer_id,
                role,
                peer: backend_lease_slot_from_wire(channel.peer),
            });
    }
    Ok(scan_peers)
}

fn backend_lease_slot_from_wire(peer: runtime_protocol::BackendLeaseSlotWire) -> BackendLeaseSlot {
    BackendLeaseSlot::new(
        peer.slot_id(),
        BackendLeaseId::new(peer.generation(), peer.lease_epoch()),
    )
}

fn build_worker_planning_session_state() -> SessionState {
    // Match the repo-wide planning contract: worker-side lowering must stay in
    // one DataFusion partition so a single PostgreSQL scan id never turns into
    // a repartitioned or multi-partition physical pipeline.
    let mut options = ConfigOptions::default();
    options.execution.target_partitions = 1;
    SessionStateBuilder::new()
        .with_config(options.into())
        .with_default_features()
        .build()
}

/// Thin worker-side attachment to `control_transport`.
///
/// It does not own execution semantics. Callers decode payloads with
/// [`WorkerRuntimeCore`] and decide when a slot can be safely touched.
pub struct TransportWorkerRuntime {
    transport: WorkerTransport,
    scratch: Vec<u8>,
}

impl std::fmt::Debug for TransportWorkerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportWorkerRuntime")
            .field("scratch_len", &self.scratch.len())
            .finish_non_exhaustive()
    }
}

impl TransportWorkerRuntime {
    /// Attach one process-local worker handle to the transport region.
    pub fn attach(
        region: &TransportRegion,
        config: &WorkerRuntimeConfig,
    ) -> Result<Self, WorkerRuntimeError> {
        Ok(Self {
            transport: WorkerTransport::attach(region)?,
            scratch: vec![0_u8; config.control_frame_capacity],
        })
    }

    /// Publish a fresh online worker generation for this process.
    pub fn activate_generation(&self, pid: i32) -> Result<u64, WorkerRuntimeError> {
        Ok(self.transport.activate_generation(pid)?)
    }

    /// Invalidate the current worker generation and leave the transport offline.
    pub fn deactivate_generation(&self) -> Result<u64, WorkerRuntimeError> {
        Ok(self.transport.deactivate_generation()?)
    }

    /// Release worker-owned slots during orderly worker shutdown.
    pub fn release_owned_slots_for_exit(&self) {
        self.transport.release_owned_slots_for_exit();
    }

    /// Return the next backend peer with worker-visible traffic in this poll pass.
    ///
    /// The caller owns `cursor` and resets it to `0` for each new outer poll
    /// pass. This keeps the ready-peer lookup usable with the mutable receive
    /// and send APIs on the same `TransportWorkerRuntime`.
    pub fn next_ready_backend_lease(&self, cursor: &mut u32) -> Option<BackendLeaseSlot> {
        self.transport.next_ready_backend_lease(cursor)
    }

    /// Drain all currently queued inbound frames from one backend peer.
    ///
    /// Frames are copied into the transport scratch buffer and exposed as
    /// borrowed slices to `on_frame`, avoiding per-frame heap allocation.
    pub fn recv_peer_frames<F>(
        &mut self,
        peer: BackendLeaseSlot,
        mut on_frame: F,
    ) -> Result<(), WorkerRuntimeError>
    where
        F: FnMut(&[u8]) -> Result<(), WorkerRuntimeError>,
    {
        let mut slot = self.transport.slot_for_backend_lease(peer)?;
        let mut rx = slot.from_backend_rx()?;
        while let Some(len) = rx.recv_frame_into(&mut self.scratch)? {
            on_frame(&self.scratch[..len])?;
        }
        Ok(())
    }

    /// Send one already-encoded control payload through the chosen backend peer.
    pub fn send_peer_bytes(
        &mut self,
        peer: BackendLeaseSlot,
        payload: &[u8],
    ) -> Result<CommitOutcome, WorkerRuntimeError> {
        let mut slot = self.transport.slot_for_backend_lease(peer)?;
        let mut tx = slot.to_backend_tx()?;
        Ok(tx.send_frame(payload)?)
    }

    /// Encode and send one `runtime_protocol` worker-to-backend message.
    pub fn send_peer_message(
        &mut self,
        peer: BackendLeaseSlot,
        message: WorkerToBackend,
    ) -> Result<CommitOutcome, WorkerRuntimeError> {
        let written = encoded_len_worker_execution_to_backend(message);
        if written > self.scratch.len() {
            return Err(WorkerRuntimeError::ControlFrameTooLarge);
        }
        let written = encode_worker_execution_to_backend_into(message, &mut self.scratch)?;
        let mut slot = self.transport.slot_for_backend_lease(peer)?;
        let mut tx = slot.to_backend_tx()?;
        Ok(tx.send_frame(&self.scratch[..written])?)
    }

    /// Encode one control payload directly into the internal scratch buffer and send it.
    ///
    /// This is intended for helpers such as scan-open control payloads that
    /// already know how to encode themselves into a caller-provided scratch
    /// slice.
    pub fn send_peer_encoded<F>(
        &mut self,
        peer: BackendLeaseSlot,
        encode: F,
    ) -> Result<CommitOutcome, WorkerRuntimeError>
    where
        F: FnOnce(&mut [u8]) -> Result<usize, WorkerRuntimeError>,
    {
        let written = encode(&mut self.scratch)?;
        let mut slot = self.transport.slot_for_backend_lease(peer)?;
        let mut tx = slot.to_backend_tx()?;
        Ok(tx.send_frame(&self.scratch[..written])?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::alloc::{alloc_zeroed, dealloc, Layout};
    use std::cmp::Ordering;
    use std::fmt;
    use std::hash::{Hash, Hasher};
    use std::ptr::NonNull;
    use std::sync::Mutex;

    use control_transport::{
        BackendLeaseId, BackendSlotLease, TransportRegion, TransportRegionLayout,
    };
    use datafusion::physical_plan::{ExecutionPlanProperties, SendableRecordBatchStream};
    use datafusion_common::{DFSchema, DFSchemaRef, Result as DFResult};
    use datafusion_expr::logical_plan::{EmptyRelation, Extension as LogicalExtension};
    use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
    use futures::executor::block_on;
    use issuance::{IssuanceConfig, IssuancePool, IssuedTx};
    use pool::{PagePool, PagePoolConfig};
    use runtime_protocol::{
        BackendExecutionToWorker as BackendToWorker, BackendLeaseSlotWire, PlanFlowDescriptor,
        ScanChannelDescriptorWire, ScanChannelSet,
    };
    use transfer::{PageRx, PageTx};

    #[derive(Debug, Default)]
    struct NullScanSource {
        requests: Mutex<usize>,
    }

    impl ScanBatchSource for NullScanSource {
        fn open_scan(
            &self,
            _request: crate::OpenScanRequest,
        ) -> DFResult<SendableRecordBatchStream> {
            *self.requests.lock().unwrap() += 1;
            Err(datafusion_common::DataFusionError::Plan(
                "test source has no batches".into(),
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
            unsafe { IssuancePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
        (region, pool)
    }

    fn init_transport_region(
        slot_count: u32,
        backend_to_worker_cap: usize,
        worker_to_backend_cap: usize,
    ) -> (OwnedRegion, TransportRegion) {
        let layout =
            TransportRegionLayout::new(slot_count, backend_to_worker_cap, worker_to_backend_cap)
                .expect("transport layout");
        let region_mem = OwnedRegion::new(layout.size, layout.align);
        let region =
            unsafe { TransportRegion::init_in_place(region_mem.base, layout.size, layout) }
                .expect("transport region");
        (region_mem, region)
    }

    fn core() -> WorkerRuntimeCore {
        WorkerRuntimeCore::new(
            WorkerRuntimeConfig::default(),
            Arc::new(NullScanSource::default()),
        )
    }

    fn peer_a() -> BackendLeaseSlot {
        BackendLeaseSlot::new(0, BackendLeaseId::new(1, 1))
    }

    fn peer_b() -> BackendLeaseSlot {
        BackendLeaseSlot::new(1, BackendLeaseId::new(1, 2))
    }

    fn accept_from_peer(
        core: &mut WorkerRuntimeCore,
        peer: BackendLeaseSlot,
        message: BackendToWorker<'_>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        let mut encoded =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(message)];
        let written =
            runtime_protocol::encode_backend_execution_to_worker_into(message, &mut encoded)
                .expect("encode backend control");
        let decoded =
            runtime_protocol::decode_backend_execution_to_worker(&encoded[..written]).unwrap();
        core.accept_backend_control(peer, decoded)
    }

    fn accept(
        core: &mut WorkerRuntimeCore,
        message: BackendToWorker<'_>,
    ) -> Result<WorkerRuntimeStep, WorkerRuntimeError> {
        accept_from_peer(core, peer_a(), message)
    }

    fn plan_descriptor(plan_id: u64) -> PlanFlowDescriptor {
        PlanFlowDescriptor {
            plan_id,
            page_kind: 0x5150,
            page_flags: 0,
        }
    }

    fn scan_channel(scan_id: u64, peer: BackendLeaseSlot) -> ScanChannelDescriptorWire {
        ScanChannelDescriptorWire {
            scan_id,
            producer_id: 0,
            role: runtime_protocol::ProducerRole::Leader,
            peer: BackendLeaseSlotWire::new(
                peer.slot_id(),
                peer.lease_id().generation(),
                peer.lease_id().lease_epoch(),
            ),
        }
    }

    #[derive(Debug, Clone)]
    struct UnsupportedNode {
        schema: DFSchemaRef,
    }

    impl PartialEq for UnsupportedNode {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for UnsupportedNode {}

    impl Hash for UnsupportedNode {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0u8.hash(state);
        }
    }

    impl PartialOrd for UnsupportedNode {
        fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
            Some(Ordering::Equal)
        }
    }

    impl UserDefinedLogicalNodeCore for UnsupportedNode {
        fn name(&self) -> &str {
            "Unsupported"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            Vec::new()
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            Vec::new()
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Unsupported")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> datafusion_common::Result<Self> {
            if !exprs.is_empty() || !inputs.is_empty() {
                return Err(datafusion_common::DataFusionError::Plan(
                    "UnsupportedNode does not accept rewrites".into(),
                ));
            }
            Ok(self.clone())
        }
    }

    fn unsupported_plan() -> LogicalPlan {
        LogicalPlan::Extension(LogicalExtension {
            node: Arc::new(UnsupportedNode {
                schema: Arc::new(DFSchema::empty()),
            }),
        })
    }

    fn empty_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    #[test]
    fn worker_planning_session_state_is_single_partition() {
        let state = build_worker_planning_session_state();
        assert_eq!(state.config_options().execution.target_partitions, 1);
    }

    #[test]
    fn start_execution_opens_plan_flow() {
        let mut core = core();
        let step = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        assert!(matches!(
            step,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 10,
                plan_id: 20
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::ReceivingPlan);
        assert_eq!(core.session_epoch(), Some(10));
    }

    #[test]
    fn start_execution_retains_scan_peers_and_cleanup_clears_them() {
        let mut core = core();
        let channels = [scan_channel(7, peer_a()), scan_channel(8, peer_b())];
        let step = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::new(&channels).unwrap(),
            },
        )
        .unwrap();

        assert!(matches!(
            step,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 10,
                plan_id: 20
            }
        ));
        assert_eq!(core.scan_peers[&7][0].peer, peer_a());
        assert_eq!(core.scan_peers[&8][0].peer, peer_b());

        let cancelled = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        assert!(matches!(
            cancelled,
            WorkerRuntimeStep::ExecutionCancelled { session_epoch: 10 }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert_eq!(core.scan_peers.len(), 0);

        core.cleanup().unwrap();
        assert!(core.scan_peers.is_empty());
    }

    #[test]
    fn stale_cancel_is_ignored() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        let step = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 9 },
        )
        .unwrap();

        assert!(matches!(
            step,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 9
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::ReceivingPlan);
    }

    #[test]
    fn current_cancel_reaches_terminal_and_cleanup_resets() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        let step = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();

        assert!(matches!(
            step,
            WorkerRuntimeStep::ExecutionCancelled { session_epoch: 10 }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);

        core.cleanup().unwrap();
        assert_eq!(core.state(), WorkerExecutionState::Idle);
        assert_eq!(core.session_epoch(), None);
    }

    #[test]
    fn local_failure_from_running_reaches_terminal_and_cleanup_resets() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();
        core.consume_event(WorkerExecutionEvent::PhysicalPlanReady)
            .unwrap();
        assert_eq!(core.state(), WorkerExecutionState::Running);

        let failed = core
            .fail_execution_locally(ExecutionFailureCode::Internal, Some(7))
            .unwrap();
        assert!(matches!(
            failed,
            WorkerRuntimeStep::ExecutionFailed {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                detail: Some(7)
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);

        core.cleanup().unwrap();
        assert_eq!(core.state(), WorkerExecutionState::Idle);

        let restart = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 11,
                plan: plan_descriptor(21),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            restart,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 11,
                plan_id: 21
            }
        ));
    }

    #[test]
    fn late_equal_epoch_control_after_complete_is_ignored_before_cleanup() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();
        core.consume_event(WorkerExecutionEvent::PhysicalPlanReady)
            .unwrap();

        let complete = core.mark_execution_complete().unwrap();
        assert!(matches!(
            complete,
            WorkerRuntimeStep::ExecutionCompleted { session_epoch: 10 }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert_eq!(core.session_epoch(), None);

        let cancel = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        assert!(matches!(
            cancel,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));

        let fail = accept(
            &mut core,
            BackendToWorker::FailExecution {
                session_epoch: 10,
                code: ExecutionFailureCode::Cancelled,
                detail: None,
            },
        )
        .unwrap();
        assert!(matches!(
            fail,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));
    }

    #[test]
    fn stale_cancel_after_cleanup_is_ignored() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        core.cleanup().unwrap();

        let step = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();

        assert!(matches!(
            step,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Idle);
        assert_eq!(core.session_epoch(), None);
    }

    #[test]
    fn stale_fail_after_cleanup_is_ignored() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        accept(
            &mut core,
            BackendToWorker::FailExecution {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                detail: Some(55),
            },
        )
        .unwrap();
        core.cleanup().unwrap();

        let step = accept(
            &mut core,
            BackendToWorker::FailExecution {
                session_epoch: 9,
                code: ExecutionFailureCode::Cancelled,
                detail: None,
            },
        )
        .unwrap();

        assert!(matches!(
            step,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 9
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Idle);
        assert_eq!(core.session_epoch(), None);
    }

    #[test]
    fn future_cancel_is_rejected() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        let err = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 11 },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            WorkerRuntimeError::FutureSession {
                current: 10,
                incoming: 11
            }
        ));
    }

    #[test]
    fn future_cancel_after_cleanup_uses_latest_epoch() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        core.cleanup().unwrap();

        let err = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 11 },
        )
        .unwrap_err();

        assert!(matches!(
            err,
            WorkerRuntimeError::FutureSession {
                current: 10,
                incoming: 11
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Idle);
        assert_eq!(core.session_epoch(), None);
    }

    #[test]
    fn stale_or_duplicate_start_after_cleanup_is_ignored_but_newer_start_is_accepted() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        core.cleanup().unwrap();

        let duplicate = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(21),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            duplicate,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Idle);

        let stale = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 9,
                plan: plan_descriptor(22),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            stale,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 9
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Idle);

        let newer = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 11,
                plan: plan_descriptor(23),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            newer,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 11,
                plan_id: 23
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::ReceivingPlan);
        assert_eq!(core.session_epoch(), Some(11));
    }

    #[test]
    fn decode_inbound_prefers_runtime_protocol_control() {
        let message = BackendToWorker::CancelExecution { session_epoch: 7 };
        let mut encoded =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(message)];
        let written =
            runtime_protocol::encode_backend_execution_to_worker_into(message, &mut encoded)
                .unwrap();

        let decoded = WorkerRuntimeCore::decode_inbound(&encoded[..written]).unwrap();
        assert!(matches!(
            decoded,
            DecodedInbound::Control(BackendToWorkerRef::CancelExecution { session_epoch: 7 })
        ));
    }

    #[test]
    fn decode_inbound_valid_issued_frame_emits_once() {
        let frame = issuance::encode_issued_frame(IssuedOwnedFrame::Close(transfer::CloseFrame {
            transfer_id: 42,
        }))
        .unwrap();

        assert!(matches!(
            WorkerRuntimeCore::decode_inbound(&frame).unwrap(),
            DecodedInbound::IssuedFrame(IssuedOwnedFrame::Close(transfer::CloseFrame {
                transfer_id: 42
            }))
        ));
    }

    #[test]
    fn decode_inbound_rejects_short_non_control_frame_without_poisoning_next_control() {
        let err = WorkerRuntimeCore::decode_inbound(&[0x91, 0x01]);
        assert!(err.is_err());

        let message = BackendToWorker::CancelExecution { session_epoch: 7 };
        let mut encoded =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(message)];
        let written =
            runtime_protocol::encode_backend_execution_to_worker_into(message, &mut encoded)
                .unwrap();

        assert!(matches!(
            WorkerRuntimeCore::decode_inbound(&encoded[..written]).unwrap(),
            DecodedInbound::Control(BackendToWorkerRef::CancelExecution { session_epoch: 7 })
        ));
    }

    #[test]
    fn decode_inbound_rejects_short_non_control_frame_without_poisoning_next_issued_frame() {
        let err = WorkerRuntimeCore::decode_inbound(&[0x91, 0x01]);
        assert!(err.is_err());

        let frame = issuance::encode_issued_frame(IssuedOwnedFrame::Close(transfer::CloseFrame {
            transfer_id: 99,
        }))
        .unwrap();

        assert!(matches!(
            WorkerRuntimeCore::decode_inbound(&frame).unwrap(),
            DecodedInbound::IssuedFrame(IssuedOwnedFrame::Close(transfer::CloseFrame {
                transfer_id: 99
            }))
        ));
    }

    #[test]
    fn decode_inbound_preserves_runtime_decode_error_for_truncated_control() {
        let message = BackendToWorker::CancelExecution { session_epoch: 7 };
        let mut encoded =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(message)];
        let written =
            runtime_protocol::encode_backend_execution_to_worker_into(message, &mut encoded)
                .unwrap();

        let err = WorkerRuntimeCore::decode_inbound(&encoded[..written - 1]);
        assert!(matches!(err, Err(WorkerRuntimeError::RuntimeDecode(_))));
    }

    #[test]
    fn decode_inbound_preserves_runtime_decode_error_for_corrupted_control_envelope() {
        let message = BackendToWorker::CancelExecution { session_epoch: 7 };
        let mut encoded =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(message)];
        let written =
            runtime_protocol::encode_backend_execution_to_worker_into(message, &mut encoded)
                .unwrap();
        encoded[0] = 0x95;

        let err = WorkerRuntimeCore::decode_inbound(&encoded[..written]);
        assert!(matches!(err, Err(WorkerRuntimeError::RuntimeDecode(_))));
    }

    #[test]
    fn logical_plan_decode_failure_returns_structured_execution_failure_step() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        let (_page_region, page_pool) = init_page_pool(128, 1);
        let (_issuance_region, issuance_pool) = init_issuance_pool(1);
        let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
        let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

        let mut writer = tx.begin(1, 0).expect("writer");
        writer.payload_mut()[..4].copy_from_slice(b"oops");
        let outbound = writer.finish_with_payload_len(4).expect("outbound");
        let frame = outbound.frame();
        outbound.mark_sent();

        let step = core
            .accept_issued_plan_frame(peer_a(), &rx, &frame)
            .unwrap();
        assert!(matches!(
            step,
            WorkerRuntimeStep::ExecutionFailed {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                detail: None
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert_eq!(core.session_epoch(), None);

        let stale = accept(
            &mut core,
            BackendToWorker::FailExecution {
                session_epoch: 10,
                code: ExecutionFailureCode::Cancelled,
                detail: None,
            },
        )
        .unwrap();
        assert!(matches!(
            stale,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));

        core.cleanup().unwrap();
        let restart = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 11,
                plan: plan_descriptor(21),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            restart,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 11,
                plan_id: 21
            }
        ));
    }

    #[test]
    fn physical_planning_failure_reaches_terminal_and_cleanup_allows_restart() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        let flow = core.active_plan_flow.expect("active flow");
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();

        let pending = core.begin_physical_planning(flow, unsupported_plan());
        let peer = pending.peer();
        let err = core
            .finish_physical_planning(peer, flow, block_on(pending.plan()))
            .unwrap_err();

        assert!(matches!(err, WorkerRuntimeError::DataFusion(_)));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert_eq!(core.session_epoch(), None);
        assert!(core.physical_plan().is_none());

        let cancel = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        assert!(matches!(
            cancel,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));

        core.cleanup().unwrap();
        assert_eq!(core.state(), WorkerExecutionState::Idle);
        assert_eq!(core.session_epoch(), None);

        let restart = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 11,
                plan: plan_descriptor(21),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            restart,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 11,
                plan_id: 21
            }
        ));
    }

    #[test]
    fn planning_close_failure_reaches_terminal_and_cleanup_allows_restart() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        let flow = core.active_plan_flow.expect("active flow");
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();

        let pending = core.begin_physical_planning(flow, empty_plan());
        let peer = pending.peer();
        let err = core
            .finish_physical_planning(peer, flow, block_on(pending.plan()))
            .unwrap_err();

        assert!(matches!(err, WorkerRuntimeError::PlanFlow(_)));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert_eq!(core.session_epoch(), None);
        assert!(core.physical_plan().is_none());

        let fail = accept(
            &mut core,
            BackendToWorker::FailExecution {
                session_epoch: 10,
                code: ExecutionFailureCode::Internal,
                detail: Some(7),
            },
        )
        .unwrap();
        assert!(matches!(
            fail,
            WorkerRuntimeStep::StaleControlIgnored {
                current: 10,
                incoming: 10
            }
        ));

        core.cleanup().unwrap();
        assert_eq!(core.state(), WorkerExecutionState::Idle);
        assert_eq!(core.session_epoch(), None);

        let restart = accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 11,
                plan: plan_descriptor(21),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        assert!(matches!(
            restart,
            WorkerRuntimeStep::PlanOpened {
                session_epoch: 11,
                plan_id: 21
            }
        ));
    }

    #[test]
    fn pending_physical_planning_produces_single_partition_plan() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        let flow = core.active_plan_flow.expect("active flow");
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();

        let pending = core.begin_physical_planning(flow, empty_plan());
        let plan = block_on(pending.plan()).expect("physical plan");

        assert_eq!(plan.output_partitioning().partition_count(), 1);
    }

    #[test]
    fn cancel_during_planning_makes_late_completion_non_authoritative() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        let flow = core.active_plan_flow.expect("active flow");
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();
        let pending = core.begin_physical_planning(flow, empty_plan());
        let peer = pending.peer();

        let cancelled = accept(
            &mut core,
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap();
        assert!(matches!(
            cancelled,
            WorkerRuntimeStep::ExecutionCancelled { session_epoch: 10 }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);

        let ignored = core
            .finish_physical_planning(peer, flow, block_on(pending.plan()))
            .unwrap();
        assert!(matches!(
            ignored,
            WorkerRuntimeStep::PlanningResultIgnored {
                session_epoch: 10,
                plan_id: 20
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert!(core.physical_plan().is_none());
    }

    #[test]
    fn transport_restart_during_planning_makes_late_completion_non_authoritative() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();
        let flow = core.active_plan_flow.expect("active flow");
        core.consume_event(WorkerExecutionEvent::PlanDecoded)
            .unwrap();
        let pending = core.begin_physical_planning(flow, empty_plan());
        let peer = pending.peer();

        core.abort_for_transport_restart().unwrap();
        assert_eq!(core.state(), WorkerExecutionState::Terminal);

        let ignored = core
            .finish_physical_planning(peer, flow, block_on(pending.plan()))
            .unwrap();
        assert!(matches!(
            ignored,
            WorkerRuntimeStep::PlanningResultIgnored {
                session_epoch: 10,
                plan_id: 20
            }
        ));
        assert_eq!(core.state(), WorkerExecutionState::Terminal);
        assert!(core.physical_plan().is_none());
    }

    #[test]
    fn foreign_peer_control_is_rejected_while_execution_is_active() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        let err = accept_from_peer(
            &mut core,
            peer_b(),
            BackendToWorker::CancelExecution { session_epoch: 10 },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            WorkerRuntimeError::BackendPeerMismatch {
                active_peer,
                incoming_peer
            } if active_peer == peer_a() && incoming_peer == peer_b()
        ));
    }

    #[test]
    fn foreign_peer_plan_frame_is_rejected_while_receiving_plan() {
        let mut core = core();
        accept(
            &mut core,
            BackendToWorker::StartExecution {
                session_epoch: 10,
                plan: plan_descriptor(20),
                scans: ScanChannelSet::empty(),
            },
        )
        .unwrap();

        let (_page_region, page_pool) = init_page_pool(128, 1);
        let (_issuance_region, issuance_pool) = init_issuance_pool(1);
        let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);
        let frame = IssuedOwnedFrame::Close(transfer::CloseFrame { transfer_id: 1 });

        let err = core
            .accept_issued_plan_frame(peer_b(), &rx, &frame)
            .unwrap_err();
        assert!(matches!(
            err,
            WorkerRuntimeError::BackendPeerMismatch {
                active_peer,
                incoming_peer
            } if active_peer == peer_a() && incoming_peer == peer_b()
        ));
        assert_eq!(core.state(), WorkerExecutionState::ReceivingPlan);
    }

    #[test]
    fn slot_reuse_by_different_backend_peer_accepts_epoch_one_again() {
        let mut core = core();
        let (_transport_region, region) = init_transport_region(1, 128, 128);
        let config = WorkerRuntimeConfig::default();
        let mut worker = TransportWorkerRuntime::attach(&region, &config).expect("attach worker");
        worker
            .activate_generation(std::process::id() as i32)
            .expect("activate generation");
        worker.transport.clear_worker_pid();

        let mut backend_a = BackendSlotLease::acquire(&region).expect("backend a");
        let start_a = BackendToWorker::StartExecution {
            session_epoch: 5,
            plan: plan_descriptor(20),
            scans: ScanChannelSet::empty(),
        };
        let mut encoded_a =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(start_a)];
        let written_a =
            runtime_protocol::encode_backend_execution_to_worker_into(start_a, &mut encoded_a)
                .unwrap();
        backend_a
            .to_worker_tx()
            .send_frame(&encoded_a[..written_a])
            .expect("send start a");

        let mut ready_cursor = 0;
        let actual_peer_a = worker
            .next_ready_backend_lease(&mut ready_cursor)
            .expect("ready peer a");
        worker
            .recv_peer_frames(actual_peer_a, |bytes| {
                match WorkerRuntimeCore::decode_inbound(bytes)? {
                    DecodedInbound::Control(message) => {
                        let step = core.accept_backend_control(actual_peer_a, message)?;
                        assert!(matches!(
                            step,
                            WorkerRuntimeStep::PlanOpened {
                                session_epoch: 5,
                                plan_id: 20
                            }
                        ));
                    }
                    DecodedInbound::IssuedFrame(_) => unreachable!("unexpected issued frame"),
                }
                Ok(())
            })
            .expect("recv start a");
        assert_eq!(worker.next_ready_backend_lease(&mut ready_cursor), None);

        let cancelled = accept_from_peer(
            &mut core,
            actual_peer_a,
            BackendToWorker::CancelExecution { session_epoch: 5 },
        )
        .unwrap();
        assert!(matches!(
            cancelled,
            WorkerRuntimeStep::ExecutionCancelled { session_epoch: 5 }
        ));
        core.cleanup().unwrap();
        let stale_peer = backend_a.backend_lease_slot();
        backend_a.release();

        let mut backend_b = BackendSlotLease::acquire(&region).expect("backend b");
        assert_eq!(backend_b.slot_id(), stale_peer.slot_id());

        let err = worker.send_peer_bytes(stale_peer, b"wrong").unwrap_err();
        assert!(matches!(
            err,
            WorkerRuntimeError::SlotAccess(control_transport::SlotAccessError::StaleLeaseEpoch {
                slot_id: 0,
                claimed_generation: 1,
                ..
            })
        ));
        let mut worker_rx = backend_b.from_worker_rx();
        let mut worker_buf = [0_u8; 16];
        assert_eq!(
            worker_rx.recv_frame_into(&mut worker_buf).expect("empty"),
            None
        );

        let start_b = BackendToWorker::StartExecution {
            session_epoch: 1,
            plan: plan_descriptor(21),
            scans: ScanChannelSet::empty(),
        };
        let mut encoded_b =
            vec![0_u8; runtime_protocol::encoded_len_backend_execution_to_worker(start_b)];
        let written_b =
            runtime_protocol::encode_backend_execution_to_worker_into(start_b, &mut encoded_b)
                .unwrap();
        backend_b
            .to_worker_tx()
            .send_frame(&encoded_b[..written_b])
            .expect("send start b");

        let mut ready_cursor = 0;
        let actual_peer_b = worker
            .next_ready_backend_lease(&mut ready_cursor)
            .expect("ready peer b");
        worker
            .recv_peer_frames(actual_peer_b, |bytes| {
                match WorkerRuntimeCore::decode_inbound(bytes)? {
                    DecodedInbound::Control(message) => {
                        let step = core.accept_backend_control(actual_peer_b, message)?;
                        assert!(matches!(
                            step,
                            WorkerRuntimeStep::PlanOpened {
                                session_epoch: 1,
                                plan_id: 21
                            }
                        ));
                    }
                    DecodedInbound::IssuedFrame(_) => unreachable!("unexpected issued frame"),
                }
                Ok(())
            })
            .expect("recv start b");
        assert_eq!(worker.next_ready_backend_lease(&mut ready_cursor), None);

        assert_ne!(actual_peer_a, actual_peer_b);
        assert_eq!(core.state(), WorkerExecutionState::ReceivingPlan);
        assert_eq!(core.session_epoch(), Some(1));
    }
}
