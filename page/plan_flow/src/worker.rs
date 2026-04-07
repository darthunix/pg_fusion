use datafusion_expr::logical_plan::LogicalPlan;
use issuance::{IssueEvent, IssuedOwnedFrame, IssuedReceivedPage, IssuedRx, IssuedRxError};
use plan_codec::{DecodeProgress, PlanDecodeSession};

use crate::error::WorkerPlanError;
use crate::fsm::worker_plan_flow::StateMachine as WorkerPlanMachine;
use crate::fsm::{WorkerPlanEvent, WorkerPlanState};
use crate::{FlowId, PlanOpen};

/// Result of one worker-side plan-flow event.
#[derive(Debug)]
pub enum WorkerStep {
    /// More frames are required before the flow can terminate.
    Idle,
    /// The logical plan was fully decoded after the terminal close frame.
    Plan {
        /// Flow that produced the decoded plan.
        flow: FlowId,
        /// Decoded logical plan.
        plan: Box<LogicalPlan>,
    },
    /// The worker-side flow failed logically.
    LogicalError {
        /// Flow that failed.
        flow: FlowId,
        /// Human-readable failure message.
        message: String,
    },
}

struct WorkerRuntime {
    open: PlanOpen,
    decoder: PlanDecodeSession,
}

/// Worker-side single-receiver plan transfer role.
pub struct WorkerPlanRole {
    machine: WorkerPlanMachine,
    runtime: Option<WorkerRuntime>,
}

impl Default for WorkerPlanRole {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerPlanRole {
    /// Create a worker role with no active flow.
    pub fn new() -> Self {
        Self {
            machine: WorkerPlanMachine::new(),
            runtime: None,
        }
    }

    /// Current worker FSM state.
    pub fn state(&self) -> WorkerPlanState {
        *self.machine.state()
    }

    /// Open one logical plan flow.
    pub fn open(&mut self, open: PlanOpen) -> Result<(), WorkerPlanError> {
        if self.machine.state() != &WorkerPlanState::Closed {
            return Err(WorkerPlanError::InvalidState {
                action: "open",
                state: *self.machine.state(),
            });
        }

        consume_worker_event(&mut self.machine, WorkerPlanEvent::Open)?;
        self.runtime = Some(WorkerRuntime {
            open,
            decoder: PlanDecodeSession::new(),
        });
        Ok(())
    }

    /// Accept one issued transport frame for the active flow.
    ///
    /// Page frames are fed into the inner decoder synchronously from the
    /// detached page payload, and the page is dropped before this method
    /// returns so its page/permit lease goes back to the pool promptly.
    ///
    /// A close frame finalizes the decode and may yield [`WorkerStep::Plan`].
    /// Wrong-flow or terminal-state page frames are still drained so the
    /// detached page returns to the pool.
    pub fn accept_frame(
        &mut self,
        flow: FlowId,
        rx: &IssuedRx,
        frame: &IssuedOwnedFrame,
    ) -> Result<WorkerStep, WorkerPlanError> {
        match self.machine.state() {
            WorkerPlanState::Opened | WorkerPlanState::Receiving => {}
            WorkerPlanState::SuccessTerminal
            | WorkerPlanState::Failed
            | WorkerPlanState::Closed => {
                maybe_drain_rejected_page_frame(rx, frame)?;
                return Err(WorkerPlanError::InvalidState {
                    action: "accept frame",
                    state: *self.machine.state(),
                });
            }
        }

        if let Err(err) = self.validate_active_flow(flow) {
            maybe_drain_rejected_page_frame(rx, frame)?;
            return Err(err);
        }

        match rx.accept(frame) {
            Ok(IssueEvent::Page(page)) => self.accept_page(flow, page),
            Ok(IssueEvent::Closed) => self.accept_close(flow),
            Err(err) if is_ingress_error(&err) => Err(WorkerPlanError::Ingress(err)),
            Err(err) => self.fail_runtime(flow, format!("failed to accept plan frame: {err}")),
        }
    }

    /// Observe an explicit backend-side logical failure for the active flow.
    pub fn accept_sender_error(
        &mut self,
        flow: FlowId,
        message: String,
    ) -> Result<WorkerStep, WorkerPlanError> {
        match self.machine.state() {
            WorkerPlanState::Opened | WorkerPlanState::Receiving => {}
            state => {
                return Err(WorkerPlanError::InvalidState {
                    action: "accept sender error",
                    state: *state,
                });
            }
        }
        self.validate_active_flow(flow)?;
        consume_worker_event(&mut self.machine, WorkerPlanEvent::AcceptSenderError)?;
        Ok(WorkerStep::LogicalError { flow, message })
    }

    /// Close a terminal worker role and release its runtime state.
    pub fn close(&mut self) -> Result<(), WorkerPlanError> {
        match self.machine.state() {
            WorkerPlanState::SuccessTerminal | WorkerPlanState::Failed => {}
            state => {
                return Err(WorkerPlanError::InvalidState {
                    action: "close",
                    state: *state,
                });
            }
        }

        self.runtime = None;
        consume_worker_event(&mut self.machine, WorkerPlanEvent::Close)
    }

    /// Reset the worker role immediately.
    pub fn abort(&mut self) {
        self.runtime = None;
        self.machine = WorkerPlanMachine::new();
    }

    fn accept_page(
        &mut self,
        flow: FlowId,
        page: IssuedReceivedPage,
    ) -> Result<WorkerStep, WorkerPlanError> {
        let runtime = self
            .runtime
            .as_mut()
            .expect("worker plan flow must be open");
        if page.kind() != runtime.open.page_kind || page.flags() != runtime.open.page_flags {
            let message = format!(
                "received unexpected page envelope kind={} flags={}; expected kind={} flags={}",
                page.kind(),
                page.flags(),
                runtime.open.page_kind,
                runtime.open.page_flags
            );
            drop(page);
            return self.fail_runtime(flow, message);
        }

        let result = runtime.decoder.push_chunk(page.payload());
        drop(page);
        match result {
            Ok(DecodeProgress::NeedMoreInput) => {
                consume_worker_event(&mut self.machine, WorkerPlanEvent::AcceptPage)?;
                Ok(WorkerStep::Idle)
            }
            Ok(DecodeProgress::Done(_)) => {
                self.fail_runtime(flow, "plan decoder unexpectedly finished before EOF".into())
            }
            Err(err) => self.fail_runtime(flow, format!("failed to decode plan chunk: {err}")),
        }
    }

    fn accept_close(&mut self, flow: FlowId) -> Result<WorkerStep, WorkerPlanError> {
        let runtime = self
            .runtime
            .as_mut()
            .expect("worker plan flow must be open");
        match runtime.decoder.finish_input() {
            Ok(DecodeProgress::Done(plan)) => {
                consume_worker_event(&mut self.machine, WorkerPlanEvent::AcceptClose)?;
                Ok(WorkerStep::Plan { flow, plan })
            }
            Ok(DecodeProgress::NeedMoreInput) => self.fail_runtime(
                flow,
                "plan decoder requested more input after close frame".into(),
            ),
            Err(err) => self.fail_runtime(flow, format!("failed to finish plan decode: {err}")),
        }
    }

    fn fail_runtime(
        &mut self,
        flow: FlowId,
        message: String,
    ) -> Result<WorkerStep, WorkerPlanError> {
        consume_worker_event(&mut self.machine, WorkerPlanEvent::Fail)?;
        Ok(WorkerStep::LogicalError { flow, message })
    }

    fn validate_active_flow(&self, flow: FlowId) -> Result<(), WorkerPlanError> {
        let runtime = self
            .runtime
            .as_ref()
            .expect("worker plan flow must be open");
        if runtime.open.flow != flow {
            return Err(WorkerPlanError::UnexpectedFlow {
                expected: runtime.open.flow,
                actual: flow,
            });
        }
        Ok(())
    }
}

fn consume_worker_event(
    machine: &mut WorkerPlanMachine,
    event: WorkerPlanEvent,
) -> Result<(), WorkerPlanError> {
    machine
        .consume(&event)
        .map(|_| ())
        .map_err(|err| WorkerPlanError::StateMachine(err.to_string()))
}

fn is_ingress_error(err: &IssuedRxError) -> bool {
    match err {
        IssuedRxError::PermitPoolMismatch { .. } => true,
        IssuedRxError::Rx(transfer::RxError::Busy)
        | IssuedRxError::Rx(transfer::RxError::Closed)
        | IssuedRxError::Rx(transfer::RxError::Access(_))
        | IssuedRxError::Rx(transfer::RxError::Release(_))
        | IssuedRxError::Rx(transfer::RxError::UnexpectedTransferId { .. }) => true,
        IssuedRxError::Rx(transfer::RxError::InvalidPage(_))
        | IssuedRxError::Rx(transfer::RxError::Decode(_))
        | IssuedRxError::PermitRelease(_)
        | IssuedRxError::RxAndPermitRelease { .. } => false,
    }
}

fn maybe_drain_rejected_page_frame(
    rx: &IssuedRx,
    frame: &IssuedOwnedFrame,
) -> Result<(), WorkerPlanError> {
    if !matches!(frame, IssuedOwnedFrame::Page(_)) {
        return Ok(());
    }

    match rx.accept(frame) {
        Ok(IssueEvent::Page(page)) => {
            drop(page);
            Ok(())
        }
        Ok(IssueEvent::Closed) => Ok(()),
        Err(err) => Err(WorkerPlanError::Ingress(err)),
    }
}
