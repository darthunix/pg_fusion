use crate::error::WorkerRoleError;
use crate::fsm::worker_scan_flow::StateMachine as WorkerScanMachine;
use crate::fsm::{WorkerScanEvent, WorkerScanState};
use crate::types::ProducerTerminalState;
use crate::{FlowId, ProducerId, ScanOpen};
use issuance::{IssueEvent, IssuedOwnedFrame, IssuedReceivedPage, IssuedRx, IssuedRxError};

/// Result of one worker-side scan event.
pub enum WorkerStep {
    Idle,
    Page {
        flow: FlowId,
        producer_id: ProducerId,
        page: IssuedReceivedPage,
    },
    LogicalEof {
        flow: FlowId,
    },
    LogicalError {
        flow: FlowId,
        producer_id: ProducerId,
        message: String,
    },
}

struct WorkerProducer {
    producer_id: ProducerId,
    state: ProducerTerminalState,
}

struct WorkerRuntime {
    flow: FlowId,
    page_kind: transfer::MessageKind,
    page_flags: u16,
    producers: Vec<WorkerProducer>,
}

/// Worker-side logical scan fan-in runtime.
pub struct WorkerScanRole {
    machine: WorkerScanMachine,
    runtime: Option<WorkerRuntime>,
}

impl Default for WorkerScanRole {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerScanRole {
    pub fn new() -> Self {
        Self {
            machine: WorkerScanMachine::new(),
            runtime: None,
        }
    }

    /// Current worker role state.
    pub fn state(&self) -> WorkerScanState {
        *self.machine.state()
    }

    /// Open one logical scan with its full declared producer set.
    pub fn open(&mut self, scan: ScanOpen) -> Result<(), WorkerRoleError> {
        if self.machine.state() != &WorkerScanState::Closed {
            return Err(WorkerRoleError::InvalidState {
                action: "open",
                state: *self.machine.state(),
            });
        }
        scan.validate()?;
        let _ = self.machine.consume(&WorkerScanEvent::Open).unwrap();
        self.runtime = Some(WorkerRuntime {
            flow: scan.flow,
            page_kind: scan.page_kind,
            page_flags: scan.page_flags,
            producers: scan
                .producers
                .into_iter()
                .map(|producer| WorkerProducer {
                    producer_id: producer.producer_id,
                    state: ProducerTerminalState::Active,
                })
                .collect(),
        });
        Ok(())
    }

    /// Accept one page frame from the declared producer stream.
    pub fn accept_page_frame(
        &mut self,
        flow: FlowId,
        producer_id: ProducerId,
        rx: &IssuedRx,
        frame: &IssuedOwnedFrame,
    ) -> Result<WorkerStep, WorkerRoleError> {
        match self.machine.state() {
            WorkerScanState::Opened | WorkerScanState::Streaming => {}
            WorkerScanState::CloseTerminal => {
                maybe_drain_rejected_page_frame(rx, frame)?;
                return Err(WorkerRoleError::InvalidState {
                    action: "accept page frame",
                    state: *self.machine.state(),
                });
            }
            WorkerScanState::Closed => {
                maybe_drain_rejected_page_frame(rx, frame)?;
                return Err(WorkerRoleError::InvalidState {
                    action: "accept page frame",
                    state: *self.machine.state(),
                });
            }
        }

        if let Err(err) = self.validate_active_flow(flow) {
            maybe_drain_rejected_page_frame(rx, frame)?;
            return Err(err);
        }
        match self.producer_state(producer_id) {
            Ok(ProducerTerminalState::Active) => {}
            Ok(_) => {
                maybe_drain_rejected_page_frame(rx, frame)?;
                return Err(WorkerRoleError::ProducerAlreadyTerminal { producer_id });
            }
            Err(err) => {
                maybe_drain_rejected_page_frame(rx, frame)?;
                return Err(err);
            }
        }

        match rx.accept(frame) {
            Ok(IssueEvent::Page(page)) => {
                let runtime = self.runtime.as_ref().expect("worker must be open");
                if page.kind() != runtime.page_kind || page.flags() != runtime.page_flags {
                    let message = format!(
                        "received unexpected page envelope kind={} flags={} from producer {}; expected kind={} flags={}",
                        page.kind(),
                        page.flags(),
                        producer_id,
                        runtime.page_kind,
                        runtime.page_flags
                    );
                    drop(page);
                    return Ok(self.fail_logical(flow, producer_id, message));
                }

                let _ = self.machine.consume(&WorkerScanEvent::AcceptPage).unwrap();
                Ok(WorkerStep::Page {
                    flow,
                    producer_id,
                    page,
                })
            }
            Ok(IssueEvent::Closed) => Ok(self.fail_logical(
                flow,
                producer_id,
                "received transfer close frame during scan page stream".to_string(),
            )),
            Err(err) if is_ingress_error(&err) => Err(WorkerRoleError::Ingress(err)),
            Err(err) => Ok(self.fail_logical(
                flow,
                producer_id,
                format!("failed to accept page frame: {err}"),
            )),
        }
    }

    /// Observe EOF from one producer.
    pub fn accept_producer_eof(
        &mut self,
        flow: FlowId,
        producer_id: ProducerId,
    ) -> Result<WorkerStep, WorkerRoleError> {
        match self.machine.state() {
            WorkerScanState::Opened | WorkerScanState::Streaming => {}
            state => {
                return Err(WorkerRoleError::InvalidState {
                    action: "accept producer eof",
                    state: *state,
                });
            }
        }

        self.validate_active_flow(flow)?;
        let new_state = {
            let runtime = self.runtime.as_mut().expect("worker must be open");
            let producer = runtime
                .producers
                .iter_mut()
                .find(|producer| producer.producer_id == producer_id)
                .ok_or(WorkerRoleError::UnknownProducer { producer_id })?;
            if producer.state != ProducerTerminalState::Active {
                return Err(WorkerRoleError::ProducerAlreadyTerminal { producer_id });
            }
            producer.state = ProducerTerminalState::Eof;
            runtime
                .producers
                .iter()
                .all(|producer| producer.state == ProducerTerminalState::Eof)
        };

        if new_state {
            let _ = self
                .machine
                .consume(&WorkerScanEvent::ObserveLogicalEof)
                .unwrap();
            Ok(WorkerStep::LogicalEof { flow })
        } else {
            let _ = self
                .machine
                .consume(&WorkerScanEvent::ObserveProducerEof)
                .unwrap();
            Ok(WorkerStep::Idle)
        }
    }

    /// Observe the first producer error and fail the whole logical scan.
    pub fn accept_producer_error(
        &mut self,
        flow: FlowId,
        producer_id: ProducerId,
        message: String,
    ) -> Result<WorkerStep, WorkerRoleError> {
        match self.machine.state() {
            WorkerScanState::Opened | WorkerScanState::Streaming => {}
            state => {
                return Err(WorkerRoleError::InvalidState {
                    action: "accept producer error",
                    state: *state,
                });
            }
        }

        self.validate_active_flow(flow)?;
        let state = self.producer_state(producer_id)?;
        if state != ProducerTerminalState::Active {
            return Err(WorkerRoleError::ProducerAlreadyTerminal { producer_id });
        }
        Ok(self.fail_logical(flow, producer_id, message))
    }

    /// Close a terminal logical scan.
    pub fn close(&mut self) -> Result<(), WorkerRoleError> {
        if self.machine.state() != &WorkerScanState::CloseTerminal {
            return Err(WorkerRoleError::InvalidState {
                action: "close",
                state: *self.machine.state(),
            });
        }
        self.runtime = None;
        let _ = self.machine.consume(&WorkerScanEvent::Close).unwrap();
        Ok(())
    }

    /// Reset the worker role immediately.
    pub fn abort(&mut self) {
        self.machine = WorkerScanMachine::new();
        self.runtime = None;
    }

    fn validate_active_flow(&self, flow: FlowId) -> Result<(), WorkerRoleError> {
        let runtime = self.runtime.as_ref().expect("worker must be open");
        if runtime.flow != flow {
            return Err(WorkerRoleError::UnexpectedFlow {
                expected: runtime.flow,
                actual: flow,
            });
        }
        Ok(())
    }

    fn producer_state(
        &self,
        producer_id: ProducerId,
    ) -> Result<ProducerTerminalState, WorkerRoleError> {
        let runtime = self.runtime.as_ref().expect("worker must be open");
        runtime
            .producers
            .iter()
            .find(|producer| producer.producer_id == producer_id)
            .map(|producer| producer.state)
            .ok_or(WorkerRoleError::UnknownProducer { producer_id })
    }

    fn fail_logical(
        &mut self,
        flow: FlowId,
        producer_id: ProducerId,
        message: String,
    ) -> WorkerStep {
        let runtime = self.runtime.as_mut().expect("worker must be open");
        if let Some(producer) = runtime
            .producers
            .iter_mut()
            .find(|producer| producer.producer_id == producer_id)
        {
            if producer.state == ProducerTerminalState::Active {
                producer.state = ProducerTerminalState::Failed;
            }
        }
        let _ = self
            .machine
            .consume(&WorkerScanEvent::ObserveProducerError)
            .unwrap();
        WorkerStep::LogicalError {
            flow,
            producer_id,
            message,
        }
    }
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
) -> Result<(), WorkerRoleError> {
    if !matches!(frame, IssuedOwnedFrame::Page(_)) {
        return Ok(());
    }
    match rx.accept(frame) {
        Ok(IssueEvent::Page(page)) => {
            drop(page);
            Ok(())
        }
        Ok(IssueEvent::Closed) => Ok(()),
        Err(err) => Err(WorkerRoleError::Ingress(err)),
    }
}
