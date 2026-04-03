use crate::error::{BackendCoordinatorError, BackendProducerError, BackendStreamError};
use crate::fsm::backend_coordinator_flow::StateMachine as BackendCoordinatorMachine;
use crate::fsm::backend_producer_flow::StateMachine as BackendProducerMachine;
use crate::fsm::{
    BackendCoordinatorEvent, BackendCoordinatorState, BackendProducerEvent, BackendProducerState,
};
use crate::types::ProducerTerminalState;
use crate::{FlowId, LogicalTerminal, ProducerId, ScanOpen, SourcePageStatus};
use issuance::{IssuedOutboundPage, IssuedTx};

/// Source callback used by one backend producer role.
///
/// `fill_next_page()` writes into the full writable page payload and returns
/// the number of bytes that should be exposed to the receiver for this page.
pub trait BackendPageSource {
    type Error: std::fmt::Display;

    fn open(&mut self, flow: FlowId) -> Result<(), Self::Error>;

    fn fill_next_page(&mut self, payload: &mut [u8]) -> Result<SourcePageStatus, Self::Error>;

    fn close(&mut self) -> Result<(), Self::Error>;
}

/// Result of one backend producer step.
#[derive(Debug)]
pub enum BackendProducerStep {
    OutboundPage {
        flow: FlowId,
        producer_id: ProducerId,
        outbound: IssuedOutboundPage,
    },
    Blocked {
        flow: FlowId,
        producer_id: ProducerId,
    },
    ProducerEof {
        flow: FlowId,
        producer_id: ProducerId,
    },
    ProducerError {
        flow: FlowId,
        producer_id: ProducerId,
        error: BackendStreamError,
    },
}

struct ActiveProducer<S> {
    flow: FlowId,
    producer_id: ProducerId,
    page_kind: transfer::MessageKind,
    page_flags: u16,
    source: S,
}

/// One producer-side runtime over a page source callback.
pub struct BackendProducerRole<S> {
    machine: BackendProducerMachine,
    tx: IssuedTx,
    active: Option<ActiveProducer<S>>,
}

struct CoordinatorProducer {
    producer_id: ProducerId,
    state: ProducerTerminalState,
}

struct CoordinatorRuntime {
    flow: FlowId,
    producers: Vec<CoordinatorProducer>,
}

/// Logical coordinator that aggregates terminal state across declared producers.
pub struct BackendScanCoordinator {
    machine: BackendCoordinatorMachine,
    runtime: Option<CoordinatorRuntime>,
}

impl<S> BackendProducerRole<S> {
    /// Create one producer runtime bound to one sender stream.
    pub fn new(tx: IssuedTx) -> Self {
        Self {
            machine: BackendProducerMachine::new(),
            tx,
            active: None,
        }
    }

    /// Current producer runtime state.
    pub fn state(&self) -> BackendProducerState {
        *self.machine.state()
    }
}

impl<S> BackendProducerRole<S>
where
    S: BackendPageSource,
{
    /// Open one declared producer for the given logical scan.
    pub fn open(
        &mut self,
        scan: &ScanOpen,
        producer_id: ProducerId,
        mut source: S,
    ) -> Result<(), BackendProducerError> {
        if self.machine.state() != &BackendProducerState::Closed {
            return Err(BackendProducerError::InvalidState {
                action: "open",
                state: *self.machine.state(),
            });
        }
        scan.validate()?;
        if !scan.producers.iter().any(|p| p.producer_id == producer_id) {
            return Err(BackendProducerError::UnknownProducer { producer_id });
        }
        source
            .open(scan.flow)
            .map_err(|err| BackendProducerError::SourceOpen(err.to_string()))?;
        let _ = self.machine.consume(&BackendProducerEvent::Open).unwrap();
        self.active = Some(ActiveProducer {
            flow: scan.flow,
            producer_id,
            page_kind: scan.page_kind,
            page_flags: scan.page_flags,
            source,
        });
        Ok(())
    }

    /// Produce the next page, EOF, or producer-local error.
    pub fn step(&mut self) -> Result<BackendProducerStep, BackendProducerError> {
        match self.machine.state() {
            BackendProducerState::Opened | BackendProducerState::Streaming => {}
            state => {
                return Err(BackendProducerError::InvalidState {
                    action: "step",
                    state: *state,
                });
            }
        }

        let active = self
            .active
            .as_mut()
            .expect("active producer must exist while opened");

        let mut writer = match self.tx.begin(active.page_kind, active.page_flags) {
            Ok(writer) => writer,
            Err(err) if err.is_retryable_backpressure() => {
                return Ok(BackendProducerStep::Blocked {
                    flow: active.flow,
                    producer_id: active.producer_id,
                });
            }
            Err(err) => {
                let _ = self
                    .machine
                    .consume(&BackendProducerEvent::EmitError)
                    .unwrap();
                return Ok(BackendProducerStep::ProducerError {
                    flow: active.flow,
                    producer_id: active.producer_id,
                    error: BackendStreamError::Transport(err.to_string()),
                });
            }
        };

        match active.source.fill_next_page(writer.payload_mut()) {
            Ok(SourcePageStatus::Page { payload_len }) => {
                let capacity = writer.payload_mut().len();
                if payload_len > capacity {
                    drop(writer);
                    let _ = self
                        .machine
                        .consume(&BackendProducerEvent::EmitError)
                        .unwrap();
                    return Ok(BackendProducerStep::ProducerError {
                        flow: active.flow,
                        producer_id: active.producer_id,
                        error: BackendStreamError::InvalidPayloadLen {
                            actual: payload_len,
                            capacity,
                        },
                    });
                }

                let outbound = match writer.finish_with_payload_len(payload_len) {
                    Ok(outbound) => outbound,
                    Err(err) => {
                        let _ = self
                            .machine
                            .consume(&BackendProducerEvent::EmitError)
                            .unwrap();
                        return Ok(BackendProducerStep::ProducerError {
                            flow: active.flow,
                            producer_id: active.producer_id,
                            error: BackendStreamError::Transport(err.to_string()),
                        });
                    }
                };
                let _ = self
                    .machine
                    .consume(&BackendProducerEvent::EmitPage)
                    .unwrap();
                Ok(BackendProducerStep::OutboundPage {
                    flow: active.flow,
                    producer_id: active.producer_id,
                    outbound,
                })
            }
            Ok(SourcePageStatus::Eof) => {
                drop(writer);
                let _ = self
                    .machine
                    .consume(&BackendProducerEvent::EmitEof)
                    .unwrap();
                Ok(BackendProducerStep::ProducerEof {
                    flow: active.flow,
                    producer_id: active.producer_id,
                })
            }
            Err(err) => {
                drop(writer);
                let _ = self
                    .machine
                    .consume(&BackendProducerEvent::EmitError)
                    .unwrap();
                Ok(BackendProducerStep::ProducerError {
                    flow: active.flow,
                    producer_id: active.producer_id,
                    error: BackendStreamError::Source(err.to_string()),
                })
            }
        }
    }

    /// Close a terminal producer role and release its source object.
    pub fn close(&mut self) -> Result<(), BackendProducerError> {
        if self.machine.state() != &BackendProducerState::CloseTerminal {
            return Err(BackendProducerError::InvalidState {
                action: "close",
                state: *self.machine.state(),
            });
        }
        let active = self
            .active
            .as_mut()
            .expect("terminal producer must keep source until close");
        active
            .source
            .close()
            .map_err(|err| BackendProducerError::SourceClose(err.to_string()))?;
        self.active = None;
        let _ = self.machine.consume(&BackendProducerEvent::Close).unwrap();
        Ok(())
    }

    /// Reset the producer runtime immediately after attempting source cleanup.
    pub fn abort(&mut self) -> Result<(), BackendProducerError> {
        let close_result = if let Some(mut active) = self.active.take() {
            active
                .source
                .close()
                .map_err(|err| BackendProducerError::SourceClose(err.to_string()))
        } else {
            Ok(())
        };
        self.machine = BackendProducerMachine::new();
        self.active = None;
        close_result
    }
}

impl Default for BackendScanCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendScanCoordinator {
    pub fn new() -> Self {
        Self {
            machine: BackendCoordinatorMachine::new(),
            runtime: None,
        }
    }

    /// Current logical coordinator state.
    pub fn state(&self) -> BackendCoordinatorState {
        *self.machine.state()
    }

    /// Open one logical scan and register its full producer set.
    pub fn open(&mut self, scan: ScanOpen) -> Result<(), BackendCoordinatorError> {
        if self.machine.state() != &BackendCoordinatorState::Closed {
            return Err(BackendCoordinatorError::InvalidState {
                action: "open",
                state: *self.machine.state(),
            });
        }
        scan.validate()?;
        let _ = self
            .machine
            .consume(&BackendCoordinatorEvent::Open)
            .unwrap();
        self.runtime = Some(CoordinatorRuntime {
            flow: scan.flow,
            producers: scan
                .producers
                .into_iter()
                .map(|producer| CoordinatorProducer {
                    producer_id: producer.producer_id,
                    state: ProducerTerminalState::Active,
                })
                .collect(),
        });
        Ok(())
    }

    /// Observe EOF from one producer.
    pub fn accept_producer_eof(
        &mut self,
        flow: FlowId,
        producer_id: ProducerId,
    ) -> Result<Option<LogicalTerminal>, BackendCoordinatorError> {
        match self.machine.state() {
            BackendCoordinatorState::Opened | BackendCoordinatorState::Running => {}
            state => {
                return Err(BackendCoordinatorError::InvalidState {
                    action: "accept producer eof",
                    state: *state,
                });
            }
        }
        let runtime = self.runtime.as_mut().expect("coordinator must be open");
        if runtime.flow != flow {
            return Err(BackendCoordinatorError::UnexpectedFlow {
                expected: runtime.flow,
                actual: flow,
            });
        }
        let producer = runtime
            .producers
            .iter_mut()
            .find(|producer| producer.producer_id == producer_id)
            .ok_or(BackendCoordinatorError::UnknownProducer { producer_id })?;
        if producer.state != ProducerTerminalState::Active {
            return Err(BackendCoordinatorError::ProducerAlreadyTerminal { producer_id });
        }
        producer.state = ProducerTerminalState::Eof;

        if runtime
            .producers
            .iter()
            .all(|producer| producer.state == ProducerTerminalState::Eof)
        {
            let _ = self
                .machine
                .consume(&BackendCoordinatorEvent::ObserveLogicalEof)
                .unwrap();
            Ok(Some(LogicalTerminal::LogicalEof { flow }))
        } else {
            let event = match self.machine.state() {
                BackendCoordinatorState::Opened => BackendCoordinatorEvent::ObserveProducerEof,
                BackendCoordinatorState::Running => BackendCoordinatorEvent::ObserveProducerEof,
                _ => unreachable!("validated above"),
            };
            let _ = self.machine.consume(&event).unwrap();
            Ok(None)
        }
    }

    /// Observe the first producer error and fail the whole logical scan.
    pub fn accept_producer_error(
        &mut self,
        flow: FlowId,
        producer_id: ProducerId,
        message: String,
    ) -> Result<LogicalTerminal, BackendCoordinatorError> {
        match self.machine.state() {
            BackendCoordinatorState::Opened | BackendCoordinatorState::Running => {}
            state => {
                return Err(BackendCoordinatorError::InvalidState {
                    action: "accept producer error",
                    state: *state,
                });
            }
        }
        let runtime = self.runtime.as_mut().expect("coordinator must be open");
        if runtime.flow != flow {
            return Err(BackendCoordinatorError::UnexpectedFlow {
                expected: runtime.flow,
                actual: flow,
            });
        }
        let producer = runtime
            .producers
            .iter_mut()
            .find(|producer| producer.producer_id == producer_id)
            .ok_or(BackendCoordinatorError::UnknownProducer { producer_id })?;
        if producer.state != ProducerTerminalState::Active {
            return Err(BackendCoordinatorError::ProducerAlreadyTerminal { producer_id });
        }
        producer.state = ProducerTerminalState::Failed;
        let _ = self
            .machine
            .consume(&BackendCoordinatorEvent::ObserveProducerError)
            .unwrap();
        Ok(LogicalTerminal::LogicalError {
            flow,
            producer_id,
            message,
        })
    }

    /// Close one terminal logical scan.
    pub fn close(&mut self) -> Result<(), BackendCoordinatorError> {
        if self.machine.state() != &BackendCoordinatorState::CloseTerminal {
            return Err(BackendCoordinatorError::InvalidState {
                action: "close",
                state: *self.machine.state(),
            });
        }
        self.runtime = None;
        let _ = self
            .machine
            .consume(&BackendCoordinatorEvent::Close)
            .unwrap();
        Ok(())
    }

    /// Reset the coordinator immediately.
    pub fn abort(&mut self) {
        self.machine = BackendCoordinatorMachine::new();
        self.runtime = None;
    }
}
