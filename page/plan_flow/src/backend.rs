use datafusion_expr::logical_plan::LogicalPlan;
use issuance::{IssuedOutboundPage, IssuedOwnedFrame, IssuedTx, IssuedTxError};
use plan_codec::{EncodeProgress, PlanEncodeSession};

use crate::error::BackendPlanError;
use crate::fsm::backend_plan_flow::StateMachine as BackendPlanMachine;
use crate::fsm::{BackendPlanEvent, BackendPlanState};
use crate::{FlowId, PlanOpen};

/// Result of one backend plan-flow step.
#[derive(Debug)]
pub enum BackendPlanStep {
    /// One detached page is ready to be encoded onto the outer transport.
    OutboundPage {
        /// Flow that produced the page.
        flow: FlowId,
        /// Detached issued page that must be either marked sent or dropped.
        outbound: IssuedOutboundPage,
    },
    /// The logical plan stream has finished and the terminal close frame is
    /// ready for transport.
    CloseFrame {
        /// Flow that produced the close frame.
        flow: FlowId,
        /// Issued transport close frame used as normal EOF.
        frame: IssuedOwnedFrame,
    },
    /// Sender-side progress is temporarily blocked by page-pool / issuance
    /// backpressure or an outstanding unsent page.
    Blocked {
        /// Flow that is currently blocked.
        flow: FlowId,
    },
    /// The backend flow failed logically and should be torn down by the outer
    /// runtime.
    LogicalError {
        /// Flow that failed.
        flow: FlowId,
        /// Human-readable failure message.
        message: String,
    },
}

struct BackendRuntime {
    open: PlanOpen,
    encoder: PlanEncodeSession,
}

/// Backend-side single-sender plan transfer role.
pub struct BackendPlanRole {
    machine: BackendPlanMachine,
    tx: IssuedTx,
    runtime: Option<BackendRuntime>,
}

impl BackendPlanRole {
    /// Create a backend role bound to one issued sender stream.
    pub fn new(tx: IssuedTx) -> Self {
        Self {
            machine: BackendPlanMachine::new(),
            tx,
            runtime: None,
        }
    }

    /// Current backend FSM state.
    pub fn state(&self) -> BackendPlanState {
        *self.machine.state()
    }

    /// Open one logical plan flow for the provided plan.
    ///
    /// The role keeps one inner [`plan_codec::PlanEncodeSession`] alive until
    /// the stream finishes normally via [`Self::step`] and [`Self::close`], or
    /// is reset via [`Self::abort`].
    ///
    /// After a successful close-frame emission and [`Self::close`], the role
    /// becomes exhausted because the embedded [`issuance::IssuedTx`] is
    /// permanently closed and cannot be reused for another flow.
    pub fn open(&mut self, open: PlanOpen, plan: &LogicalPlan) -> Result<(), BackendPlanError> {
        if self.machine.state() != &BackendPlanState::Closed {
            return Err(BackendPlanError::InvalidState {
                action: "open",
                state: *self.machine.state(),
            });
        }

        let encoder = PlanEncodeSession::new(plan)
            .map_err(|err| BackendPlanError::EncoderInit(err.to_string()))?;
        consume_backend_event(&mut self.machine, BackendPlanEvent::Open)?;
        self.runtime = Some(BackendRuntime { open, encoder });
        Ok(())
    }

    /// Advance the backend role by at most one transport action.
    ///
    /// Each successful call emits either:
    ///
    /// - one detached page
    /// - one close frame
    /// - one retryable blocked signal
    /// - one logical error
    pub fn step(&mut self) -> Result<BackendPlanStep, BackendPlanError> {
        match self.machine.state() {
            BackendPlanState::Opened | BackendPlanState::Streaming => {}
            state => {
                return Err(BackendPlanError::InvalidState {
                    action: "step",
                    state: *state,
                });
            }
        }

        let runtime = self
            .runtime
            .as_mut()
            .expect("backend plan flow must be open");
        let flow = runtime.open.flow;

        if runtime.encoder.is_finished() {
            return match self.tx.close() {
                Ok(frame) => {
                    consume_backend_event(&mut self.machine, BackendPlanEvent::EmitClose)?;
                    Ok(BackendPlanStep::CloseFrame { flow, frame })
                }
                Err(IssuedTxError::Tx(transfer::TxError::Busy)) => {
                    Ok(BackendPlanStep::Blocked { flow })
                }
                Err(err) => {
                    consume_backend_event(&mut self.machine, BackendPlanEvent::EmitError)?;
                    Ok(BackendPlanStep::LogicalError {
                        flow,
                        message: format!("failed to emit close frame: {err}"),
                    })
                }
            };
        }

        let mut writer = match self
            .tx
            .begin(runtime.open.page_kind, runtime.open.page_flags)
        {
            Ok(writer) => writer,
            Err(err) if err.is_retryable_backpressure() => {
                return Ok(BackendPlanStep::Blocked { flow });
            }
            Err(err) => {
                consume_backend_event(&mut self.machine, BackendPlanEvent::EmitError)?;
                return Ok(BackendPlanStep::LogicalError {
                    flow,
                    message: format!("failed to begin plan page: {err}"),
                });
            }
        };

        let progress = match runtime.encoder.write_chunk(writer.payload_mut()) {
            Ok(progress) => progress,
            Err(err) => {
                drop(writer);
                consume_backend_event(&mut self.machine, BackendPlanEvent::EmitError)?;
                return Ok(BackendPlanStep::LogicalError {
                    flow,
                    message: format!("failed to encode plan bytes: {err}"),
                });
            }
        };

        let written = match progress {
            EncodeProgress::NeedMoreOutput { written } | EncodeProgress::Done { written } => {
                written
            }
        };
        if written == 0 {
            drop(writer);
            consume_backend_event(&mut self.machine, BackendPlanEvent::EmitError)?;
            return Ok(BackendPlanStep::LogicalError {
                flow,
                message: "plan encoder made no forward progress".into(),
            });
        }

        match writer.finish_with_payload_len(written) {
            Ok(outbound) => {
                consume_backend_event(&mut self.machine, BackendPlanEvent::EmitPage)?;
                Ok(BackendPlanStep::OutboundPage { flow, outbound })
            }
            Err(err) => {
                consume_backend_event(&mut self.machine, BackendPlanEvent::EmitError)?;
                Ok(BackendPlanStep::LogicalError {
                    flow,
                    message: format!("failed to finish plan page: {err}"),
                })
            }
        }
    }

    /// Close a terminal backend role and release its runtime state.
    ///
    /// Closing a successfully completed role moves it into
    /// [`crate::BackendPlanState::Exhausted`], which deliberately prevents
    /// reopening the same sender stream.
    pub fn close(&mut self) -> Result<(), BackendPlanError> {
        match self.machine.state() {
            BackendPlanState::SuccessTerminal | BackendPlanState::Failed => {}
            state => {
                return Err(BackendPlanError::InvalidState {
                    action: "close",
                    state: *state,
                });
            }
        }

        self.runtime = None;
        consume_backend_event(&mut self.machine, BackendPlanEvent::Close)
    }

    /// Reset the backend role immediately.
    ///
    /// Dropping the active encoder and any unsent outbound page returns
    /// sender-side resources such as detached pages and issuance permits.
    ///
    /// If the role has already emitted a successful close frame, the
    /// underlying sender stream is permanently consumed and the role remains
    /// exhausted after abort.
    pub fn abort(&mut self) {
        self.runtime = None;
        self.machine = match self.machine.state() {
            BackendPlanState::SuccessTerminal | BackendPlanState::Exhausted => {
                BackendPlanMachine::from_state(BackendPlanState::Exhausted)
            }
            _ => BackendPlanMachine::new(),
        };
    }
}

fn consume_backend_event(
    machine: &mut BackendPlanMachine,
    event: BackendPlanEvent,
) -> Result<(), BackendPlanError> {
    machine
        .consume(&event)
        .map(|_| ())
        .map_err(|err| BackendPlanError::StateMachine(err.to_string()))
}
