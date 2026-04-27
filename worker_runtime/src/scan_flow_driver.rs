use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use import::ArrowPageDecoder;
use issuance::{IssuedOwnedFrame, IssuedRx};
use runtime_protocol::{
    encode_worker_scan_to_backend_into, encoded_len_worker_scan_to_backend, ProducerDescriptorWire,
    ProducerRole, ScanFlowDescriptor, WorkerScanToBackend,
};
use scan_flow::{FlowId, ProducerDescriptor, ProducerId, ScanOpen, WorkerScanRole, WorkerStep};

use crate::error::WorkerRuntimeError;
use crate::scan_exec::ScanProducerPeer;

/// Parameters required to open one worker-side logical scan stream.
#[derive(Debug, Clone)]
pub struct ScanFlowOpen {
    pub session_epoch: u64,
    pub scan_id: u64,
    pub page_kind: transfer::MessageKind,
    pub page_flags: u16,
    pub output_schema: SchemaRef,
    pub producers: Vec<ScanProducerPeer>,
}

impl ScanFlowOpen {
    /// Build one scan-open descriptor with all declared producers.
    pub fn new(
        session_epoch: u64,
        scan_id: u64,
        page_kind: transfer::MessageKind,
        page_flags: u16,
        output_schema: SchemaRef,
        producers: Vec<ScanProducerPeer>,
    ) -> Self {
        Self {
            session_epoch,
            scan_id,
            page_kind,
            page_flags,
            output_schema,
            producers,
        }
    }
}

/// One observable step produced by [`ScanFlowDriver`].
#[derive(Debug)]
pub enum ScanFlowDriverStep {
    Idle,
    Batch {
        flow: FlowId,
        producer_id: ProducerId,
        batch: RecordBatch,
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

/// Worker-side scan flow helper.
///
/// This is deliberately sans-IO for control transport: callers send the
/// returned `OpenScan` control payload on the active slot, then feed issued
/// scan frames into this driver as they arrive.
pub struct ScanFlowDriver {
    flow: FlowId,
    role: WorkerScanRole,
    decoder: ArrowPageDecoder,
}

/// Encoder for a worker `OpenScan` control payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenScanControl {
    session_epoch: u64,
    scan_id: u64,
    page_kind: transfer::MessageKind,
    page_flags: u16,
    producers: Vec<ProducerDescriptorWire>,
}

impl std::fmt::Debug for ScanFlowDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanFlowDriver")
            .field("flow", &self.flow)
            .field("state", &self.role.state())
            .finish()
    }
}

impl ScanFlowDriver {
    /// Open one worker-side scan-flow role and return its control payload handle.
    pub fn open(open: ScanFlowOpen) -> Result<(Self, OpenScanControl), WorkerRuntimeError> {
        let flow = FlowId {
            session_epoch: open.session_epoch,
            scan_id: open.scan_id,
        };
        let producers = open
            .producers
            .iter()
            .map(|producer| ProducerDescriptor {
                producer_id: producer.producer_id,
                role: producer.role,
            })
            .collect::<Vec<_>>();
        let wire_producers = open
            .producers
            .iter()
            .map(|producer| ProducerDescriptorWire {
                producer_id: producer.producer_id,
                role: match producer.role {
                    scan_flow::ProducerRoleKind::Leader => ProducerRole::Leader,
                    scan_flow::ProducerRoleKind::Worker => ProducerRole::Worker,
                },
            })
            .collect::<Vec<_>>();
        let mut role = WorkerScanRole::new();
        role.open(ScanOpen::new(
            flow,
            open.page_kind,
            open.page_flags,
            producers,
        )?)?;

        let decoder = ArrowPageDecoder::new(open.output_schema)?;
        let control_payload = OpenScanControl {
            session_epoch: open.session_epoch,
            scan_id: open.scan_id,
            page_kind: open.page_kind,
            page_flags: open.page_flags,
            producers: wire_producers,
        };

        Ok((
            Self {
                flow,
                role,
                decoder,
            },
            control_payload,
        ))
    }

    /// Flow identity currently owned by this driver.
    pub fn flow(&self) -> FlowId {
        self.flow
    }

    /// Accept one issued scan page frame from the declared producer.
    pub fn accept_page_frame(
        &mut self,
        producer_id: ProducerId,
        rx: &IssuedRx,
        frame: &IssuedOwnedFrame,
    ) -> Result<ScanFlowDriverStep, WorkerRuntimeError> {
        match self
            .role
            .accept_page_frame(self.flow, producer_id, rx, frame)?
        {
            WorkerStep::Idle => Ok(ScanFlowDriverStep::Idle),
            WorkerStep::Page {
                flow,
                producer_id,
                page,
            } => {
                let batch = self.decoder.import_owned(page)?;
                Ok(ScanFlowDriverStep::Batch {
                    flow,
                    producer_id,
                    batch,
                })
            }
            WorkerStep::LogicalEof { flow } => Ok(ScanFlowDriverStep::LogicalEof { flow }),
            WorkerStep::LogicalError {
                flow,
                producer_id,
                message,
            } => Ok(ScanFlowDriverStep::LogicalError {
                flow,
                producer_id,
                message,
            }),
        }
    }

    /// Accept EOF from the single declared producer.
    pub fn accept_producer_eof(
        &mut self,
        producer_id: ProducerId,
    ) -> Result<ScanFlowDriverStep, WorkerRuntimeError> {
        match self.role.accept_producer_eof(self.flow, producer_id)? {
            WorkerStep::Idle => Ok(ScanFlowDriverStep::Idle),
            WorkerStep::LogicalEof { flow } => Ok(ScanFlowDriverStep::LogicalEof { flow }),
            WorkerStep::LogicalError {
                flow,
                producer_id,
                message,
            } => Ok(ScanFlowDriverStep::LogicalError {
                flow,
                producer_id,
                message,
            }),
            WorkerStep::Page { .. } => unreachable!("EOF cannot yield a page"),
        }
    }

    /// Accept a logical scan failure from the declared producer.
    pub fn accept_producer_error(
        &mut self,
        producer_id: ProducerId,
        message: String,
    ) -> Result<ScanFlowDriverStep, WorkerRuntimeError> {
        match self
            .role
            .accept_producer_error(self.flow, producer_id, message)?
        {
            WorkerStep::LogicalError {
                flow,
                producer_id,
                message,
            } => Ok(ScanFlowDriverStep::LogicalError {
                flow,
                producer_id,
                message,
            }),
            WorkerStep::Idle | WorkerStep::LogicalEof { .. } | WorkerStep::Page { .. } => {
                unreachable!("producer error must fail scan")
            }
        }
    }

    /// Close the worker scan role after reaching a terminal scan outcome.
    pub fn close(&mut self) -> Result<(), WorkerRuntimeError> {
        self.role.close()?;
        Ok(())
    }

    /// Abort the worker scan role without requiring a terminal outcome first.
    pub fn abort(&mut self) {
        self.role.abort();
    }
}

impl OpenScanControl {
    /// Encode this `OpenScan` message into caller-provided scratch storage.
    pub fn encode_into(&self, dst: &mut [u8]) -> Result<usize, WorkerRuntimeError> {
        let scan = ScanFlowDescriptor::new(self.page_kind, self.page_flags, &self.producers)?;
        let message = WorkerScanToBackend::OpenScan {
            session_epoch: self.session_epoch,
            scan_id: self.scan_id,
            scan,
        };
        let needed = encoded_len_worker_scan_to_backend(message);
        if needed > dst.len() {
            return Err(WorkerRuntimeError::ControlFrameTooLarge);
        }
        Ok(encode_worker_scan_to_backend_into(message, dst)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use runtime_protocol::{decode_worker_scan_to_backend, WorkerScanToBackendRef};

    #[test]
    fn open_scan_control_payload_uses_declared_producers() {
        let control = OpenScanControl {
            session_epoch: 11,
            scan_id: 22,
            page_kind: 0x4152,
            page_flags: 0,
            producers: vec![
                ProducerDescriptorWire {
                    producer_id: 0,
                    role: ProducerRole::Leader,
                },
                ProducerDescriptorWire {
                    producer_id: 1,
                    role: ProducerRole::Worker,
                },
            ],
        };
        let mut encoded = [0_u8; 128];
        let written = control.encode_into(&mut encoded).unwrap();
        let decoded = decode_worker_scan_to_backend(&encoded[..written]).unwrap();

        let WorkerScanToBackendRef::OpenScan {
            session_epoch,
            scan_id,
            scan,
        } = decoded
        else {
            panic!("expected open scan");
        };

        assert_eq!(session_epoch, 11);
        assert_eq!(scan_id, 22);
        assert_eq!(scan.page_kind, 0x4152);
        assert_eq!(scan.page_flags, 0);
        let producers: Vec<_> = scan.producers().iter().collect();
        assert_eq!(producers.len(), 2);
        assert_eq!(producers[0].producer_id, 0);
        assert_eq!(producers[0].role, ProducerRole::Leader);
        assert_eq!(producers[1].producer_id, 1);
        assert_eq!(producers[1].role, ProducerRole::Worker);
    }
}
