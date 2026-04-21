use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use import::ArrowPageDecoder;
use issuance::{IssuedOwnedFrame, IssuedRx};
use runtime_protocol::{
    encode_worker_to_backend_into, encoded_len_worker_to_backend, ProducerDescriptorWire,
    ProducerRole, ScanFlowDescriptor, WorkerToBackend,
};
use scan_flow::{FlowId, ProducerDescriptor, ProducerId, ScanOpen, WorkerScanRole, WorkerStep};

use crate::error::WorkerRuntimeError;

/// Fixed producer id used by the single-leader worker scan path.
pub const SINGLE_SCAN_PRODUCER_ID: ProducerId = 0;

/// Single-producer scan descriptor for the current worker implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SingleLeaderScanDescriptor {
    pub producer_id: ProducerId,
}

impl Default for SingleLeaderScanDescriptor {
    fn default() -> Self {
        Self {
            producer_id: SINGLE_SCAN_PRODUCER_ID,
        }
    }
}

/// Parameters required to open one worker-side logical scan stream.
#[derive(Debug, Clone)]
pub struct ScanFlowOpen {
    pub session_epoch: u64,
    pub scan_id: u64,
    pub page_kind: transfer::MessageKind,
    pub page_flags: u16,
    pub output_schema: SchemaRef,
    pub producer: SingleLeaderScanDescriptor,
}

impl ScanFlowOpen {
    /// Build one single-leader scan-open descriptor.
    pub fn single_leader(
        session_epoch: u64,
        scan_id: u64,
        page_kind: transfer::MessageKind,
        page_flags: u16,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            session_epoch,
            scan_id,
            page_kind,
            page_flags,
            output_schema,
            producer: SingleLeaderScanDescriptor::default(),
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
    producer_id: ProducerId,
    role: WorkerScanRole,
    rx: IssuedRx,
    decoder: ArrowPageDecoder,
}

/// Heap-free encoder for the single-leader `OpenScan` control payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SingleLeaderOpenScanControl {
    session_epoch: u64,
    scan_id: u64,
    page_kind: transfer::MessageKind,
    page_flags: u16,
    producer_id: ProducerId,
}

impl std::fmt::Debug for ScanFlowDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanFlowDriver")
            .field("flow", &self.flow)
            .field("producer_id", &self.producer_id)
            .field("state", &self.role.state())
            .finish()
    }
}

impl ScanFlowDriver {
    /// Open one worker-side scan-flow role and return its control payload handle.
    pub fn open(
        open: ScanFlowOpen,
        rx: IssuedRx,
    ) -> Result<(Self, SingleLeaderOpenScanControl), WorkerRuntimeError> {
        let flow = FlowId {
            session_epoch: open.session_epoch,
            scan_id: open.scan_id,
        };
        let producer_id = open.producer.producer_id;
        let mut role = WorkerScanRole::new();
        role.open(ScanOpen::new(
            flow,
            open.page_kind,
            open.page_flags,
            vec![ProducerDescriptor::leader(producer_id)],
        )?)?;

        let decoder = ArrowPageDecoder::new(open.output_schema)?;
        let control_payload = SingleLeaderOpenScanControl {
            session_epoch: open.session_epoch,
            scan_id: open.scan_id,
            page_kind: open.page_kind,
            page_flags: open.page_flags,
            producer_id,
        };

        Ok((
            Self {
                flow,
                producer_id,
                role,
                rx,
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
        frame: &IssuedOwnedFrame,
    ) -> Result<ScanFlowDriverStep, WorkerRuntimeError> {
        match self
            .role
            .accept_page_frame(self.flow, producer_id, &self.rx, frame)?
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

impl SingleLeaderOpenScanControl {
    /// Encode this `OpenScan` message into caller-provided scratch storage.
    pub fn encode_into(self, dst: &mut [u8]) -> Result<usize, WorkerRuntimeError> {
        let producers = [ProducerDescriptorWire {
            producer_id: self.producer_id,
            role: ProducerRole::Leader,
        }];
        let scan = ScanFlowDescriptor::new(self.page_kind, self.page_flags, &producers)?;
        let message = WorkerToBackend::OpenScan {
            session_epoch: self.session_epoch,
            scan_id: self.scan_id,
            scan,
        };
        let needed = encoded_len_worker_to_backend(message);
        if needed > dst.len() {
            return Err(WorkerRuntimeError::ControlFrameTooLarge);
        }
        Ok(encode_worker_to_backend_into(message, dst)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use runtime_protocol::{decode_worker_to_backend, WorkerToBackendRef};

    #[test]
    fn open_scan_control_payload_uses_single_leader_producer() {
        let control = SingleLeaderOpenScanControl {
            session_epoch: 11,
            scan_id: 22,
            page_kind: 0x4152,
            page_flags: 0,
            producer_id: 0,
        };
        let mut encoded = [0_u8; 128];
        let written = control.encode_into(&mut encoded).unwrap();
        let decoded = decode_worker_to_backend(&encoded[..written]).unwrap();

        let WorkerToBackendRef::OpenScan {
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
        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].producer_id, 0);
        assert_eq!(producers[0].role, ProducerRole::Leader);
    }
}
