use transfer::MessageKind;

/// One logical scan identity scoped to one executor/backend session epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FlowId {
    pub session_epoch: u64,
    pub scan_id: u64,
}

/// Stable producer identifier within one logical scan.
pub type ProducerId = u16;

/// Producer role kind within one logical scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProducerRoleKind {
    Leader,
    Worker,
}

/// Static descriptor for one declared producer in a logical scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProducerDescriptor {
    pub producer_id: ProducerId,
    pub role: ProducerRoleKind,
}

impl ProducerDescriptor {
    pub fn leader(producer_id: ProducerId) -> Self {
        Self {
            producer_id,
            role: ProducerRoleKind::Leader,
        }
    }

    pub fn worker(producer_id: ProducerId) -> Self {
        Self {
            producer_id,
            role: ProducerRoleKind::Worker,
        }
    }
}

/// Open descriptor for one logical scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanOpen {
    pub flow: FlowId,
    pub page_kind: MessageKind,
    pub page_flags: u16,
    pub producers: Vec<ProducerDescriptor>,
}

impl ScanOpen {
    pub fn new(
        flow: FlowId,
        page_kind: MessageKind,
        page_flags: u16,
        producers: Vec<ProducerDescriptor>,
    ) -> Result<Self, crate::ScanOpenError> {
        let scan = Self {
            flow,
            page_kind,
            page_flags,
            producers,
        };
        scan.validate()?;
        Ok(scan)
    }

    pub fn validate(&self) -> Result<(), crate::ScanOpenError> {
        if self.producers.is_empty() {
            return Err(crate::ScanOpenError::EmptyProducerSet);
        }

        let mut leader_seen = false;
        for (index, producer) in self.producers.iter().enumerate() {
            if producer.role == ProducerRoleKind::Leader {
                if leader_seen {
                    return Err(crate::ScanOpenError::MultipleLeaders);
                }
                leader_seen = true;
            }

            if self.producers[..index]
                .iter()
                .any(|other| other.producer_id == producer.producer_id)
            {
                return Err(crate::ScanOpenError::DuplicateProducerId {
                    producer_id: producer.producer_id,
                });
            }
        }

        Ok(())
    }
}

/// Callback result for one producer page attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourcePageStatus {
    /// One page was written; only `payload_len` bytes are valid payload.
    Page {
        payload_len: usize,
    },
    Eof,
}

/// Logical terminal notification propagated across producer and worker roles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalTerminal {
    LogicalEof {
        flow: FlowId,
    },
    LogicalError {
        flow: FlowId,
        producer_id: ProducerId,
        message: String,
    },
}

/// Producer-local terminal state used by coordinator and worker runtimes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProducerTerminalState {
    Active,
    Eof,
    Failed,
}
