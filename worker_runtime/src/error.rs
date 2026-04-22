use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkerRuntimeError {
    #[error("failed to attach worker transport: {0}")]
    WorkerAttach(#[from] control_transport::WorkerAttachError),
    #[error("worker lifecycle operation failed: {0}")]
    WorkerLifecycle(#[from] control_transport::WorkerLifecycleError),
    #[error("failed to access control slot: {0}")]
    SlotAccess(#[from] control_transport::SlotAccessError),
    #[error("failed to receive worker control frame: {0}")]
    WorkerRx(#[from] control_transport::WorkerRxError),
    #[error("failed to send worker control frame: {0}")]
    WorkerTx(#[from] control_transport::WorkerTxError),
    #[error("runtime protocol decode failed: {0}")]
    RuntimeDecode(#[from] runtime_protocol::DecodeError),
    #[error("runtime protocol encode failed: {0}")]
    RuntimeEncode(#[from] runtime_protocol::EncodeError),
    #[error("runtime protocol violation: {0}")]
    ProtocolViolation(String),
    #[error("runtime protocol scan producer set is invalid: {0}")]
    ProducerSet(#[from] runtime_protocol::ProducerSetError),
    #[error("issued frame decode failed: {0}")]
    IssuedDecode(#[from] issuance::DecodeError),
    #[error("plan flow failed: {0}")]
    PlanFlow(#[from] plan_flow::WorkerPlanError),
    #[error("scan flow failed: {0}")]
    ScanFlow(#[from] scan_flow::WorkerRoleError),
    #[error("scan open descriptor failed: {0}")]
    ScanOpen(#[from] scan_flow::ScanOpenError),
    #[error("arrow page decoder configuration failed: {0}")]
    ImportConfig(#[from] import::ConfigError),
    #[error("arrow page import failed: {0}")]
    Import(#[from] import::ImportError),
    #[error("DataFusion failed: {0}")]
    DataFusion(#[from] datafusion_common::DataFusionError),
    #[error("worker execution FSM rejected transition: {0}")]
    StateMachine(String),
    #[error("cannot {action} while worker execution is in state {state:?}")]
    InvalidState {
        action: &'static str,
        state: crate::fsm::WorkerExecutionState,
    },
    #[error("no active worker execution")]
    NoActiveExecution,
    #[error(
        "received backend traffic from peer {incoming_peer:?} while active execution belongs to {active_peer:?}"
    )]
    BackendPeerMismatch {
        active_peer: control_transport::BackendLeaseSlot,
        incoming_peer: control_transport::BackendLeaseSlot,
    },
    #[error("received a future session epoch {incoming}; current epoch is {current}")]
    FutureSession { current: u64, incoming: u64 },
    #[error("no dedicated scan peer was published for scan_id {scan_id}")]
    MissingScanPeer { scan_id: u64 },
    #[error(
        "no issued scan ingress is configured for session_epoch={session_epoch}, scan_id={scan_id}"
    )]
    MissingScanIngress { session_epoch: u64, scan_id: u64 },
    #[error(
        "control frame scratch capacity is too small: need at least {required} bytes, got {actual}"
    )]
    ControlFrameCapacityTooSmall { required: usize, actual: usize },
    #[error(
        "scan transport {direction} ring is too small: need at least {required} bytes, got {actual}"
    )]
    ScanTransportRingTooSmall {
        direction: &'static str,
        required: usize,
        actual: usize,
    },
    #[error("control frame payload is too large for configured scratch buffer")]
    ControlFrameTooLarge,
    #[error("worker runtime has no physical plan yet")]
    MissingPhysicalPlan,
    #[error("failed to spawn worker scan thread: {0}")]
    ThreadSpawn(String),
}
