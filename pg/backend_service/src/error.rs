use crate::fsm::BackendExecutionState;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackendServiceError {
    #[error("a backend execution is already active in this process")]
    ExecutionAlreadyActive,
    #[error("no active backend execution")]
    NoActiveExecution,
    #[error("no active backend scan stream")]
    NoActiveScan,
    #[error("cannot {action} while backend execution is in state {state:?}")]
    InvalidExecutionState {
        action: &'static str,
        state: BackendExecutionState,
    },
    #[error(
        "incoming session_epoch {incoming} is newer than current backend session_epoch {current}"
    )]
    FutureSession { current: u64, incoming: u64 },
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    #[error("active PostgreSQL snapshot is required before beginning execution")]
    MissingActiveSnapshot,
    #[error("current PostgreSQL resource owner is null")]
    MissingResourceOwner,
    #[error("scan {scan_id} uses dummy projection and is unsupported in backend_service v1")]
    UnsupportedDummyProjection { scan_id: u64 },
    #[error("unknown scan_id {scan_id} in the current execution")]
    UnknownScanId { scan_id: u64 },
    #[error("scan {scan_id} has already been consumed")]
    ScanAlreadyUsed { scan_id: u64 },
    #[error("scan {scan_id} is already streaming")]
    ScanAlreadyStreaming { scan_id: u64 },
    #[error("active scan stream does not match cancel request for scan_id {scan_id}")]
    ScanNotActive { scan_id: u64 },
    #[error("scan {scan_id} did not produce a logical terminal event on EOF")]
    MissingLogicalTerminal { scan_id: u64 },
    #[error("scan {scan_id} output field {index} has unsupported Arrow type {data_type}")]
    UnsupportedArrowType {
        scan_id: u64,
        index: usize,
        data_type: String,
    },
    #[error("backend execution FSM rejected transition: {0}")]
    StateMachine(String),
    #[error("logical plan build failed: {0}")]
    PlanBuild(#[from] plan_builder::PlanBuildError),
    #[error("scan preparation failed: {0}")]
    PrepareScan(#[from] slot_scan::ScanError),
    #[error("row estimator seed failed: {0}")]
    EstimatorSeed(#[from] row_estimator_seed::SeedError),
    #[error("row estimator failed: {0}")]
    Estimator(#[from] row_estimator::EstimateError),
    #[error("arrow layout planning failed: {0}")]
    Layout(#[from] arrow_layout::LayoutError),
    #[error("plan publication failed: {0}")]
    PlanFlow(#[from] plan_flow::BackendPlanError),
    #[error("scan producer failed: {0}")]
    ScanProducer(#[from] scan_flow::BackendProducerError),
    #[error("invalid scan descriptor: {0}")]
    ScanOpen(#[from] scan_flow::ScanOpenError),
    #[error("scan coordinator failed: {0}")]
    ScanCoordinator(#[from] scan_flow::BackendCoordinatorError),
    #[error("slot encoder configuration failed: {0}")]
    SlotEncoderConfig(#[from] slot_encoder::ConfigError),
    #[error("slot encoder failed: {0}")]
    SlotEncode(#[from] slot_encoder::EncodeError),
    #[error("backend scan page source failed: {0}")]
    PageSource(String),
    #[error("PostgreSQL error: {0}")]
    Postgres(String),
}
