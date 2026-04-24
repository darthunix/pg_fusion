use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use control_transport::TransportRegion;
use control_transport::WorkerTransport;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{DataFusionError, Result as DFResult};
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{SinkExt, Stream};
use issuance::{decode_issued_frame, IssuedOwnedFrame, IssuedRx};
use runtime_protocol::codec::{decode_backend_scan_to_worker, decode_runtime_message_family};
use runtime_protocol::message::{
    BackendScanToWorkerRef, RuntimeMessageFamily, WorkerScanToBackend,
};
use runtime_protocol::session::{
    MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
};
use tracing::{debug, warn};

use crate::error::WorkerRuntimeError;
use crate::scan_exec::{OpenScanRequest, ScanBatchSource};
use crate::scan_flow_driver::{
    ScanFlowDriver, ScanFlowDriverStep, ScanFlowOpen, SingleLeaderOpenScanControl,
    SINGLE_SCAN_PRODUCER_ID,
};

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// External provider of per-scan issued ingress bindings.
///
/// `worker_runtime` intentionally keeps scan data-plane ownership outside
/// `WorkerRuntimeCore`. A transport-backed scan source therefore needs an
/// external way to obtain the `IssuedRx` bound to one `(session_epoch, scan_id)`
/// pair before it can open a logical scan.
pub trait ScanIngressProvider: std::fmt::Debug + Send + Sync {
    /// Return the issued ingress receiver for one scan stream.
    fn issued_rx(&self, session_epoch: u64, scan_id: u64) -> Result<IssuedRx, WorkerRuntimeError>;
}

/// Transport-backed production `ScanBatchSource`.
///
/// Each opened scan owns one worker thread for the lifetime of the returned
/// `RecordBatchStream`. That thread:
///
/// - claims the dedicated scan peer for the scan lifetime
/// - sends `OpenScan` / `CancelScan` on that peer
/// - demultiplexes `runtime_protocol::BackendScanToWorker` terminals and
///   fixed-size `issuance` page headers from the same slot
/// - feeds imported `RecordBatch` values back into the DataFusion stream
pub struct TransportScanBatchSource {
    region: TransportRegion,
    control_frame_capacity: usize,
    ingress: Arc<dyn ScanIngressProvider>,
}

impl std::fmt::Debug for TransportScanBatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportScanBatchSource")
            .field("control_frame_capacity", &self.control_frame_capacity)
            .finish_non_exhaustive()
    }
}

impl TransportScanBatchSource {
    /// Build one transport-backed scan source over a shared transport region.
    pub fn new(
        region: TransportRegion,
        control_frame_capacity: usize,
        ingress: Arc<dyn ScanIngressProvider>,
    ) -> Result<Self, WorkerRuntimeError> {
        let inbound_capacity = region.backend_to_worker_capacity();
        if inbound_capacity < MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY {
            return Err(WorkerRuntimeError::ScanTransportRingTooSmall {
                direction: "backend_to_worker",
                required: MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
                actual: inbound_capacity,
            });
        }

        let outbound_capacity = region.worker_to_backend_capacity();
        if outbound_capacity < MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY {
            return Err(WorkerRuntimeError::ScanTransportRingTooSmall {
                direction: "worker_to_backend",
                required: MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
                actual: outbound_capacity,
            });
        }

        let required = region.backend_to_worker_capacity();
        if control_frame_capacity < required {
            return Err(WorkerRuntimeError::ControlFrameCapacityTooSmall {
                required,
                actual: control_frame_capacity,
            });
        }
        Ok(Self {
            region,
            control_frame_capacity,
            ingress,
        })
    }
}

impl ScanBatchSource for TransportScanBatchSource {
    fn open_scan(&self, request: OpenScanRequest) -> DFResult<SendableRecordBatchStream> {
        // Fail fast on worker-registry mismatches instead of surfacing them
        // only after the scan thread starts polling.
        WorkerTransport::attach(&self.region)
            .map_err(|err| DataFusionError::External(Box::new(WorkerRuntimeError::from(err))))?;
        debug!(
            component = "worker_scan",
            session_epoch = request.session_epoch,
            scan_id = request.scan_id.get(),
            peer = ?request.peer,
            "opening transport-backed scan stream"
        );

        let issued_rx = self
            .ingress
            .issued_rx(request.session_epoch, request.scan_id.get())
            .map_err(df_external)?;
        let schema = Arc::clone(&request.output_schema);
        let (tx, rx) = channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = Arc::clone(&stop);
        let region = self.region;
        let control_frame_capacity = self.control_frame_capacity;
        let thread_name = format!(
            "pgf-scan-{}-{}",
            request.session_epoch,
            request.scan_id.get()
        );
        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_transport_scan_thread(
                    region,
                    control_frame_capacity,
                    request,
                    issued_rx,
                    tx,
                    stop_for_thread,
                )
            })
            .map_err(|err| {
                DataFusionError::External(Box::new(WorkerRuntimeError::ThreadSpawn(
                    err.to_string(),
                )))
            })?;

        Ok(Box::pin(TransportScanStream { schema, rx, stop }))
    }
}

#[derive(Debug)]
struct TransportScanStream {
    schema: SchemaRef,
    rx: Receiver<DFResult<RecordBatch>>,
    stop: Arc<AtomicBool>,
}

impl Stream for TransportScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

impl RecordBatchStream for TransportScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for TransportScanStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

enum ScanInbound<'a> {
    Control(BackendScanToWorkerRef<'a>),
    Issued(IssuedOwnedFrame),
}

fn run_transport_scan_thread(
    region: TransportRegion,
    control_frame_capacity: usize,
    request: OpenScanRequest,
    issued_rx: IssuedRx,
    mut tx: Sender<DFResult<RecordBatch>>,
    stop: Arc<AtomicBool>,
) {
    if let Err(err) = run_transport_scan_thread_inner(
        region,
        control_frame_capacity,
        &request,
        issued_rx,
        &mut tx,
        &stop,
    ) {
        warn!(
            component = "worker_scan",
            session_epoch = request.session_epoch,
            scan_id = request.scan_id.get(),
            peer = ?request.peer,
            error = %err,
            "transport scan thread failed"
        );
        let _ = futures::executor::block_on(tx.send(Err(df_external(err))));
    }
}

fn run_transport_scan_thread_inner(
    region: TransportRegion,
    control_frame_capacity: usize,
    request: &OpenScanRequest,
    issued_rx: IssuedRx,
    tx: &mut Sender<DFResult<RecordBatch>>,
    stop: &AtomicBool,
) -> Result<(), WorkerRuntimeError> {
    let transport = WorkerTransport::attach(&region)?;
    let mut slot = transport.slot_for_backend_lease(request.peer)?;
    let (mut driver, open_scan) = ScanFlowDriver::open(
        ScanFlowOpen::single_leader(
            request.session_epoch,
            request.scan_id.get(),
            request.page_kind,
            request.page_flags,
            Arc::clone(&request.output_schema),
        ),
        issued_rx,
    )?;
    let mut scratch = vec![0_u8; control_frame_capacity];
    debug!(
        component = "worker_scan",
        session_epoch = request.session_epoch,
        scan_id = request.scan_id.get(),
        peer = ?request.peer,
        "transport scan thread sending OpenScan"
    );
    send_open_scan(&mut slot, open_scan, &mut scratch)?;
    debug!(
        component = "worker_scan",
        session_epoch = request.session_epoch,
        scan_id = request.scan_id.get(),
        peer = ?request.peer,
        "transport scan thread entered receive loop"
    );

    let mut terminal = false;
    let loop_result: Result<(), WorkerRuntimeError> = loop {
        if stop.load(Ordering::Acquire) {
            debug!(
                component = "worker_scan",
                session_epoch = request.session_epoch,
                scan_id = request.scan_id.get(),
                peer = ?request.peer,
                "transport scan stream was dropped; terminating scan thread"
            );
            break Ok(());
        }

        let received = {
            let mut rx = slot.from_backend_rx()?;
            rx.recv_frame_into(&mut scratch)?
        };
        let Some(len) = received else {
            thread::sleep(IDLE_POLL_INTERVAL);
            continue;
        };

        match decode_scan_inbound(&scratch[..len])? {
            ScanInbound::Issued(frame) => {
                let step = driver.accept_page_frame(SINGLE_SCAN_PRODUCER_ID, &frame)?;
                if forward_driver_step(&mut driver, step, tx, stop)? {
                    terminal = true;
                    debug!(
                        component = "worker_scan",
                        session_epoch = request.session_epoch,
                        scan_id = request.scan_id.get(),
                        peer = ?request.peer,
                        "transport scan thread reached terminal state from issued page path"
                    );
                    break Ok(());
                }
            }
            ScanInbound::Control(control) => {
                let step = control_to_driver_step(request, &mut driver, control)?;
                if forward_driver_step(&mut driver, step, tx, stop)? {
                    terminal = true;
                    debug!(
                        component = "worker_scan",
                        session_epoch = request.session_epoch,
                        scan_id = request.scan_id.get(),
                        peer = ?request.peer,
                        "transport scan thread reached terminal state from control path"
                    );
                    break Ok(());
                }
            }
        }
    };

    if !terminal {
        debug!(
            component = "worker_scan",
            session_epoch = request.session_epoch,
            scan_id = request.scan_id.get(),
            peer = ?request.peer,
            "transport scan thread sending CancelScan during teardown"
        );
        let _ = send_cancel_scan(
            &mut slot,
            request.session_epoch,
            request.scan_id.get(),
            &mut scratch,
        );
        driver.abort();
    }

    loop_result
}

fn decode_scan_inbound(bytes: &[u8]) -> Result<ScanInbound<'_>, WorkerRuntimeError> {
    match decode_runtime_message_family(bytes) {
        Ok(RuntimeMessageFamily::BackendScanToWorker) => {
            Ok(ScanInbound::Control(decode_backend_scan_to_worker(bytes)?))
        }
        Ok(other) => Err(WorkerRuntimeError::ProtocolViolation(format!(
            "unexpected runtime family {other:?} on dedicated scan peer"
        ))),
        Err(runtime_error)
            if matches!(
                runtime_error,
                runtime_protocol::DecodeError::InvalidMagic { .. }
                    | runtime_protocol::DecodeError::UnsupportedVersion { .. }
                    | runtime_protocol::DecodeError::TruncatedEnvelope { .. }
            ) =>
        {
            match decode_issued_frame(bytes) {
                Ok(frame) => Ok(ScanInbound::Issued(frame)),
                Err(_) => Err(runtime_error.into()),
            }
        }
        Err(runtime_error) => Err(runtime_error.into()),
    }
}

fn control_to_driver_step(
    request: &OpenScanRequest,
    driver: &mut ScanFlowDriver,
    control: BackendScanToWorkerRef<'_>,
) -> Result<ScanFlowDriverStep, WorkerRuntimeError> {
    match control {
        BackendScanToWorkerRef::ScanFinished {
            session_epoch,
            scan_id,
            producer_id,
        } => {
            debug!(
                component = "worker_scan",
                session_epoch, scan_id, producer_id, "received ScanFinished on dedicated scan peer"
            );
            validate_scan_terminal(request, session_epoch, scan_id)?;
            driver.accept_producer_eof(producer_id)
        }
        BackendScanToWorkerRef::ScanFailed {
            session_epoch,
            scan_id,
            producer_id,
            message,
        } => {
            warn!(
                component = "worker_scan",
                session_epoch,
                scan_id,
                producer_id,
                message,
                "received ScanFailed on dedicated scan peer"
            );
            validate_scan_terminal(request, session_epoch, scan_id)?;
            driver.accept_producer_error(producer_id, message.to_string())
        }
    }
}

fn validate_scan_terminal(
    request: &OpenScanRequest,
    session_epoch: u64,
    scan_id: u64,
) -> Result<(), WorkerRuntimeError> {
    if session_epoch != request.session_epoch || scan_id != request.scan_id.get() {
        return Err(WorkerRuntimeError::ProtocolViolation(format!(
            "scan terminal targeted session_epoch={session_epoch}, scan_id={scan_id}; expected session_epoch={}, scan_id={}",
            request.session_epoch,
            request.scan_id.get()
        )));
    }
    Ok(())
}

fn forward_driver_step(
    driver: &mut ScanFlowDriver,
    step: ScanFlowDriverStep,
    tx: &mut Sender<DFResult<RecordBatch>>,
    stop: &AtomicBool,
) -> Result<bool, WorkerRuntimeError> {
    match step {
        ScanFlowDriverStep::Idle => Ok(false),
        ScanFlowDriverStep::Batch { batch, .. } => {
            if futures::executor::block_on(tx.send(Ok(batch))).is_err() {
                stop.store(true, Ordering::Release);
            }
            Ok(false)
        }
        ScanFlowDriverStep::LogicalEof { .. } => {
            driver.close()?;
            Ok(true)
        }
        ScanFlowDriverStep::LogicalError { message, .. } => {
            let _ = futures::executor::block_on(tx.send(Err(DataFusionError::Execution(message))));
            driver.close()?;
            Ok(true)
        }
    }
}

fn send_open_scan(
    slot: &mut control_transport::WorkerSlot<'_>,
    control: SingleLeaderOpenScanControl,
    scratch: &mut [u8],
) -> Result<(), WorkerRuntimeError> {
    let written = control.encode_into(scratch)?;
    let mut tx = slot.to_backend_tx()?;
    let _ = tx.send_frame(&scratch[..written])?;
    Ok(())
}

fn send_cancel_scan(
    slot: &mut control_transport::WorkerSlot<'_>,
    session_epoch: u64,
    scan_id: u64,
    scratch: &mut [u8],
) -> Result<(), WorkerRuntimeError> {
    let message = WorkerScanToBackend::CancelScan {
        session_epoch,
        scan_id,
    };
    let written = runtime_protocol::encoded_len_worker_scan_to_backend(message);
    if written > scratch.len() {
        return Err(WorkerRuntimeError::ControlFrameTooLarge);
    }
    let written = runtime_protocol::encode_worker_scan_to_backend_into(message, scratch)?;
    let mut tx = slot.to_backend_tx()?;
    let _ = tx.send_frame(&scratch[..written])?;
    Ok(())
}

fn df_external(err: WorkerRuntimeError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
