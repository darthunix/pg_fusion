# runtime_protocol

`runtime_protocol` is the typed control-plane message codec for the new
backend/worker runtime.

It intentionally sits:

- above `control_transport`, which only moves opaque framed bytes
- alongside `plan_flow` / `scan_flow`, whose page-stream descriptors it mirrors
- below any backend-side or worker-side execution FSM

The crate is intentionally narrow:

- one control message per `control_transport` frame
- fixed binary runtime envelope header owned by this crate
- borrow-friendly decode for scan-producer descriptors
- borrow-friendly decode for upfront `scan_id -> scan slot` maps
- no dependency on the legacy `protocol` crate
- no snapshot or execution registry ownership

The main contracts are:

- `session_epoch` is carried in every message and used by higher layers to drop
  stale traffic before it mutates fresh execution state
- the primary execution slot carries only execution lifecycle control
- secondary scan slots carry only scan lifecycle control and scan terminal signals
- dedicated scan slots must provide at least `256` bytes backend-to-worker and
  `256` bytes worker-to-backend raw ring capacity; the outbound bound covers
  `OpenScan` with one leader plus 32 worker producers
- `PlanFlowDescriptor` reconstructs a `plan_flow::PlanOpen` when paired with
  `session_epoch`
- `ExecutionOptionsWire` carries query-scoped worker scan tuning captured by
  the backend at `StartExecution`; it keeps session-level GUC values visible to
  background workers without relying on their local GUC state
- `ScanChannelDescriptorWire` publishes one dedicated scan producer slot for one
  `(scan_id, producer_id)` up front in `StartExecution`
- `scan_channels` are encoded in strictly increasing
  `(scan_id, producer_id)` order
- each `scan_id` has exactly one leader producer and may have additional worker
  producers
- `BackendScanToWorker::ScanFailed.message` is bounded to `220` UTF-8 bytes so
  it always fits into the minimum dedicated inbound scan ring
- `ScanFlowDescriptorRef` reconstructs a `scan_flow::ScanOpen` when paired with
  `session_epoch` and `scan_id`
- `ScanFlowDescriptor::new` validates the declared producer set up front, so
  locally encoded `OpenScan` messages cannot be malformed
- message sizes are bounded by the chosen `control_transport` ring capacity

Typical backend-to-worker flow:

```rust,ignore
use runtime_protocol::{
    encode_backend_execution_to_worker_into, BackendExecutionToWorker,
    BackendLeaseSlotWire, ExecutionOptionsWire, PlanFlowDescriptor, ProducerRole,
    ScanChannelDescriptorWire, ScanChannelSet,
};

let scans = [ScanChannelDescriptorWire {
    scan_id: 11,
    producer_id: 0,
    role: ProducerRole::Leader,
    peer: BackendLeaseSlotWire::new(7, 3, 19),
}];

let msg = BackendExecutionToWorker::StartExecution {
    session_epoch: 7,
    plan: PlanFlowDescriptor {
        plan_id: 42,
        page_kind: 0x4152,
        page_flags: 0,
    },
    options: ExecutionOptionsWire::default(),
    scans: ScanChannelSet::new(&scans)?,
};

let mut buf = [0u8; 128];
let len = encode_backend_execution_to_worker_into(msg, &mut buf)?;
control_tx.send_frame(&buf[..len])?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Typical worker-side inbound decode on the primary execution slot:

```rust,ignore
use runtime_protocol::{
    classify_session, decode_backend_execution_to_worker, BackendExecutionToWorkerRef,
    SessionDisposition,
};

let msg = decode_backend_execution_to_worker(frame_bytes)?;
if classify_session(current_session_epoch, msg.session_epoch()) == SessionDisposition::Stale {
    return Ok(());
}

match msg {
    BackendExecutionToWorkerRef::StartExecution {
        session_epoch,
        plan,
        options,
        scans,
    } => {
        // Entries are already validated to be sorted by `(scan_id, producer_id)`.
        let scan_channels: Vec<_> = scans.iter().collect();
        let _ = (session_epoch, plan, options, scan_channels);
    }
    BackendExecutionToWorkerRef::CancelExecution { .. }
    | BackendExecutionToWorkerRef::FailExecution { .. } => {}
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

Typical worker-to-backend scan control encode on a dedicated scan slot:

```rust,ignore
use runtime_protocol::{
    encode_worker_scan_to_backend_into, ProducerDescriptorWire, ProducerRole, ScanFlowDescriptor,
    WorkerScanToBackend,
};

let producers = [ProducerDescriptorWire {
    producer_id: 0,
    role: ProducerRole::Leader,
}];
let scan = ScanFlowDescriptor::new(0x4152, 0, &producers)?;
let msg = WorkerScanToBackend::OpenScan {
    session_epoch: 7,
    scan_id: 11,
    scan,
};

let mut buf = [0u8; 128];
let len = encode_worker_scan_to_backend_into(msg, &mut buf)?;
scan_tx.send_frame(&buf[..len])?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Typical worker-to-backend execution control encode on the primary slot:

```rust,ignore
use runtime_protocol::{encode_worker_execution_to_backend_into, WorkerExecutionToBackend};

let msg = WorkerExecutionToBackend::CompleteExecution { session_epoch: 7 };
let mut buf = [0u8; 64];
let len = encode_worker_execution_to_backend_into(msg, &mut buf)?;
control_tx.send_frame(&buf[..len])?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Typical backend-to-worker scan terminal decode on a dedicated scan slot:

```rust,ignore
use runtime_protocol::{decode_backend_scan_to_worker, BackendScanToWorkerRef};

match decode_backend_scan_to_worker(frame_bytes)? {
    BackendScanToWorkerRef::ScanFinished {
        session_epoch,
        scan_id,
        producer_id,
    } => {
        let _ = (session_epoch, scan_id, producer_id);
    }
    BackendScanToWorkerRef::ScanFailed {
        session_epoch,
        scan_id,
        producer_id,
        message,
    } => {
        let _ = (session_epoch, scan_id, producer_id, message);
    }
}
# Ok::<(), Box<dyn std::error::Error>>(())
```
