# runtime_protocol

`runtime_protocol` is the typed control-plane message codec for the new
backend/worker runtime.

It intentionally sits:

- above `control_transport`, which only moves opaque framed bytes
- alongside `plan_flow` / `scan_flow`, whose page-stream descriptors it mirrors
- below any backend-side or worker-side execution FSM

The crate is intentionally narrow:

- one control message per `control_transport` frame
- versioned MsgPack envelope owned by this crate
- borrow-friendly decode for scan-producer descriptors
- no dependency on the legacy `protocol` crate
- no snapshot or execution registry ownership

The main contracts are:

- `session_epoch` is carried in every message and used by higher layers to drop
  stale traffic before it mutates fresh execution state
- `PlanFlowDescriptor` reconstructs a `plan_flow::PlanOpen` when paired with
  `session_epoch`
- `ScanFlowDescriptorRef` reconstructs a `scan_flow::ScanOpen` when paired with
  `session_epoch` and `scan_id`
- `ScanFlowDescriptor::new` validates the declared producer set up front, so
  locally encoded `OpenScan` messages cannot be malformed
- message sizes are bounded by the chosen `control_transport` ring capacity

Typical backend-to-worker flow:

```rust,ignore
use runtime_protocol::{
    encode_backend_to_worker_into, BackendToWorker, PlanFlowDescriptor,
};

let msg = BackendToWorker::StartExecution {
    session_epoch: 7,
    plan: PlanFlowDescriptor {
        plan_id: 42,
        page_kind: 0x4152,
        page_flags: 0,
    },
};

let mut buf = [0u8; 128];
let len = encode_backend_to_worker_into(msg, &mut buf)?;
control_tx.send_frame(&buf[..len])?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Typical worker-to-backend decode:

```rust,ignore
use runtime_protocol::{classify_session, decode_worker_to_backend, SessionDisposition};

let msg = decode_worker_to_backend(frame_bytes)?;
if classify_session(current_session_epoch, msg.session_epoch()) == SessionDisposition::Stale {
    return Ok(());
}

match msg {
    _ => {}
}
# Ok::<(), Box<dyn std::error::Error>>(())
```
