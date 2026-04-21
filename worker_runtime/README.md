# worker_runtime

Worker-side runtime stack for the new execution path.

This crate intentionally owns a fresh worker-side FSM and does not depend on
`executor::server`, `executor::fsm`, `backend_service`, `slot_scan`, or `pgrx`.
It attaches to `control_transport`, consumes `runtime_protocol` control
messages, receives logical plans through `plan_flow`, lowers `PgScanNode`
locally, and imports scan pages through `page/import`.

Snapshot ownership is backend-only. The worker requests scans by `scan_id` and
does not expose an explain path.

Typical control-path usage:

```rust,ignore
use std::sync::Arc;

use worker_runtime::{
    DecodedInbound, TransportWorkerRuntime, WorkerRuntimeConfig, WorkerRuntimeCore,
};

let config = WorkerRuntimeConfig::default();
let scan_source = Arc::new(MyScanSource::new());
let mut core = WorkerRuntimeCore::new(config.clone(), scan_source);
let mut transport = TransportWorkerRuntime::attach(&region, &config)?;

let mut ready_cursor = 0;
while let Some(peer) = transport.next_ready_backend_lease(&mut ready_cursor) {
    transport.recv_peer_frames(peer, |bytes| {
        match WorkerRuntimeCore::decode_inbound(bytes)? {
            DecodedInbound::Control(message) => {
                let _step = core.accept_backend_control(peer, message)?;
            }
            DecodedInbound::IssuedFrame(frame) => {
                let _step = core.accept_issued_plan_frame(peer, &issued_rx, &frame)?;
            }
        }
        Ok(())
    })?;
}
# Ok::<(), worker_runtime::WorkerRuntimeError>(())
```

`decode_inbound()` expects one already framed `control_transport` payload per
call. Malformed non-control frames fail immediately for that frame; they are not
buffered and cannot contaminate the next slot payload.

Typical scan-open control encoding without heap allocation:

```rust,ignore
use worker_runtime::{ScanFlowDriver, ScanFlowOpen};

let (driver, open_scan) = ScanFlowDriver::open(open, issued_rx)?;
transport.send_peer_encoded(peer, |scratch| open_scan.encode_into(scratch))?;
# let _ = driver;
# Ok::<(), worker_runtime::WorkerRuntimeError>(())
```
