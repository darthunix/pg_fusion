# worker_runtime

Worker-side runtime stack for the new execution path.

This crate intentionally owns a fresh worker-side FSM and does not depend on
the retired raw-heap executor stack, `backend_service`, `slot_scan`, or `pgrx`.
It attaches to `control_transport`, consumes `runtime_protocol` control
messages, receives logical plans through `plan_flow`, lowers `PgScanNode`
locally, and imports scan pages through `page/import`.

Snapshot ownership is backend-only. The worker requests scans by `scan_id` and
the dedicated scan producer peers published in `StartExecution`; it does not
expose an explain path.

For a transport-backed scan data-plane, use `TransportScanBatchSource` together
with a `ScanIngressProvider`. That source claims the dedicated scan peer for
the lifetime of each scan producer, sends `OpenScan` / `CancelScan` on each
producer peer, and consumes both issued page headers and backend-emitted scan
terminal messages from those slots. `ScanIngressProvider` returns an `IssuedRx`
per `(session_epoch, scan_id, producer_id)` because each producer owns an
independent ordered transfer stream that starts at `transfer_id = 1`.
`OpenScanRequest.tuning` controls the bounded worker scan batch channel and the
idle poll sleep used by the scan thread; production values come from
`runtime_protocol::ExecutionOptionsWire` captured at `StartExecution`.
The same options carry the runtime-filter enable flag. When it is set,
physical planning can allocate shared runtime-filter slots for eligible integer
inner hash joins and wrap the build side so backend scan producers can filter
probe rows before Arrow encoding.
Dedicated scan slots must provide at least:

- `256` bytes raw backend-to-worker ring capacity
- `256` bytes raw worker-to-backend ring capacity

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
for producer in &request.producers {
    transport.send_peer_encoded(producer.peer, |scratch| open_scan.encode_into(scratch))?;
}
# let _ = driver;
# Ok::<(), worker_runtime::WorkerRuntimeError>(())
```
