# scan_flow

`scan_flow` is a sans-io core for the page-backed table-scan data path between
a PostgreSQL-side backend producer and a worker-side consumer.

It is intentionally narrow:

- one logical scan at a time per runtime
- multiple producers can fan in to one logical scan
- one `IssuedTx` / `IssuedRx` stream per producer
- no direct dependency on `pgrx`, `DataFusion`, `import`, or workspace `protocol`
- only `page/transfer` and `page/issuance` for data movement and backpressure

## Roles

- `BackendScanCoordinator`: logical terminal aggregation across declared producers
- `BackendProducerRole<S>`: one producer runtime over a source callback
- `WorkerScanRole`: worker-side fan-in runtime returning `IssuedReceivedPage`

## Basic shape

```rust
use issuance::{IssuedRx, IssuedTx};
use scan_flow::{
    BackendPageSource, BackendProducerRole, BackendScanCoordinator, FlowId, ProducerDescriptor,
    ProducerRoleKind, ScanOpen, WorkerScanRole,
};

let scan = ScanOpen::new(
    FlowId {
        session_epoch: 1,
        scan_id: 42,
    },
    0x4152,
    0,
    vec![
        ProducerDescriptor::leader(0),
        ProducerDescriptor::worker(1),
    ],
)?;

let mut coordinator = BackendScanCoordinator::new();
coordinator.open(scan.clone())?;

let mut worker = WorkerScanRole::new();
worker.open(scan.clone())?;

let producer_tx: IssuedTx = todo!();
let producer_rx: IssuedRx = todo!();
let source: impl BackendPageSource = todo!();

let mut producer = BackendProducerRole::new(producer_tx);
producer.open(&scan, 0, source)?;

// External wiring drives:
// 1. producer.step()
// 2. let frame = outbound.frame(); encode/write it through the carrier
// 3. only after successful carrier delivery: outbound.mark_sent()
// 4. worker.accept_page_frame(..., &producer_rx, &frame)
// 4. explicit per-producer EOF/error signals
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Contract

- `ScanOpen` declares the full producer set up front.
- `BackendPageSource` returns the exact `payload_len` for each produced page;
  the worker only exposes that prefix to consumers.
- A logical EOF is reached only after EOF from every declared producer.
- The first producer error fails the whole logical scan immediately.
- `WorkerScanRole` returns opaque `IssuedReceivedPage` owners; the outer sink is
  responsible for dropping or copying them quickly.
- `transfer::Close` is not normal scan EOF for this crate.
