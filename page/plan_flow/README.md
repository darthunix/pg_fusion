# plan_flow

`plan_flow` is a sans-io single-sender flow for transferring one logical plan
through shared-memory pages.

It sits above:

- `issuance` for page/permit ownership
- `plan_codec` for streaming plan encode/decode

And intentionally stays below:

- transport I/O
- worker execution setup
- scan-page streaming

Normal success uses an issued close frame as EOF. Backend-side logical failures
are reported out-of-band through `accept_sender_error()`.

The intended sequence is:

1. Backend opens `BackendPlanRole` with a `LogicalPlan`.
2. The role emits one or more page frames and then one close frame.
3. Worker feeds those frames into `WorkerPlanRole`.
4. On close, the worker finalizes `PlanDecodeSession` and yields the decoded
   `LogicalPlan`.

Accepted plan pages are consumed synchronously on the worker and released
before `accept_frame()` returns. After a successful terminal close frame,
`BackendPlanRole::close()` exhausts the backend role; the embedded issued
sender stream cannot be reopened for a second transfer.

This flow is separate from `scan_flow` and is expected to complete before any
scan-page flow for the decoded plan starts.

## Usage

The backend owns one [`BackendPlanRole`] until it has emitted all plan pages
and one terminal close frame:

```rust,ignore
use issuance::IssuedTx;
use plan_flow::{BackendPlanRole, BackendPlanStep, PlanOpen};

let open = PlanOpen::new(flow, PAGE_KIND, PAGE_FLAGS);
let mut backend = BackendPlanRole::new(issued_tx);
backend.open(open, &logical_plan)?;

loop {
    match backend.step()? {
        BackendPlanStep::OutboundPage { outbound, .. } => {
            let frame = outbound.frame();
            transport_send(frame)?;
            outbound.mark_sent();
        }
        BackendPlanStep::CloseFrame { frame, .. } => {
            transport_send(frame)?;
            break;
        }
        BackendPlanStep::Blocked { .. } => retry_later(),
        BackendPlanStep::LogicalError { message, .. } => return Err(message.into()),
    }
}

backend.close()?;
```

At that point the backend role is exhausted and cannot be reused with another
`open()`.

The worker feeds received issued frames into one [`WorkerPlanRole`] and waits
for the final decoded [`LogicalPlan`]:

```rust,ignore
use plan_flow::{PlanOpen, WorkerPlanRole, WorkerStep};

let open = PlanOpen::new(flow, PAGE_KIND, PAGE_FLAGS);
let mut worker = WorkerPlanRole::new();
worker.open(open)?;

for frame in inbound_frames {
    match worker.accept_frame(flow, &issued_rx, &frame)? {
        WorkerStep::Idle => {}
        WorkerStep::Plan { plan, .. } => {
            install_plan(*plan)?;
            break;
        }
        WorkerStep::LogicalError { message, .. } => return Err(message.into()),
    }
}

worker.close()?;
```

If the backend fails before EOF, report that explicitly instead of sending a
close frame:

```rust,ignore
worker.accept_sender_error(flow, "backend aborted while building plan".into())?;
```
