# SingleSlotTransport

This model covers one duplex transport slot with abstract half-open endpoint
states and a bounded FIFO per direction. The bounded state matches the real
transport contract better than an unbounded send/receive history: each queued
payload consumes `FrameBytes(msg)` bytes, and sends are
allowed only while the direction-local buffered byte total stays within
`CapBytes`.

The model intentionally excludes freelist reuse, `region_generation`,
`slot_generation`, and memory identity. Those belong to the lifecycle spec and
the bridge contract.

## Variable mapping

- `backendEp` -> backend-side endpoint liveness/attachment
- `workerEp` -> worker-side endpoint liveness/attachment
- `Directions` -> abstract transport directions `B2W` and `W2B`
- `MsgSet == {"m1", "m2"}` -> two symbolic payload kinds used for TLC smoke
  runs
- `queues[dir]` -> current buffered frame payloads for one direction
- `CapBytes` -> abstract per-direction capacity checked before publish
- `FramePrefixBytes` -> abstract frame prefix size, corresponding to
  `FRAME_PREFIX_LEN`
- `M1Bytes` / `M2Bytes` -> abstract payload lengths for the two symbolic
  payload kinds
- `QueueBytes(queues[dir])` -> current buffered bytes for one direction
- `lastSendOutcome[dir]` -> most recent observable send outcome for one
  direction

## TLC constants

- `M1Bytes = 1`
- `M2Bytes = 2`
- `FramePrefixBytes = 1`
- `CapBytes = 5`

## TLC run

- config file: `SingleSlotTransport.cfg`
- specification operator: `Spec`
- constants:
  - `M1Bytes = 1`
  - `M2Bytes = 2`
  - `FramePrefixBytes = 1`
  - `CapBytes = 5`
- invariants:
  - `TypeOK`
  - `QueueCapacityOK`

## Invariants to check

- `TypeOK`
- `QueueCapacityOK`

## What this spec proves well

- half-open transport is allowed
- `Dead` is terminal in this abstract model; there is no reattach-from-dead
  transition
- queue capacity is bounded by bytes, not by message count
- the message alphabet is finite because the smoke model uses two symbolic
  payload kinds with distinct sizes
- publish is not rolled back by `PeerMissing`
- current buffered payloads are finite-state and directly inspectable in TLC
- sender and receiver liveness are separated per direction through endpoint
  state and `NotifyOutcome`

## What this spec does not try to prove

- atomics or memory ordering
- `ready_flag` correctness
- ring wraparound
- exact `available_bytes(...)` arithmetic
- slot reuse or stale handle invalidation
- `NotifyFailed` as a distinct outcome

Those belong in `loom` and in the lifecycle/bridge artifacts.

Constant validity is expressed with module-level `ASSUME` statements rather
than TLC invariants, so the default smoke run focuses on reachable-state
properties.

With the recommended constants TLC should finish a full exhaustive run quickly.
This model is intended to stay finite-state and small enough for regular smoke
checks.
