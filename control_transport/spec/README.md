# control_transport/spec

This directory holds reference specifications for `control_transport`.

The split is intentional:

- `transport/` models half-open duplex delivery semantics for one slot.
  The transport model is a bounded FIFO-by-bytes abstraction, not an event
  history.
- `lifecycle/` models leasing, invalidation, stale ownership, and slot reuse.
- `bridge/` records the cross-model contract around incarnation identity and
  safe reuse.
- `micro/` models the implementation-level intermediate protocol states that
  exist inside multi-step Rust operations such as claim/publish/rollback and
  finalize/reuse.

These specs are for reasoning, TLC checking, and guiding future `loom` tests.
They are not code generation inputs, and they do not attempt to model atomics,
ring indices, or `SIGUSR1` ordering.

The checked models are the hand-written safety specs in each file:

- `Init`
- `Next`
- `Spec == Init /\ [][Next]_vars`

The PlusCal blocks are there for readability and translation experiments in
Toolbox. They intentionally do not carry fairness in the canonical checked
specs. The micro-spec layer also defines optional fair `FairSpec` entrypoints
for bounded liveness checks in the `*.live.cfg` runs; those are supplemental,
not the default safety checks.

## Prerequisites

- a Java runtime
- `tla2tools.jar` somewhere on disk for CLI use
- optionally TLA+ Toolbox for interactive browsing and PlusCal translation

For CLI examples below, assume:

```sh
TLA_JAR=/path/to/tla2tools.jar
```

## Layout

- `transport/SingleSlotTransport.tla`
- `transport/SingleSlotTransport.cfg`
- `transport/README.md`
- `lifecycle/SingleSlotLifecycle.tla`
- `lifecycle/SingleSlotLifecycle.cfg`
- `lifecycle/README.md`
- `bridge/SingleSlotBridge.tla`
- `bridge/SingleSlotBridge.cfg`
- `bridge/README.md`
- `micro/WorkerClaimPendingReuse.tla`
- `micro/WorkerClaimPendingReuse.cfg`
- `micro/WorkerClaimPendingReuse.deadlock.cfg`
- `micro/WorkerClaimPendingReuse.live.cfg`
- `micro/WorkerClaimPendingReuse.deep.cfg`
- `micro/BackendAcquirePublish.tla`
- `micro/BackendAcquirePublish.cfg`
- `micro/BackendAcquirePublish.deadlock.cfg`
- `micro/BackendAcquirePublish.deep.cfg`
- `micro/FinalizeReuse.tla`
- `micro/FinalizeReuse.cfg`
- `micro/FinalizeReuse.deadlock.cfg`
- `micro/FinalizeReuse.live.cfg`
- `micro/FinalizeReuse.deep.cfg`
- `micro/RegionLifecycleGate.tla`
- `micro/RegionLifecycleGate.cfg`
- `micro/RegionLifecycleGate.deadlock.cfg`
- `micro/RegionLifecycleGate.deep.cfg`
- `micro/SlotOwnershipProtocol.tla`
- `micro/SlotOwnershipProtocol.cfg`
- `micro/SlotOwnershipProtocol.deadlock.cfg`
- `micro/SlotOwnershipProtocol.live.cfg`
- `micro/SlotOwnershipProtocol.deep.cfg`
- `micro/ReinitPreservesOldSlots.tla`
- `micro/ReinitPreservesOldSlots.cfg`
- `micro/ReinitPreservesOldSlots.deadlock.cfg`
- `micro/ReinitPreservesOldSlots.live.cfg`
- `micro/ReinitPreservesOldSlots.deep.cfg`
- `micro/README.md`
- `IMPLEMENTATION_REFINEMENT.md`

## What "Compile" Means Here

There is no separate build step like Rust compilation.

- `SANY` parses and semantically checks a `.tla` module.
- PlusCal translation is a source-to-source step from the comment-block
  algorithm into TLA+, usually done in TLA+ Toolbox.
- TLC runs the hand-written `Spec` operator from the module together with a
  `.cfg` file that selects constants, invariants, and optional bounds.

## CLI Workflow

Use `SANY` when you want a fast parse/semantic check:

```sh
java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/transport/SingleSlotTransport.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/lifecycle/SingleSlotLifecycle.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/bridge/SingleSlotBridge.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/micro/WorkerClaimPendingReuse.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/micro/BackendAcquirePublish.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/micro/FinalizeReuse.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/micro/RegionLifecycleGate.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/micro/SlotOwnershipProtocol.tla

java -cp "$TLA_JAR" tla2sany.SANY \
  control_transport/spec/micro/ReinitPreservesOldSlots.tla
```

Use TLC with the committed configs for reproducible runs:

```sh
java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/transport/SingleSlotTransport.cfg \
  control_transport/spec/transport/SingleSlotTransport.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/lifecycle/SingleSlotLifecycle.cfg \
  control_transport/spec/lifecycle/SingleSlotLifecycle.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/bridge/SingleSlotBridge.cfg \
  control_transport/spec/bridge/SingleSlotBridge.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/WorkerClaimPendingReuse.cfg \
  control_transport/spec/micro/WorkerClaimPendingReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/BackendAcquirePublish.cfg \
  control_transport/spec/micro/BackendAcquirePublish.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/FinalizeReuse.cfg \
  control_transport/spec/micro/FinalizeReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/RegionLifecycleGate.cfg \
  control_transport/spec/micro/RegionLifecycleGate.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/SlotOwnershipProtocol.cfg \
  control_transport/spec/micro/SlotOwnershipProtocol.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/ReinitPreservesOldSlots.cfg \
  control_transport/spec/micro/ReinitPreservesOldSlots.tla
```

For the micro-spec layer, there are also:

- `*.deadlock.cfg` for deadlock-enabled TLC runs without the CLI `-deadlock`
  flag
- `*.live.cfg` for bounded fair progress checks
- `*.deep.cfg` for larger bounded exploration

Notes:

- `transport/SingleSlotTransport.cfg` is a full smoke-check config.
- `lifecycle/SingleSlotLifecycle.cfg` includes a TLC `CONSTRAINT` on the
  `TlcSmokeBound` operator, which bounds `regionGen` for a finite smoke run.
  Without that bound, the model keeps generating new generations and full BFS
  does not terminate.
- `bridge/SingleSlotBridge.cfg` also uses `TlcSmokeBound`; it bounds both
  `regionGen` and the bridge-local `nextLeaseEpoch`, so TLC can explore reuse
  scenarios without unbounded reincarnation.
- The `micro/*.cfg` files also use small `TlcSmokeBound` constraints. These
  models are protocol-level and intentionally bounded to a few reincarnation
  cycles so TLC can exhaustively explore the relevant interleavings.
- The `micro/*.deep.cfg` files use `TlcDeepBound` for larger bounded runs.
- The `micro/*.live.cfg` files use fair specs for bounded liveness checks.

## TLA+ Toolbox Workflow

1. Open the `.tla` module in TLA+ Toolbox.
2. If you want to inspect the PlusCal algorithm, translate the comment-block
   algorithm in Toolbox.
3. Keep using the hand-written `Init`, `Next`, and `Spec` operators in the
   file as the canonical model-checking entrypoints.
4. Mirror the committed `.cfg` settings in the Toolbox model, rather than
   inventing ad-hoc constants/invariants by hand.

## Why Commit `.cfg` Files?

Configs are not required for every task:

- `SANY` does not need them.
- PlusCal translation does not need them.
- quick experiments in Toolbox can be done without them.

They are still worth committing because they make TLC runs reproducible:

- they pin the `Spec` operator
- they pin transport constants
- they pin which invariants are part of the default smoke run
- they document bounded smoke-check assumptions such as the lifecycle
  generation cap and bridge lease-epoch cap

## Modeling boundaries

- Transport answers: what delivery and half-open behavior is allowed within one
  stable slot incarnation?
- Lifecycle answers: when is one slot incarnation alive, stale, finalized, and
  safe to reuse?
- Bridge answers: what extra identity and visibility rules must hold at the
  seam so reuse cannot leak old transport state into a new incarnation?
- Micro-specs answer: what intermediate protocol states exist inside one
  multi-step Rust operation, and how do we prevent stale steps from mutating a
  fresh incarnation before a later recheck notices the mismatch?
  They also cover packed global admission gates such as
  `region_generation + worker_state`.

## Out of scope

- `ready_flag` behavior
- `SIGUSR1` wakeup ordering
- `head` / `tail` / wraparound details
- memory-ordering rules
- direct proof that Rust code refines these specs

Those belong in Rust-side reference tests and `loom` harnesses.

## Rust-side `loom` harnesses

`control_transport` also carries reduced-core `loom` tests for the atomic
seams that the TLA+ specs intentionally leave out:

- `control_transport/tests/loom_ring_ready.rs`
- `control_transport/tests/loom_slot_incarnation.rs`
- `control_transport/tests/loom_worker_claim.rs`
- `control_transport/tests/loom_region_gate.rs`

Run them explicitly with a small preemption bound:

```sh
LOOM_MAX_PREEMPTIONS=2 cargo test -p control_transport --test loom_ring_ready
LOOM_MAX_PREEMPTIONS=2 cargo test -p control_transport --test loom_slot_incarnation
LOOM_MAX_PREEMPTIONS=2 cargo test -p control_transport --test loom_worker_claim
LOOM_MAX_PREEMPTIONS=2 cargo test -p control_transport --test loom_region_gate
```

If `LOOM_MAX_PREEMPTIONS` is not set, the shared loom test helper defaults to
`2` so `cargo test -p control_transport` stays usable as a normal crate-level
check.

These harnesses model only reduced-core atomics and interleavings. They do not
run the full shared-memory region, ring byte layout, or `SIGUSR1` delivery.

## Refinement Notes

`IMPLEMENTATION_REFINEMENT.md` documents how multi-step Rust operations are
reviewed against the atomic TLA+ transitions, including operation classes,
identity guards, rollback rules, and the reduced-core `loom` strategy.

`micro/README.md` documents the lower-level protocol models that sit between
the high-level specs and the reduced-core `loom` harnesses.
