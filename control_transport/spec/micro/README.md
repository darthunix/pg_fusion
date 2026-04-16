# `control_transport/spec/micro`

This directory holds implementation-level micro-protocol specs for
`control_transport`.

These are not replacements for the high-level `transport/`, `lifecycle/`, and
`bridge/` models. They sit one layer below them and model the intermediate
states that either exist in the Rust implementation today or are required as
the authoritative refinement target:

- tentative reservations
- publish/recheck/rollback flows
- finalize/reuse boundaries

Unlike the high-level specs, these micro-specs are written directly in TLA+.
That is intentional: the protocol phases are easier to read as explicit TLA+
actions than as translated PlusCal for these small state machines.

## Modules

- `WorkerClaimPendingReuse.tla`
  - Rust seam: `claim_worker_slot`, pending reservation, rollback, same-generation reuse
  - Loom mapping: `tests/loom_worker_claim.rs`
  - Regression mapping: worker-claim pending/reuse tests in `src/tests.rs`
  - Extra configs:
    - `WorkerClaimPendingReuse.deadlock.cfg` for deadlock-enabled TLC runs
    - `WorkerClaimPendingReuse.live.cfg` for fair progress checks
    - `WorkerClaimPendingReuse.deep.cfg` for larger bounded exploration

- `BackendAcquirePublish.tla`
  - Rust seam: backend acquire, metadata publish, generation/offline recheck, rollback
  - Loom mapping: `tests/loom_slot_incarnation.rs`
  - Regression mapping: backend-acquire rollback tests in `src/tests.rs`
  - Extra configs:
    - `BackendAcquirePublish.deadlock.cfg` for deadlock-enabled TLC runs
    - `BackendAcquirePublish.deep.cfg` for larger bounded exploration

- `FinalizeReuse.tla`
  - Rust seam: backend/worker release, reap, finalize, freelist return, same-generation reacquire
  - Loom mapping: `tests/loom_slot_incarnation.rs`
  - Regression mapping: stale-drop / same-generation reuse tests in `src/tests.rs`
  - Extra configs:
    - `FinalizeReuse.deadlock.cfg` for deadlock-enabled TLC runs
    - `FinalizeReuse.live.cfg` for fair progress checks
    - `FinalizeReuse.deep.cfg` for larger bounded exploration

- `RegionLifecycleGate.tla`
  - Rust seam: packed region lifecycle gate for backend acquire / worker rechecks
  - Loom mapping: `tests/loom_region_gate.rs`
  - Regression mapping: backend admission and generation transition tests in `src/tests.rs`
  - Extra configs:
    - `RegionLifecycleGate.deadlock.cfg` for deadlock-enabled TLC runs
    - `RegionLifecycleGate.deep.cfg` for larger bounded exploration

- `SlotOwnershipProtocol.tla`
  - Rust seam: authoritative token-handoff protocol for acquire, finalize, free publication, and reinit rebuild
  - Loom mapping: follow-up adversarial ownership harness; the current `tests/loom_reinit_retention.rs` still covers the older state projection
  - Regression mapping:
    - `reinit_recovers_slot_popped_from_freelist_before_lease_publish`
    - `reinit_recovers_slot_finalized_before_freelist_push`
    - `reinit_in_place_retains_old_backend_owned_slot_until_release`
    - `reinit_keeps_already_free_slots_reusable`
  - Scope note: this is the authoritative slot/freelist token-handoff model; `BackendAcquirePublish`, `FinalizeReuse`, and `ReinitPreservesOldSlots` are narrower projections
  - Extra configs:
    - `SlotOwnershipProtocol.deadlock.cfg` for deadlock-enabled TLC runs
    - `SlotOwnershipProtocol.live.cfg` for fair progress checks
    - `SlotOwnershipProtocol.deep.cfg` for larger bounded exploration

- `ReinitPreservesOldSlots.tla`
  - Rust seam: existing `reinit_in_place`, with wait-old-slots retention as a projection of `SlotOwnershipProtocol`
  - Loom mapping: `tests/loom_reinit_retention.rs`
  - Regression mapping:
    - `reinit_in_place_retains_old_backend_owned_slot_until_release`
    - `reinit_in_place_makes_old_worker_handle_stale_but_does_not_free_slot`
    - `stale_worker_drop_after_reinit_is_harmless_with_retention`
    - `reinit_keeps_already_free_slots_reusable`
    - `reinit_plus_activate_reaps_dead_old_backend_owner`
    - `reinit_recovers_slot_popped_from_freelist_before_lease_publish`
    - `reinit_recovers_slot_finalized_before_freelist_push`
  - Scope note: stale worker-drop is modeled out-of-band via local-registry reset;
    this spec covers shared-memory retention/reuse only and intentionally
    abstracts away the popped-token / freelist-epoch handoff modeled in
    `SlotOwnershipProtocol`
  - Extra configs:
    - `ReinitPreservesOldSlots.deadlock.cfg` for deadlock-enabled TLC runs
    - `ReinitPreservesOldSlots.live.cfg` for fair progress checks
    - `ReinitPreservesOldSlots.deep.cfg` for larger bounded exploration

## Commands

Assuming:

```sh
TLA_JAR=/tmp/tla2tools.jar
```

Parse / semantic check:

```sh
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

TLC smoke runs:

```sh
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

Deadlock-enabled runs use the `*.deadlock.cfg` files and intentionally omit
the CLI `-deadlock` flag:

```sh
java -cp "$TLA_JAR" tlc2.TLC -cleanup -workers 1 \
  -config control_transport/spec/micro/WorkerClaimPendingReuse.deadlock.cfg \
  control_transport/spec/micro/WorkerClaimPendingReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -cleanup -workers 1 \
  -config control_transport/spec/micro/BackendAcquirePublish.deadlock.cfg \
  control_transport/spec/micro/BackendAcquirePublish.tla

java -cp "$TLA_JAR" tlc2.TLC -cleanup -workers 1 \
  -config control_transport/spec/micro/FinalizeReuse.deadlock.cfg \
  control_transport/spec/micro/FinalizeReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -cleanup -workers 1 \
  -config control_transport/spec/micro/RegionLifecycleGate.deadlock.cfg \
  control_transport/spec/micro/RegionLifecycleGate.tla

java -cp "$TLA_JAR" tlc2.TLC -cleanup -workers 1 \
  -config control_transport/spec/micro/SlotOwnershipProtocol.deadlock.cfg \
  control_transport/spec/micro/SlotOwnershipProtocol.tla

java -cp "$TLA_JAR" tlc2.TLC -cleanup -workers 1 \
  -config control_transport/spec/micro/ReinitPreservesOldSlots.deadlock.cfg \
  control_transport/spec/micro/ReinitPreservesOldSlots.tla
```

Fair progress checks use the `*.live.cfg` files:

```sh
java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/WorkerClaimPendingReuse.live.cfg \
  control_transport/spec/micro/WorkerClaimPendingReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/FinalizeReuse.live.cfg \
  control_transport/spec/micro/FinalizeReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/SlotOwnershipProtocol.live.cfg \
  control_transport/spec/micro/SlotOwnershipProtocol.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/ReinitPreservesOldSlots.live.cfg \
  control_transport/spec/micro/ReinitPreservesOldSlots.tla
```

Deeper bounded runs use the `*.deep.cfg` files:

```sh
java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/WorkerClaimPendingReuse.deep.cfg \
  control_transport/spec/micro/WorkerClaimPendingReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/BackendAcquirePublish.deep.cfg \
  control_transport/spec/micro/BackendAcquirePublish.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/FinalizeReuse.deep.cfg \
  control_transport/spec/micro/FinalizeReuse.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/RegionLifecycleGate.deep.cfg \
  control_transport/spec/micro/RegionLifecycleGate.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/SlotOwnershipProtocol.deep.cfg \
  control_transport/spec/micro/SlotOwnershipProtocol.tla

java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config control_transport/spec/micro/ReinitPreservesOldSlots.deep.cfg \
  control_transport/spec/micro/ReinitPreservesOldSlots.tla
```

The default `*.cfg` files remain the small smoke runs. They all include a
bounded `TlcSmokeBound` constraint so TLC can terminate after exploring a few
reincarnation cycles. The `*.deep.cfg` variants switch to `TlcDeepBound` for
nightly-style exploration. The `*.live.cfg` variants use fair specs to check
progress properties; they are supplemental to the canonical safety `Spec`.
Because the live configs stay bounded, TLC will warn that temporal checks with
constraints are only bounded smoke checks. That warning is expected here.
