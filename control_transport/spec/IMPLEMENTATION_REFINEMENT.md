# `control_transport` Implementation Refinement

This document bridges the gap between the atomic TLA+ operations in `spec/`
and the multi-step Rust implementations in `src/`.

The TLA+ models stay intentionally high-level:

- `transport/` defines delivery and half-open semantics
- `lifecycle/` defines leasing, invalidation, and reuse
- `bridge/` defines incarnation identity and stale-handle visibility

The Rust code must additionally ensure that multi-step atomic protocols do not
let stale paths mutate a fresh incarnation.

There are two authoritative metadata domains:

- `slot_meta`
  packs slot-local `lease_epoch`, lease state, and owner bits
- `region_meta`
  packs `region_generation` and `worker_state`

Runtime code must not reconstruct either logical state machine from torn reads
of multiple independent atomics.

## Operation Catalog

These runtime operations correspond to atomic spec transitions but are
multi-step in Rust.

### Observation

- `validate_lease`
- `validate_worker_slot_access`
- `WorkerTransport::ready_slots`
- backend/worker generation admission gates
- `probe_pid_alive`
- `signal_pid_usr1` / `signal_peer`

### Tentative mutation with rollback

- `BackendSlotLease::acquire`
- `TransportRegion::claim_worker_slot`

### Guarded mutation

- `BackendSlotLease::release`
- `TransportRegion::release_worker_slot`
- `TransportRegion::reap_dead_backend_owner`

### Reclamation / reuse

- `TransportRegion::try_finalize_slot`
- `TransportRegion::reap_current_generation_dead_backend_slots`
- `TransportRegion::sweep_old_generation_slots`

### Publish / consume with hint

- `FramedRing::send_frame`
- `FramedRing::recv_frame_into`
- `FramedRing::update_ready_after_consume`

### Mirror-state mutation

- `insert_local_worker_owner`
- `remove_local_worker_owner`
- `take_local_worker_owners`

## Operation Classes

Use these classes when designing or reviewing low-level changes.

- `Observation`
  Reads shared state and returns an outcome. Stale paths must not mutate shared
  state.
- `Guarded mutation`
  Performs one decisive mutation under a current identity guard.
- `Tentative mutation with rollback`
  Makes a temporary reservation, re-checks state, and then commits or rolls
  back. Rollback is a mutation, not harmless cleanup.
- `Reclamation / reuse`
  Finalize, clear, or freelist-return logic. It must be exact-incarnation only.
- `Publish / consume with hint`
  The ring state is the source of truth. Ready flags and signals are hints.

## Required Per-Operation Notes

Every nontrivial low-level mutation should be explainable in these terms:

- `Identity guard`
  Which incarnation protects the mutation. For slot metadata this is
  `(slot_generation, lease_epoch)`.
- `Linearization point`
  The step where the abstract transition becomes visible.
- `Post-LP recheck`
  What is re-read after a tentative mutation.
- `Rollback rule`
  What exact condition allows rollback to mutate shared state.
- `Forbidden cross-incarnation effect`
  What the operation must never do to a fresh lease.

If any one of these items cannot be written down clearly, the implementation is
too implicit and should be restructured.

## Standardized Internal Shapes

### Slot metadata operations

Slot lifecycle code is standardized around:

- `RegionSnapshot`
  - decoded `region_meta`
    - current `region_generation`
    - current `worker_state`
- `SlotSnapshot`
  - `slot_generation`
  - decoded `slot_meta`
    - `lease_state`
    - `lease_epoch`
    - `owner_mask`
  - `backend_pid`
- `OwnerMutationResult`
  - whether a guarded owner-bit mutation changed shared state
  - what owner bits remained afterward
- `WorkerOwnerReservation`
  - the process-local worker registry reservation used by worker claim
  - dropped reservations auto-clean up unless explicitly kept

All guarded owner-bit updates flow through:

- `clear_owner_bits_if_matching(...)`
- `finalize_if_ownerless(...)`

This is the canonical shape for backend release, worker release, dead-backend
reap, worker-claim rollback, and old-generation worker sweep.

The authoritative mutation domain is one packed `slot_meta` word:

- `lease_epoch`
- `lease_state`
- owner bits `B | W | P`

The authoritative slot ownership/publication model is now a token-handoff
protocol:

- slot lifecycle
  - `Free`
  - `Reserved`
  - `Leased`
- availability token
  - `InFreelist`
  - `Popped`
  - `None`
  - `PushClaimed`
  - `Pushed`
- `freelistEpoch`

`BackendSlotLease::acquire`, `try_finalize_slot`, `publish_free_slot`, and
`reinit_in_place()` are all refinements of that one protocol. The
`SlotOwnershipProtocol.tla` micro-spec is the source of truth for the slot /
freelist handoff; `BackendAcquirePublish`, `FinalizeReuse`, and
`ReinitPreservesOldSlots` are projections over that model.

For free publication, the runtime refinement must preserve this exact order:

1. `FREE_PENDING -> FREE_PUSH_CLAIMED`
2. `release_slot(slot_id)` publishes the availability token into the freelist
3. `FREE_PUSH_CLAIMED -> FREE_PUSHED`
4. `FREE_PUSHED -> FREE_PUBLISHED`

`release_slot(slot_id)` is the publication linearization point. `FREE_PUSHED`
must never be observable before the freelist push has actually happened. This
is why `reinit` may treat `FREE_PUSH_CLAIMED` as transitional but
`FREE_PUSHED` as stable/free-like.

The current Rust and reduced-core `loom` models still lag the authoritative
spec in two seams:

- `freelist.allocate() -> popped token -> reserve claim`
- `reinit` fixed-point completion after rebuild-preparation

Follow-up runtime changes in those areas must refine the token-handoff model,
not the older state-only projection.

Lifecycle mutations do not open-code owner/state changes across separate
atomics. They must CAS the packed `slot_meta` word and treat `slot_generation`
as an external invalidation/classification field.

### Region lifecycle operations

Worker-generation lifecycle code is standardized around one packed `region_meta`
word:

- `region_generation`
- `worker_state`

Admission decisions such as `BackendSlotLease::acquire`, worker attach, and
`ready_slots()` rechecks must read and interpret one `region_meta` snapshot.
They must not rebuild an "online generation" predicate from separate
`region_generation` and `worker_state` loads.

### Ring operations

Ring I/O is standardized around:

- `RingSnapshot`
  - `head`, `tail`, `capacity`
- `PublishPlan`
  - computed circular write layout for one send operation
- `ConsumePlan`
  - computed circular read layout for one receive operation

`send_frame` follows:

1. load checked snapshot
2. build publish plan
3. write wrapped frame prefix and wrapped payload bytes
4. publish `tail`
5. set `ready_flag`
6. attempt signal

`recv_frame_into` follows:

1. load checked snapshot
2. read wrapped frame prefix
3. build consume plan
4. copy out wrapped payload bytes and publish `head`
5. maintain `ready_flag`

Ring frames may wrap across the buffer boundary. Empty rings at nonzero offsets
remain valid and fully reusable; correctness no longer depends on returning to a
canonical `(head = 0, tail = 0)` empty state after drain or slot reuse.

## Current Design Rules

- Any shared metadata mutation must belong to exactly one incarnation.
- Any destructive mutation must be guarded by the same identity that guards
  reuse.
- Lifecycle ownership/state mutations must linearize through `slot_meta`, not
  through separate owner/state/epoch stores.
- Region lifecycle admission must linearize through `region_meta`, not through
  separate generation/state loads.
- Stale paths may return `Released`, `StaleGeneration`, or `StaleLeaseEpoch`,
  but they may not mutate a fresh incarnation.
- Rollback is a first-class mutation and must obey the same guard rules as
  commit.
- If an operation cannot have one clean linearization point, model it as a
  two-phase protocol and test it as such.
- Open-coded destructive `fetch_and` / `fetch_or` on slot metadata are not
  allowed in long procedures; use standardized guarded helpers instead.
- Process-local ownership mirrors must carry the full `LeaseIncarnation`, not
  just `slot_id` or `generation`.
- The process-local worker registry is valid only for the exact tuple
  `(pid, region_key, slot_count)`.
- A PID change invalidates the entire local registry and must rebuild it from
  empty state before the next attach.
- Same-layout `reinit_in_place()` resets the local registry from empty state
  for the current `(pid, region_key, slot_count)` tuple so late stale worker
  drops cannot observe phantom local owners.
- The worker-owner registry is sticky per PID lifetime. A process may attach
  multiple handles to one region, but attaching a different region in the same
  PID lifetime is unsupported and returns `RegionAlreadyAttached`.
- Reinitializing the same region address with a different layout is rejected;
  `reinit_in_place()` is same-layout only.
- Same-layout `reinit_in_place()` must preserve monotonic incarnation identity:
  it bumps `region_generation` forward and never rewinds `next_lease_epoch`, so
  stale backend handles cannot collide with fresh leases after reset.
- Same-layout `reinit_in_place()` is a spec-atomic but runtime multi-step
  token protocol:
  - publish newer `REINITING` `region_meta`
  - quiesce or adopt all live transitional ownership states
    - popped token
    - reserved slot
    - push-claimed token
  - rotate to a fresh `freelistEpoch`
  - republish stable free slots into the current freelist epoch
  - only then leave `REINITING`
- `reinit_in_place()` must not clear or republish a slot before it has claimed
  that slot through the ownership protocol. Free publication is exact-once and
  is never implemented by rewinding a live push-claimed token.
- `reinit_in_place()` must not republish a backend-owned old slot to the
  freelist. Retained old leases stay out of the freelist until exact old
  release/finalize or later dead-backend reap.
- Free-slot publication is itself multi-step:
  `Free(None) -> Free(PushClaimed) -> Free(Pushed) -> Free(InFreelist)`.
  `Free(None)` means logically free but carrying no current-epoch availability
  token.
- The acquire side is likewise multi-step:
  `Free(InFreelist) -> Free(Popped) -> Reserved(None) -> Leased(None)`.
- Same-layout `reinit_in_place()` must not finish while any slot is still in a
  transitional token state, and it must not leave a free slot without a
  current-epoch published token.
- This process-local registry is intentionally outside the TLA+ shared-memory
  state; it is covered by deterministic Rust tests rather than by the TLA+
  models.

`claim_worker_slot` currently follows a two-phase reservation protocol on
`slot_meta`:

1. exact CAS `Leased(epoch, B) -> Leased(epoch, B|P)`
2. re-check shared state and backend liveness
3. exact CAS `Leased(epoch, B|P) -> Leased(epoch, B|W)`

Same-generation reuse is blocked while the pending bit is present. This avoids
stale rollback mutating a fresh incarnation.

`BackendSlotLease::acquire` must be treated as a token handoff:

1. pop a current-epoch freelist token
2. claim the popped token for one acquire attempt
3. publish `slot_generation`, backend pid, and then one authoritative leased
   incarnation
4. re-check packed `region_meta`
5. on stale/offline failure, return through the normal free-publication path

The gate before and after publish uses one decoded `region_meta` snapshot.
That prevents torn generation/state admission during activate/deactivate
transitions.

`try_finalize_slot` must be treated as a staged free-publication protocol:

1. exact transition `Leased(ownerless) -> Free(None)`
2. clear slot storage and zero `slot_generation`
3. exact claim `Free(None) -> Free(PushClaimed)`
4. publish the slot into the current freelist epoch
5. canonicalize to `Free(InFreelist)`

`reinit_in_place` must be treated as a staged retention and recovery protocol:

1. validate an existing same-layout region
2. reset the process-local worker registry and clear `worker_pid`
3. publish a newer `REINITING` `region_meta`
4. quiesce or adopt any slot still carrying a live transitional token
   - `Free(Popped)`
   - `Reserved(None)`
   - `Free(PushClaimed)`
5. rotate to a fresh `freelistEpoch`
6. republish every stable free slot into the current epoch
7. publish the same bumped generation in `OFFLINE`
8. later `activate_worker_generation()` sweeps old generations and may reap
   dead old backends or clear old worker ownership

This is the target runtime refinement of the authoritative token-handoff
actions in `micro/SlotOwnershipProtocol.tla`. The current runtime and
`loom_reinit_retention` harness still model an older state-only projection and
need a follow-up refactor to match the new authoritative spec.

## Reduced-Core `loom` Targets

The `loom` harnesses cover protocol seams, not the whole crate:

- `loom_ring_ready.rs`
  Ready-flag ordering versus publish/consume.
- `loom_slot_incarnation.rs`
  Same-generation reuse, lease epochs, and local worker registry ownership.
- `loom_worker_claim.rs`
  Tentative worker claim, pending reservation, backend release/reap, finalize,
  and reuse.
- `loom_region_gate.rs`
  Packed generation/state admission versus activate/deactivate transitions.

Every concurrency bug found in the real implementation should produce:

1. a deterministic regression test in `src/tests.rs`
2. a reduced-core `loom` scenario when the bug is about interleavings
3. an update to this refinement document if the operation shape changed

## Micro-Spec Coverage

The high-level specs are intentionally atomic. The `micro/` specs model the
intermediate states that exist inside the Rust implementations.

- `micro/WorkerClaimPendingReuse.tla`
  - `claim_worker_slot`
  - pending reservation
  - stale recheck / rollback
  - same-generation finalize + reacquire races

- `micro/BackendAcquirePublish.tla`
  - `BackendSlotLease::acquire`
  - freelist reserve
  - metadata publish
  - generation/offline recheck
  - rollback to reusable state

- `micro/FinalizeReuse.tla`
  - `release_backend`
  - `release_worker_slot`
  - `reap_dead_backend_owner`
  - `try_finalize_slot`
  - same-generation reuse after finalize

- `micro/RegionLifecycleGate.tla`
  - packed region lifecycle gate
  - backend acquire admission
  - worker-ready/attach style online-generation rechecks
  - activate/deactivate transitions

- `micro/SlotOwnershipProtocol.tla`
  - authoritative token-handoff protocol for acquire, finalize,
    free publication, and reinit rebuild
  - current Rust / `loom` models still lag the popped-token and
    fixed-point-finish seams
  - `BackendAcquirePublish`, `FinalizeReuse`, and `ReinitPreservesOldSlots`
    are projections over this model

- `micro/ReinitPreservesOldSlots.tla`
  - existing same-layout `reinit_in_place`
  - old-slot retention across reinit
  - delayed freelist return until exact old finalize
  - crash-window republish of logically free slots
  - fresh acquire only after old-incarnation retirement

Default rule:

- if a lifecycle/reuse bug is about intermediate protocol steps, add or update
  a `micro/` spec
- if it is about concrete atomic interleavings, add or update a reduced-core
  `loom` harness
- if it affects observable crate behavior, also add a deterministic regression
  test in `src/tests.rs`

## Review Checklist

Use this checklist for lifecycle/ring changes:

- Which abstract operation from the TLA+ specs is this implementing?
- Is this an observation, guarded mutation, tentative mutation, reclamation, or
  publish/consume-with-hint operation?
- What is the identity guard?
- Where is the linearization point?
- Can a stale path still reach a destructive mutation?
- Can rollback touch a fresh incarnation?
- Can finalize/reuse happen while a tentative reservation is still live?
- Is there already a `loom` harness for this seam?
- Do we need a new deterministic regression test?

Every new lifecycle or ring mutation should be classifiable using the sections
above before code review is considered complete.
