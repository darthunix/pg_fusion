# SingleSlotBridge

This directory now contains an executable one-slot bridge model plus the seam
contract it stands for.

`SingleSlotBridge.tla` does not duplicate queue semantics from `transport/` and
does not duplicate ownership cleanup detail from `lifecycle/`. Instead it
models the missing glue:

- what identifies one slot incarnation across reuse
- when current handles are still usable
- when old handles must become stale
- why a new lease must start with empty transport state

## Why the bridge needs `leaseEpoch`

The lifecycle model uses `slotGen == regionGen` for the active lease, which is
good enough for restart invalidation but not for all reuse scenarios.

If a slot is finalized and then leased again without a worker restart, the new
lease can reuse the same `slot_generation`. To make “old handle cannot observe
new incarnation state” checkable, the bridge introduces a bridge-local
monotonic `leaseEpoch`.

The bridge therefore models incarnation identity as:

- `Incarnation = (slot_id, leaseEpoch)`

while still tracking `slotGen` separately to model generation invalidation.

## Variable mapping

- `regionGen` -> `region_generation`
- `workerState` -> global worker online/offline state
- `leased` -> slot currently leased or free
- `slotGen` -> current `slot_generation` value published in the slot
- `leaseEpoch` -> bridge-only per-lease incarnation id
- `nextLeaseEpoch` -> next incarnation id to allocate on reuse
- `backendOwned` / `workerOwned` -> ownership bits
- `backendAlive` / `workerAlive` -> owner process liveness
- `residueEpoch` -> which incarnation currently owns any transport residue
- `backendHandleEpoch` / `workerHandleEpoch` -> currently tracked handles
- `staleBackendHandleEpoch` / `staleWorkerHandleEpoch` -> remembered old
  handles preserved across finalize/reuse; only the most recent stale handle
  per side is remembered
- `lastBackendUse` / `lastWorkerUse` -> observable result of the last explicit
  handle-use action

## TLC run

- config file: `SingleSlotBridge.cfg`
- specification operator: `Spec`
- constants: none
- TLC bound:
  - `CONSTRAINT TlcSmokeBound`
  - `TlcSmokeBound == regionGen <= 3 /\ nextLeaseEpoch <= 4`
- invariants:
  - `TypeOK`
  - `LeaseEpochMatchesLeaseState`
  - `NextLeaseEpochAhead`
  - `OwnersRequireLease`
  - `LiveOwnerRequiresOwnedBit`
  - `OwnedSideCarriesCurrentHandle`
  - `ResidueRequiresLease`
  - `ResidueMatchesCurrentLease`
  - `FreeSlotIsEmpty`
  - `VisibleMeansCurrentIncarnation`
  - `StaleHandleEpochDiffersFromCurrent`

## What this spec proves well

- reuse gets a new incarnation identity even when `slot_generation` could be
  reused
- finalize is the boundary that snapshots old handles into explicitly stale
  ones
- current handles can observe only current-incarnation residue
- stale handles stay stale after reuse
- generation bumps can stale out current handles without immediately making the
  slot reusable

## What this spec does not try to prove

- queue capacity or frame ordering
- notify semantics
- `ready_flag`
- `head` / `tail` / wraparound detail
- exact Rust implementation refinement

Those remain in `transport/`, `lifecycle/`, and Rust-side `loom` checks.

The bridge intentionally remembers only one stale handle per side. That is
enough for the current seam property, which is “the most recently invalidated
handle cannot observe a newer incarnation”, but it does not model arbitrarily
old stale handles.

## Contract Summary

The executable bridge model stands for these seam obligations:

1. Transport state belongs to an incarnation, not just a memory address.
2. Reuse starts empty for the new incarnation.
3. Finalization is the only reuse boundary.
4. Generation invalidation and reuse are distinct events.
5. Old handles must not observe new incarnation state.
6. `PeerMissing` never means publish rollback; that remains a transport-level
   guarantee.

## Mapping to artifacts

| Obligation | Transport spec | Lifecycle spec | Bridge spec | Rust-side follow-up |
| --- | --- | --- | --- | --- |
| Half-open delivery is allowed | yes | no | no | scenario tests |
| Publish survives `PeerMissing` | yes | no | no | send/notify tests |
| Slot cannot be reused while owned | no | yes | yes | owner/reap/finalize tests |
| Generation bump makes old state stale | no | yes | yes | stale-handle tests |
| New incarnation starts empty | yes | partial | yes | bridge tests plus `loom` |
| Old incarnation cannot read new data | no | no | yes | bridge tests plus `loom` |

The lifecycle spec intentionally tracks only “some residue may still exist”.
The bridge lifts that into explicit incarnation ownership of residue without
pulling queue detail into the seam model.
