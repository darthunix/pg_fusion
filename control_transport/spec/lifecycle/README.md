# SingleSlotLifecycle

This model covers one leased slot across worker activation, invalidation,
stale-owner cleanup, finalization, and reuse. It intentionally abstracts the
transport itself down to one boolean, `hasResidue`, so that the model can
express "transport residue may still exist" without duplicating the full queue
semantics from `transport/`.

## Variable mapping

- `regionGen` -> `region_generation`
- `workerState` -> global worker lifecycle state
- `leased` -> `lease_state != FREE`
- `backendOwned` / `workerOwned` -> ownership bits in `owner_mask`
- `backendAlive` / `workerAlive` -> whether the owner process is still alive
- `slotGen` -> `slot_generation`
- `hasResidue` -> abstract "some transport state for this incarnation may still exist" flag

## TLC constants

This module has no user constants.

## TLC run

- config file: `SingleSlotLifecycle.cfg`
- specification operator: `Spec`
- constants: none
- TLC bound:
  - `CONSTRAINT TlcSmokeBound`
- invariants:
  - `TypeOK`
  - `OwnersRequireLease`
  - `LiveOwnerRequiresOwnedBit`
  - `ResidueRequiresLease`
  - `SlotGenerationBounded`
  - `ReusableSlotIsClean`

## Invariants to check

- `TypeOK`
- `OwnersRequireLease`
- `LiveOwnerRequiresOwnedBit`
- `ResidueRequiresLease`
- `SlotGenerationBounded`
- `ReusableSlotIsClean`

## What this spec proves well

- stale owners can exist after crash and must be reaped separately
- generation bumps and slot finalization are different events
- slot reuse only begins from a clean metadata state
- old incarnations can remain leased and stale after generation changes
- residue is lifecycle-only state, not a second queue model shadowing `transport/`
- `workerState = "Offline"` is only a global availability signal; slot-local
  owners may still persist until explicit reap/finalize

## What this spec does not try to prove

- frame ordering or message duplication
- notify semantics
- `ready_flag`
- ring-level detail

Those belong to the transport spec and to Rust-side `loom` checks.

This model has unbounded generation bumps in the raw spec. The committed
`SingleSlotLifecycle.cfg` therefore uses `CONSTRAINT TlcSmokeBound`, where
`TlcSmokeBound == regionGen <= 3`, to turn the default TLC run into a finite
smoke check.
