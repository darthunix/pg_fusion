---- MODULE BackendAcquirePublish ----
EXTENDS Naturals

\* Rust seam:
\*   - src/region/backend.rs: BackendSlotLease::acquire
\* Scope:
\*   - models reserve -> clear -> publish -> recheck -> rollback
\*   - intentionally over-approximates invalidation by allowing generation
\*     bumps and worker online/offline transitions independently
\*   - this micro-spec is weaker than SingleSlotLifecycle on that axis
\*   - workerState is not snapshotted; the recheck uses the current online gate
\*     only, so an offline -> online round-trip may still succeed here

WorkerStates == {"Offline", "Online"}
SlotStates == {"Free", "Reserved", "Leased"}
StorageStates == {"Dirty", "Cleared"}
AcquirePhases == {"Idle", "FreelistReserved", "StorageCleared", "MetadataPublished", "Done"}

VARIABLES
    regionGen,
    workerState,
    slotState,
    slotGen,
    leaseEpoch,
    nextLeaseEpoch,
    ownerMask,
    backendPidPresent,
    storageState,
    acquirePhase,
    snapGen

vars ==
    << regionGen,
       workerState,
       slotState,
       slotGen,
       leaseEpoch,
       nextLeaseEpoch,
       ownerMask,
       backendPidPresent,
       storageState,
       acquirePhase,
       snapGen >>

Init ==
    /\ regionGen = 1
    /\ workerState = "Online"
    /\ slotState = "Free"
    /\ slotGen = 0
    /\ leaseEpoch = 0
    /\ nextLeaseEpoch = 1
    /\ ownerMask = {}
    /\ backendPidPresent = FALSE
    /\ storageState = "Dirty"
    /\ acquirePhase = "Idle"
    /\ snapGen = 0

ReserveFreelistSlot ==
    /\ acquirePhase = "Idle"
    /\ slotState = "Free"
    /\ workerState = "Online"
    /\ slotState' = "Reserved"
    /\ acquirePhase' = "FreelistReserved"
    /\ snapGen' = regionGen
    /\ UNCHANGED << regionGen, workerState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendPidPresent, storageState >>

ClearSlotStorage ==
    /\ acquirePhase = "FreelistReserved"
    /\ storageState' = "Cleared"
    /\ acquirePhase' = "StorageCleared"
    /\ UNCHANGED << regionGen, workerState, slotState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendPidPresent, snapGen >>

PublishLeaseMetadata ==
    /\ acquirePhase = "StorageCleared"
    /\ slotState = "Reserved"
    /\ slotState' = "Leased"
    /\ slotGen' = snapGen
    /\ leaseEpoch' = nextLeaseEpoch
    /\ nextLeaseEpoch' = nextLeaseEpoch + 1
    /\ ownerMask' = {"B"}
    /\ backendPidPresent' = TRUE
    /\ acquirePhase' = "MetadataPublished"
    /\ UNCHANGED << regionGen, workerState, storageState, snapGen >>

GenerationBump ==
    /\ regionGen < 2
    /\ regionGen' = regionGen + 1
    /\ UNCHANGED << workerState, slotState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendPidPresent, storageState, acquirePhase, snapGen >>

WorkerGoesOffline ==
    /\ workerState = "Online"
    /\ workerState' = "Offline"
    /\ UNCHANGED << regionGen, slotState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendPidPresent, storageState, acquirePhase, snapGen >>

WorkerComesOnline ==
    /\ workerState = "Offline"
    /\ workerState' = "Online"
    /\ UNCHANGED << regionGen, slotState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendPidPresent, storageState, acquirePhase, snapGen >>

RecheckAcquireSuccess ==
    /\ acquirePhase = "MetadataPublished"
    /\ workerState = "Online"
    /\ regionGen = snapGen
    /\ acquirePhase' = "Done"
    /\ UNCHANGED << regionGen, workerState, slotState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendPidPresent, storageState, snapGen >>

RollbackAcquire ==
    /\ acquirePhase = "MetadataPublished"
    /\ ~(workerState = "Online" /\ regionGen = snapGen)
    /\ slotState' = "Free"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ ownerMask' = {}
    /\ backendPidPresent' = FALSE
    /\ storageState' = "Dirty"
    /\ acquirePhase' = "Idle"
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, snapGen >>

ReleaseBackend ==
    /\ acquirePhase = "Done"
    /\ slotState = "Leased"
    /\ ownerMask = {"B"}
    /\ slotState' = "Free"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ ownerMask' = {}
    /\ backendPidPresent' = FALSE
    /\ storageState' = "Dirty"
    /\ acquirePhase' = "Idle"
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, snapGen >>

Next ==
    \/ ReserveFreelistSlot
    \/ ClearSlotStorage
    \/ PublishLeaseMetadata
    \/ GenerationBump
    \/ WorkerGoesOffline
    \/ WorkerComesOnline
    \/ RecheckAcquireSuccess
    \/ RollbackAcquire
    \/ ReleaseBackend

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ slotState \in SlotStates
    /\ slotGen \in Nat
    /\ leaseEpoch \in Nat
    /\ nextLeaseEpoch \in Nat
    /\ ownerMask \subseteq {"B"}
    /\ backendPidPresent \in BOOLEAN
    /\ storageState \in StorageStates
    /\ acquirePhase \in AcquirePhases
    /\ snapGen \in Nat

PublishedAcquireHasMetadata ==
    acquirePhase \in {"MetadataPublished", "Done"} =>
        /\ slotState = "Leased"
        /\ slotGen = snapGen
        /\ leaseEpoch # 0
        /\ ownerMask = {"B"}
        /\ backendPidPresent
        /\ storageState = "Cleared"

FreeSlotHasNoPublishedLease ==
    slotState = "Free" =>
        /\ slotGen = 0
        /\ leaseEpoch = 0
        /\ ownerMask = {}
        /\ ~backendPidPresent

ReservedSlotHasNoPublishedLease ==
    slotState = "Reserved" =>
        /\ slotGen = 0
        /\ leaseEpoch = 0
        /\ ownerMask = {}
        /\ ~backendPidPresent

PublishedEpochIsFresh ==
    leaseEpoch = 0 \/ leaseEpoch < nextLeaseEpoch

LeasedSlotStartsCleared ==
    slotState = "Leased" => storageState = "Cleared"

TlcSmokeBound ==
    /\ regionGen <= 2
    /\ nextLeaseEpoch <= 3

TlcDeepBound ==
    /\ regionGen <= 3
    /\ nextLeaseEpoch <= 5
====
