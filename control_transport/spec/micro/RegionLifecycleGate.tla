---- MODULE RegionLifecycleGate ----
EXTENDS Naturals

\* Rust seam:
\*   - src/region/backend.rs: BackendSlotLease::acquire
\*   - src/region/lifecycle.rs: activate_worker_generation / deactivate_worker_generation
\* Scope:
\*   - models the global lifecycle admission gate as one logical region_meta
\*   - tracks snapshot -> publish -> recheck against packed generation/state
\*   - excludes slot-local lease_epoch and owner-bit protocols; those stay in
\*     BackendAcquirePublish / WorkerClaimPendingReuse / FinalizeReuse

WorkerStates == {"Offline", "Restarting", "Online"}
SlotStates == {"Free", "Reserved", "Leased"}
AcquirePhases == {"Idle", "SnapshotTaken", "MetadataPublished", "Done"}

VARIABLES
    regionGen,
    workerState,
    slotState,
    slotGen,
    acquirePhase,
    snapGen

vars == << regionGen, workerState, slotState, slotGen, acquirePhase, snapGen >>

Init ==
    /\ regionGen = 1
    /\ workerState = "Online"
    /\ slotState = "Free"
    /\ slotGen = 0
    /\ acquirePhase = "Idle"
    /\ snapGen = 0

ReserveFreelistSlot ==
    /\ acquirePhase = "Idle"
    /\ slotState = "Free"
    /\ workerState = "Online"
    /\ regionGen # 0
    /\ slotState' = "Reserved"
    /\ acquirePhase' = "SnapshotTaken"
    /\ snapGen' = regionGen
    /\ UNCHANGED << regionGen, workerState, slotGen >>

PublishLease ==
    /\ acquirePhase = "SnapshotTaken"
    /\ slotState = "Reserved"
    /\ slotState' = "Leased"
    /\ slotGen' = snapGen
    /\ acquirePhase' = "MetadataPublished"
    /\ UNCHANGED << regionGen, workerState, snapGen >>

RecheckAcquireSuccess ==
    /\ acquirePhase = "MetadataPublished"
    /\ workerState = "Online"
    /\ regionGen = snapGen
    /\ acquirePhase' = "Done"
    /\ UNCHANGED << regionGen, workerState, slotState, slotGen, snapGen >>

RollbackAcquire ==
    /\ acquirePhase = "MetadataPublished"
    /\ ~(workerState = "Online" /\ regionGen = snapGen)
    /\ slotState' = "Free"
    /\ slotGen' = 0
    /\ acquirePhase' = "Idle"
    /\ snapGen' = 0
    /\ UNCHANGED << regionGen, workerState >>

ReleaseLease ==
    /\ acquirePhase = "Done"
    /\ slotState = "Leased"
    /\ slotState' = "Free"
    /\ slotGen' = 0
    /\ acquirePhase' = "Idle"
    /\ snapGen' = 0
    /\ UNCHANGED << regionGen, workerState >>

ActivateRestarting ==
    /\ regionGen < 3
    /\ workerState \in {"Offline", "Online"}
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Restarting"
    /\ UNCHANGED << slotState, slotGen, acquirePhase, snapGen >>

ActivateOnline ==
    /\ workerState = "Restarting"
    /\ workerState' = "Online"
    /\ UNCHANGED << regionGen, slotState, slotGen, acquirePhase, snapGen >>

Deactivate ==
    /\ regionGen < 3
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Offline"
    /\ UNCHANGED << slotState, slotGen, acquirePhase, snapGen >>

Next ==
    \/ ReserveFreelistSlot
    \/ PublishLease
    \/ RecheckAcquireSuccess
    \/ RollbackAcquire
    \/ ReleaseLease
    \/ ActivateRestarting
    \/ ActivateOnline
    \/ Deactivate

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ slotState \in SlotStates
    /\ slotGen \in Nat
    /\ acquirePhase \in AcquirePhases
    /\ snapGen \in Nat

PublishedLeaseMatchesSnapshot ==
    acquirePhase \in {"MetadataPublished", "Done"} =>
        /\ slotState = "Leased"
        /\ slotGen = snapGen
        /\ snapGen # 0

DoneLeaseMatchesCurrentOnlineGeneration ==
    acquirePhase = "Done" =>
        /\ slotState = "Leased"
        /\ (regionGen = slotGen => workerState = "Online")

FreeSlotClearsPublishedGeneration ==
    slotState = "Free" => slotGen = 0

TlcSmokeBound ==
    regionGen <= 3

TlcDeepBound ==
    regionGen <= 4
====
