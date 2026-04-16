---- MODULE WorkerClaimPendingReuse ----
EXTENDS Naturals

\* Rust seam:
\*   - src/region/lifecycle.rs: TransportRegion::claim_worker_slot
\*   - src/region/lifecycle.rs: rollback_worker_claim
\* Scope:
\*   - models the pending-reservation protocol for same-generation reuse
\*   - keeps regionGen/workerState only as ambient state carried through the
\*     claim protocol
\*   - in this narrow scope they are intentionally sticky; generation bumps
\*     and worker online/offline rechecks are covered by BackendAcquirePublish

OwnerBits == {"B", "W", "P"}
WorkerStates == {"Offline", "Online"}
LeaseStates == {"Free", "Leased"}
ClaimPhases == {"Idle", "SnapshotTaken", "LocalReserved", "PendingReserved", "Committed", "RollingBack"}

VARIABLES
    regionGen,
    workerState,
    leaseState,
    slotGen,
    leaseEpoch,
    nextLeaseEpoch,
    ownerMask,
    backendAlive,
    localWorkerReserved,
    claimPhase,
    snapGen,
    snapEpoch

vars ==
    << regionGen,
       workerState,
       leaseState,
       slotGen,
       leaseEpoch,
       nextLeaseEpoch,
       ownerMask,
       backendAlive,
       localWorkerReserved,
       claimPhase,
       snapGen,
       snapEpoch >>

Init ==
    /\ regionGen = 1
    /\ workerState = "Online"
    /\ leaseState = "Leased"
    /\ slotGen = 1
    /\ leaseEpoch = 1
    /\ nextLeaseEpoch = 2
    /\ ownerMask = {"B"}
    /\ backendAlive = TRUE
    /\ localWorkerReserved = FALSE
    /\ claimPhase = "Idle"
    /\ snapGen = 0
    /\ snapEpoch = 0

TakeClaimSnapshot ==
    /\ claimPhase = "Idle"
    /\ workerState = "Online"
    /\ leaseState = "Leased"
    /\ ownerMask = {"B"}
    /\ snapGen' = slotGen
    /\ snapEpoch' = leaseEpoch
    /\ claimPhase' = "SnapshotTaken"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendAlive, localWorkerReserved >>

ReserveLocalWorker ==
    /\ claimPhase = "SnapshotTaken"
    /\ ~localWorkerReserved
    /\ localWorkerReserved' = TRUE
    /\ claimPhase' = "LocalReserved"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendAlive, snapGen, snapEpoch >>

\* The protocol bug came from reserving pending state by owner bits alone.
\* The corrected protocol requires the current incarnation to still match the
\* original claim snapshot at the reservation point.
ReservePending ==
    /\ claimPhase = "LocalReserved"
    /\ leaseState = "Leased"
    /\ ownerMask = {"B"}
    /\ slotGen = snapGen
    /\ leaseEpoch = snapEpoch
    /\ ownerMask' = {"B", "P"}
    /\ claimPhase' = "PendingReserved"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    backendAlive, localWorkerReserved, snapGen, snapEpoch >>

CommitClaim ==
    /\ claimPhase = "PendingReserved"
    /\ workerState = "Online"
    /\ backendAlive
    /\ leaseState = "Leased"
    /\ slotGen = snapGen
    /\ leaseEpoch = snapEpoch
    /\ ownerMask = {"B", "P"}
    /\ ownerMask' = {"B", "W"}
    /\ claimPhase' = "Committed"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    backendAlive, localWorkerReserved, snapGen, snapEpoch >>

StartRollback ==
    /\ claimPhase = "PendingReserved"
    /\ ~( /\ workerState = "Online"
          /\ backendAlive
          /\ leaseState = "Leased"
          /\ slotGen = snapGen
          /\ leaseEpoch = snapEpoch
          /\ ownerMask = {"B", "P"} )
    /\ localWorkerReserved' = FALSE
    /\ claimPhase' = "RollingBack"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendAlive, snapGen, snapEpoch >>

RollbackClaim ==
    /\ claimPhase = "RollingBack"
    /\ leaseState = "Leased"
    /\ slotGen = snapGen
    /\ leaseEpoch = snapEpoch
    /\ "P" \in ownerMask
    /\ ownerMask' = ownerMask \ {"P"}
    /\ claimPhase' = "Idle"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    backendAlive, localWorkerReserved, snapGen, snapEpoch >>

ReleaseCommittedWorker ==
    /\ claimPhase = "Committed"
    /\ "W" \in ownerMask
    /\ ownerMask' = ownerMask \ {"W"}
    /\ localWorkerReserved' = FALSE
    /\ claimPhase' = "Idle"
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    backendAlive, snapGen, snapEpoch >>

ReleaseBackend ==
    /\ leaseState = "Leased"
    /\ "B" \in ownerMask
    /\ backendAlive
    /\ ownerMask' = ownerMask \ {"B"}
    /\ backendAlive' = FALSE
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    localWorkerReserved, claimPhase, snapGen, snapEpoch >>

BackendDies ==
    /\ "B" \in ownerMask
    /\ backendAlive
    /\ backendAlive' = FALSE
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, localWorkerReserved, claimPhase, snapGen, snapEpoch >>

ReapDeadBackend ==
    /\ leaseState = "Leased"
    /\ ~backendAlive
    /\ "B" \in ownerMask
    /\ ownerMask' = ownerMask \ {"B"}
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    backendAlive, localWorkerReserved, claimPhase, snapGen, snapEpoch >>

Finalize ==
    /\ leaseState = "Leased"
    /\ ownerMask = {}
    /\ leaseState' = "Free"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, ownerMask, backendAlive,
                    localWorkerReserved, claimPhase, snapGen, snapEpoch >>

ReacquireSameGeneration ==
    /\ leaseState = "Free"
    /\ leaseState' = "Leased"
    /\ slotGen' = regionGen
    /\ leaseEpoch' = nextLeaseEpoch
    /\ nextLeaseEpoch' = nextLeaseEpoch + 1
    /\ ownerMask' = {"B"}
    /\ backendAlive' = TRUE
    /\ UNCHANGED << regionGen, workerState, localWorkerReserved, claimPhase, snapGen, snapEpoch >>

Next ==
    \/ TakeClaimSnapshot
    \/ ReserveLocalWorker
    \/ ReservePending
    \/ CommitClaim
    \/ StartRollback
    \/ RollbackClaim
    \/ ReleaseCommittedWorker
    \/ ReleaseBackend
    \/ BackendDies
    \/ ReapDeadBackend
    \/ Finalize
    \/ ReacquireSameGeneration

Spec == Init /\ [][Next]_vars
ClaimResolutionActions == CommitClaim \/ StartRollback \/ RollbackClaim
FairSpec == Spec /\ WF_vars(ClaimResolutionActions)

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ leaseState \in LeaseStates
    /\ slotGen \in Nat
    /\ leaseEpoch \in Nat
    /\ nextLeaseEpoch \in Nat
    /\ ownerMask \subseteq OwnerBits
    /\ backendAlive \in BOOLEAN
    /\ localWorkerReserved \in BOOLEAN
    /\ claimPhase \in ClaimPhases
    /\ snapGen \in Nat
    /\ snapEpoch \in Nat

PendingMatchesSnapshot ==
    claimPhase = "PendingReserved" =>
        /\ "P" \in ownerMask
        /\ leaseState = "Leased"
        /\ slotGen = snapGen
        /\ leaseEpoch = snapEpoch

PendingFreezesLeaseMetadata ==
    "P" \in ownerMask =>
        /\ leaseState = "Leased"
        /\ slotGen = snapGen
        /\ leaseEpoch = snapEpoch

CommittedMatchesSnapshot ==
    claimPhase = "Committed" =>
        /\ "W" \in ownerMask
        /\ leaseState = "Leased"
        /\ slotGen = snapGen
        /\ leaseEpoch = snapEpoch

NoFreshLeaseWedgeFromStalePending ==
    ~(/\ leaseState = "Leased"
      /\ ownerMask = {"B", "P"}
      /\ snapEpoch # 0
      /\ leaseEpoch # snapEpoch)

LocalReservationRequiresActiveClaim ==
    localWorkerReserved =>
        claimPhase \in {"LocalReserved", "PendingReserved", "Committed"}

PendingEventuallyResolves ==
    [](claimPhase = "PendingReserved" => <>(claimPhase # "PendingReserved"))

TlcSmokeBound ==
    /\ nextLeaseEpoch <= 4
    /\ snapEpoch <= 3

TlcDeepBound ==
    /\ nextLeaseEpoch <= 6
    /\ snapEpoch <= 5
====
