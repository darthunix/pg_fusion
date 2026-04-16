---- MODULE FinalizeReuse ----
EXTENDS Naturals

\* Rust seam:
\*   - src/region/backend.rs: BackendSlotLease::release
\*   - src/region/lifecycle.rs: release_worker_slot
\*   - src/region/lifecycle.rs: reap_dead_backend_owner
\*   - src/region/lifecycle.rs: try_finalize_slot
\* Scope:
\*   - models same-generation reuse only
\*   - slotGen is intentionally trivial here: 0 means free, 1 means leased
\*   - the reuse seam is driven by leaseEpoch, not generation bumps
\*   - regionGen and workerState are intentionally omitted; they live in the
\*     higher-level lifecycle/bridge specs

LeaseStates == {"Free", "Leased"}
StorageStates == {"Dirty", "Cleared"}
StaleOutcomes == {"None", "IgnoredRelease", "IgnoredFinalize"}
OwnerBits == {"B", "W"}

VARIABLES
    leaseState,
    slotGen,
    leaseEpoch,
    nextLeaseEpoch,
    ownerMask,
    backendAlive,
    workerAlive,
    backendPidPresent,
    freelistContainsSlot,
    storageState,
    staleEpoch,
    lastStaleOp

vars ==
    << leaseState,
       slotGen,
       leaseEpoch,
       nextLeaseEpoch,
       ownerMask,
       backendAlive,
       workerAlive,
       backendPidPresent,
       freelistContainsSlot,
       storageState,
       staleEpoch,
       lastStaleOp >>

Init ==
    /\ leaseState = "Leased"
    /\ slotGen = 1
    /\ leaseEpoch = 1
    /\ nextLeaseEpoch = 2
    /\ ownerMask = {"B", "W"}
    /\ backendAlive = TRUE
    /\ workerAlive = TRUE
    /\ backendPidPresent = TRUE
    /\ freelistContainsSlot = FALSE
    /\ storageState = "Dirty"
    /\ staleEpoch = 0
    /\ lastStaleOp = "None"

ReleaseBackend ==
    /\ leaseState = "Leased"
    /\ "B" \in ownerMask
    /\ backendAlive
    /\ ownerMask' = ownerMask \ {"B"}
    /\ backendAlive' = FALSE
    /\ backendPidPresent' = FALSE
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    workerAlive, freelistContainsSlot, storageState, staleEpoch >>

CrashBackend ==
    /\ leaseState = "Leased"
    /\ "B" \in ownerMask
    /\ backendAlive
    /\ backendAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    workerAlive, backendPidPresent, freelistContainsSlot, storageState, staleEpoch >>

ReapDeadBackend ==
    /\ leaseState = "Leased"
    /\ ~backendAlive
    /\ "B" \in ownerMask
    /\ ownerMask' = ownerMask \ {"B"}
    /\ backendPidPresent' = FALSE
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch,
                    backendAlive, workerAlive, freelistContainsSlot, storageState, staleEpoch >>

ReleaseWorker ==
    /\ leaseState = "Leased"
    /\ "W" \in ownerMask
    /\ ownerMask' = ownerMask \ {"W"}
    /\ workerAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, backendAlive,
                    backendPidPresent, freelistContainsSlot, storageState, staleEpoch >>

CrashWorker ==
    /\ leaseState = "Leased"
    /\ workerAlive
    /\ workerAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    backendAlive, backendPidPresent, freelistContainsSlot, storageState, staleEpoch >>

Finalize ==
    /\ leaseState = "Leased"
    /\ ownerMask = {}
    /\ leaseState' = "Free"
    /\ slotGen' = 0
    /\ staleEpoch' = leaseEpoch
    /\ leaseEpoch' = 0
    /\ freelistContainsSlot' = TRUE
    /\ storageState' = "Cleared"
    /\ backendAlive' = FALSE
    /\ workerAlive' = FALSE
    /\ backendPidPresent' = FALSE
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << nextLeaseEpoch, ownerMask >>

Reacquire ==
    /\ leaseState = "Free"
    /\ freelistContainsSlot
    /\ leaseState' = "Leased"
    /\ slotGen' = 1
    /\ leaseEpoch' = nextLeaseEpoch
    /\ nextLeaseEpoch' = nextLeaseEpoch + 1
    /\ ownerMask' = {"B"}
    /\ backendAlive' = TRUE
    /\ workerAlive' = FALSE
    /\ backendPidPresent' = TRUE
    /\ freelistContainsSlot' = FALSE
    /\ storageState' = "Cleared"
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << staleEpoch >>

CreateResidue ==
    /\ leaseState = "Leased"
    /\ storageState = "Cleared"
    /\ storageState' = "Dirty"
    /\ lastStaleOp' = "None"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    backendAlive, workerAlive, backendPidPresent, freelistContainsSlot, staleEpoch >>

StaleReleaseBackend ==
    /\ staleEpoch # 0
    /\ leaseState = "Leased"
    /\ leaseEpoch # staleEpoch
    /\ lastStaleOp' = "IgnoredRelease"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    backendAlive, workerAlive, backendPidPresent, freelistContainsSlot,
                    storageState, staleEpoch >>

StaleFinalize ==
    /\ staleEpoch # 0
    /\ leaseState = "Leased"
    /\ leaseEpoch # staleEpoch
    /\ lastStaleOp' = "IgnoredFinalize"
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    backendAlive, workerAlive, backendPidPresent, freelistContainsSlot,
                    storageState, staleEpoch >>

Next ==
    \/ ReleaseBackend
    \/ CrashBackend
    \/ ReapDeadBackend
    \/ ReleaseWorker
    \/ CrashWorker
    \/ Finalize
    \/ Reacquire
    \/ CreateResidue
    \/ StaleReleaseBackend
    \/ StaleFinalize

Spec == Init /\ [][Next]_vars
FinalizeActions == Finalize
FairSpec == Spec /\ WF_vars(FinalizeActions)

TypeOK ==
    /\ leaseState \in LeaseStates
    /\ slotGen \in Nat
    /\ leaseEpoch \in Nat
    /\ nextLeaseEpoch \in Nat
    /\ ownerMask \subseteq OwnerBits
    /\ backendAlive \in BOOLEAN
    /\ workerAlive \in BOOLEAN
    /\ backendPidPresent \in BOOLEAN
    /\ freelistContainsSlot \in BOOLEAN
    /\ storageState \in StorageStates
    /\ staleEpoch \in Nat
    /\ lastStaleOp \in StaleOutcomes

FreelistMatchesLeaseState ==
    freelistContainsSlot <=> leaseState = "Free"

FreeSlotIsClearedAndOwnerless ==
    leaseState = "Free" =>
        /\ ownerMask = {}
        /\ leaseEpoch = 0
        /\ slotGen = 0
        /\ storageState = "Cleared"

LeasedSlotNotInFreelist ==
    leaseState = "Leased" =>
        /\ ~freelistContainsSlot
        /\ leaseEpoch # 0

ReacquireUsesFreshEpoch ==
    staleEpoch # 0 /\ leaseState = "Leased" => staleEpoch # leaseEpoch

IgnoredReleaseSeesNewIncarnation ==
    lastStaleOp = "IgnoredRelease" =>
        /\ leaseState = "Leased"
        /\ staleEpoch # 0
        /\ leaseEpoch # staleEpoch

IgnoredFinalizeSeesNewIncarnation ==
    lastStaleOp = "IgnoredFinalize" =>
        /\ leaseState = "Leased"
        /\ staleEpoch # 0
        /\ leaseEpoch # staleEpoch

OwnerlessEventuallyFinalizes ==
    []((leaseState = "Leased" /\ ownerMask = {}) => <>(leaseState = "Free"))

TlcSmokeBound ==
    nextLeaseEpoch <= 3

TlcDeepBound ==
    nextLeaseEpoch <= 5
====
