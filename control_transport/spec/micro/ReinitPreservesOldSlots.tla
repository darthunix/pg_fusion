---- MODULE ReinitPreservesOldSlots ----
EXTENDS Naturals

\* Rust seam:
\*   - existing `TransportRegion::reinit_in_place`
\*   - `BackendSlotLease::release`
\*   - `try_finalize_slot`
\*   - `BackendSlotLease::acquire`
\* Scope:
\*   - models same-layout `reinit` with "wait old slots" semantics
\*   - this is a retention/reuse projection of `SlotOwnershipProtocol`, not the
\*     authoritative slot/freelist handoff model
\*   - it intentionally abstracts away the explicit popped-token and
\*     freelist-epoch handoff that now lives in `SlotOwnershipProtocol`
\*   - the `reinit` function exists today; this module pins down the current
\*     Rust refinement with transient `Reiniting` fencing and delayed
\*     `FreePending -> PublishFreeSlot -> Free` publication
\*   - `reinit` invalidates the old generation immediately
\*   - old backend-owned slots stay out of the freelist until exact old finalize
\*   - logically free crash-window slots are republished during reinit
\*   - stale old release/finalize after fresh reuse are ignored
\*   - excludes byte-level ring I/O and process-local worker registry behavior
\*   - stale worker-drop is modeled out-of-band via local-registry reset and
\*     deterministic Rust tests; this spec covers shared-memory retention only

WorkerStates == {"Reiniting", "Offline", "Online"}
LeaseStates == {"Free", "FreePending", "Leased"}
StorageStates == {"Dirty", "Cleared"}
OwnerBits == {"B"}
StaleOutcomes == {"None", "IgnoredOldRelease", "IgnoredOldFinalize"}

VARIABLES
    regionGen,
    workerState,
    leaseState,
    slotGen,
    leaseEpoch,
    nextLeaseEpoch,
    ownerMask,
    freelistContainsSlot,
    backendAlive,
    backendPidPresent,
    storageState,
    staleGeneration,
    staleEpoch,
    staleLeaseHeld,
    lastStaleOp,
    freshAcquireSeen

vars ==
    << regionGen,
       workerState,
       leaseState,
       slotGen,
       leaseEpoch,
       nextLeaseEpoch,
       ownerMask,
       freelistContainsSlot,
       backendAlive,
       backendPidPresent,
       storageState,
       staleGeneration,
       staleEpoch,
       staleLeaseHeld,
       lastStaleOp,
       freshAcquireSeen >>

Init ==
    /\ regionGen = 1
    /\ workerState = "Online"
    /\ leaseState = "Leased"
    /\ slotGen = 1
    /\ leaseEpoch = 1
    /\ nextLeaseEpoch = 2
    /\ ownerMask = {"B"}
    /\ freelistContainsSlot = FALSE
    /\ backendAlive = TRUE
    /\ backendPidPresent = TRUE
    /\ storageState = "Dirty"
    /\ staleGeneration = 0
    /\ staleEpoch = 0
    /\ staleLeaseHeld = FALSE
    /\ lastStaleOp = "None"
    /\ freshAcquireSeen = FALSE

BeginReinit ==
    /\ workerState = "Online"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Reiniting"
    /\ staleGeneration' =
        IF staleLeaseHeld THEN staleGeneration
        ELSE IF leaseState = "Leased" THEN slotGen
        ELSE 0
    /\ staleEpoch' =
        IF staleLeaseHeld THEN staleEpoch
        ELSE IF leaseState = "Leased" THEN leaseEpoch
        ELSE 0
    /\ staleLeaseHeld' =
        IF staleLeaseHeld THEN TRUE ELSE leaseState = "Leased"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = FALSE
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    freelistContainsSlot, backendAlive, backendPidPresent,
                    storageState >>

\* `reinit` bumps `regionGen` once into `Reiniting`, then publish/recovery
\* happens while the region stays non-online. Activation after reinit bumps the
\* generation again, which matches the Rust lifecycle tests.
FinishReinit ==
    /\ workerState = "Reiniting"
    /\ regionGen' = regionGen
    /\ workerState' = "Offline"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    freelistContainsSlot, backendAlive, backendPidPresent,
                    storageState, staleGeneration, staleEpoch, staleLeaseHeld >>

ActivateAfterReinit ==
    /\ workerState = "Offline"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Online"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << leaseState, slotGen, leaseEpoch, nextLeaseEpoch, ownerMask,
                    freelistContainsSlot, backendAlive, backendPidPresent,
                    storageState, staleGeneration, staleEpoch, staleLeaseHeld >>

CrashBackend ==
    /\ leaseState = "Leased"
    /\ backendAlive
    /\ backendAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistContainsSlot,
                    backendPidPresent, storageState, staleGeneration,
                    staleEpoch, staleLeaseHeld >>

OldReleaseBackend ==
    /\ staleLeaseHeld
    /\ leaseState = "Leased"
    /\ slotGen = staleGeneration
    /\ leaseEpoch = staleEpoch
    /\ "B" \in ownerMask
    /\ backendAlive
    /\ ownerMask' = {}
    /\ backendAlive' = FALSE
    /\ backendPidPresent' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch,
                    nextLeaseEpoch, freelistContainsSlot, storageState,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ReapOldBackend ==
    /\ staleLeaseHeld
    /\ leaseState = "Leased"
    /\ slotGen = staleGeneration
    /\ leaseEpoch = staleEpoch
    /\ ~backendAlive
    /\ "B" \in ownerMask
    /\ ownerMask' = {}
    /\ backendPidPresent' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch,
                    nextLeaseEpoch, freelistContainsSlot, backendAlive,
                    storageState, staleGeneration, staleEpoch, staleLeaseHeld >>

FinalizeOldSlot ==
    /\ staleLeaseHeld
    /\ leaseState = "Leased"
    /\ slotGen = staleGeneration
    /\ leaseEpoch = staleEpoch
    /\ ownerMask = {}
    /\ leaseState' = "FreePending"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ freelistContainsSlot' = FALSE
    /\ storageState' = "Cleared"
    /\ staleLeaseHeld' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, ownerMask,
                    backendAlive, backendPidPresent, staleGeneration, staleEpoch >>

PublishFreeSlot ==
    /\ leaseState = "FreePending"
    /\ ~freelistContainsSlot
    /\ leaseState' = "Free"
    /\ freelistContainsSlot' = TRUE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendAlive, backendPidPresent, storageState,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

LoseFreeSlot ==
    /\ leaseState = "Free"
    /\ freelistContainsSlot
    /\ leaseState' = "Free"
    /\ freelistContainsSlot' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendAlive, backendPidPresent, storageState,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ReconcileLostFreeSlot ==
    /\ workerState = "Reiniting"
    /\ leaseState = "Free"
    /\ ~freelistContainsSlot
    /\ leaseState' = "FreePending"
    /\ freelistContainsSlot' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, backendAlive, backendPidPresent, storageState,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

FreshAcquire ==
    /\ ~staleLeaseHeld
    /\ staleEpoch # 0
    /\ workerState = "Online"
    /\ leaseState = "Free"
    /\ freelistContainsSlot
    /\ leaseState' = "Leased"
    /\ slotGen' = regionGen
    /\ leaseEpoch' = nextLeaseEpoch
    /\ nextLeaseEpoch' = nextLeaseEpoch + 1
    /\ ownerMask' = {"B"}
    /\ freelistContainsSlot' = FALSE
    /\ backendAlive' = TRUE
    /\ backendPidPresent' = TRUE
    /\ storageState' = "Cleared"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = TRUE
    /\ UNCHANGED << regionGen, workerState, staleGeneration, staleEpoch, staleLeaseHeld >>

ReleaseFreshBackend ==
    /\ ~staleLeaseHeld
    /\ leaseState = "Leased"
    /\ "B" \in ownerMask
    /\ ownerMask' = {}
    /\ backendAlive' = FALSE
    /\ backendPidPresent' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch,
                    nextLeaseEpoch, freelistContainsSlot, storageState,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

FinalizeFreshSlot ==
    /\ ~staleLeaseHeld
    /\ leaseState = "Leased"
    /\ ownerMask = {}
    /\ leaseState' = "FreePending"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ freelistContainsSlot' = FALSE
    /\ storageState' = "Cleared"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, ownerMask,
                    backendAlive, backendPidPresent, staleGeneration,
                    staleEpoch, staleLeaseHeld >>

IgnoredOldRelease ==
    /\ staleEpoch # 0
    /\ ~staleLeaseHeld
    /\ freshAcquireSeen
    /\ (leaseState = "Free" \/ slotGen # staleGeneration \/ leaseEpoch # staleEpoch)
    /\ lastStaleOp' = "IgnoredOldRelease"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistContainsSlot, backendAlive,
                    backendPidPresent, storageState, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

IgnoredOldFinalize ==
    /\ staleEpoch # 0
    /\ ~staleLeaseHeld
    /\ freshAcquireSeen
    /\ (leaseState = "Free" \/ slotGen # staleGeneration \/ leaseEpoch # staleEpoch)
    /\ lastStaleOp' = "IgnoredOldFinalize"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, leaseState, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistContainsSlot, backendAlive,
                    backendPidPresent, storageState, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

Next ==
    \/ BeginReinit
    \/ FinishReinit
    \/ ActivateAfterReinit
    \/ CrashBackend
    \/ OldReleaseBackend
    \/ ReapOldBackend
    \/ FinalizeOldSlot
    \/ PublishFreeSlot
    \/ LoseFreeSlot
    \/ ReconcileLostFreeSlot
    \/ FreshAcquire
    \/ ReleaseFreshBackend
    \/ FinalizeFreshSlot
    \/ IgnoredOldRelease
    \/ IgnoredOldFinalize

Spec == Init /\ [][Next]_vars
FinalizeActions == FinalizeOldSlot
FairSpec == Spec /\ WF_vars(FinalizeActions)

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ leaseState \in LeaseStates
    /\ slotGen \in Nat
    /\ leaseEpoch \in Nat
    /\ nextLeaseEpoch \in Nat
    /\ ownerMask \subseteq OwnerBits
    /\ freelistContainsSlot \in BOOLEAN
    /\ backendAlive \in BOOLEAN
    /\ backendPidPresent \in BOOLEAN
    /\ storageState \in StorageStates
    /\ staleGeneration \in Nat
    /\ staleEpoch \in Nat
    /\ staleLeaseHeld \in BOOLEAN
    /\ lastStaleOp \in StaleOutcomes
    /\ freshAcquireSeen \in BOOLEAN

FreelistImpliesPublishedFreeState ==
    freelistContainsSlot => leaseState = "Free"

LeasedSlotNotInFreelist ==
    leaseState = "Leased" => ~freelistContainsSlot

FreePendingStaysOutOfFreelist ==
    leaseState = "FreePending" => ~freelistContainsSlot

RetainedStaleLeaseStaysOutOfFreelist ==
    staleLeaseHeld =>
        /\ leaseState = "Leased"
        /\ slotGen = staleGeneration
        /\ leaseEpoch = staleEpoch
        /\ ~freelistContainsSlot

OldBackendOwnerPreventsFreshReuse ==
    staleLeaseHeld /\ "B" \in ownerMask =>
        /\ leaseState = "Leased"
        /\ slotGen = staleGeneration
        /\ leaseEpoch = staleEpoch

FreshLeaseUsesNewIdentity ==
    ~staleLeaseHeld /\ staleEpoch # 0 /\ leaseState = "Leased" =>
        /\ slotGen = regionGen
        /\ leaseEpoch # staleEpoch
        /\ leaseEpoch # 0

IgnoredOldReleaseRequiresFreshAcquireSeen ==
    lastStaleOp = "IgnoredOldRelease" =>
        freshAcquireSeen

IgnoredOldFinalizeRequiresFreshAcquireSeen ==
    lastStaleOp = "IgnoredOldFinalize" =>
        freshAcquireSeen

OwnerlessRetainedSlotEventuallyFinalizes ==
    [](staleLeaseHeld /\ ownerMask = {} => <> ~staleLeaseHeld)

TlcSmokeBound ==
    /\ regionGen <= 3
    /\ nextLeaseEpoch <= 3

TlcDeepBound ==
    /\ regionGen <= 7
    /\ nextLeaseEpoch <= 5
====
