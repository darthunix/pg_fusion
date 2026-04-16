---- MODULE SlotOwnershipProtocol ----
EXTENDS Naturals

\* Rust seam:
\*   - `BackendSlotLease::acquire`
\*   - `TransportRegion::try_finalize_slot`
\*   - `TransportRegion::publish_free_slot`
\*   - `TransportRegion::reinit_in_place`
\* Scope:
\*   - one slot, one availability token, one freelist epoch, one region
\*     lifecycle word
\*   - authoritative token-handoff model for slot reuse
\*   - current Rust / `loom` models still lag this spec in the popped-token and
\*     `FinishReinit` fixed-point seams
\*   - `BackendAcquirePublish`, `FinalizeReuse`, and `ReinitPreservesOldSlots`
\*     are projections of this state machine

WorkerStates == {"Online", "Reiniting", "Offline"}
SlotLifecycles == {"Free", "Reserved", "Leased"}
TokenStates == {"InFreelist", "Popped", "None", "PushClaimed", "Pushed"}
OwnerBits == {"B"}
StaleOutcomes == {"None", "IgnoredOldRelease", "IgnoredOldFinalize"}

VARIABLES
    regionGen,
    workerState,
    slotLifecycle,
    slotGen,
    leaseEpoch,
    nextLeaseEpoch,
    ownerMask,
    freelistEpoch,
    tokenState,
    tokenEpoch,
    popActorAlive,
    reserveActorAlive,
    pushActorAlive,
    reinitResetDone,
    staleGeneration,
    staleEpoch,
    staleLeaseHeld,
    lastStaleOp,
    freshAcquireSeen

vars ==
    << regionGen,
       workerState,
       slotLifecycle,
       slotGen,
       leaseEpoch,
       nextLeaseEpoch,
       ownerMask,
       freelistEpoch,
       tokenState,
       tokenEpoch,
       popActorAlive,
       reserveActorAlive,
       pushActorAlive,
       reinitResetDone,
       staleGeneration,
       staleEpoch,
       staleLeaseHeld,
       lastStaleOp,
       freshAcquireSeen >>

Init ==
    /\ regionGen = 1
    /\ workerState = "Online"
    /\ slotLifecycle = "Leased"
    /\ slotGen = 1
    /\ leaseEpoch = 1
    /\ nextLeaseEpoch = 2
    /\ ownerMask = {"B"}
    /\ freelistEpoch = 1
    /\ tokenState = "None"
    /\ tokenEpoch = 0
    /\ popActorAlive = FALSE
    /\ reserveActorAlive = FALSE
    /\ pushActorAlive = FALSE
    /\ reinitResetDone = FALSE
    /\ staleGeneration = 0
    /\ staleEpoch = 0
    /\ staleLeaseHeld = FALSE
    /\ lastStaleOp = "None"
    /\ freshAcquireSeen = FALSE

PopFreelistToken ==
    /\ workerState = "Online"
    /\ slotLifecycle = "Free"
    /\ tokenState = "InFreelist"
    /\ tokenEpoch = freelistEpoch
    /\ tokenState' = "Popped"
    /\ popActorAlive' = TRUE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenEpoch,
                    reserveActorAlive, pushActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ClaimPoppedForAcquire ==
    /\ workerState = "Online"
    /\ slotLifecycle = "Free"
    /\ tokenState = "Popped"
    /\ popActorAlive
    /\ slotLifecycle' = "Reserved"
    /\ tokenState' = "None"
    /\ tokenEpoch' = 0
    /\ popActorAlive' = FALSE
    /\ reserveActorAlive' = TRUE
    /\ ownerMask' = {}
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotGen, leaseEpoch, nextLeaseEpoch,
                    freelistEpoch, pushActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

AbortPoppedOrReservedAcquire ==
    /\ (
        /\ slotLifecycle = "Free"
        /\ tokenState = "Popped"
        /\ popActorAlive
        /\ slotLifecycle' = "Free"
        /\ slotGen' = 0
        /\ leaseEpoch' = 0
        /\ ownerMask' = {}
        /\ tokenState' = "None"
        /\ tokenEpoch' = 0
        /\ popActorAlive' = FALSE
        /\ reserveActorAlive' = reserveActorAlive
       )
       \/
       (
        /\ slotLifecycle = "Reserved"
        /\ reserveActorAlive
        /\ slotLifecycle' = "Free"
        /\ slotGen' = 0
        /\ leaseEpoch' = 0
        /\ ownerMask' = {}
        /\ tokenState' = "None"
        /\ tokenEpoch' = 0
        /\ popActorAlive' = popActorAlive
        /\ reserveActorAlive' = FALSE
       )
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, freelistEpoch,
                    pushActorAlive, reinitResetDone, staleGeneration,
                    staleEpoch, staleLeaseHeld >>

PublishLease ==
    /\ slotLifecycle = "Reserved"
    /\ reserveActorAlive
    /\ workerState = "Online"
    /\ slotLifecycle' = "Leased"
    /\ slotGen' = regionGen
    /\ leaseEpoch' = nextLeaseEpoch
    /\ nextLeaseEpoch' = nextLeaseEpoch + 1
    /\ ownerMask' = {"B"}
    /\ tokenState' = "None"
    /\ tokenEpoch' = 0
    /\ reserveActorAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = (staleEpoch # 0)
    /\ UNCHANGED << regionGen, workerState, freelistEpoch, popActorAlive,
                    pushActorAlive, reinitResetDone, staleGeneration,
                    staleEpoch, staleLeaseHeld >>

CrashPoppedActor ==
    /\ tokenState = "Popped"
    /\ popActorAlive
    /\ popActorAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenState,
                    tokenEpoch, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

CrashReservedActor ==
    /\ slotLifecycle = "Reserved"
    /\ reserveActorAlive
    /\ reserveActorAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenState,
                    tokenEpoch, popActorAlive, pushActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ReleaseBackend ==
    /\ slotLifecycle = "Leased"
    /\ "B" \in ownerMask
    /\ ownerMask' = {}
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, freelistEpoch, tokenState, tokenEpoch,
                    popActorAlive, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

FinalizeOwnerlessLeaseToPending ==
    /\ slotLifecycle = "Leased"
    /\ ownerMask = {}
    /\ slotLifecycle' = "Free"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ ownerMask' = {}
    /\ tokenState' = "None"
    /\ tokenEpoch' = 0
    /\ staleLeaseHeld' =
        IF staleLeaseHeld /\ slotGen = staleGeneration /\ leaseEpoch = staleEpoch
            THEN FALSE
            ELSE staleLeaseHeld
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, freelistEpoch,
                    popActorAlive, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch >>

ClaimFreePush ==
    /\ slotLifecycle = "Free"
    /\ tokenState = "None"
    /\ workerState # "Reiniting"
    /\ tokenState' = "PushClaimed"
    /\ pushActorAlive' = TRUE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenEpoch,
                    popActorAlive, reserveActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ExecuteFreelistPush ==
    /\ slotLifecycle = "Free"
    /\ tokenState = "PushClaimed"
    /\ pushActorAlive
    /\ tokenState' = "Pushed"
    /\ tokenEpoch' = freelistEpoch
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, popActorAlive,
                    reserveActorAlive, pushActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

CanonicalizeFree ==
    /\ slotLifecycle = "Free"
    /\ tokenState = "Pushed"
    /\ tokenState' = "InFreelist"
    /\ pushActorAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenEpoch,
                    popActorAlive, reserveActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

CrashPushActor ==
    /\ tokenState = "PushClaimed"
    /\ pushActorAlive
    /\ pushActorAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenState,
                    tokenEpoch, popActorAlive, reserveActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

BeginReinit ==
    /\ workerState = "Online"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Reiniting"
    /\ reinitResetDone' = FALSE
    /\ staleGeneration' =
        IF staleLeaseHeld THEN staleGeneration
        ELSE IF slotLifecycle = "Leased" THEN slotGen
        ELSE 0
    /\ staleEpoch' =
        IF staleLeaseHeld THEN staleEpoch
        ELSE IF slotLifecycle = "Leased" THEN leaseEpoch
        ELSE 0
    /\ staleLeaseHeld' =
        IF staleLeaseHeld THEN TRUE ELSE slotLifecycle = "Leased"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = FALSE
    /\ UNCHANGED << slotLifecycle, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, freelistEpoch, tokenState, tokenEpoch,
                    popActorAlive, reserveActorAlive, pushActorAlive >>

ReinitAdoptDeadPopped ==
    /\ workerState = "Reiniting"
    /\ slotLifecycle = "Free"
    /\ tokenState = "Popped"
    /\ ~popActorAlive
    /\ tokenState' = "None"
    /\ tokenEpoch' = 0
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, popActorAlive,
                    reserveActorAlive, pushActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ReinitAdoptDeadReserved ==
    /\ workerState = "Reiniting"
    /\ slotLifecycle = "Reserved"
    /\ tokenState = "None"
    /\ ~reserveActorAlive
    /\ slotLifecycle' = "Free"
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ ownerMask' = {}
    /\ tokenState' = "None"
    /\ tokenEpoch' = 0
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, nextLeaseEpoch, freelistEpoch,
                    popActorAlive, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

ReinitAdoptDeadPushClaim ==
    /\ workerState = "Reiniting"
    /\ slotLifecycle = "Free"
    /\ tokenState = "PushClaimed"
    /\ ~pushActorAlive
    /\ tokenState' = "None"
    /\ tokenEpoch' = 0
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, popActorAlive,
                    reserveActorAlive, pushActorAlive, reinitResetDone,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ReinitResetFreelistEpoch ==
    /\ workerState = "Reiniting"
    /\ ~reinitResetDone
    /\ slotLifecycle # "Reserved"
    /\ tokenState # "Popped"
    /\ tokenState # "PushClaimed"
    /\ freelistEpoch' = freelistEpoch + 1
    /\ reinitResetDone' = TRUE
    /\ tokenState' =
        IF slotLifecycle = "Free" /\ tokenState \in {"InFreelist", "Pushed"}
            THEN "None"
            ELSE tokenState
    /\ tokenEpoch' =
        IF slotLifecycle = "Free" /\ tokenState \in {"InFreelist", "Pushed"}
            THEN 0
            ELSE tokenEpoch
    /\ pushActorAlive' =
        IF slotLifecycle = "Free" /\ tokenState \in {"InFreelist", "Pushed"}
            THEN FALSE
            ELSE pushActorAlive
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, popActorAlive, reserveActorAlive,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

ReinitRepublishStableFree ==
    /\ workerState = "Reiniting"
    /\ reinitResetDone
    /\ slotLifecycle = "Free"
    /\ tokenState = "None"
    /\ tokenState' = "InFreelist"
    /\ tokenEpoch' = freelistEpoch
    /\ pushActorAlive' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, popActorAlive,
                    reserveActorAlive, reinitResetDone, staleGeneration,
                    staleEpoch, staleLeaseHeld >>

FinishReinit ==
    /\ workerState = "Reiniting"
    /\ reinitResetDone
    /\ slotLifecycle # "Reserved"
    /\ tokenState # "Popped"
    /\ tokenState # "PushClaimed"
    /\ slotLifecycle = "Leased" => tokenState = "None"
    /\ slotLifecycle = "Free" => /\ tokenState = "InFreelist"
                                 /\ tokenEpoch = freelistEpoch
    /\ workerState' = "Offline"
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenState,
                    tokenEpoch, popActorAlive, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

ActivateAfterReinit ==
    /\ workerState = "Offline"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Online"
    /\ reinitResetDone' = FALSE
    /\ lastStaleOp' = "None"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << slotLifecycle, slotGen, leaseEpoch, nextLeaseEpoch,
                    ownerMask, freelistEpoch, tokenState, tokenEpoch,
                    popActorAlive, reserveActorAlive, pushActorAlive,
                    staleGeneration, staleEpoch, staleLeaseHeld >>

IgnoredOldRelease ==
    /\ staleEpoch # 0
    /\ ~staleLeaseHeld
    /\ freshAcquireSeen
    /\ (slotLifecycle # "Leased" \/ slotGen # staleGeneration \/ leaseEpoch # staleEpoch)
    /\ lastStaleOp' = "IgnoredOldRelease"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenState,
                    tokenEpoch, popActorAlive, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

IgnoredOldFinalize ==
    /\ staleEpoch # 0
    /\ ~staleLeaseHeld
    /\ freshAcquireSeen
    /\ (slotLifecycle # "Leased" \/ slotGen # staleGeneration \/ leaseEpoch # staleEpoch)
    /\ lastStaleOp' = "IgnoredOldFinalize"
    /\ freshAcquireSeen' = freshAcquireSeen
    /\ UNCHANGED << regionGen, workerState, slotLifecycle, slotGen, leaseEpoch,
                    nextLeaseEpoch, ownerMask, freelistEpoch, tokenState,
                    tokenEpoch, popActorAlive, reserveActorAlive, pushActorAlive,
                    reinitResetDone, staleGeneration, staleEpoch,
                    staleLeaseHeld >>

Next ==
    \/ PopFreelistToken
    \/ ClaimPoppedForAcquire
    \/ AbortPoppedOrReservedAcquire
    \/ PublishLease
    \/ CrashPoppedActor
    \/ CrashReservedActor
    \/ ReleaseBackend
    \/ FinalizeOwnerlessLeaseToPending
    \/ ClaimFreePush
    \/ ExecuteFreelistPush
    \/ CanonicalizeFree
    \/ CrashPushActor
    \/ BeginReinit
    \/ ReinitAdoptDeadPopped
    \/ ReinitAdoptDeadReserved
    \/ ReinitAdoptDeadPushClaim
    \/ ReinitResetFreelistEpoch
    \/ ReinitRepublishStableFree
    \/ FinishReinit
    \/ ActivateAfterReinit
    \/ IgnoredOldRelease
    \/ IgnoredOldFinalize

Spec == Init /\ [][Next]_vars
ResetAction == ReinitResetFreelistEpoch
RepublishAction == ReinitRepublishStableFree
FairSpec == Spec /\ WF_vars(ResetAction) /\ WF_vars(RepublishAction)

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ slotLifecycle \in SlotLifecycles
    /\ slotGen \in Nat
    /\ leaseEpoch \in Nat
    /\ nextLeaseEpoch \in Nat
    /\ ownerMask \subseteq OwnerBits
    /\ freelistEpoch \in Nat
    /\ tokenState \in TokenStates
    /\ tokenEpoch \in Nat
    /\ popActorAlive \in BOOLEAN
    /\ reserveActorAlive \in BOOLEAN
    /\ pushActorAlive \in BOOLEAN
    /\ reinitResetDone \in BOOLEAN
    /\ staleGeneration \in Nat
    /\ staleEpoch \in Nat
    /\ staleLeaseHeld \in BOOLEAN
    /\ lastStaleOp \in StaleOutcomes
    /\ freshAcquireSeen \in BOOLEAN

PublishedTokenMatchesCurrentEpoch ==
    tokenState = "InFreelist" => tokenEpoch = freelistEpoch

PushedTokenMatchesCurrentEpoch ==
    tokenState = "Pushed" => tokenEpoch = freelistEpoch

NoPublishedTokenForReservedOrLeased ==
    slotLifecycle \in {"Reserved", "Leased"} => tokenState # "InFreelist"

PoppedTokenNotReusable ==
    tokenState = "Popped" => slotLifecycle = "Free"

ReservedHasNoToken ==
    slotLifecycle = "Reserved" => tokenState = "None"

LeasedHasNoToken ==
    slotLifecycle = "Leased" => tokenState = "None"

OnlyLeasedHasOwners ==
    slotLifecycle # "Leased" => ownerMask = {}

ActorFlagsTrackStates ==
    /\ popActorAlive => tokenState = "Popped"
    /\ reserveActorAlive => slotLifecycle = "Reserved"
    /\ pushActorAlive => tokenState \in {"PushClaimed", "Pushed"}

StableFreeMustBeRepublishedBeforeFinish ==
    workerState = "Offline" /\ slotLifecycle = "Free" =>
        /\ tokenState = "InFreelist"
        /\ tokenEpoch = freelistEpoch

RetainedLeaseBlocksReuse ==
    staleLeaseHeld =>
        /\ slotLifecycle = "Leased"
        /\ slotGen = staleGeneration
        /\ leaseEpoch = staleEpoch
        /\ tokenState = "None"

FreshLeaseUsesNewIdentity ==
    ~staleLeaseHeld /\ staleEpoch # 0 /\ slotLifecycle = "Leased" =>
        /\ slotGen = regionGen
        /\ leaseEpoch # staleEpoch
        /\ leaseEpoch # 0

IgnoredOldReleaseRequiresFreshAcquireSeen ==
    lastStaleOp = "IgnoredOldRelease" => freshAcquireSeen

IgnoredOldFinalizeRequiresFreshAcquireSeen ==
    lastStaleOp = "IgnoredOldFinalize" => freshAcquireSeen

FreeTokenEventuallyRepublished ==
    [](workerState = "Reiniting" /\ slotLifecycle = "Free" /\ tokenState = "None"
      => <> (tokenState = "InFreelist" /\ tokenEpoch = freelistEpoch))

TlcSmokeBound ==
    /\ regionGen <= 5
    /\ nextLeaseEpoch <= 4
    /\ freelistEpoch <= 3

TlcDeepBound ==
    /\ regionGen <= 7
    /\ nextLeaseEpoch <= 5
    /\ freelistEpoch <= 4
====
