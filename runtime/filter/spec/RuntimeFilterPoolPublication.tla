---- MODULE RuntimeFilterPoolPublication ----
EXTENDS Naturals

CONSTANTS MaxProbeRefs, MaxEpoch

PoolStates == {"Free", "Initializing", "Allocated", "Retiring"}
LookupResults == {"None", "Blocked", "Observed", "Attached", "Miss", "Stale"}
NoPending == MaxEpoch + 1
OptionEpoch == 0..NoPending

VARIABLES poolState, epoch, ownerLive, metadataInit, refs, probeRefs,
          pendingLookupEpoch, lastLookup

vars == <<poolState, epoch, ownerLive, metadataInit, refs, probeRefs,
          pendingLookupEpoch, lastLookup>>

Init ==
    /\ poolState = "Free"
    /\ epoch = 0
    /\ ownerLive = FALSE
    /\ metadataInit = FALSE
    /\ refs = 0
    /\ probeRefs = 0
    /\ pendingLookupEpoch = NoPending
    /\ lastLookup = "None"

\* The worker claims a pool slot before target metadata and the owner ref are
\* visible to probes.  This state corresponds to SLOT_INITIALIZING.
BeginInitialize ==
    /\ poolState = "Free"
    /\ epoch < MaxEpoch
    /\ poolState' = "Initializing"
    /\ epoch' = epoch + 1
    /\ ownerLive' = FALSE
    /\ metadataInit' = FALSE
    /\ refs' = 0
    /\ probeRefs' = 0
    /\ pendingLookupEpoch' = pendingLookupEpoch
    /\ lastLookup' = "None"

InstallMetadata ==
    /\ poolState = "Initializing"
    /\ ~metadataInit
    /\ metadataInit' = TRUE
    /\ UNCHANGED <<poolState, epoch, ownerLive, refs, probeRefs,
                  pendingLookupEpoch, lastLookup>>

\* Publication is the linearization point for lookup visibility.  Rust stores a
\* packed SLOT_ALLOCATED word that includes the owner ref and publication epoch
\* only after target metadata is initialized.
PublishAllocated ==
    /\ poolState = "Initializing"
    /\ metadataInit
    /\ poolState' = "Allocated"
    /\ ownerLive' = TRUE
    /\ refs' = 1
    /\ lastLookup' = "None"
    /\ UNCHANGED <<epoch, metadataInit, probeRefs, pendingLookupEpoch>>

RollbackInitialize ==
    /\ poolState = "Initializing"
    /\ poolState' = "Free"
    /\ ownerLive' = FALSE
    /\ metadataInit' = FALSE
    /\ refs' = 0
    /\ probeRefs' = 0
    /\ lastLookup' = "None"
    /\ UNCHANGED <<epoch, pendingLookupEpoch>>

\* A lookup may observe an allocated slot before attempting the refcount CAS.
\* The observed epoch must match at the CAS point or the lookup is stale.
ObserveAllocated ==
    /\ pendingLookupEpoch = NoPending
    /\ poolState = "Allocated"
    /\ pendingLookupEpoch' = epoch
    /\ lastLookup' = "Observed"
    /\ UNCHANGED <<poolState, epoch, ownerLive, metadataInit, refs, probeRefs>>

AttachObservedLookup ==
    /\ pendingLookupEpoch # NoPending
    /\ poolState = "Allocated"
    /\ epoch = pendingLookupEpoch
    /\ probeRefs < MaxProbeRefs
    /\ refs' = refs + 1
    /\ probeRefs' = probeRefs + 1
    /\ pendingLookupEpoch' = NoPending
    /\ lastLookup' = "Attached"
    /\ UNCHANGED <<poolState, epoch, ownerLive, metadataInit>>

StaleObservedLookup ==
    /\ pendingLookupEpoch # NoPending
    /\ \/ poolState # "Allocated"
       \/ epoch # pendingLookupEpoch
    /\ pendingLookupEpoch' = NoPending
    /\ lastLookup' = "Stale"
    /\ UNCHANGED <<poolState, epoch, ownerLive, metadataInit, refs, probeRefs>>

\* A non-matching lookup that observes the current publication may acquire and
\* release a temporary ref with no net abstract refcount change.
LookupMiss ==
    /\ pendingLookupEpoch = NoPending
    /\ poolState = "Allocated"
    /\ lastLookup' = "Miss"
    /\ UNCHANGED <<poolState, epoch, ownerLive, metadataInit, refs, probeRefs,
                  pendingLookupEpoch>>

LookupBlocked ==
    /\ pendingLookupEpoch = NoPending
    /\ poolState # "Allocated"
    /\ lastLookup' = "Blocked"
    /\ UNCHANGED <<poolState, epoch, ownerLive, metadataInit, refs, probeRefs,
                  pendingLookupEpoch>>

ReleaseOwner ==
    /\ ownerLive
    /\ refs > 0
    /\ poolState = "Allocated"
    /\ ownerLive' = FALSE
    /\ refs' = refs - 1
    /\ poolState' = "Retiring"
    /\ lastLookup' = "None"
    /\ UNCHANGED <<epoch, metadataInit, probeRefs, pendingLookupEpoch>>

ReleaseProbe ==
    /\ probeRefs > 0
    /\ refs' = refs - 1
    /\ probeRefs' = probeRefs - 1
    /\ lastLookup' = "None"
    /\ UNCHANGED <<poolState, epoch, ownerLive, metadataInit,
                  pendingLookupEpoch>>

FinishRetire ==
    /\ poolState = "Retiring"
    /\ refs = 0
    /\ probeRefs = 0
    /\ ~ownerLive
    /\ poolState' = "Free"
    /\ metadataInit' = FALSE
    /\ lastLookup' = "None"
    /\ UNCHANGED <<epoch, ownerLive, refs, probeRefs, pendingLookupEpoch>>

Next ==
    \/ BeginInitialize
    \/ InstallMetadata
    \/ PublishAllocated
    \/ RollbackInitialize
    \/ ObserveAllocated
    \/ AttachObservedLookup
    \/ StaleObservedLookup
    \/ LookupMiss
    \/ LookupBlocked
    \/ ReleaseOwner
    \/ ReleaseProbe
    \/ FinishRetire

TypeInvariant ==
    /\ poolState \in PoolStates
    /\ epoch \in 0..MaxEpoch
    /\ ownerLive \in BOOLEAN
    /\ metadataInit \in BOOLEAN
    /\ refs \in 0..(MaxProbeRefs + 1)
    /\ probeRefs \in 0..MaxProbeRefs
    /\ pendingLookupEpoch \in OptionEpoch
    /\ lastLookup \in LookupResults

RefAccounting ==
    refs = probeRefs + (IF ownerLive THEN 1 ELSE 0)

FreeClean ==
    poolState = "Free" => /\ refs = 0
                          /\ probeRefs = 0
                          /\ ~ownerLive
                          /\ ~metadataInit

InitializingUnprobeable ==
    poolState = "Initializing" => probeRefs = 0

PublishedMetadata ==
    poolState \in {"Allocated", "Retiring"} => metadataInit

AllocatedHasOwner ==
    poolState = "Allocated" => /\ ownerLive
                               /\ refs >= 1

RetiringHasNoOwner ==
    poolState = "Retiring" => /\ ~ownerLive
                              /\ refs = probeRefs

LookupAttachOnlyCurrentPublication ==
    lastLookup = "Attached" => /\ poolState = "Allocated"
                               /\ pendingLookupEpoch = NoPending

Spec == Init /\ [][Next]_vars

====
