---- MODULE RuntimeFilterLifecycle ----
EXTENDS Naturals, FiniteSets

CONSTANTS Keys, Builders, MaxGeneration

States == {"Free", "Building", "Ready", "Disabled"}
Owners == Builders \cup {"None"}
Decisions == {"Pass", "Maybe", "Reject"}

VARIABLES state, generation, owner, payload, expectedGeneration, probeKey, decision

vars == <<state, generation, owner, payload, expectedGeneration, probeKey, decision>>

Init ==
    /\ state = "Free"
    /\ generation = 0
    /\ owner = "None"
    /\ payload = {}
    /\ expectedGeneration \in 0..MaxGeneration
    /\ probeKey \in Keys
    /\ decision = "Pass"

AcquireBuild(b) ==
    /\ b \in Builders
    /\ state \in {"Free", "Disabled"}
    /\ generation < MaxGeneration
    /\ state' = "Building"
    /\ generation' = generation + 1
    /\ owner' = b
    /\ payload' = {}
    /\ decision' = "Pass"
    /\ UNCHANGED <<expectedGeneration, probeKey>>

InsertKey(b) ==
    /\ b \in Builders
    /\ state = "Building"
    /\ owner = b
    /\ \E k \in Keys: payload' = payload \cup {k}
    /\ UNCHANGED <<state, generation, owner, expectedGeneration, probeKey, decision>>

PublishReady(b) ==
    /\ b \in Builders
    /\ state = "Building"
    /\ owner = b
    /\ state' = "Ready"
    /\ owner' = "None"
    /\ UNCHANGED <<generation, payload, expectedGeneration, probeKey, decision>>

DisableBuilder(b) ==
    /\ b \in Builders
    /\ state = "Building"
    /\ owner = b
    /\ state' = "Disabled"
    /\ owner' = "None"
    /\ decision' = "Pass"
    /\ UNCHANGED <<generation, payload, expectedGeneration, probeKey>>

\* Retiring a ready filter is only valid after external quiescence: no old
\* probe may already have observed Ready and still be about to read payload.
RetireReady ==
    /\ state = "Ready"
    /\ state' = "Disabled"
    /\ decision' = "Pass"
    /\ UNCHANGED <<generation, owner, payload, expectedGeneration, probeKey>>

ChangeProbe ==
    /\ expectedGeneration' \in 0..MaxGeneration
    /\ probeKey' \in Keys
    /\ decision' = "Pass"
    /\ UNCHANGED <<state, generation, owner, payload>>

ProbePass ==
    /\ state # "Ready" \/ expectedGeneration # generation
    /\ decision' = "Pass"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey>>

ProbeMaybe ==
    /\ state = "Ready"
    /\ expectedGeneration = generation
    /\ probeKey \in payload
    /\ decision' = "Maybe"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey>>

ProbeReject ==
    /\ state = "Ready"
    /\ expectedGeneration = generation
    /\ probeKey \notin payload
    /\ decision' = "Reject"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey>>

Next ==
    \/ \E b \in Builders: AcquireBuild(b)
    \/ \E b \in Builders: InsertKey(b)
    \/ \E b \in Builders: PublishReady(b)
    \/ \E b \in Builders: DisableBuilder(b)
    \/ RetireReady
    \/ ChangeProbe
    \/ ProbePass
    \/ ProbeMaybe
    \/ ProbeReject

TypeInvariant ==
    /\ state \in States
    /\ generation \in 0..MaxGeneration
    /\ owner \in Owners
    /\ payload \subseteq Keys
    /\ expectedGeneration \in 0..MaxGeneration
    /\ probeKey \in Keys
    /\ decision \in Decisions

OwnerMatchesBuilding ==
    (state = "Building") <=> (owner \in Builders)

NoReadyOwner ==
    state = "Ready" => owner = "None"

NoRejectBeforeReady ==
    decision = "Reject" => state = "Ready"

StaleGenerationIgnored ==
    expectedGeneration # generation => decision # "Reject"

NoFalseNegativeAfterReady ==
    /\ state = "Ready"
    /\ expectedGeneration = generation
    /\ probeKey \in payload
    => decision # "Reject"

Spec == Init /\ [][Next]_vars

====
