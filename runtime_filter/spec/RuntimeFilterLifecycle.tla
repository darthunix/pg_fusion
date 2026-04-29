---- MODULE RuntimeFilterLifecycle ----
EXTENDS Naturals, FiniteSets

CONSTANTS Keys, Builders, MaxGeneration, MaxProbes

States == {"Free", "Building", "Ready", "Disabled"}
Owners == Builders \cup {"None"}
Decisions == {"Pass", "Maybe", "Reject"}

VARIABLES state, generation, owner, payload, expectedGeneration, probeKey, decision, activeProbes

vars == <<state, generation, owner, payload, expectedGeneration, probeKey, decision, activeProbes>>

Init ==
    /\ state = "Free"
    /\ generation = 0
    /\ owner = "None"
    /\ payload = {}
    /\ expectedGeneration \in 0..MaxGeneration
    /\ probeKey \in Keys
    /\ decision = "Pass"
    /\ activeProbes = 0

AcquireBuild(b) ==
    /\ b \in Builders
    /\ state \in {"Free", "Disabled"}
    /\ generation < MaxGeneration
    /\ activeProbes = 0
    /\ state' = "Building"
    /\ generation' = generation + 1
    /\ owner' = b
    /\ payload' = {}
    /\ decision' = "Pass"
    /\ UNCHANGED <<expectedGeneration, probeKey, activeProbes>>

InsertKey(b) ==
    /\ b \in Builders
    /\ state = "Building"
    /\ owner = b
    /\ \E k \in Keys: payload' = payload \cup {k}
    /\ UNCHANGED <<state, generation, owner, expectedGeneration, probeKey, decision, activeProbes>>

PublishReady(b) ==
    /\ b \in Builders
    /\ state = "Building"
    /\ owner = b
    /\ state' = "Ready"
    /\ owner' = "None"
    /\ UNCHANGED <<generation, payload, expectedGeneration, probeKey, decision, activeProbes>>

DisableBuilder(b) ==
    /\ b \in Builders
    /\ state = "Building"
    /\ owner = b
    /\ state' = "Disabled"
    /\ owner' = "None"
    /\ decision' = "Pass"
    /\ UNCHANGED <<generation, payload, expectedGeneration, probeKey, activeProbes>>

\* Retiring a ready filter is only valid after external quiescence: no old
\* probe may already have observed Ready and still be about to read payload.
RetireReady ==
    /\ state = "Ready"
    /\ activeProbes = 0
    /\ state' = "Disabled"
    /\ decision' = "Pass"
    /\ UNCHANGED <<generation, owner, payload, expectedGeneration, probeKey, activeProbes>>

AttachProbe ==
    /\ activeProbes < MaxProbes
    /\ activeProbes' = activeProbes + 1
    /\ expectedGeneration' = generation
    /\ probeKey' \in Keys
    /\ decision' = "Pass"
    /\ UNCHANGED <<state, generation, owner, payload>>

DetachProbe ==
    /\ activeProbes > 0
    /\ activeProbes' = activeProbes - 1
    /\ decision' = "Pass"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey>>

ChangeProbe ==
    /\ expectedGeneration' \in 0..MaxGeneration
    /\ probeKey' \in Keys
    /\ decision' = "Pass"
    /\ UNCHANGED <<state, generation, owner, payload, activeProbes>>

ProbePass ==
    /\ activeProbes > 0
    /\ state # "Ready" \/ expectedGeneration # generation
    /\ decision' = "Pass"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey, activeProbes>>

ProbeMaybe ==
    /\ activeProbes > 0
    /\ state = "Ready"
    /\ expectedGeneration = generation
    /\ probeKey \in payload
    /\ decision' = "Maybe"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey, activeProbes>>

ProbeReject ==
    /\ activeProbes > 0
    /\ state = "Ready"
    /\ expectedGeneration = generation
    /\ probeKey \notin payload
    /\ decision' = "Reject"
    /\ UNCHANGED <<state, generation, owner, payload, expectedGeneration, probeKey, activeProbes>>

Next ==
    \/ \E b \in Builders: AcquireBuild(b)
    \/ \E b \in Builders: InsertKey(b)
    \/ \E b \in Builders: PublishReady(b)
    \/ \E b \in Builders: DisableBuilder(b)
    \/ RetireReady
    \/ AttachProbe
    \/ DetachProbe
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
    /\ activeProbes \in 0..MaxProbes

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
