---- MODULE SingleSlotLifecycle ----
EXTENDS Naturals

WorkerStates == {"Offline", "Online"}

VARIABLES
    regionGen,
    workerState,
    leased,
    backendOwned,
    workerOwned,
    backendAlive,
    workerAlive,
    slotGen,
    hasResidue

vars ==
    << regionGen,
       workerState,
       leased,
       backendOwned,
       workerOwned,
       backendAlive,
       workerAlive,
       slotGen,
       hasResidue >>

(*
--algorithm SingleSlotLifecycle
variables
  regionGen = 0,
  workerState = "Offline",
  leased = FALSE,
  backendOwned = FALSE,
  workerOwned = FALSE,
  backendAlive = FALSE,
  workerAlive = FALSE,
  slotGen = 0,
  hasResidue = FALSE;

process Main = "main"
begin
Loop:
  while TRUE do
    either
      await workerState = "Offline";
      regionGen := regionGen + 1;
      workerState := "Online";

    or
      await workerState = "Online";
      regionGen := regionGen + 1;
      workerState := "Offline";

    or
      await workerState = "Online" /\ ~leased;
      leased := TRUE;
      backendOwned := TRUE;
      workerOwned := FALSE;
      backendAlive := TRUE;
      workerAlive := FALSE;
      slotGen := regionGen;
      hasResidue := FALSE;

    or
      await workerState = "Online"
         /\ leased
         /\ backendOwned
         /\ backendAlive
         /\ ~workerOwned
         /\ slotGen = regionGen;
      workerOwned := TRUE;
      workerAlive := TRUE;

    or
      await leased /\ slotGen = regionGen
         /\ ((backendOwned /\ backendAlive) \/ (workerOwned /\ workerAlive));
      hasResidue := TRUE;

    or
      await leased /\ slotGen = regionGen /\ hasResidue
         /\ ((backendOwned /\ backendAlive) \/ (workerOwned /\ workerAlive));
      hasResidue := FALSE;

    or
      await backendOwned /\ backendAlive;
      backendOwned := FALSE;
      backendAlive := FALSE;

    or
      await backendOwned /\ backendAlive;
      backendAlive := FALSE;

    or
      await backendOwned /\ ~backendAlive;
      backendOwned := FALSE;

    or
      await workerOwned /\ workerAlive;
      workerOwned := FALSE;
      workerAlive := FALSE;

    or
      await workerOwned /\ workerAlive;
      workerAlive := FALSE;

    or
      await workerOwned /\ ~workerAlive;
      workerOwned := FALSE;

    or
      await leased /\ ~backendOwned /\ ~workerOwned;
      leased := FALSE;
      backendAlive := FALSE;
      workerAlive := FALSE;
      slotGen := 0;
      hasResidue := FALSE;

    end either;
  end while;
end process;
end algorithm;
*)

Init ==
    /\ regionGen = 0
    /\ workerState = "Offline"
    /\ leased = FALSE
    /\ backendOwned = FALSE
    /\ workerOwned = FALSE
    /\ backendAlive = FALSE
    /\ workerAlive = FALSE
    /\ slotGen = 0
    /\ hasResidue = FALSE

CurrentIncarnation ==
    leased /\ slotGen = regionGen

LiveBackendOwner ==
    backendOwned /\ backendAlive

LiveWorkerOwner ==
    workerOwned /\ workerAlive

AnyLiveOwner ==
    LiveBackendOwner \/ LiveWorkerOwner

ActivateGeneration ==
    /\ workerState = "Offline"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Online"
    /\ UNCHANGED << leased, backendOwned, workerOwned, backendAlive, workerAlive, slotGen, hasResidue >>

DeactivateGeneration ==
    \* `workerState` is a global availability gate; stale per-slot owners can
    \* persist until explicit reap/finalize actions run.
    /\ workerState = "Online"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Offline"
    /\ UNCHANGED << leased, backendOwned, workerOwned, backendAlive, workerAlive, slotGen, hasResidue >>

AcquireBackend ==
    /\ workerState = "Online"
    /\ ~leased
    /\ leased' = TRUE
    /\ backendOwned' = TRUE
    /\ workerOwned' = FALSE
    /\ backendAlive' = TRUE
    /\ workerAlive' = FALSE
    /\ slotGen' = regionGen
    /\ hasResidue' = FALSE
    /\ UNCHANGED << regionGen, workerState >>

ClaimWorker ==
    /\ workerState = "Online"
    /\ leased
    /\ LiveBackendOwner
    /\ ~workerOwned
    /\ slotGen = regionGen
    /\ workerOwned' = TRUE
    /\ workerAlive' = TRUE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, backendAlive, slotGen, hasResidue >>

CreateResidue ==
    /\ CurrentIncarnation
    /\ AnyLiveOwner
    /\ ~hasResidue
    /\ hasResidue' = TRUE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, workerOwned, backendAlive, workerAlive, slotGen >>

ClearResidue ==
    /\ CurrentIncarnation
    /\ AnyLiveOwner
    /\ hasResidue
    /\ hasResidue' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, workerOwned, backendAlive, workerAlive, slotGen >>

ReleaseBackend ==
    /\ backendOwned
    /\ backendAlive
    /\ backendOwned' = FALSE
    /\ backendAlive' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, workerOwned, workerAlive, slotGen, hasResidue >>

CrashBackend ==
    /\ backendOwned
    /\ backendAlive
    /\ backendAlive' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, workerOwned, workerAlive, slotGen, hasResidue >>

ReapDeadBackendOwner ==
    /\ backendOwned
    /\ ~backendAlive
    /\ backendOwned' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, workerOwned, backendAlive, workerAlive, slotGen, hasResidue >>

ReleaseWorker ==
    /\ workerOwned
    /\ workerAlive
    /\ workerOwned' = FALSE
    /\ workerAlive' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, backendAlive, slotGen, hasResidue >>

CrashWorker ==
    /\ workerOwned
    /\ workerAlive
    /\ workerAlive' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, workerOwned, backendAlive, slotGen, hasResidue >>

ReapDeadWorkerOwner ==
    /\ workerOwned
    /\ ~workerAlive
    /\ workerOwned' = FALSE
    /\ UNCHANGED << regionGen, workerState, leased, backendOwned, backendAlive, workerAlive, slotGen, hasResidue >>

Finalize ==
    /\ leased
    /\ ~backendOwned
    /\ ~workerOwned
    /\ leased' = FALSE
    /\ slotGen' = 0
    /\ hasResidue' = FALSE
    /\ UNCHANGED << regionGen,
                    workerState,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive >>

Next ==
    \/ ActivateGeneration
    \/ DeactivateGeneration
    \/ AcquireBackend
    \/ ClaimWorker
    \/ CreateResidue
    \/ ClearResidue
    \/ ReleaseBackend
    \/ CrashBackend
    \/ ReapDeadBackendOwner
    \/ ReleaseWorker
    \/ CrashWorker
    \/ ReapDeadWorkerOwner
    \/ Finalize

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ leased \in BOOLEAN
    /\ backendOwned \in BOOLEAN
    /\ workerOwned \in BOOLEAN
    /\ backendAlive \in BOOLEAN
    /\ workerAlive \in BOOLEAN
    /\ slotGen \in Nat
    /\ hasResidue \in BOOLEAN

OwnersRequireLease ==
    (backendOwned \/ workerOwned) => leased

LiveOwnerRequiresOwnedBit ==
    /\ backendAlive => backendOwned
    /\ workerAlive => workerOwned

ResidueRequiresLease ==
    hasResidue => leased

SlotGenerationBounded ==
    slotGen <= regionGen

ReusableSlotIsClean ==
    ~leased =>
      /\ ~backendOwned
      /\ ~workerOwned
      /\ ~backendAlive
      /\ ~workerAlive
      /\ slotGen = 0
      /\ ~hasResidue

TlcSmokeBound ==
    regionGen <= 3

====
