---- MODULE SingleSlotBridge ----
EXTENDS Naturals

WorkerStates == {"Offline", "Online"}
UseOutcomes == {"None", "Visible", "Empty", "Stale"}

VARIABLES
    regionGen,
    workerState,
    leased,
    slotGen,
    leaseEpoch,
    nextLeaseEpoch,
    backendOwned,
    workerOwned,
    backendAlive,
    workerAlive,
    residueEpoch,
    backendHandleEpoch,
    workerHandleEpoch,
    staleBackendHandleEpoch,
    staleWorkerHandleEpoch,
    lastBackendUse,
    lastWorkerUse

vars ==
    << regionGen,
       workerState,
       leased,
       slotGen,
       leaseEpoch,
       nextLeaseEpoch,
       backendOwned,
       workerOwned,
       backendAlive,
       workerAlive,
       residueEpoch,
       backendHandleEpoch,
       workerHandleEpoch,
       staleBackendHandleEpoch,
       staleWorkerHandleEpoch,
       lastBackendUse,
       lastWorkerUse >>

(*
--algorithm SingleSlotBridge
variables
  regionGen = 0,
  workerState = "Offline",
  leased = FALSE,
  slotGen = 0,
  leaseEpoch = 0,
  nextLeaseEpoch = 1,
  backendOwned = FALSE,
  workerOwned = FALSE,
  backendAlive = FALSE,
  workerAlive = FALSE,
  residueEpoch = 0,
  backendHandleEpoch = 0,
  workerHandleEpoch = 0,
  staleBackendHandleEpoch = 0,
  staleWorkerHandleEpoch = 0,
  lastBackendUse = "None",
  lastWorkerUse = "None";

\* The bridge remembers only the most recent stale handle per side.
process Main = "main"
begin
Loop:
  while TRUE do
    either
      await workerState = "Offline";
      regionGen := regionGen + 1;
      workerState := "Online";
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await workerState = "Online";
      regionGen := regionGen + 1;
      workerState := "Offline";
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await workerState = "Online" /\ ~leased;
      leased := TRUE;
      slotGen := regionGen;
      leaseEpoch := nextLeaseEpoch;
      nextLeaseEpoch := nextLeaseEpoch + 1;
      backendOwned := TRUE;
      workerOwned := FALSE;
      backendAlive := TRUE;
      workerAlive := FALSE;
      residueEpoch := 0;
      backendHandleEpoch := leaseEpoch;
      workerHandleEpoch := 0;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await workerState = "Online"
         /\ leased
         /\ backendOwned
         /\ backendAlive
         /\ ~workerOwned
         /\ slotGen = regionGen;
      workerOwned := TRUE;
      workerAlive := TRUE;
      workerHandleEpoch := leaseEpoch;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await leased /\ leaseEpoch # 0 /\ slotGen = regionGen
         /\ ((backendOwned /\ backendAlive) \/ (workerOwned /\ workerAlive));
      residueEpoch := leaseEpoch;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await leased /\ leaseEpoch # 0 /\ slotGen = regionGen
         /\ residueEpoch = leaseEpoch
         /\ ((backendOwned /\ backendAlive) \/ (workerOwned /\ workerAlive));
      residueEpoch := 0;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await backendOwned /\ backendAlive;
      backendOwned := FALSE;
      backendAlive := FALSE;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await backendOwned /\ backendAlive;
      backendAlive := FALSE;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await backendOwned /\ ~backendAlive;
      backendOwned := FALSE;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await workerOwned /\ workerAlive;
      workerOwned := FALSE;
      workerAlive := FALSE;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await workerOwned /\ workerAlive;
      workerAlive := FALSE;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await workerOwned /\ ~workerAlive;
      workerOwned := FALSE;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await leased /\ ~backendOwned /\ ~workerOwned;
      staleBackendHandleEpoch := backendHandleEpoch;
      staleWorkerHandleEpoch := workerHandleEpoch;
      leased := FALSE;
      slotGen := 0;
      leaseEpoch := 0;
      backendAlive := FALSE;
      workerAlive := FALSE;
      residueEpoch := 0;
      backendHandleEpoch := 0;
      workerHandleEpoch := 0;
      lastBackendUse := "None";
      lastWorkerUse := "None";

    or
      await backendHandleEpoch # 0;
      if leased /\ leaseEpoch # 0 /\ slotGen = regionGen
         /\ backendOwned /\ backendAlive
         /\ backendHandleEpoch = leaseEpoch then
        if residueEpoch = leaseEpoch then
          lastBackendUse := "Visible";
        else
          lastBackendUse := "Empty";
        end if;
      else
        lastBackendUse := "Stale";
      end if;
      lastWorkerUse := "None";

    or
      await workerHandleEpoch # 0;
      if leased /\ leaseEpoch # 0 /\ slotGen = regionGen
         /\ workerOwned /\ workerAlive
         /\ workerHandleEpoch = leaseEpoch then
        if residueEpoch = leaseEpoch then
          lastWorkerUse := "Visible";
        else
          lastWorkerUse := "Empty";
        end if;
      else
        lastWorkerUse := "Stale";
      end if;
      lastBackendUse := "None";

    or
      await staleBackendHandleEpoch # 0 /\ staleBackendHandleEpoch # leaseEpoch;
      lastBackendUse := "Stale";
      lastWorkerUse := "None";

    or
      await staleWorkerHandleEpoch # 0 /\ staleWorkerHandleEpoch # leaseEpoch;
      lastWorkerUse := "Stale";
      lastBackendUse := "None";

    end either;
  end while;
end process;
end algorithm;
*)

Init ==
    /\ regionGen = 0
    /\ workerState = "Offline"
    /\ leased = FALSE
    /\ slotGen = 0
    /\ leaseEpoch = 0
    /\ nextLeaseEpoch = 1
    /\ backendOwned = FALSE
    /\ workerOwned = FALSE
    /\ backendAlive = FALSE
    /\ workerAlive = FALSE
    /\ residueEpoch = 0
    /\ backendHandleEpoch = 0
    /\ workerHandleEpoch = 0
    /\ staleBackendHandleEpoch = 0
    /\ staleWorkerHandleEpoch = 0
    /\ lastBackendUse = "None"
    /\ lastWorkerUse = "None"

CurrentIncarnation ==
    leased /\ leaseEpoch # 0 /\ slotGen = regionGen

LiveBackendOwner ==
    backendOwned /\ backendAlive

LiveWorkerOwner ==
    workerOwned /\ workerAlive

AnyLiveOwner ==
    LiveBackendOwner \/ LiveWorkerOwner

CurrentBackendHandleUsable ==
    /\ CurrentIncarnation
    /\ LiveBackendOwner
    /\ backendHandleEpoch # 0
    /\ backendHandleEpoch = leaseEpoch

CurrentWorkerHandleUsable ==
    /\ CurrentIncarnation
    /\ LiveWorkerOwner
    /\ workerHandleEpoch # 0
    /\ workerHandleEpoch = leaseEpoch

BackendUseOutcome ==
    IF CurrentBackendHandleUsable
        THEN IF residueEpoch = leaseEpoch THEN "Visible" ELSE "Empty"
        ELSE "Stale"

WorkerUseOutcome ==
    IF CurrentWorkerHandleUsable
        THEN IF residueEpoch = leaseEpoch THEN "Visible" ELSE "Empty"
        ELSE "Stale"

ResetUseObservations ==
    /\ lastBackendUse' = "None"
    /\ lastWorkerUse' = "None"

ActivateGeneration ==
    /\ workerState = "Offline"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Online"
    /\ ResetUseObservations
    /\ UNCHANGED << leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

DeactivateGeneration ==
    /\ workerState = "Online"
    /\ regionGen' = regionGen + 1
    /\ workerState' = "Offline"
    /\ ResetUseObservations
    /\ UNCHANGED << leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

AcquireBackend ==
    /\ workerState = "Online"
    /\ ~leased
    /\ leased' = TRUE
    /\ slotGen' = regionGen
    /\ leaseEpoch' = nextLeaseEpoch
    /\ nextLeaseEpoch' = nextLeaseEpoch + 1
    /\ backendOwned' = TRUE
    /\ workerOwned' = FALSE
    /\ backendAlive' = TRUE
    /\ workerAlive' = FALSE
    /\ residueEpoch' = 0
    /\ backendHandleEpoch' = nextLeaseEpoch
    /\ workerHandleEpoch' = 0
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

ClaimWorker ==
    /\ workerState = "Online"
    /\ leased
    /\ LiveBackendOwner
    /\ ~workerOwned
    /\ CurrentIncarnation
    /\ workerOwned' = TRUE
    /\ workerAlive' = TRUE
    /\ workerHandleEpoch' = leaseEpoch
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    backendAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

CreateResidue ==
    /\ CurrentIncarnation
    /\ AnyLiveOwner
    /\ residueEpoch' = leaseEpoch
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

ClearResidue ==
    /\ CurrentIncarnation
    /\ AnyLiveOwner
    /\ residueEpoch = leaseEpoch
    /\ residueEpoch' = 0
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

ReleaseBackend ==
    /\ backendOwned
    /\ backendAlive
    /\ backendOwned' = FALSE
    /\ backendAlive' = FALSE
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    workerOwned,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

CrashBackend ==
    /\ backendOwned
    /\ backendAlive
    /\ backendAlive' = FALSE
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

ReapDeadBackendOwner ==
    /\ backendOwned
    /\ ~backendAlive
    /\ backendOwned' = FALSE
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

ReleaseWorker ==
    /\ workerOwned
    /\ workerAlive
    /\ workerOwned' = FALSE
    /\ workerAlive' = FALSE
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    backendAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

CrashWorker ==
    /\ workerOwned
    /\ workerAlive
    /\ workerAlive' = FALSE
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

ReapDeadWorkerOwner ==
    /\ workerOwned
    /\ ~workerAlive
    /\ workerOwned' = FALSE
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

Finalize ==
    /\ leased
    /\ ~backendOwned
    /\ ~workerOwned
    /\ staleBackendHandleEpoch' = backendHandleEpoch
    /\ staleWorkerHandleEpoch' = workerHandleEpoch
    /\ leased' = FALSE
    /\ slotGen' = 0
    /\ leaseEpoch' = 0
    /\ backendAlive' = FALSE
    /\ workerAlive' = FALSE
    /\ residueEpoch' = 0
    /\ backendHandleEpoch' = 0
    /\ workerHandleEpoch' = 0
    /\ ResetUseObservations
    /\ UNCHANGED << regionGen,
                    workerState,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned >>

UseCurrentBackendHandle ==
    /\ backendHandleEpoch # 0
    /\ lastBackendUse' = BackendUseOutcome
    /\ lastWorkerUse' = "None"
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

UseCurrentWorkerHandle ==
    /\ workerHandleEpoch # 0
    /\ lastWorkerUse' = WorkerUseOutcome
    /\ lastBackendUse' = "None"
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

UseStaleBackendHandle ==
    /\ staleBackendHandleEpoch # 0
    /\ staleBackendHandleEpoch # leaseEpoch
    /\ lastBackendUse' = "Stale"
    /\ lastWorkerUse' = "None"
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

UseStaleWorkerHandle ==
    /\ staleWorkerHandleEpoch # 0
    /\ staleWorkerHandleEpoch # leaseEpoch
    /\ lastWorkerUse' = "Stale"
    /\ lastBackendUse' = "None"
    /\ UNCHANGED << regionGen,
                    workerState,
                    leased,
                    slotGen,
                    leaseEpoch,
                    nextLeaseEpoch,
                    backendOwned,
                    workerOwned,
                    backendAlive,
                    workerAlive,
                    residueEpoch,
                    backendHandleEpoch,
                    workerHandleEpoch,
                    staleBackendHandleEpoch,
                    staleWorkerHandleEpoch >>

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
    \/ UseCurrentBackendHandle
    \/ UseCurrentWorkerHandle
    \/ UseStaleBackendHandle
    \/ UseStaleWorkerHandle

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ regionGen \in Nat
    /\ workerState \in WorkerStates
    /\ leased \in BOOLEAN
    /\ slotGen \in Nat
    /\ leaseEpoch \in Nat
    /\ nextLeaseEpoch \in Nat
    /\ nextLeaseEpoch > 0
    /\ backendOwned \in BOOLEAN
    /\ workerOwned \in BOOLEAN
    /\ backendAlive \in BOOLEAN
    /\ workerAlive \in BOOLEAN
    /\ residueEpoch \in Nat
    /\ backendHandleEpoch \in Nat
    /\ workerHandleEpoch \in Nat
    /\ staleBackendHandleEpoch \in Nat
    /\ staleWorkerHandleEpoch \in Nat
    /\ lastBackendUse \in UseOutcomes
    /\ lastWorkerUse \in UseOutcomes

LeaseEpochMatchesLeaseState ==
    /\ leased => leaseEpoch # 0
    /\ ~leased => leaseEpoch = 0

NextLeaseEpochAhead ==
    nextLeaseEpoch > leaseEpoch

OwnersRequireLease ==
    (backendOwned \/ workerOwned) => leased

LiveOwnerRequiresOwnedBit ==
    /\ backendAlive => backendOwned
    /\ workerAlive => workerOwned

OwnedSideCarriesCurrentHandle ==
    /\ backendOwned => backendHandleEpoch = leaseEpoch
    /\ workerOwned => workerHandleEpoch = leaseEpoch

ResidueRequiresLease ==
    residueEpoch # 0 => leased

ResidueMatchesCurrentLease ==
    residueEpoch # 0 => residueEpoch = leaseEpoch

FreeSlotIsEmpty ==
    ~leased =>
      /\ slotGen = 0
      /\ leaseEpoch = 0
      /\ residueEpoch = 0
      /\ backendHandleEpoch = 0
      /\ workerHandleEpoch = 0

VisibleMeansCurrentIncarnation ==
    /\ lastBackendUse = "Visible" =>
         /\ CurrentIncarnation
         /\ residueEpoch = leaseEpoch
         /\ LiveBackendOwner
         /\ backendHandleEpoch = leaseEpoch
    /\ lastWorkerUse = "Visible" =>
         /\ CurrentIncarnation
         /\ residueEpoch = leaseEpoch
         /\ LiveWorkerOwner
         /\ workerHandleEpoch = leaseEpoch

StaleHandleEpochDiffersFromCurrent ==
    leased =>
      /\ staleBackendHandleEpoch = 0 \/ staleBackendHandleEpoch # leaseEpoch
      /\ staleWorkerHandleEpoch = 0 \/ staleWorkerHandleEpoch # leaseEpoch

TlcSmokeBound ==
    /\ regionGen <= 3
    /\ nextLeaseEpoch <= 4

====
