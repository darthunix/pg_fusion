---- MODULE SingleSlotTransport ----
EXTENDS Naturals, Sequences

CONSTANTS CapBytes, FramePrefixBytes, M1Bytes, M2Bytes

EndpointStates == {"Detached", "Alive", "Dead"}
NotifyStates == {"Notified", "PeerMissing"}
LastOutcomeStates == {"None"} \union NotifyStates
Directions == {"B2W", "W2B"}
MsgSet == {"m1", "m2"}

RECURSIVE QueueBytes(_)

CanTransition(from, to) ==
    \/ /\ from = "Detached"
       /\ to = "Alive"
    \/ /\ from = "Alive"
       /\ to \in {"Detached", "Dead"}

SenderEp(dir, backend, worker) ==
    IF dir = "B2W" THEN backend ELSE worker

ReceiverEp(dir, backend, worker) ==
    IF dir = "B2W" THEN worker ELSE backend

NotifyOutcome(dir, backend, worker) ==
    IF ReceiverEp(dir, backend, worker) = "Alive"
        THEN "Notified"
        ELSE "PeerMissing"

FrameBytes(msg) ==
    IF msg = "m1"
        THEN FramePrefixBytes + M1Bytes
        ELSE FramePrefixBytes + M2Bytes

QueueBytes(q) ==
    IF Len(q) = 0
        THEN 0
        ELSE FrameBytes(Head(q)) + QueueBytes(Tail(q))

ASSUME
    /\ CapBytes \in Nat
    /\ FramePrefixBytes \in Nat
    /\ FramePrefixBytes > 0
    /\ M1Bytes \in Nat
    /\ M2Bytes \in Nat
    /\ M1Bytes > 0
    /\ M2Bytes > 0
    /\ FrameBytes("m1") <= CapBytes
    /\ FrameBytes("m2") <= CapBytes

CanSend(dir, msg, backend, worker, queuesByDir) ==
    /\ dir \in Directions
    /\ msg \in MsgSet
    /\ SenderEp(dir, backend, worker) = "Alive"
    /\ QueueBytes(queuesByDir[dir]) + FrameBytes(msg) <= CapBytes

CanRecv(dir, backend, worker, queuesByDir) ==
    /\ dir \in Directions
    /\ ReceiverEp(dir, backend, worker) = "Alive"
    /\ Len(queuesByDir[dir]) > 0

VARIABLES
    backendEp,
    workerEp,
    queues,
    lastSendOutcome

vars ==
    << backendEp,
       workerEp,
       queues,
       lastSendOutcome >>

(*
--algorithm SingleSlotTransport
variables
  backendEp = "Detached",
  workerEp = "Detached",
  queues = [dir \in Directions |-> <<>>],
  lastSendOutcome = [dir \in Directions |-> "None"];

process Main = "main"
begin
Loop:
  while TRUE do
    either
      with next \in EndpointStates do
        await CanTransition(backendEp, next);
        backendEp := next;
      end with;

    or
      with next \in EndpointStates do
        await CanTransition(workerEp, next);
        workerEp := next;
      end with;

    or
      with dir \in Directions, m \in MsgSet do
        await CanSend(dir, m, backendEp, workerEp, queues);
        queues := [queues EXCEPT ![dir] = Append(@, m)];
        lastSendOutcome := [
          lastSendOutcome EXCEPT
            ![dir] = NotifyOutcome(dir, backendEp, workerEp)
        ];
      end with;

    or
      with dir \in Directions do
        await CanRecv(dir, backendEp, workerEp, queues);
        queues := [queues EXCEPT ![dir] = Tail(@)];
      end with;

    end either;
  end while;
end process;
end algorithm;
*)

Init ==
    /\ backendEp = "Detached"
    /\ workerEp = "Detached"
    /\ queues = [dir \in Directions |-> <<>>]
    /\ lastSendOutcome = [dir \in Directions |-> "None"]

BackendEndpointTransition(next) ==
    /\ next \in EndpointStates
    /\ CanTransition(backendEp, next)
    /\ backendEp' = next
    /\ UNCHANGED << workerEp, queues, lastSendOutcome >>

WorkerEndpointTransition(next) ==
    /\ next \in EndpointStates
    /\ CanTransition(workerEp, next)
    /\ workerEp' = next
    /\ UNCHANGED << backendEp, queues, lastSendOutcome >>

Send(dir, msg) ==
    /\ dir \in Directions
    /\ msg \in MsgSet
    /\ CanSend(dir, msg, backendEp, workerEp, queues)
    /\ queues' = [queues EXCEPT ![dir] = Append(@, msg)]
    /\ lastSendOutcome' =
        [lastSendOutcome EXCEPT
            ![dir] = NotifyOutcome(dir, backendEp, workerEp)]
    /\ UNCHANGED << backendEp, workerEp >>

Recv(dir) ==
    /\ dir \in Directions
    /\ CanRecv(dir, backendEp, workerEp, queues)
    /\ queues' = [queues EXCEPT ![dir] = Tail(@)]
    /\ UNCHANGED << backendEp, workerEp, lastSendOutcome >>

Next ==
    \/ \E next \in EndpointStates : BackendEndpointTransition(next)
    \/ \E next \in EndpointStates : WorkerEndpointTransition(next)
    \/ \E dir \in Directions, msg \in MsgSet : Send(dir, msg)
    \/ \E dir \in Directions : Recv(dir)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ backendEp \in EndpointStates
    /\ workerEp \in EndpointStates
    /\ queues \in [Directions -> Seq(MsgSet)]
    /\ lastSendOutcome \in [Directions -> LastOutcomeStates]

QueueCapacityOK ==
    \A dir \in Directions : QueueBytes(queues[dir]) <= CapBytes

====
