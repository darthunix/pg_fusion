use rust_fsm::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerExecutionEvent {
    StartExecution,
    PlanFrame,
    PlanDecoded,
    PhysicalPlanReady,
    OpenScan,
    ScanPage,
    ScanEof,
    CompleteExecution,
    FailExecution,
    CancelExecution,
    TransportRestart,
    Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerExecutionAction {
    OpenPlanFlow,
    AcceptPlanFrame,
    PlanPhysical,
    MarkRunning,
    OpenScan,
    AcceptScanPage,
    ObserveScanEof,
    CompleteExecution,
    FailExecution,
    CancelExecution,
    AbortForTransportRestart,
    CleanupExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerExecutionState {
    Idle,
    ReceivingPlan,
    Planning,
    Running,
    Terminal,
}

state_machine! {
    #[state_machine(
        input(crate::fsm::WorkerExecutionEvent),
        state(crate::fsm::WorkerExecutionState),
        output(crate::fsm::WorkerExecutionAction)
    )]
    pub worker_execution_flow(Idle)

    Idle => {
        StartExecution => ReceivingPlan[OpenPlanFlow],
        TransportRestart => Terminal[AbortForTransportRestart],
    },
    ReceivingPlan => {
        PlanFrame => ReceivingPlan[AcceptPlanFrame],
        PlanDecoded => Planning[PlanPhysical],
        FailExecution => Terminal[FailExecution],
        CancelExecution => Terminal[CancelExecution],
        TransportRestart => Terminal[AbortForTransportRestart],
    },
    Planning => {
        PhysicalPlanReady => Running[MarkRunning],
        FailExecution => Terminal[FailExecution],
        CancelExecution => Terminal[CancelExecution],
        TransportRestart => Terminal[AbortForTransportRestart],
    },
    Running => {
        OpenScan => Running[OpenScan],
        ScanPage => Running[AcceptScanPage],
        ScanEof => Running[ObserveScanEof],
        CompleteExecution => Terminal[CompleteExecution],
        FailExecution => Terminal[FailExecution],
        CancelExecution => Terminal[CancelExecution],
        TransportRestart => Terminal[AbortForTransportRestart],
    },
    Terminal => {
        Cleanup => Idle[CleanupExecution],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        worker_execution_flow, WorkerExecutionAction, WorkerExecutionEvent, WorkerExecutionState,
    };

    #[test]
    fn start_plan_run_complete_cleanup_happy_path() {
        let mut machine = worker_execution_flow::StateMachine::new();
        assert_eq!(machine.state(), &WorkerExecutionState::Idle);

        assert_eq!(
            machine
                .consume(&WorkerExecutionEvent::StartExecution)
                .unwrap(),
            Some(WorkerExecutionAction::OpenPlanFlow)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::ReceivingPlan);

        assert_eq!(
            machine.consume(&WorkerExecutionEvent::PlanFrame).unwrap(),
            Some(WorkerExecutionAction::AcceptPlanFrame)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::ReceivingPlan);

        assert_eq!(
            machine.consume(&WorkerExecutionEvent::PlanDecoded).unwrap(),
            Some(WorkerExecutionAction::PlanPhysical)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Planning);

        assert_eq!(
            machine
                .consume(&WorkerExecutionEvent::PhysicalPlanReady)
                .unwrap(),
            Some(WorkerExecutionAction::MarkRunning)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Running);

        assert_eq!(
            machine
                .consume(&WorkerExecutionEvent::CompleteExecution)
                .unwrap(),
            Some(WorkerExecutionAction::CompleteExecution)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Terminal);

        assert_eq!(
            machine.consume(&WorkerExecutionEvent::Cleanup).unwrap(),
            Some(WorkerExecutionAction::CleanupExecution)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Idle);
    }

    #[test]
    fn cancel_from_receiving_plan_reaches_terminal() {
        let mut machine = worker_execution_flow::StateMachine::new();
        machine
            .consume(&WorkerExecutionEvent::StartExecution)
            .unwrap();

        assert_eq!(
            machine
                .consume(&WorkerExecutionEvent::CancelExecution)
                .unwrap(),
            Some(WorkerExecutionAction::CancelExecution)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Terminal);
    }

    #[test]
    fn scan_events_are_running_self_loops() {
        let mut machine = worker_execution_flow::StateMachine::new();
        machine
            .consume(&WorkerExecutionEvent::StartExecution)
            .unwrap();
        machine.consume(&WorkerExecutionEvent::PlanDecoded).unwrap();
        machine
            .consume(&WorkerExecutionEvent::PhysicalPlanReady)
            .unwrap();

        assert_eq!(
            machine.consume(&WorkerExecutionEvent::OpenScan).unwrap(),
            Some(WorkerExecutionAction::OpenScan)
        );
        assert_eq!(
            machine.consume(&WorkerExecutionEvent::ScanPage).unwrap(),
            Some(WorkerExecutionAction::AcceptScanPage)
        );
        assert_eq!(
            machine.consume(&WorkerExecutionEvent::ScanEof).unwrap(),
            Some(WorkerExecutionAction::ObserveScanEof)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Running);
    }

    #[test]
    fn transport_restart_aborts_active_execution() {
        let mut machine = worker_execution_flow::StateMachine::new();
        machine
            .consume(&WorkerExecutionEvent::StartExecution)
            .unwrap();

        assert_eq!(
            machine
                .consume(&WorkerExecutionEvent::TransportRestart)
                .unwrap(),
            Some(WorkerExecutionAction::AbortForTransportRestart)
        );
        assert_eq!(machine.state(), &WorkerExecutionState::Terminal);
    }
}
