use rust_fsm::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionEvent {
    BeginExecution,
    PlanPublished,
    OpenScan,
    CompleteExecution,
    FailExecution,
    CancelExecution,
    Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionAction {
    InstallExecution,
    MarkRunning,
    ObserveScanOpen,
    CompleteExecution,
    FailExecution,
    CancelExecution,
    CleanupExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionState {
    Idle,
    Starting,
    Running,
    Terminal,
}

state_machine! {
    #[state_machine(
        input(crate::fsm::BackendExecutionEvent),
        state(crate::fsm::BackendExecutionState),
        output(crate::fsm::BackendExecutionAction)
    )]
    pub backend_execution_flow(Idle)

    Idle => {
        BeginExecution => Starting[InstallExecution],
    },
    Starting => {
        PlanPublished => Running[MarkRunning],
        FailExecution => Terminal[FailExecution],
        CancelExecution => Terminal[CancelExecution],
    },
    Running => {
        OpenScan => Running[ObserveScanOpen],
        CompleteExecution => Terminal[CompleteExecution],
        FailExecution => Terminal[FailExecution],
        CancelExecution => Terminal[CancelExecution],
    },
    Terminal => {
        Cleanup => Idle[CleanupExecution],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        backend_execution_flow, BackendExecutionAction, BackendExecutionEvent,
        BackendExecutionState,
    };

    #[test]
    fn begin_publish_and_cleanup_happy_path() {
        let mut machine = backend_execution_flow::StateMachine::new();
        assert_eq!(machine.state(), &BackendExecutionState::Idle);
        assert_eq!(
            machine
                .consume(&BackendExecutionEvent::BeginExecution)
                .unwrap(),
            Some(BackendExecutionAction::InstallExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Starting);
        assert_eq!(
            machine
                .consume(&BackendExecutionEvent::PlanPublished)
                .unwrap(),
            Some(BackendExecutionAction::MarkRunning)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Running);
        assert_eq!(
            machine
                .consume(&BackendExecutionEvent::CompleteExecution)
                .unwrap(),
            Some(BackendExecutionAction::CompleteExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Terminal);
        assert_eq!(
            machine.consume(&BackendExecutionEvent::Cleanup).unwrap(),
            Some(BackendExecutionAction::CleanupExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Idle);
    }

    #[test]
    fn cancel_from_starting_reaches_terminal() {
        let mut machine = backend_execution_flow::StateMachine::new();
        let _ = machine
            .consume(&BackendExecutionEvent::BeginExecution)
            .unwrap();
        assert_eq!(
            machine
                .consume(&BackendExecutionEvent::CancelExecution)
                .unwrap(),
            Some(BackendExecutionAction::CancelExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Terminal);
    }

    #[test]
    fn open_scan_from_running_is_self_loop() {
        let mut machine = backend_execution_flow::StateMachine::new();
        let _ = machine
            .consume(&BackendExecutionEvent::BeginExecution)
            .unwrap();
        let _ = machine
            .consume(&BackendExecutionEvent::PlanPublished)
            .unwrap();

        assert_eq!(
            machine.consume(&BackendExecutionEvent::OpenScan).unwrap(),
            Some(BackendExecutionAction::ObserveScanOpen)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Running);
    }

    #[test]
    fn fail_from_running_reaches_terminal_and_cleans_up() {
        let mut machine = backend_execution_flow::StateMachine::new();
        let _ = machine
            .consume(&BackendExecutionEvent::BeginExecution)
            .unwrap();
        let _ = machine
            .consume(&BackendExecutionEvent::PlanPublished)
            .unwrap();

        assert_eq!(
            machine
                .consume(&BackendExecutionEvent::FailExecution)
                .unwrap(),
            Some(BackendExecutionAction::FailExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Terminal);
        assert_eq!(
            machine.consume(&BackendExecutionEvent::Cleanup).unwrap(),
            Some(BackendExecutionAction::CleanupExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Idle);
    }

    #[test]
    fn cancel_from_running_reaches_terminal_and_cleans_up() {
        let mut machine = backend_execution_flow::StateMachine::new();
        let _ = machine
            .consume(&BackendExecutionEvent::BeginExecution)
            .unwrap();
        let _ = machine
            .consume(&BackendExecutionEvent::PlanPublished)
            .unwrap();

        assert_eq!(
            machine
                .consume(&BackendExecutionEvent::CancelExecution)
                .unwrap(),
            Some(BackendExecutionAction::CancelExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Terminal);
        assert_eq!(
            machine.consume(&BackendExecutionEvent::Cleanup).unwrap(),
            Some(BackendExecutionAction::CleanupExecution)
        );
        assert_eq!(machine.state(), &BackendExecutionState::Idle);
    }
}
