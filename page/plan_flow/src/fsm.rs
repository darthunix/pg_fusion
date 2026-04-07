use rust_fsm::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendPlanEvent {
    Open,
    EmitPage,
    EmitClose,
    EmitError,
    Fail,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendPlanAction {
    OpenPlan,
    EmitPage,
    EmitCloseFrame,
    EmitLogicalError,
    Poison,
    ClosePlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendPlanState {
    Closed,
    Opened,
    Streaming,
    SuccessTerminal,
    Failed,
    Exhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerPlanEvent {
    Open,
    AcceptPage,
    AcceptClose,
    AcceptSenderError,
    Fail,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerPlanAction {
    OpenPlan,
    FeedChunk,
    FinishDecode,
    FailLogicalPlan,
    Poison,
    ClosePlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerPlanState {
    Closed,
    Opened,
    Receiving,
    SuccessTerminal,
    Failed,
}

state_machine! {
    #[state_machine(input(crate::fsm::BackendPlanEvent), state(crate::fsm::BackendPlanState), output(crate::fsm::BackendPlanAction))]
    pub backend_plan_flow(Closed)

    Closed => {
        Open => Opened[OpenPlan],
    },
    Opened => {
        EmitPage => Streaming[EmitPage],
        EmitClose => SuccessTerminal[EmitCloseFrame],
        EmitError => Failed[EmitLogicalError],
        Fail => Failed[Poison],
    },
    Streaming => {
        EmitPage => Streaming[EmitPage],
        EmitClose => SuccessTerminal[EmitCloseFrame],
        EmitError => Failed[EmitLogicalError],
        Fail => Failed[Poison],
    },
    SuccessTerminal => {
        Close => Exhausted[ClosePlan],
    },
    Failed => {
        Close => Closed[ClosePlan],
    }
}

state_machine! {
    #[state_machine(input(crate::fsm::WorkerPlanEvent), state(crate::fsm::WorkerPlanState), output(crate::fsm::WorkerPlanAction))]
    pub worker_plan_flow(Closed)

    Closed => {
        Open => Opened[OpenPlan],
    },
    Opened => {
        AcceptPage => Receiving[FeedChunk],
        AcceptClose => SuccessTerminal[FinishDecode],
        AcceptSenderError => Failed[FailLogicalPlan],
        Fail => Failed[Poison],
    },
    Receiving => {
        AcceptPage => Receiving[FeedChunk],
        AcceptClose => SuccessTerminal[FinishDecode],
        AcceptSenderError => Failed[FailLogicalPlan],
        Fail => Failed[Poison],
    },
    SuccessTerminal => {
        Close => Closed[ClosePlan],
    },
    Failed => {
        Close => Closed[ClosePlan],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        backend_plan_flow, worker_plan_flow, BackendPlanAction, BackendPlanEvent, BackendPlanState,
        WorkerPlanAction, WorkerPlanEvent, WorkerPlanState,
    };

    #[test]
    fn backend_happy_path_reaches_success_terminal() {
        let mut machine = backend_plan_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&BackendPlanEvent::Open).unwrap(),
            Some(BackendPlanAction::OpenPlan)
        );
        assert_eq!(machine.state(), &BackendPlanState::Opened);
        assert_eq!(
            machine.consume(&BackendPlanEvent::EmitPage).unwrap(),
            Some(BackendPlanAction::EmitPage)
        );
        assert_eq!(machine.state(), &BackendPlanState::Streaming);
        assert_eq!(
            machine.consume(&BackendPlanEvent::EmitClose).unwrap(),
            Some(BackendPlanAction::EmitCloseFrame)
        );
        assert_eq!(machine.state(), &BackendPlanState::SuccessTerminal);
        assert_eq!(
            machine.consume(&BackendPlanEvent::Close).unwrap(),
            Some(BackendPlanAction::ClosePlan)
        );
        assert_eq!(machine.state(), &BackendPlanState::Exhausted);
    }

    #[test]
    fn backend_error_path_reaches_failed() {
        let mut machine = backend_plan_flow::StateMachine::new();
        machine.consume(&BackendPlanEvent::Open).unwrap();
        assert_eq!(
            machine.consume(&BackendPlanEvent::EmitError).unwrap(),
            Some(BackendPlanAction::EmitLogicalError)
        );
        assert_eq!(machine.state(), &BackendPlanState::Failed);
    }

    #[test]
    fn worker_happy_path_reaches_success_terminal() {
        let mut machine = worker_plan_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&WorkerPlanEvent::Open).unwrap(),
            Some(WorkerPlanAction::OpenPlan)
        );
        assert_eq!(machine.state(), &WorkerPlanState::Opened);
        assert_eq!(
            machine.consume(&WorkerPlanEvent::AcceptPage).unwrap(),
            Some(WorkerPlanAction::FeedChunk)
        );
        assert_eq!(machine.state(), &WorkerPlanState::Receiving);
        assert_eq!(
            machine.consume(&WorkerPlanEvent::AcceptClose).unwrap(),
            Some(WorkerPlanAction::FinishDecode)
        );
        assert_eq!(machine.state(), &WorkerPlanState::SuccessTerminal);
        assert_eq!(
            machine.consume(&WorkerPlanEvent::Close).unwrap(),
            Some(WorkerPlanAction::ClosePlan)
        );
        assert_eq!(machine.state(), &WorkerPlanState::Closed);
    }

    #[test]
    fn worker_error_path_reaches_failed() {
        let mut machine = worker_plan_flow::StateMachine::new();
        machine.consume(&WorkerPlanEvent::Open).unwrap();
        assert_eq!(
            machine
                .consume(&WorkerPlanEvent::AcceptSenderError)
                .unwrap(),
            Some(WorkerPlanAction::FailLogicalPlan)
        );
        assert_eq!(machine.state(), &WorkerPlanState::Failed);
    }
}
