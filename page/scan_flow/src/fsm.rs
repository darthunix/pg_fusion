use rust_fsm::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendProducerEvent {
    Open,
    EmitPage,
    EmitEof,
    EmitError,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendProducerAction {
    OpenSource,
    EmitPage,
    EmitProducerEof,
    EmitProducerError,
    CloseSource,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendProducerState {
    Closed,
    Opened,
    Streaming,
    CloseTerminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendCoordinatorEvent {
    Open,
    ObserveProducerEof,
    ObserveLogicalEof,
    ObserveProducerError,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendCoordinatorAction {
    OpenScan,
    ObserveProducerEof,
    FinishLogicalEof,
    FailLogicalScan,
    CloseScan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendCoordinatorState {
    Closed,
    Opened,
    Running,
    CloseTerminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanEvent {
    Open,
    AcceptPage,
    ObserveProducerEof,
    ObserveLogicalEof,
    ObserveProducerError,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanAction {
    OpenScan,
    RoutePage,
    ObserveProducerEof,
    FinishLogicalEof,
    FailLogicalScan,
    CloseScan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanState {
    Closed,
    Opened,
    Streaming,
    CloseTerminal,
}

state_machine! {
    #[state_machine(input(crate::fsm::BackendProducerEvent), state(crate::fsm::BackendProducerState), output(crate::fsm::BackendProducerAction))]
    pub backend_producer_flow(Closed)

    Closed => {
        Open => Opened[OpenSource],
    },
    Opened => {
        EmitPage => Streaming[EmitPage],
        EmitEof => CloseTerminal[EmitProducerEof],
        EmitError => CloseTerminal[EmitProducerError],
    },
    Streaming => {
        EmitPage => Streaming[EmitPage],
        EmitEof => CloseTerminal[EmitProducerEof],
        EmitError => CloseTerminal[EmitProducerError],
    },
    CloseTerminal => {
        Close => Closed[CloseSource],
    }
}

state_machine! {
    #[state_machine(input(crate::fsm::BackendCoordinatorEvent), state(crate::fsm::BackendCoordinatorState), output(crate::fsm::BackendCoordinatorAction))]
    pub backend_coordinator_flow(Closed)

    Closed => {
        Open => Opened[OpenScan],
    },
    Opened => {
        ObserveProducerEof => Running[ObserveProducerEof],
        ObserveLogicalEof => CloseTerminal[FinishLogicalEof],
        ObserveProducerError => CloseTerminal[FailLogicalScan],
    },
    Running => {
        ObserveProducerEof => Running[ObserveProducerEof],
        ObserveLogicalEof => CloseTerminal[FinishLogicalEof],
        ObserveProducerError => CloseTerminal[FailLogicalScan],
    },
    CloseTerminal => {
        Close => Closed[CloseScan],
    }
}

state_machine! {
    #[state_machine(input(crate::fsm::WorkerScanEvent), state(crate::fsm::WorkerScanState), output(crate::fsm::WorkerScanAction))]
    pub worker_scan_flow(Closed)

    Closed => {
        Open => Opened[OpenScan],
    },
    Opened => {
        AcceptPage => Streaming[RoutePage],
        ObserveProducerEof => Opened[ObserveProducerEof],
        ObserveLogicalEof => CloseTerminal[FinishLogicalEof],
        ObserveProducerError => CloseTerminal[FailLogicalScan],
    },
    Streaming => {
        AcceptPage => Streaming[RoutePage],
        ObserveProducerEof => Streaming[ObserveProducerEof],
        ObserveLogicalEof => CloseTerminal[FinishLogicalEof],
        ObserveProducerError => CloseTerminal[FailLogicalScan],
    },
    CloseTerminal => {
        Close => Closed[CloseScan],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        backend_coordinator_flow, backend_producer_flow, worker_scan_flow,
        BackendCoordinatorAction, BackendCoordinatorEvent, BackendCoordinatorState,
        BackendProducerAction, BackendProducerEvent, BackendProducerState, WorkerScanAction,
        WorkerScanEvent, WorkerScanState,
    };

    #[test]
    fn backend_producer_happy_path() {
        let mut machine = backend_producer_flow::StateMachine::new();
        assert_eq!(machine.state(), &BackendProducerState::Closed);
        assert_eq!(
            machine.consume(&BackendProducerEvent::Open).unwrap(),
            Some(BackendProducerAction::OpenSource)
        );
        assert_eq!(machine.state(), &BackendProducerState::Opened);
        assert_eq!(
            machine.consume(&BackendProducerEvent::EmitPage).unwrap(),
            Some(BackendProducerAction::EmitPage)
        );
        assert_eq!(machine.state(), &BackendProducerState::Streaming);
        assert_eq!(
            machine.consume(&BackendProducerEvent::EmitEof).unwrap(),
            Some(BackendProducerAction::EmitProducerEof)
        );
        assert_eq!(machine.state(), &BackendProducerState::CloseTerminal);
        assert_eq!(
            machine.consume(&BackendProducerEvent::Close).unwrap(),
            Some(BackendProducerAction::CloseSource)
        );
        assert_eq!(machine.state(), &BackendProducerState::Closed);
    }

    #[test]
    fn backend_coordinator_finishes_after_logical_eof() {
        let mut machine = backend_coordinator_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&BackendCoordinatorEvent::Open).unwrap(),
            Some(BackendCoordinatorAction::OpenScan)
        );
        assert_eq!(machine.state(), &BackendCoordinatorState::Opened);
        assert_eq!(
            machine
                .consume(&BackendCoordinatorEvent::ObserveProducerEof)
                .unwrap(),
            Some(BackendCoordinatorAction::ObserveProducerEof)
        );
        assert_eq!(machine.state(), &BackendCoordinatorState::Running);
        assert_eq!(
            machine
                .consume(&BackendCoordinatorEvent::ObserveLogicalEof)
                .unwrap(),
            Some(BackendCoordinatorAction::FinishLogicalEof)
        );
        assert_eq!(machine.state(), &BackendCoordinatorState::CloseTerminal);
    }

    #[test]
    fn worker_scan_finishes_from_opened_without_pages() {
        let mut machine = worker_scan_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&WorkerScanEvent::Open).unwrap(),
            Some(WorkerScanAction::OpenScan)
        );
        assert_eq!(machine.state(), &WorkerScanState::Opened);
        assert_eq!(
            machine
                .consume(&WorkerScanEvent::ObserveLogicalEof)
                .unwrap(),
            Some(WorkerScanAction::FinishLogicalEof)
        );
        assert_eq!(machine.state(), &WorkerScanState::CloseTerminal);
        assert_eq!(
            machine.consume(&WorkerScanEvent::Close).unwrap(),
            Some(WorkerScanAction::CloseScan)
        );
        assert_eq!(machine.state(), &WorkerScanState::Closed);
    }

    #[test]
    fn backend_producer_error_path_reaches_close_terminal() {
        let mut machine = backend_producer_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&BackendProducerEvent::Open).unwrap(),
            Some(BackendProducerAction::OpenSource)
        );
        assert_eq!(
            machine.consume(&BackendProducerEvent::EmitError).unwrap(),
            Some(BackendProducerAction::EmitProducerError)
        );
        assert_eq!(machine.state(), &BackendProducerState::CloseTerminal);
    }
}
