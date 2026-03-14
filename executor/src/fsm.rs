use rust_fsm::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryFlowAction {
    Bind,
    CaptureColumnLayout,
    Parse,
    Compile,
    Flush,
    Explain,
    Optimize,
    Translate,
    OpenDataFlow,
    StartDataFlow,
    EndDataFlow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryFlowState {
    Statement,
    Initialized,
    LogicalPlan,
    PhysicalPlan,
    Execution,
}

/// Events for the execution-time data flow that begins after a query reaches execution-ready state.
///
/// This is intentionally narrower than the top-level executor FSM:
/// it models only the coarse lifecycle after `query_flow` enters `Execution`.
/// Per-scan traffic like heap pages and scan EOF lives in `scan_flow`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataFlowEvent {
    BeginScan,
    ExecScan,
    TaskFinished,
    EndScan,
    Failure,
}

/// Side effects we expect the runtime layer to perform while transitioning the
/// data flow lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataFlowAction {
    OpenFlow,
    StartFlow,
    FinishFlow,
    CloseFlow,
    ResetFlow,
}

/// High-level state of the per-connection scan/runtime pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataFlowState {
    Closed,
    Opened,
    Running,
    Finished,
}

/// Events for a single heap scan lifecycle.
///
/// This FSM deliberately models the scan lifecycle, not scheduler counters like
/// `inflight` credits. Those remain runtime data carried alongside the state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanFlowEvent {
    Register,
    StartStream,
    HeapPage,
    ScanEof,
    EndScan,
    Failure,
}

/// Side effects expected from a single scan lifecycle transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanFlowAction {
    RegisterScan,
    ObserveStreamStart,
    RouteHeapPage,
    CloseScan,
    ResetScan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanFlowState {
    Closed,
    Active,
}

state_machine! {
    #[state_machine(input(protocol::QueryPacket), state(crate::fsm::QueryFlowState), output(crate::fsm::QueryFlowAction))]
    pub query_flow(Initialized)

    Initialized => {
        Parse => Statement[Parse],
    },
    Statement => {
        Failure => Initialized[Flush],
        Metadata => LogicalPlan[Compile],
    },
    LogicalPlan => {
        Failure => Initialized[Flush],
        Bind => LogicalPlan[Bind],
        Optimize => LogicalPlan[Optimize],
        Translate => PhysicalPlan[Translate],
    },
    PhysicalPlan => {
        Failure => Initialized[Flush],
        Explain => Initialized[Explain],
        ColumnLayout => Execution[CaptureColumnLayout],
    },
    Execution => {
        Failure => Initialized[Flush],
        BeginScan => Execution[OpenDataFlow],
        ExecScan => Execution[StartDataFlow],
        EndScan => Initialized[EndDataFlow],
    }
}

state_machine! {
    #[state_machine(input(crate::fsm::DataFlowEvent), state(crate::fsm::DataFlowState), output(crate::fsm::DataFlowAction))]
    pub data_flow(Closed)

    Closed => {
        BeginScan => Opened[OpenFlow],
    },
    Opened => {
        Failure => Closed[ResetFlow],
        ExecScan => Running[StartFlow],
        EndScan => Closed[CloseFlow],
    },
    Running => {
        Failure => Closed[ResetFlow],
        TaskFinished => Finished[FinishFlow],
        EndScan => Closed[CloseFlow],
    },
    Finished => {
        Failure => Closed[ResetFlow],
        EndScan => Closed[CloseFlow],
    }
}

state_machine! {
    #[state_machine(input(crate::fsm::ScanFlowEvent), state(crate::fsm::ScanFlowState), output(crate::fsm::ScanFlowAction))]
    pub scan_flow(Closed)

    Closed => {
        Register => Active[RegisterScan],
    },
    Active => {
        Failure => Closed[ResetScan],
        StartStream => Active[ObserveStreamStart],
        HeapPage => Active[RouteHeapPage],
        EndScan => Closed[CloseScan],
        ScanEof => Closed[CloseScan],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        data_flow, query_flow, scan_flow, DataFlowAction, DataFlowEvent, DataFlowState,
        QueryFlowAction, QueryFlowState, ScanFlowAction, ScanFlowEvent, ScanFlowState,
    };

    #[test]
    fn query_flow_happy_path_reaches_execution() {
        let mut machine = query_flow::StateMachine::new();

        assert_eq!(machine.state(), &QueryFlowState::Initialized);
        assert_eq!(
            machine.consume(&protocol::QueryPacket::Parse).unwrap(),
            Some(QueryFlowAction::Parse)
        );
        assert_eq!(machine.state(), &QueryFlowState::Statement);
        assert_eq!(
            machine.consume(&protocol::QueryPacket::Metadata).unwrap(),
            Some(QueryFlowAction::Compile)
        );
        assert_eq!(machine.state(), &QueryFlowState::LogicalPlan);
        assert_eq!(
            machine.consume(&protocol::QueryPacket::Bind).unwrap(),
            Some(QueryFlowAction::Bind)
        );
        assert_eq!(
            machine.consume(&protocol::QueryPacket::Optimize).unwrap(),
            Some(QueryFlowAction::Optimize)
        );
        assert_eq!(
            machine.consume(&protocol::QueryPacket::Translate).unwrap(),
            Some(QueryFlowAction::Translate)
        );
        assert_eq!(machine.state(), &QueryFlowState::PhysicalPlan);
        assert_eq!(
            machine.consume(&protocol::QueryPacket::ColumnLayout).unwrap(),
            Some(QueryFlowAction::CaptureColumnLayout)
        );
        assert_eq!(machine.state(), &QueryFlowState::Execution);
    }

    #[test]
    fn data_flow_happy_path_runs_to_finished() {
        let mut machine = data_flow::StateMachine::new();

        assert_eq!(machine.state(), &DataFlowState::Closed);
        assert_eq!(
            machine.consume(&DataFlowEvent::BeginScan).unwrap(),
            Some(DataFlowAction::OpenFlow)
        );
        assert_eq!(machine.state(), &DataFlowState::Opened);
        assert_eq!(
            machine.consume(&DataFlowEvent::ExecScan).unwrap(),
            Some(DataFlowAction::StartFlow)
        );
        assert_eq!(machine.state(), &DataFlowState::Running);
        assert_eq!(
            machine.consume(&DataFlowEvent::TaskFinished).unwrap(),
            Some(DataFlowAction::FinishFlow)
        );
        assert_eq!(machine.state(), &DataFlowState::Finished);
        assert_eq!(
            machine.consume(&DataFlowEvent::EndScan).unwrap(),
            Some(DataFlowAction::CloseFlow)
        );
        assert_eq!(machine.state(), &DataFlowState::Closed);
    }

    #[test]
    fn data_flow_rejects_exec_before_open() {
        let mut machine = data_flow::StateMachine::new();

        assert!(machine.consume(&DataFlowEvent::ExecScan).is_err());
        assert_eq!(machine.state(), &DataFlowState::Closed);
    }

    #[test]
    fn scan_flow_happy_path_streams_until_eof() {
        let mut machine = scan_flow::StateMachine::new();

        assert_eq!(machine.state(), &ScanFlowState::Closed);
        assert_eq!(
            machine.consume(&ScanFlowEvent::Register).unwrap(),
            Some(ScanFlowAction::RegisterScan)
        );
        assert_eq!(machine.state(), &ScanFlowState::Active);
        assert_eq!(
            machine.consume(&ScanFlowEvent::StartStream).unwrap(),
            Some(ScanFlowAction::ObserveStreamStart)
        );
        assert_eq!(machine.state(), &ScanFlowState::Active);
        assert_eq!(
            machine.consume(&ScanFlowEvent::HeapPage).unwrap(),
            Some(ScanFlowAction::RouteHeapPage)
        );
        assert_eq!(
            machine.consume(&ScanFlowEvent::ScanEof).unwrap(),
            Some(ScanFlowAction::CloseScan)
        );
        assert_eq!(machine.state(), &ScanFlowState::Closed);
    }

    #[test]
    fn scan_flow_rejects_pages_before_stream_attach() {
        let mut machine = scan_flow::StateMachine::new();

        assert!(machine.consume(&ScanFlowEvent::HeapPage).is_err());
        assert_eq!(machine.state(), &ScanFlowState::Closed);
    }
}
