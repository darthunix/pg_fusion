use rust_fsm::*;

#[derive(Debug)]
pub enum Action {
    Bind,
    Parse,
    Compile,
    Flush,
    Explain,
    Optimize,
    Translate,
    ReadHeap,
}

#[derive(Debug, PartialEq)]
pub enum ExecutorState {
    Statement,
    Initialized,
    LogicalPlan,
    PhysicalPlan,
}

state_machine! {
    #[state_machine(input(protocol::Packet), state(crate::fsm::ExecutorState), output(crate::fsm::Action))]
    pub executor(Initialized)

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
        Explain => Initialized[Explain],
        Optimize => LogicalPlan[Optimize],
        Translate => PhysicalPlan[Translate],
    },
    PhysicalPlan => {
        Failure => Initialized[Flush],
        Explain => Initialized[Explain],
        Heap => PhysicalPlan[ReadHeap],
    }
}
