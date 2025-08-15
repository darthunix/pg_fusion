use rust_fsm::*;

#[derive(Debug)]
pub enum Action {
    Bind,
    Parse,
    Compile,
    Flush,
    Explain,
}

#[derive(Debug, PartialEq)]
pub enum ExecutorState {
    Statement,
    Initialized,
    LogicalPlan,
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
    }
}
