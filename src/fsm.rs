use rust_fsm::*;

#[derive(Debug)]
pub enum ExecutorOutput {
    Bind,
    Parse,
    Compile,
    Flush,
}

pub enum ExecutorState {
    Statement,
    Initialized,
    LogicalPlan,
}

state_machine! {
    #[state_machine(input(crate::protocol::Packet), state(crate::fsm::ExecutorState), output(crate::fsm::ExecutorOutput))]
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
    }
}
