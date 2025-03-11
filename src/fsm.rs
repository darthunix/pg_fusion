use crate::protocol::Packet;
use rust_fsm::*;

pub enum BackendState {
    CreatedCustomScan,
    Failed,
    Initialized,
    MetadataProvider,
}

pub enum BackendEvent {
    Error,
    Compile,
    MetadataLookup,
    Save,
}

pub enum BackendOutput {}

state_machine! {
    #[state_machine(input(crate::fsm::BackendEvent), state(crate::fsm::BackendState), output(crate::fsm::BackendOutput))]
    pub backend(Initialized)

    Initialized => {
        Error => Failed,
        Compile => MetadataProvider,
    },
    MetadataProvider => {
        Error => Failed,
        MetadataLookup => MetadataProvider,
        Save => CreatedCustomScan,
    },
}

#[derive(Debug)]
pub enum ExecutorEvent {
    Bind,
    Error,
    Metadata,
    Parse,
    Save,
    SpuriousWakeup,
}

impl From<&Packet> for ExecutorEvent {
    fn from(packet: &Packet) -> Self {
        match packet {
            Packet::Failure => Self::Error,
            Packet::Parse => Self::Parse,
            Packet::None => Self::SpuriousWakeup,
        }
    }
}

#[derive(Debug)]
pub enum ExecutorOutput {
    Bind,
    Parse,
    Compile,
    Flush,
    Sleep,
}

pub enum ExecutorState {
    Statement,
    Initialized,
    LogicalPlan,
}

state_machine! {
    #[state_machine(input(crate::fsm::ExecutorEvent), state(crate::fsm::ExecutorState), output(crate::fsm::ExecutorOutput))]
    pub executor(Initialized)

    Initialized => {
        SpuriousWakeup => Initialized[Sleep],
        Parse => Statement[Parse],
    },
    Statement => {
        Error => Initialized[Flush],
        Metadata => LogicalPlan[Compile],
        SpuriousWakeup => Statement[Sleep],
    },
    LogicalPlan => {
        Bind => LogicalPlan[Bind],
        Error => Initialized[Flush]
    }
}
