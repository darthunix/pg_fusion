use crate::protocol::Packet;
use pgrx::prelude::*;
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
    Lookup,
    Metadata,
    Parse,
    Save,
    SpuriousWakeup,
}

impl From<&Packet> for ExecutorEvent {
    fn from(packet: &Packet) -> Self {
        match packet {
            Packet::Error => Self::Error,
            Packet::Parse => Self::Parse,
            Packet::None => Self::SpuriousWakeup,
        }
    }
}

#[derive(Debug)]
pub enum ExecutorOutput {
    BindParameters,
    BuildAst,
    Compile,
    Flush,
    ProcessMetadata,
    RequestMetadata,
    SaveBasePlan,
    Sleep,
}

pub enum ExecutorState {
    BasePlan,
    Initialized,
    LogicalPlan,
}

state_machine! {
    #[state_machine(input(crate::fsm::ExecutorEvent), state(crate::fsm::ExecutorState), output(crate::fsm::ExecutorOutput))]
    pub executor(Initialized)

    Initialized => {
        SpuriousWakeup => Initialized[Sleep],
        Parse => BasePlan[BuildAst],
    },
    BasePlan => {
        Error => Initialized[Flush],
        Metadata => BasePlan[Compile],
        Lookup => BasePlan[RequestMetadata],
        SpuriousWakeup => BasePlan[Sleep],
        Save => LogicalPlan[SaveBasePlan],
    },
    LogicalPlan => {
        Bind => LogicalPlan[BindParameters],
        Error => Initialized[Flush]
    }
}
