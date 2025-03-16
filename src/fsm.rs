use crate::{error::FusionError, protocol::Packet};
use anyhow::Result as AnyResult;
use rust_fsm::*;

#[derive(Debug)]
pub enum ExecutorEvent {
    Bind,
    Error,
    Metadata,
    Parse,
    Save,
    SpuriousWakeup,
}

impl TryFrom<&Packet> for ExecutorEvent {
    type Error = FusionError;

    fn try_from(packet: &Packet) -> Result<Self, FusionError> {
        match packet {
            Packet::Failure => Ok(Self::Error),
            Packet::Metadata => Ok(Self::Metadata),
            Packet::Parse => Ok(Self::Parse),
            _ => Err(FusionError::DeserializeU8(
                "executor event".to_string(),
                packet.clone() as u8,
            )),
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
