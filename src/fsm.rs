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
    Resolved,
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
        Resolved => CreatedCustomScan,
    },
}

pub enum ExecutorEvent {
    Bind,
    Error,
    Metadata,
    NeedLookup,
    Parse,
    Resolved,
}

pub enum ExecutorOutput {
    WaitingLookup,
}

pub enum ExecutorState {
    ResolvingMetadata,
    CreatedLogicalPlan,
    Initialized,
}

state_machine! {
    #[state_machine(input(crate::fsm::ExecutorEvent), state(crate::fsm::ExecutorState), output(crate::fsm::ExecutorOutput))]
    pub executor(Initialized)

    Initialized(Parse) => ResolvingMetadata,
    ResolvingMetadata => {
        Error => Initialized,
        Metadata => ResolvingMetadata,
        NeedLookup => ResolvingMetadata[WaitingLookup],
        Resolved => CreatedLogicalPlan,
    },
    CreatedLogicalPlan => {
        Bind => CreatedLogicalPlan,
        Error => Initialized,
    }
}
