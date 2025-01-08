use rust_fsm::*;

pub enum BackendState {
    CustomScanBuilder,
    Failed,
    Initial,
    MetadataProvider,
}

pub enum Input {
    Bind,
    Error,
    Parse,
}

pub enum Output {}

state_machine! {
    #[state_machine(input(crate::fsm::Input), state(crate::fsm::BackendState), output(crate::fsm::Output))]
    pub backend(Initial)

    Initial(Error) => Failed,
    Initial(Parse) => MetadataProvider,
    MetadataProvider(Error) => Failed,
    MetadataProvider(Bind) => CustomScanBuilder,
    CustomScanBuilder(Error) => Failed,
}

pub enum ExecutorState {
    BasePlanBuilder,
    Failed,
    Initial,
    LogicalPlanBuilder,
}

state_machine! {
    #[state_machine(input(crate::fsm::Input), state(crate::fsm::ExecutorState), output(crate::fsm::Output))]
    pub executor(Initial)

    Initial(Error) => Failed,
    Initial(Parse) => BasePlanBuilder,
    BasePlanBuilder(Error) => Failed,
    BasePlanBuilder(Bind) => LogicalPlanBuilder,
    LogicalPlanBuilder(Error) => Failed,
}
