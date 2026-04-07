use rust_fsm::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeEvent {
    Begin,
    Fail,
    AtomFinished,
    ScanSpecsStarted,
    ScanSpecFinished,
    ScanSpecsFinished,
    LogicalPlanFinished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeAction {
    WriteEnvelopeArrayLen,
    Poison,
    WriteMagic,
    WriteVersion,
    WriteScanSpecsArrayLen,
    WriteScanSpec,
    WriteLogicalPlanBinLen,
    WriteLogicalPlanBin,
    Finish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeState {
    Start,
    Failed,
    EnvelopeArrayLen,
    Magic,
    Version,
    ScanSpecsArrayLen,
    ScanSpecs,
    LogicalPlanBinLen,
    LogicalPlanBin,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeEvent {
    Begin,
    Fail,
    Eof,
    AtomDecoded,
    ScanSpecsStarted,
    ScanSpecDecoded,
    ScanSpecsFinished,
    LogicalPlanDecoded,
    LogicalPlanBuilt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeAction {
    ReadEnvelopeArrayLen,
    Poison,
    ReadMagic,
    ReadVersion,
    ReadScanSpecsArrayLen,
    ReadScanSpec,
    ReadLogicalPlanBinLen,
    ReadLogicalPlanBin,
    BuildLogicalPlan,
    Finish,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeState {
    Start,
    Failed,
    EnvelopeArrayLen,
    Magic,
    Version,
    ScanSpecsArrayLen,
    ScanSpecs,
    LogicalPlanBinLen,
    LogicalPlanBin,
    BuildLogicalPlan,
    AwaitEof,
    Done,
}

state_machine! {
    #[state_machine(input(crate::fsm::EncodeEvent), state(crate::fsm::EncodeState), output(crate::fsm::EncodeAction))]
    pub encode_flow(Start)

    Start => {
        Begin => EnvelopeArrayLen[WriteEnvelopeArrayLen],
        Fail => Failed[Poison],
    },
    EnvelopeArrayLen => {
        AtomFinished => Magic[WriteMagic],
        Fail => Failed[Poison],
    },
    Magic => {
        AtomFinished => Version[WriteVersion],
        Fail => Failed[Poison],
    },
    Version => {
        AtomFinished => ScanSpecsArrayLen[WriteScanSpecsArrayLen],
        Fail => Failed[Poison],
    },
    ScanSpecsArrayLen => {
        ScanSpecsStarted => ScanSpecs[WriteScanSpec],
        ScanSpecsFinished => LogicalPlanBinLen[WriteLogicalPlanBinLen],
        Fail => Failed[Poison],
    },
    ScanSpecs => {
        ScanSpecFinished => ScanSpecs[WriteScanSpec],
        ScanSpecsFinished => LogicalPlanBinLen[WriteLogicalPlanBinLen],
        Fail => Failed[Poison],
    },
    LogicalPlanBinLen => {
        AtomFinished => LogicalPlanBin[WriteLogicalPlanBin],
        Fail => Failed[Poison],
    },
    LogicalPlanBin => {
        LogicalPlanFinished => Done[Finish],
        Fail => Failed[Poison],
    }
}

state_machine! {
    #[state_machine(input(crate::fsm::DecodeEvent), state(crate::fsm::DecodeState), output(crate::fsm::DecodeAction))]
    pub decode_flow(Start)

    Start => {
        Begin => EnvelopeArrayLen[ReadEnvelopeArrayLen],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    EnvelopeArrayLen => {
        AtomDecoded => Magic[ReadMagic],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    Magic => {
        AtomDecoded => Version[ReadVersion],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    Version => {
        AtomDecoded => ScanSpecsArrayLen[ReadScanSpecsArrayLen],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    ScanSpecsArrayLen => {
        ScanSpecsStarted => ScanSpecs[ReadScanSpec],
        ScanSpecsFinished => LogicalPlanBinLen[ReadLogicalPlanBinLen],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    ScanSpecs => {
        ScanSpecDecoded => ScanSpecs[ReadScanSpec],
        ScanSpecsFinished => LogicalPlanBinLen[ReadLogicalPlanBinLen],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    LogicalPlanBinLen => {
        AtomDecoded => LogicalPlanBin[ReadLogicalPlanBin],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    LogicalPlanBin => {
        LogicalPlanDecoded => BuildLogicalPlan[BuildLogicalPlan],
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    BuildLogicalPlan => {
        LogicalPlanBuilt => AwaitEof,
        Eof => Failed[Poison],
        Fail => Failed[Poison],
    },
    AwaitEof => {
        Eof => Done[Finish],
        Fail => Failed[Poison],
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_flow, encode_flow, DecodeAction, DecodeEvent, DecodeState, EncodeAction,
        EncodeEvent, EncodeState,
    };

    #[test]
    fn encode_machine_handles_empty_scan_spec_table() {
        let mut machine = encode_flow::StateMachine::new();
        assert_eq!(machine.state(), &EncodeState::Start);
        assert_eq!(
            machine.consume(&EncodeEvent::Begin).unwrap(),
            Some(EncodeAction::WriteEnvelopeArrayLen)
        );
        assert_eq!(machine.state(), &EncodeState::EnvelopeArrayLen);
        assert_eq!(
            machine.consume(&EncodeEvent::AtomFinished).unwrap(),
            Some(EncodeAction::WriteMagic)
        );
        assert_eq!(
            machine.consume(&EncodeEvent::AtomFinished).unwrap(),
            Some(EncodeAction::WriteVersion)
        );
        assert_eq!(
            machine.consume(&EncodeEvent::AtomFinished).unwrap(),
            Some(EncodeAction::WriteScanSpecsArrayLen)
        );
        assert_eq!(
            machine.consume(&EncodeEvent::ScanSpecsFinished).unwrap(),
            Some(EncodeAction::WriteLogicalPlanBinLen)
        );
        assert_eq!(machine.state(), &EncodeState::LogicalPlanBinLen);
    }

    #[test]
    fn encode_machine_loops_while_scan_specs_remain() {
        let mut machine = encode_flow::StateMachine::new();
        machine.consume(&EncodeEvent::Begin).unwrap();
        machine.consume(&EncodeEvent::AtomFinished).unwrap();
        machine.consume(&EncodeEvent::AtomFinished).unwrap();
        machine.consume(&EncodeEvent::AtomFinished).unwrap();
        assert_eq!(
            machine.consume(&EncodeEvent::ScanSpecsStarted).unwrap(),
            Some(EncodeAction::WriteScanSpec)
        );
        assert_eq!(machine.state(), &EncodeState::ScanSpecs);
        assert_eq!(
            machine.consume(&EncodeEvent::ScanSpecFinished).unwrap(),
            Some(EncodeAction::WriteScanSpec)
        );
        assert_eq!(machine.state(), &EncodeState::ScanSpecs);
    }

    #[test]
    fn decode_machine_reaches_build_state() {
        let mut machine = decode_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&DecodeEvent::Begin).unwrap(),
            Some(DecodeAction::ReadEnvelopeArrayLen)
        );
        assert_eq!(
            machine.consume(&DecodeEvent::AtomDecoded).unwrap(),
            Some(DecodeAction::ReadMagic)
        );
        assert_eq!(
            machine.consume(&DecodeEvent::AtomDecoded).unwrap(),
            Some(DecodeAction::ReadVersion)
        );
        assert_eq!(
            machine.consume(&DecodeEvent::AtomDecoded).unwrap(),
            Some(DecodeAction::ReadScanSpecsArrayLen)
        );
        assert_eq!(
            machine.consume(&DecodeEvent::ScanSpecsFinished).unwrap(),
            Some(DecodeAction::ReadLogicalPlanBinLen)
        );
        assert_eq!(
            machine.consume(&DecodeEvent::AtomDecoded).unwrap(),
            Some(DecodeAction::ReadLogicalPlanBin)
        );
        assert_eq!(
            machine.consume(&DecodeEvent::LogicalPlanDecoded).unwrap(),
            Some(DecodeAction::BuildLogicalPlan)
        );
        assert_eq!(machine.state(), &DecodeState::BuildLogicalPlan);
        assert_eq!(
            machine.consume(&DecodeEvent::LogicalPlanBuilt).unwrap(),
            None
        );
        assert_eq!(machine.state(), &DecodeState::AwaitEof);
        assert_eq!(
            machine.consume(&DecodeEvent::Eof).unwrap(),
            Some(DecodeAction::Finish)
        );
        assert_eq!(machine.state(), &DecodeState::Done);
    }

    #[test]
    fn decode_machine_moves_to_failed_on_eof() {
        let mut machine = decode_flow::StateMachine::new();
        assert_eq!(
            machine.consume(&DecodeEvent::Eof).unwrap(),
            Some(DecodeAction::Poison)
        );
        assert_eq!(machine.state(), &DecodeState::Failed);
    }

    #[test]
    fn encode_machine_moves_to_failed_on_fail() {
        let mut machine = encode_flow::StateMachine::new();
        machine.consume(&EncodeEvent::Begin).unwrap();
        assert_eq!(
            machine.consume(&EncodeEvent::Fail).unwrap(),
            Some(EncodeAction::Poison)
        );
        assert_eq!(machine.state(), &EncodeState::Failed);
    }
}
