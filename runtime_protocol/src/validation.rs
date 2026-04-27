use crate::error::DecodeError;
use crate::msgpack::read_array_len_from;
use crate::scan::{
    read_producer_descriptor_from, read_scan_channel_descriptor_from, ProducerDescriptorWire,
    ProducerRole, ProducerSetError, ProducerSetRef, ScanChannelDescriptorWire, ScanChannelSetError,
    ScanChannelSetRef,
};

const PRODUCER_ID_BITMAP_WORD_BITS: usize = u64::BITS as usize;
const PRODUCER_ID_BITMAP_WORDS: usize = (u16::MAX as usize + 1) / PRODUCER_ID_BITMAP_WORD_BITS;

pub(crate) fn decode_producer_set_ref<'a>(
    original: &'a [u8],
    source: &mut &'a [u8],
) -> Result<ProducerSetRef<'a>, DecodeError> {
    let start = original.len() - source.len();
    let count = read_array_len_from(source)?;
    if count == 0 {
        return Err(DecodeError::EmptyProducerSet);
    }
    let mut leader_seen = false;
    let mut seen_ids = ProducerIdBitmap::new();

    for _ in 0..count {
        let producer = read_producer_descriptor_from(source)?;
        if let Some(error) = observe_producer(
            &mut seen_ids,
            &mut leader_seen,
            producer.producer_id,
            producer.role,
        ) {
            return Err(map_invariant_error_to_decode(error));
        }
    }

    let end = original.len() - source.len();
    Ok(ProducerSetRef {
        bytes: &original[start..end],
        len: count,
    })
}

pub(crate) fn decode_scan_channel_set_ref<'a>(
    original: &'a [u8],
    source: &mut &'a [u8],
) -> Result<ScanChannelSetRef<'a>, DecodeError> {
    let start = original.len() - source.len();
    let count = read_array_len_from(source)?;
    let mut scan_state = ScanChannelValidationState::default();

    for _ in 0..count {
        let descriptor = read_scan_channel_descriptor_from(source)?;
        observe_decode_scan_channel(&mut scan_state, descriptor)?;
    }
    finish_decode_scan_channel_validation(&scan_state)?;

    let end = original.len() - source.len();
    Ok(ScanChannelSetRef {
        bytes: &original[start..end],
        len: count,
    })
}

pub(crate) fn validate_encode_producer_slice(
    producers: &[ProducerDescriptorWire],
) -> Result<(), ProducerSetError> {
    if producers.is_empty() {
        return Err(ProducerSetError::EmptyProducerSet);
    }

    let mut leader_seen = false;
    let mut seen_ids = ProducerIdBitmap::new();

    for producer in producers {
        if let Some(error) = observe_producer(
            &mut seen_ids,
            &mut leader_seen,
            producer.producer_id,
            producer.role,
        ) {
            return Err(map_invariant_error_to_encode(error));
        }
    }

    Ok(())
}

pub(crate) fn validate_scan_channel_slice(
    channels: &[ScanChannelDescriptorWire],
) -> Result<(), ScanChannelSetError> {
    let mut scan_state = ScanChannelValidationState::default();
    for channel in channels {
        observe_encode_scan_channel(&mut scan_state, *channel)?;
    }
    finish_encode_scan_channel_validation(&scan_state)?;
    Ok(())
}

#[derive(Default)]
struct ScanChannelValidationState {
    previous: Option<(u64, u16)>,
    current_scan_id: Option<u64>,
    leader_seen: bool,
}

fn observe_decode_scan_channel(
    state: &mut ScanChannelValidationState,
    current: ScanChannelDescriptorWire,
) -> Result<(), DecodeError> {
    if let Some((previous_scan_id, previous_producer_id)) = state.previous {
        if current.scan_id == previous_scan_id && current.producer_id == previous_producer_id {
            return Err(DecodeError::DuplicateScanProducer {
                scan_id: current.scan_id,
                producer_id: current.producer_id,
            });
        }
        if (current.scan_id, current.producer_id) < (previous_scan_id, previous_producer_id) {
            return Err(DecodeError::ScanChannelOutOfOrder {
                previous_scan_id,
                previous_producer_id,
                current_scan_id: current.scan_id,
                current_producer_id: current.producer_id,
            });
        }
    }

    if state.current_scan_id != Some(current.scan_id) {
        finish_decode_scan_channel_validation(state)?;
        state.current_scan_id = Some(current.scan_id);
        state.leader_seen = false;
    }
    if current.role == ProducerRole::Leader {
        if state.leader_seen {
            return Err(DecodeError::MultipleScanChannelLeaders {
                scan_id: current.scan_id,
            });
        }
        state.leader_seen = true;
    }
    state.previous = Some((current.scan_id, current.producer_id));
    Ok(())
}

fn finish_decode_scan_channel_validation(
    state: &ScanChannelValidationState,
) -> Result<(), DecodeError> {
    if let Some(scan_id) = state.current_scan_id {
        if !state.leader_seen {
            return Err(DecodeError::MissingScanChannelLeader { scan_id });
        }
    }
    Ok(())
}

fn observe_encode_scan_channel(
    state: &mut ScanChannelValidationState,
    current: ScanChannelDescriptorWire,
) -> Result<(), ScanChannelSetError> {
    if let Some((previous_scan_id, previous_producer_id)) = state.previous {
        if current.scan_id == previous_scan_id && current.producer_id == previous_producer_id {
            return Err(ScanChannelSetError::DuplicateProducer {
                scan_id: current.scan_id,
                producer_id: current.producer_id,
            });
        }
        if (current.scan_id, current.producer_id) < (previous_scan_id, previous_producer_id) {
            return Err(ScanChannelSetError::ChannelOutOfOrder {
                previous_scan_id,
                previous_producer_id,
                current_scan_id: current.scan_id,
                current_producer_id: current.producer_id,
            });
        }
    }

    if state.current_scan_id != Some(current.scan_id) {
        finish_encode_scan_channel_validation(state)?;
        state.current_scan_id = Some(current.scan_id);
        state.leader_seen = false;
    }
    if current.role == ProducerRole::Leader {
        if state.leader_seen {
            return Err(ScanChannelSetError::MultipleLeaders {
                scan_id: current.scan_id,
            });
        }
        state.leader_seen = true;
    }
    state.previous = Some((current.scan_id, current.producer_id));
    Ok(())
}

fn finish_encode_scan_channel_validation(
    state: &ScanChannelValidationState,
) -> Result<(), ScanChannelSetError> {
    if let Some(scan_id) = state.current_scan_id {
        if !state.leader_seen {
            return Err(ScanChannelSetError::MissingLeader { scan_id });
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProducerSetInvariantError {
    DuplicateProducerId { producer_id: u16 },
    MultipleLeaders,
}

#[derive(Clone, Copy)]
struct ProducerIdBitmap {
    words: [u64; PRODUCER_ID_BITMAP_WORDS],
}

impl ProducerIdBitmap {
    fn new() -> Self {
        Self {
            words: [0; PRODUCER_ID_BITMAP_WORDS],
        }
    }

    fn insert(&mut self, producer_id: u16) -> bool {
        let producer_id = producer_id as usize;
        let word_index = producer_id / PRODUCER_ID_BITMAP_WORD_BITS;
        let bit_index = producer_id % PRODUCER_ID_BITMAP_WORD_BITS;
        let bit = 1u64 << bit_index;
        let word = &mut self.words[word_index];
        let was_absent = (*word & bit) == 0;
        *word |= bit;
        was_absent
    }
}

fn observe_producer(
    seen_ids: &mut ProducerIdBitmap,
    leader_seen: &mut bool,
    producer_id: u16,
    role: ProducerRole,
) -> Option<ProducerSetInvariantError> {
    if role == ProducerRole::Leader {
        if *leader_seen {
            return Some(ProducerSetInvariantError::MultipleLeaders);
        }
        *leader_seen = true;
    }

    if !seen_ids.insert(producer_id) {
        return Some(ProducerSetInvariantError::DuplicateProducerId { producer_id });
    }

    None
}

fn map_invariant_error_to_decode(error: ProducerSetInvariantError) -> DecodeError {
    match error {
        ProducerSetInvariantError::DuplicateProducerId { producer_id } => {
            DecodeError::DuplicateProducerId { producer_id }
        }
        ProducerSetInvariantError::MultipleLeaders => DecodeError::MultipleLeaders,
    }
}

fn map_invariant_error_to_encode(error: ProducerSetInvariantError) -> ProducerSetError {
    match error {
        ProducerSetInvariantError::DuplicateProducerId { producer_id } => {
            ProducerSetError::DuplicateProducerId { producer_id }
        }
        ProducerSetInvariantError::MultipleLeaders => ProducerSetError::MultipleLeaders,
    }
}
