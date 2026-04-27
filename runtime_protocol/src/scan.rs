//! Plan and scan descriptor types used by the runtime control plane.
//!
//! This module contains the narrow descriptor types mirrored by
//! `runtime_protocol`:
//!
//! - plan descriptors published in `StartExecution`
//! - scan channel descriptors published up front in `StartExecution`
//! - scan open descriptors and producer sets carried in `OpenScan`
//! - borrowed decode-side views that avoid allocation while keeping the
//!   original frame bytes borrowed

use crate::error::DecodeError;
use crate::msgpack::{
    expect_message_len, read_array_len_from, read_u16_from, read_u32_from, read_u64_from,
    read_u8_from, write_array_len_to, write_u16_to, write_u32_to, write_u64_to, write_u8_to,
};
use crate::validation::{validate_encode_producer_slice, validate_scan_channel_slice};
use std::fmt;
use thiserror::Error;
use transfer::MessageKind;

pub(crate) const PRODUCER_DESCRIPTOR_LEN: u32 = 2;
pub(crate) const SCAN_CHANNEL_DESCRIPTOR_LEN: u32 = 6;

/// Transport-agnostic wire identity of one backend lease slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackendLeaseSlotWire {
    slot_id: u32,
    generation: u64,
    lease_epoch: u64,
}

impl BackendLeaseSlotWire {
    /// Construct one transport-agnostic backend lease slot identity.
    pub const fn new(slot_id: u32, generation: u64, lease_epoch: u64) -> Self {
        Self {
            slot_id,
            generation,
            lease_epoch,
        }
    }

    /// Return the physical slot index.
    pub const fn slot_id(self) -> u32 {
        self.slot_id
    }

    /// Return the owning transport generation.
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Return the per-generation lease epoch.
    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }
}

/// One execution scan producer channel published up front in `StartExecution`.
///
/// Encoded channel lists must be in strictly increasing
/// `(scan_id, producer_id)` order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChannelDescriptorWire {
    /// Logical scan identifier inside one execution.
    pub scan_id: u64,
    /// Producer identifier scoped to `scan_id`.
    pub producer_id: u16,
    /// Producer role inside this logical scan.
    pub role: ProducerRole,
    /// Dedicated backend peer reserved for this producer.
    pub peer: BackendLeaseSlotWire,
}

/// Validation errors for one encode-side scan channel set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ScanChannelSetError {
    #[error("duplicate scan channel for scan_id {scan_id}, producer_id {producer_id}")]
    DuplicateProducer { scan_id: u64, producer_id: u16 },
    #[error(
        "scan channel set is not sorted by scan_id/producer_id: previous=({previous_scan_id}, {previous_producer_id}), current=({current_scan_id}, {current_producer_id})"
    )]
    ChannelOutOfOrder {
        previous_scan_id: u64,
        previous_producer_id: u16,
        current_scan_id: u64,
        current_producer_id: u16,
    },
    #[error("scan channel set declares multiple leader producers for scan_id {scan_id}")]
    MultipleLeaders { scan_id: u64 },
    #[error("scan channel set declares no leader producer for scan_id {scan_id}")]
    MissingLeader { scan_id: u64 },
}

/// Encode-side borrowed execution scan-channel set.
///
/// Channels must be in strictly increasing `scan_id` order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChannelSet<'a> {
    channels: &'a [ScanChannelDescriptorWire],
}

impl<'a> ScanChannelSet<'a> {
    /// Return an empty scan-channel set.
    pub const fn empty() -> Self {
        Self { channels: &[] }
    }

    /// Validate and construct one encode-side scan-channel set.
    pub fn new(channels: &'a [ScanChannelDescriptorWire]) -> Result<Self, ScanChannelSetError> {
        validate_scan_channel_slice(channels)?;
        Ok(Self { channels })
    }

    /// Return the number of published scan channels.
    pub fn len(self) -> usize {
        self.channels.len()
    }

    /// Return whether the set is empty.
    pub fn is_empty(self) -> bool {
        self.channels.is_empty()
    }

    /// Return the validated channel slice borrowed by this set.
    pub fn channels(self) -> &'a [ScanChannelDescriptorWire] {
        self.channels
    }
}

/// Borrowed decode-side view of one encoded scan-channel set.
///
/// Decoded channel entries are already validated to be in strictly increasing
/// `scan_id` order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChannelSetRef<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) len: u32,
}

impl<'a> ScanChannelSetRef<'a> {
    /// Return the canonical empty encoded scan-channel set.
    pub const fn empty() -> Self {
        Self {
            bytes: &[0x90],
            len: 0,
        }
    }

    /// Return the number of published scan channels.
    pub fn len(self) -> usize {
        self.len as usize
    }

    /// Return whether the borrowed set is empty.
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// Return the original validated encoded bytes.
    pub fn as_encoded(self) -> &'a [u8] {
        self.bytes
    }

    /// Iterate over the validated borrowed scan-channel descriptors.
    pub fn iter(self) -> ScanChannelIter<'a> {
        let mut source = self.bytes;
        let len = read_array_len_from(&mut source).expect("validated scan-channel array");
        debug_assert_eq!(len, self.len);
        ScanChannelIter {
            source,
            remaining: len,
        }
    }
}

/// Iterator over one borrowed decode-side scan-channel set.
pub struct ScanChannelIter<'a> {
    source: &'a [u8],
    remaining: u32,
}

impl Iterator for ScanChannelIter<'_> {
    type Item = ScanChannelDescriptorWire;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(read_scan_channel_descriptor_from(&mut self.source).expect("validated descriptor"))
    }
}

impl fmt::Debug for ScanChannelIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanChannelIter")
            .field("remaining", &self.remaining)
            .finish()
    }
}

/// Descriptor sufficient to reconstruct one `plan_flow::PlanOpen` together with
/// an outer `session_epoch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanFlowDescriptor {
    /// Plan transfer identifier.
    pub plan_id: u64,
    /// Transfer page kind mirrored from `plan_flow::PlanOpen`.
    pub page_kind: MessageKind,
    /// Transfer page flags mirrored from `plan_flow::PlanOpen`.
    pub page_flags: u16,
}

/// One scan producer role inside a logical scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ProducerRole {
    /// The leader producer owns the coordination role for the scan.
    Leader = 1,
    /// A worker producer contributes pages under the same scan.
    Worker = 2,
}

impl TryFrom<u8> for ProducerRole {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Leader),
            2 => Ok(Self::Worker),
            actual => Err(DecodeError::InvalidProducerRole { actual }),
        }
    }
}

/// Encode-side producer descriptor for one scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerDescriptorWire {
    /// Producer identifier scoped to one logical scan.
    pub producer_id: u16,
    /// Producer role inside that logical scan.
    pub role: ProducerRole,
}

/// Validation errors for one encode-side scan producer set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ProducerSetError {
    #[error("scan open must declare at least one producer")]
    EmptyProducerSet,
    #[error("duplicate producer id {producer_id} in scan open")]
    DuplicateProducerId { producer_id: u16 },
    #[error("scan open may declare at most one leader producer")]
    MultipleLeaders,
}

/// Encode-side descriptor sufficient to reconstruct one `scan_flow::ScanOpen`
/// together with an outer `session_epoch` and `scan_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanFlowDescriptor<'a> {
    /// Transfer page kind mirrored from `scan_flow::ScanOpen`.
    pub page_kind: MessageKind,
    /// Transfer page flags mirrored from `scan_flow::ScanOpen`.
    pub page_flags: u16,
    producers: &'a [ProducerDescriptorWire],
}

impl<'a> ScanFlowDescriptor<'a> {
    /// Create one encode-side scan descriptor.
    ///
    /// The producer set must satisfy the same invariants as
    /// `scan_flow::ScanOpen`: it must be non-empty, may declare at most one
    /// leader, and may not repeat `producer_id`.
    pub fn new(
        page_kind: MessageKind,
        page_flags: u16,
        producers: &'a [ProducerDescriptorWire],
    ) -> Result<Self, ProducerSetError> {
        validate_encode_producer_slice(producers)?;
        Ok(Self {
            page_kind,
            page_flags,
            producers,
        })
    }

    /// Return the validated producer slice borrowed by this descriptor.
    pub fn producers(self) -> &'a [ProducerDescriptorWire] {
        self.producers
    }
}

/// Borrowed decode-side view of one encoded producer set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerSetRef<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) len: u32,
}

impl<'a> ProducerSetRef<'a> {
    /// Return the number of encoded producers.
    pub fn len(self) -> usize {
        self.len as usize
    }

    /// Return whether the borrowed set is empty.
    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    /// Return the original validated encoded bytes.
    pub fn as_encoded(self) -> &'a [u8] {
        self.bytes
    }

    /// Iterate over the validated borrowed producer descriptors.
    pub fn iter(self) -> ProducerIter<'a> {
        let mut source = self.bytes;
        let len = read_array_len_from(&mut source).expect("validated producer array");
        debug_assert_eq!(len, self.len);
        ProducerIter {
            source,
            remaining: len,
        }
    }
}

/// Iterator over one borrowed decode-side producer set.
pub struct ProducerIter<'a> {
    source: &'a [u8],
    remaining: u32,
}

impl Iterator for ProducerIter<'_> {
    type Item = ProducerDescriptorWire;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        Some(
            read_producer_descriptor_from(&mut self.source).expect("validated producer descriptor"),
        )
    }
}

impl fmt::Debug for ProducerIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerIter")
            .field("remaining", &self.remaining)
            .finish()
    }
}

/// Borrowed decode-side scan descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanFlowDescriptorRef<'a> {
    /// Transfer page kind mirrored from `scan_flow::ScanOpen`.
    pub page_kind: MessageKind,
    /// Transfer page flags mirrored from `scan_flow::ScanOpen`.
    pub page_flags: u16,
    producers: ProducerSetRef<'a>,
}

impl<'a> ScanFlowDescriptorRef<'a> {
    /// Construct one borrowed decode-side scan descriptor from validated parts.
    pub fn new(page_kind: MessageKind, page_flags: u16, producers: ProducerSetRef<'a>) -> Self {
        Self {
            page_kind,
            page_flags,
            producers,
        }
    }

    /// Return the validated borrowed producer set.
    pub fn producers(self) -> ProducerSetRef<'a> {
        self.producers
    }
}

pub(crate) fn read_producer_descriptor_from(
    source: &mut &[u8],
) -> Result<ProducerDescriptorWire, DecodeError> {
    expect_message_len(read_array_len_from(source)?, PRODUCER_DESCRIPTOR_LEN)?;
    Ok(ProducerDescriptorWire {
        producer_id: read_u16_from(source)?,
        role: ProducerRole::try_from(read_u8_from(source)?)?,
    })
}

pub(crate) fn read_scan_channel_descriptor_from(
    source: &mut &[u8],
) -> Result<ScanChannelDescriptorWire, DecodeError> {
    expect_message_len(read_array_len_from(source)?, SCAN_CHANNEL_DESCRIPTOR_LEN)?;
    Ok(ScanChannelDescriptorWire {
        scan_id: read_u64_from(source)?,
        producer_id: read_u16_from(source)?,
        role: ProducerRole::try_from(read_u8_from(source)?)?,
        peer: BackendLeaseSlotWire::new(
            read_u32_from(source)?,
            read_u64_from(source)?,
            read_u64_from(source)?,
        ),
    })
}

pub(crate) fn write_producer_slice_to<W: std::io::Write>(
    sink: &mut W,
    producers: &[ProducerDescriptorWire],
) -> Result<(), crate::error::EncodeError> {
    let len = u32::try_from(producers.len()).map_err(|_| {
        crate::error::EncodeError::TooManyProducers {
            count: producers.len(),
        }
    })?;
    write_array_len_to(sink, len)?;
    for producer in producers {
        write_array_len_to(sink, PRODUCER_DESCRIPTOR_LEN)?;
        write_u16_to(sink, producer.producer_id)?;
        write_u8_to(sink, producer.role as u8)?;
    }
    Ok(())
}

pub(crate) fn write_scan_channel_slice_to<W: std::io::Write>(
    sink: &mut W,
    channels: &[ScanChannelDescriptorWire],
) -> Result<(), crate::error::EncodeError> {
    validate_scan_channel_slice(channels)?;
    let len = u32::try_from(channels.len()).map_err(|_| {
        crate::error::EncodeError::TooManyScanChannels {
            count: channels.len(),
        }
    })?;
    write_array_len_to(sink, len)?;
    for channel in channels {
        write_array_len_to(sink, SCAN_CHANNEL_DESCRIPTOR_LEN)?;
        write_u64_to(sink, channel.scan_id)?;
        write_u16_to(sink, channel.producer_id)?;
        write_u8_to(sink, channel.role as u8)?;
        write_u32_to(sink, channel.peer.slot_id())?;
        write_u64_to(sink, channel.peer.generation())?;
        write_u64_to(sink, channel.peer.lease_epoch())?;
    }
    Ok(())
}
