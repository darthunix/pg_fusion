//! Typed control-plane protocol carried in `control_transport` frames.
//!
//! `runtime_protocol` stays intentionally small:
//!
//! - one protocol payload fits into one `control_transport` frame
//! - `session_epoch` is always explicit
//! - plan and scan page streams are referenced through narrow descriptor types
//! - decode of scan producer descriptors is borrow-friendly and allocation-free
//!
//! The crate does not own execution state or flow runtimes. Higher layers are
//! responsible for classifying `session_epoch`, dropping stale traffic, and
//! reconstructing concrete `plan_flow` / `scan_flow` descriptors from the
//! values returned here.

use rmp::decode::{read_array_len, read_marker, read_u16, read_u64, read_u8, RmpRead};
use rmp::encode::{write_array_len, write_nil, write_u16, write_u64, write_u8};
use rmp::Marker;
use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, Write};
use thiserror::Error;
use transfer::MessageKind;

const RUNTIME_PROTOCOL_MAGIC: u32 = 0x5046_5232;
const RUNTIME_PROTOCOL_VERSION: u16 = 2;
pub const RUNTIME_ENVELOPE_HEADER_LEN: usize = 8;

const BACKEND_EXECUTION_START_TAG: u8 = 1;
const BACKEND_EXECUTION_CANCEL_TAG: u8 = 2;
const BACKEND_EXECUTION_FAIL_TAG: u8 = 3;

const WORKER_EXECUTION_COMPLETE_TAG: u8 = 1;
const WORKER_EXECUTION_FAIL_TAG: u8 = 2;
const WORKER_SCAN_OPEN_TAG: u8 = 1;
const WORKER_SCAN_CANCEL_TAG: u8 = 2;

const PRODUCER_DESCRIPTOR_LEN: u32 = 2;
const SCAN_CHANNEL_DESCRIPTOR_LEN: u32 = 4;
const PRODUCER_ID_BITMAP_WORD_BITS: usize = u64::BITS as usize;
const PRODUCER_ID_BITMAP_WORDS: usize = (u16::MAX as usize + 1) / PRODUCER_ID_BITMAP_WORD_BITS;

/// Bytes unavailable for `control_transport` payload data because framed rings
/// reserve a four-byte prefix plus one extra byte to distinguish empty from full.
pub const CONTROL_TRANSPORT_PAYLOAD_OVERHEAD: usize = 5;

/// Maximum protocol payload size that can fit into one `control_transport`
/// ring with the given raw data capacity.
pub fn max_message_len_for_ring_capacity(capacity: usize) -> usize {
    capacity.saturating_sub(CONTROL_TRANSPORT_PAYLOAD_OVERHEAD)
}

/// How an incoming `session_epoch` compares to the local current one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionDisposition {
    Current,
    Stale,
    Future,
}

/// Classify an incoming `session_epoch` against the local current one.
#[inline]
pub fn classify_session(current: u64, incoming: u64) -> SessionDisposition {
    match incoming.cmp(&current) {
        std::cmp::Ordering::Equal => SessionDisposition::Current,
        std::cmp::Ordering::Less => SessionDisposition::Stale,
        std::cmp::Ordering::Greater => SessionDisposition::Future,
    }
}

/// Runtime wire-message family carried in the fixed binary envelope header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RuntimeMessageFamily {
    BackendExecutionToWorker = 1,
    WorkerExecutionToBackend = 2,
    WorkerScanToBackend = 3,
}

impl TryFrom<u8> for RuntimeMessageFamily {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::BackendExecutionToWorker),
            2 => Ok(Self::WorkerExecutionToBackend),
            3 => Ok(Self::WorkerScanToBackend),
            actual => Err(DecodeError::UnexpectedMessageFamily { actual }),
        }
    }
}

/// Transport-agnostic wire identity of one backend lease slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BackendLeaseSlotWire {
    slot_id: u32,
    generation: u64,
    lease_epoch: u64,
}

impl BackendLeaseSlotWire {
    pub const fn new(slot_id: u32, generation: u64, lease_epoch: u64) -> Self {
        Self {
            slot_id,
            generation,
            lease_epoch,
        }
    }

    pub const fn slot_id(self) -> u32 {
        self.slot_id
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn lease_epoch(self) -> u64 {
        self.lease_epoch
    }
}

/// One execution scan channel published up front in `StartExecution`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChannelDescriptorWire {
    pub scan_id: u64,
    pub peer: BackendLeaseSlotWire,
}

/// Validation errors for one encode-side scan channel set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ScanChannelSetError {
    #[error("duplicate scan_id {scan_id} in scan channel set")]
    DuplicateScanId { scan_id: u64 },
}

/// Encode-side borrowed execution scan-channel set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChannelSet<'a> {
    channels: &'a [ScanChannelDescriptorWire],
}

impl<'a> ScanChannelSet<'a> {
    pub const fn empty() -> Self {
        Self { channels: &[] }
    }

    pub fn new(channels: &'a [ScanChannelDescriptorWire]) -> Result<Self, ScanChannelSetError> {
        validate_scan_channel_slice(channels)?;
        Ok(Self { channels })
    }

    pub fn len(self) -> usize {
        self.channels.len()
    }

    pub fn is_empty(self) -> bool {
        self.channels.is_empty()
    }

    pub fn channels(self) -> &'a [ScanChannelDescriptorWire] {
        self.channels
    }
}

/// Borrowed decode-side view of one encoded scan-channel set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChannelSetRef<'a> {
    bytes: &'a [u8],
    len: u32,
}

impl<'a> ScanChannelSetRef<'a> {
    pub const fn empty() -> Self {
        Self {
            bytes: &[0x90],
            len: 0,
        }
    }

    pub fn len(self) -> usize {
        self.len as usize
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn as_encoded(self) -> &'a [u8] {
        self.bytes
    }

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

/// Versioned failure codes for runtime control-plane failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ExecutionFailureCode {
    Cancelled = 1,
    ProtocolViolation = 2,
    TransportRestarted = 3,
    Internal = 4,
}

impl TryFrom<u8> for ExecutionFailureCode {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Cancelled),
            2 => Ok(Self::ProtocolViolation),
            3 => Ok(Self::TransportRestarted),
            4 => Ok(Self::Internal),
            actual => Err(DecodeError::InvalidFailureCode { actual }),
        }
    }
}

/// Descriptor sufficient to reconstruct one `plan_flow::PlanOpen` together with
/// an outer `session_epoch`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanFlowDescriptor {
    pub plan_id: u64,
    pub page_kind: MessageKind,
    pub page_flags: u16,
}

/// One scan producer role inside a logical scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ProducerRole {
    Leader = 1,
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
    pub producer_id: u16,
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
    pub page_kind: MessageKind,
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

    pub fn producers(self) -> &'a [ProducerDescriptorWire] {
        self.producers
    }
}

/// Borrowed decode-side view of one encoded producer set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerSetRef<'a> {
    bytes: &'a [u8],
    len: u32,
}

impl<'a> ProducerSetRef<'a> {
    pub fn len(self) -> usize {
        self.len as usize
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }

    pub fn as_encoded(self) -> &'a [u8] {
        self.bytes
    }

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
    pub page_kind: MessageKind,
    pub page_flags: u16,
    producers: ProducerSetRef<'a>,
}

impl<'a> ScanFlowDescriptorRef<'a> {
    pub fn new(page_kind: MessageKind, page_flags: u16, producers: ProducerSetRef<'a>) -> Self {
        Self {
            page_kind,
            page_flags,
            producers,
        }
    }

    pub fn producers(self) -> ProducerSetRef<'a> {
        self.producers
    }
}

/// Encode-side backend execution control messages carried on the primary slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionToWorker<'a> {
    StartExecution {
        session_epoch: u64,
        plan: PlanFlowDescriptor,
        scans: ScanChannelSet<'a>,
    },
    CancelExecution {
        session_epoch: u64,
    },
    FailExecution {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
}

impl BackendExecutionToWorker<'_> {
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::StartExecution { session_epoch, .. }
            | Self::CancelExecution { session_epoch }
            | Self::FailExecution { session_epoch, .. } => session_epoch,
        }
    }
}

/// Decode-side borrowed backend execution control messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendExecutionToWorkerRef<'a> {
    StartExecution {
        session_epoch: u64,
        plan: PlanFlowDescriptor,
        scans: ScanChannelSetRef<'a>,
    },
    CancelExecution {
        session_epoch: u64,
    },
    FailExecution {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
}

impl BackendExecutionToWorkerRef<'_> {
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::StartExecution { session_epoch, .. }
            | Self::CancelExecution { session_epoch }
            | Self::FailExecution { session_epoch, .. } => session_epoch,
        }
    }
}

/// Execution-level worker-to-backend control sent only on the primary slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerExecutionToBackend {
    CompleteExecution {
        session_epoch: u64,
    },
    FailExecution {
        session_epoch: u64,
        code: ExecutionFailureCode,
        detail: Option<u64>,
    },
}

impl WorkerExecutionToBackend {
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::CompleteExecution { session_epoch }
            | Self::FailExecution { session_epoch, .. } => session_epoch,
        }
    }
}

/// Scan-level worker-to-backend control sent only on dedicated scan slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanToBackend<'a> {
    OpenScan {
        session_epoch: u64,
        scan_id: u64,
        scan: ScanFlowDescriptor<'a>,
    },
    CancelScan {
        session_epoch: u64,
        scan_id: u64,
    },
}

impl WorkerScanToBackend<'_> {
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::OpenScan { session_epoch, .. } | Self::CancelScan { session_epoch, .. } => {
                session_epoch
            }
        }
    }
}

/// Borrowed decode-side scan-level worker-to-backend control.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerScanToBackendRef<'a> {
    OpenScan {
        session_epoch: u64,
        scan_id: u64,
        scan: ScanFlowDescriptorRef<'a>,
    },
    CancelScan {
        session_epoch: u64,
        scan_id: u64,
    },
}

impl WorkerScanToBackendRef<'_> {
    pub fn session_epoch(self) -> u64 {
        match self {
            Self::OpenScan { session_epoch, .. } | Self::CancelScan { session_epoch, .. } => {
                session_epoch
            }
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    #[error("output buffer too small: expected at least {expected} bytes, got {actual}")]
    BufferTooSmall { expected: usize, actual: usize },
    #[error("too many scan producers to encode: {count}")]
    TooManyProducers { count: usize },
    #[error("too many scan channels to encode: {count}")]
    TooManyScanChannels { count: usize },
    #[error("MsgPack encoding failed: {0}")]
    MsgPack(String),
    #[error("runtime envelope encoding failed: {0}")]
    Envelope(String),
    #[error(transparent)]
    ScanChannels(#[from] ScanChannelSetError),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("protocol array expected length {expected}, got {actual}")]
    InvalidArrayLen { expected: u32, actual: u32 },
    #[error("runtime envelope is truncated: expected at least {expected} bytes, got {actual}")]
    TruncatedEnvelope { expected: usize, actual: usize },
    #[error("invalid runtime magic: expected 0x{expected:08x}, got 0x{actual:08x}")]
    InvalidMagic { expected: u32, actual: u32 },
    #[error("unsupported runtime protocol version: expected {expected}, got {actual}")]
    UnsupportedVersion { expected: u16, actual: u16 },
    #[error("unexpected runtime message family {actual}")]
    UnexpectedMessageFamily { actual: u8 },
    #[error("unexpected message tag {actual}")]
    UnexpectedTag { actual: u8 },
    #[error("invalid failure code {actual}")]
    InvalidFailureCode { actual: u8 },
    #[error("invalid producer role {actual}")]
    InvalidProducerRole { actual: u8 },
    #[error("duplicate scan_id {scan_id} in scan channel set")]
    DuplicateScanId { scan_id: u64 },
    #[error("scan open must declare at least one producer")]
    EmptyProducerSet,
    #[error("duplicate producer id {producer_id} in scan open")]
    DuplicateProducerId { producer_id: u16 },
    #[error("scan open may declare at most one leader producer")]
    MultipleLeaders,
    #[error("decoded payload has trailing bytes: {remaining}")]
    TrailingBytes { remaining: usize },
    #[error("MsgPack decoding failed: {0}")]
    MsgPack(String),
}

pub fn encoded_len_backend_execution_to_worker(message: BackendExecutionToWorker<'_>) -> usize {
    try_encoded_len_backend_execution_to_worker(message)
        .expect("runtime_protocol backend execution length must fit into usize")
}

pub fn encoded_len_worker_execution_to_backend(message: WorkerExecutionToBackend) -> usize {
    try_encoded_len_worker_execution_to_backend(message)
        .expect("runtime_protocol worker execution length must fit into usize")
}

pub fn encoded_len_worker_scan_to_backend(message: WorkerScanToBackend<'_>) -> usize {
    try_encoded_len_worker_scan_to_backend(message)
        .expect("runtime_protocol worker scan length must fit into usize")
}

pub fn encode_backend_execution_to_worker_into(
    message: BackendExecutionToWorker<'_>,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_backend_execution_to_worker(message)?,
        out,
        |mut writer| encode_backend_execution_to_worker_to(message, &mut writer),
    )
}

pub fn encode_worker_execution_to_backend_into(
    message: WorkerExecutionToBackend,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_worker_execution_to_backend(message)?,
        out,
        |mut writer| encode_worker_execution_to_backend_to(message, &mut writer),
    )
}

pub fn encode_worker_scan_to_backend_into(
    message: WorkerScanToBackend<'_>,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_worker_scan_to_backend(message)?,
        out,
        |mut writer| encode_worker_scan_to_backend_to(message, &mut writer),
    )
}

pub fn decode_runtime_message_family(bytes: &[u8]) -> Result<RuntimeMessageFamily, DecodeError> {
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    Ok(header.family)
}

pub fn decode_backend_execution_to_worker(
    bytes: &[u8],
) -> Result<BackendExecutionToWorkerRef<'_>, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(
        header.family,
        RuntimeMessageFamily::BackendExecutionToWorker,
    )?;

    let session_epoch = read_u64_from(&mut source)?;
    let message = match header.tag {
        BACKEND_EXECUTION_START_TAG => {
            let plan = PlanFlowDescriptor {
                plan_id: read_u64_from(&mut source)?,
                page_kind: read_u16_from(&mut source)?,
                page_flags: read_u16_from(&mut source)?,
            };
            let scans = decode_scan_channel_set_ref(original, &mut source)?;
            BackendExecutionToWorkerRef::StartExecution {
                session_epoch,
                plan,
                scans,
            }
        }
        BACKEND_EXECUTION_CANCEL_TAG => {
            BackendExecutionToWorkerRef::CancelExecution { session_epoch }
        }
        BACKEND_EXECUTION_FAIL_TAG => BackendExecutionToWorkerRef::FailExecution {
            session_epoch,
            code: ExecutionFailureCode::try_from(read_u8_from(&mut source)?)?,
            detail: read_optional_u64_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

pub fn decode_worker_execution_to_backend(
    bytes: &[u8],
) -> Result<WorkerExecutionToBackend, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(
        header.family,
        RuntimeMessageFamily::WorkerExecutionToBackend,
    )?;

    let session_epoch = read_u64_from(&mut source)?;
    let message = match header.tag {
        WORKER_EXECUTION_COMPLETE_TAG => {
            WorkerExecutionToBackend::CompleteExecution { session_epoch }
        }
        WORKER_EXECUTION_FAIL_TAG => WorkerExecutionToBackend::FailExecution {
            session_epoch,
            code: ExecutionFailureCode::try_from(read_u8_from(&mut source)?)?,
            detail: read_optional_u64_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

pub fn decode_worker_scan_to_backend(
    bytes: &[u8],
) -> Result<WorkerScanToBackendRef<'_>, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(header.family, RuntimeMessageFamily::WorkerScanToBackend)?;

    let session_epoch = read_u64_from(&mut source)?;
    let message = match header.tag {
        WORKER_SCAN_OPEN_TAG => {
            let scan_id = read_u64_from(&mut source)?;
            let page_kind = read_u16_from(&mut source)?;
            let page_flags = read_u16_from(&mut source)?;
            let producers = decode_producer_set_ref(original, &mut source)?;
            WorkerScanToBackendRef::OpenScan {
                session_epoch,
                scan_id,
                scan: ScanFlowDescriptorRef::new(page_kind, page_flags, producers),
            }
        }
        WORKER_SCAN_CANCEL_TAG => WorkerScanToBackendRef::CancelScan {
            session_epoch,
            scan_id: read_u64_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

fn try_encoded_len_backend_execution_to_worker(
    message: BackendExecutionToWorker<'_>,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_backend_execution_to_worker_to(message, sink))
}

fn try_encoded_len_worker_execution_to_backend(
    message: WorkerExecutionToBackend,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_worker_execution_to_backend_to(message, sink))
}

fn try_encoded_len_worker_scan_to_backend(
    message: WorkerScanToBackend<'_>,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_worker_scan_to_backend_to(message, sink))
}

fn encode_backend_execution_to_worker_to<W: Write>(
    message: BackendExecutionToWorker<'_>,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        BackendExecutionToWorker::StartExecution {
            session_epoch,
            plan,
            scans,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendExecutionToWorker,
                BACKEND_EXECUTION_START_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, plan.plan_id)?;
            write_u16_to(sink, plan.page_kind)?;
            write_u16_to(sink, plan.page_flags)?;
            write_scan_channel_slice_to(sink, scans.channels())?;
        }
        BackendExecutionToWorker::CancelExecution { session_epoch } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendExecutionToWorker,
                BACKEND_EXECUTION_CANCEL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
        }
        BackendExecutionToWorker::FailExecution {
            session_epoch,
            code,
            detail,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendExecutionToWorker,
                BACKEND_EXECUTION_FAIL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u8_to(sink, code as u8)?;
            write_optional_u64_to(sink, detail)?;
        }
    }
    Ok(())
}

fn encode_worker_execution_to_backend_to<W: Write>(
    message: WorkerExecutionToBackend,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        WorkerExecutionToBackend::CompleteExecution { session_epoch } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerExecutionToBackend,
                WORKER_EXECUTION_COMPLETE_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
        }
        WorkerExecutionToBackend::FailExecution {
            session_epoch,
            code,
            detail,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerExecutionToBackend,
                WORKER_EXECUTION_FAIL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u8_to(sink, code as u8)?;
            write_optional_u64_to(sink, detail)?;
        }
    }
    Ok(())
}

fn encode_worker_scan_to_backend_to<W: Write>(
    message: WorkerScanToBackend<'_>,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        WorkerScanToBackend::OpenScan {
            session_epoch,
            scan_id,
            scan,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerScanToBackend,
                WORKER_SCAN_OPEN_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, scan_id)?;
            write_u16_to(sink, scan.page_kind)?;
            write_u16_to(sink, scan.page_flags)?;
            write_producer_slice_to(sink, scan.producers())?;
        }
        WorkerScanToBackend::CancelScan {
            session_epoch,
            scan_id,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerScanToBackend,
                WORKER_SCAN_CANCEL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, scan_id)?;
        }
    }
    Ok(())
}

fn decode_producer_set_ref<'a>(
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

fn decode_scan_channel_set_ref<'a>(
    original: &'a [u8],
    source: &mut &'a [u8],
) -> Result<ScanChannelSetRef<'a>, DecodeError> {
    let start = original.len() - source.len();
    let count = read_array_len_from(source)?;
    let mut seen_ids = BTreeSet::new();

    for _ in 0..count {
        let descriptor = read_scan_channel_descriptor_from(source)?;
        if !seen_ids.insert(descriptor.scan_id) {
            return Err(DecodeError::DuplicateScanId {
                scan_id: descriptor.scan_id,
            });
        }
    }

    let end = original.len() - source.len();
    Ok(ScanChannelSetRef {
        bytes: &original[start..end],
        len: count,
    })
}

fn validate_encode_producer_slice(
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

fn validate_scan_channel_slice(
    channels: &[ScanChannelDescriptorWire],
) -> Result<(), ScanChannelSetError> {
    let mut seen_ids = BTreeSet::new();
    for channel in channels {
        if !seen_ids.insert(channel.scan_id) {
            return Err(ScanChannelSetError::DuplicateScanId {
                scan_id: channel.scan_id,
            });
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

fn read_producer_descriptor_from(
    source: &mut &[u8],
) -> Result<ProducerDescriptorWire, DecodeError> {
    expect_message_len(read_array_len_from(source)?, PRODUCER_DESCRIPTOR_LEN)?;
    Ok(ProducerDescriptorWire {
        producer_id: read_u16_from(source)?,
        role: ProducerRole::try_from(read_u8_from(source)?)?,
    })
}

fn read_scan_channel_descriptor_from(
    source: &mut &[u8],
) -> Result<ScanChannelDescriptorWire, DecodeError> {
    expect_message_len(read_array_len_from(source)?, SCAN_CHANNEL_DESCRIPTOR_LEN)?;
    Ok(ScanChannelDescriptorWire {
        scan_id: read_u64_from(source)?,
        peer: BackendLeaseSlotWire::new(
            read_u32_from(source)?,
            read_u64_from(source)?,
            read_u64_from(source)?,
        ),
    })
}

fn write_producer_slice_to<W: Write>(
    sink: &mut W,
    producers: &[ProducerDescriptorWire],
) -> Result<(), EncodeError> {
    let len = u32::try_from(producers.len()).map_err(|_| EncodeError::TooManyProducers {
        count: producers.len(),
    })?;
    write_array_len_to(sink, len)?;
    for producer in producers {
        write_array_len_to(sink, PRODUCER_DESCRIPTOR_LEN)?;
        write_u16_to(sink, producer.producer_id)?;
        write_u8_to(sink, producer.role as u8)?;
    }
    Ok(())
}

fn write_scan_channel_slice_to<W: Write>(
    sink: &mut W,
    channels: &[ScanChannelDescriptorWire],
) -> Result<(), EncodeError> {
    validate_scan_channel_slice(channels)?;
    let len = u32::try_from(channels.len()).map_err(|_| EncodeError::TooManyScanChannels {
        count: channels.len(),
    })?;
    write_array_len_to(sink, len)?;
    for channel in channels {
        write_array_len_to(sink, SCAN_CHANNEL_DESCRIPTOR_LEN)?;
        write_u64_to(sink, channel.scan_id)?;
        write_u32_to(sink, channel.peer.slot_id())?;
        write_u64_to(sink, channel.peer.generation())?;
        write_u64_to(sink, channel.peer.lease_epoch())?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RuntimeEnvelopeHeader {
    family: RuntimeMessageFamily,
    tag: u8,
}

fn decode_runtime_header(source: &mut &[u8]) -> Result<RuntimeEnvelopeHeader, DecodeError> {
    if source.len() < RUNTIME_ENVELOPE_HEADER_LEN {
        return Err(DecodeError::TruncatedEnvelope {
            expected: RUNTIME_ENVELOPE_HEADER_LEN,
            actual: source.len(),
        });
    }

    let (header, tail) = source.split_at(RUNTIME_ENVELOPE_HEADER_LEN);
    *source = tail;

    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != RUNTIME_PROTOCOL_MAGIC {
        return Err(DecodeError::InvalidMagic {
            expected: RUNTIME_PROTOCOL_MAGIC,
            actual: magic,
        });
    }

    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != RUNTIME_PROTOCOL_VERSION {
        return Err(DecodeError::UnsupportedVersion {
            expected: RUNTIME_PROTOCOL_VERSION,
            actual: version,
        });
    }

    Ok(RuntimeEnvelopeHeader {
        family: RuntimeMessageFamily::try_from(header[6])?,
        tag: header[7],
    })
}

fn write_runtime_header_to<W: Write>(
    sink: &mut W,
    family: RuntimeMessageFamily,
    tag: u8,
) -> Result<(), EncodeError> {
    sink.write_all(&RUNTIME_PROTOCOL_MAGIC.to_be_bytes())
        .map_err(|error| EncodeError::Envelope(error.to_string()))?;
    sink.write_all(&RUNTIME_PROTOCOL_VERSION.to_be_bytes())
        .map_err(|error| EncodeError::Envelope(error.to_string()))?;
    sink.write_all(&[family as u8, tag])
        .map_err(|error| EncodeError::Envelope(error.to_string()))?;
    Ok(())
}

fn expect_runtime_family(
    actual: RuntimeMessageFamily,
    expected: RuntimeMessageFamily,
) -> Result<(), DecodeError> {
    if actual == expected {
        Ok(())
    } else {
        Err(DecodeError::UnexpectedMessageFamily {
            actual: actual as u8,
        })
    }
}

fn write_optional_u64_to<W: Write>(sink: &mut W, value: Option<u64>) -> Result<(), EncodeError> {
    match value {
        Some(value) => write_u64_to(sink, value),
        None => write_nil_to(sink),
    }
}

fn read_optional_u64_from(source: &mut &[u8]) -> Result<Option<u64>, DecodeError> {
    match read_marker(source).map_err(|error| DecodeError::MsgPack(format!("{error:?}")))? {
        Marker::Null => Ok(None),
        Marker::FixPos(value) => Ok(Some(value as u64)),
        Marker::U8 => source
            .read_data_u8()
            .map(|value| Some(value as u64))
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        Marker::U16 => source
            .read_data_u16()
            .map(|value| Some(value as u64))
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        Marker::U32 => source
            .read_data_u32()
            .map(|value| Some(value as u64))
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        Marker::U64 => source
            .read_data_u64()
            .map(Some)
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        marker => Err(DecodeError::MsgPack(format!(
            "expected nil or unsigned integer detail, got {marker:?}"
        ))),
    }
}

fn ensure_no_trailing_bytes(original: &[u8], remaining: &[u8]) -> Result<(), DecodeError> {
    if remaining.is_empty() {
        return Ok(());
    }
    let consumed = original.len() - remaining.len();
    let _ = consumed;
    Err(DecodeError::TrailingBytes {
        remaining: remaining.len(),
    })
}

fn expect_message_len(actual: u32, expected: u32) -> Result<(), DecodeError> {
    if actual != expected {
        return Err(DecodeError::InvalidArrayLen { expected, actual });
    }
    Ok(())
}

fn encoded_len_with<F>(encode: F) -> Result<usize, EncodeError>
where
    F: FnOnce(&mut CountingWriter) -> Result<(), EncodeError>,
{
    let mut sink = CountingWriter::default();
    encode(&mut sink)?;
    Ok(sink.written)
}

fn encode_into_with_len<F>(expected: usize, out: &mut [u8], encode: F) -> Result<usize, EncodeError>
where
    F: FnOnce(&mut [u8]) -> Result<(), EncodeError>,
{
    if out.len() < expected {
        return Err(EncodeError::BufferTooSmall {
            expected,
            actual: out.len(),
        });
    }

    let writer = &mut out[..expected];
    encode(writer)?;
    Ok(expected)
}

#[derive(Default)]
struct CountingWriter {
    written: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.written = self.written.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn read_array_len_from(source: &mut &[u8]) -> Result<u32, DecodeError> {
    read_array_len(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn read_u8_from(source: &mut &[u8]) -> Result<u8, DecodeError> {
    read_u8(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn read_u32_from(source: &mut &[u8]) -> Result<u32, DecodeError> {
    rmp::decode::read_u32(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn read_u16_from(source: &mut &[u8]) -> Result<u16, DecodeError> {
    read_u16(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn read_u64_from(source: &mut &[u8]) -> Result<u64, DecodeError> {
    read_u64(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_array_len_to<W: Write>(sink: &mut W, len: u32) -> Result<(), EncodeError> {
    write_array_len(sink, len)
        .map(|_| ())
        .map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn write_u8_to<W: Write>(sink: &mut W, value: u8) -> Result<(), EncodeError> {
    write_u8(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn write_u32_to<W: Write>(sink: &mut W, value: u32) -> Result<(), EncodeError> {
    rmp::encode::write_u32(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn write_u16_to<W: Write>(sink: &mut W, value: u16) -> Result<(), EncodeError> {
    write_u16(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn write_u64_to<W: Write>(sink: &mut W, value: u64) -> Result<(), EncodeError> {
    write_u64(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn write_nil_to<W: Write>(sink: &mut W) -> Result<(), EncodeError> {
    write_nil(sink).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

#[cfg(test)]
mod tests;
