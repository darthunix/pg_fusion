//! Versioned logical-plan codec for backend-built PostgreSQL scan plans.
//!
//! `plan_codec` provides symmetric streaming sessions:
//!
//! - [`PlanEncodeSession`] writes a plan incrementally into caller-provided
//!   fixed slices without staging the full serialized payload in memory
//! - [`PlanDecodeSession`] consumes the same byte stream page-by-page and
//!   rebuilds the logical plan once the full envelope has arrived
//!
//! The wire format is intentionally layered:
//!
//! - an outer crate-owned MsgPack envelope
//! - a built-in DataFusion logical plan encoded through `datafusion-proto`
//! - `scan_node::PgScanNode` payloads reduced to a tiny private `scan_id`
//!   reference inside that protobuf plan
//! - a separate MsgPack table of full [`scan_node::PgScanSpec`] values
//!
//! This keeps DataFusion-owned logical / expression serialization delegated to
//! `datafusion-proto`, while avoiding a single staged byte buffer for the full
//! serialized plan payload. Protobuf payloads may be re-encoded multiple times
//! while the encoder streams across small output chunks.
//!
//! The codec is intentionally scoped to plans emitted by `pg/plan_builder`.
//! Expr-level subqueries are out of scope and are rejected by `plan_builder`
//! before serialization.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::{buf::UninitSlice, Buf, BufMut, Bytes, BytesMut};
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result as DataFusionResult, TableReference};
use datafusion_expr::logical_plan::{Extension, LogicalPlan};
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use datafusion_proto::protobuf;
use prost::Message;
use rmp::decode::{
    read_array_len, read_bin_len, read_bool, read_str_len, read_u32, read_u64, read_u8,
};
use rmp::encode::{
    write_array_len, write_bin_len, write_bool, write_str, write_u32, write_u64, write_u8,
};
use scan_node::{PgScanFetchHints, PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{CompiledFilter, CompiledScan, PgRelation};
use smallvec::SmallVec;
use thiserror::Error;

mod fsm;

const PLAN_CODEC_MAGIC: &str = "PFPL";
const PLAN_CODEC_VERSION: u8 = 1;
const PLAN_CODEC_ENVELOPE_LEN: u32 = 4;

const PG_SCAN_SPEC_VERSION: u8 = 1;
const PG_SCAN_SPEC_LEN: u32 = 6;
const PG_SCAN_RELATION_LEN: u32 = 2;
const PG_SCAN_FETCH_HINTS_LEN: u32 = 2;
const PG_SCAN_COMPILED_SCAN_LEN: u32 = 11;
const PG_SCAN_PUSHED_FILTER_LEN: u32 = 2;

const PG_SCAN_ID_PAYLOAD_VERSION: u8 = 1;
const PG_SCAN_ID_PAYLOAD_LEN: usize = 1 + std::mem::size_of::<u64>();

const BUF_SCRATCH_LEN: usize = 64;

/// Progress from one call to [`PlanEncodeSession::write_chunk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeProgress {
    NeedMoreOutput { written: usize },
    Done { written: usize },
}

/// Progress from one call to a decode session method.
#[derive(Debug)]
pub enum DecodeProgress {
    NeedMoreInput,
    Done(Box<LogicalPlan>),
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("plan encode chunk must not be empty")]
    EmptyOutputChunk,
    #[error("encode session is poisoned in state {state}: {reason}")]
    SessionFailed { state: String, reason: String },
    #[error("duplicate PgScanId in logical plan: {scan_id}")]
    DuplicateScanId { scan_id: u64 },
    #[error("too many PgScan specs to encode: {count}")]
    TooManyScanSpecs { count: usize },
    #[error("protobuf payload too large: {len} bytes")]
    PayloadTooLarge { len: usize },
    #[error("encoder FSM rejected transition: {0}")]
    StateMachine(String),
    #[error("logical plan serialization failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("protobuf encoding failed: {0}")]
    Protobuf(#[from] prost::EncodeError),
    #[error("MsgPack encoding failed: {0}")]
    MsgPack(String),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("decode session is already finished")]
    AlreadyFinished,
    #[error("decode session is poisoned in state {state}: {reason}")]
    SessionFailed { state: String, reason: String },
    #[error("unexpected EOF while decoding in state {state}; buffered {buffered} bytes")]
    UnexpectedEof { state: String, buffered: usize },
    #[error("plan envelope expected MsgPack array of length {expected}, got {actual}")]
    InvalidEnvelope { expected: u32, actual: u32 },
    #[error("invalid plan magic: expected {expected:?}, got {actual:?}")]
    InvalidMagic {
        expected: &'static str,
        actual: String,
    },
    #[error("unsupported plan codec version: {version}")]
    UnsupportedVersion { version: u8 },
    #[error("missing PgScanSpec for scan_id={scan_id}")]
    MissingScanSpec { scan_id: u64 },
    #[error("orphan PgScanSpec not referenced by decoded plan: scan_id={scan_id}")]
    OrphanScanSpec { scan_id: u64 },
    #[error("duplicate PgScanId in decoded plan: {scan_id}")]
    DuplicateScanId { scan_id: u64 },
    #[error("invalid PgScan reference payload: {0}")]
    InvalidScanPayload(String),
    #[error("decoded payload has trailing bytes: {remaining}")]
    TrailingBytes { remaining: usize },
    #[error("protobuf decoding failed: {0}")]
    Protobuf(String),
    #[error("logical plan deserialization failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("decoder FSM rejected transition: {0}")]
    StateMachine(String),
    #[error("MsgPack decoding failed: {0}")]
    MsgPack(String),
}

#[derive(Debug)]
struct PlanEnvelope {
    pg_scan_specs: BTreeMap<PgScanId, Arc<PgScanSpec>>,
    logical_plan: protobuf::LogicalPlanNode,
}

#[derive(Debug, Clone)]
struct SessionFailure {
    state: String,
    reason: String,
}

#[derive(Debug, Clone, Copy)]
enum EncodeItemKind {
    EnvelopeArrayLen,
    Magic,
    Version,
    ScanSpecsArrayLen,
    ScanSpec { index: usize },
    LogicalPlanBinLen,
    LogicalPlanBin,
}

#[derive(Debug, Clone, Copy)]
struct ActiveEncodeItem {
    kind: EncodeItemKind,
    len: usize,
    emitted: usize,
}

/// Streaming encoder for one logical plan payload.
pub struct PlanEncodeSession {
    envelope: PlanEnvelope,
    ordered_scan_specs: Vec<Arc<PgScanSpec>>,
    scan_spec_lens: Vec<usize>,
    logical_plan_len: usize,
    machine: fsm::encode_flow::StateMachine,
    scan_spec_index: usize,
    active_item: Option<ActiveEncodeItem>,
    finished: bool,
    failed: Option<SessionFailure>,
}

impl PlanEncodeSession {
    /// Create a new streaming encoder for one logical plan.
    pub fn new(plan: &LogicalPlan) -> Result<Self, EncodeError> {
        let envelope = collect_plan_envelope(plan)?;
        let ordered_scan_specs = envelope.pg_scan_specs.values().cloned().collect::<Vec<_>>();
        let mut scan_spec_lens = Vec::with_capacity(ordered_scan_specs.len());
        for spec in &ordered_scan_specs {
            scan_spec_lens.push(encoded_len_with(|sink| {
                encode_pg_scan_spec_into(sink, spec)
            })?);
        }

        Ok(Self {
            logical_plan_len: envelope.logical_plan.encoded_len(),
            envelope,
            ordered_scan_specs,
            scan_spec_lens,
            machine: fsm::encode_flow::StateMachine::new(),
            scan_spec_index: 0,
            active_item: None,
            finished: false,
            failed: None,
        })
    }

    /// Return whether the encoder has emitted the full payload.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Write the next encoded bytes into the provided output slice.
    pub fn write_chunk(&mut self, out: &mut [u8]) -> Result<EncodeProgress, EncodeError> {
        if let Some(failure) = &self.failed {
            return Err(EncodeError::SessionFailed {
                state: failure.state.clone(),
                reason: failure.reason.clone(),
            });
        }
        if self.finished {
            return Ok(EncodeProgress::Done { written: 0 });
        }
        if out.is_empty() {
            return Err(EncodeError::EmptyOutputChunk);
        }

        let result = self.write_chunk_inner(out);
        if let Err(error) = &result {
            if should_poison_encode_error(error) {
                self.poison_encode(error);
            }
        }
        result
    }

    fn write_chunk_inner(&mut self, out: &mut [u8]) -> Result<EncodeProgress, EncodeError> {
        let mut written = 0usize;
        loop {
            self.ensure_encode_state_ready()?;

            if self.finished {
                return Ok(EncodeProgress::Done { written });
            }

            let (kind, emitted) = {
                let item = self
                    .active_item
                    .as_ref()
                    .expect("active item must exist once encoder is ready");
                (item.kind, item.emitted)
            };
            let copied = self.encode_item_range(kind, emitted, &mut out[written..])?;
            let item = self
                .active_item
                .as_mut()
                .expect("active item must exist once encoder is ready");
            item.emitted += copied;
            written += copied;

            if item.emitted == item.len {
                self.finish_active_item()?;
                if self.finished {
                    return Ok(EncodeProgress::Done { written });
                }
                if written == out.len() {
                    return Ok(EncodeProgress::NeedMoreOutput { written });
                }
                continue;
            }

            return Ok(EncodeProgress::NeedMoreOutput { written });
        }
    }

    fn ensure_encode_state_ready(&mut self) -> Result<(), EncodeError> {
        loop {
            match self.machine.state() {
                fsm::EncodeState::Start => {
                    consume_encode_event(&mut self.machine, fsm::EncodeEvent::Begin)?;
                }
                fsm::EncodeState::Failed => {
                    self.active_item = None;
                    return Ok(());
                }
                fsm::EncodeState::Done => {
                    self.finished = true;
                    self.active_item = None;
                    return Ok(());
                }
                _ if self.active_item.is_none() => {
                    let kind = self.current_encode_item_kind();
                    let len = self.item_len(kind)?;
                    self.active_item = Some(ActiveEncodeItem {
                        kind,
                        len,
                        emitted: 0,
                    });
                    return Ok(());
                }
                _ => return Ok(()),
            }
        }
    }

    fn current_encode_item_kind(&self) -> EncodeItemKind {
        match self.machine.state() {
            fsm::EncodeState::EnvelopeArrayLen => EncodeItemKind::EnvelopeArrayLen,
            fsm::EncodeState::Magic => EncodeItemKind::Magic,
            fsm::EncodeState::Version => EncodeItemKind::Version,
            fsm::EncodeState::ScanSpecsArrayLen => EncodeItemKind::ScanSpecsArrayLen,
            fsm::EncodeState::ScanSpecs => EncodeItemKind::ScanSpec {
                index: self.scan_spec_index,
            },
            fsm::EncodeState::LogicalPlanBinLen => EncodeItemKind::LogicalPlanBinLen,
            fsm::EncodeState::LogicalPlanBin => EncodeItemKind::LogicalPlanBin,
            state => unreachable!("unexpected encode state without active item: {state:?}"),
        }
    }

    fn item_len(&self, kind: EncodeItemKind) -> Result<usize, EncodeError> {
        match kind {
            EncodeItemKind::EnvelopeArrayLen => {
                encoded_len_with(|sink| write_array_len_to(sink, PLAN_CODEC_ENVELOPE_LEN))
            }
            EncodeItemKind::Magic => {
                encoded_len_with(|sink| write_string_to(sink, PLAN_CODEC_MAGIC))
            }
            EncodeItemKind::Version => {
                encoded_len_with(|sink| write_u8_to(sink, PLAN_CODEC_VERSION))
            }
            EncodeItemKind::ScanSpecsArrayLen => encoded_len_with(|sink| {
                write_array_len_to(
                    sink,
                    u32::try_from(self.ordered_scan_specs.len()).map_err(|_| {
                        EncodeError::TooManyScanSpecs {
                            count: self.ordered_scan_specs.len(),
                        }
                    })?,
                )
            }),
            EncodeItemKind::ScanSpec { index } => Ok(self.scan_spec_lens[index]),
            EncodeItemKind::LogicalPlanBinLen => {
                encoded_len_with(|sink| write_bin_len_to(sink, self.logical_plan_len))
            }
            EncodeItemKind::LogicalPlanBin => Ok(self.logical_plan_len),
        }
    }

    fn encode_item_range(
        &self,
        kind: EncodeItemKind,
        offset: usize,
        out: &mut [u8],
    ) -> Result<usize, EncodeError> {
        let mut sink = OverlapBufMut::new(out, offset);
        match kind {
            EncodeItemKind::EnvelopeArrayLen => {
                write_array_len_to(&mut sink, PLAN_CODEC_ENVELOPE_LEN)?
            }
            EncodeItemKind::Magic => write_string_to(&mut sink, PLAN_CODEC_MAGIC)?,
            EncodeItemKind::Version => write_u8_to(&mut sink, PLAN_CODEC_VERSION)?,
            EncodeItemKind::ScanSpecsArrayLen => write_array_len_to(
                &mut sink,
                u32::try_from(self.ordered_scan_specs.len()).map_err(|_| {
                    EncodeError::TooManyScanSpecs {
                        count: self.ordered_scan_specs.len(),
                    }
                })?,
            )?,
            EncodeItemKind::ScanSpec { index } => {
                encode_pg_scan_spec_into(&mut sink, &self.ordered_scan_specs[index])?
            }
            EncodeItemKind::LogicalPlanBinLen => {
                write_bin_len_to(&mut sink, self.logical_plan_len)?
            }
            EncodeItemKind::LogicalPlanBin => self.envelope.logical_plan.encode(&mut sink)?,
        }
        Ok(sink.written())
    }

    fn finish_active_item(&mut self) -> Result<(), EncodeError> {
        let active = self
            .active_item
            .take()
            .expect("active encode item must be present");
        match active.kind {
            EncodeItemKind::EnvelopeArrayLen
            | EncodeItemKind::Magic
            | EncodeItemKind::Version
            | EncodeItemKind::LogicalPlanBinLen => {
                consume_encode_event(&mut self.machine, fsm::EncodeEvent::AtomFinished)?;
            }
            EncodeItemKind::ScanSpecsArrayLen => {
                if self.scan_spec_lens.is_empty() {
                    consume_encode_event(&mut self.machine, fsm::EncodeEvent::ScanSpecsFinished)?;
                } else {
                    consume_encode_event(&mut self.machine, fsm::EncodeEvent::ScanSpecsStarted)?;
                }
            }
            EncodeItemKind::ScanSpec { .. } => {
                self.scan_spec_index += 1;
                if self.scan_spec_index == self.scan_spec_lens.len() {
                    consume_encode_event(&mut self.machine, fsm::EncodeEvent::ScanSpecsFinished)?;
                } else {
                    consume_encode_event(&mut self.machine, fsm::EncodeEvent::ScanSpecFinished)?;
                }
            }
            EncodeItemKind::LogicalPlanBin => {
                consume_encode_event(&mut self.machine, fsm::EncodeEvent::LogicalPlanFinished)?;
                self.finished = matches!(self.machine.state(), fsm::EncodeState::Done);
            }
        }
        Ok(())
    }

    fn poison_encode(&mut self, error: &EncodeError) {
        if self.failed.is_some() || self.finished {
            return;
        }

        let state = format!("{:?}", self.machine.state());
        self.active_item = None;
        self.failed = Some(SessionFailure {
            state: state.clone(),
            reason: error.to_string(),
        });

        if !matches!(
            self.machine.state(),
            fsm::EncodeState::Failed | fsm::EncodeState::Done
        ) {
            let _ = consume_encode_event(&mut self.machine, fsm::EncodeEvent::Fail);
        }
    }
}

/// Streaming decoder for one logical plan payload.
pub struct PlanDecodeSession {
    ctx: SessionContext,
    machine: fsm::decode_flow::StateMachine,
    control_buf: BytesMut,
    scan_specs_remaining: Option<usize>,
    pg_scan_specs: BTreeMap<PgScanId, Arc<PgScanSpec>>,
    logical_plan_len: Option<usize>,
    logical_plan_received: usize,
    logical_plan_segments: SmallVec<[Bytes; 4]>,
    logical_plan: Option<protobuf::LogicalPlanNode>,
    ready_plan: Option<Box<LogicalPlan>>,
    finished: bool,
    failed: Option<SessionFailure>,
}

impl PlanDecodeSession {
    /// Create a new streaming decoder.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            machine: fsm::decode_flow::StateMachine::new(),
            control_buf: BytesMut::new(),
            scan_specs_remaining: None,
            pg_scan_specs: BTreeMap::new(),
            logical_plan_len: None,
            logical_plan_received: 0,
            logical_plan_segments: SmallVec::new(),
            logical_plan: None,
            ready_plan: None,
            finished: false,
            failed: None,
        }
    }

    /// Return whether the decoder has already observed EOF and emitted the plan.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Push the next input chunk into the decoder.
    ///
    /// This never returns [`DecodeProgress::Done`]. A decoded plan becomes
    /// ready only after the session has parsed a full envelope; the caller
    /// must then invoke [`Self::finish_input`] to validate EOF at the plan
    /// boundary and receive the final plan.
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<DecodeProgress, DecodeError> {
        if let Some(failure) = &self.failed {
            return Err(DecodeError::SessionFailed {
                state: failure.state.clone(),
                reason: failure.reason.clone(),
            });
        }
        if self.finished {
            return Err(DecodeError::AlreadyFinished);
        }

        self.buffer_decode_chunk(chunk);

        let result = self.drive_decode(false);
        if let Err(error) = &result {
            self.poison_decode(error);
        }
        result
    }

    /// Signal that the input stream has reached EOF.
    ///
    /// If the buffered bytes are still insufficient to complete the current
    /// decode state, this returns [`DecodeError::UnexpectedEof`].
    pub fn finish_input(&mut self) -> Result<DecodeProgress, DecodeError> {
        if let Some(failure) = &self.failed {
            return Err(DecodeError::SessionFailed {
                state: failure.state.clone(),
                reason: failure.reason.clone(),
            });
        }
        if self.finished {
            return Err(DecodeError::AlreadyFinished);
        }

        let result = self.drive_decode(true);
        if let Err(error) = &result {
            self.poison_decode(error);
        }
        result
    }

    fn drive_decode(&mut self, eof: bool) -> Result<DecodeProgress, DecodeError> {
        loop {
            self.ensure_decode_state_ready()?;
            match self.machine.state() {
                fsm::DecodeState::Failed => {
                    let failure = self
                        .failed
                        .as_ref()
                        .expect("failed decode state must carry error");
                    return Err(DecodeError::SessionFailed {
                        state: failure.state.clone(),
                        reason: failure.reason.clone(),
                    });
                }
                fsm::DecodeState::EnvelopeArrayLen => {
                    let Some((len, consumed)) =
                        try_parse_prefix(&self.control_buf, |source: &mut &[u8]| {
                            read_array_len_from(source)
                        })?
                    else {
                        return self.decode_need_more(eof);
                    };
                    if len != PLAN_CODEC_ENVELOPE_LEN {
                        return Err(DecodeError::InvalidEnvelope {
                            expected: PLAN_CODEC_ENVELOPE_LEN,
                            actual: len,
                        });
                    }
                    self.discard_control_prefix(consumed);
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::AtomDecoded)?;
                }
                fsm::DecodeState::Magic => {
                    let Some((magic, consumed)) = try_parse_prefix(&self.control_buf, |source| {
                        read_string_from(source, "plan magic")
                    })?
                    else {
                        return self.decode_need_more(eof);
                    };
                    if magic != PLAN_CODEC_MAGIC {
                        return Err(DecodeError::InvalidMagic {
                            expected: PLAN_CODEC_MAGIC,
                            actual: magic,
                        });
                    }
                    self.discard_control_prefix(consumed);
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::AtomDecoded)?;
                }
                fsm::DecodeState::Version => {
                    let Some((version, consumed)) =
                        try_parse_prefix(&self.control_buf, |source: &mut &[u8]| {
                            read_u8_from(source)
                        })?
                    else {
                        return self.decode_need_more(eof);
                    };
                    if version != PLAN_CODEC_VERSION {
                        return Err(DecodeError::UnsupportedVersion { version });
                    }
                    self.discard_control_prefix(consumed);
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::AtomDecoded)?;
                }
                fsm::DecodeState::ScanSpecsArrayLen => {
                    let Some((len, consumed)) =
                        try_parse_prefix(&self.control_buf, |source: &mut &[u8]| {
                            read_array_len_from(source)
                        })?
                    else {
                        return self.decode_need_more(eof);
                    };
                    self.scan_specs_remaining = Some(len as usize);
                    self.discard_control_prefix(consumed);
                    if len == 0 {
                        consume_decode_event(
                            &mut self.machine,
                            fsm::DecodeEvent::ScanSpecsFinished,
                        )?;
                    } else {
                        consume_decode_event(
                            &mut self.machine,
                            fsm::DecodeEvent::ScanSpecsStarted,
                        )?;
                    }
                }
                fsm::DecodeState::ScanSpecs => {
                    let Some((spec, consumed)) = try_parse_prefix(&self.control_buf, |source| {
                        decode_pg_scan_spec_from(source, &self.ctx)
                    })?
                    else {
                        return self.decode_need_more(eof);
                    };
                    let spec = Arc::new(spec);
                    if self
                        .pg_scan_specs
                        .insert(spec.scan_id, Arc::clone(&spec))
                        .is_some()
                    {
                        return Err(DecodeError::DuplicateScanId {
                            scan_id: spec.scan_id.get(),
                        });
                    }
                    self.discard_control_prefix(consumed);
                    let remaining = self
                        .scan_specs_remaining
                        .as_mut()
                        .expect("scan spec count must be set while decoding specs");
                    *remaining -= 1;
                    if *remaining == 0 {
                        consume_decode_event(
                            &mut self.machine,
                            fsm::DecodeEvent::ScanSpecsFinished,
                        )?;
                    } else {
                        consume_decode_event(&mut self.machine, fsm::DecodeEvent::ScanSpecDecoded)?;
                    }
                }
                fsm::DecodeState::LogicalPlanBinLen => {
                    let Some((len, consumed)) =
                        try_parse_prefix(&self.control_buf, |source: &mut &[u8]| {
                            read_bin_len_from(source)
                        })?
                    else {
                        return self.decode_need_more(eof);
                    };
                    self.logical_plan_len = Some(len as usize);
                    self.discard_control_prefix(consumed);
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::AtomDecoded)?;
                }
                fsm::DecodeState::LogicalPlanBin => {
                    let len = self
                        .logical_plan_len
                        .expect("logical plan length must be set before payload decode");
                    self.drain_control_into_logical_plan_segments();
                    if self.logical_plan_received < len {
                        return self.decode_need_more(eof);
                    }

                    let mut payload = SegmentedSource::new(&self.logical_plan_segments, len);
                    let logical_plan =
                        protobuf::LogicalPlanNode::decode(&mut payload).map_err(|error| {
                            DecodeError::Protobuf(format!("failed to decode logical plan: {error}"))
                        })?;
                    if payload.remaining() != 0 {
                        return Err(DecodeError::Protobuf(format!(
                            "logical plan protobuf payload has {} trailing bytes",
                            payload.remaining()
                        )));
                    }
                    self.logical_plan = Some(logical_plan);
                    self.logical_plan_len = None;
                    self.logical_plan_received = 0;
                    self.logical_plan_segments.clear();
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::LogicalPlanDecoded)?;
                }
                fsm::DecodeState::BuildLogicalPlan => {
                    let plan = self.build_decoded_plan()?;
                    if self.control_buf.has_remaining() {
                        return Err(DecodeError::TrailingBytes {
                            remaining: self.control_buf.remaining(),
                        });
                    }
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::LogicalPlanBuilt)?;
                    self.ready_plan = Some(Box::new(plan));
                }
                fsm::DecodeState::AwaitEof => {
                    if self.control_buf.has_remaining() {
                        return Err(DecodeError::TrailingBytes {
                            remaining: self.control_buf.remaining(),
                        });
                    }
                    if eof {
                        consume_decode_event(&mut self.machine, fsm::DecodeEvent::Eof)?;
                        self.finished = true;
                        let plan = self.ready_plan.take().ok_or_else(|| {
                            DecodeError::MsgPack(
                                "decoded logical plan is missing while awaiting EOF".into(),
                            )
                        })?;
                        return Ok(DecodeProgress::Done(plan));
                    }
                    return Ok(DecodeProgress::NeedMoreInput);
                }
                fsm::DecodeState::Done => {
                    self.finished = true;
                    return Err(DecodeError::AlreadyFinished);
                }
                fsm::DecodeState::Start => {
                    consume_decode_event(&mut self.machine, fsm::DecodeEvent::Begin)?;
                }
            }
        }
    }

    fn ensure_decode_state_ready(&mut self) -> Result<(), DecodeError> {
        if matches!(self.machine.state(), fsm::DecodeState::Start) {
            consume_decode_event(&mut self.machine, fsm::DecodeEvent::Begin)?;
        }
        Ok(())
    }

    fn buffer_decode_chunk(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }

        if matches!(self.machine.state(), fsm::DecodeState::LogicalPlanBin)
            && self.logical_plan_len.is_some()
        {
            self.drain_control_into_logical_plan_segments();
            let mut consumed = 0usize;
            let needed = self
                .logical_plan_len
                .expect("logical plan length must exist in payload state")
                .saturating_sub(self.logical_plan_received);
            if needed > 0 {
                let take = needed.min(chunk.len());
                self.push_logical_plan_segment(Bytes::copy_from_slice(&chunk[..take]));
                consumed = take;
            }
            if consumed < chunk.len() {
                self.control_buf.extend_from_slice(&chunk[consumed..]);
            }
            return;
        }

        self.control_buf.extend_from_slice(chunk);
    }

    fn push_logical_plan_segment(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        self.logical_plan_received += bytes.len();
        self.logical_plan_segments.push(bytes);
    }

    fn drain_control_into_logical_plan_segments(&mut self) {
        let Some(len) = self.logical_plan_len else {
            return;
        };
        let needed = len.saturating_sub(self.logical_plan_received);
        let take = needed.min(self.control_buf.len());
        if take == 0 {
            return;
        }
        let segment = self.control_buf.split_to(take).freeze();
        self.push_logical_plan_segment(segment);
    }

    fn decode_need_more(&mut self, eof: bool) -> Result<DecodeProgress, DecodeError> {
        if eof {
            return Err(DecodeError::UnexpectedEof {
                state: format!("{:?}", self.machine.state()),
                buffered: self.buffered_len(),
            });
        }
        Ok(DecodeProgress::NeedMoreInput)
    }

    fn buffered_len(&self) -> usize {
        self.control_buf.len() + self.logical_plan_received
    }

    fn discard_control_prefix(&mut self, consumed: usize) {
        let _ = self.control_buf.split_to(consumed);
    }

    fn build_decoded_plan(&mut self) -> Result<LogicalPlan, DecodeError> {
        let logical_plan = self.logical_plan.take().ok_or_else(|| {
            DecodeError::MsgPack("logical plan payload is missing in build stage".into())
        })?;
        let codec = PgScanDecodeExtensionCodec::new(self.pg_scan_specs.clone());
        let plan = logical_plan
            .try_into_logical_plan(&self.ctx, &codec)
            .map_err(DecodeError::from)?;
        validate_scan_spec_usage(&plan, &self.pg_scan_specs)?;
        Ok(plan)
    }

    fn poison_decode(&mut self, error: &DecodeError) {
        if self.failed.is_some() || self.finished {
            return;
        }

        let state = format!("{:?}", self.machine.state());
        self.failed = Some(SessionFailure {
            state: state.clone(),
            reason: error.to_string(),
        });

        if !matches!(
            self.machine.state(),
            fsm::DecodeState::Failed | fsm::DecodeState::Done
        ) {
            let event = match error {
                DecodeError::UnexpectedEof { .. } => fsm::DecodeEvent::Eof,
                _ => fsm::DecodeEvent::Fail,
            };
            let _ = consume_decode_event(&mut self.machine, event);
        }
    }
}

impl Default for PlanDecodeSession {
    fn default() -> Self {
        Self::new()
    }
}

fn collect_plan_envelope(plan: &LogicalPlan) -> Result<PlanEnvelope, EncodeError> {
    let pg_scan_specs = collect_pg_scan_specs(plan)?;
    let codec = PgScanEncodeExtensionCodec;
    let logical_plan = protobuf::LogicalPlanNode::try_from_logical_plan(plan, &codec)?;
    Ok(PlanEnvelope {
        pg_scan_specs,
        logical_plan,
    })
}

#[cfg(test)]
fn encode_envelope_into<S>(envelope: &PlanEnvelope, sink: &mut S) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(sink, PLAN_CODEC_ENVELOPE_LEN)?;
    write_string_to(sink, PLAN_CODEC_MAGIC)?;
    write_u8_to(sink, PLAN_CODEC_VERSION)?;
    encode_pg_scan_specs_into(sink, &envelope.pg_scan_specs)?;
    write_protobuf_bin_to(sink, &envelope.logical_plan)?;
    Ok(())
}

#[cfg(test)]
fn decode_envelope_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<PlanEnvelope, DecodeError>
where
    S: Buf,
{
    let actual_len = read_array_len_from(source)?;
    if actual_len != PLAN_CODEC_ENVELOPE_LEN {
        return Err(DecodeError::InvalidEnvelope {
            expected: PLAN_CODEC_ENVELOPE_LEN,
            actual: actual_len,
        });
    }

    let magic = read_string_from(source, "plan magic")?;
    if magic != PLAN_CODEC_MAGIC {
        return Err(DecodeError::InvalidMagic {
            expected: PLAN_CODEC_MAGIC,
            actual: magic,
        });
    }

    let version = read_u8_from(source)?;
    if version != PLAN_CODEC_VERSION {
        return Err(DecodeError::UnsupportedVersion { version });
    }

    let pg_scan_specs = decode_pg_scan_specs_from(source, ctx)?;
    let logical_plan =
        read_protobuf_bin_from::<protobuf::LogicalPlanNode, _>(source, "logical plan")?;

    Ok(PlanEnvelope {
        pg_scan_specs,
        logical_plan,
    })
}

fn collect_pg_scan_specs(
    plan: &LogicalPlan,
) -> Result<BTreeMap<PgScanId, Arc<PgScanSpec>>, EncodeError> {
    let mut specs = BTreeMap::new();
    collect_pg_scan_specs_inner(plan, &mut specs)?;
    Ok(specs)
}

fn collect_pg_scan_specs_inner(
    plan: &LogicalPlan,
    specs: &mut BTreeMap<PgScanId, Arc<PgScanSpec>>,
) -> Result<(), EncodeError> {
    if let LogicalPlan::Extension(extension) = plan {
        if let Some(pg_scan) = extension.node.as_any().downcast_ref::<PgScanNode>() {
            let spec = pg_scan.spec();
            if specs.insert(spec.scan_id, spec.clone()).is_some() {
                return Err(EncodeError::DuplicateScanId {
                    scan_id: spec.scan_id.get(),
                });
            }
        }
    }

    for input in plan.inputs() {
        collect_pg_scan_specs_inner(input, specs)?;
    }

    Ok(())
}

fn validate_scan_spec_usage(
    plan: &LogicalPlan,
    specs: &BTreeMap<PgScanId, Arc<PgScanSpec>>,
) -> Result<(), DecodeError> {
    let mut used = BTreeSet::new();
    collect_used_scan_ids(plan, &mut used)?;

    for scan_id in specs.keys() {
        if !used.contains(scan_id) {
            return Err(DecodeError::OrphanScanSpec {
                scan_id: scan_id.get(),
            });
        }
    }

    Ok(())
}

fn collect_used_scan_ids(
    plan: &LogicalPlan,
    used: &mut BTreeSet<PgScanId>,
) -> Result<(), DecodeError> {
    if let LogicalPlan::Extension(extension) = plan {
        if let Some(pg_scan) = extension.node.as_any().downcast_ref::<PgScanNode>() {
            let scan_id = pg_scan.spec().scan_id;
            if !used.insert(scan_id) {
                return Err(DecodeError::DuplicateScanId {
                    scan_id: scan_id.get(),
                });
            }
        }
    }

    for input in plan.inputs() {
        collect_used_scan_ids(input, used)?;
    }

    Ok(())
}

#[derive(Debug, Default, Clone, Copy)]
struct NoopLogicalExtensionCodec;

impl LogicalExtensionCodec for NoopLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> DataFusionResult<Extension> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode logical extension nodes in nested protobuf payloads".into(),
        ))
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Err(DataFusionError::Plan(
            "plan_codec does not encode logical extension nodes in nested protobuf payloads".into(),
        ))
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: datafusion::arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> DataFusionResult<Arc<dyn datafusion::datasource::TableProvider>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode table providers".into(),
        ))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> DataFusionResult<()> {
        Err(DataFusionError::Plan(
            "plan_codec does not encode table providers".into(),
        ))
    }

    fn try_decode_udf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom scalar UDF definitions".into(),
        ))
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }

    fn try_decode_udaf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom aggregate UDF definitions".into(),
        ))
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<WindowUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom window UDF definitions".into(),
        ))
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PgScanEncodeExtensionCodec;

impl LogicalExtensionCodec for PgScanEncodeExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> DataFusionResult<Extension> {
        Err(DataFusionError::Plan(
            "plan_codec encode codec does not decode PgScanNode payloads".into(),
        ))
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> DataFusionResult<()> {
        let Some(pg_scan) = node.node.as_any().downcast_ref::<PgScanNode>() else {
            return Err(DataFusionError::Plan(format!(
                "unsupported logical extension node for plan codec: {}",
                node.node.name()
            )));
        };

        encode_scan_id_payload(pg_scan.spec().scan_id, buf);
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: datafusion::arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> DataFusionResult<Arc<dyn datafusion::datasource::TableProvider>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode table providers".into(),
        ))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> DataFusionResult<()> {
        Err(DataFusionError::Plan(
            "plan_codec does not encode table providers".into(),
        ))
    }

    fn try_decode_udf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom scalar UDF definitions".into(),
        ))
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }

    fn try_decode_udaf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom aggregate UDF definitions".into(),
        ))
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<WindowUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom window UDF definitions".into(),
        ))
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PgScanDecodeExtensionCodec {
    specs: BTreeMap<PgScanId, Arc<PgScanSpec>>,
}

impl PgScanDecodeExtensionCodec {
    fn new(specs: BTreeMap<PgScanId, Arc<PgScanSpec>>) -> Self {
        Self { specs }
    }
}

impl LogicalExtensionCodec for PgScanDecodeExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> DataFusionResult<Extension> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "PgScanNode decode received unexpected logical inputs".into(),
            ));
        }

        let scan_id = decode_scan_id_payload(buf).map_err(|error| {
            DataFusionError::Plan(format!("failed to decode PgScanNode reference: {error}"))
        })?;
        let spec = self.specs.get(&scan_id).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "PgScanNode reference points at missing PgScanSpec: scan_id={}",
                scan_id.get()
            ))
        })?;

        Ok(Extension {
            node: Arc::new(PgScanNode::new(Arc::clone(spec))),
        })
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Err(DataFusionError::Plan(
            "plan_codec decode codec does not encode PgScanNode payloads".into(),
        ))
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: datafusion::arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> DataFusionResult<Arc<dyn datafusion::datasource::TableProvider>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode table providers".into(),
        ))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> DataFusionResult<()> {
        Err(DataFusionError::Plan(
            "plan_codec does not encode table providers".into(),
        ))
    }

    fn try_decode_udf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom scalar UDF definitions".into(),
        ))
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }

    fn try_decode_udaf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom aggregate UDF definitions".into(),
        ))
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, _name: &str, _buf: &[u8]) -> DataFusionResult<Arc<WindowUDF>> {
        Err(DataFusionError::Plan(
            "plan_codec does not decode custom window UDF definitions".into(),
        ))
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> DataFusionResult<()> {
        Ok(())
    }
}

fn encode_scan_id_payload(scan_id: PgScanId, buf: &mut Vec<u8>) {
    buf.clear();
    buf.reserve(PG_SCAN_ID_PAYLOAD_LEN);
    buf.push(PG_SCAN_ID_PAYLOAD_VERSION);
    buf.extend_from_slice(&scan_id.get().to_be_bytes());
}

fn decode_scan_id_payload(buf: &[u8]) -> Result<PgScanId, DecodeError> {
    if buf.len() != PG_SCAN_ID_PAYLOAD_LEN {
        return Err(DecodeError::InvalidScanPayload(format!(
            "expected {PG_SCAN_ID_PAYLOAD_LEN} bytes, got {}",
            buf.len()
        )));
    }

    let version = buf[0];
    if version != PG_SCAN_ID_PAYLOAD_VERSION {
        return Err(DecodeError::InvalidScanPayload(format!(
            "unsupported PgScan reference payload version: {version}"
        )));
    }

    let mut scan_id_bytes = [0u8; std::mem::size_of::<u64>()];
    scan_id_bytes.copy_from_slice(&buf[1..]);
    Ok(PgScanId::new(u64::from_be_bytes(scan_id_bytes)))
}

#[cfg(test)]
fn encode_pg_scan_specs_into<S>(
    sink: &mut S,
    specs: &BTreeMap<PgScanId, Arc<PgScanSpec>>,
) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(
        sink,
        u32::try_from(specs.len())
            .map_err(|_| EncodeError::TooManyScanSpecs { count: specs.len() })?,
    )?;

    for spec in specs.values() {
        encode_pg_scan_spec_into(sink, spec)?;
    }

    Ok(())
}

#[cfg(test)]
fn decode_pg_scan_specs_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<BTreeMap<PgScanId, Arc<PgScanSpec>>, DecodeError>
where
    S: Buf,
{
    let len = read_array_len_from(source)?;
    let mut specs = BTreeMap::new();

    for _ in 0..len {
        let spec = Arc::new(decode_pg_scan_spec_from(source, ctx)?);
        if specs.insert(spec.scan_id, Arc::clone(&spec)).is_some() {
            return Err(DecodeError::DuplicateScanId {
                scan_id: spec.scan_id.get(),
            });
        }
    }

    Ok(specs)
}

fn encode_pg_scan_spec_into<S>(sink: &mut S, spec: &PgScanSpec) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(sink, PG_SCAN_SPEC_LEN)?;
    write_u8_to(sink, PG_SCAN_SPEC_VERSION)?;
    write_u64_to(sink, spec.scan_id.get())?;
    write_u32_to(sink, spec.table_oid)?;
    encode_pg_relation_into(sink, &spec.relation)?;
    encode_compiled_scan_into(sink, &spec.compiled_scan)?;
    encode_fetch_hints_into(sink, spec.fetch_hints)?;
    encode_df_schema_into(sink, spec.schema())?;
    Ok(())
}

fn decode_pg_scan_spec_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<PgScanSpec, DecodeError>
where
    S: Buf,
{
    expect_array_len_from(source, PG_SCAN_SPEC_LEN, "PgScanSpec")?;
    let version = read_u8_from(source)?;
    if version != PG_SCAN_SPEC_VERSION {
        return Err(DecodeError::InvalidScanPayload(format!(
            "unsupported PgScanSpec version: {version}"
        )));
    }

    let scan_id = PgScanId::new(read_u64_from(source)?);
    let table_oid = read_u32_from(source)?;
    let relation = decode_pg_relation_from(source)?;
    let compiled_scan = decode_compiled_scan_from(source, ctx)?;
    let fetch_hints = decode_fetch_hints_from(source)?;
    let schema = decode_df_schema_from(source)?;

    PgScanSpec::try_new_with_schema(
        scan_id,
        table_oid,
        relation,
        compiled_scan,
        fetch_hints,
        schema,
    )
    .map_err(DecodeError::from)
}

fn encode_pg_relation_into<S>(sink: &mut S, relation: &PgRelation) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(sink, PG_SCAN_RELATION_LEN)?;
    write_optional_string_to(sink, relation.schema.as_deref())?;
    write_string_to(sink, &relation.table)?;
    Ok(())
}

fn decode_pg_relation_from<S>(source: &mut S) -> Result<PgRelation, DecodeError>
where
    S: Buf,
{
    expect_array_len_from(source, PG_SCAN_RELATION_LEN, "PgRelation")?;
    let schema = read_optional_string_from(source)?;
    let table = read_string_from(source, "table name")?;
    Ok(PgRelation { schema, table })
}

fn encode_compiled_scan_into<S>(sink: &mut S, scan: &CompiledScan) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(sink, PG_SCAN_COMPILED_SCAN_LEN)?;
    write_string_to(sink, &scan.sql)?;
    write_optional_usize_to(sink, scan.requested_limit)?;
    write_optional_usize_to(sink, scan.sql_limit)?;
    write_usize_vec_to(sink, &scan.selected_columns)?;
    write_usize_vec_to(sink, &scan.output_columns)?;
    write_usize_vec_to(sink, &scan.filter_only_columns)?;
    write_usize_vec_to(sink, &scan.residual_filter_columns)?;
    encode_pushed_filters_into(sink, &scan.pushed_filters)?;
    encode_residual_filters_into(sink, &scan.residual_filters)?;
    write_bool_to(sink, scan.all_filters_compiled)?;
    write_bool_to(sink, scan.uses_dummy_projection)?;
    Ok(())
}

fn decode_compiled_scan_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<CompiledScan, DecodeError>
where
    S: Buf,
{
    expect_array_len_from(source, PG_SCAN_COMPILED_SCAN_LEN, "CompiledScan")?;
    let sql = read_string_from(source, "compiled scan SQL")?;
    let requested_limit = read_optional_usize_from(source)?;
    let sql_limit = read_optional_usize_from(source)?;
    let selected_columns = read_usize_vec_from(source)?;
    let output_columns = read_usize_vec_from(source)?;
    let filter_only_columns = read_usize_vec_from(source)?;
    let residual_filter_columns = read_usize_vec_from(source)?;
    let pushed_filters = decode_pushed_filters_from(source)?;
    let residual_filters = decode_residual_filters_from(source, ctx)?;
    let all_filters_compiled = read_bool_from(source)?;
    let uses_dummy_projection = read_bool_from(source)?;

    Ok(CompiledScan {
        sql,
        requested_limit,
        sql_limit,
        selected_columns,
        output_columns,
        filter_only_columns,
        residual_filter_columns,
        pushed_filters,
        residual_filters,
        all_filters_compiled,
        uses_dummy_projection,
    })
}

fn encode_fetch_hints_into<S>(sink: &mut S, hints: PgScanFetchHints) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(sink, PG_SCAN_FETCH_HINTS_LEN)?;
    write_optional_usize_to(sink, hints.planner_fetch_hint)?;
    write_optional_usize_to(sink, hints.local_row_cap)?;
    Ok(())
}

fn decode_fetch_hints_from<S>(source: &mut S) -> Result<PgScanFetchHints, DecodeError>
where
    S: Buf,
{
    expect_array_len_from(source, PG_SCAN_FETCH_HINTS_LEN, "PgScanFetchHints")?;
    Ok(PgScanFetchHints {
        planner_fetch_hint: read_optional_usize_from(source)?,
        local_row_cap: read_optional_usize_from(source)?,
    })
}

fn encode_pushed_filters_into<S>(
    sink: &mut S,
    filters: &[CompiledFilter],
) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(
        sink,
        u32::try_from(filters.len()).map_err(|_| EncodeError::TooManyScanSpecs {
            count: filters.len(),
        })?,
    )?;

    for filter in filters {
        write_array_len_to(sink, PG_SCAN_PUSHED_FILTER_LEN)?;
        write_u64_to(
            sink,
            u64::try_from(filter.original_index).map_err(|_| {
                EncodeError::MsgPack(format!(
                    "compiled filter index {} does not fit into u64",
                    filter.original_index
                ))
            })?,
        )?;
        write_string_to(sink, &filter.sql)?;
    }

    Ok(())
}

fn decode_pushed_filters_from<S>(source: &mut S) -> Result<Vec<CompiledFilter>, DecodeError>
where
    S: Buf,
{
    let len = read_array_len_from(source)?;
    let mut filters = Vec::with_capacity(len as usize);
    for _ in 0..len {
        expect_array_len_from(source, PG_SCAN_PUSHED_FILTER_LEN, "CompiledFilter")?;
        let original_index = usize::try_from(read_u64_from(source)?).map_err(|_| {
            DecodeError::MsgPack("compiled filter index does not fit into usize".into())
        })?;
        let sql = read_string_from(source, "compiled filter SQL")?;
        filters.push(CompiledFilter {
            original_index,
            sql,
        });
    }
    Ok(filters)
}

fn encode_residual_filters_into<S>(
    sink: &mut S,
    filters: &[datafusion_expr::Expr],
) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(
        sink,
        u32::try_from(filters.len()).map_err(|_| {
            EncodeError::MsgPack(format!(
                "too many residual filters to encode: {}",
                filters.len()
            ))
        })?,
    )?;

    let codec = NoopLogicalExtensionCodec;
    for filter in filters {
        let expr = serialize_expr(filter, &codec).map_err(|error| {
            EncodeError::DataFusion(DataFusionError::Plan(format!(
                "failed to serialize residual filter into protobuf: {error}"
            )))
        })?;
        write_protobuf_bin_to(sink, &expr)?;
    }

    Ok(())
}

fn decode_residual_filters_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<Vec<datafusion_expr::Expr>, DecodeError>
where
    S: Buf,
{
    let len = read_array_len_from(source)?;
    let mut filters = Vec::with_capacity(len as usize);
    let codec = NoopLogicalExtensionCodec;

    for _ in 0..len {
        let expr =
            read_protobuf_bin_from::<protobuf::LogicalExprNode, _>(source, "residual filter")?;
        filters.push(parse_expr(&expr, ctx, &codec).map_err(|error| {
            DecodeError::DataFusion(DataFusionError::Plan(format!(
                "failed to decode residual filter from protobuf: {error}"
            )))
        })?);
    }

    Ok(filters)
}

fn encode_df_schema_into<S>(
    sink: &mut S,
    schema: &datafusion_common::DFSchemaRef,
) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let proto: protobuf::DfSchema = schema.try_into().map_err(|error| {
        EncodeError::DataFusion(DataFusionError::Plan(format!(
            "failed to serialize DFSchema into protobuf: {error}"
        )))
    })?;
    write_protobuf_bin_to(sink, &proto)
}

fn decode_df_schema_from<S>(source: &mut S) -> Result<datafusion_common::DFSchemaRef, DecodeError>
where
    S: Buf,
{
    let proto = read_protobuf_bin_from::<protobuf::DfSchema, _>(source, "DFSchema")?;
    proto.try_into().map_err(|error| {
        DecodeError::DataFusion(DataFusionError::Plan(format!(
            "failed to rebuild DFSchema from protobuf: {error}"
        )))
    })
}

fn write_optional_usize_to<S>(sink: &mut S, value: Option<usize>) -> Result<(), EncodeError>
where
    S: BufMut,
{
    match value {
        Some(value) => {
            write_bool_to(sink, true)?;
            write_u64_to(
                sink,
                u64::try_from(value).map_err(|_| {
                    EncodeError::MsgPack(format!("usize value {value} does not fit into u64"))
                })?,
            )?;
        }
        None => write_bool_to(sink, false)?,
    }
    Ok(())
}

fn read_optional_usize_from<S>(source: &mut S) -> Result<Option<usize>, DecodeError>
where
    S: Buf,
{
    if !read_bool_from(source)? {
        return Ok(None);
    }

    let value = read_u64_from(source)?;
    usize::try_from(value)
        .map(Some)
        .map_err(|_| DecodeError::MsgPack(format!("u64 value {value} does not fit into usize")))
}

fn write_optional_string_to<S>(sink: &mut S, value: Option<&str>) -> Result<(), EncodeError>
where
    S: BufMut,
{
    match value {
        Some(value) => {
            write_bool_to(sink, true)?;
            write_string_to(sink, value)?;
        }
        None => write_bool_to(sink, false)?,
    }
    Ok(())
}

fn read_optional_string_from<S>(source: &mut S) -> Result<Option<String>, DecodeError>
where
    S: Buf,
{
    if read_bool_from(source)? {
        read_string_from(source, "optional string").map(Some)
    } else {
        Ok(None)
    }
}

fn write_usize_vec_to<S>(sink: &mut S, values: &[usize]) -> Result<(), EncodeError>
where
    S: BufMut,
{
    write_array_len_to(
        sink,
        u32::try_from(values.len()).map_err(|_| {
            EncodeError::MsgPack(format!("too many usize values to encode: {}", values.len()))
        })?,
    )?;

    for &value in values {
        write_u64_to(
            sink,
            u64::try_from(value).map_err(|_| {
                EncodeError::MsgPack(format!("usize value {value} does not fit into u64"))
            })?,
        )?;
    }

    Ok(())
}

fn read_usize_vec_from<S>(source: &mut S) -> Result<Vec<usize>, DecodeError>
where
    S: Buf,
{
    let len = read_array_len_from(source)?;
    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let value = read_u64_from(source)?;
        values.push(usize::try_from(value).map_err(|_| {
            DecodeError::MsgPack(format!("u64 value {value} does not fit into usize"))
        })?);
    }
    Ok(values)
}

fn write_protobuf_bin_to<M, S>(sink: &mut S, message: &M) -> Result<(), EncodeError>
where
    M: Message,
    S: BufMut,
{
    let len = message.encoded_len();
    write_bin_len_to(sink, len)?;
    message.encode(sink)?;
    Ok(())
}

fn read_protobuf_bin_from<M, S>(source: &mut S, what: &str) -> Result<M, DecodeError>
where
    M: Message + Default,
    S: Buf,
{
    let len = read_bin_len_from(source)? as usize;
    if source.remaining() < len {
        return Err(DecodeError::MsgPack(format!(
            "{what} protobuf payload is truncated: need {len} bytes, have {}",
            source.remaining()
        )));
    }

    let mut limited = LimitedSource::new(source, len);
    let message = M::decode(&mut limited)
        .map_err(|error| DecodeError::Protobuf(format!("failed to decode {what}: {error}")))?;
    if limited.remaining() != 0 {
        return Err(DecodeError::Protobuf(format!(
            "{what} protobuf payload has {} trailing bytes",
            limited.remaining()
        )));
    }

    Ok(message)
}

fn write_array_len_to<S>(sink: &mut S, len: u32) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let mut writer = (&mut *sink).writer();
    write_array_len(&mut writer, len)
        .map(|_| ())
        .map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn expect_array_len_from<S>(source: &mut S, expected: u32, what: &str) -> Result<(), DecodeError>
where
    S: Buf,
{
    let actual = read_array_len_from(source)?;
    if actual != expected {
        return Err(DecodeError::MsgPack(format!(
            "{what} expected MsgPack array of length {expected}, got {actual}"
        )));
    }
    Ok(())
}

fn read_array_len_from<S>(source: &mut S) -> Result<u32, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    read_array_len(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_string_to<S>(sink: &mut S, value: &str) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let mut writer = (&mut *sink).writer();
    write_str(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_string_from<S>(source: &mut S, what: &str) -> Result<String, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    let len = read_str_len(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))?
        as usize;
    if source.remaining() < len {
        return Err(DecodeError::MsgPack(format!(
            "{what} is truncated: need {len} bytes, have {}",
            source.remaining()
        )));
    }

    let mut bytes = vec![0u8; len];
    source.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map_err(|error| DecodeError::MsgPack(format!("invalid UTF-8 in {what}: {error}")))
}

fn write_u8_to<S>(sink: &mut S, value: u8) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let mut writer = (&mut *sink).writer();
    write_u8(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_u8_from<S>(source: &mut S) -> Result<u8, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    read_u8(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_u32_to<S>(sink: &mut S, value: u32) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let mut writer = (&mut *sink).writer();
    write_u32(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_u32_from<S>(source: &mut S) -> Result<u32, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    read_u32(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_u64_to<S>(sink: &mut S, value: u64) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let mut writer = (&mut *sink).writer();
    write_u64(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_u64_from<S>(source: &mut S) -> Result<u64, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    read_u64(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_bool_to<S>(sink: &mut S, value: bool) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let mut writer = (&mut *sink).writer();
    write_bool(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_bool_from<S>(source: &mut S) -> Result<bool, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    read_bool(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_bin_len_to<S>(sink: &mut S, len: usize) -> Result<(), EncodeError>
where
    S: BufMut,
{
    let len = u32::try_from(len).map_err(|_| EncodeError::PayloadTooLarge { len })?;
    let mut writer = (&mut *sink).writer();
    write_bin_len(&mut writer, len)
        .map(|_| ())
        .map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_bin_len_from<S>(source: &mut S) -> Result<u32, DecodeError>
where
    S: Buf,
{
    let mut reader = (&mut *source).reader();
    read_bin_len(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn consume_encode_event(
    machine: &mut fsm::encode_flow::StateMachine,
    event: fsm::EncodeEvent,
) -> Result<(), EncodeError> {
    machine
        .consume(&event)
        .map(|_| ())
        .map_err(|error| EncodeError::StateMachine(error.to_string()))
}

fn consume_decode_event(
    machine: &mut fsm::decode_flow::StateMachine,
    event: fsm::DecodeEvent,
) -> Result<(), DecodeError> {
    machine
        .consume(&event)
        .map(|_| ())
        .map_err(|error| DecodeError::StateMachine(error.to_string()))
}

fn try_parse_prefix<T, F>(buffer: &[u8], parse: F) -> Result<Option<(T, usize)>, DecodeError>
where
    F: FnOnce(&mut &[u8]) -> Result<T, DecodeError>,
{
    let initial_len = buffer.len();
    let mut source = buffer;
    match parse(&mut source) {
        Ok(value) => Ok(Some((value, initial_len - source.remaining()))),
        Err(error) if is_incomplete_decode_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn is_incomplete_decode_error(error: &DecodeError) -> bool {
    match error {
        DecodeError::MsgPack(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("truncated")
                || lower.contains("unexpected eof")
                || lower.contains("failed to fill whole buffer")
                || lower.contains("failed to read messagepack data")
                || lower.contains("failed to read messagepack marker")
                || lower.contains("io error")
        }
        DecodeError::Protobuf(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("buffer underflow") || lower.contains("unexpected eof")
        }
        _ => false,
    }
}

fn should_poison_encode_error(error: &EncodeError) -> bool {
    !matches!(
        error,
        EncodeError::EmptyOutputChunk | EncodeError::SessionFailed { .. }
    )
}

fn encoded_len_with<F>(encode: F) -> Result<usize, EncodeError>
where
    F: FnOnce(&mut CountingBufMut) -> Result<(), EncodeError>,
{
    let mut sink = CountingBufMut::new();
    encode(&mut sink)?;
    Ok(sink.len())
}

struct CountingBufMut {
    len: usize,
    scratch: [u8; BUF_SCRATCH_LEN],
}

impl CountingBufMut {
    fn new() -> Self {
        Self {
            len: 0,
            scratch: [0u8; BUF_SCRATCH_LEN],
        }
    }

    fn len(&self) -> usize {
        self.len
    }
}

unsafe impl BufMut for CountingBufMut {
    fn remaining_mut(&self) -> usize {
        usize::MAX.saturating_sub(self.len)
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        // SAFETY: the scratch buffer is writable for its full length.
        unsafe { UninitSlice::from_raw_parts_mut(self.scratch.as_mut_ptr(), self.scratch.len()) }
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.len = self.len.saturating_add(cnt);
    }

    fn put_slice(&mut self, src: &[u8]) {
        self.len = self.len.saturating_add(src.len());
    }
}

struct OverlapBufMut<'a> {
    out: &'a mut [u8],
    skip: usize,
    observed: usize,
    written: usize,
    scratch: [u8; BUF_SCRATCH_LEN],
}

impl<'a> OverlapBufMut<'a> {
    fn new(out: &'a mut [u8], skip: usize) -> Self {
        Self {
            out,
            skip,
            observed: 0,
            written: 0,
            scratch: [0u8; BUF_SCRATCH_LEN],
        }
    }

    fn written(&self) -> usize {
        self.written
    }

    fn observe_bytes(&mut self, src: &[u8]) {
        let start = self.observed;
        let end = start + src.len();
        self.observed = end;

        let window_start = self.skip;
        let window_end = self.skip + self.out.len();
        if end <= window_start || start >= window_end {
            return;
        }

        let copy_start = window_start.saturating_sub(start);
        let copy_end = src.len().min(window_end.saturating_sub(start));
        if copy_start >= copy_end {
            return;
        }

        let dst_start = start.max(window_start) - window_start;
        let len = copy_end - copy_start;
        self.out[dst_start..dst_start + len].copy_from_slice(&src[copy_start..copy_end]);
        self.written += len;
    }
}

unsafe impl BufMut for OverlapBufMut<'_> {
    fn remaining_mut(&self) -> usize {
        usize::MAX.saturating_sub(self.observed)
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        // SAFETY: the scratch buffer is writable for its full length.
        unsafe { UninitSlice::from_raw_parts_mut(self.scratch.as_mut_ptr(), self.scratch.len()) }
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        let mut scratch = [0u8; BUF_SCRATCH_LEN];
        scratch[..cnt].copy_from_slice(&self.scratch[..cnt]);
        self.observe_bytes(&scratch[..cnt]);
    }

    fn put_slice(&mut self, src: &[u8]) {
        self.observe_bytes(src);
    }
}

struct LimitedSource<'a, S> {
    inner: &'a mut S,
    remaining: usize,
}

impl<'a, S> LimitedSource<'a, S> {
    fn new(inner: &'a mut S, remaining: usize) -> Self {
        Self { inner, remaining }
    }
}

impl<S> Buf for LimitedSource<'_, S>
where
    S: Buf,
{
    fn remaining(&self) -> usize {
        self.remaining.min(self.inner.remaining())
    }

    fn chunk(&self) -> &[u8] {
        let chunk = self.inner.chunk();
        &chunk[..chunk.len().min(self.remaining)]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining);
        self.remaining -= cnt;
        self.inner.advance(cnt);
    }
}

struct SegmentedSource<'a> {
    segments: &'a [Bytes],
    index: usize,
    offset: usize,
    remaining: usize,
}

impl<'a> SegmentedSource<'a> {
    fn new(segments: &'a [Bytes], remaining: usize) -> Self {
        Self {
            segments,
            index: 0,
            offset: 0,
            remaining,
        }
    }
}

impl Buf for SegmentedSource<'_> {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }

        let segment = &self.segments[self.index];
        &segment[self.offset..]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining);
        self.remaining -= cnt;

        let mut cnt = cnt;
        while cnt > 0 {
            let segment = &self.segments[self.index];
            let available = segment.len() - self.offset;
            if cnt < available {
                self.offset += cnt;
                return;
            }

            cnt -= available;
            self.index += 1;
            self.offset = 0;
        }
    }
}

#[cfg(test)]
mod tests;
