//! Versioned logical-plan codec for backend-built PostgreSQL scan plans.
//!
//! `plan_codec` serializes backend-built logical plans for later worker-side
//! use without owning transport or page rollover itself.
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
//! serialized plan payload.
//!
//! The codec is intentionally scoped to plans emitted by `pg/plan_builder`.
//! Expr-level subqueries are out of scope and are rejected by `plan_builder`
//! before serialization.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::{Buf, BufMut};
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
use thiserror::Error;

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

/// Byte sink for `plan_codec`.
pub trait PlanSink: BufMut {}

impl<T> PlanSink for T where T: BufMut {}

/// Byte source for `plan_codec`.
pub trait PlanSource: Buf {}

impl<T> PlanSource for T where T: Buf {}

/// Encode a logical plan into a caller-provided sink.
pub fn encode_plan_into<S>(plan: &LogicalPlan, sink: &mut S) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    let envelope = collect_plan_envelope(plan)?;
    encode_envelope_into(&envelope, sink)
}

/// Decode a logical plan from a caller-provided source.
pub fn decode_plan_from<S>(source: &mut S) -> Result<LogicalPlan, DecodeError>
where
    S: PlanSource,
{
    let ctx = SessionContext::new();
    let envelope = decode_envelope_from(source, &ctx)?;
    let codec = PgScanDecodeExtensionCodec::new(envelope.pg_scan_specs.clone());
    let plan = envelope
        .logical_plan
        .try_into_logical_plan(&ctx, &codec)
        .map_err(DecodeError::from)?;
    validate_scan_spec_usage(&plan, &envelope.pg_scan_specs)?;

    if source.has_remaining() {
        return Err(DecodeError::TrailingBytes {
            remaining: source.remaining(),
        });
    }

    Ok(plan)
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("duplicate PgScanId in logical plan: {scan_id}")]
    DuplicateScanId { scan_id: u64 },
    #[error("too many PgScan specs to encode: {count}")]
    TooManyScanSpecs { count: usize },
    #[error("protobuf payload too large: {len} bytes")]
    PayloadTooLarge { len: usize },
    #[error("logical plan serialization failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("protobuf encoding failed: {0}")]
    Protobuf(#[from] prost::EncodeError),
    #[error("MsgPack encoding failed: {0}")]
    MsgPack(String),
}

#[derive(Debug, Error)]
pub enum DecodeError {
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
    #[error("MsgPack decoding failed: {0}")]
    MsgPack(String),
}

#[derive(Debug)]
struct PlanEnvelope {
    pg_scan_specs: BTreeMap<PgScanId, Arc<PgScanSpec>>,
    logical_plan: protobuf::LogicalPlanNode,
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

fn encode_envelope_into<S>(envelope: &PlanEnvelope, sink: &mut S) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    write_array_len_to(sink, PLAN_CODEC_ENVELOPE_LEN)?;
    write_string_to(sink, PLAN_CODEC_MAGIC)?;
    write_u8_to(sink, PLAN_CODEC_VERSION)?;
    encode_pg_scan_specs_into(sink, &envelope.pg_scan_specs)?;
    write_protobuf_bin_to(sink, &envelope.logical_plan)?;
    Ok(())
}

fn decode_envelope_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<PlanEnvelope, DecodeError>
where
    S: PlanSource,
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

fn encode_pg_scan_specs_into<S>(
    sink: &mut S,
    specs: &BTreeMap<PgScanId, Arc<PgScanSpec>>,
) -> Result<(), EncodeError>
where
    S: PlanSink,
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

fn decode_pg_scan_specs_from<S>(
    source: &mut S,
    ctx: &SessionContext,
) -> Result<BTreeMap<PgScanId, Arc<PgScanSpec>>, DecodeError>
where
    S: PlanSource,
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
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
{
    write_array_len_to(sink, PG_SCAN_RELATION_LEN)?;
    write_optional_string_to(sink, relation.schema.as_deref())?;
    write_string_to(sink, &relation.table)?;
    Ok(())
}

fn decode_pg_relation_from<S>(source: &mut S) -> Result<PgRelation, DecodeError>
where
    S: PlanSource,
{
    expect_array_len_from(source, PG_SCAN_RELATION_LEN, "PgRelation")?;
    let schema = read_optional_string_from(source)?;
    let table = read_string_from(source, "table name")?;
    Ok(PgRelation { schema, table })
}

fn encode_compiled_scan_into<S>(sink: &mut S, scan: &CompiledScan) -> Result<(), EncodeError>
where
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
{
    write_array_len_to(sink, PG_SCAN_FETCH_HINTS_LEN)?;
    write_optional_usize_to(sink, hints.planner_fetch_hint)?;
    write_optional_usize_to(sink, hints.local_row_cap)?;
    Ok(())
}

fn decode_fetch_hints_from<S>(source: &mut S) -> Result<PgScanFetchHints, DecodeError>
where
    S: PlanSource,
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
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
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
    S: PlanSource,
{
    if read_bool_from(source)? {
        read_string_from(source, "optional string").map(Some)
    } else {
        Ok(None)
    }
}

fn write_usize_vec_to<S>(sink: &mut S, values: &[usize]) -> Result<(), EncodeError>
where
    S: PlanSink,
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
    S: PlanSource,
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
    S: PlanSink,
{
    let len = message.encoded_len();
    write_bin_len_to(sink, len)?;
    message.encode(sink)?;
    Ok(())
}

fn read_protobuf_bin_from<M, S>(source: &mut S, what: &str) -> Result<M, DecodeError>
where
    M: Message + Default,
    S: PlanSource,
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
    S: PlanSink,
{
    let mut writer = (&mut *sink).writer();
    write_array_len(&mut writer, len)
        .map(|_| ())
        .map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn expect_array_len_from<S>(source: &mut S, expected: u32, what: &str) -> Result<(), DecodeError>
where
    S: PlanSource,
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
    S: PlanSource,
{
    let mut reader = (&mut *source).reader();
    read_array_len(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_string_to<S>(sink: &mut S, value: &str) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    let mut writer = (&mut *sink).writer();
    write_str(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_string_from<S>(source: &mut S, what: &str) -> Result<String, DecodeError>
where
    S: PlanSource,
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
    S: PlanSink,
{
    let mut writer = (&mut *sink).writer();
    write_u8(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_u8_from<S>(source: &mut S) -> Result<u8, DecodeError>
where
    S: PlanSource,
{
    let mut reader = (&mut *source).reader();
    read_u8(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_u32_to<S>(sink: &mut S, value: u32) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    let mut writer = (&mut *sink).writer();
    write_u32(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_u32_from<S>(source: &mut S) -> Result<u32, DecodeError>
where
    S: PlanSource,
{
    let mut reader = (&mut *source).reader();
    read_u32(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_u64_to<S>(sink: &mut S, value: u64) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    let mut writer = (&mut *sink).writer();
    write_u64(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_u64_from<S>(source: &mut S) -> Result<u64, DecodeError>
where
    S: PlanSource,
{
    let mut reader = (&mut *source).reader();
    read_u64(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_bool_to<S>(sink: &mut S, value: bool) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    let mut writer = (&mut *sink).writer();
    write_bool(&mut writer, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_bool_from<S>(source: &mut S) -> Result<bool, DecodeError>
where
    S: PlanSource,
{
    let mut reader = (&mut *source).reader();
    read_bool(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

fn write_bin_len_to<S>(sink: &mut S, len: usize) -> Result<(), EncodeError>
where
    S: PlanSink,
{
    let len = u32::try_from(len).map_err(|_| EncodeError::PayloadTooLarge { len })?;
    let mut writer = (&mut *sink).writer();
    write_bin_len(&mut writer, len)
        .map(|_| ())
        .map_err(|error| EncodeError::MsgPack(error.to_string()))
}

fn read_bin_len_from<S>(source: &mut S) -> Result<u32, DecodeError>
where
    S: PlanSource,
{
    let mut reader = (&mut *source).reader();
    read_bin_len(&mut reader).map_err(|error| DecodeError::MsgPack(error.to_string()))
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
    S: PlanSource,
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

#[cfg(test)]
mod tests;
