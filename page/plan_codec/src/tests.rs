use super::*;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use bytes::BytesMut;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::logical_plan::{
    Extension as LogicalExtension, Filter, LogicalPlan, Projection, UserDefinedLogicalNodeCore,
};
use datafusion_expr::{lit, Expr, Operator};
use datafusion_proto::protobuf::logical_plan_node::LogicalPlanType;
use df_catalog::{CatalogResolver, ResolveError, ResolvedTable};
use plan_builder::{BuiltPlan, PlanBuildInput, PlanBuilder, PlanBuilderConfig};
use scan_node::{PgCteRefNode, PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{compile_scan, CompileScanInput, LimitLowering, PgRelation};

const TEST_IDENTIFIER_MAX_BYTES: usize = 63;

#[derive(Debug, Clone)]
struct FakeResolver {
    tables: HashMap<datafusion_common::TableReference, ResolvedTable>,
}

impl FakeResolver {
    fn new(
        tables: impl IntoIterator<Item = (datafusion_common::TableReference, ResolvedTable)>,
    ) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
    }
}

impl CatalogResolver for FakeResolver {
    fn resolve_table(
        &self,
        table: &datafusion_common::TableReference,
    ) -> Result<ResolvedTable, ResolveError> {
        self.tables
            .get(table)
            .cloned()
            .ok_or_else(|| ResolveError::TableNotFound {
                schema: table.schema().map(|schema| schema.to_string()),
                table: table.table().to_owned(),
            })
    }
}

fn user_table() -> ResolvedTable {
    ResolvedTable {
        table_oid: 42,
        relation: PgRelation::new(Some("public"), "users"),
        column_attnums: vec![1, 2, 3],
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ])),
    }
}

fn order_table() -> ResolvedTable {
    ResolvedTable {
        table_oid: 77,
        relation: PgRelation::new(Some("public"), "orders"),
        column_attnums: vec![1, 2],
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
        ])),
    }
}

fn builder() -> PlanBuilder<FakeResolver> {
    PlanBuilder::with_resolver(FakeResolver::new([
        (
            datafusion_common::TableReference::bare("users"),
            user_table(),
        ),
        (
            datafusion_common::TableReference::bare("orders"),
            order_table(),
        ),
        (
            datafusion_common::TableReference::partial("public", "users"),
            ResolvedTable {
                relation: PgRelation::new(Some("public"), "users"),
                ..user_table()
            },
        ),
    ]))
    .with_config(PlanBuilderConfig {
        target_partitions: 1,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        first_scan_id: 1,
        ..PlanBuilderConfig::default()
    })
}

fn build_sql(sql: &str) -> BuiltPlan {
    builder()
        .build(PlanBuildInput {
            sql,
            params: Vec::new(),
        })
        .expect("build plan")
}

fn encode_all<const PAGE: usize>(plan: &LogicalPlan) -> (Vec<u8>, usize) {
    assert!(PAGE > 0);
    let mut session = PlanEncodeSession::new(plan).expect("create encode session");
    let mut bytes = Vec::new();
    let mut pages = 0usize;

    loop {
        let mut chunk = [0u8; PAGE];
        match session.write_chunk(&mut chunk).expect("write chunk") {
            EncodeProgress::NeedMoreOutput { written } => {
                assert!(written > 0, "encoder must make forward progress");
                bytes.extend_from_slice(&chunk[..written]);
                pages += 1;
            }
            EncodeProgress::Done { written } => {
                bytes.extend_from_slice(&chunk[..written]);
                pages += 1;
                break;
            }
        }
    }

    assert!(session.is_finished());
    (bytes, pages)
}

fn decode_all<const PAGE: usize>(bytes: &[u8]) -> Result<LogicalPlan, DecodeError> {
    assert!(PAGE > 0);
    let mut session = PlanDecodeSession::new();
    for chunk in bytes.chunks(PAGE) {
        let progress = session.push_chunk(chunk)?;
        assert!(
            matches!(progress, DecodeProgress::NeedMoreInput),
            "push_chunk must wait for finish_input to finalize the plan"
        );
    }

    match session.finish_input()? {
        DecodeProgress::Done(plan) => {
            assert!(session.is_finished());
            Ok(*plan)
        }
        DecodeProgress::NeedMoreInput => Err(DecodeError::MsgPack(
            "decode session requires more input".into(),
        )),
    }
}

fn roundtrip(plan: &LogicalPlan) -> LogicalPlan {
    let (bytes, pages) = encode_all::<17>(plan);
    assert!(pages > 1, "test encoding should cross page boundaries");
    decode_all::<17>(&bytes).expect("decode plan")
}

fn encode_bytes(plan: &LogicalPlan) -> Vec<u8> {
    encode_all::<256>(plan).0
}

fn decode_bytes(bytes: &[u8]) -> Result<LogicalPlan, DecodeError> {
    decode_all::<256>(bytes)
}

fn collect_pg_scans(plan: &LogicalPlan) -> Vec<Arc<PgScanSpec>> {
    let mut scans = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::Extension(extension) = node {
            if let Some(pg_scan) = extension.node.as_any().downcast_ref::<PgScanNode>() {
                scans.push(pg_scan.spec());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk plan");
    scans
}

fn count_cte_refs(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::Extension(extension) = node {
            if extension.node.as_any().is::<PgCteRefNode>() {
                count += 1;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk plan");
    count
}

fn assert_scan_specs_eq(expected: &PgScanSpec, actual: &PgScanSpec) {
    assert_eq!(expected.scan_id, actual.scan_id);
    assert_eq!(expected.table_oid, actual.table_oid);
    assert_eq!(expected.relation, actual.relation);
    assert_eq!(expected.compiled_scan, actual.compiled_scan);
    assert_eq!(expected.fetch_hints, actual.fetch_hints);
    assert_eq!(expected.schema().as_ref(), actual.schema().as_ref());
}

fn residual_filter_plan() -> LogicalPlan {
    let resolved = user_table();
    let source_schema = DFSchema::try_from_qualified_schema(
        datafusion_common::TableReference::partial("public", "users"),
        resolved.schema.as_ref(),
    )
    .expect("dfschema");
    let regex_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("name"))),
        Operator::RegexMatch,
        Box::new(lit("^a")),
    ));
    let compiled = compile_scan(CompileScanInput {
        relation: &resolved.relation,
        schema: resolved.schema.as_ref(),
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: std::slice::from_ref(&regex_filter),
        requested_limit: Some(10),
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan");

    assert!(!compiled.all_filters_compiled);

    let residual_filter = compiled
        .residual_filters
        .first()
        .cloned()
        .expect("residual filter");
    let selected_output_len = compiled.selected_columns.len();
    let needs_output_projection = !compiled.residual_filter_columns.is_empty();
    let spec = Arc::new(
        PgScanSpec::try_new(
            PgScanId::new(1),
            resolved.table_oid,
            resolved.relation,
            &source_schema,
            compiled,
        )
        .expect("scan spec"),
    );

    let mut plan = PgScanNode::new(spec).into_logical_plan();
    plan = LogicalPlan::Filter(Filter::try_new(residual_filter, Arc::new(plan)).expect("filter"));

    if needs_output_projection {
        let expr = (0..selected_output_len)
            .map(|index| {
                let (qualifier, field) = plan.schema().qualified_field(index);
                Expr::Column(datafusion_common::Column::from((qualifier, field)))
            })
            .collect::<Vec<_>>();
        plan =
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(plan)).expect("projection"));
    }

    plan
}

#[test]
fn roundtrips_pg_scan_with_residual_filters() {
    let plan = residual_filter_plan();
    let decoded = roundtrip(&plan);
    let decoded_explain = decoded.display_indent().to_string();

    assert!(decoded_explain.contains("Projection: public.users.id"));
    assert!(decoded_explain.contains("Filter:"));
    assert!(decoded_explain.contains("PgScan: scan_id=1"));

    let expected_scans = collect_pg_scans(&plan);
    let actual_scans = collect_pg_scans(&decoded);
    assert_eq!(expected_scans.len(), 1);
    assert_eq!(actual_scans.len(), 1);
    assert_scan_specs_eq(&expected_scans[0], &actual_scans[0]);
    assert_eq!(actual_scans[0].fetch_hints.planner_fetch_hint, Some(10));
    assert_eq!(actual_scans[0].fetch_hints.local_row_cap, None);
}

#[test]
fn roundtrips_join_with_multiple_pg_scans() {
    let built = build_sql(
        "SELECT users.id, orders.id \
         FROM users JOIN orders ON users.id = orders.user_id",
    );
    let decoded = roundtrip(&built.logical_plan);

    assert_eq!(
        built.logical_plan.display_indent().to_string(),
        decoded.display_indent().to_string()
    );

    let expected_scans = collect_pg_scans(&built.logical_plan);
    let actual_scans = collect_pg_scans(&decoded);
    assert_eq!(expected_scans.len(), 2);
    assert_eq!(actual_scans.len(), 2);
    for (expected, actual) in expected_scans.iter().zip(&actual_scans) {
        assert_scan_specs_eq(expected, actual);
    }
    let ids = actual_scans
        .iter()
        .map(|spec| spec.scan_id.get())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn roundtrips_rewritten_in_subquery_with_multiple_pg_scans() {
    let built = build_sql("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)");
    let decoded = roundtrip(&built.logical_plan);

    assert_eq!(
        built.logical_plan.display_indent().to_string(),
        decoded.display_indent().to_string()
    );

    let expected_scans = collect_pg_scans(&built.logical_plan);
    let actual_scans = collect_pg_scans(&decoded);
    assert_eq!(expected_scans.len(), 2);
    assert_eq!(actual_scans.len(), 2);
    for (expected, actual) in expected_scans.iter().zip(&actual_scans) {
        assert_scan_specs_eq(expected, actual);
    }
    assert!(decoded
        .display_indent()
        .to_string()
        .contains("LeftSemi Join"));
}

#[test]
fn roundtrips_materialized_multi_use_cte() {
    let built = build_sql(
        "WITH u AS (SELECT id, score FROM users) \
         SELECT a.id FROM u a JOIN u b ON a.id = b.id",
    );
    let decoded = roundtrip(&built.logical_plan);

    assert_eq!(
        built.logical_plan.display_indent().to_string(),
        decoded.display_indent().to_string()
    );
    assert_eq!(count_cte_refs(&decoded), 2);
    let scans = collect_pg_scans(&decoded);
    assert_eq!(scans.len(), 2);
    assert_eq!(scans[0].scan_id, scans[1].scan_id);
    assert_eq!(scans[0].scan_id.get(), 1);
}

#[test]
fn roundtrips_builtin_no_scan_query() {
    let built = builder()
        .build(PlanBuildInput {
            sql: "SELECT extract(day from now())",
            params: Vec::new(),
        })
        .expect("build no-scan plan");
    let decoded = roundtrip(&built.logical_plan);

    assert_eq!(
        built.logical_plan.display_indent().to_string(),
        decoded.display_indent().to_string()
    );
    assert!(collect_pg_scans(&decoded).is_empty());
}

#[test]
fn rejects_unsupported_extension_nodes() {
    #[derive(Debug, Clone)]
    struct DummyNode {
        schema: datafusion_common::DFSchemaRef,
    }

    impl PartialEq for DummyNode {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for DummyNode {}

    impl Hash for DummyNode {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0u8.hash(state);
        }
    }

    impl PartialOrd for DummyNode {
        fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
            Some(Ordering::Equal)
        }
    }

    impl UserDefinedLogicalNodeCore for DummyNode {
        fn name(&self) -> &str {
            "Dummy"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            Vec::new()
        }

        fn schema(&self) -> &datafusion_common::DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            Vec::new()
        }

        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Dummy")
        }

        fn with_exprs_and_inputs(
            &self,
            exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> DataFusionResult<Self> {
            if !exprs.is_empty() || !inputs.is_empty() {
                return Err(DataFusionError::Plan(
                    "DummyNode does not accept rewrites".into(),
                ));
            }
            Ok(self.clone())
        }
    }

    let plan = LogicalPlan::Extension(LogicalExtension {
        node: Arc::new(DummyNode {
            schema: Arc::new(DFSchema::empty()),
        }),
    });

    let err = PlanEncodeSession::new(&plan)
        .err()
        .expect("unsupported extension should fail");
    assert!(matches!(err, EncodeError::DataFusion(_)));
}

#[test]
fn encoder_rejects_empty_output_chunk() {
    let plan = build_sql("SELECT id FROM users").logical_plan;
    let mut session = PlanEncodeSession::new(&plan).expect("encode session");
    let err = session
        .write_chunk(&mut [])
        .expect_err("empty chunk should fail");
    assert!(matches!(err, EncodeError::EmptyOutputChunk));
}

#[test]
fn decoder_empty_chunk_waits_for_more_input() {
    let mut session = PlanDecodeSession::new();
    let progress = session.push_chunk(&[]).expect("empty decode chunk");
    assert!(matches!(progress, DecodeProgress::NeedMoreInput));
    assert!(!session.is_finished());
}

#[test]
fn decoder_reports_unexpected_eof_for_truncated_input() {
    let built = build_sql("SELECT id FROM users");
    let bytes = encode_bytes(&built.logical_plan);
    let truncated = &bytes[..bytes.len() - 1];
    let mut session = PlanDecodeSession::new();

    let progress = session
        .push_chunk(truncated)
        .expect("truncated chunk should still be buffered");
    assert!(matches!(progress, DecodeProgress::NeedMoreInput));

    let err = session
        .finish_input()
        .expect_err("EOF should fail on truncation");
    assert!(matches!(err, DecodeError::UnexpectedEof { .. }));

    let poisoned = session
        .finish_input()
        .expect_err("poisoned session should stay failed");
    assert!(matches!(poisoned, DecodeError::SessionFailed { .. }));
}

#[test]
fn decoder_requires_eof_before_returning_done() {
    let built = build_sql("SELECT id FROM users");
    let bytes = encode_bytes(&built.logical_plan);
    let mut session = PlanDecodeSession::new();

    let progress = session
        .push_chunk(&bytes)
        .expect("complete payload should be buffered");
    assert!(matches!(progress, DecodeProgress::NeedMoreInput));
    assert!(!session.is_finished());

    match session
        .finish_input()
        .expect("EOF should finalize the plan")
    {
        DecodeProgress::Done(plan) => {
            assert_eq!(
                built.logical_plan.display_indent().to_string(),
                plan.display_indent().to_string()
            );
            assert!(session.is_finished());
        }
        DecodeProgress::NeedMoreInput => panic!("EOF must finish or fail"),
    }
}

#[test]
fn decoder_rejects_trailing_bytes_after_plan_boundary_before_eof() {
    let built = build_sql("SELECT id FROM users");
    let bytes = encode_bytes(&built.logical_plan);
    let mut session = PlanDecodeSession::new();

    let progress = session
        .push_chunk(&bytes)
        .expect("complete payload should be buffered");
    assert!(matches!(progress, DecodeProgress::NeedMoreInput));

    let err = session
        .push_chunk(&[0x99])
        .expect_err("bytes after the plan boundary must be rejected");
    assert!(matches!(err, DecodeError::TrailingBytes { remaining: 1 }));

    let poisoned = session
        .finish_input()
        .expect_err("poisoned session should stay failed");
    assert!(matches!(poisoned, DecodeError::SessionFailed { .. }));
}

#[test]
fn decoder_poisoned_after_build_stage_failure() {
    let built = build_sql("SELECT extract(day from now())");
    let mut envelope = collect_plan_envelope(&built.logical_plan).expect("collect envelope");
    let source_schema = DFSchema::try_from_qualified_schema(
        datafusion_common::TableReference::partial("public", "users"),
        user_table().schema.as_ref(),
    )
    .expect("dfschema");
    let compiled = compile_scan(CompileScanInput {
        relation: &PgRelation::new(Some("public"), "users"),
        schema: user_table().schema.as_ref(),
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: &[],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan");
    let orphan = Arc::new(
        PgScanSpec::try_new(
            PgScanId::new(42),
            42,
            PgRelation::new(Some("public"), "users"),
            &source_schema,
            compiled,
        )
        .expect("orphan spec"),
    );
    envelope.pg_scan_specs.insert(orphan.scan_id, orphan);

    let mut bytes = BytesMut::new();
    encode_envelope_into(&envelope, &mut bytes).expect("encode envelope");
    let mut session = PlanDecodeSession::new();
    let err = session
        .push_chunk(&bytes)
        .expect_err("orphan scan spec should fail at build stage");
    assert!(matches!(err, DecodeError::OrphanScanSpec { scan_id: 42 }));

    let poisoned = session
        .push_chunk(&[])
        .expect_err("poisoned decoder should not panic");
    assert!(matches!(poisoned, DecodeError::SessionFailed { .. }));
}

#[test]
fn rejects_bad_magic_and_version() {
    let mut bad_magic = BytesMut::new();
    write_array_len_to(&mut bad_magic, PLAN_CODEC_ENVELOPE_LEN).expect("envelope len");
    write_string_to(&mut bad_magic, "BAD!").expect("bad magic");
    write_u8_to(&mut bad_magic, PLAN_CODEC_VERSION).expect("version");
    write_array_len_to(&mut bad_magic, 0).expect("empty scan specs");
    write_bin_len_to(&mut bad_magic, 0).expect("empty logical plan");
    let err = decode_bytes(&bad_magic).expect_err("bad magic should fail");
    assert!(matches!(err, DecodeError::InvalidMagic { .. }));

    let mut bad_version = BytesMut::new();
    write_array_len_to(&mut bad_version, PLAN_CODEC_ENVELOPE_LEN).expect("envelope len");
    write_string_to(&mut bad_version, PLAN_CODEC_MAGIC).expect("magic");
    write_u8_to(&mut bad_version, 99).expect("bad version");
    write_array_len_to(&mut bad_version, 0).expect("empty scan specs");
    write_bin_len_to(&mut bad_version, 0).expect("empty logical plan");
    let err = decode_bytes(&bad_version).expect_err("bad version should fail");
    assert!(matches!(
        err,
        DecodeError::UnsupportedVersion { version: 99 }
    ));
}

#[test]
fn rejects_missing_pg_scan_spec_reference() {
    let built = build_sql("SELECT id FROM users");
    let bytes = encode_bytes(&built.logical_plan);
    let ctx = SessionContext::new();
    let mut source = Bytes::from(bytes);
    let mut envelope = decode_envelope_from(&mut source, &ctx).expect("decode envelope");
    envelope.pg_scan_specs.clear();

    let mut sink = BytesMut::new();
    encode_envelope_into(&envelope, &mut sink).expect("re-encode envelope");
    let err = decode_bytes(&sink).expect_err("missing scan spec should fail");
    assert!(matches!(err, DecodeError::DataFusion(_)));
}

#[test]
fn rejects_orphan_pg_scan_spec() {
    let built = build_sql("SELECT extract(day from now())");
    let mut envelope = collect_plan_envelope(&built.logical_plan).expect("collect envelope");
    let source_schema = DFSchema::try_from_qualified_schema(
        datafusion_common::TableReference::partial("public", "users"),
        user_table().schema.as_ref(),
    )
    .expect("dfschema");
    let compiled = compile_scan(CompileScanInput {
        relation: &PgRelation::new(Some("public"), "users"),
        schema: user_table().schema.as_ref(),
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: &[],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan");
    let orphan = Arc::new(
        PgScanSpec::try_new(
            PgScanId::new(42),
            42,
            PgRelation::new(Some("public"), "users"),
            &source_schema,
            compiled,
        )
        .expect("orphan spec"),
    );
    envelope.pg_scan_specs.insert(orphan.scan_id, orphan);

    let mut sink = BytesMut::new();
    encode_envelope_into(&envelope, &mut sink).expect("encode envelope");
    let err = decode_bytes(&sink).expect_err("orphan scan spec should fail");
    assert!(matches!(err, DecodeError::OrphanScanSpec { scan_id: 42 }));
}

#[test]
fn rejects_malformed_pg_scan_reference_payload() {
    let relation = PgRelation::new(Some("public"), "users");
    let arrow_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let source_schema = DFSchema::try_from_qualified_schema(
        datafusion_common::TableReference::partial("public", "users"),
        &arrow_schema,
    )
    .expect("dfschema");
    let compiled = compile_scan(CompileScanInput {
        relation: &relation,
        schema: &arrow_schema,
        identifier_max_bytes: TEST_IDENTIFIER_MAX_BYTES,
        projection: Some(&[0]),
        filters: &[],
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan");
    let spec = Arc::new(
        PgScanSpec::try_new(PgScanId::new(1), 42, relation, &source_schema, compiled)
            .expect("scan spec"),
    );
    let plan = PgScanNode::new(spec).into_logical_plan();

    let bytes = encode_bytes(&plan);
    let ctx = SessionContext::new();
    let mut source = Bytes::from(bytes);
    let mut envelope = decode_envelope_from(&mut source, &ctx).expect("decode envelope");

    if let Some(LogicalPlanType::Extension(extension)) =
        envelope.logical_plan.logical_plan_type.as_mut()
    {
        extension.node = vec![0xff];
    } else {
        panic!("expected top-level extension plan");
    }

    let mut sink = BytesMut::new();
    encode_envelope_into(&envelope, &mut sink).expect("re-encode envelope");
    let err = decode_bytes(&sink).expect_err("corrupted extension payload should fail");
    assert!(matches!(err, DecodeError::DataFusion(_)));
}
