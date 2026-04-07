use std::sync::Arc;

use ::plan_builder::{PlanBuildInput, PlanBuilder};
use ::plan_codec::{DecodeProgress, EncodeProgress, PlanDecodeSession, PlanEncodeSession};
use bytes::Bytes;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::ScalarValue;
use datafusion_expr::logical_plan::LogicalPlan;
use pgrx::prelude::*;
use scan_node::PgScanNode;

fn roundtrip(sql: &str, params: Vec<ScalarValue>) -> (LogicalPlan, LogicalPlan) {
    let built = PlanBuilder::new()
        .build(PlanBuildInput { sql, params })
        .expect("build plan");
    let encoded = encode_all::<29>(&built.logical_plan);
    let decoded = decode_all::<31>(&encoded).expect("decode plan");
    (built.logical_plan, decoded)
}

fn encode_all<const PAGE: usize>(plan: &LogicalPlan) -> Vec<u8> {
    assert!(PAGE > 0);
    let mut session = PlanEncodeSession::new(plan).expect("create encode session");
    let mut bytes = Vec::new();

    loop {
        let mut chunk = [0u8; PAGE];
        match session.write_chunk(&mut chunk).expect("write chunk") {
            EncodeProgress::NeedMoreOutput { written } => {
                assert!(written > 0, "encoder must make forward progress");
                bytes.extend_from_slice(&chunk[..written]);
            }
            EncodeProgress::Done { written } => {
                bytes.extend_from_slice(&chunk[..written]);
                break;
            }
        }
    }

    assert!(session.is_finished());
    bytes
}

fn decode_all<const PAGE: usize>(bytes: &[u8]) -> Result<LogicalPlan, ::plan_codec::DecodeError> {
    assert!(PAGE > 0);
    let mut session = PlanDecodeSession::new();
    for chunk in bytes.chunks(PAGE) {
        let progress = session.push_chunk(Bytes::copy_from_slice(chunk))?;
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
        DecodeProgress::NeedMoreInput => Err(::plan_codec::DecodeError::MsgPack(
            "decode session requires more input".into(),
        )),
    }
}

fn collect_pg_scans(plan: &LogicalPlan) -> Vec<Arc<scan_node::PgScanSpec>> {
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

pub fn plan_codec_roundtrips_live_pg_scan() {
    Spi::run("DROP SCHEMA IF EXISTS plan_codec_ns CASCADE").unwrap();
    Spi::run("CREATE SCHEMA plan_codec_ns").unwrap();
    Spi::run("CREATE TABLE plan_codec_ns.items (id int8 NOT NULL, payload text)").unwrap();

    let (built, decoded) = roundtrip(
        "SELECT id, payload FROM plan_codec_ns.items WHERE id > $1 LIMIT 5",
        vec![ScalarValue::Int64(Some(7))],
    );

    assert_eq!(
        built.display_indent().to_string(),
        decoded.display_indent().to_string()
    );

    let scans = collect_pg_scans(&decoded);
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_id.get(), 1);
    assert_eq!(scans[0].table_oid > 0, true);
    assert_eq!(scans[0].relation.schema.as_deref(), Some("plan_codec_ns"));
    assert_eq!(scans[0].relation.table, "items");
    assert_eq!(
        scans[0].compiled_scan.sql,
        "SELECT \"id\", \"payload\" FROM \"plan_codec_ns\".\"items\" WHERE (\"id\" > 7)"
    );
    assert_eq!(scans[0].fetch_hints.planner_fetch_hint, Some(5));
    assert_eq!(scans[0].fetch_hints.local_row_cap, Some(5));
}

pub fn plan_codec_roundtrips_builtin_sql_forms() {
    Spi::run("DROP TABLE IF EXISTS public.plan_codec_functions").unwrap();
    Spi::run("CREATE TABLE public.plan_codec_functions (id int8 NOT NULL, payload text NOT NULL)")
        .unwrap();

    let (built, decoded) = roundtrip(
        "SELECT length(payload) AS len, substring(payload FROM 1 FOR 1), \
         position('a' in payload) FROM public.plan_codec_functions WHERE payload ~ '^a'",
        Vec::new(),
    );

    assert_eq!(
        built.display_indent().to_string(),
        decoded.display_indent().to_string()
    );
    assert_eq!(collect_pg_scans(&decoded).len(), 1);
    assert!(decoded.display_indent().to_string().contains("Projection"));
}
