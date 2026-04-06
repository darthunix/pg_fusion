use std::sync::Arc;

use ::plan_builder::{PlanBuildInput, PlanBuilder};
use ::plan_codec::{decode_plan_from, encode_plan_into};
use bytes::BytesMut;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::ScalarValue;
use datafusion_expr::logical_plan::LogicalPlan;
use pgrx::prelude::*;
use scan_node::PgScanNode;

fn roundtrip(sql: &str, params: Vec<ScalarValue>) -> (LogicalPlan, LogicalPlan) {
    let built = PlanBuilder::new()
        .build(PlanBuildInput { sql, params })
        .expect("build plan");
    let mut sink = BytesMut::new();
    encode_plan_into(&built.logical_plan, &mut sink).expect("encode plan");
    let mut source = sink.freeze();
    let decoded = decode_plan_from(&mut source).expect("decode plan");
    (built.logical_plan, decoded)
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
