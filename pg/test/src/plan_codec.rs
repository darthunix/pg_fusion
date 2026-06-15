use std::sync::Arc;

use ::plan_builder::{build_preplanned_logical_plan, HybridPlan, PlanBuilderConfig};
use ::plan_codec::{DecodeProgress, EncodeProgress, PlanDecodeSession, PlanEncodeSession};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, TableReference};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::logical_plan::{LogicalPlan, TableScan};
use datafusion_expr::{lit, Expr, Operator, TableSource};
use df_catalog::{CatalogResolver, PgPlanningTableSource, PgrxCatalogResolver};
use pgrx::prelude::*;
use scan_node::PgScanNode;

fn roundtrip(plan: LogicalPlan) -> (LogicalPlan, LogicalPlan) {
    let built = build_hybrid(plan);
    let encoded = encode_all::<29>(&built.logical_plan);
    let decoded = decode_all::<31>(&encoded).expect("decode plan");
    (built.logical_plan, decoded)
}

fn build_hybrid(plan: LogicalPlan) -> HybridPlan {
    build_preplanned_logical_plan(plan, PlanBuilderConfig::default()).expect("build plan")
}

fn live_table_scan(
    table: TableReference,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
) -> LogicalPlan {
    let resolved = PgrxCatalogResolver::new()
        .resolve_table(&table)
        .expect("resolve live table");
    let source = Arc::new(PgPlanningTableSource::new(resolved)) as Arc<dyn TableSource>;
    LogicalPlan::TableScan(TableScan::try_new(table, source, projection, filters, fetch).unwrap())
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

fn normalize_plan_display_for_builtin_sql_forms(plan: &LogicalPlan) -> String {
    plan.display_indent()
        .to_string()
        .replace("public.plan_codec_functions.", "")
}

pub fn plan_codec_roundtrips_live_pg_scan() {
    Spi::run("DROP SCHEMA IF EXISTS plan_codec_ns CASCADE").unwrap();
    Spi::run("CREATE SCHEMA plan_codec_ns").unwrap();
    Spi::run("CREATE TABLE plan_codec_ns.items (id int8 NOT NULL, payload text)").unwrap();

    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("id"))),
        Operator::Gt,
        Box::new(lit(7_i64)),
    ));
    let (built, decoded) = roundtrip(live_table_scan(
        TableReference::partial("plan_codec_ns", "items"),
        Some(vec![0, 1]),
        vec![filter],
        Some(5),
    ));

    assert_eq!(
        normalize_plan_display_for_builtin_sql_forms(&built),
        normalize_plan_display_for_builtin_sql_forms(&decoded)
    );

    let scans = collect_pg_scans(&decoded);
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].scan_id.get(), 1);
    assert!(scans[0].table_oid > 0);
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

    let filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::from_name("payload"))),
        Operator::RegexMatch,
        Box::new(lit("^a")),
    ));
    let (built, decoded) = roundtrip(live_table_scan(
        TableReference::partial("public", "plan_codec_functions"),
        None,
        vec![filter],
        None,
    ));

    assert_eq!(
        normalize_plan_display_for_builtin_sql_forms(&built),
        normalize_plan_display_for_builtin_sql_forms(&decoded)
    );
    let scans = collect_pg_scans(&decoded);
    assert_eq!(scans.len(), 1);
    assert_eq!(
        scans[0].compiled_scan.sql,
        "SELECT \"id\", \"payload\" FROM \"public\".\"plan_codec_functions\" WHERE (\"payload\" ~ '^a')"
    );
    assert_eq!(scans[0].compiled_scan.pushed_filters.len(), 1);
}
