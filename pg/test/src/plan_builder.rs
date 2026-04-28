use ::plan_builder::{BuiltPlan, PlanBuildError, PlanBuildInput, PlanBuilder, PlanBuilderConfig};
use datafusion_common::ScalarValue;
use datafusion_expr::logical_plan::LogicalPlan;
use pgrx::prelude::*;

fn build(sql: &str, params: Vec<ScalarValue>) -> BuiltPlan {
    PlanBuilder::new()
        .build(PlanBuildInput { sql, params })
        .expect("build plan")
}

fn build_err(sql: &str, params: Vec<ScalarValue>) -> PlanBuildError {
    PlanBuilder::new()
        .build(PlanBuildInput { sql, params })
        .expect_err("plan build should fail")
}

fn bottom_join_on(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Join(join) => {
            let left_nested = bottom_join_on(&join.left);
            let right_nested = bottom_join_on(&join.right);
            left_nested.or(right_nested).or_else(|| {
                Some(
                    join.on
                        .iter()
                        .map(|(left, right)| format!("{left} = {right}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            })
        }
        LogicalPlan::Projection(projection) => bottom_join_on(&projection.input),
        LogicalPlan::Filter(filter) => bottom_join_on(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => bottom_join_on(&alias.input),
        _ => None,
    }
}

pub fn plan_builder_lowers_live_table_scan() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_live").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_live (id int8 NOT NULL, payload text)").unwrap();

    let built = build(
        "SELECT id, payload FROM plan_builder_live WHERE id > 1 LIMIT 5",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 1);
    assert_eq!(built.scans[0].scan_id.get(), 1);
    assert_eq!(built.scans[0].relation.schema.as_deref(), Some("public"));
    assert_eq!(built.scans[0].relation.table, "plan_builder_live");
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\", \"payload\" FROM \"public\".\"plan_builder_live\" WHERE (\"id\" > 1)"
    );
    assert_eq!(built.scans[0].fetch_hints.planner_fetch_hint, Some(5));
    assert_eq!(built.scans[0].fetch_hints.local_row_cap, Some(5));

    let rendered = built.logical_plan.display_indent().to_string();
    assert!(rendered.contains("PgScan:"));
    assert!(!rendered.contains("TableScan:"));
}

pub fn plan_builder_resolves_schema_qualified_table() {
    Spi::run("DROP SCHEMA IF EXISTS plan_builder_ns CASCADE").unwrap();
    Spi::run("CREATE SCHEMA plan_builder_ns").unwrap();
    Spi::run("CREATE TABLE plan_builder_ns.items (id int8 NOT NULL, payload text)").unwrap();

    let built = build(
        "SELECT payload FROM plan_builder_ns.items WHERE id >= 10",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].relation.schema.as_deref(),
        Some("plan_builder_ns")
    );
    assert_eq!(built.scans[0].relation.table, "items");
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"payload\" FROM \"plan_builder_ns\".\"items\" WHERE (\"id\" >= 10)"
    );
}

pub fn plan_builder_binds_params_before_lowering() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_params").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_params (id int8 NOT NULL)").unwrap();

    let built = build(
        "SELECT id FROM public.plan_builder_params WHERE id > $1",
        vec![ScalarValue::Int64(Some(7))],
    );

    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"plan_builder_params\" WHERE (\"id\" > 7)"
    );
}

pub fn plan_builder_partitioned_parent_lowers_to_pg_scan() {
    Spi::run("DROP SCHEMA IF EXISTS plan_builder_part CASCADE").unwrap();
    Spi::run("CREATE SCHEMA plan_builder_part").unwrap();
    Spi::run(
        "CREATE TABLE plan_builder_part.events (id int8 NOT NULL, payload text) \
         PARTITION BY RANGE (id)",
    )
    .unwrap();
    Spi::run(
        "CREATE TABLE plan_builder_part.events_1 PARTITION OF plan_builder_part.events \
         FOR VALUES FROM (1) TO (100)",
    )
    .unwrap();

    let built = build(
        "SELECT id FROM plan_builder_part.events WHERE id >= 1",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].relation.schema.as_deref(),
        Some("plan_builder_part")
    );
    assert_eq!(built.scans[0].relation.table, "events");
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"plan_builder_part\".\"events\" WHERE (\"id\" >= 1)"
    );
    assert!(built
        .logical_plan
        .display_indent()
        .to_string()
        .contains("PgScan:"));
}

pub fn plan_builder_supports_builtin_sql_forms() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_functions").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_functions (payload text NOT NULL)").unwrap();

    let built = build(
        "SELECT length(payload) AS len FROM public.plan_builder_functions",
        Vec::new(),
    );
    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"payload\" FROM \"public\".\"plan_builder_functions\""
    );

    let built = build(
        "SELECT char_length(payload) AS len FROM public.plan_builder_functions",
        Vec::new(),
    );
    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"payload\" FROM \"public\".\"plan_builder_functions\""
    );

    let built = build(
        "SELECT substring(payload FROM 1 FOR 1), position('a' in payload) \
         FROM public.plan_builder_functions",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 1);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"payload\" FROM \"public\".\"plan_builder_functions\""
    );
    assert!(built
        .logical_plan
        .display_indent()
        .to_string()
        .contains("Projection"));

    let built = build("SELECT extract(day from now())", Vec::new());
    assert!(built.scans.is_empty());
}

pub fn plan_builder_rejects_exists_subqueries() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_exists_users").unwrap();
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_exists_orders").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_exists_users (id int8 NOT NULL)").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_exists_orders (user_id int8 NOT NULL)").unwrap();

    let error = build_err(
        "SELECT EXISTS(SELECT 1 FROM public.plan_builder_exists_orders) \
         FROM public.plan_builder_exists_users",
        Vec::new(),
    );

    match error {
        PlanBuildError::UnsupportedSubquery(message) => {
            assert!(message.contains("EXISTS"), "{message}");
        }
        other => panic!("expected unsupported subquery error, got {other:?}"),
    }
}

pub fn plan_builder_rewrites_in_subquery_predicates() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_in_users").unwrap();
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_in_orders").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_in_users (id int8 NOT NULL)").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_in_orders (user_id int8 NOT NULL)").unwrap();

    let built = build(
        "SELECT id FROM public.plan_builder_in_users \
         WHERE id IN (SELECT user_id FROM public.plan_builder_in_orders)",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 2);
    assert_eq!(
        built.scans[0].compiled_scan.sql,
        "SELECT \"id\" FROM \"public\".\"plan_builder_in_users\""
    );
    assert_eq!(
        built.scans[1].compiled_scan.sql,
        "SELECT \"user_id\" FROM \"public\".\"plan_builder_in_orders\""
    );
    assert!(built
        .logical_plan
        .display_indent()
        .to_string()
        .contains("LeftSemi Join"));
}

pub fn plan_builder_reorders_inner_joins_from_live_stats() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_jr_users").unwrap();
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_jr_items").unwrap();
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_jr_orders").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_jr_users (id int8 NOT NULL)").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_jr_items (id int8 NOT NULL, user_id int8 NOT NULL)")
        .unwrap();
    Spi::run(
        "CREATE TABLE public.plan_builder_jr_orders (id int8 NOT NULL, user_id int8 NOT NULL)",
    )
    .unwrap();
    Spi::run("INSERT INTO public.plan_builder_jr_users SELECT g FROM generate_series(1, 10000) g")
        .unwrap();
    Spi::run(
        "INSERT INTO public.plan_builder_jr_items SELECT g, g FROM generate_series(1, 1000) g",
    )
    .unwrap();
    Spi::run("INSERT INTO public.plan_builder_jr_orders SELECT g, g FROM generate_series(1, 10) g")
        .unwrap();
    Spi::run("ANALYZE public.plan_builder_jr_users").unwrap();
    Spi::run("ANALYZE public.plan_builder_jr_items").unwrap();
    Spi::run("ANALYZE public.plan_builder_jr_orders").unwrap();

    let built = build(
        "SELECT u.id, i.id, o.id \
         FROM public.plan_builder_jr_users u \
         JOIN public.plan_builder_jr_items i ON u.id = i.user_id \
         JOIN public.plan_builder_jr_orders o ON u.id = o.user_id",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 3);
    assert_eq!(
        bottom_join_on(&built.logical_plan).as_deref(),
        Some("u.id = o.user_id")
    );

    let disabled = PlanBuilder::new()
        .with_config(PlanBuilderConfig {
            join_reordering_enabled: false,
            ..PlanBuilderConfig::default()
        })
        .build(PlanBuildInput {
            sql: "SELECT u.id, i.id, o.id \
                  FROM public.plan_builder_jr_users u \
                  JOIN public.plan_builder_jr_items i ON u.id = i.user_id \
                  JOIN public.plan_builder_jr_orders o ON u.id = o.user_id",
            params: Vec::new(),
        })
        .expect("build plan with join reordering disabled");
    assert_eq!(
        bottom_join_on(&disabled.logical_plan).as_deref(),
        Some("u.id = i.user_id")
    );
}

pub fn plan_builder_join_reordering_uses_live_attnums_after_drop() {
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_jr_drop_a").unwrap();
    Spi::run("DROP TABLE IF EXISTS public.plan_builder_jr_drop_b").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_jr_drop_a (id int8, dropped int8, key int8)")
        .unwrap();
    Spi::run("ALTER TABLE public.plan_builder_jr_drop_a DROP COLUMN dropped").unwrap();
    Spi::run("CREATE TABLE public.plan_builder_jr_drop_b (id int8, key int8)").unwrap();
    Spi::run(
        "INSERT INTO public.plan_builder_jr_drop_a (id, key) \
         SELECT g, g FROM generate_series(1, 100) g",
    )
    .unwrap();
    Spi::run(
        "INSERT INTO public.plan_builder_jr_drop_b \
         SELECT g, g FROM generate_series(1, 10) g",
    )
    .unwrap();
    Spi::run("ANALYZE public.plan_builder_jr_drop_a").unwrap();
    Spi::run("ANALYZE public.plan_builder_jr_drop_b").unwrap();

    let built = build(
        "SELECT a.id, b.id \
         FROM public.plan_builder_jr_drop_a a \
         JOIN public.plan_builder_jr_drop_b b ON a.key = b.key",
        Vec::new(),
    );

    assert_eq!(built.scans.len(), 2);
    assert!(built
        .logical_plan
        .display_indent()
        .to_string()
        .contains("a.key = b.key"));
}
