use pg_statistics::{
    estimate_equi_join_selectivity, estimate_scan_sql, load_column_stats, load_relation_stats,
    load_unique_keys, EquiJoinInput, EstimateOptions, EstimateQuality,
};
use pgrx::prelude::*;

fn relation_oid(qualified: &str) -> pg_sys::Oid {
    let oid: i32 = Spi::get_one(&format!("SELECT '{qualified}'::regclass::oid::int4"))
        .unwrap()
        .unwrap();
    pg_sys::Oid::from(oid as u32)
}

fn attribute_number(qualified: &str, attribute: &str) -> i16 {
    Spi::get_one(&format!(
        "SELECT attnum::int2 \
         FROM pg_attribute \
         WHERE attrelid = '{qualified}'::regclass \
           AND attname = '{attribute}' \
           AND NOT attisdropped"
    ))
    .unwrap()
    .unwrap()
}

pub fn pg_statistics_estimates_filtered_scan_rows() {
    Spi::run("DROP TABLE IF EXISTS public.pg_statistics_scan").unwrap();
    Spi::run(
        "CREATE TABLE public.pg_statistics_scan AS \
         SELECT g::int4 AS id, md5(g::text) AS payload \
         FROM generate_series(1, 1000) AS g",
    )
    .unwrap();
    Spi::run("ANALYZE public.pg_statistics_scan").unwrap();

    let full = estimate_scan_sql(
        "SELECT id, payload FROM public.pg_statistics_scan",
        EstimateOptions::default(),
    )
    .expect("full scan estimate");
    let filtered = estimate_scan_sql(
        "SELECT id, payload FROM public.pg_statistics_scan WHERE id = 1",
        EstimateOptions::default(),
    )
    .expect("filtered scan estimate");

    assert!(
        full.rows > filtered.rows,
        "expected filter to reduce rows: full={full:?}, filtered={filtered:?}"
    );
    assert!(
        filtered.rows > 0.0,
        "filtered estimate should stay positive"
    );
    assert!(filtered.width > 0, "filtered estimate should expose width");
    assert_eq!(filtered.bytes, filtered.rows * f64::from(filtered.width));
}

pub fn pg_statistics_reads_column_stats() {
    Spi::run("DROP TABLE IF EXISTS public.pg_statistics_column_stats").unwrap();
    Spi::run(
        "CREATE TABLE public.pg_statistics_column_stats AS \
         SELECT g::int4 AS id, \
                CASE WHEN g % 5 = 0 THEN NULL ELSE (g % 7)::text END AS category \
         FROM generate_series(1, 1000) AS g",
    )
    .unwrap();
    Spi::run("ANALYZE public.pg_statistics_column_stats").unwrap();

    let oid = relation_oid("public.pg_statistics_column_stats");
    let category_attnum = attribute_number("public.pg_statistics_column_stats", "category");

    let relation_stats = load_relation_stats(oid).expect("relation stats");
    assert!(relation_stats.rows.unwrap_or_default() > 0.0);
    assert!(relation_stats.pages.unwrap_or_default() > 0);

    let stats = load_column_stats(oid, &[category_attnum]).expect("column stats");
    assert_eq!(stats.len(), 1);

    let category = &stats[0];
    assert_eq!(category.attnum, category_attnum);
    assert!(
        category.null_frac.unwrap_or_default() > 0.0,
        "category should have NULL stats: {category:?}"
    );
    assert!(
        category.ndv.unwrap_or_default() > 0.0,
        "category should have NDV stats: {category:?}"
    );
    assert!(
        category.avg_width.unwrap_or_default() > 0,
        "category should have width stats: {category:?}"
    );
}

pub fn pg_statistics_detects_unique_keys() {
    Spi::run("DROP TABLE IF EXISTS public.pg_statistics_unique").unwrap();
    Spi::run(
        "CREATE TABLE public.pg_statistics_unique \
         (id int4 PRIMARY KEY, tenant int4 NOT NULL, code int4 NOT NULL, UNIQUE (tenant, code))",
    )
    .unwrap();

    let oid = relation_oid("public.pg_statistics_unique");
    let id_attnum = attribute_number("public.pg_statistics_unique", "id");
    let tenant_attnum = attribute_number("public.pg_statistics_unique", "tenant");
    let code_attnum = attribute_number("public.pg_statistics_unique", "code");

    let keys = load_unique_keys(oid).expect("unique keys");
    assert!(
        keys.iter()
            .any(|key| key.primary && key.attnums == vec![id_attnum]),
        "expected primary key in {keys:?}"
    );
    assert!(
        keys.iter()
            .any(|key| key.attnums == vec![tenant_attnum, code_attnum]),
        "expected composite unique key in {keys:?}"
    );
}

pub fn pg_statistics_skips_partial_unique_keys() {
    Spi::run("DROP TABLE IF EXISTS public.pg_statistics_partial_unique").unwrap();
    Spi::run(
        "CREATE TABLE public.pg_statistics_partial_unique \
         (id int4 NOT NULL, active bool NOT NULL)",
    )
    .unwrap();
    Spi::run(
        "CREATE UNIQUE INDEX pg_statistics_partial_unique_id_active_idx \
         ON public.pg_statistics_partial_unique (id) WHERE active",
    )
    .unwrap();

    let oid = relation_oid("public.pg_statistics_partial_unique");
    let id_attnum = attribute_number("public.pg_statistics_partial_unique", "id");

    let keys = load_unique_keys(oid).expect("unique keys");
    assert!(
        keys.iter().all(|key| key.attnums != vec![id_attnum]),
        "partial unique index must not be reported as relation-wide unique key: {keys:?}"
    );
}

pub fn pg_statistics_estimates_equi_join_selectivity() {
    let estimate = estimate_equi_join_selectivity(EquiJoinInput {
        left_rows: 100.0,
        right_rows: 1000.0,
        left_ndv: Some(100.0),
        right_ndv: Some(500.0),
        left_null_frac: Some(0.1),
        right_null_frac: Some(0.2),
        left_unique: false,
        right_unique: true,
    });

    assert_eq!(estimate.quality, EstimateQuality::Full);
    assert_eq!(estimate.rows, 90.0);
    assert_eq!(estimate.selectivity, 90.0 / 100_000.0);
}
