use arrow_layout::TypeTag;
use pgrx::prelude::*;
use row_estimator::EstimatorConfig;
use row_estimator_seed::{seed_estimator_config, ProjectedColumnRef, SeedError};

fn relation_oid(qualified: &str) -> pg_sys::Oid {
    let oid: i32 = Spi::get_one(&format!("SELECT '{qualified}'::regclass::oid::int4"))
        .unwrap()
        .unwrap();
    pg_sys::Oid::from(oid as u32)
}

fn stawidth(qualified: &str, attribute: &str, stainherit: bool) -> Option<i32> {
    Spi::get_one(&format!(
        "SELECT s.stawidth::int4 \
         FROM pg_statistic s \
         JOIN pg_attribute a \
           ON a.attrelid = s.starelid \
          AND a.attnum = s.staattnum \
         WHERE s.starelid = '{qualified}'::regclass \
           AND a.attname = '{attribute}' \
           AND s.stainherit = {stainherit}"
    ))
    .unwrap()
}

pub fn row_estimator_seed_uses_sum_of_text_and_bytea_widths() {
    Spi::run("DROP TABLE IF EXISTS public.row_estimator_seed_stats").unwrap();
    Spi::run(
        "CREATE TABLE public.row_estimator_seed_stats \
         (id int4, payload text, bytes bytea)",
    )
    .unwrap();
    Spi::run(
        "INSERT INTO public.row_estimator_seed_stats VALUES \
         (1, 'alpha', decode('0001', 'hex')), \
         (2, 'alphabet soup', decode('001122334455', 'hex')), \
         (3, repeat('x', 40), decode(repeat('ab', 12), 'hex'))",
    )
    .unwrap();
    Spi::run("ANALYZE public.row_estimator_seed_stats").unwrap();

    let oid = relation_oid("public.row_estimator_seed_stats");
    let expected = stawidth("public.row_estimator_seed_stats", "payload", false)
        .unwrap()
        .saturating_add(stawidth("public.row_estimator_seed_stats", "bytes", false).unwrap());

    let seeded = seed_estimator_config(
        oid,
        &[
            ProjectedColumnRef::relation_attribute("payload", TypeTag::Utf8View),
            ProjectedColumnRef::relation_attribute("bytes", TypeTag::BinaryView),
        ],
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("seed config");

    assert_eq!(seeded.initial_tail_bytes_per_row, expected as u32);
}

pub fn row_estimator_seed_ignores_fixed_width_columns() {
    Spi::run("DROP TABLE IF EXISTS public.row_estimator_seed_mixed").unwrap();
    Spi::run(
        "CREATE TABLE public.row_estimator_seed_mixed \
         (id int4, payload text, bytes bytea)",
    )
    .unwrap();
    Spi::run(
        "INSERT INTO public.row_estimator_seed_mixed VALUES \
         (1, 'short', decode('00', 'hex')), \
         (2, repeat('z', 12), decode(repeat('cd', 8), 'hex'))",
    )
    .unwrap();
    Spi::run("ANALYZE public.row_estimator_seed_mixed").unwrap();

    let oid = relation_oid("public.row_estimator_seed_mixed");
    let expected = stawidth("public.row_estimator_seed_mixed", "payload", false).unwrap();

    let seeded = seed_estimator_config(
        oid,
        &[
            ProjectedColumnRef::relation_attribute("id", TypeTag::Int32),
            ProjectedColumnRef::relation_attribute("payload", TypeTag::Utf8View),
        ],
        EstimatorConfig {
            initial_tail_bytes_per_row: 17,
        },
    )
    .expect("seed config");

    assert_eq!(seeded.initial_tail_bytes_per_row, expected as u32);
}

pub fn row_estimator_seed_preserves_default_without_stats() {
    Spi::run("CREATE TEMP TABLE row_estimator_seed_no_stats (payload text)").unwrap();
    Spi::run("INSERT INTO row_estimator_seed_no_stats VALUES ('alpha'), ('beta')").unwrap();

    let default = EstimatorConfig {
        initial_tail_bytes_per_row: 91,
    };
    let seeded = seed_estimator_config(
        relation_oid("pg_temp.row_estimator_seed_no_stats"),
        &[ProjectedColumnRef::relation_attribute(
            "payload",
            TypeTag::Utf8View,
        )],
        default,
    )
    .expect("seed config");

    assert_eq!(seeded, default);
}

pub fn row_estimator_seed_preserves_default_for_synthetic_columns() {
    let default = EstimatorConfig {
        initial_tail_bytes_per_row: 77,
    };
    let seeded = seed_estimator_config(
        pg_sys::InvalidOid,
        &[ProjectedColumnRef::synthetic(TypeTag::Utf8View)],
        default,
    )
    .expect("seed config");

    assert_eq!(seeded, default);
}

pub fn row_estimator_seed_name_columns_preserve_default() {
    Spi::run("DROP TABLE IF EXISTS public.row_estimator_seed_name_stats").unwrap();
    Spi::run("CREATE TABLE public.row_estimator_seed_name_stats (label name)").unwrap();
    Spi::run(
        "INSERT INTO public.row_estimator_seed_name_stats VALUES \
         ('pg_class'), ('short'), ('very_long_relation_name')",
    )
    .unwrap();
    Spi::run("ANALYZE public.row_estimator_seed_name_stats").unwrap();

    let default = EstimatorConfig {
        initial_tail_bytes_per_row: 83,
    };
    let stat_width = stawidth("public.row_estimator_seed_name_stats", "label", false)
        .expect("name columns should still expose a positive stawidth");
    assert!(stat_width > 0);

    let seeded = seed_estimator_config(
        relation_oid("public.row_estimator_seed_name_stats"),
        &[ProjectedColumnRef::relation_attribute(
            "label",
            TypeTag::Utf8View,
        )],
        default,
    )
    .expect("seed config");

    assert_eq!(seeded, default);
}

pub fn row_estimator_seed_prefers_inherited_stats_for_partitioned_parent() {
    Spi::run("DROP TABLE IF EXISTS public.row_estimator_seed_parent CASCADE").unwrap();
    Spi::run(
        "CREATE TABLE public.row_estimator_seed_parent \
         (id int4, payload text) PARTITION BY RANGE (id)",
    )
    .unwrap();
    Spi::run(
        "CREATE TABLE public.row_estimator_seed_parent_p1 \
         PARTITION OF public.row_estimator_seed_parent FOR VALUES FROM (0) TO (100)",
    )
    .unwrap();
    Spi::run(
        "CREATE TABLE public.row_estimator_seed_parent_p2 \
         PARTITION OF public.row_estimator_seed_parent FOR VALUES FROM (100) TO (200)",
    )
    .unwrap();
    Spi::run(
        "INSERT INTO public.row_estimator_seed_parent VALUES \
         (1, repeat('a', 5)), \
         (2, repeat('b', 20)), \
         (101, repeat('c', 60))",
    )
    .unwrap();
    Spi::run("ANALYZE public.row_estimator_seed_parent").unwrap();

    let expected = stawidth("public.row_estimator_seed_parent", "payload", true)
        .expect("partitioned parent should expose inherited stats");
    let seeded = seed_estimator_config(
        relation_oid("public.row_estimator_seed_parent"),
        &[ProjectedColumnRef::relation_attribute(
            "payload",
            TypeTag::Utf8View,
        )],
        EstimatorConfig {
            initial_tail_bytes_per_row: 12,
        },
    )
    .expect("seed config");

    assert_eq!(seeded.initial_tail_bytes_per_row, expected as u32);
}

pub fn row_estimator_seed_reports_missing_attribute() {
    Spi::run("CREATE TEMP TABLE row_estimator_seed_missing_attr (id int4)").unwrap();

    let err = seed_estimator_config(
        relation_oid("pg_temp.row_estimator_seed_missing_attr"),
        &[ProjectedColumnRef::relation_attribute(
            "missing",
            TypeTag::Utf8View,
        )],
        EstimatorConfig::default(),
    )
    .expect_err("missing attr should error");

    assert!(matches!(
        err,
        SeedError::AttributeNotFound { attribute, .. } if attribute == "missing"
    ));
}

pub fn row_estimator_seed_reports_type_mismatch() {
    Spi::run("CREATE TEMP TABLE row_estimator_seed_type_mismatch (id int4)").unwrap();

    let err = seed_estimator_config(
        relation_oid("pg_temp.row_estimator_seed_type_mismatch"),
        &[ProjectedColumnRef::relation_attribute(
            "id",
            TypeTag::Utf8View,
        )],
        EstimatorConfig::default(),
    )
    .expect_err("type mismatch should error");

    assert!(matches!(
        err,
        SeedError::TypeMismatch {
            attribute,
            expected: TypeTag::Utf8View,
            actual_oid,
            ..
        } if attribute == "id" && actual_oid == pg_sys::INT4OID.to_u32()
    ));
}
