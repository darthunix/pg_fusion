use datafusion_common::Column;
use datafusion_common::TableReference;
use datafusion_expr::{lit, Expr};
use df_catalog::{CatalogResolver, PgrxCatalogResolver, ResolveError};
use pgrx::prelude::*;
use scan_sql::{compile_scan, CompileScanInput, LimitLowering};

fn resolver() -> PgrxCatalogResolver {
    PgrxCatalogResolver::new()
}

fn pg_identifier_max_bytes() -> usize {
    (pg_sys::NAMEDATALEN as usize).saturating_sub(1)
}

fn exact_limit_identifier(prefix: &str) -> String {
    let max_bytes = pg_identifier_max_bytes();
    assert!(prefix.len() < max_bytes);
    format!("{prefix}{}", "x".repeat(max_bytes - prefix.len()))
}

fn expect_overlong_resolve_error(err: ResolveError, kind: &'static str, identifier: &str) {
    assert_eq!(
        err,
        ResolveError::OverlongIdentifier {
            kind,
            identifier: identifier.to_owned(),
            max_bytes: pg_identifier_max_bytes(),
        }
    );
}

pub fn df_catalog_resolves_bare_names_via_search_path() {
    Spi::run("DROP SCHEMA IF EXISTS df_catalog_a CASCADE").unwrap();
    Spi::run("DROP SCHEMA IF EXISTS df_catalog_b CASCADE").unwrap();
    Spi::run("CREATE SCHEMA df_catalog_a").unwrap();
    Spi::run("CREATE SCHEMA df_catalog_b").unwrap();
    Spi::run("CREATE TABLE df_catalog_a.t_search (id int4 NOT NULL)").unwrap();
    Spi::run("CREATE TABLE df_catalog_b.t_search (id int8)").unwrap();
    Spi::run("SET LOCAL search_path = df_catalog_b, df_catalog_a").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("t_search"))
        .expect("resolve bare table");

    assert_eq!(resolved.relation.schema.as_deref(), Some("df_catalog_b"));
    assert_eq!(resolved.relation.table, "t_search");
    assert_eq!(resolved.schema.fields().len(), 1);
    assert_eq!(
        resolved.schema.field(0).data_type(),
        &arrow_schema::DataType::Int64
    );
}

pub fn df_catalog_resolves_schema_qualified_tables() {
    Spi::run("DROP SCHEMA IF EXISTS df_catalog_q CASCADE").unwrap();
    Spi::run("CREATE SCHEMA df_catalog_q").unwrap();
    Spi::run("CREATE TABLE df_catalog_q.t_partial (id int4)").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::partial("df_catalog_q", "t_partial"))
        .expect("resolve qualified table");

    assert_eq!(resolved.relation.schema.as_deref(), Some("df_catalog_q"));
    assert_eq!(resolved.relation.table, "t_partial");
    assert_eq!(
        resolved.schema.field(0).data_type(),
        &arrow_schema::DataType::Int32
    );
}

pub fn df_catalog_maps_text_like_columns_to_utf8view() {
    Spi::run("DROP TABLE IF EXISTS public.df_catalog_text_like").unwrap();
    Spi::run(
        "CREATE TABLE public.df_catalog_text_like \
         (txt text, vc varchar(16), bp bpchar(4), nm name)",
    )
    .unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("df_catalog_text_like"))
        .expect("resolve text-like table");

    let fields = resolved.schema.fields();
    assert_eq!(fields.len(), 4);
    for field in fields {
        assert_eq!(
            field.data_type(),
            &arrow_schema::DataType::Utf8View,
            "{} should be planned as Utf8View",
            field.name()
        );
    }
}

pub fn df_catalog_bare_lookup_prefers_temp_tables() {
    Spi::run("DROP TABLE IF EXISTS public.df_catalog_temp_shadow").unwrap();
    Spi::run("CREATE TABLE public.df_catalog_temp_shadow (id int4)").unwrap();
    Spi::run("CREATE TEMP TABLE df_catalog_temp_shadow (id int8)").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("df_catalog_temp_shadow"))
        .expect("resolve temp-shadowed bare table");

    assert_eq!(resolved.relation.table, "df_catalog_temp_shadow");
    assert_eq!(resolved.relation.schema.as_deref(), Some("pg_temp"));
    assert_eq!(
        resolved.schema.field(0).data_type(),
        &arrow_schema::DataType::Int64
    );
}

pub fn df_catalog_resolves_pg_temp_alias() {
    Spi::run("CREATE TEMP TABLE df_catalog_temp_alias (id int8)").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::partial("pg_temp", "df_catalog_temp_alias"))
        .expect("resolve pg_temp alias");

    assert_eq!(resolved.relation.table, "df_catalog_temp_alias");
    assert_eq!(resolved.relation.schema.as_deref(), Some("pg_temp"));
    assert_eq!(
        resolved.schema.field(0).data_type(),
        &arrow_schema::DataType::Int64
    );
}

pub fn df_catalog_pg_temp_identity_matches_scan_sql_columns() {
    Spi::run("CREATE TEMP TABLE df_catalog_temp_scan_sql (id int8, payload text)").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::partial(
            "pg_temp",
            "df_catalog_temp_scan_sql",
        ))
        .expect("resolve pg_temp relation for scan_sql");

    let qualified_ref = TableReference::partial("pg_temp", "df_catalog_temp_scan_sql");
    let filters = vec![Expr::Column(Column::new(Some(qualified_ref), "id")).eq(lit(1_i64))];

    let compiled = compile_scan(CompileScanInput {
        relation: &resolved.relation,
        schema: resolved.schema.as_ref(),
        identifier_max_bytes: pg_identifier_max_bytes(),
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan with explicit pg_temp relation");

    assert_eq!(
        compiled.sql,
        "SELECT \"id\" FROM \"pg_temp\".\"df_catalog_temp_scan_sql\" WHERE (\"id\" = 1)"
    );
}

pub fn df_catalog_bare_temp_identity_matches_pg_temp_columns() {
    Spi::run("CREATE TEMP TABLE df_catalog_temp_bare_scan_sql (id int8, payload text)").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("df_catalog_temp_bare_scan_sql"))
        .expect("resolve bare temp relation for scan_sql");

    assert_eq!(
        resolved.relation.schema.as_deref(),
        Some("pg_temp"),
        "bare temp lookup should preserve the logical pg_temp alias"
    );

    let qualified_ref = TableReference::partial("pg_temp", "df_catalog_temp_bare_scan_sql");
    let filters = vec![Expr::Column(Column::new(Some(qualified_ref), "id")).eq(lit(1_i64))];

    let compiled = compile_scan(CompileScanInput {
        relation: &resolved.relation,
        schema: resolved.schema.as_ref(),
        identifier_max_bytes: pg_identifier_max_bytes(),
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect("compile scan with pg_temp-qualified column against bare temp source");

    assert_eq!(
        compiled.sql,
        "SELECT \"id\" FROM \"pg_temp\".\"df_catalog_temp_bare_scan_sql\" WHERE (\"id\" = 1)"
    );
}

pub fn df_catalog_rejects_overlong_bare_identifiers() {
    let long_table = format!("df_catalog_bare_{}", "x".repeat(64));
    let err = resolver()
        .resolve_table(&TableReference::bare(long_table.as_str()))
        .expect_err("overlong bare table should be rejected");

    expect_overlong_resolve_error(err, "table", &long_table);
}

pub fn df_catalog_rejects_overlong_qualified_identifiers() {
    let long_schema = format!("df_catalog_schema_{}", "s".repeat(62));
    let long_table = format!("df_catalog_table_{}", "t".repeat(63));

    let err = resolver()
        .resolve_table(&TableReference::partial(
            long_schema.as_str(),
            long_table.as_str(),
        ))
        .expect_err("overlong qualified table should be rejected");

    expect_overlong_resolve_error(err, "schema", &long_schema);
}

pub fn df_catalog_accepts_exact_limit_bare_identifiers() {
    let exact_table = exact_limit_identifier("df_catalog_exact_");

    Spi::run(&format!("DROP TABLE IF EXISTS public.{exact_table}")).unwrap();
    Spi::run(&format!("CREATE TABLE public.{exact_table} (id int8)")).unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare(exact_table.as_str()))
        .expect("exact-limit bare table should resolve");

    assert_eq!(resolved.relation.schema.as_deref(), Some("public"));
    assert_eq!(resolved.relation.table, exact_table);
}

pub fn df_catalog_rejects_overlong_column_names_in_scan_sql() {
    let long_column = format!("df_catalog_col_{}", "c".repeat(80));

    Spi::run("DROP TABLE IF EXISTS public.df_catalog_long_column").unwrap();
    Spi::run(&format!(
        "CREATE TABLE public.df_catalog_long_column ({long_column} int8)"
    ))
    .unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("df_catalog_long_column"))
        .expect("resolve table with overlong column");

    assert_eq!(resolved.schema.fields().len(), 1);
    assert_ne!(resolved.schema.field(0).name(), long_column.as_str());

    let filters = vec![Expr::Column(Column::from_name(long_column.as_str())).eq(lit(1_i64))];
    let err = compile_scan(CompileScanInput {
        relation: &resolved.relation,
        schema: resolved.schema.as_ref(),
        identifier_max_bytes: pg_identifier_max_bytes(),
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect_err("overlong column name should be rejected");

    assert_eq!(
        err,
        scan_sql::CompileError::OverlongIdentifier {
            kind: "column",
            identifier: long_column,
            max_bytes: pg_identifier_max_bytes(),
        }
    );
}

pub fn df_catalog_rejects_overlong_relation_qualifiers_in_scan_sql() {
    let long_schema = format!("df_catalog_match_schema_{}", "s".repeat(56));
    let long_table = format!("df_catalog_match_table_{}", "t".repeat(57));
    Spi::run("DROP TABLE IF EXISTS public.df_catalog_short_relation").unwrap();
    Spi::run("CREATE TABLE public.df_catalog_short_relation (id int8)").unwrap();
    let resolved = resolver()
        .resolve_table(&TableReference::bare("df_catalog_short_relation"))
        .expect("resolve regular relation for overlong qualifier rejection");

    let qualified_ref = TableReference::partial(long_schema.as_str(), long_table.as_str());
    let filters = vec![Expr::Column(Column::new(Some(qualified_ref), "id")).eq(lit(1_i64))];
    let err = compile_scan(CompileScanInput {
        relation: &resolved.relation,
        schema: resolved.schema.as_ref(),
        identifier_max_bytes: pg_identifier_max_bytes(),
        projection: Some(&[0]),
        filters: &filters,
        requested_limit: None,
        limit_lowering: LimitLowering::ExternalHint,
    })
    .expect_err("overlong relation qualifier should be rejected");

    assert_eq!(
        err,
        scan_sql::CompileError::OverlongIdentifier {
            kind: "schema",
            identifier: long_schema,
            max_bytes: pg_identifier_max_bytes(),
        }
    );
}

pub fn df_catalog_bare_lookup_handles_long_search_paths() {
    for idx in 0..20 {
        Spi::run(&format!(
            "DROP SCHEMA IF EXISTS df_catalog_long_{idx} CASCADE"
        ))
        .unwrap();
        Spi::run(&format!("CREATE SCHEMA df_catalog_long_{idx}")).unwrap();
    }
    Spi::run("CREATE TABLE df_catalog_long_19.t_long_path (id int4)").unwrap();

    let search_path = (0..20)
        .map(|idx| format!("df_catalog_long_{idx}"))
        .collect::<Vec<_>>()
        .join(", ");
    Spi::run(&format!("SET LOCAL search_path = {search_path}")).unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("t_long_path"))
        .expect("resolve table on long search_path");

    assert_eq!(
        resolved.relation.schema.as_deref(),
        Some("df_catalog_long_19")
    );
    assert_eq!(resolved.relation.table, "t_long_path");
}

pub fn df_catalog_pg_temp_without_temp_namespace_reports_missing_table() {
    let err = resolver()
        .resolve_table(&TableReference::partial(
            "pg_temp",
            "df_catalog_missing_before_temp",
        ))
        .expect_err("unresolved pg_temp alias should be a missing table");

    assert_eq!(
        err,
        ResolveError::TableNotFound {
            schema: Some("pg_temp".into()),
            table: "df_catalog_missing_before_temp".into(),
        }
    );
}

pub fn df_catalog_rejects_full_references() {
    let err = resolver()
        .resolve_table(&TableReference::full("postgres", "public", "pg_class"))
        .expect_err("full reference should fail");
    assert_eq!(err, ResolveError::FullReferenceUnsupported);
}

pub fn df_catalog_rejects_plain_views_and_resolves_materialized_views() {
    Spi::run("DROP SCHEMA IF EXISTS df_catalog_v CASCADE").unwrap();
    Spi::run("CREATE SCHEMA df_catalog_v").unwrap();
    Spi::run("CREATE TABLE df_catalog_v.base_t (id int4, payload text)").unwrap();
    Spi::run("CREATE VIEW df_catalog_v.v_plain AS SELECT id, payload FROM df_catalog_v.base_t")
        .unwrap();
    Spi::run(
        "CREATE MATERIALIZED VIEW df_catalog_v.mv_plain AS \
         SELECT id, payload FROM df_catalog_v.base_t",
    )
    .unwrap();

    let view = resolver()
        .resolve_table(&TableReference::partial("df_catalog_v", "v_plain"))
        .expect_err("plain view should be rejected");
    let matview = resolver()
        .resolve_table(&TableReference::partial("df_catalog_v", "mv_plain"))
        .expect("resolve materialized view");

    assert!(matches!(view, ResolveError::UnsupportedRelationKind('v')));
    assert_eq!(matview.relation.schema.as_deref(), Some("df_catalog_v"));
    assert_eq!(matview.relation.table, "mv_plain");
    assert_eq!(matview.schema.fields().len(), 2);
}

pub fn df_catalog_resolves_partitioned_tables() {
    Spi::run("DROP SCHEMA IF EXISTS df_catalog_p CASCADE").unwrap();
    Spi::run("CREATE SCHEMA df_catalog_p").unwrap();
    Spi::run(
        "CREATE TABLE df_catalog_p.part_t (id int4 NOT NULL, payload text) \
         PARTITION BY RANGE (id)",
    )
    .unwrap();
    Spi::run(
        "CREATE TABLE df_catalog_p.part_t_1 PARTITION OF df_catalog_p.part_t \
         FOR VALUES FROM (1) TO (100)",
    )
    .unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::partial("df_catalog_p", "part_t"))
        .expect("resolve partitioned table");

    assert_eq!(resolved.relation.schema.as_deref(), Some("df_catalog_p"));
    assert_eq!(resolved.relation.table, "part_t");
    assert_eq!(resolved.schema.fields().len(), 2);
}

pub fn df_catalog_rejects_unsupported_relation_kinds() {
    Spi::run("DROP SEQUENCE IF EXISTS df_catalog_seq").unwrap();
    Spi::run("CREATE SEQUENCE df_catalog_seq").unwrap();

    let err = resolver()
        .resolve_table(&TableReference::bare("df_catalog_seq"))
        .expect_err("sequence should fail");
    assert!(matches!(err, ResolveError::UnsupportedRelationKind('S')));
}

pub fn df_catalog_rejects_unsupported_types() {
    Spi::run("DROP TABLE IF EXISTS df_catalog_unsupported").unwrap();
    Spi::run("CREATE TABLE df_catalog_unsupported (id int4, payload jsonb)").unwrap();

    let err = resolver()
        .resolve_table(&TableReference::bare("df_catalog_unsupported"))
        .expect_err("jsonb should fail");
    assert!(matches!(
        err,
        ResolveError::UnsupportedType {
            column,
            type_oid: _
        } if column == "payload"
    ));
}

pub fn df_catalog_rejects_timetz_columns() {
    Spi::run("DROP TABLE IF EXISTS df_catalog_timetz").unwrap();
    Spi::run("CREATE TABLE df_catalog_timetz (id int4, at_local timetz)").unwrap();

    let err = resolver()
        .resolve_table(&TableReference::bare("df_catalog_timetz"))
        .expect_err("timetz should fail");
    assert!(matches!(
        err,
        ResolveError::UnsupportedType {
            column,
            type_oid
        } if column == "at_local" && type_oid == pg_sys::TIMETZOID.to_u32()
    ));
}

pub fn df_catalog_skips_dropped_columns_and_preserves_nullability() {
    Spi::run("DROP TABLE IF EXISTS df_catalog_drop").unwrap();
    Spi::run(
        "CREATE TABLE df_catalog_drop (id int4 NOT NULL, gone text, payload text NULL, created date NOT NULL)",
    )
    .unwrap();
    Spi::run("ALTER TABLE df_catalog_drop DROP COLUMN gone").unwrap();

    let resolved = resolver()
        .resolve_table(&TableReference::bare("df_catalog_drop"))
        .expect("resolve table with dropped column");

    let fields = resolved.schema.fields();
    assert_eq!(fields.len(), 3);
    assert_eq!(fields[0].name(), "id");
    assert!(!fields[0].is_nullable());
    assert_eq!(fields[1].name(), "payload");
    assert_eq!(fields[1].data_type(), &arrow_schema::DataType::Utf8View);
    assert!(fields[1].is_nullable());
    assert_eq!(fields[2].name(), "created");
    assert!(!fields[2].is_nullable());
}
