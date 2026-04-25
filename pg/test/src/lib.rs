#[cfg(any(test, feature = "pg_test"))]
use pgrx::pg_schema;

::pgrx::pg_module_magic!();

#[cfg(any(test, feature = "pg_test"))]
mod backend_service;
#[cfg(any(test, feature = "pg_test"))]
mod backend_service_unit;
#[cfg(any(test, feature = "pg_test"))]
mod df_catalog;
#[cfg(any(test, feature = "pg_test"))]
mod page_arrow_pipeline;
#[cfg(any(test, feature = "pg_test"))]
mod plan_builder;
#[cfg(any(test, feature = "pg_test"))]
mod plan_codec;
#[cfg(any(test, feature = "pg_test"))]
mod row_estimator_seed;
#[cfg(any(test, feature = "pg_test"))]
mod slot_deform_bench;
#[cfg(any(test, feature = "pg_test"))]
mod slot_import;
#[cfg(any(test, feature = "pg_test"))]
mod slot_scan;

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_extern]
    fn slot_deform_vs_page_encode_bench(
        profile: default!(String, "'mixed'"),
        rows: default!(i32, "100000"),
        iterations: default!(i32, "3"),
        rows_per_page: default!(i32, "64"),
        payload_capacity_bytes: default!(i32, "8172"),
    ) -> pgrx::JsonB {
        super::slot_deform_bench::slot_deform_vs_page_encode_bench(
            profile,
            rows,
            iterations,
            rows_per_page,
            payload_capacity_bytes,
        )
    }

    #[pg_extern]
    fn slot_deform_bench_prepare(
        profile: default!(String, "'mixed'"),
        rows: default!(i32, "100000"),
    ) -> pgrx::JsonB {
        super::slot_deform_bench::slot_deform_bench_prepare(profile, rows)
    }

    #[pg_test]
    fn backend_service_streams_scan_under_saved_snapshot() {
        super::backend_service::backend_service_streams_scan_under_saved_snapshot();
    }

    #[pg_test]
    fn backend_service_yields_for_control_on_permit_backpressure() {
        super::backend_service::backend_service_yields_for_control_on_permit_backpressure();
    }

    #[pg_test]
    fn backend_service_driver_fail_execution_from_control_yield() {
        super::backend_service::backend_service_driver_fail_execution_from_control_yield();
    }

    #[pg_test]
    fn backend_service_wait_interrupt_cleans_up_active_execution() {
        super::backend_service::backend_service_wait_interrupt_cleans_up_active_execution();
    }

    #[pg_test]
    fn backend_service_stale_cancel_is_ignored_after_new_execution() {
        super::backend_service::backend_service_stale_cancel_is_ignored_after_new_execution();
    }

    #[pg_test]
    fn backend_service_render_explain_uses_physical_plan_and_pg_leaf() {
        super::backend_service::backend_service_render_explain_uses_physical_plan_and_pg_leaf();
    }

    #[pg_test]
    fn backend_service_cancel_during_stream_marks_scan_used() {
        super::backend_service::backend_service_cancel_during_stream_marks_scan_used();
    }

    #[pg_test]
    fn backend_service_rejects_descriptor_mismatch_cleanly() {
        super::backend_service::backend_service_rejects_descriptor_mismatch_without_poisoning_execution();
    }

    #[pg_test]
    fn backend_service_interleaves_two_scan_portals_under_shared_spi() {
        super::backend_service::backend_service_interleaves_two_scan_portals_under_shared_spi();
    }

    #[pg_test]
    fn backend_service_finished_driver_drop_keeps_sibling_alive() {
        super::backend_service::backend_service_drop_finished_driver_does_not_cancel_sibling_scan();
    }

    #[pg_test]
    fn bs_unit_future_session_rejected() {
        super::backend_service_unit::future_session_is_rejected_without_active_execution();
    }

    #[pg_test]
    fn bs_unit_stale_session_ignored() {
        super::backend_service_unit::stale_session_is_ignored_for_active_execution();
    }

    #[pg_test]
    fn bs_unit_same_epoch_other_slot_ignored() {
        super::backend_service_unit::same_epoch_other_slot_is_ignored();
    }

    #[pg_test]
    fn bs_unit_fail_while_starting() {
        super::backend_service_unit::fail_execution_is_accepted_while_starting();
    }

    #[pg_test]
    fn bs_unit_cancel_while_starting() {
        super::backend_service_unit::cancel_execution_is_accepted_while_starting();
    }

    #[pg_test]
    fn bs_unit_explain_rejects_active_execution() {
        super::backend_service_unit::render_explain_is_rejected_while_execution_is_active();
    }

    #[pg_test]
    fn bs_unit_finalize_error_preserves_start() {
        super::backend_service_unit::finalize_execution_start_error_preserves_starting_runtime();
    }

    #[pg_test]
    fn bs_unit_scan_descriptor_exact_match() {
        super::backend_service_unit::scan_descriptor_matches_accepts_exact_ordered_match();
    }

    #[pg_test]
    fn bs_unit_scan_descriptor_page_kind_mismatch() {
        super::backend_service_unit::scan_descriptor_matches_rejects_page_kind_mismatch();
    }

    #[pg_test]
    fn bs_unit_scan_descriptor_page_flags_mismatch() {
        super::backend_service_unit::scan_descriptor_matches_rejects_page_flags_mismatch();
    }

    #[pg_test]
    fn bs_unit_scan_descriptor_order_mismatch() {
        super::backend_service_unit::scan_descriptor_matches_rejects_producer_order_mismatch();
    }

    #[pg_test]
    fn bs_unit_scan_descriptor_role_mismatch() {
        super::backend_service_unit::scan_descriptor_matches_rejects_producer_role_mismatch();
    }

    #[pg_test]
    fn bs_unit_scan_descriptor_count_mismatch() {
        super::backend_service_unit::scan_descriptor_matches_rejects_missing_or_extra_producers();
    }

    #[pg_test]
    fn df_catalog_resolves_bare_names_via_search_path() {
        super::df_catalog::df_catalog_resolves_bare_names_via_search_path();
    }

    #[pg_test]
    fn df_catalog_resolves_schema_qualified_tables() {
        super::df_catalog::df_catalog_resolves_schema_qualified_tables();
    }

    #[pg_test]
    fn df_catalog_bare_lookup_prefers_temp_tables() {
        super::df_catalog::df_catalog_bare_lookup_prefers_temp_tables();
    }

    #[pg_test]
    fn df_catalog_resolves_pg_temp_alias() {
        super::df_catalog::df_catalog_resolves_pg_temp_alias();
    }

    #[pg_test]
    fn df_catalog_pg_temp_identity_matches_scan_sql_columns() {
        super::df_catalog::df_catalog_pg_temp_identity_matches_scan_sql_columns();
    }

    #[pg_test]
    fn df_catalog_bare_temp_identity_matches_pg_temp_columns() {
        super::df_catalog::df_catalog_bare_temp_identity_matches_pg_temp_columns();
    }

    #[pg_test]
    fn df_catalog_rejects_overlong_bare_identifiers() {
        super::df_catalog::df_catalog_rejects_overlong_bare_identifiers();
    }

    #[pg_test]
    fn df_catalog_rejects_overlong_qualified_identifiers() {
        super::df_catalog::df_catalog_rejects_overlong_qualified_identifiers();
    }

    #[pg_test]
    fn df_catalog_accepts_exact_limit_bare_identifiers() {
        super::df_catalog::df_catalog_accepts_exact_limit_bare_identifiers();
    }

    #[pg_test]
    fn df_catalog_rejects_overlong_column_names_in_scan_sql() {
        super::df_catalog::df_catalog_rejects_overlong_column_names_in_scan_sql();
    }

    #[pg_test]
    fn df_catalog_rejects_overlong_relation_qualifiers_in_scan_sql() {
        super::df_catalog::df_catalog_rejects_overlong_relation_qualifiers_in_scan_sql();
    }

    #[pg_test]
    fn df_catalog_bare_lookup_handles_long_search_paths() {
        super::df_catalog::df_catalog_bare_lookup_handles_long_search_paths();
    }

    #[pg_test]
    fn df_catalog_pg_temp_without_temp_namespace_reports_missing_table() {
        super::df_catalog::df_catalog_pg_temp_without_temp_namespace_reports_missing_table();
    }

    #[pg_test]
    fn df_catalog_rejects_full_references() {
        super::df_catalog::df_catalog_rejects_full_references();
    }

    #[pg_test]
    fn df_catalog_rejects_plain_views_and_resolves_materialized_views() {
        super::df_catalog::df_catalog_rejects_plain_views_and_resolves_materialized_views();
    }

    #[pg_test]
    fn df_catalog_resolves_partitioned_tables() {
        super::df_catalog::df_catalog_resolves_partitioned_tables();
    }

    #[pg_test]
    fn df_catalog_rejects_unsupported_relation_kinds() {
        super::df_catalog::df_catalog_rejects_unsupported_relation_kinds();
    }

    #[pg_test]
    fn df_catalog_rejects_unsupported_types() {
        super::df_catalog::df_catalog_rejects_unsupported_types();
    }

    #[pg_test]
    fn df_catalog_rejects_timetz_columns() {
        super::df_catalog::df_catalog_rejects_timetz_columns();
    }

    #[pg_test]
    fn row_estimator_seed_uses_sum_of_text_and_bytea_widths() {
        super::row_estimator_seed::row_estimator_seed_uses_sum_of_text_and_bytea_widths();
    }

    #[pg_test]
    fn row_estimator_seed_ignores_fixed_width_columns() {
        super::row_estimator_seed::row_estimator_seed_ignores_fixed_width_columns();
    }

    #[pg_test]
    fn row_estimator_seed_preserves_default_without_stats() {
        super::row_estimator_seed::row_estimator_seed_preserves_default_without_stats();
    }

    #[pg_test]
    fn row_estimator_seed_preserves_default_for_synthetic_columns() {
        super::row_estimator_seed::row_estimator_seed_preserves_default_for_synthetic_columns();
    }

    #[pg_test]
    fn row_estimator_seed_name_columns_preserve_default() {
        super::row_estimator_seed::row_estimator_seed_name_columns_preserve_default();
    }

    #[pg_test]
    fn row_estimator_seed_partitioned_parent_uses_inherited_stats() {
        super::row_estimator_seed::row_estimator_seed_prefers_inherited_stats_for_partitioned_parent();
    }

    #[pg_test]
    fn row_estimator_seed_reports_missing_attribute() {
        super::row_estimator_seed::row_estimator_seed_reports_missing_attribute();
    }

    #[pg_test]
    fn row_estimator_seed_reports_type_mismatch() {
        super::row_estimator_seed::row_estimator_seed_reports_type_mismatch();
    }

    #[pg_test]
    fn df_catalog_skips_dropped_columns_and_preserves_nullability() {
        super::df_catalog::df_catalog_skips_dropped_columns_and_preserves_nullability();
    }

    #[pg_test]
    fn plan_builder_lowers_live_table_scan() {
        super::plan_builder::plan_builder_lowers_live_table_scan();
    }

    #[pg_test]
    fn plan_builder_resolves_schema_qualified_table() {
        super::plan_builder::plan_builder_resolves_schema_qualified_table();
    }

    #[pg_test]
    fn plan_builder_binds_params_before_lowering() {
        super::plan_builder::plan_builder_binds_params_before_lowering();
    }

    #[pg_test]
    fn plan_builder_partitioned_parent_lowers_to_pg_scan() {
        super::plan_builder::plan_builder_partitioned_parent_lowers_to_pg_scan();
    }

    #[pg_test]
    fn plan_builder_supports_builtin_sql_forms() {
        super::plan_builder::plan_builder_supports_builtin_sql_forms();
    }

    #[pg_test]
    fn plan_builder_rejects_exists_subqueries() {
        super::plan_builder::plan_builder_rejects_exists_subqueries();
    }

    #[pg_test]
    fn plan_builder_rejects_in_subquery_predicates() {
        super::plan_builder::plan_builder_rejects_in_subquery_predicates();
    }

    #[pg_test]
    fn plan_codec_roundtrips_live_pg_scan() {
        super::plan_codec::plan_codec_roundtrips_live_pg_scan();
    }

    #[pg_test]
    fn plan_codec_roundtrips_builtin_sql_forms() {
        super::plan_codec::plan_codec_roundtrips_builtin_sql_forms();
    }

    #[pg_extern]
    fn slot_deform_baseline_bench(
        profile: default!(String, "'mixed'"),
        iterations: default!(i32, "3"),
    ) -> pgrx::JsonB {
        super::slot_deform_bench::slot_deform_baseline_bench(profile, iterations)
    }

    #[pg_extern]
    fn slot_deform_arrow_bench(
        profile: default!(String, "'mixed'"),
        iterations: default!(i32, "3"),
        rows_per_page: default!(i32, "64"),
        payload_capacity_bytes: default!(i32, "8172"),
    ) -> pgrx::JsonB {
        super::slot_deform_bench::slot_deform_arrow_bench(
            profile,
            iterations,
            rows_per_page,
            payload_capacity_bytes,
        )
    }

    #[pg_test]
    fn page_arrow_pipeline_roundtrip_inside_postgres() {
        super::page_arrow_pipeline::page_arrow_pipeline_roundtrip_inside_postgres();
    }

    #[pg_test]
    fn slot_deform_vs_page_encode_bench_fixed_smoke() {
        super::slot_deform_bench::slot_deform_vs_page_encode_bench_fixed_smoke();
    }

    #[pg_test]
    fn slot_deform_vs_page_encode_bench_mixed_smoke() {
        super::slot_deform_bench::slot_deform_vs_page_encode_bench_mixed_smoke();
    }

    #[pg_test]
    fn slot_deform_vs_page_encode_bench_large_page_smoke() {
        super::slot_deform_bench::slot_deform_vs_page_encode_bench_large_page_smoke();
    }

    #[pg_test]
    fn slot_deform_split_bench_smoke() {
        super::slot_deform_bench::slot_deform_split_bench_smoke();
    }

    #[pg_test]
    fn slot_scan_prepare_and_run_smoke() {
        super::slot_scan::slot_scan_prepare_and_run_smoke();
    }

    #[pg_test]
    fn slot_scan_explain_renders_postgres_plan() {
        super::slot_scan::slot_scan_explain_renders_postgres_plan();
    }

    #[pg_test]
    fn slot_scan_parallel_plan_metadata_smoke() {
        super::slot_scan::slot_scan_parallel_plan_metadata_smoke();
    }

    #[pg_test]
    fn slot_scan_local_row_cap_smoke() {
        super::slot_scan::slot_scan_local_row_cap_smoke();
    }

    #[pg_test]
    fn slot_scan_rejects_limit_node() {
        super::slot_scan::slot_scan_rejects_limit_node();
    }

    #[pg_test]
    fn slot_scan_accepts_scan_sql_external_hint_and_rejects_sql_clause() {
        super::slot_scan::slot_scan_accepts_scan_sql_external_hint_and_rejects_sql_clause();
    }

    #[pg_test]
    fn slot_scan_rejects_join_plan() {
        super::slot_scan::slot_scan_rejects_join_plan();
    }

    #[pg_test]
    fn slot_scan_rejects_subplans() {
        super::slot_scan::slot_scan_rejects_subplans();
    }

    #[pg_test]
    fn slot_scan_rejects_modifying_cte() {
        super::slot_scan::slot_scan_rejects_modifying_cte();
    }

    #[pg_test]
    fn slot_scan_prepare_catches_postgres_errors() {
        super::slot_scan::slot_scan_prepare_catches_postgres_errors();
    }

    #[pg_test]
    fn slot_scan_run_uses_saved_plan_and_aborts_on_error() {
        super::slot_scan::slot_scan_run_uses_saved_plan_and_aborts_on_error();
    }

    #[pg_test]
    fn slot_scan_run_converts_init_pg_error_and_aborts_once() {
        super::slot_scan::slot_scan_run_converts_init_pg_error_and_aborts_once();
    }

    #[pg_test]
    fn slot_scan_run_converts_consume_pg_error_and_aborts_once() {
        super::slot_scan::slot_scan_run_converts_consume_pg_error_and_aborts_once();
    }

    #[pg_test]
    fn slot_scan_run_converts_finish_pg_error_and_aborts_once() {
        super::slot_scan::slot_scan_run_converts_finish_pg_error_and_aborts_once();
    }

    #[pg_test]
    fn slot_scan_abort_pg_error_keeps_primary_error() {
        super::slot_scan::slot_scan_run_preserves_primary_error_when_abort_raises_pg_error();
    }

    #[pg_test]
    fn slot_scan_run_revalidates_across_search_path_changes() {
        super::slot_scan::slot_scan_run_revalidates_across_search_path_changes();
    }

    #[pg_test]
    fn slot_scan_run_rejects_replanned_limit_shape() {
        super::slot_scan::slot_scan_run_rejects_replanned_limit_shape();
    }

    #[pg_test]
    fn slot_scan_reuses_active_snapshot_for_read_only_cursor() {
        super::slot_scan::slot_scan_reuses_active_snapshot_for_read_only_cursor();
    }

    #[pg_test]
    fn slot_scan_planner_fetch_hint_reports_plan_kind() {
        super::slot_scan::slot_scan_planner_fetch_hint_reports_plan_kind();
    }

    #[pg_test]
    fn slot_scan_planner_fetch_hint_is_independent_from_local_cap() {
        super::slot_scan::slot_scan_planner_fetch_hint_is_independent_from_local_cap();
    }

    #[pg_test]
    fn slot_scan_append_merges_uniform_child_plan_kind() {
        super::slot_scan::slot_scan_append_merges_uniform_child_plan_kind();
    }

    #[pg_test]
    fn slot_import_roundtrips_slot_encoder_page_into_virtual_slot() {
        super::slot_import::slot_import_roundtrips_slot_encoder_page_into_virtual_slot();
    }

    #[pg_test]
    fn slot_import_releases_page_on_first_none_after_last_row() {
        super::slot_import::slot_import_releases_page_on_first_none_after_last_row();
    }

    #[pg_test]
    fn slot_import_releases_issuance_permit_after_eof() {
        super::slot_import::slot_import_releases_issuance_permit_after_eof();
    }

    #[pg_test]
    fn slot_import_uuid_is_page_backed_but_text_and_bytea_are_copied() {
        super::slot_import::slot_import_uuid_is_page_backed_but_text_and_bytea_are_copied();
    }

    #[pg_test]
    fn slot_import_rejects_schema_tupledesc_mismatch() {
        super::slot_import::slot_import_rejects_schema_tupledesc_mismatch();
    }

    #[pg_test]
    fn slot_import_name_overflow_errors() {
        super::slot_import::slot_import_name_overflow_errors();
    }

    #[pg_test]
    fn slot_import_varchar_typmod_rejects_overlength_values() {
        super::slot_import::slot_import_varchar_typmod_rejects_overlength_values();
    }

    #[pg_test]
    fn slot_import_bpchar_typmod_pads_values() {
        super::slot_import::slot_import_bpchar_typmod_pads_values();
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
