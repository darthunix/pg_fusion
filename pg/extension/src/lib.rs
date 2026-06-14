#![doc = include_str!("../README.md")]

#[cfg(feature = "pg_test")]
use pgrx::pg_schema;
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;

mod custom_scan;
mod diag;
mod guc;
mod logging;
mod metrics;
#[cfg(feature = "pg_test")]
mod pg_compat;
mod plan_payload;
mod planner;
mod result_ingress;
#[cfg(feature = "pg_test")]
mod result_ingress_tests;
mod scan_worker_job;
mod shmem;
#[cfg(feature = "pg_test")]
mod smoke_tests;
mod utility_hook;
mod worker;

pub use guc::{host_config, HostConfig, HostConfigError};

pgrx::pg_module_magic!();

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    mark_guc_prefix_reserved("pg_fusion");
    shmem::register_shmem_request_hook();
    custom_scan::register_methods();
    planner::register_hooks();
    utility_hook::register_hook();
    worker::register_background_worker();
}

fn mark_guc_prefix_reserved(guc_prefix: &str) {
    unsafe { pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr()) }
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn pg_fusion_simple_select_smoke() {
        super::smoke_tests::simple_select_smoke();
    }

    #[pg_test]
    fn pg_fusion_numeric_special_value_error_smoke() {
        super::smoke_tests::numeric_special_value_error_smoke();
    }

    #[pg_test]
    fn pg_fusion_float_avg_special_value_smoke() {
        super::smoke_tests::float_avg_special_value_smoke();
    }

    #[pg_test]
    fn pg_fusion_explain_smoke() {
        super::smoke_tests::explain_smoke();
    }

    #[pg_test]
    fn pg_fusion_planner_catalog_strict_smoke() {
        super::smoke_tests::planner_catalog_strict_smoke();
    }

    #[pg_test]
    fn pg_fusion_planner_bound_params_strict_smoke() {
        super::smoke_tests::planner_bound_params_strict_smoke();
    }

    #[pg_test]
    fn pg_fusion_scalar_subquery_cardinality_smoke() {
        super::smoke_tests::scalar_subquery_cardinality_smoke();
    }

    #[pg_test]
    fn pg_fusion_checked_integer_arithmetic_smoke() {
        super::smoke_tests::checked_integer_arithmetic_smoke();
    }

    #[pg_test]
    fn pg_fusion_fetch_with_ties_rejected_smoke() {
        super::smoke_tests::fetch_with_ties_rejected_smoke();
    }

    #[pg_test]
    fn pg_fusion_strict_frontend_smoke() {
        super::smoke_tests::strict_frontend_smoke();
    }

    #[pg_test]
    fn pg_fusion_copy_select_smoke() {
        super::smoke_tests::copy_select_smoke();
    }

    #[pg_test]
    fn pg_fusion_copy_catalog_strict_smoke() {
        super::smoke_tests::copy_catalog_strict_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_select_single_row_smoke() {
        super::smoke_tests::heap_select_single_row_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_select_filtered_row_smoke() {
        super::smoke_tests::heap_select_filtered_row_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_date_literal_filter_smoke() {
        super::smoke_tests::heap_date_literal_filter_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_date_infinity_rejected_smoke() {
        super::smoke_tests::heap_date_infinity_rejected_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_numeric_scan_smoke() {
        super::smoke_tests::heap_numeric_scan_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_numeric_scan_error_smoke() {
        super::smoke_tests::heap_numeric_scan_error_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_avg_full_scan_smoke() {
        super::smoke_tests::heap_avg_full_scan_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_avg_window_sliding_smoke() {
        super::smoke_tests::heap_avg_window_sliding_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_interval_avg_smoke() {
        super::smoke_tests::heap_interval_avg_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_varlena_full_scan_smoke() {
        super::smoke_tests::heap_varlena_full_scan_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_join_two_tables_smoke() {
        super::smoke_tests::heap_join_two_tables_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_multi_use_cte_smoke() {
        super::smoke_tests::heap_multi_use_cte_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_parallel_scan_smoke() {
        super::smoke_tests::heap_parallel_scan_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_parallel_scan_search_path_smoke() {
        super::smoke_tests::heap_parallel_scan_search_path_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_parallel_worker_budget_smoke() {
        super::smoke_tests::heap_parallel_worker_budget_smoke();
    }

    #[pg_test]
    fn pg_fusion_heap_leader_only_scan_smoke() {
        super::smoke_tests::heap_leader_only_scan_smoke();
    }

    #[pg_test]
    fn pg_fusion_statement_timeout_cleans_up_active_execution_smoke() {
        super::smoke_tests::statement_timeout_cleans_up_active_execution_smoke();
    }

    #[pg_test]
    fn pg_fusion_parallel_statement_timeout_keeps_worker_generation_online_smoke() {
        super::smoke_tests::parallel_statement_timeout_keeps_worker_generation_online_smoke();
    }

    #[pg_test]
    fn pg_fusion_result_ingress_roundtrip_smoke() {
        super::result_ingress_tests::result_ingress_roundtrip_smoke();
    }

    #[pg_test]
    fn pg_fusion_metrics_smoke() {
        super::smoke_tests::metrics_smoke();
    }

    #[pg_test]
    fn pg_fusion_runtime_filter_uuid_smoke() {
        super::smoke_tests::runtime_filter_uuid_smoke();
    }

    #[pg_test]
    fn pg_fusion_runtime_filter_bytea_smoke() {
        super::smoke_tests::runtime_filter_bytea_smoke();
    }

    #[pg_test]
    fn pg_fusion_runtime_filter_temporal_smoke() {
        super::smoke_tests::runtime_filter_temporal_smoke();
    }

    #[pg_test]
    fn pg_fusion_runtime_filter_numeric_smoke() {
        super::smoke_tests::runtime_filter_numeric_smoke();
    }

    #[pg_test]
    fn pg_fusion_spill_metrics_smoke() {
        super::smoke_tests::spill_metrics_smoke();
    }

    #[pg_test]
    fn pg_fusion_pg_compat_allowlist() {
        super::pg_compat::pg_compat_allowlist();
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        let mut options = vec!["shared_preload_libraries = 'pg_fusion'"];
        if std::env::var("PG_FUSION_SPILL_PG_TEST").is_ok_and(|value| value == "1") {
            options.push("pg_fusion.worker_memory_limit_mb = 128");
        }
        options
    }
}
