#![doc = include_str!("../README.md")]

#[cfg(feature = "pg_test")]
use pgrx::pg_schema;
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;

mod custom_scan;
mod guc;
mod logging;
mod planner;
mod result_ingress;
#[cfg(feature = "pg_test")]
mod result_ingress_tests;
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
    fn pg_fusion_host_simple_select_smoke() {
        super::smoke_tests::simple_select_smoke();
    }

    #[pg_test]
    fn pg_fusion_host_explain_smoke() {
        super::smoke_tests::explain_smoke();
    }

    #[pg_test]
    fn pg_fusion_host_heap_select_single_row_smoke() {
        super::smoke_tests::heap_select_single_row_smoke();
    }

    #[pg_test]
    fn pg_fusion_host_heap_select_filtered_row_smoke() {
        super::smoke_tests::heap_select_filtered_row_smoke();
    }

    #[pg_test]
    fn pg_fusion_host_result_ingress_roundtrip_smoke() {
        super::result_ingress_tests::result_ingress_roundtrip_smoke();
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_fusion_host'"]
    }
}
