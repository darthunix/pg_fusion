use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use planner_hook::init_datafusion_planner_hook;
use worker::init_datafusion_worker;

mod ipc;
mod planner_hook;
mod worker;

pgrx::pg_module_magic!();

pub(crate) static ENABLE_DATAFUSION: GucSetting<bool> = GucSetting::<bool>::new(false);

#[pg_guard]
#[allow(non_snake_case)]
pub extern "C" fn _PG_init() {
    init_gucs();
    mark_guc_prefix_reserved("pg_fusion");
    init_datafusion_worker();
    init_datafusion_planner_hook();
}

fn init_gucs() {
    GucRegistry::define_bool_guc(
        "pg_fusion.enable",
        "Enable DataFusion runtime",
        "Enable DataFusion runtime",
        &ENABLE_DATAFUSION,
        GucContext::Userset,
        GucFlags::default(),
    );
}

fn mark_guc_prefix_reserved(guc_prefix: &str) {
    unsafe { pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr()) }
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
