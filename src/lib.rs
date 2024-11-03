use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::pg_sys::AsPgCStr;
use planner_hook::init_datafusion_planner_hook;

mod planner_hook;

pgrx::pg_module_magic!();

pub(crate) static ENABLE_DATAFUSION: GucSetting<bool> = GucSetting::<bool>::new(false);

#[pg_guard]
#[allow(non_snake_case)]
pub extern "C" fn _PG_init() {
    GucRegistry::define_bool_guc(
        "pg_fusion.enable",
        "Enable DataFusion runtime",
        "Enable DataFusion runtime",
        &ENABLE_DATAFUSION,
        GucContext::Userset,
        GucFlags::default(),
    );
    mark_guc_prefix_reserved("pg_fusion");
    init_datafusion_planner_hook();
}

#[allow(unused_variables)]
fn mark_guc_prefix_reserved(guc_prefix: &str) {
    unsafe {
        pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr())
    }
}
