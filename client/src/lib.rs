use fasthash::Seed;
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use worker::init_datafusion_worker;

mod worker;

pgrx::pg_module_magic!();

pub(crate) static ENABLE_DATAFUSION: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static mut SEED: Option<u64> = None;

#[pg_guard]
#[allow(non_snake_case)]
pub extern "C" fn _PG_init() {
    init_seed();
    init_gucs();
    mark_guc_prefix_reserved("pg_fusion");
    init_datafusion_worker();
}

fn init_seed() {
    unsafe {
        SEED = Some(Seed::gen().into());
    }
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

/// The change of MaxBackends value requires cluster restart.
/// So, it is safe to use it as a constant on startup.
#[inline]
pub(crate) fn max_backends() -> u32 {
    #[cfg(not(any(test, feature = "pg_test")))]
    unsafe {
        pgrx::pg_sys::MaxBackends as u32
    }
    #[cfg(any(test, feature = "pg_test"))]
    10
}
