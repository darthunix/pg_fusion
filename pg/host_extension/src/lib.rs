#![doc = include_str!("../README.md")]

use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;

mod custom_scan;
mod guc;
mod planner;
mod result_ingress;
mod shmem;
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
