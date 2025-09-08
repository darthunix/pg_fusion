use fasthash::Seed;
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use worker::init_datafusion_worker;

mod backend;
mod ipc;
mod planner_hook;
mod worker;

pgrx::pg_module_magic!();

pub(crate) static ENABLE_DATAFUSION: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static mut SEED: Option<u64> = None;

#[pg_guard]
#[allow(non_snake_case)]
pub unsafe extern "C-unwind" fn _PG_init() {
    init_seed();
    init_gucs();
    mark_guc_prefix_reserved("pg_fusion");
    register_shmem_request_hook();
    init_datafusion_worker();
    backend::init_datafusion_methods();
    planner_hook::init_datafusion_planner_hook();
}

fn init_seed() {
    unsafe {
        SEED = Some(Seed::gen().into());
    }
}

fn init_gucs() {
    GucRegistry::define_bool_guc(
        c"pg_fusion.enable",
        c"Enable DataFusion runtime",
        c"Enable DataFusion runtime",
        &ENABLE_DATAFUSION,
        GucContext::Userset,
        GucFlags::default(),
    );
}

fn mark_guc_prefix_reserved(guc_prefix: &str) {
    unsafe { pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr()) }
}

// Previous shmem_request_hook for chaining
static mut PREV_SHMEM_REQUEST_HOOK: pgrx::pg_sys::shmem_request_hook_type = None;

fn register_shmem_request_hook() {
    unsafe {
        // Save previous hook and install ours
        PREV_SHMEM_REQUEST_HOOK = pgrx::pg_sys::shmem_request_hook;
        pgrx::pg_sys::shmem_request_hook = Some(pg_fusion_shmem_request_hook);
    }
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn pg_fusion_shmem_request_hook() {
    // Chain to any previous hook first
    if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
        prev();
    }

    // Compute total shared memory we will allocate and request it here,
    // as required by PostgreSQL (must be within shmem_request_hook).
    let num = max_backends() as usize;

    let flags_sz = executor::layout::shared_state_layout(num)
        .expect("shared_state_layout")
        .layout
        .size();

    let conn_layout = executor::layout::connection_layout(worker::RECV_CAP, worker::SEND_CAP)
        .expect("connection_layout");
    let conns_sz = conn_layout.layout.size() * num;

    let stack_sz = executor::layout::treiber_stack_layout(num)
        .expect("treiber_stack_layout")
        .layout
        .size();

    let pid_sz = executor::layout::server_pid_layout()
        .expect("server_pid_layout")
        .layout
        .size();

    let total = flags_sz
        .saturating_add(conns_sz)
        .saturating_add(stack_sz)
        .saturating_add(pid_sz);

    pgrx::pg_sys::RequestAddinShmemSpace(total);
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
