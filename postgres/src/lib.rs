use fasthash::Seed;
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use worker::init_datafusion_worker;

mod backend;
mod ipc;
mod planner_hook;
mod utility_hook;
mod worker;

pgrx::pg_module_magic!();

pub(crate) static ENABLE_DATAFUSION: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static TOKIO_THREADS: GucSetting<i32> = GucSetting::<i32>::new(0);
pub(crate) static HEAP_QUANTUM: GucSetting<i32> = GucSetting::<i32>::new(1);
pub(crate) static HEAP_MAX_INFLIGHT_PER_SCAN: GucSetting<i32> = GucSetting::<i32>::new(4);
pub(crate) static HEAP_DISPATCH_BATCH: GucSetting<i32> = GucSetting::<i32>::new(8);
pub(crate) static HEAP_REQUEST_BATCH: GucSetting<i32> = GucSetting::<i32>::new(8);
pub(crate) static EXECUTOR_LOG_PATH: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"/tmp/pg_fusion_worker.log"));
pub(crate) static EXECUTOR_LOG_FILTER: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"info"));
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
    utility_hook::init_datafusion_utility_hook();
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

    GucRegistry::define_int_guc(
        c"pg_fusion.tokio_threads",
        c"Executor Tokio worker threads",
        c"Tokio worker thread count for the DataFusion background worker (0 = auto)",
        &TOKIO_THREADS,
        0,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_fusion.heap_quantum",
        c"Per-scan budget quantum",
        c"Weighted-fair scheduler quantum for heap scan credit dispatch",
        &HEAP_QUANTUM,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_fusion.heap_max_inflight_per_scan",
        c"Max in-flight heap requests per scan",
        c"Upper bound on concurrently leased heap page credits per logical heap scan",
        &HEAP_MAX_INFLIGHT_PER_SCAN,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_fusion.heap_dispatch_batch",
        c"Heap request dispatch batch size",
        c"Maximum number of heap block requests the worker dispatches in one scheduling cycle",
        &HEAP_DISPATCH_BATCH,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_fusion.heap_request_batch",
        c"Heap request service batch size",
        c"Maximum number of pending heap block requests a backend serves per poll iteration",
        &HEAP_REQUEST_BATCH,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_fusion.executor_log_path",
        c"Executor log file path",
        c"Absolute path to background worker log file",
        &EXECUTOR_LOG_PATH,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_fusion.executor_log_filter",
        c"Executor tracing filter",
        c"Tracing filter expression used by background worker subscriber",
        &EXECUTOR_LOG_FILTER,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

#[inline]
pub(crate) fn tokio_threads() -> Option<usize> {
    let v = TOKIO_THREADS.get();
    if v <= 0 {
        None
    } else {
        Some(v as usize)
    }
}

#[inline]
pub(crate) fn heap_quantum() -> usize {
    HEAP_QUANTUM.get().max(1) as usize
}

#[inline]
pub(crate) fn heap_max_inflight_per_scan() -> usize {
    HEAP_MAX_INFLIGHT_PER_SCAN.get().max(1) as usize
}

#[inline]
pub(crate) fn heap_dispatch_batch() -> usize {
    HEAP_DISPATCH_BATCH.get().max(1) as usize
}

#[inline]
pub(crate) fn heap_request_batch() -> usize {
    HEAP_REQUEST_BATCH.get().max(1) as usize
}

#[inline]
pub(crate) fn executor_log_path() -> String {
    EXECUTOR_LOG_PATH
        .get()
        .and_then(|v| v.into_string().ok())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "/tmp/pg_fusion_worker.log".to_string())
}

#[inline]
pub(crate) fn executor_log_filter() -> String {
    EXECUTOR_LOG_FILTER
        .get()
        .and_then(|v| v.into_string().ok())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "info".to_string())
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

    // Slot blocks (per-connection heap page buffers)
    let blksz = pgrx::pg_sys::BLCKSZ as usize;
    let slot_blocks_layout = executor::layout::slot_blocks_layout(
        worker::SLOTS_PER_CONN,
        blksz,
        worker::BLOCKS_PER_SLOT,
    )
    .expect("slot_blocks_layout");
    let slot_blocks_sz = slot_blocks_layout.layout.size() * num;

    // Per-connection result ring buffers
    let result_ring_layout =
        executor::layout::result_ring_layout(worker::RESULT_RING_CAP).expect("result_ring_layout");
    let result_ring_sz = result_ring_layout.layout.size() * num;

    let telemetry_sz = executor::telemetry::telemetry_layout(num)
        .expect("telemetry_layout")
        .layout
        .size();

    let total = flags_sz
        .saturating_add(conns_sz)
        .saturating_add(stack_sz)
        .saturating_add(pid_sz)
        .saturating_add(slot_blocks_sz)
        .saturating_add(result_ring_sz)
        .saturating_add(telemetry_sz);

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

// tests live in storage/pg_test per project policy
