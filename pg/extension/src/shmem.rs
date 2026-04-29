use std::ptr::NonNull;

use control_transport::{TransportRegion, TransportRegionLayout};
use issuance::{IssuanceConfig, IssuancePool};
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;
use pool::{PagePool, PagePoolConfig};
use runtime_filter::RuntimeFilterPool;
use runtime_metrics::{RuntimeMetrics, RuntimeMetricsConfig};

use crate::guc::host_config;
use crate::scan_worker_job::{ScanWorkerJobRegistry, ScanWorkerJobRegistryHandle};

const CONTROL_REGION_NAME: &str = "pg_fusion:control_transport";
const SCAN_REGION_NAME: &str = "pg_fusion:scan_transport";
const PAGE_POOL_NAME: &str = "pg_fusion:page_pool";
const ISSUANCE_POOL_NAME: &str = "pg_fusion:issuance_pool";
const RUNTIME_METRICS_NAME: &str = "pg_fusion:runtime_metrics";
const RUNTIME_FILTER_POOL_NAME: &str = "pg_fusion:runtime_filters";
const SCAN_WORKER_JOBS_NAME: &str = "pg_fusion:scan_worker_jobs";

static mut PREV_SHMEM_REQUEST_HOOK: pgrx::pg_sys::shmem_request_hook_type = None;

pub(crate) fn register_shmem_request_hook() {
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pgrx::pg_sys::shmem_request_hook;
        pgrx::pg_sys::shmem_request_hook = Some(pg_fusion_shmem_request_hook);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_fusion_shmem_request_hook() {
    if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
        prev();
    }

    let config = host_config().expect("pg_fusion host config must be valid at shmem request");
    let control_layout = TransportRegionLayout::new(
        config.control_slot_count,
        config.control_backend_to_worker_capacity,
        config.control_worker_to_backend_capacity,
    )
    .expect("primary control transport layout");
    let scan_layout = TransportRegionLayout::new(
        config.scan_slot_count,
        config.scan_backend_to_worker_capacity,
        config.scan_worker_to_backend_capacity,
    )
    .expect("scan control transport layout");
    let page_layout =
        PagePool::layout(PagePoolConfig::new(config.page_size, config.page_count).expect("cfg"))
            .expect("page pool layout");
    let issuance_layout =
        IssuancePool::layout(IssuanceConfig::new(config.page_count).expect("issuance config"))
            .expect("issuance pool layout");
    let metrics_layout = RuntimeMetrics::layout(
        RuntimeMetricsConfig::new(config.page_count).expect("runtime metrics config"),
    )
    .expect("runtime metrics layout");
    let runtime_filter_layout = RuntimeFilterPool::layout(config.runtime_filter_pool_config())
        .expect("runtime filter pool layout");
    let scan_worker_jobs_layout = ScanWorkerJobRegistry::layout();

    let total = control_layout
        .size
        .saturating_add(scan_layout.size)
        .saturating_add(page_layout.size)
        .saturating_add(issuance_layout.size)
        .saturating_add(metrics_layout.size)
        .saturating_add(runtime_filter_layout.size)
        .saturating_add(scan_worker_jobs_layout.size());
    pgrx::pg_sys::RequestAddinShmemSpace(total);
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn init_shmem() {
    let config = host_config().expect("pg_fusion host config must be valid at shmem init");

    init_control_region(
        CONTROL_REGION_NAME,
        config.control_slot_count,
        config.control_backend_to_worker_capacity,
        config.control_worker_to_backend_capacity,
    );
    init_control_region(
        SCAN_REGION_NAME,
        config.scan_slot_count,
        config.scan_backend_to_worker_capacity,
        config.scan_worker_to_backend_capacity,
    );
    init_page_pool(PAGE_POOL_NAME, config.page_size, config.page_count);
    init_issuance_pool(ISSUANCE_POOL_NAME, config.page_count);
    init_runtime_metrics(RUNTIME_METRICS_NAME, config.page_count);
    init_runtime_filters(RUNTIME_FILTER_POOL_NAME);
    init_scan_worker_jobs(SCAN_WORKER_JOBS_NAME);
}

pub(crate) fn attach_control_region() -> TransportRegion {
    attach_control_region_named(
        CONTROL_REGION_NAME,
        host_config()
            .expect("host config")
            .control_transport_layout()
            .expect("control transport layout"),
    )
}

pub(crate) fn attach_scan_region() -> TransportRegion {
    attach_control_region_named(
        SCAN_REGION_NAME,
        host_config()
            .expect("host config")
            .scan_transport_layout()
            .expect("scan transport layout"),
    )
}

pub(crate) fn attach_page_pool() -> PagePool {
    let config = host_config().expect("host config");
    let cfg = PagePoolConfig::new(config.page_size, config.page_count).expect("page pool config");
    let layout = PagePool::layout(cfg).expect("page pool layout");
    let base = lookup_shmem(PAGE_POOL_NAME, layout.size);
    unsafe { PagePool::attach(base, layout.size) }.expect("attach page pool")
}

pub(crate) fn attach_issuance_pool() -> IssuancePool {
    let config = host_config().expect("host config");
    let cfg = IssuanceConfig::new(config.page_count).expect("issuance config");
    let layout = IssuancePool::layout(cfg).expect("issuance layout");
    let base = lookup_shmem(ISSUANCE_POOL_NAME, layout.size);
    unsafe { IssuancePool::attach(base, layout.size) }.expect("attach issuance pool")
}

pub(crate) fn attach_runtime_metrics() -> RuntimeMetrics {
    let config = host_config().expect("host config");
    let cfg = RuntimeMetricsConfig::new(config.page_count).expect("runtime metrics config");
    let layout = RuntimeMetrics::layout(cfg).expect("runtime metrics layout");
    let base = lookup_shmem(RUNTIME_METRICS_NAME, layout.size);
    unsafe { RuntimeMetrics::attach(base, layout.size) }.expect("attach runtime metrics")
}

pub(crate) fn attach_runtime_filters() -> RuntimeFilterPool {
    let config = host_config().expect("host config");
    let cfg = config.runtime_filter_pool_config();
    let layout = RuntimeFilterPool::layout(cfg).expect("runtime filter pool layout");
    let base = lookup_shmem(RUNTIME_FILTER_POOL_NAME, layout.size);
    unsafe { RuntimeFilterPool::attach(base.as_ptr(), layout.size, cfg) }
        .expect("attach runtime filter pool")
}

pub(crate) fn attach_scan_worker_jobs() -> ScanWorkerJobRegistryHandle {
    let layout = ScanWorkerJobRegistry::layout();
    let base = lookup_shmem(SCAN_WORKER_JOBS_NAME, layout.size());
    unsafe { ScanWorkerJobRegistryHandle::attach(base) }
}

fn init_control_region(
    name: &str,
    slot_count: u32,
    backend_to_worker_capacity: usize,
    worker_to_backend_capacity: usize,
) {
    let layout = TransportRegionLayout::new(
        slot_count,
        backend_to_worker_capacity,
        worker_to_backend_capacity,
    )
    .expect("control transport layout");
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), layout.size, &mut found) as *mut u8
    };
    let base = NonNull::new(base).expect("control transport shmem");
    let region = unsafe {
        if found {
            TransportRegion::attach(base, layout.size).map_err(|err| err.to_string())
        } else {
            TransportRegion::init_in_place(base, layout.size, layout).map_err(|err| err.to_string())
        }
    };
    region.expect("control transport region");
}

fn init_page_pool(name: &str, page_size: usize, page_count: u32) {
    let cfg = PagePoolConfig::new(page_size, page_count).expect("page pool config");
    let layout = PagePool::layout(cfg).expect("page pool layout");
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), layout.size, &mut found) as *mut u8
    };
    let base = NonNull::new(base).expect("page pool shmem");
    let pool = unsafe {
        if found {
            PagePool::attach(base, layout.size).map_err(|err| err.to_string())
        } else {
            PagePool::init_in_place(base, layout.size, cfg).map_err(|err| err.to_string())
        }
    };
    pool.expect("page pool");
}

fn init_issuance_pool(name: &str, permit_count: u32) {
    let cfg = IssuanceConfig::new(permit_count).expect("issuance config");
    let layout = IssuancePool::layout(cfg).expect("issuance layout");
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), layout.size, &mut found) as *mut u8
    };
    let base = NonNull::new(base).expect("issuance pool shmem");
    let pool = unsafe {
        if found {
            IssuancePool::attach(base, layout.size).map_err(|err| err.to_string())
        } else {
            IssuancePool::init_in_place(base, layout.size, cfg).map_err(|err| err.to_string())
        }
    };
    pool.expect("issuance pool");
}

fn init_runtime_metrics(name: &str, page_count: u32) {
    let cfg = RuntimeMetricsConfig::new(page_count).expect("runtime metrics config");
    let layout = RuntimeMetrics::layout(cfg).expect("runtime metrics layout");
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), layout.size, &mut found) as *mut u8
    };
    let base = NonNull::new(base).expect("runtime metrics shmem");
    let metrics = unsafe {
        if found {
            RuntimeMetrics::attach(base, layout.size).map_err(|err| err.to_string())
        } else {
            RuntimeMetrics::init_in_place(base, layout.size, cfg).map_err(|err| err.to_string())
        }
    };
    metrics.expect("runtime metrics");
}

fn init_runtime_filters(name: &str) {
    let config = host_config().expect("host config");
    let cfg = config.runtime_filter_pool_config();
    let layout = RuntimeFilterPool::layout(cfg).expect("runtime filter pool layout");
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), layout.size, &mut found) as *mut u8
    };
    let base = NonNull::new(base).expect("runtime filter pool shmem");
    let pool = unsafe {
        if found {
            RuntimeFilterPool::attach(base.as_ptr(), layout.size, cfg)
                .map_err(|err| err.to_string())
        } else {
            RuntimeFilterPool::init_in_place(base.as_ptr(), layout.size, cfg)
                .map_err(|err| err.to_string())
        }
    };
    pool.expect("runtime filter pool");
}

fn init_scan_worker_jobs(name: &str) {
    let layout = ScanWorkerJobRegistry::layout();
    let mut found = false;
    let base = unsafe {
        pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), layout.size(), &mut found) as *mut u8
    };
    let base = NonNull::new(base).expect("scan worker jobs shmem");
    unsafe {
        ScanWorkerJobRegistryHandle::init_or_attach(base, found);
    }
}

fn attach_control_region_named(name: &str, layout: TransportRegionLayout) -> TransportRegion {
    let base = lookup_shmem(name, layout.size);
    unsafe { TransportRegion::attach(base, layout.size) }.expect("attach control region")
}

fn lookup_shmem(name: &str, size: usize) -> NonNull<u8> {
    let mut found = false;
    let base =
        unsafe { pgrx::pg_sys::ShmemInitStruct(name.as_pg_cstr(), size, &mut found) as *mut u8 };
    assert!(
        found,
        "shared memory object {name} must already be initialized"
    );
    NonNull::new(base).expect("shared memory base must be non-null")
}
