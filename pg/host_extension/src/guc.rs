use backend_service::BackendServiceConfig;
use control_transport::TransportRegionLayout;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use runtime_protocol::{
    MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
};
use thiserror::Error;
use worker_runtime::WorkerRuntimeConfig;

pub(crate) static ENABLE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static WORKER_THREADS: GucSetting<i32> = GucSetting::<i32>::new(0);
pub(crate) static WORKER_LOG_PATH: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"/tmp/pg_fusion_worker.log"));
pub(crate) static WORKER_LOG_FILTER: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"info"));

pub(crate) static CONTROL_SLOT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(64);
pub(crate) static CONTROL_BACKEND_TO_WORKER_CAPACITY: GucSetting<i32> =
    GucSetting::<i32>::new(8192);
pub(crate) static CONTROL_WORKER_TO_BACKEND_CAPACITY: GucSetting<i32> =
    GucSetting::<i32>::new(8192);

pub(crate) static SCAN_SLOT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(64);
pub(crate) static SCAN_BACKEND_TO_WORKER_CAPACITY: GucSetting<i32> =
    GucSetting::<i32>::new(MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY as i32);
pub(crate) static SCAN_WORKER_TO_BACKEND_CAPACITY: GucSetting<i32> =
    GucSetting::<i32>::new(MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY as i32);

pub(crate) static PAGE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(64 * 1024);
pub(crate) static PAGE_COUNT: GucSetting<i32> = GucSetting::<i32>::new(256);
pub(crate) static PERMIT_COUNT: GucSetting<i32> = GucSetting::<i32>::new(256);

pub(crate) static SCAN_PAYLOAD_BLOCK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(4096);
pub(crate) static SCAN_FETCH_BATCH_ROWS: GucSetting<i32> = GucSetting::<i32>::new(1024);
pub(crate) static ESTIMATOR_INITIAL_TAIL_BYTES_PER_ROW: GucSetting<i32> =
    GucSetting::<i32>::new(64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostConfig {
    pub enable: bool,
    pub worker_threads: Option<usize>,
    pub worker_log_path: String,
    pub worker_log_filter: String,
    pub control_slot_count: u32,
    pub control_backend_to_worker_capacity: usize,
    pub control_worker_to_backend_capacity: usize,
    pub scan_slot_count: u32,
    pub scan_backend_to_worker_capacity: usize,
    pub scan_worker_to_backend_capacity: usize,
    pub page_size: usize,
    pub page_count: u32,
    pub permit_count: u32,
    pub scan_payload_block_size: u32,
    pub scan_fetch_batch_rows: u32,
    pub estimator_initial_tail_bytes_per_row: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HostConfigError {
    #[error("GUC {name} must be positive, got {actual}")]
    NonPositive { name: &'static str, actual: i32 },
    #[error("scan backend-to-worker capacity must be at least {required}, got {actual}")]
    ScanInboundCapacityTooSmall { required: usize, actual: usize },
    #[error("scan worker-to-backend capacity must be at least {required}, got {actual}")]
    ScanOutboundCapacityTooSmall { required: usize, actual: usize },
}

pub fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pg_fusion.enable",
        c"Enable pg_fusion",
        c"Enable the new pg_fusion runtime path",
        &ENABLE,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_fusion.worker_threads",
        c"Worker thread count",
        c"Background worker thread count for pg_fusion (0 = auto)",
        &WORKER_THREADS,
        0,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_fusion.worker_log_path",
        c"Worker log file path",
        c"Absolute path to the pg_fusion background worker log file",
        &WORKER_LOG_PATH,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_fusion.worker_log_filter",
        c"Worker tracing filter",
        c"Tracing filter expression used by the pg_fusion background worker",
        &WORKER_LOG_FILTER,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    define_positive_int(
        c"pg_fusion.control_slot_count",
        c"Primary control slot count",
        c"Number of primary execution/control transport slots",
        &CONTROL_SLOT_COUNT,
    );
    define_positive_int(
        c"pg_fusion.control_backend_to_worker_capacity",
        c"Primary inbound control ring capacity",
        c"Per-slot capacity for backend-to-worker primary control rings",
        &CONTROL_BACKEND_TO_WORKER_CAPACITY,
    );
    define_positive_int(
        c"pg_fusion.control_worker_to_backend_capacity",
        c"Primary outbound control ring capacity",
        c"Per-slot capacity for worker-to-backend primary control rings",
        &CONTROL_WORKER_TO_BACKEND_CAPACITY,
    );
    define_positive_int(
        c"pg_fusion.scan_slot_count",
        c"Dedicated scan slot count",
        c"Number of dedicated scan control transport slots",
        &SCAN_SLOT_COUNT,
    );
    define_positive_int(
        c"pg_fusion.scan_backend_to_worker_capacity",
        c"Dedicated scan inbound control ring capacity",
        c"Per-slot capacity for backend-to-worker dedicated scan control rings",
        &SCAN_BACKEND_TO_WORKER_CAPACITY,
    );
    define_positive_int(
        c"pg_fusion.scan_worker_to_backend_capacity",
        c"Dedicated scan outbound control ring capacity",
        c"Per-slot capacity for worker-to-backend dedicated scan control rings",
        &SCAN_WORKER_TO_BACKEND_CAPACITY,
    );
    define_positive_int(
        c"pg_fusion.page_size",
        c"Shared Arrow page size",
        c"Byte size of one shared page in the unified data pool",
        &PAGE_SIZE,
    );
    define_positive_int(
        c"pg_fusion.page_count",
        c"Shared Arrow page count",
        c"Number of pages in the unified shared data pool",
        &PAGE_COUNT,
    );
    define_positive_int(
        c"pg_fusion.permit_count",
        c"Unified issuance permit count",
        c"Number of outstanding shared-page transfers allowed at once",
        &PERMIT_COUNT,
    );
    define_positive_int(
        c"pg_fusion.scan_payload_block_size",
        c"Backend scan payload block size",
        c"Payload block size used by backend scan page encoding",
        &SCAN_PAYLOAD_BLOCK_SIZE,
    );
    define_positive_int(
        c"pg_fusion.scan_fetch_batch_rows",
        c"Backend scan fetch batch rows",
        c"Number of rows fetched per PostgreSQL cursor batch in backend scan streaming",
        &SCAN_FETCH_BATCH_ROWS,
    );
    define_positive_int(
        c"pg_fusion.estimator_initial_tail_bytes_per_row",
        c"Initial variable-width tail bytes per row",
        c"Initial estimator prior for variable-width Arrow page tails",
        &ESTIMATOR_INITIAL_TAIL_BYTES_PER_ROW,
    );
}

pub fn host_config() -> Result<HostConfig, HostConfigError> {
    let scan_backend_to_worker_capacity = positive_usize(
        "pg_fusion.scan_backend_to_worker_capacity",
        SCAN_BACKEND_TO_WORKER_CAPACITY.get(),
    )?;
    if scan_backend_to_worker_capacity < MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY {
        return Err(HostConfigError::ScanInboundCapacityTooSmall {
            required: MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
            actual: scan_backend_to_worker_capacity,
        });
    }

    let scan_worker_to_backend_capacity = positive_usize(
        "pg_fusion.scan_worker_to_backend_capacity",
        SCAN_WORKER_TO_BACKEND_CAPACITY.get(),
    )?;
    if scan_worker_to_backend_capacity < MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY {
        return Err(HostConfigError::ScanOutboundCapacityTooSmall {
            required: MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
            actual: scan_worker_to_backend_capacity,
        });
    }

    Ok(HostConfig {
        enable: ENABLE.get(),
        worker_threads: normalize_worker_threads(WORKER_THREADS.get()),
        worker_log_path: string_setting(&WORKER_LOG_PATH, "/tmp/pg_fusion_worker.log"),
        worker_log_filter: string_setting(&WORKER_LOG_FILTER, "info"),
        control_slot_count: positive_u32("pg_fusion.control_slot_count", CONTROL_SLOT_COUNT.get())?,
        control_backend_to_worker_capacity: positive_usize(
            "pg_fusion.control_backend_to_worker_capacity",
            CONTROL_BACKEND_TO_WORKER_CAPACITY.get(),
        )?,
        control_worker_to_backend_capacity: positive_usize(
            "pg_fusion.control_worker_to_backend_capacity",
            CONTROL_WORKER_TO_BACKEND_CAPACITY.get(),
        )?,
        scan_slot_count: positive_u32("pg_fusion.scan_slot_count", SCAN_SLOT_COUNT.get())?,
        scan_backend_to_worker_capacity,
        scan_worker_to_backend_capacity,
        page_size: positive_usize("pg_fusion.page_size", PAGE_SIZE.get())?,
        page_count: positive_u32("pg_fusion.page_count", PAGE_COUNT.get())?,
        permit_count: positive_u32("pg_fusion.permit_count", PERMIT_COUNT.get())?,
        scan_payload_block_size: positive_u32(
            "pg_fusion.scan_payload_block_size",
            SCAN_PAYLOAD_BLOCK_SIZE.get(),
        )?,
        scan_fetch_batch_rows: positive_u32(
            "pg_fusion.scan_fetch_batch_rows",
            SCAN_FETCH_BATCH_ROWS.get(),
        )?,
        estimator_initial_tail_bytes_per_row: positive_u32(
            "pg_fusion.estimator_initial_tail_bytes_per_row",
            ESTIMATOR_INITIAL_TAIL_BYTES_PER_ROW.get(),
        )?,
    })
}

impl HostConfig {
    pub fn control_transport_layout(&self) -> Result<TransportRegionLayout, control_transport::ConfigError> {
        TransportRegionLayout::new(
            self.control_slot_count,
            self.control_backend_to_worker_capacity,
            self.control_worker_to_backend_capacity,
        )
    }

    pub fn scan_transport_layout(&self) -> Result<TransportRegionLayout, control_transport::ConfigError> {
        TransportRegionLayout::new(
            self.scan_slot_count,
            self.scan_backend_to_worker_capacity,
            self.scan_worker_to_backend_capacity,
        )
    }

    pub fn backend_service_config(&self) -> BackendServiceConfig {
        let mut config = BackendServiceConfig::default();
        config.scan_payload_block_size = self.scan_payload_block_size;
        config.scan_fetch_batch_rows = self.scan_fetch_batch_rows;
        config.estimator_default.initial_tail_bytes_per_row =
            self.estimator_initial_tail_bytes_per_row;
        config
    }

    pub fn worker_runtime_config(&self) -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            control_frame_capacity: self.control_backend_to_worker_capacity,
            ..WorkerRuntimeConfig::default()
        }
    }
}

fn define_positive_int(
    name: &'static std::ffi::CStr,
    short_desc: &'static std::ffi::CStr,
    long_desc: &'static std::ffi::CStr,
    setting: &'static GucSetting<i32>,
) {
    GucRegistry::define_int_guc(
        name,
        short_desc,
        long_desc,
        setting,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

fn normalize_worker_threads(value: i32) -> Option<usize> {
    if value <= 0 {
        None
    } else {
        Some(value as usize)
    }
}

fn string_setting(setting: &GucSetting<Option<std::ffi::CString>>, default: &str) -> String {
    setting
        .get()
        .and_then(|v| v.into_string().ok())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn positive_u32(name: &'static str, actual: i32) -> Result<u32, HostConfigError> {
    if actual <= 0 {
        return Err(HostConfigError::NonPositive { name, actual });
    }
    Ok(actual as u32)
}

fn positive_usize(name: &'static str, actual: i32) -> Result<usize, HostConfigError> {
    if actual <= 0 {
        return Err(HostConfigError::NonPositive { name, actual });
    }
    Ok(actual as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_service_config_uses_new_guc_surface() {
        let config = HostConfig {
            enable: true,
            worker_threads: Some(4),
            worker_log_path: "/tmp/pg_fusion_worker.log".into(),
            worker_log_filter: "info".into(),
            control_slot_count: 8,
            control_backend_to_worker_capacity: 4096,
            control_worker_to_backend_capacity: 4096,
            scan_slot_count: 8,
            scan_backend_to_worker_capacity: MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
            scan_worker_to_backend_capacity: MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
            page_size: 65536,
            page_count: 256,
            permit_count: 256,
            scan_payload_block_size: 2048,
            scan_fetch_batch_rows: 77,
            estimator_initial_tail_bytes_per_row: 33,
        };

        let backend = config.backend_service_config();
        let worker = config.worker_runtime_config();

        assert_eq!(backend.scan_payload_block_size, 2048);
        assert_eq!(backend.scan_fetch_batch_rows, 77);
        assert_eq!(backend.estimator_default.initial_tail_bytes_per_row, 33);
        assert_eq!(worker.control_frame_capacity, 4096);
    }
}
