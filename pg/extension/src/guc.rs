use backend_service::{BackendServiceConfig, DiagnosticLogLevel, DiagnosticsConfig};
use control_transport::TransportRegionLayout;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use runtime_protocol::{
    MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY, MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
};
use thiserror::Error;
use worker_runtime::WorkerRuntimeConfig;

pub(crate) static ENABLE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static WORKER_THREADS: GucSetting<i32> = GucSetting::<i32>::new(0);
pub(crate) static LOG_PATH: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"/tmp/pg_fusion.log"));
pub(crate) static WORKER_LOG_FILTER: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"warn"));
pub(crate) static BACKEND_LOG_LEVEL: GucSetting<i32> = GucSetting::<i32>::new(0);

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

pub(crate) static SCAN_FETCH_BATCH_ROWS: GucSetting<i32> = GucSetting::<i32>::new(1024);
pub(crate) static ESTIMATOR_INITIAL_TAIL_BYTES_PER_ROW: GucSetting<i32> =
    GucSetting::<i32>::new(64);
pub(crate) static SCAN_TIMING_DETAIL: GucSetting<bool> = GucSetting::<bool>::new(false);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostConfig {
    pub enable: bool,
    pub worker_threads: Option<usize>,
    pub log_path: String,
    pub worker_log_filter: String,
    pub backend_log_level: DiagnosticLogLevel,
    pub control_slot_count: u32,
    pub control_backend_to_worker_capacity: usize,
    pub control_worker_to_backend_capacity: usize,
    pub scan_slot_count: u32,
    pub scan_backend_to_worker_capacity: usize,
    pub scan_worker_to_backend_capacity: usize,
    pub page_size: usize,
    pub page_count: u32,
    pub scan_fetch_batch_rows: u32,
    pub estimator_initial_tail_bytes_per_row: u32,
    pub scan_timing_detail: bool,
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
        c"pg_fusion.log_path",
        c"Extension log file path",
        c"Absolute path to the shared pg_fusion extension log file",
        &LOG_PATH,
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

    GucRegistry::define_int_guc(
        c"pg_fusion.backend_log_level",
        c"Backend diagnostic log level",
        c"Diagnostic verbosity for backend-side pg_fusion code: 0=off, 1=basic, 2=trace",
        &BACKEND_LOG_LEVEL,
        0,
        2,
        GucContext::Userset,
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
        c"pg_fusion.scan_fetch_batch_rows",
        c"Backend scan fetch batch rows",
        c"Number of rows fetched per PostgreSQL portal drain in backend scan streaming",
        &SCAN_FETCH_BATCH_ROWS,
    );
    define_positive_int(
        c"pg_fusion.estimator_initial_tail_bytes_per_row",
        c"Initial variable-width tail bytes per row",
        c"Initial estimator prior for variable-width Arrow page tails",
        &ESTIMATOR_INITIAL_TAIL_BYTES_PER_ROW,
    );
    GucRegistry::define_bool_guc(
        c"pg_fusion.scan_timing_detail",
        c"Enable detailed scan timing",
        c"Measure per-row scan callback time to split PostgreSQL read time from Arrow serialization time",
        &SCAN_TIMING_DETAIL,
        GucContext::Userset,
        GucFlags::default(),
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
        log_path: extension_log_path(),
        worker_log_filter: string_setting(&WORKER_LOG_FILTER, "warn"),
        backend_log_level: backend_log_level(),
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
        scan_fetch_batch_rows: positive_u32(
            "pg_fusion.scan_fetch_batch_rows",
            SCAN_FETCH_BATCH_ROWS.get(),
        )?,
        estimator_initial_tail_bytes_per_row: positive_u32(
            "pg_fusion.estimator_initial_tail_bytes_per_row",
            ESTIMATOR_INITIAL_TAIL_BYTES_PER_ROW.get(),
        )?,
        scan_timing_detail: SCAN_TIMING_DETAIL.get(),
    })
}

impl HostConfig {
    pub fn control_transport_layout(
        &self,
    ) -> Result<TransportRegionLayout, control_transport::ConfigError> {
        TransportRegionLayout::new(
            self.control_slot_count,
            self.control_backend_to_worker_capacity,
            self.control_worker_to_backend_capacity,
        )
    }

    pub fn scan_transport_layout(
        &self,
    ) -> Result<TransportRegionLayout, control_transport::ConfigError> {
        TransportRegionLayout::new(
            self.scan_slot_count,
            self.scan_backend_to_worker_capacity,
            self.scan_worker_to_backend_capacity,
        )
    }

    pub fn backend_service_config(&self) -> BackendServiceConfig {
        let mut config = BackendServiceConfig::default();
        config.scan_fetch_batch_rows = self.scan_fetch_batch_rows;
        config.estimator_default.initial_tail_bytes_per_row =
            self.estimator_initial_tail_bytes_per_row;
        config.diagnostics = DiagnosticsConfig::new(self.backend_log_level, self.log_path.clone());
        config.scan_timing_detail = self.scan_timing_detail;
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

pub(crate) fn extension_log_path() -> String {
    string_setting(&LOG_PATH, "/tmp/pg_fusion.log")
}

pub(crate) fn backend_log_level() -> DiagnosticLogLevel {
    DiagnosticLogLevel::from_i32(BACKEND_LOG_LEVEL.get())
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
            log_path: "/tmp/pg_fusion.log".into(),
            worker_log_filter: "warn".into(),
            backend_log_level: DiagnosticLogLevel::Trace,
            control_slot_count: 8,
            control_backend_to_worker_capacity: 4096,
            control_worker_to_backend_capacity: 4096,
            scan_slot_count: 8,
            scan_backend_to_worker_capacity: MIN_SCAN_BACKEND_TO_WORKER_RING_CAPACITY,
            scan_worker_to_backend_capacity: MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY,
            page_size: 65536,
            page_count: 256,
            scan_fetch_batch_rows: 77,
            estimator_initial_tail_bytes_per_row: 33,
            scan_timing_detail: true,
        };

        let backend = config.backend_service_config();
        let worker = config.worker_runtime_config();

        assert_eq!(backend.scan_fetch_batch_rows, 77);
        assert_eq!(backend.estimator_default.initial_tail_bytes_per_row, 33);
        assert!(backend.scan_timing_detail);
        assert_eq!(backend.diagnostics.level, DiagnosticLogLevel::Trace);
        assert_eq!(backend.diagnostics.log_path.as_ref(), "/tmp/pg_fusion.log");
        assert_eq!(worker.control_frame_capacity, 4096);
    }
}
