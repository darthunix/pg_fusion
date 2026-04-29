use std::alloc::Layout;
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use pool::PageDescriptor;

const METRICS_MAGIC: u64 = 0x5047_4655_4D45_5431;
const METRICS_VERSION: u32 = 9;
const NO_STAMP: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeMetricsConfig {
    page_count: u32,
}

impl RuntimeMetricsConfig {
    pub fn new(page_count: u32) -> Result<Self, MetricsError> {
        if page_count == 0 {
            return Err(MetricsError::ZeroPageCount);
        }
        Ok(Self { page_count })
    }

    pub fn page_count(self) -> u32 {
        self.page_count
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeMetricsLayout {
    pub size: usize,
    pub align: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Timer,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Timer => "timer",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricUnit {
    Count,
    Nanoseconds,
    Bytes,
}

impl MetricUnit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Nanoseconds => "ns",
            Self::Bytes => "bytes",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricDescriptor {
    pub id: MetricId,
    pub component: &'static str,
    pub metric: &'static str,
    pub kind: MetricKind,
    pub unit: MetricUnit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum MetricId {
    QueryTotalNs = 0,
    BackendTotalNs,
    BackendExecCallsTotal,
    BackendRowsReturnedTotal,
    BackendWaitLatchNs,
    BackendWaitLatchTotal,
    ScanPageFillNs,
    ScanPagePrepareNs,
    ScanPageFinishNs,
    ScanPageSnapshotNs,
    ScanSlotDrainNs,
    ScanOverflowCopyNs,
    ScanPageRetryNs,
    ScanPageRetryTotal,
    ScanFillPreDrainNs,
    ScanFillPostDrainNs,
    ScanFillOverflowEncodeNs,
    ScanFillEmitNs,
    ScanFillUnclassifiedNs,
    ScanFetchCallsTotal,
    ScanRowsEncodedTotal,
    ScanFullPagesTotal,
    ScanEofPagesTotal,
    ScanPagesSentTotal,
    ScanBytesSentTotal,
    ScanB2wWaitNs,
    ScanB2wWaitTotal,
    ScanPageReadNs,
    ScanPagesReadTotal,
    ScanBatchSendNs,
    ScanBatchSendTotal,
    ScanBatchDeliveryNs,
    ScanBatchDeliveryTotal,
    ScanIdleSleepNs,
    ScanIdleSleepTotal,
    WorkerTotalNs,
    WorkerPhysicalPlanNs,
    WorkerPhysicalPlanTotal,
    WorkerResultPageFillNs,
    WorkerResultPagesTotal,
    WorkerResultBytesSentTotal,
    ResultW2bWaitNs,
    ResultW2bWaitTotal,
    ResultPageReadNs,
    ResultPagesReadTotal,
    RuntimeFilterAllocatedTotal,
    RuntimeFilterReadyTotal,
    RuntimeFilterPoolExhaustedTotal,
    RuntimeFilterBuildRowsTotal,
    RuntimeFilterProbeRowsTotal,
    RuntimeFilterProbeRowsRejectedTotal,
    RuntimeFilterProbePassUnfilteredTotal,
}

pub const METRIC_COUNT: usize = MetricId::RuntimeFilterProbePassUnfilteredTotal as usize + 1;

pub const METRIC_DESCRIPTORS: [MetricDescriptor; METRIC_COUNT] = [
    MetricDescriptor {
        id: MetricId::QueryTotalNs,
        component: "query",
        metric: "query_total_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::BackendTotalNs,
        component: "backend",
        metric: "backend_total_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::BackendExecCallsTotal,
        component: "backend",
        metric: "backend_exec_calls_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::BackendRowsReturnedTotal,
        component: "backend",
        metric: "backend_rows_returned_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::BackendWaitLatchNs,
        component: "backend",
        metric: "backend_wait_latch_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::BackendWaitLatchTotal,
        component: "backend",
        metric: "backend_wait_latch_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanPageFillNs,
        component: "scan",
        metric: "scan_page_fill_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanPagePrepareNs,
        component: "scan",
        metric: "scan_page_prepare_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanPageFinishNs,
        component: "scan",
        metric: "scan_page_finish_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanPageSnapshotNs,
        component: "scan",
        metric: "scan_page_snapshot_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanSlotDrainNs,
        component: "scan",
        metric: "scan_slot_drain_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanOverflowCopyNs,
        component: "scan",
        metric: "scan_overflow_copy_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanPageRetryNs,
        component: "scan",
        metric: "scan_page_retry_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanPageRetryTotal,
        component: "scan",
        metric: "scan_page_retry_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanFillPreDrainNs,
        component: "scan",
        metric: "scan_fill_pre_drain_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanFillPostDrainNs,
        component: "scan",
        metric: "scan_fill_post_drain_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanFillOverflowEncodeNs,
        component: "scan",
        metric: "scan_fill_overflow_encode_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanFillEmitNs,
        component: "scan",
        metric: "scan_fill_emit_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanFillUnclassifiedNs,
        component: "scan",
        metric: "scan_fill_unclassified_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanFetchCallsTotal,
        component: "scan",
        metric: "scan_fetch_calls_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanRowsEncodedTotal,
        component: "scan",
        metric: "scan_rows_encoded_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanFullPagesTotal,
        component: "scan",
        metric: "scan_full_pages_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanEofPagesTotal,
        component: "scan",
        metric: "scan_eof_pages_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanPagesSentTotal,
        component: "scan",
        metric: "scan_pages_sent_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanBytesSentTotal,
        component: "scan",
        metric: "scan_bytes_sent_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Bytes,
    },
    MetricDescriptor {
        id: MetricId::ScanB2wWaitNs,
        component: "scan",
        metric: "scan_b2w_wait_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanB2wWaitTotal,
        component: "scan",
        metric: "scan_b2w_wait_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanPageReadNs,
        component: "scan",
        metric: "scan_page_read_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanPagesReadTotal,
        component: "scan",
        metric: "scan_pages_read_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanBatchSendNs,
        component: "scan",
        metric: "scan_batch_send_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanBatchSendTotal,
        component: "scan",
        metric: "scan_batch_send_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanBatchDeliveryNs,
        component: "scan",
        metric: "scan_batch_delivery_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanBatchDeliveryTotal,
        component: "scan",
        metric: "scan_batch_delivery_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ScanIdleSleepNs,
        component: "scan",
        metric: "scan_idle_sleep_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ScanIdleSleepTotal,
        component: "scan",
        metric: "scan_idle_sleep_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::WorkerTotalNs,
        component: "worker",
        metric: "worker_total_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::WorkerPhysicalPlanNs,
        component: "worker",
        metric: "worker_physical_plan_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::WorkerPhysicalPlanTotal,
        component: "worker",
        metric: "worker_physical_plan_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::WorkerResultPageFillNs,
        component: "worker",
        metric: "worker_result_page_fill_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::WorkerResultPagesTotal,
        component: "worker",
        metric: "worker_result_pages_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::WorkerResultBytesSentTotal,
        component: "worker",
        metric: "worker_result_bytes_sent_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Bytes,
    },
    MetricDescriptor {
        id: MetricId::ResultW2bWaitNs,
        component: "result",
        metric: "result_w2b_wait_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ResultW2bWaitTotal,
        component: "result",
        metric: "result_w2b_wait_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::ResultPageReadNs,
        component: "result",
        metric: "result_page_read_ns",
        kind: MetricKind::Timer,
        unit: MetricUnit::Nanoseconds,
    },
    MetricDescriptor {
        id: MetricId::ResultPagesReadTotal,
        component: "result",
        metric: "result_pages_read_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterAllocatedTotal,
        component: "runtime_filter",
        metric: "runtime_filter_allocated_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterReadyTotal,
        component: "runtime_filter",
        metric: "runtime_filter_ready_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterPoolExhaustedTotal,
        component: "runtime_filter",
        metric: "runtime_filter_pool_exhausted_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterBuildRowsTotal,
        component: "runtime_filter",
        metric: "runtime_filter_build_rows_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterProbeRowsTotal,
        component: "runtime_filter",
        metric: "runtime_filter_probe_rows_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterProbeRowsRejectedTotal,
        component: "runtime_filter",
        metric: "runtime_filter_probe_rows_rejected_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
    MetricDescriptor {
        id: MetricId::RuntimeFilterProbePassUnfilteredTotal,
        component: "runtime_filter",
        metric: "runtime_filter_probe_pass_unfiltered_total",
        kind: MetricKind::Counter,
        unit: MetricUnit::Count,
    },
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PageDirection {
    BackendToWorker = 1,
    WorkerToBackend = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageObservation {
    pub wait_ns: u64,
    pub payload_bytes: u64,
}

#[derive(Clone, Copy, Default)]
pub struct RuntimeMetrics {
    header: Option<NonNull<MetricsHeader>>,
    values: Option<NonNull<AtomicU64>>,
    stamps: Option<NonNull<PageStamp>>,
    page_count: u32,
}

unsafe impl Send for RuntimeMetrics {}
unsafe impl Sync for RuntimeMetrics {}

impl fmt::Debug for RuntimeMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeMetrics")
            .field("attached", &self.header.is_some())
            .field("page_count", &self.page_count)
            .finish()
    }
}

impl PartialEq for RuntimeMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.header.map(NonNull::as_ptr) == other.header.map(NonNull::as_ptr)
            && self.page_count == other.page_count
    }
}

impl Eq for RuntimeMetrics {}

impl RuntimeMetrics {
    pub fn layout(config: RuntimeMetricsConfig) -> Result<RuntimeMetricsLayout, MetricsError> {
        let computed = ComputedLayout::new(config.page_count)?;
        Ok(RuntimeMetricsLayout {
            size: computed.region.size(),
            align: computed.region.align(),
        })
    }

    /// # Safety
    /// `base` must point at a writable shared-memory region of at least `len`
    /// bytes that remains valid for the returned handle's lifetime.
    pub unsafe fn init_in_place(
        base: NonNull<u8>,
        len: usize,
        config: RuntimeMetricsConfig,
    ) -> Result<Self, MetricsError> {
        let computed = ComputedLayout::new(config.page_count)?;
        validate_region(base, len, computed.region)?;

        let header = base.as_ptr().cast::<MetricsHeader>();
        let values = base
            .as_ptr()
            .add(computed.values_offset)
            .cast::<AtomicU64>();
        let stamps = base
            .as_ptr()
            .add(computed.stamps_offset)
            .cast::<PageStamp>();

        for index in 0..METRIC_COUNT {
            std::ptr::write(values.add(index), AtomicU64::new(0));
        }
        for index in 0..config.page_count as usize {
            std::ptr::write(stamps.add(index), PageStamp::zeroed());
        }
        std::ptr::write(
            header,
            MetricsHeader {
                magic: METRICS_MAGIC,
                version: METRICS_VERSION,
                metric_count: METRIC_COUNT as u32,
                page_count: config.page_count,
                region_size: computed.region.size() as u64,
                reset_epoch: AtomicU64::new(1),
            },
        );

        Ok(Self {
            header: Some(NonNull::new_unchecked(header)),
            values: Some(NonNull::new_unchecked(values)),
            stamps: Some(NonNull::new_unchecked(stamps)),
            page_count: config.page_count,
        })
    }

    /// # Safety
    /// `base` must point at an initialized metrics region that remains valid
    /// for the returned handle's lifetime.
    pub unsafe fn attach(base: NonNull<u8>, len: usize) -> Result<Self, MetricsError> {
        validate_min_header(base, len)?;
        let header = &*base.as_ptr().cast::<MetricsHeader>();
        if header.magic != METRICS_MAGIC {
            return Err(MetricsError::BadMagic {
                expected: METRICS_MAGIC,
                actual: header.magic,
            });
        }
        if header.version != METRICS_VERSION {
            return Err(MetricsError::UnsupportedVersion {
                expected: METRICS_VERSION,
                actual: header.version,
            });
        }
        if header.metric_count != METRIC_COUNT as u32 {
            return Err(MetricsError::MetricCountMismatch {
                expected: METRIC_COUNT as u32,
                actual: header.metric_count,
            });
        }

        let computed = ComputedLayout::new(header.page_count)?;
        validate_region(base, len, computed.region)?;
        if header.region_size as usize != computed.region.size() {
            return Err(MetricsError::LayoutMismatch {
                expected: computed.region.size(),
                actual: header.region_size as usize,
            });
        }

        Ok(Self {
            header: Some(NonNull::new_unchecked(
                base.as_ptr().cast::<MetricsHeader>(),
            )),
            values: Some(NonNull::new_unchecked(
                base.as_ptr()
                    .add(computed.values_offset)
                    .cast::<AtomicU64>(),
            )),
            stamps: Some(NonNull::new_unchecked(
                base.as_ptr()
                    .add(computed.stamps_offset)
                    .cast::<PageStamp>(),
            )),
            page_count: header.page_count,
        })
    }

    pub fn is_attached(&self) -> bool {
        self.header.is_some()
    }

    pub fn now_ns(&self) -> u64 {
        monotonic_ns()
    }

    pub fn reset_epoch(&self) -> u64 {
        self.header_ref()
            .map(|header| header.reset_epoch.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    pub fn reset(&self) -> u64 {
        let Some(header) = self.header_ref() else {
            return 0;
        };
        let next_epoch = header.reset_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        if let Some(values) = self.values {
            for index in 0..METRIC_COUNT {
                unsafe { values.as_ptr().add(index).as_ref() }
                    .expect("metric value pointer must be valid")
                    .store(0, Ordering::Relaxed);
            }
        }
        if let Some(stamps) = self.stamps {
            for index in 0..self.page_count as usize {
                unsafe { stamps.as_ptr().add(index).as_ref() }
                    .expect("page stamp pointer must be valid")
                    .clear();
            }
        }
        next_epoch
    }

    pub fn add(&self, id: MetricId, value: u64) {
        if value == 0 {
            return;
        }
        if let Some(metric) = self.metric_ref(id) {
            let _ = metric.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(value))
            });
        }
    }

    pub fn increment(&self, id: MetricId) {
        self.add(id, 1);
    }

    pub fn add_elapsed(&self, id: MetricId, start_ns: u64) {
        if start_ns == 0 {
            return;
        }
        self.add(id, monotonic_ns().saturating_sub(start_ns));
    }

    pub fn get(&self, id: MetricId) -> u64 {
        self.metric_ref(id)
            .map(|value| value.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn snapshot(&self) -> Vec<MetricValue> {
        METRIC_DESCRIPTORS
            .iter()
            .map(|descriptor| MetricValue {
                descriptor: *descriptor,
                value: self.get(descriptor.id),
                reset_epoch: self.reset_epoch(),
            })
            .collect()
    }

    pub fn stamp_page(
        &self,
        direction: PageDirection,
        descriptor: PageDescriptor,
        payload_bytes: usize,
    ) {
        let Some(stamp) = self.stamp_ref(descriptor.page_id) else {
            return;
        };
        stamp.pool_id.store(descriptor.pool_id, Ordering::Relaxed);
        stamp
            .generation
            .store(descriptor.generation, Ordering::Relaxed);
        stamp.sent_ns.store(monotonic_ns(), Ordering::Relaxed);
        stamp.payload_bytes.store(
            u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        stamp.epoch_direction.store(
            pack_epoch_direction(self.reset_epoch(), direction),
            Ordering::Release,
        );
    }

    pub fn observe_page(
        &self,
        direction: PageDirection,
        descriptor: PageDescriptor,
    ) -> Option<PageObservation> {
        self.observe_page_at(direction, descriptor, monotonic_ns())
    }

    pub fn observe_page_at(
        &self,
        direction: PageDirection,
        descriptor: PageDescriptor,
        observed_ns: u64,
    ) -> Option<PageObservation> {
        let stamp = self.stamp_ref(descriptor.page_id)?;
        let expected = pack_epoch_direction(self.reset_epoch(), direction);
        if stamp.epoch_direction.load(Ordering::Acquire) != expected {
            return None;
        }
        if stamp.pool_id.load(Ordering::Relaxed) != descriptor.pool_id {
            return None;
        }
        if stamp.generation.load(Ordering::Relaxed) != descriptor.generation {
            return None;
        }
        let sent_ns = stamp.sent_ns.load(Ordering::Relaxed);
        if sent_ns == 0 {
            return None;
        }
        Some(PageObservation {
            wait_ns: observed_ns.saturating_sub(sent_ns),
            payload_bytes: stamp.payload_bytes.load(Ordering::Relaxed),
        })
    }

    fn header_ref(&self) -> Option<&MetricsHeader> {
        self.header.map(|ptr| unsafe { ptr.as_ref() })
    }

    fn metric_ref(&self, id: MetricId) -> Option<&AtomicU64> {
        self.values.map(|values| unsafe {
            values
                .as_ptr()
                .add(id as usize)
                .as_ref()
                .expect("metric value pointer must be valid")
        })
    }

    fn stamp_ref(&self, page_id: u32) -> Option<&PageStamp> {
        if page_id >= self.page_count {
            return None;
        }
        self.stamps.map(|stamps| unsafe {
            stamps
                .as_ptr()
                .add(page_id as usize)
                .as_ref()
                .expect("page stamp pointer must be valid")
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricValue {
    pub descriptor: MetricDescriptor,
    pub value: u64,
    pub reset_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsError {
    ZeroPageCount,
    LayoutOverflow,
    RegionTooSmall { expected: usize, actual: usize },
    BadAlignment { expected: usize, actual: usize },
    BadMagic { expected: u64, actual: u64 },
    UnsupportedVersion { expected: u32, actual: u32 },
    MetricCountMismatch { expected: u32, actual: u32 },
    LayoutMismatch { expected: usize, actual: usize },
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPageCount => write!(f, "runtime metrics page count must be positive"),
            Self::LayoutOverflow => write!(f, "runtime metrics layout overflow"),
            Self::RegionTooSmall { expected, actual } => {
                write!(
                    f,
                    "runtime metrics region too small: expected {expected}, got {actual}"
                )
            }
            Self::BadAlignment { expected, actual } => {
                write!(
                    f,
                    "runtime metrics region has bad alignment: expected {expected}, got {actual}"
                )
            }
            Self::BadMagic { expected, actual } => {
                write!(
                    f,
                    "runtime metrics bad magic: expected {expected:#x}, got {actual:#x}"
                )
            }
            Self::UnsupportedVersion { expected, actual } => {
                write!(
                    f,
                    "runtime metrics unsupported version: expected {expected}, got {actual}"
                )
            }
            Self::MetricCountMismatch { expected, actual } => {
                write!(
                    f,
                    "runtime metrics metric count mismatch: expected {expected}, got {actual}"
                )
            }
            Self::LayoutMismatch { expected, actual } => {
                write!(
                    f,
                    "runtime metrics layout mismatch: expected {expected}, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for MetricsError {}

#[repr(C)]
struct MetricsHeader {
    magic: u64,
    version: u32,
    metric_count: u32,
    page_count: u32,
    region_size: u64,
    reset_epoch: AtomicU64,
}

#[repr(C)]
struct PageStamp {
    epoch_direction: AtomicU64,
    pool_id: AtomicU64,
    generation: AtomicU64,
    sent_ns: AtomicU64,
    payload_bytes: AtomicU64,
}

impl PageStamp {
    fn zeroed() -> Self {
        Self {
            epoch_direction: AtomicU64::new(NO_STAMP),
            pool_id: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            sent_ns: AtomicU64::new(0),
            payload_bytes: AtomicU64::new(0),
        }
    }

    fn clear(&self) {
        self.epoch_direction.store(NO_STAMP, Ordering::Release);
        self.pool_id.store(0, Ordering::Relaxed);
        self.generation.store(0, Ordering::Relaxed);
        self.sent_ns.store(0, Ordering::Relaxed);
        self.payload_bytes.store(0, Ordering::Relaxed);
    }
}

struct ComputedLayout {
    region: Layout,
    values_offset: usize,
    stamps_offset: usize,
}

impl ComputedLayout {
    fn new(page_count: u32) -> Result<Self, MetricsError> {
        if page_count == 0 {
            return Err(MetricsError::ZeroPageCount);
        }
        let align = std::mem::align_of::<MetricsHeader>()
            .max(std::mem::align_of::<AtomicU64>())
            .max(std::mem::align_of::<PageStamp>());
        let header_size = std::mem::size_of::<MetricsHeader>();
        let values_offset = align_up(header_size, std::mem::align_of::<AtomicU64>())?;
        let values_size = std::mem::size_of::<AtomicU64>()
            .checked_mul(METRIC_COUNT)
            .ok_or(MetricsError::LayoutOverflow)?;
        let stamps_offset = align_up(
            values_offset
                .checked_add(values_size)
                .ok_or(MetricsError::LayoutOverflow)?,
            std::mem::align_of::<PageStamp>(),
        )?;
        let stamps_size = std::mem::size_of::<PageStamp>()
            .checked_mul(page_count as usize)
            .ok_or(MetricsError::LayoutOverflow)?;
        let size = stamps_offset
            .checked_add(stamps_size)
            .ok_or(MetricsError::LayoutOverflow)?;
        let region = Layout::from_size_align(align_up(size, align)?, align)
            .map_err(|_| MetricsError::LayoutOverflow)?;
        Ok(Self {
            region,
            values_offset,
            stamps_offset,
        })
    }
}

fn validate_min_header(base: NonNull<u8>, len: usize) -> Result<(), MetricsError> {
    let header = Layout::new::<MetricsHeader>();
    validate_region(base, len, header)
}

fn validate_region(base: NonNull<u8>, len: usize, layout: Layout) -> Result<(), MetricsError> {
    if (base.as_ptr() as usize) % layout.align() != 0 {
        return Err(MetricsError::BadAlignment {
            expected: layout.align(),
            actual: base.as_ptr() as usize,
        });
    }
    if len < layout.size() {
        return Err(MetricsError::RegionTooSmall {
            expected: layout.size(),
            actual: len,
        });
    }
    Ok(())
}

fn align_up(value: usize, align: usize) -> Result<usize, MetricsError> {
    let mask = align.checked_sub(1).ok_or(MetricsError::LayoutOverflow)?;
    value
        .checked_add(mask)
        .map(|value| value & !mask)
        .ok_or(MetricsError::LayoutOverflow)
}

fn pack_epoch_direction(epoch: u64, direction: PageDirection) -> u64 {
    epoch.checked_shl(8).unwrap_or(u64::MAX & !0xff) | direction as u64
}

pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 {
        return 0;
    }
    u64::try_from(ts.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(ts.tv_nsec).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{alloc_zeroed, dealloc};

    struct TestRegion {
        base: NonNull<u8>,
        layout: Layout,
    }

    impl TestRegion {
        fn new(layout: RuntimeMetricsLayout) -> Self {
            let layout = Layout::from_size_align(layout.size, layout.align).expect("layout");
            let ptr = unsafe { alloc_zeroed(layout) };
            let base = NonNull::new(ptr).expect("allocated test region");
            Self { base, layout }
        }
    }

    impl Drop for TestRegion {
        fn drop(&mut self) {
            unsafe { dealloc(self.base.as_ptr(), self.layout) };
        }
    }

    #[test]
    fn init_attach_and_count_metrics() {
        let cfg = RuntimeMetricsConfig::new(4).expect("config");
        let layout = RuntimeMetrics::layout(cfg).expect("layout");
        let region = TestRegion::new(layout);
        let metrics =
            unsafe { RuntimeMetrics::init_in_place(region.base, layout.size, cfg) }.expect("init");

        metrics.increment(MetricId::BackendExecCallsTotal);
        metrics.add(MetricId::ScanBytesSentTotal, 42);

        let attached =
            unsafe { RuntimeMetrics::attach(region.base, layout.size) }.expect("attach metrics");
        assert_eq!(attached.get(MetricId::BackendExecCallsTotal), 1);
        assert_eq!(attached.get(MetricId::ScanBytesSentTotal), 42);
        assert_eq!(attached.snapshot().len(), METRIC_COUNT);
    }

    #[test]
    fn metric_descriptors_have_unique_names() {
        let mut names = std::collections::HashSet::new();
        for descriptor in METRIC_DESCRIPTORS {
            assert!(
                names.insert(descriptor.metric),
                "duplicate metric name {}",
                descriptor.metric
            );
        }
        assert_eq!(names.len(), METRIC_COUNT);
    }

    #[test]
    fn page_stamp_observes_matching_generation() {
        let cfg = RuntimeMetricsConfig::new(2).expect("config");
        let layout = RuntimeMetrics::layout(cfg).expect("layout");
        let region = TestRegion::new(layout);
        let metrics =
            unsafe { RuntimeMetrics::init_in_place(region.base, layout.size, cfg) }.expect("init");
        let descriptor = PageDescriptor {
            pool_id: 7,
            page_id: 1,
            generation: 3,
        };

        metrics.stamp_page(PageDirection::BackendToWorker, descriptor, 128);
        let observed_at = metrics.now_ns();
        let observed = metrics
            .observe_page_at(PageDirection::BackendToWorker, descriptor, observed_at)
            .expect("matching stamp observed");
        assert_eq!(observed.payload_bytes, 128);

        assert!(metrics
            .observe_page(
                PageDirection::WorkerToBackend,
                PageDescriptor {
                    generation: 3,
                    ..descriptor
                },
            )
            .is_none());
        assert!(metrics
            .observe_page(
                PageDirection::BackendToWorker,
                PageDescriptor {
                    generation: 4,
                    ..descriptor
                },
            )
            .is_none());
    }

    #[test]
    fn reset_ignores_stale_page_stamps() {
        let cfg = RuntimeMetricsConfig::new(1).expect("config");
        let layout = RuntimeMetrics::layout(cfg).expect("layout");
        let region = TestRegion::new(layout);
        let metrics =
            unsafe { RuntimeMetrics::init_in_place(region.base, layout.size, cfg) }.expect("init");
        let descriptor = PageDescriptor {
            pool_id: 9,
            page_id: 0,
            generation: 1,
        };

        metrics.increment(MetricId::BackendExecCallsTotal);
        metrics.stamp_page(PageDirection::BackendToWorker, descriptor, 64);
        let epoch = metrics.reset();

        assert_eq!(metrics.get(MetricId::BackendExecCallsTotal), 0);
        assert_eq!(metrics.reset_epoch(), epoch);
        assert!(metrics
            .observe_page(PageDirection::BackendToWorker, descriptor)
            .is_none());
    }
}
