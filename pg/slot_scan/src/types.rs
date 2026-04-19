use crate::error::SinkError;
use pgrx::pg_sys;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::sync::Arc;

/// Options that affect one `slot_scan` execution.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanOptions {
    /// Optional prepare-time planner hint used to bias PostgreSQL toward
    /// fast-start plans.
    ///
    /// `slot_scan` currently lowers this to `CURSOR_OPT_FAST_PLAN` during
    /// `prepare_scan()`. The numeric value is preserved in the API as a fetch
    /// hint from upstream code, but `slot_scan` does not interpret it as an
    /// exact row goal.
    ///
    /// In the default `scan_sql -> slot_scan` path, this is one of the
    /// intended lowering targets for `CompiledScan.requested_limit`.
    pub planner_fetch_hint: Option<usize>,
    /// Optional early-stop hint applied by the scan loop in the current
    /// executor process. This is a local cap, not an exact global SQL LIMIT.
    ///
    /// In the default `scan_sql -> slot_scan` path, this is the intended
    /// run-time lowering target for `CompiledScan.requested_limit`.
    pub local_row_cap: Option<usize>,
}

/// Leaf scan shape chosen by the current run-time PostgreSQL plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScanPlanKind {
    #[default]
    Unknown,
    SeqScan,
    IndexScan,
    IndexOnlyScan,
    BitmapHeapScan,
}

/// Run-time statistics returned after a scan finishes.
///
/// These fields reflect the current revalidated portal plan that actually ran,
/// not metadata captured during `prepare_scan()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanStats {
    /// Number of rows delivered to the sink.
    pub rows_seen: usize,
    /// Whether the local early-stop cap was reached.
    pub hit_local_row_cap: bool,
    /// Whether the run-time portal plan was parallel-capable.
    pub parallel_capable: bool,
    /// Number of workers requested by the run-time `Gather` node, if any.
    pub planned_workers: usize,
    /// Leaf scan shape chosen by the current run-time PostgreSQL plan.
    pub plan_kind: ScanPlanKind,
}

/// Result of one sink row callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotSinkAction {
    /// Continue scanning more rows.
    Continue,
    /// Stop the local scan loop early.
    Stop,
}

/// Mutable run-time context shared across sink callbacks during one
/// [`PreparedScan::run`](crate::PreparedScan::run) call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSinkContext {
    worker_index: usize,
    planned_workers: usize,
    rows_seen: usize,
    parallel_capable: bool,
    plan_kind: ScanPlanKind,
}

impl SlotSinkContext {
    pub(crate) fn new() -> Self {
        Self {
            worker_index: 0,
            planned_workers: 0,
            rows_seen: 0,
            parallel_capable: false,
            plan_kind: ScanPlanKind::Unknown,
        }
    }

    /// Worker index for the current callback source.
    ///
    /// The current implementation always runs callbacks in the leader backend,
    /// so this is `0` today.
    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    /// Number of workers requested by the current run-time plan.
    pub fn planned_workers(&self) -> usize {
        self.planned_workers
    }

    /// Number of rows already delivered to the sink in this run.
    pub fn rows_seen(&self) -> usize {
        self.rows_seen
    }

    /// Whether the current run-time plan is parallel-capable.
    pub fn parallel_capable(&self) -> bool {
        self.parallel_capable
    }

    /// Leaf scan shape chosen by the current run-time PostgreSQL plan.
    pub fn plan_kind(&self) -> ScanPlanKind {
        self.plan_kind
    }

    pub(crate) fn set_runtime_metadata(
        &mut self,
        parallel_capable: bool,
        planned_workers: usize,
        plan_kind: ScanPlanKind,
    ) {
        self.parallel_capable = parallel_capable;
        self.planned_workers = planned_workers;
        self.plan_kind = plan_kind;
    }

    pub(crate) fn bump_rows(&mut self) {
        self.rows_seen += 1;
    }
}

/// Callback table used by [`SlotSink`].
///
/// The callbacks are invoked in this order:
///
/// 1. `init`
/// 2. zero or more `consume_slot`
/// 3. `finish` on success, or `abort` on failure
///
/// `PreparedScan::run()` executes every callback behind a PostgreSQL exception
/// boundary. PostgreSQL errors and panics raised by `init`, `consume_slot`, or
/// `finish` are converted into ordinary `ScanError::Postgres` failures. On any
/// non-success exit, `abort` is invoked best-effort exactly once.
///
/// `init` receives the current run-time `TupleDesc`. That descriptor is valid
/// only for the lifetime of the current [`PreparedScan::run`](crate::PreparedScan::run)
/// call and must not be retained after `finish`/`abort`.
pub struct SlotSinkMethods {
    /// Optional initialization callback, invoked after the cursor is opened and
    /// after run-time plan metadata has been populated in [`SlotSinkContext`].
    pub init: Option<
        unsafe fn(
            ctx: &mut SlotSinkContext,
            private: *mut c_void,
            tuple_desc: pg_sys::TupleDesc,
        ) -> Result<(), SinkError>,
    >,
    /// Required row callback. The provided slot is reused across rows and is
    /// only valid for the duration of the callback.
    pub consume_slot: unsafe fn(
        ctx: &mut SlotSinkContext,
        private: *mut c_void,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<SlotSinkAction, SinkError>,
    /// Optional success callback, invoked after the scan loop completes.
    pub finish:
        Option<unsafe fn(ctx: &mut SlotSinkContext, private: *mut c_void) -> Result<(), SinkError>>,
    /// Optional failure callback, invoked exactly once if `run()` exits with an
    /// error after the sink has been constructed.
    pub abort: Option<unsafe fn(ctx: &mut SlotSinkContext, private: *mut c_void)>,
}

/// Bound sink instance passed into [`PreparedScan::run`](crate::PreparedScan::run).
///
/// The `private` pointer is owned by the caller. It must outlive the `run()`
/// call and point to memory that the callback table knows how to interpret.
pub struct SlotSink<'a> {
    pub(crate) methods: &'static SlotSinkMethods,
    pub(crate) private: *mut c_void,
    _marker: PhantomData<&'a mut c_void>,
}

impl<'a> SlotSink<'a> {
    /// Binds a typed sink-private value to a static callback table.
    pub fn new<T>(methods: &'static SlotSinkMethods, private: &'a mut T) -> Self {
        Self {
            methods,
            private: private as *mut T as *mut c_void,
            _marker: PhantomData,
        }
    }

    /// Binds an already-erased sink-private pointer to a static callback table.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `private` remains valid for the entire
    /// [`PreparedScan::run`](crate::PreparedScan::run) call and points to
    /// memory that the callback table knows how to interpret.
    pub unsafe fn from_raw(methods: &'static SlotSinkMethods, private: *mut c_void) -> Self {
        Self {
            methods,
            private,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub(crate) struct OwnedSpiPlan {
    ptr: pg_sys::SPIPlanPtr,
}

impl OwnedSpiPlan {
    pub(crate) unsafe fn from_spi_plan(ptr: pg_sys::SPIPlanPtr) -> Self {
        Self { ptr }
    }

    pub(crate) fn as_ptr(&self) -> pg_sys::SPIPlanPtr {
        self.ptr
    }
}

impl Drop for OwnedSpiPlan {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                pg_sys::SPI_freeplan(self.ptr);
            }
        }
    }
}

/// Reusable prepared scan state returned by [`crate::prepare_scan`].
///
/// `PreparedScan` stores trusted scan SQL together with a saved SPI plan. The
/// current result schema and plan metadata are determined at [`run`](Self::run)
/// time from the revalidated portal, not frozen at prepare time.
#[derive(Clone, Debug)]
pub struct PreparedScan {
    pub(crate) sql: String,
    pub(crate) options: ScanOptions,
    pub(crate) plan: Arc<OwnedSpiPlan>,
}

impl PreparedScan {
    /// Returns the original SQL text that was prepared.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns the execution options that will be applied by [`run`](Self::run).
    pub fn options(&self) -> &ScanOptions {
        &self.options
    }
}

/// Internal stackful scan session used by backend-side page streaming.
///
/// This type is intentionally hidden from normal crate docs. It is not a
/// durable resumable cursor: it keeps one SPI connection, one portal, and the
/// current fetched batch alive across calls. Callers must not interleave other
/// SPI or planning work while a session is active.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamingScanSession {
    pub(crate) prepared: PreparedScan,
    pub(crate) portal: pg_sys::Portal,
    pub(crate) spi_connected: bool,
    pub(crate) slot: *mut pg_sys::TupleTableSlot,
    pub(crate) fetch_batch_rows: usize,
    pub(crate) batch: *mut pg_sys::SPITupleTable,
    pub(crate) batch_processed: usize,
    pub(crate) batch_index: usize,
    pub(crate) row_loaded: bool,
    pub(crate) tuple_desc: pg_sys::TupleDesc,
    pub(crate) rows_seen: usize,
    pub(crate) remaining: usize,
    pub(crate) parallel_capable: bool,
    pub(crate) planned_workers: usize,
    pub(crate) plan_kind: ScanPlanKind,
    pub(crate) closed: bool,
}

impl StreamingScanSession {
    /// Current run-time tuple descriptor for this cursor.
    pub fn tuple_desc(&self) -> pg_sys::TupleDesc {
        self.tuple_desc
    }

    /// Whether the current portal plan is parallel-capable.
    pub fn parallel_capable(&self) -> bool {
        self.parallel_capable
    }

    /// Number of workers requested by the current portal plan.
    pub fn planned_workers(&self) -> usize {
        self.planned_workers
    }

    /// Leaf scan shape chosen by the current portal plan.
    pub fn plan_kind(&self) -> ScanPlanKind {
        self.plan_kind
    }

    /// Number of rows already yielded by this cursor.
    pub fn rows_seen(&self) -> usize {
        self.rows_seen
    }

    /// Whether this cursor has already reached the configured local row cap.
    pub fn hit_local_row_cap(&self) -> bool {
        self.prepared
            .options
            .local_row_cap
            .is_some_and(|cap| self.rows_seen >= cap)
    }
}
