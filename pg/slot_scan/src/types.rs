use crate::error::SinkError;
use pgrx::pg_sys;
use std::cell::RefCell;
use std::ffi::c_void;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

const DEFAULT_EXTENSION_LOG_PATH: &str = "/tmp/pg_fusion.log";

/// Diagnostic log verbosity for PostgreSQL-side scan internals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagnosticLogLevel {
    #[default]
    Off = 0,
    Basic = 1,
    Trace = 2,
}

impl DiagnosticLogLevel {
    pub fn from_i32(value: i32) -> Self {
        match value {
            1 => Self::Basic,
            value if value >= 2 => Self::Trace,
            _ => Self::Off,
        }
    }

    fn allows(self, required: Self) -> bool {
        self >= required && self != Self::Off
    }
}

/// Diagnostics sink configuration shared with callers that embed `slot_scan`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticsConfig {
    pub level: DiagnosticLogLevel,
    pub log_path: Arc<str>,
}

impl DiagnosticsConfig {
    pub fn new(level: DiagnosticLogLevel, log_path: impl Into<Arc<str>>) -> Self {
        Self {
            level,
            log_path: log_path.into(),
        }
    }

    fn enabled(&self, required: DiagnosticLogLevel) -> bool {
        self.level.allows(required)
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            level: DiagnosticLogLevel::Off,
            log_path: Arc::from(DEFAULT_EXTENSION_LOG_PATH),
        }
    }
}

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
    /// Optional diagnostics for debug/repro runs. Defaults to disabled.
    pub diagnostics: DiagnosticsConfig,
}

/// Options used when rendering a PostgreSQL plan for trusted scan SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanExplainOptions {
    pub verbose: bool,
    pub costs: bool,
}

impl Default for ScanExplainOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            costs: true,
        }
    }
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

/// Result of draining one direct portal fetch into a slot callback.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotDrainResult {
    /// Rows consumed from the PostgreSQL portal during this drain call.
    pub rows_consumed: usize,
    /// Whether the portal reached EOF, including a configured local row cap.
    pub eof: bool,
    /// Whether the caller callback requested an early stop.
    pub stopped: bool,
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
    diagnostics: DiagnosticsConfig,
}

impl OwnedSpiPlan {
    pub(crate) unsafe fn from_spi_plan(
        ptr: pg_sys::SPIPlanPtr,
        diagnostics: DiagnosticsConfig,
    ) -> Self {
        Self { ptr, diagnostics }
    }

    pub(crate) fn as_ptr(&self) -> pg_sys::SPIPlanPtr {
        self.ptr
    }
}

impl Drop for OwnedSpiPlan {
    fn drop(&mut self) {
        slot_scan_diag(&self.diagnostics, DiagnosticLogLevel::Trace, || {
            format!(
                "drop OwnedSpiPlan ptr={:p} current_mcxt={:p}",
                self.ptr,
                diagnostic_current_memory_context()
            )
        });
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

#[derive(Debug)]
pub(crate) struct ExecutionSpiConnection {
    pub(crate) finish_restore_context: pg_sys::MemoryContext,
    pub(crate) diagnostics: DiagnosticsConfig,
}

impl Drop for ExecutionSpiConnection {
    fn drop(&mut self) {
        slot_scan_diag(&self.diagnostics, DiagnosticLogLevel::Trace, || {
            format!(
                "drop ExecutionSpiConnection before SPI_finish current_mcxt={:p} finish_restore_mcxt={:p}",
                diagnostic_current_memory_context(),
                self.finish_restore_context,
            )
        });
        unsafe {
            let _ = pg_sys::SPI_finish();
        }
        slot_scan_diag(&self.diagnostics, DiagnosticLogLevel::Trace, || {
            format!(
                "drop ExecutionSpiConnection after SPI_finish current_mcxt={:p}",
                diagnostic_current_memory_context()
            )
        });
    }
}

/// Internal execution-scoped SPI connection shared across one or more
/// streaming scan portals.
///
/// This type is intentionally hidden from normal crate docs. It exists so
/// backend-side multi-scan execution can keep one PostgreSQL SPI connection
/// alive while switching between multiple open portals.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ExecutionSpiContext {
    pub(crate) _inner: Rc<ExecutionSpiConnection>,
}

struct CachedLogFile {
    path: Arc<str>,
    file: File,
}

thread_local! {
    static LOG_FILE: RefCell<Option<CachedLogFile>> = const { RefCell::new(None) };
}

fn slot_scan_diag(
    diagnostics: &DiagnosticsConfig,
    required: DiagnosticLogLevel,
    message: impl FnOnce() -> String,
) {
    if !diagnostics.enabled(required) {
        return;
    }
    write_diag_line(diagnostics, "slot_scan", required, &message());
}

fn write_diag_line(
    diagnostics: &DiagnosticsConfig,
    component: &str,
    level: DiagnosticLogLevel,
    message: &str,
) {
    LOG_FILE.with(|slot| {
        let mut cached = slot.borrow_mut();
        if cached
            .as_ref()
            .is_none_or(|cached| cached.path.as_ref() != diagnostics.log_path.as_ref())
        {
            *cached = open_log_file(Arc::clone(&diagnostics.log_path));
        }

        let Some(cached) = cached.as_mut() else {
            return;
        };
        let _ = writeln!(
            cached.file,
            "pid={} component={} level={:?} target=slot_scan {}",
            std::process::id(),
            component,
            level,
            message
        );
    });
}

fn open_log_file(path: Arc<str>) -> Option<CachedLogFile> {
    let path_ref = Path::new(path.as_ref());
    if let Some(parent) = path_ref.parent() {
        let _ = create_dir_all(parent);
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path_ref)
        .ok()?;
    Some(CachedLogFile { path, file })
}

fn diagnostic_current_memory_context() -> pg_sys::MemoryContext {
    #[cfg(test)]
    {
        std::ptr::null_mut()
    }
    #[cfg(not(test))]
    unsafe {
        pg_sys::CurrentMemoryContext
    }
}

/// Internal stackful scan session used by backend-side page streaming.
///
/// This type is intentionally hidden from normal crate docs. It is not a
/// durable resumable cursor: it keeps one portal alive across calls and drains
/// it through a direct `DestReceiver` callback. It relies on an
/// execution-scoped SPI connection remaining active for the lifetime of the
/// session. Callers must not interleave other SPI or planning work while such
/// sessions are active.
#[doc(hidden)]
#[derive(Debug)]
pub struct StreamingScanSession {
    pub(crate) prepared: PreparedScan,
    pub(crate) _spi: ExecutionSpiContext,
    pub(crate) portal: pg_sys::Portal,
    pub(crate) fetch_batch_rows: usize,
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
