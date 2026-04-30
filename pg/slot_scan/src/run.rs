use crate::error::{ScanError, SinkError};
use crate::plan::{inspect_planned_stmt, scan_error_from_caught_error, spi_status_error};
use crate::types::{
    ExecutionSpiConnection, ExecutionSpiContext, PreparedScan, ScanStats, SlotDrainResult,
    SlotSink, SlotSinkAction, SlotSinkContext, SlotSinkMethods, StreamingScanSession,
};
use pgrx::pg_sys;
use pgrx::PgTryBuilder;
use std::cell::Cell;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
use std::rc::Rc;
use std::time::Instant;

const DEFAULT_FETCH_BATCH_ROWS: usize = 1024;

fn normalize_fetch_batch_rows(fetch_batch_rows: usize) -> usize {
    fetch_batch_rows.max(1)
}

fn cursor_fetch_rows(
    fetch_batch_rows: usize,
    row_budget: usize,
    remaining: usize,
) -> std::ffi::c_long {
    fetch_batch_rows
        .min(row_budget)
        .min(remaining)
        .try_into()
        .unwrap()
}

#[repr(C)]
struct DirectSlotDestReceiver {
    dest: pg_sys::DestReceiver,
    state: *mut c_void,
}

struct DirectSlotReceiverState<E, F> {
    consume_slot: NonNull<F>,
    rows_consumed: usize,
    stopped: bool,
    error: Option<E>,
    profile_callbacks: bool,
    callback_ns: u64,
}

impl<E, F> DirectSlotReceiverState<E, F>
where
    F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
{
    fn new(consume_slot: &mut F, profile_callbacks: bool) -> Self {
        Self {
            consume_slot: NonNull::from(consume_slot),
            rows_consumed: 0,
            stopped: false,
            error: None,
            profile_callbacks,
            callback_ns: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DirectSlotReceiverMode {
    Guarded,
    Fast,
}

impl DirectSlotDestReceiver {
    fn new_guarded<E, F>(state: &mut DirectSlotReceiverState<E, F>) -> Self
    where
        E: From<ScanError> + 'static,
        F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
    {
        Self {
            dest: pg_sys::DestReceiver {
                receiveSlot: Some(direct_receive_slot::<E, F>),
                rStartup: Some(direct_receiver_startup),
                rShutdown: Some(direct_receiver_shutdown),
                rDestroy: Some(direct_receiver_destroy),
                mydest: pg_sys::CommandDest::DestNone,
            },
            state: state as *mut DirectSlotReceiverState<E, F> as *mut c_void,
        }
    }

    fn new_fast<E, F>(state: &mut DirectSlotReceiverState<E, F>) -> Self
    where
        E: From<ScanError> + 'static,
        F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
    {
        Self {
            dest: pg_sys::DestReceiver {
                receiveSlot: Some(direct_receive_slot_fast::<E, F>),
                rStartup: Some(direct_receiver_startup),
                rShutdown: Some(direct_receiver_shutdown),
                rDestroy: Some(direct_receiver_destroy),
                mydest: pg_sys::CommandDest::DestNone,
            },
            state: state as *mut DirectSlotReceiverState<E, F> as *mut c_void,
        }
    }
}

unsafe extern "C-unwind" fn direct_receive_slot<E, F>(
    slot: *mut pg_sys::TupleTableSlot,
    receiver: *mut pg_sys::DestReceiver,
) -> bool
where
    E: From<ScanError> + 'static,
    F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
{
    let receiver = unsafe { &mut *(receiver.cast::<DirectSlotDestReceiver>()) };
    let state = unsafe { &mut *(receiver.state.cast::<DirectSlotReceiverState<E, F>>()) };

    if state.error.is_some() {
        return false;
    }

    match catch_unwind(AssertUnwindSafe(|| receive_slot_callback(state, slot))) {
        Ok(result) => handle_receive_slot_result(state, result),
        Err(_) => {
            state.error =
                Some(ScanError::Postgres("slot receiver callback panicked".into()).into());
            false
        }
    }
}

unsafe extern "C-unwind" fn direct_receive_slot_fast<E, F>(
    slot: *mut pg_sys::TupleTableSlot,
    receiver: *mut pg_sys::DestReceiver,
) -> bool
where
    E: From<ScanError> + 'static,
    F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
{
    let receiver = unsafe { &mut *(receiver.cast::<DirectSlotDestReceiver>()) };
    let state = unsafe { &mut *(receiver.state.cast::<DirectSlotReceiverState<E, F>>()) };

    if state.error.is_some() {
        return false;
    }

    let result = receive_slot_callback(state, slot);
    handle_receive_slot_result(state, result)
}

#[inline(always)]
fn receive_slot_callback<E, F>(
    state: &mut DirectSlotReceiverState<E, F>,
    slot: *mut pg_sys::TupleTableSlot,
) -> Result<SlotSinkAction, E>
where
    F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
{
    let consume_slot = unsafe { state.consume_slot.as_mut() };
    if !state.profile_callbacks {
        return consume_slot(slot);
    }

    let start = Instant::now();
    let result = consume_slot(slot);
    state.callback_ns = state.callback_ns.saturating_add(elapsed_ns(start));
    result
}

#[inline(always)]
fn handle_receive_slot_result<E, F>(
    state: &mut DirectSlotReceiverState<E, F>,
    result: Result<SlotSinkAction, E>,
) -> bool
where
    F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
{
    match result {
        Ok(SlotSinkAction::Continue) => {
            state.rows_consumed += 1;
            true
        }
        Ok(SlotSinkAction::Stop) => {
            state.rows_consumed += 1;
            state.stopped = true;
            false
        }
        Err(error) => {
            state.error = Some(error);
            false
        }
    }
}

unsafe extern "C-unwind" fn direct_receiver_startup(
    _receiver: *mut pg_sys::DestReceiver,
    _operation: std::ffi::c_int,
    _typeinfo: pg_sys::TupleDesc,
) {
}

unsafe extern "C-unwind" fn direct_receiver_shutdown(_receiver: *mut pg_sys::DestReceiver) {}

unsafe extern "C-unwind" fn direct_receiver_destroy(_receiver: *mut pg_sys::DestReceiver) {}

struct SinkAbortGuard {
    ctx: *mut SlotSinkContext,
    methods: &'static SlotSinkMethods,
    private: *mut c_void,
    armed: bool,
}

impl SinkAbortGuard {
    fn new(
        ctx: &mut SlotSinkContext,
        methods: &'static SlotSinkMethods,
        private: *mut c_void,
    ) -> Self {
        Self {
            ctx,
            methods,
            private,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SinkAbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        unsafe {
            best_effort_sink_abort(&mut *self.ctx, self.methods, self.private);
        }
    }
}

impl PreparedScan {
    #[doc(hidden)]
    pub fn open_streaming_session(
        &self,
        fetch_batch_rows: usize,
    ) -> Result<StreamingScanSession, ScanError> {
        let spi = ExecutionSpiContext::connect(self.options.diagnostics.clone())?;
        self.open_streaming_session_in(&spi, fetch_batch_rows)
    }

    #[doc(hidden)]
    pub fn open_streaming_session_in(
        &self,
        spi: &ExecutionSpiContext,
        fetch_batch_rows: usize,
    ) -> Result<StreamingScanSession, ScanError> {
        let fetch_batch_rows = normalize_fetch_batch_rows(fetch_batch_rows);
        let portal = Cell::new(std::ptr::null_mut());
        let success = Cell::new(false);
        let previous_context = Cell::new(std::ptr::null_mut());

        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            previous_context.set(pg_sys::CurrentMemoryContext);
            portal.set(pg_sys::SPI_cursor_open(
                std::ptr::null(),
                self.plan.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null(),
                true,
            ));
            if portal.get().is_null() {
                return Err(spi_status_error("SPI_cursor_open", pg_sys::SPI_result));
            }

            let planned_stmt = pg_sys::PortalGetPrimaryStmt(portal.get());
            if planned_stmt.is_null() {
                return Err(ScanError::UnsupportedPlan(
                    "cursor portal has no primary planned statement".into(),
                ));
            }
            let metadata = inspect_planned_stmt(planned_stmt)?;

            let tuple_desc = (*portal.get()).tupDesc;
            if tuple_desc.is_null() {
                return Err(ScanError::MissingTupleDesc);
            }

            success.set(true);
            Ok(StreamingScanSession {
                prepared: self.clone(),
                _spi: spi.clone(),
                portal: portal.get(),
                fetch_batch_rows,
                tuple_desc,
                rows_seen: 0,
                remaining: self.options.local_row_cap.unwrap_or(usize::MAX),
                parallel_capable: metadata.parallel_capable,
                planned_workers: metadata.planned_workers,
                plan_kind: metadata.plan_kind,
                closed: false,
            })
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .finally(|| unsafe {
            if !success.get() && !portal.get().is_null() {
                pg_sys::SPI_cursor_close(portal.get());
                portal.set(std::ptr::null_mut());
            }
            restore_current_memory_context(previous_context.get());
        })
        .execute()
    }

    /// Executes the prepared trusted scan and feeds every row into the
    /// provided sink.
    ///
    /// The slot passed to `consume_slot` is reused across rows and is valid
    /// only for the duration of that callback.
    pub fn run(&self, sink: SlotSink<'_>) -> Result<ScanStats, ScanError> {
        let mut ctx = SlotSinkContext::new();
        let mut session = match self.open_streaming_session(DEFAULT_FETCH_BATCH_ROWS) {
            Ok(session) => session,
            Err(err) => {
                best_effort_sink_abort(&mut ctx, sink.methods, sink.private);
                return Err(err);
            }
        };

        ctx.set_runtime_metadata(
            session.parallel_capable(),
            session.planned_workers(),
            session.plan_kind(),
        );
        let mut abort_guard = SinkAbortGuard::new(&mut ctx, sink.methods, sink.private);

        (|| -> Result<ScanStats, ScanError> {
            if let Some(init) = sink.methods.init {
                call_sink_callback(|| unsafe {
                    init(&mut ctx, sink.private, session.tuple_desc())
                })?;
            }

            loop {
                let drain = session.drain_slots::<ScanError>(DEFAULT_FETCH_BATCH_ROWS, |slot| {
                    let action = call_sink_callback(|| unsafe {
                        (sink.methods.consume_slot)(&mut ctx, sink.private, slot)
                    })?;
                    ctx.bump_rows();
                    Ok::<SlotSinkAction, ScanError>(action)
                })?;

                if drain.eof || drain.stopped {
                    break;
                }
            }

            if let Some(finish) = sink.methods.finish {
                call_sink_callback(|| unsafe { finish(&mut ctx, sink.private) })?;
            }

            let stats = session.close()?;
            abort_guard.disarm();
            Ok(stats)
        })()
    }
}

impl StreamingScanSession {
    #[doc(hidden)]
    pub fn drain_slots<E>(
        &mut self,
        row_budget: usize,
        mut consume_slot: impl FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
    ) -> Result<SlotDrainResult, E>
    where
        E: From<ScanError> + 'static,
    {
        self.drain_slots_inner(
            row_budget,
            &mut consume_slot,
            DirectSlotReceiverMode::Guarded,
            false,
        )
    }

    /// Drains one portal fetch without protecting the row callback with
    /// `catch_unwind`.
    ///
    /// # Safety
    ///
    /// The callback must not panic. Any expected failure must be returned
    /// through `Result`; unwinding through PostgreSQL executor frames is not a
    /// supported recovery path.
    #[doc(hidden)]
    pub unsafe fn drain_slots_without_unwind_guard<E>(
        &mut self,
        row_budget: usize,
        mut consume_slot: impl FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
    ) -> Result<SlotDrainResult, E>
    where
        E: From<ScanError> + 'static,
    {
        self.drain_slots_inner(
            row_budget,
            &mut consume_slot,
            DirectSlotReceiverMode::Fast,
            false,
        )
    }

    /// Drains one portal fetch without an unwind guard and profiles the direct
    /// receiver callback time.
    ///
    /// # Safety
    ///
    /// The callback must not panic. Any expected failure must be returned
    /// through `Result`; unwinding through PostgreSQL executor frames is not a
    /// supported recovery path.
    #[doc(hidden)]
    pub unsafe fn drain_slots_without_unwind_guard_profiled<E>(
        &mut self,
        row_budget: usize,
        mut consume_slot: impl FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
    ) -> Result<SlotDrainResult, E>
    where
        E: From<ScanError> + 'static,
    {
        self.drain_slots_inner(
            row_budget,
            &mut consume_slot,
            DirectSlotReceiverMode::Fast,
            true,
        )
    }

    fn drain_slots_inner<E, F>(
        &mut self,
        row_budget: usize,
        consume_slot: &mut F,
        receiver_mode: DirectSlotReceiverMode,
        profile_callbacks: bool,
    ) -> Result<SlotDrainResult, E>
    where
        E: From<ScanError> + 'static,
        F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<SlotSinkAction, E>,
    {
        if self.closed {
            return Err(ScanError::CursorClosed.into());
        }

        if self.remaining == 0 {
            return Ok(SlotDrainResult {
                rows_consumed: 0,
                eof: true,
                stopped: false,
                elapsed_ns: 0,
                callback_ns: 0,
            });
        }

        if row_budget == 0 {
            return Err(ScanError::Postgres(
                "slot drain row budget must be greater than zero".into(),
            )
            .into());
        }

        let fetch_rows = cursor_fetch_rows(self.fetch_batch_rows, row_budget, self.remaining);

        let mut state = DirectSlotReceiverState::new(consume_slot, profile_callbacks);
        let mut receiver = match receiver_mode {
            DirectSlotReceiverMode::Guarded => DirectSlotDestReceiver::new_guarded(&mut state),
            DirectSlotReceiverMode::Fast => DirectSlotDestReceiver::new_fast(&mut state),
        };
        let portal_processed = Cell::new(0u64);
        let portal_elapsed_ns = Cell::new(0u64);
        let previous_context = Cell::new(std::ptr::null_mut());

        let result = PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            previous_context.set(pg_sys::CurrentMemoryContext);
            let start = profile_callbacks.then(Instant::now);
            let processed = pg_sys::PortalRunFetch(
                self.portal,
                pg_sys::FetchDirection::FETCH_FORWARD,
                fetch_rows,
                std::ptr::addr_of_mut!(receiver.dest),
            );
            if let Some(start) = start {
                portal_elapsed_ns.set(elapsed_ns(start));
            }
            portal_processed.set(processed);
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .finally(|| unsafe {
            restore_current_memory_context(previous_context.get());
        })
        .execute()
        .map_err(E::from);

        self.rows_seen += state.rows_consumed;
        self.remaining = self.remaining.saturating_sub(state.rows_consumed);

        result?;
        if let Some(error) = state.error {
            return Err(error);
        }

        let eof =
            !state.stopped && (self.remaining == 0 || portal_processed.get() < fetch_rows as u64);
        Ok(SlotDrainResult {
            rows_consumed: state.rows_consumed,
            eof,
            stopped: state.stopped,
            elapsed_ns: portal_elapsed_ns.get(),
            callback_ns: state.callback_ns,
        })
    }

    /// Closes the session, releases PostgreSQL resources, and returns the final
    /// run-time scan statistics observed by this session.
    #[doc(hidden)]
    pub fn close(mut self) -> Result<ScanStats, ScanError> {
        let stats = self.current_stats();
        self.close_inner()?;
        Ok(stats)
    }

    fn current_stats(&self) -> ScanStats {
        ScanStats {
            rows_seen: self.rows_seen,
            hit_local_row_cap: self.hit_local_row_cap(),
            parallel_capable: self.parallel_capable,
            planned_workers: self.planned_workers,
            plan_kind: self.plan_kind,
        }
    }

    fn close_inner(&mut self) -> Result<(), ScanError> {
        if self.closed {
            return Ok(());
        }

        let portal = std::mem::replace(&mut self.portal, std::ptr::null_mut());
        self.tuple_desc = std::ptr::null_mut();

        let previous_context = Cell::new(std::ptr::null_mut());
        let result = PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            previous_context.set(pg_sys::CurrentMemoryContext);
            if !portal.is_null() {
                pg_sys::SPI_cursor_close(portal);
            }
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .finally(|| unsafe {
            restore_current_memory_context(previous_context.get());
        })
        .execute();

        self.closed = true;
        result
    }

    fn best_effort_close(&mut self) {
        let _ = self.close_inner();
    }
}

impl Drop for StreamingScanSession {
    fn drop(&mut self) {
        self.best_effort_close();
    }
}

impl ExecutionSpiContext {
    #[doc(hidden)]
    pub fn connect(diagnostics: crate::types::DiagnosticsConfig) -> Result<Self, ScanError> {
        let previous_context = Cell::new(std::ptr::null_mut());
        let switched_context = Cell::new(false);

        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            let finish_restore_context = pg_sys::TopTransactionContext;
            if finish_restore_context.is_null() {
                return Err(ScanError::Postgres(
                    "TopTransactionContext is not available for SPI connection".into(),
                ));
            }

            previous_context.set(pg_sys::CurrentMemoryContext);
            pg_sys::MemoryContextSwitchTo(finish_restore_context);
            switched_context.set(true);

            let connect_rc = pg_sys::SPI_connect();
            if !previous_context.get().is_null() {
                pg_sys::MemoryContextSwitchTo(previous_context.get());
                switched_context.set(false);
            }
            if connect_rc != pg_sys::SPI_OK_CONNECT as i32 {
                return Err(spi_status_error("SPI_connect", connect_rc));
            }
            Ok(Self {
                _inner: Rc::new(ExecutionSpiConnection {
                    finish_restore_context,
                    diagnostics,
                }),
            })
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .finally(|| unsafe {
            if switched_context.get() && !previous_context.get().is_null() {
                pg_sys::MemoryContextSwitchTo(previous_context.get());
            }
        })
        .execute()
    }
}

unsafe fn restore_current_memory_context(previous_context: pg_sys::MemoryContext) {
    if !previous_context.is_null() {
        pg_sys::MemoryContextSwitchTo(previous_context);
    }
}

fn call_sink_callback<T>(f: impl FnOnce() -> Result<T, SinkError>) -> Result<T, ScanError> {
    PgTryBuilder::new(AssertUnwindSafe(|| f().map_err(ScanError::from)))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()
}

fn best_effort_sink_abort(
    ctx: &mut SlotSinkContext,
    methods: &'static SlotSinkMethods,
    private: *mut c_void,
) {
    if let Some(abort) = methods.abort {
        let _ = call_sink_callback(|| {
            unsafe { abort(ctx, private) };
            Ok(())
        });
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{cursor_fetch_rows, normalize_fetch_batch_rows, DEFAULT_FETCH_BATCH_ROWS};

    #[test]
    fn cursor_fetch_rows_uses_configured_batch_size_and_remaining_budget() {
        assert_eq!(cursor_fetch_rows(1, 1, 0), 0 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(1, 1, 1), 1 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(2, 2, 1), 1 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(2, 7, 7), 2 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(8, 3, 7), 3 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(8, 7, 3), 3 as std::ffi::c_long);
        assert_eq!(
            cursor_fetch_rows(
                DEFAULT_FETCH_BATCH_ROWS - 1,
                DEFAULT_FETCH_BATCH_ROWS,
                DEFAULT_FETCH_BATCH_ROWS
            ),
            (DEFAULT_FETCH_BATCH_ROWS - 1) as std::ffi::c_long
        );
        assert_eq!(
            cursor_fetch_rows(
                DEFAULT_FETCH_BATCH_ROWS,
                DEFAULT_FETCH_BATCH_ROWS,
                DEFAULT_FETCH_BATCH_ROWS
            ),
            DEFAULT_FETCH_BATCH_ROWS as std::ffi::c_long
        );
        assert_eq!(
            cursor_fetch_rows(
                DEFAULT_FETCH_BATCH_ROWS,
                DEFAULT_FETCH_BATCH_ROWS + 1,
                DEFAULT_FETCH_BATCH_ROWS + 1
            ),
            DEFAULT_FETCH_BATCH_ROWS as std::ffi::c_long
        );
        assert_eq!(
            cursor_fetch_rows(DEFAULT_FETCH_BATCH_ROWS, usize::MAX, usize::MAX),
            DEFAULT_FETCH_BATCH_ROWS as std::ffi::c_long
        );
    }

    #[test]
    fn fetch_batch_size_is_normalized_to_at_least_one() {
        assert_eq!(normalize_fetch_batch_rows(0), 1);
        assert_eq!(normalize_fetch_batch_rows(1), 1);
        assert_eq!(normalize_fetch_batch_rows(7), 7);
    }
}
