use crate::error::{ScanError, SinkError};
use crate::plan::{inspect_planned_stmt, scan_error_from_caught_error, spi_status_error};
use crate::types::{
    ExecutionSpiConnection, ExecutionSpiContext, PreparedScan, ScanStats, SlotSink, SlotSinkAction,
    SlotSinkContext, SlotSinkMethods, StreamingScanSession,
};
use pgrx::pg_sys;
use pgrx::PgTryBuilder;
use std::cell::Cell;
use std::ffi::c_void;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

const DEFAULT_FETCH_BATCH_ROWS: usize = 1024;

fn normalize_fetch_batch_rows(fetch_batch_rows: usize) -> usize {
    fetch_batch_rows.max(1)
}

fn cursor_fetch_rows(fetch_batch_rows: usize, remaining: usize) -> std::ffi::c_long {
    fetch_batch_rows.min(remaining).try_into().unwrap()
}

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
        let spi = ExecutionSpiContext::connect()?;
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
        let slot = Cell::new(std::ptr::null_mut());
        let success = Cell::new(false);

        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
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

            slot.set(pg_sys::MakeSingleTupleTableSlot(
                tuple_desc,
                std::ptr::addr_of!(pg_sys::TTSOpsHeapTuple),
            ));
            if slot.get().is_null() {
                return Err(ScanError::MissingTupleDesc);
            }

            success.set(true);
            Ok(StreamingScanSession {
                prepared: self.clone(),
                _spi: spi.clone(),
                portal: portal.get(),
                slot: slot.get(),
                fetch_batch_rows,
                batch: std::ptr::null_mut(),
                batch_processed: 0,
                batch_index: 0,
                row_loaded: false,
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
            if !success.get() {
                if !slot.get().is_null() {
                    clear_slot(slot.get());
                    pg_sys::ExecDropSingleTupleTableSlot(slot.get());
                    slot.set(std::ptr::null_mut());
                }
                if !portal.get().is_null() {
                    pg_sys::SPI_cursor_close(portal.get());
                    portal.set(std::ptr::null_mut());
                }
            }
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

        let result = (|| -> Result<ScanStats, ScanError> {
            if let Some(init) = sink.methods.init {
                call_sink_callback(|| unsafe {
                    init(&mut ctx, sink.private, session.tuple_desc())
                })?;
            }

            while let Some(slot) = session.current_slot()? {
                let action = call_sink_callback(|| unsafe {
                    (sink.methods.consume_slot)(&mut ctx, sink.private, slot)
                })?;
                session.consume_current_row()?;
                ctx.bump_rows();
                if action == SlotSinkAction::Stop {
                    break;
                }
            }

            if let Some(finish) = sink.methods.finish {
                call_sink_callback(|| unsafe { finish(&mut ctx, sink.private) })?;
            }

            let stats = session.close()?;
            abort_guard.disarm();
            Ok(stats)
        })();

        result
    }
}

impl StreamingScanSession {
    #[doc(hidden)]
    pub fn current_slot(&mut self) -> Result<Option<*mut pg_sys::TupleTableSlot>, ScanError> {
        if self.closed {
            return Err(ScanError::CursorClosed);
        }

        if self.remaining == 0 {
            self.release_current_row();
            self.release_batch();
            return Ok(None);
        }

        if self.row_loaded {
            return Ok(Some(self.slot));
        }

        loop {
            if self.batch_index < self.batch_processed {
                self.load_current_row()?;
                return Ok(Some(self.slot));
            }

            self.release_batch();
            self.fetch_next_batch()?;
            if self.batch_processed == 0 {
                return Ok(None);
            }
        }
    }

    #[doc(hidden)]
    pub fn consume_current_row(&mut self) -> Result<(), ScanError> {
        if self.closed {
            return Err(ScanError::CursorClosed);
        }
        if !self.row_loaded {
            return Err(ScanError::Postgres(
                "attempted to consume a scan row when no row was loaded".into(),
            ));
        }

        self.release_current_row();
        self.rows_seen += 1;
        self.remaining = self.remaining.saturating_sub(1);
        self.batch_index += 1;
        if self.batch_index >= self.batch_processed {
            self.release_batch();
        }
        Ok(())
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

    fn fetch_next_batch(&mut self) -> Result<(), ScanError> {
        let fetch_rows = cursor_fetch_rows(self.fetch_batch_rows, self.remaining);
        if fetch_rows == 0 {
            self.batch = std::ptr::null_mut();
            self.batch_processed = 0;
            self.batch_index = 0;
            self.row_loaded = false;
            return Ok(());
        }
        let processed = Cell::new(0usize);
        let batch_ptr = Cell::new(std::ptr::null_mut());
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            pg_sys::SPI_cursor_fetch(self.portal, true, fetch_rows);
            batch_ptr.set(pg_sys::SPI_tuptable);
            processed.set(pg_sys::SPI_processed as usize);
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()?;
        self.batch = batch_ptr.get();
        self.batch_processed = processed.get();
        self.batch_index = 0;
        self.row_loaded = false;
        Ok(())
    }

    fn load_current_row(&mut self) -> Result<(), ScanError> {
        debug_assert!(self.batch_index < self.batch_processed);
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            let tuple = *(*self.batch).vals.add(self.batch_index);
            clear_slot(self.slot);
            pg_sys::ExecStoreHeapTuple(tuple, self.slot, false);
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()?;
        self.row_loaded = true;
        Ok(())
    }

    fn release_current_row(&mut self) {
        unsafe {
            clear_slot(self.slot);
        }
        self.row_loaded = false;
    }

    fn release_batch(&mut self) {
        if !self.batch.is_null() {
            unsafe {
                pg_sys::SPI_freetuptable(self.batch);
            }
        }
        self.batch = std::ptr::null_mut();
        self.batch_processed = 0;
        self.batch_index = 0;
    }

    fn close_inner(&mut self) -> Result<(), ScanError> {
        if self.closed {
            return Ok(());
        }

        let portal = std::mem::replace(&mut self.portal, std::ptr::null_mut());
        let slot = std::mem::replace(&mut self.slot, std::ptr::null_mut());
        let batch = std::mem::replace(&mut self.batch, std::ptr::null_mut());
        self.batch_processed = 0;
        self.batch_index = 0;
        self.row_loaded = false;
        self.tuple_desc = std::ptr::null_mut();

        let result = PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            if !slot.is_null() {
                clear_slot(slot);
            }
            if !batch.is_null() {
                pg_sys::SPI_freetuptable(batch);
            }
            if !slot.is_null() {
                pg_sys::ExecDropSingleTupleTableSlot(slot);
            }
            if !portal.is_null() {
                pg_sys::SPI_cursor_close(portal);
            }
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
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
    pub fn connect() -> Result<Self, ScanError> {
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            let connect_rc = pg_sys::SPI_connect();
            if connect_rc != pg_sys::SPI_OK_CONNECT as i32 {
                return Err(spi_status_error("SPI_connect", connect_rc));
            }
            Ok(Self {
                _inner: Rc::new(ExecutionSpiConnection),
            })
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()
    }
}

unsafe fn clear_slot(slot: *mut pg_sys::TupleTableSlot) {
    if slot.is_null() {
        return;
    }
    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
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

#[cfg(test)]
mod tests {
    use super::{cursor_fetch_rows, normalize_fetch_batch_rows, DEFAULT_FETCH_BATCH_ROWS};

    #[test]
    fn cursor_fetch_rows_uses_configured_batch_size_and_remaining_budget() {
        assert_eq!(cursor_fetch_rows(1, 0), 0 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(1, 1), 1 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(2, 1), 1 as std::ffi::c_long);
        assert_eq!(cursor_fetch_rows(2, 7), 2 as std::ffi::c_long);
        assert_eq!(
            cursor_fetch_rows(DEFAULT_FETCH_BATCH_ROWS - 1, DEFAULT_FETCH_BATCH_ROWS),
            (DEFAULT_FETCH_BATCH_ROWS - 1) as std::ffi::c_long
        );
        assert_eq!(
            cursor_fetch_rows(DEFAULT_FETCH_BATCH_ROWS, DEFAULT_FETCH_BATCH_ROWS),
            DEFAULT_FETCH_BATCH_ROWS as std::ffi::c_long
        );
        assert_eq!(
            cursor_fetch_rows(DEFAULT_FETCH_BATCH_ROWS, DEFAULT_FETCH_BATCH_ROWS + 1),
            DEFAULT_FETCH_BATCH_ROWS as std::ffi::c_long
        );
        assert_eq!(
            cursor_fetch_rows(DEFAULT_FETCH_BATCH_ROWS, usize::MAX),
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
