use crate::error::{ScanError, SinkError};
use crate::plan::{inspect_planned_stmt, scan_error_from_caught_error, spi_status_error};
use crate::types::{
    CursorDrainOutcome, CursorRowAction, PreparedScan, PreparedScanCursor, ScanStats, SlotSink,
    SlotSinkAction, SlotSinkContext, SlotSinkMethods,
};
use pgrx::pg_sys;
use pgrx::PgTryBuilder;
use std::cell::Cell;
use std::ffi::{c_void, CStr};
use std::panic::AssertUnwindSafe;

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

struct SpiSessionGuard {
    connected: bool,
}

impl SpiSessionGuard {
    fn connect() -> Result<Self, ScanError> {
        let rc = unsafe { pg_sys::SPI_connect() };
        if rc != pg_sys::SPI_OK_CONNECT as i32 {
            return Err(spi_status_error("SPI_connect", rc));
        }
        Ok(Self { connected: true })
    }

    fn finish(mut self) -> Result<(), ScanError> {
        if !self.connected {
            return Ok(());
        }

        let rc = unsafe { pg_sys::SPI_finish() };
        self.connected = false;
        if rc != pg_sys::SPI_OK_FINISH as i32 {
            return Err(spi_status_error("SPI_finish", rc));
        }
        Ok(())
    }
}

impl Drop for SpiSessionGuard {
    fn drop(&mut self) {
        if self.connected {
            let _ = unsafe { pg_sys::SPI_finish() };
            self.connected = false;
        }
    }
}

struct SpiTupleTableGuard {
    ptr: *mut pg_sys::SPITupleTable,
}

impl SpiTupleTableGuard {
    fn new() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
        }
    }

    fn set(&mut self, ptr: *mut pg_sys::SPITupleTable) {
        self.release();
        self.ptr = ptr;
    }

    fn release(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                pg_sys::SPI_freetuptable(self.ptr);
            }
        }
        self.ptr = std::ptr::null_mut();
    }
}

impl Drop for SpiTupleTableGuard {
    fn drop(&mut self) {
        self.release();
    }
}

enum BatchDrainOutcome {
    Continue,
    Return(CursorDrainOutcome),
}

unsafe fn with_memory_context<R, F: FnOnce() -> R>(context: pg_sys::MemoryContext, f: F) -> R {
    let previous = pg_sys::MemoryContextSwitchTo(context);
    let result = f();
    pg_sys::MemoryContextSwitchTo(previous);
    result
}

impl PreparedScan {
    /// Opens an incremental read-only cursor for the prepared trusted scan.
    ///
    /// The caller must keep a PostgreSQL snapshot active when opening and
    /// driving the cursor. The returned cursor owns one revalidated portal and
    /// reusable slots, but it does not keep a SPI connection open between
    /// drain steps.
    pub fn open_cursor(&self) -> Result<PreparedScanCursor, ScanError> {
        let memory_context = Cell::new(std::ptr::null_mut());
        let portal = Cell::new(std::ptr::null_mut());
        let slot = Cell::new(std::ptr::null_mut());
        let fetch_slot = Cell::new(std::ptr::null_mut());
        let tuple_desc_copy = Cell::new(std::ptr::null_mut());
        let connected = Cell::new(false);
        let success = Cell::new(false);

        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            let parent_context = if pg_sys::TopTransactionContext.is_null() {
                pg_sys::CurrentMemoryContext
            } else {
                pg_sys::TopTransactionContext
            };
            memory_context.set(pg_sys::AllocSetContextCreateExtended(
                parent_context,
                c"slot_scan cursor".as_ptr(),
                pg_sys::ALLOCSET_SMALL_MINSIZE as usize,
                pg_sys::ALLOCSET_SMALL_INITSIZE as usize,
                pg_sys::ALLOCSET_SMALL_MAXSIZE as usize,
            ));
            if memory_context.get().is_null() {
                return Err(ScanError::Postgres(
                    "failed to allocate slot_scan cursor memory context".into(),
                ));
            }

            let connect_rc = pg_sys::SPI_connect();
            if connect_rc != pg_sys::SPI_OK_CONNECT as i32 {
                return Err(spi_status_error("SPI_connect", connect_rc));
            }
            connected.set(true);

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

            let portal_name_ptr = (*portal.get()).name;
            if portal_name_ptr.is_null() {
                return Err(ScanError::Postgres(
                    "cursor portal is missing a portal name".into(),
                ));
            }
            let portal_name = CStr::from_ptr(portal_name_ptr).to_owned();

            with_memory_context(memory_context.get(), || {
                tuple_desc_copy.set(pg_sys::CreateTupleDescCopy(tuple_desc));
                if tuple_desc_copy.get().is_null() {
                    return Err(ScanError::MissingTupleDesc);
                }

                slot.set(pg_sys::MakeSingleTupleTableSlot(
                    tuple_desc_copy.get(),
                    std::ptr::addr_of!(pg_sys::TTSOpsMinimalTuple),
                ));
                if slot.get().is_null() {
                    return Err(ScanError::MissingTupleDesc);
                }

                fetch_slot.set(pg_sys::MakeSingleTupleTableSlot(
                    tuple_desc_copy.get(),
                    std::ptr::addr_of!(pg_sys::TTSOpsHeapTuple),
                ));
                if fetch_slot.get().is_null() {
                    return Err(ScanError::MissingTupleDesc);
                }

                Ok::<(), ScanError>(())
            })?;

            let finish_rc = pg_sys::SPI_finish();
            connected.set(false);
            if finish_rc != pg_sys::SPI_OK_FINISH as i32 {
                return Err(spi_status_error("SPI_finish", finish_rc));
            }

            success.set(true);
            Ok(PreparedScanCursor {
                prepared: self.clone(),
                portal_name,
                memory_context: memory_context.get(),
                slot: slot.get(),
                fetch_slot: fetch_slot.get(),
                current_tuple: std::ptr::null_mut(),
                spill_tuple: std::ptr::null_mut(),
                tuple_desc: tuple_desc_copy.get(),
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
                if !fetch_slot.get().is_null() {
                    clear_slot(fetch_slot.get());
                    pg_sys::ExecDropSingleTupleTableSlot(fetch_slot.get());
                    fetch_slot.set(std::ptr::null_mut());
                }
                if !portal.get().is_null() {
                    pg_sys::SPI_cursor_close(portal.get());
                    portal.set(std::ptr::null_mut());
                }
                if connected.get() {
                    let _ = pg_sys::SPI_finish();
                    connected.set(false);
                }
                if !memory_context.get().is_null() {
                    pg_sys::MemoryContextDelete(memory_context.get());
                    memory_context.set(std::ptr::null_mut());
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
        let mut cursor = match self.open_cursor() {
            Ok(cursor) => cursor,
            Err(err) => {
                best_effort_sink_abort(&mut ctx, sink.methods, sink.private);
                return Err(err);
            }
        };

        ctx.set_runtime_metadata(
            cursor.parallel_capable(),
            cursor.planned_workers(),
            cursor.plan_kind(),
        );
        let mut abort_guard = SinkAbortGuard::new(&mut ctx, sink.methods, sink.private);

        let result = (|| -> Result<ScanStats, ScanError> {
            if let Some(init) = sink.methods.init {
                call_sink_callback(|| unsafe {
                    init(&mut ctx, sink.private, cursor.tuple_desc())
                })?;
            }

            loop {
                match cursor.drain_rows::<ScanError, _>(|slot| {
                    let action = call_sink_callback(|| unsafe {
                        (sink.methods.consume_slot)(&mut ctx, sink.private, slot)
                    })?;
                    ctx.bump_rows();
                    Ok::<CursorRowAction, ScanError>(match action {
                        SlotSinkAction::Continue => CursorRowAction::Continue,
                        SlotSinkAction::Stop => CursorRowAction::Stop,
                    })
                })? {
                    CursorDrainOutcome::Eof | CursorDrainOutcome::Stopped => break,
                }
            }

            if let Some(finish) = sink.methods.finish {
                call_sink_callback(|| unsafe { finish(&mut ctx, sink.private) })?;
            }

            let stats = cursor.close()?;
            abort_guard.disarm();
            Ok(stats)
        })();

        result
    }
}

impl PreparedScanCursor {
    /// Drains rows from the cursor's portal until EOF or until `visitor`
    /// requests an early stop.
    ///
    /// The slot pointer passed to `visitor` is valid only for the duration of
    /// that callback. `drain_rows()` always finishes any SPI session it opens
    /// before returning to the caller.
    pub fn drain_rows<E, F>(&mut self, mut visitor: F) -> Result<CursorDrainOutcome, E>
    where
        E: From<ScanError>,
        F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<CursorRowAction, E>,
    {
        if self.closed {
            return Err(E::from(ScanError::CursorClosed));
        }

        if self.remaining == 0 {
            self.clear_spill_tuple();
            return Ok(CursorDrainOutcome::Eof);
        }

        if let Some(outcome) = self.replay_spill_row(&mut visitor)? {
            return Ok(outcome);
        }

        let spi = SpiSessionGuard::connect().map_err(E::from)?;
        loop {
            let batch_outcome = {
                let mut batch = SpiTupleTableGuard::new();
                // `Stop` means "end this drain step, but keep the cursor open".
                // Fetching more than one row here would advance the portal past
                // undelivered rows unless we also buffered the rest of the
                // batch inside the cursor. Keep the SPI scope short-lived, but
                // fetch one row at a time within that scope.
                let processed = self.fetch_next_batch(1, &mut batch).map_err(E::from)?;
                if processed == 0 {
                    BatchDrainOutcome::Return(CursorDrainOutcome::Eof)
                } else {
                    self.drain_batch_rows(processed, &batch, &mut visitor)?
                }
            };

            match batch_outcome {
                BatchDrainOutcome::Continue => continue,
                BatchDrainOutcome::Return(outcome) => {
                    spi.finish().map_err(E::from)?;
                    return Ok(outcome);
                }
            }
        }
    }

    /// Closes the cursor, releases PostgreSQL resources, and returns the final
    /// run-time scan statistics observed by this cursor.
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

    fn replay_spill_row<E, F>(&mut self, visitor: &mut F) -> Result<Option<CursorDrainOutcome>, E>
    where
        E: From<ScanError>,
        F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<CursorRowAction, E>,
    {
        if self.spill_tuple.is_null() {
            return Ok(None);
        }

        let slot = self.load_spill_slot().map_err(E::from)?;
        let action = visitor(slot)?;
        self.finish_row_visit(action, slot, true)
    }

    fn drain_batch_rows<E, F>(
        &mut self,
        processed: usize,
        batch: &SpiTupleTableGuard,
        visitor: &mut F,
    ) -> Result<BatchDrainOutcome, E>
    where
        E: From<ScanError>,
        F: FnMut(*mut pg_sys::TupleTableSlot) -> Result<CursorRowAction, E>,
    {
        for index in 0..processed {
            let tuple = unsafe { *(*batch.ptr).vals.add(index) };
            self.prepare_current_row(tuple).map_err(E::from)?;
            let action = visitor(self.slot)?;
            if let Some(outcome) = self.finish_row_visit(action, self.slot, false)? {
                unsafe {
                    clear_slot(self.fetch_slot);
                }
                return Ok(BatchDrainOutcome::Return(outcome));
            }
        }

        unsafe {
            clear_slot(self.fetch_slot);
        }
        Ok(BatchDrainOutcome::Continue)
    }

    fn finish_row_visit<E>(
        &mut self,
        action: CursorRowAction,
        slot: *mut pg_sys::TupleTableSlot,
        replaying_spill: bool,
    ) -> Result<Option<CursorDrainOutcome>, E>
    where
        E: From<ScanError>,
    {
        match action {
            CursorRowAction::Continue => {
                if replaying_spill {
                    self.clear_spill_tuple();
                } else {
                    self.clear_current_tuple();
                }
                self.note_row_consumed();
                if self.remaining == 0 {
                    return Ok(Some(CursorDrainOutcome::Eof));
                }
                Ok(None)
            }
            CursorRowAction::Stop => {
                if replaying_spill {
                    self.clear_spill_tuple();
                } else {
                    self.clear_current_tuple();
                }
                self.note_row_consumed();
                Ok(Some(CursorDrainOutcome::Stopped))
            }
            CursorRowAction::ReplayCurrentAndStop => {
                if !replaying_spill {
                    self.move_current_tuple_to_spill();
                } else {
                    unsafe {
                        clear_slot(slot);
                    }
                }
                Ok(Some(CursorDrainOutcome::Stopped))
            }
        }
    }

    fn note_row_consumed(&mut self) {
        self.rows_seen += 1;
        self.remaining = self.remaining.saturating_sub(1);
    }

    fn fetch_next_batch(
        &self,
        fetch_rows: usize,
        batch: &mut SpiTupleTableGuard,
    ) -> Result<usize, ScanError> {
        let processed = Cell::new(0usize);
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            let portal = self.lookup_portal()?;
            pg_sys::SPI_cursor_fetch(portal, true, fetch_rows as std::ffi::c_long);
            batch.set(pg_sys::SPI_tuptable);
            processed.set(pg_sys::SPI_processed as usize);
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()?;
        Ok(processed.get())
    }

    fn load_spill_slot(&mut self) -> Result<*mut pg_sys::TupleTableSlot, ScanError> {
        let spill_tuple = self.spill_tuple;
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            clear_slot(self.slot);
            pg_sys::ExecStoreMinimalTuple(spill_tuple, self.slot, false);
            Ok(self.slot)
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()
    }

    fn prepare_current_row(&mut self, tuple: pg_sys::HeapTuple) -> Result<(), ScanError> {
        PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            clear_slot(self.fetch_slot);
            pg_sys::ExecStoreHeapTuple(tuple, self.fetch_slot, false);
            self.clear_current_tuple();
            let copied = with_memory_context(self.memory_context, || {
                pg_sys::ExecCopySlotMinimalTuple(self.fetch_slot)
            });
            if copied.is_null() {
                return Err(ScanError::Postgres(
                    "ExecCopySlotMinimalTuple returned null".into(),
                ));
            }
            self.current_tuple = copied;
            clear_slot(self.slot);
            pg_sys::ExecStoreMinimalTuple(self.current_tuple, self.slot, false);
            Ok(())
        }))
        .catch_others(|error| Err(scan_error_from_caught_error(error)))
        .execute()
    }

    fn move_current_tuple_to_spill(&mut self) {
        unsafe {
            clear_slot(self.slot);
        }
        self.clear_spill_tuple();
        self.spill_tuple = self.current_tuple;
        self.current_tuple = std::ptr::null_mut();
    }

    fn clear_current_tuple(&mut self) {
        unsafe {
            clear_slot(self.slot);
            if !self.current_tuple.is_null() {
                pg_sys::heap_free_minimal_tuple(self.current_tuple);
            }
        }
        self.current_tuple = std::ptr::null_mut();
    }

    fn clear_spill_tuple(&mut self) {
        unsafe {
            clear_slot(self.slot);
            if !self.spill_tuple.is_null() {
                pg_sys::heap_free_minimal_tuple(self.spill_tuple);
            }
        }
        self.spill_tuple = std::ptr::null_mut();
    }

    fn close_inner(&mut self) -> Result<(), ScanError> {
        if self.closed {
            return Ok(());
        }

        let slot = std::mem::replace(&mut self.slot, std::ptr::null_mut());
        let fetch_slot = std::mem::replace(&mut self.fetch_slot, std::ptr::null_mut());
        let current_tuple = std::mem::replace(&mut self.current_tuple, std::ptr::null_mut());
        let spill_tuple = std::mem::replace(&mut self.spill_tuple, std::ptr::null_mut());
        self.tuple_desc = std::ptr::null_mut();
        let memory_context = std::mem::replace(&mut self.memory_context, std::ptr::null_mut());

        let result = PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
            if !slot.is_null() {
                clear_slot(slot);
                pg_sys::ExecDropSingleTupleTableSlot(slot);
            }
            if !fetch_slot.is_null() {
                clear_slot(fetch_slot);
                pg_sys::ExecDropSingleTupleTableSlot(fetch_slot);
            }
            if !current_tuple.is_null() {
                pg_sys::heap_free_minimal_tuple(current_tuple);
            }
            if !spill_tuple.is_null() {
                pg_sys::heap_free_minimal_tuple(spill_tuple);
            }
            if !self.portal_name.as_c_str().to_bytes().is_empty() {
                let spi = SpiSessionGuard::connect()?;
                if let Ok(portal) = self.lookup_portal() {
                    pg_sys::SPI_cursor_close(portal);
                }
                spi.finish()?;
            }
            if !memory_context.is_null() {
                pg_sys::MemoryContextDelete(memory_context);
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

    fn lookup_portal(&self) -> Result<pg_sys::Portal, ScanError> {
        let portal = unsafe { pg_sys::SPI_cursor_find(self.portal_name.as_ptr()) };
        if portal.is_null() {
            return Err(ScanError::Postgres(format!(
                "cursor portal {} is no longer available",
                self.portal_name.to_string_lossy()
            )));
        }
        Ok(portal)
    }
}

impl Drop for PreparedScanCursor {
    fn drop(&mut self) {
        self.best_effort_close();
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
