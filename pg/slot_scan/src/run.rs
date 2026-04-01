use crate::error::ScanError;
use crate::plan::{inspect_planned_stmt, scan_error_from_caught_error, spi_status_error, with_spi};
use crate::types::{PreparedScan, ScanStats, SlotSink, SlotSinkAction, SlotSinkContext};
use pgrx::pg_sys;
use pgrx::PgTryBuilder;
use std::cell::Cell;
use std::panic::AssertUnwindSafe;

const FETCH_BATCH_ROWS: usize = 1024;

impl PreparedScan {
    /// Executes the prepared trusted scan and feeds every row into the
    /// provided sink.
    ///
    /// `run()` opens a read-only SPI cursor, revalidates the saved plan, and
    /// initializes sink metadata from the current portal. The slot passed to
    /// `consume_slot` is reused across rows and must not be retained.
    ///
    /// The SQL attached to this [`PreparedScan`] is expected to be trusted
    /// compiler-generated scan SQL. `slot_scan` revalidates the current portal
    /// plan shape, but it does not attempt to sandbox arbitrary expression
    /// semantics.
    pub fn run(&self, sink: SlotSink<'_>) -> Result<ScanStats, ScanError> {
        let mut ctx = SlotSinkContext::new();
        if let Err(err) = with_spi(|| unsafe { self.run_via_spi_cursor(&mut ctx, &sink) }) {
            if let Some(abort) = sink.methods.abort {
                unsafe { abort(&mut ctx, sink.private) };
            }
            Err(err)
        } else {
            Ok(ScanStats {
                rows_seen: ctx.rows_seen(),
                hit_local_row_cap: self
                    .options
                    .local_row_cap
                    .is_some_and(|cap| ctx.rows_seen() >= cap),
                parallel_capable: ctx.parallel_capable(),
                planned_workers: ctx.planned_workers(),
            })
        }
    }

    unsafe fn run_via_spi_cursor(
        &self,
        ctx: &mut SlotSinkContext,
        sink: &SlotSink<'_>,
    ) -> Result<(), ScanError> {
        let portal = Cell::new(std::ptr::null_mut());
        let slot = Cell::new(std::ptr::null_mut());

        PgTryBuilder::new(AssertUnwindSafe(|| {
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

            ctx.set_runtime_metadata(metadata.parallel_capable, metadata.planned_workers);

            if let Some(init) = sink.methods.init {
                init(ctx, sink.private, tuple_desc)?;
            }

            slot.set(pg_sys::MakeSingleTupleTableSlot(
                tuple_desc,
                std::ptr::addr_of!(pg_sys::TTSOpsHeapTuple),
            ));
            if slot.get().is_null() {
                return Err(ScanError::MissingTupleDesc);
            }

            let mut stop = false;
            let mut remaining = self.options.local_row_cap.unwrap_or(usize::MAX);

            while !stop && remaining > 0 {
                let fetch_rows = remaining.min(FETCH_BATCH_ROWS) as std::ffi::c_long;
                pg_sys::SPI_cursor_fetch(portal.get(), true, fetch_rows);

                let tuptable = pg_sys::SPI_tuptable;
                let processed = pg_sys::SPI_processed as usize;

                if processed == 0 {
                    if !tuptable.is_null() {
                        pg_sys::SPI_freetuptable(tuptable);
                    }
                    break;
                }

                for idx in 0..processed {
                    let tuple = *(*tuptable).vals.add(idx);
                    if let Some(clear) = (*(*slot.get()).tts_ops).clear {
                        clear(slot.get());
                    }
                    pg_sys::ExecStoreHeapTuple(tuple, slot.get(), false);

                    let action = (sink.methods.consume_slot)(ctx, sink.private, slot.get())?;
                    ctx.bump_rows();
                    remaining = remaining.saturating_sub(1);

                    let reached_cap = self
                        .options
                        .local_row_cap
                        .is_some_and(|cap| ctx.rows_seen() >= cap);
                    if reached_cap || matches!(action, SlotSinkAction::Stop) {
                        stop = true;
                        break;
                    }
                }

                pg_sys::SPI_freetuptable(tuptable);
            }

            if let Some(finish) = sink.methods.finish {
                finish(ctx, sink.private)?;
            }

            Ok(())
        }))
        .catch_others(|e| Err(scan_error_from_caught_error(e)))
        .finally(|| unsafe {
            if !slot.get().is_null() {
                if let Some(clear) = (*(*slot.get()).tts_ops).clear {
                    clear(slot.get());
                }
                pg_sys::ExecDropSingleTupleTableSlot(slot.get());
            }
            if !portal.get().is_null() {
                pg_sys::SPI_cursor_close(portal.get());
            }
        })
        .execute()
    }
}
