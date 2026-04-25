use crate::{with_registered_snapshot, BackendServiceError};
use arrow_layout::{init_block, LayoutPlan};
use arrow_schema::SchemaRef;
use pgrx::pg_sys;
use row_estimator::PageRowEstimator;
use scan_flow::{BackendPageSource, FlowId, SourcePageStatus};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use slot_scan::{ExecutionSpiContext, PreparedScan, SlotSinkAction, StreamingScanSession};

pub(crate) struct SlotScanPageSource {
    snapshot: pgrx::pg_sys::Snapshot,
    spi: ExecutionSpiContext,
    prepared: PreparedScan,
    schema: SchemaRef,
    block_size: u32,
    fetch_batch_rows: usize,
    single_row_drains: bool,
    estimator: PageRowEstimator,
    session: Option<StreamingScanSession>,
    overflow_slot: *mut pg_sys::TupleTableSlot,
    pending_overflow: pg_sys::HeapTuple,
}

impl SlotScanPageSource {
    pub(crate) fn new(
        snapshot: pgrx::pg_sys::Snapshot,
        spi: ExecutionSpiContext,
        prepared: PreparedScan,
        schema: SchemaRef,
        block_size: u32,
        fetch_batch_rows: usize,
        estimator: PageRowEstimator,
    ) -> Self {
        let single_row_drains = estimator.has_variable_width();
        Self {
            snapshot,
            spi,
            prepared,
            schema,
            block_size,
            fetch_batch_rows,
            single_row_drains,
            estimator,
            session: None,
            overflow_slot: std::ptr::null_mut(),
            pending_overflow: std::ptr::null_mut(),
        }
    }

    fn fill_next_page_with_snapshot(
        &mut self,
        payload: &mut [u8],
    ) -> Result<SourcePageStatus, BackendServiceError> {
        loop {
            let session = self.session.as_mut().ok_or_else(|| {
                BackendServiceError::PageSource("slot scan page source is not open".into())
            })?;
            let estimate = self.estimator.estimate()?;
            let layout = LayoutPlan::from_arrow_schema(
                self.schema.as_ref(),
                estimate.rows_per_page,
                self.block_size,
            )?;
            let max_rows = usize::try_from(layout.max_rows()).map_err(|_| {
                BackendServiceError::PageSource(format!(
                    "layout max rows {} does not fit into usize",
                    layout.max_rows()
                ))
            })?;
            if max_rows == 0 {
                return Err(BackendServiceError::PageSource(
                    "layout planned zero rows per scan page".into(),
                ));
            }
            init_block(payload, &layout)?;

            let mut encoder = unsafe { PageBatchEncoder::new(session.tuple_desc(), payload) }?;
            let mut rows_written = 0usize;

            if !self.pending_overflow.is_null() {
                match append_pending_overflow(
                    self.overflow_slot,
                    &mut self.pending_overflow,
                    &mut encoder,
                )? {
                    AppendStatus::Appended => {
                        rows_written += 1;
                    }
                    AppendStatus::Full => {
                        self.estimator
                            .observe_empty_full_page(estimate.rows_per_page)?;
                        continue;
                    }
                }
            }

            loop {
                if rows_written >= max_rows {
                    let encoded = encoder.finish()?;
                    self.estimator
                        .observe_encoded_block(&payload[..encoded.payload_len])?;
                    return Ok(SourcePageStatus::Page {
                        payload_len: encoded.payload_len,
                    });
                }

                let remaining_rows = max_rows - rows_written;
                let row_budget = if self.single_row_drains {
                    1
                } else {
                    remaining_rows
                };
                let drain = session.drain_slots::<BackendServiceError>(row_budget, |slot| {
                    match encoder.append_slot(slot)? {
                        AppendStatus::Appended => {
                            rows_written += 1;
                            Ok::<SlotSinkAction, BackendServiceError>(SlotSinkAction::Continue)
                        }
                        AppendStatus::Full => {
                            if row_budget != 1 {
                                return Err(BackendServiceError::PageSource(format!(
                                    "slot encoder filled before exhausting row budget: budget={row_budget}, rows_written={rows_written}, max_rows={max_rows}"
                                )));
                            }
                            self.pending_overflow = unsafe { pg_sys::ExecCopySlotHeapTuple(slot) };
                            if self.pending_overflow.is_null() {
                                return Err(BackendServiceError::PageSource(
                                    "ExecCopySlotHeapTuple returned null".into(),
                                ));
                            }
                            Ok::<SlotSinkAction, BackendServiceError>(SlotSinkAction::Continue)
                        }
                    }
                })?;

                if !self.pending_overflow.is_null() {
                    if rows_written > 0 {
                        let encoded = encoder.finish()?;
                        self.estimator
                            .observe_encoded_block(&payload[..encoded.payload_len])?;
                        return Ok(SourcePageStatus::Page {
                            payload_len: encoded.payload_len,
                        });
                    }

                    self.estimator
                        .observe_empty_full_page(estimate.rows_per_page)?;
                    break;
                }

                if drain.stopped {
                    return Err(BackendServiceError::PageSource(
                        "slot scan page source unexpectedly stopped a direct receiver drain".into(),
                    ));
                }

                if drain.eof {
                    if rows_written == 0 {
                        return Ok(SourcePageStatus::Eof);
                    }

                    let encoded = encoder.finish()?;
                    self.estimator
                        .observe_encoded_block(&payload[..encoded.payload_len])?;
                    return Ok(SourcePageStatus::Page {
                        payload_len: encoded.payload_len,
                    });
                }

                if drain.rows_consumed == 0 {
                    return Err(BackendServiceError::PageSource(
                        "slot scan direct receiver made no progress".into(),
                    ));
                }
            }
        }
    }
}

impl BackendPageSource for SlotScanPageSource {
    type Error = BackendServiceError;

    fn open(&mut self, _flow: FlowId) -> Result<(), Self::Error> {
        let session = with_registered_snapshot(self.snapshot, || {
            self.prepared
                .open_streaming_session_in(&self.spi, self.fetch_batch_rows)
                .map_err(BackendServiceError::PrepareScan)
        })?;
        let overflow_slot = unsafe {
            pg_sys::MakeSingleTupleTableSlot(
                session.tuple_desc(),
                std::ptr::addr_of!(pg_sys::TTSOpsHeapTuple),
            )
        };
        if overflow_slot.is_null() {
            return Err(BackendServiceError::PageSource(
                "MakeSingleTupleTableSlot(TTSOpsHeapTuple) returned null".into(),
            ));
        }
        self.overflow_slot = overflow_slot;
        self.session = Some(session);
        Ok(())
    }

    fn fill_next_page(&mut self, payload: &mut [u8]) -> Result<SourcePageStatus, Self::Error> {
        let block_size = usize::try_from(self.block_size).map_err(|_| {
            BackendServiceError::PageSource(format!(
                "scan block size {} does not fit into usize",
                self.block_size
            ))
        })?;
        if payload.len() < block_size {
            return Err(BackendServiceError::PageSource(format!(
                "scan page payload too small: required {}, got {}",
                block_size,
                payload.len()
            )));
        }
        let block = &mut payload[..block_size];

        with_registered_snapshot(self.snapshot, || self.fill_next_page_with_snapshot(block))
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        if !self.overflow_slot.is_null() {
            unsafe {
                clear_slot(self.overflow_slot);
                pg_sys::ExecDropSingleTupleTableSlot(self.overflow_slot);
            }
            self.overflow_slot = std::ptr::null_mut();
        }
        clear_pending_overflow(&mut self.pending_overflow);
        if let Some(session) = self.session.take() {
            with_registered_snapshot(self.snapshot, || {
                session
                    .close()
                    .map(|_| ())
                    .map_err(BackendServiceError::PrepareScan)
            })?;
        }
        Ok(())
    }
}

impl Drop for SlotScanPageSource {
    fn drop(&mut self) {
        if !self.overflow_slot.is_null() {
            unsafe {
                clear_slot(self.overflow_slot);
                pg_sys::ExecDropSingleTupleTableSlot(self.overflow_slot);
            }
            self.overflow_slot = std::ptr::null_mut();
        }
        clear_pending_overflow(&mut self.pending_overflow);
    }
}

fn append_pending_overflow(
    slot: *mut pg_sys::TupleTableSlot,
    pending: &mut pg_sys::HeapTuple,
    encoder: &mut PageBatchEncoder<'_>,
) -> Result<AppendStatus, BackendServiceError> {
    if slot.is_null() {
        return Err(BackendServiceError::PageSource(
            "pending overflow slot is not initialized".into(),
        ));
    }
    unsafe {
        clear_slot(slot);
        pg_sys::ExecStoreHeapTuple(*pending, slot, false);
    }

    let status = encoder.append_slot(slot)?;
    if status == AppendStatus::Appended {
        unsafe {
            clear_slot(slot);
            pg_sys::heap_freetuple(*pending);
        }
        *pending = std::ptr::null_mut();
    }
    Ok(status)
}

fn clear_pending_overflow(pending: &mut pg_sys::HeapTuple) {
    if pending.is_null() {
        return;
    }
    unsafe {
        pg_sys::heap_freetuple(*pending);
    }
    *pending = std::ptr::null_mut();
}

unsafe fn clear_slot(slot: *mut pg_sys::TupleTableSlot) {
    if slot.is_null() {
        return;
    }
    if let Some(clear) = unsafe { (*(*slot).tts_ops).clear } {
        unsafe {
            clear(slot);
        }
    }
}
