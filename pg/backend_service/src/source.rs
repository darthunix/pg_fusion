use crate::{with_registered_snapshot, BackendServiceError};
use arrow_layout::{init_block, LayoutPlan};
use arrow_schema::SchemaRef;
use pgrx::pg_sys;
use row_estimator::PageRowEstimator;
use runtime_metrics::{MetricId, RuntimeMetrics};
use scan_flow::{BackendPageSource, FlowId, SourcePageStatus};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use slot_scan::{ExecutionSpiContext, PreparedScan, SlotSinkAction, StreamingScanSession};

pub(crate) struct SlotScanPageSource {
    snapshot: pgrx::pg_sys::Snapshot,
    spi: ExecutionSpiContext,
    prepared: PreparedScan,
    schema: SchemaRef,
    source_projection: Vec<usize>,
    block_size: u32,
    fetch_batch_rows: usize,
    single_row_drains: bool,
    estimator: PageRowEstimator,
    metrics: RuntimeMetrics,
    scan_timing_detail: bool,
    session: Option<StreamingScanSession>,
    overflow_slot: *mut pg_sys::TupleTableSlot,
    pending_overflow: pg_sys::HeapTuple,
}

#[derive(Default)]
struct ScanFillTiming {
    page_prepare_ns: u64,
    page_finish_ns: u64,
    page_snapshot_ns: u64,
    slot_drain_ns: u64,
    page_retry_ns: u64,
    fill_pre_drain_ns: u64,
    fill_post_drain_ns: u64,
    fill_overflow_encode_ns: u64,
    fill_emit_ns: u64,
}

impl ScanFillTiming {
    fn record_metrics(&self, metrics: RuntimeMetrics, fill_ns: u64) {
        metrics.add(MetricId::ScanFillPreDrainNs, self.fill_pre_drain_ns);
        metrics.add(MetricId::ScanFillPostDrainNs, self.fill_post_drain_ns);
        metrics.add(
            MetricId::ScanFillOverflowEncodeNs,
            self.fill_overflow_encode_ns,
        );
        metrics.add(MetricId::ScanFillEmitNs, self.fill_emit_ns);

        let classified_ns = self
            .page_snapshot_ns
            .saturating_add(self.page_prepare_ns)
            .saturating_add(self.page_finish_ns)
            .saturating_add(self.slot_drain_ns)
            .saturating_add(self.page_retry_ns)
            .saturating_add(self.fill_pre_drain_ns)
            .saturating_add(self.fill_post_drain_ns)
            .saturating_add(self.fill_overflow_encode_ns)
            .saturating_add(self.fill_emit_ns);
        metrics.add(
            MetricId::ScanFillUnclassifiedNs,
            fill_ns.saturating_sub(classified_ns),
        );
    }
}

impl SlotScanPageSource {
    pub(crate) fn new(
        snapshot: pgrx::pg_sys::Snapshot,
        spi: ExecutionSpiContext,
        prepared: PreparedScan,
        schema: SchemaRef,
        source_projection: Vec<usize>,
        block_size: u32,
        fetch_batch_rows: usize,
        estimator: PageRowEstimator,
        metrics: RuntimeMetrics,
        scan_timing_detail: bool,
    ) -> Self {
        let single_row_drains = estimator.has_variable_width();
        Self {
            snapshot,
            spi,
            prepared,
            schema,
            source_projection,
            block_size,
            fetch_batch_rows,
            single_row_drains,
            estimator,
            metrics,
            scan_timing_detail,
            session: None,
            overflow_slot: std::ptr::null_mut(),
            pending_overflow: std::ptr::null_mut(),
        }
    }

    fn fill_next_page_with_snapshot(
        &mut self,
        payload: &mut [u8],
        timing: &mut ScanFillTiming,
    ) -> Result<SourcePageStatus, BackendServiceError> {
        loop {
            let metrics = self.metrics;
            let retry_start = self.scan_timing_detail.then(|| metrics.now_ns());
            let session = self.session.as_mut().ok_or_else(|| {
                BackendServiceError::PageSource("slot scan page source is not open".into())
            })?;
            let prepare_start = self.metrics.now_ns();
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

            let mut encoder = unsafe {
                PageBatchEncoder::new_projected(
                    session.tuple_desc(),
                    &self.source_projection,
                    payload,
                )
            }?;
            let page_prepare_ns = self.metrics.now_ns().saturating_sub(prepare_start);
            let mut rows_written = 0usize;
            let mut pending_overflow_encode_ns = 0_u64;

            if !self.pending_overflow.is_null() {
                let overflow_encode_start = self.scan_timing_detail.then(|| self.metrics.now_ns());
                let overflow_status = append_pending_overflow(
                    self.overflow_slot,
                    &mut self.pending_overflow,
                    &mut encoder,
                )?;
                if let Some(start) = overflow_encode_start {
                    let elapsed_ns = self.metrics.now_ns().saturating_sub(start);
                    self.metrics.add(MetricId::ScanArrowEncodeNs, elapsed_ns);
                    pending_overflow_encode_ns = elapsed_ns;
                }
                match overflow_status {
                    AppendStatus::Appended => {
                        rows_written += 1;
                        timing.fill_overflow_encode_ns = timing
                            .fill_overflow_encode_ns
                            .saturating_add(pending_overflow_encode_ns);
                    }
                    AppendStatus::Full => {
                        timing.page_retry_ns = timing
                            .page_retry_ns
                            .saturating_add(record_page_retry(metrics, retry_start));
                        self.estimator
                            .observe_empty_full_page(estimate.rows_per_page)?;
                        continue;
                    }
                }
            }

            loop {
                if rows_written >= max_rows {
                    let finish_start = metrics.now_ns();
                    let encoded = encoder.finish()?;
                    self.estimator
                        .observe_encoded_block(&payload[..encoded.payload_len])?;
                    let page_finish_ns = metrics.now_ns().saturating_sub(finish_start);
                    return Ok(record_finished_scan_page(
                        metrics,
                        self.scan_timing_detail,
                        page_prepare_ns,
                        page_finish_ns,
                        MetricId::ScanFullPagesTotal,
                        encoded.row_count,
                        encoded.payload_len,
                        timing,
                    ));
                }

                let pre_drain_start = self.scan_timing_detail.then(|| metrics.now_ns());
                let remaining_rows = max_rows - rows_written;
                let row_budget = if self.single_row_drains {
                    1
                } else {
                    remaining_rows
                };
                // SAFETY: this backend-only callback is controlled by pg_fusion
                // and returns expected failures through Result. A panic here is
                // a bug, not a recoverable row-level PostgreSQL error.
                let mut append_slot = |slot| match encoder.append_slot(slot)? {
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
                        let overflow_copy_start = self.scan_timing_detail.then(|| metrics.now_ns());
                        self.pending_overflow = unsafe { pg_sys::ExecCopySlotHeapTuple(slot) };
                        if let Some(start) = overflow_copy_start {
                            let elapsed_ns = metrics.now_ns().saturating_sub(start);
                            metrics.add(MetricId::ScanOverflowCopyNs, elapsed_ns);
                        }
                        if self.pending_overflow.is_null() {
                            return Err(BackendServiceError::PageSource(
                                "ExecCopySlotHeapTuple returned null".into(),
                            ));
                        }
                        Ok::<SlotSinkAction, BackendServiceError>(SlotSinkAction::Continue)
                    }
                };
                let mut attempt_pre_drain_ns = 0_u64;
                if let Some(start) = pre_drain_start {
                    attempt_pre_drain_ns = metrics.now_ns().saturating_sub(start);
                    timing.fill_pre_drain_ns = timing
                        .fill_pre_drain_ns
                        .saturating_add(attempt_pre_drain_ns);
                }
                let drain_start = self.scan_timing_detail.then(|| metrics.now_ns());
                let drain_result = unsafe {
                    if self.scan_timing_detail {
                        session.drain_slots_without_unwind_guard_profiled::<BackendServiceError>(
                            row_budget,
                            &mut append_slot,
                        )
                    } else {
                        session.drain_slots_without_unwind_guard::<BackendServiceError>(
                            row_budget,
                            &mut append_slot,
                        )
                    }
                };
                drop(append_slot);
                let mut attempt_slot_drain_ns = 0_u64;
                if let Some(start) = drain_start {
                    attempt_slot_drain_ns = metrics.now_ns().saturating_sub(start);
                    metrics.add(MetricId::ScanSlotDrainNs, attempt_slot_drain_ns);
                    timing.slot_drain_ns =
                        timing.slot_drain_ns.saturating_add(attempt_slot_drain_ns);
                }
                let post_drain_start = self.scan_timing_detail.then(|| metrics.now_ns());
                let drain = drain_result?;
                self.metrics.increment(MetricId::ScanFetchCallsTotal);
                if self.scan_timing_detail {
                    self.metrics
                        .add(MetricId::ScanArrowEncodeNs, drain.callback_ns);
                    self.metrics.add(
                        MetricId::ScanPostgresReadNs,
                        drain.elapsed_ns.saturating_sub(drain.callback_ns),
                    );
                }
                let has_pending_overflow = !self.pending_overflow.is_null();
                let drain_stopped = drain.stopped;
                let drain_eof = drain.eof;
                let drain_rows_consumed = drain.rows_consumed;
                let mut attempt_post_drain_ns = 0_u64;
                if let Some(start) = post_drain_start {
                    attempt_post_drain_ns = metrics.now_ns().saturating_sub(start);
                    timing.fill_post_drain_ns = timing
                        .fill_post_drain_ns
                        .saturating_add(attempt_post_drain_ns);
                }

                if has_pending_overflow {
                    if rows_written > 0 {
                        let finish_start = metrics.now_ns();
                        let encoded = encoder.finish()?;
                        self.estimator
                            .observe_encoded_block(&payload[..encoded.payload_len])?;
                        let page_finish_ns = metrics.now_ns().saturating_sub(finish_start);
                        return Ok(record_finished_scan_page(
                            metrics,
                            self.scan_timing_detail,
                            page_prepare_ns,
                            page_finish_ns,
                            MetricId::ScanFullPagesTotal,
                            encoded.row_count,
                            encoded.payload_len,
                            timing,
                        ));
                    }

                    timing.fill_pre_drain_ns = timing
                        .fill_pre_drain_ns
                        .saturating_sub(attempt_pre_drain_ns);
                    timing.slot_drain_ns =
                        timing.slot_drain_ns.saturating_sub(attempt_slot_drain_ns);
                    timing.fill_post_drain_ns = timing
                        .fill_post_drain_ns
                        .saturating_sub(attempt_post_drain_ns);
                    timing.page_retry_ns = timing
                        .page_retry_ns
                        .saturating_add(record_page_retry(metrics, retry_start));
                    self.estimator
                        .observe_empty_full_page(estimate.rows_per_page)?;
                    break;
                }

                if drain_stopped {
                    return Err(BackendServiceError::PageSource(
                        "slot scan page source unexpectedly stopped a direct receiver drain".into(),
                    ));
                }

                if drain_eof {
                    if rows_written == 0 {
                        return Ok(SourcePageStatus::Eof);
                    }

                    let finish_start = metrics.now_ns();
                    let encoded = encoder.finish()?;
                    self.estimator
                        .observe_encoded_block(&payload[..encoded.payload_len])?;
                    let page_finish_ns = metrics.now_ns().saturating_sub(finish_start);
                    return Ok(record_finished_scan_page(
                        metrics,
                        self.scan_timing_detail,
                        page_prepare_ns,
                        page_finish_ns,
                        MetricId::ScanEofPagesTotal,
                        encoded.row_count,
                        encoded.payload_len,
                        timing,
                    ));
                }

                if drain_rows_consumed == 0 {
                    return Err(BackendServiceError::PageSource(
                        "slot scan direct receiver made no progress".into(),
                    ));
                }
            }
        }
    }
}

fn record_finished_scan_page(
    metrics: RuntimeMetrics,
    scan_timing_detail: bool,
    page_prepare_ns: u64,
    page_finish_ns: u64,
    page_counter: MetricId,
    row_count: usize,
    payload_len: usize,
    timing: &mut ScanFillTiming,
) -> SourcePageStatus {
    timing.page_prepare_ns = timing.page_prepare_ns.saturating_add(page_prepare_ns);
    timing.page_finish_ns = timing.page_finish_ns.saturating_add(page_finish_ns);

    let emit_start = scan_timing_detail.then(|| metrics.now_ns());
    metrics.add(MetricId::ScanPagePrepareNs, page_prepare_ns);
    metrics.add(MetricId::ScanPageFinishNs, page_finish_ns);
    metrics.increment(page_counter);
    metrics.add(MetricId::ScanRowsEncodedTotal, row_count as u64);
    let status = SourcePageStatus::Page { payload_len };
    if let Some(start) = emit_start {
        timing.fill_emit_ns = timing
            .fill_emit_ns
            .saturating_add(metrics.now_ns().saturating_sub(start));
    }
    status
}

fn record_page_retry(metrics: RuntimeMetrics, retry_start: Option<u64>) -> u64 {
    if let Some(start) = retry_start {
        let elapsed_ns = metrics.now_ns().saturating_sub(start);
        metrics.add(MetricId::ScanPageRetryNs, elapsed_ns);
        metrics.increment(MetricId::ScanPageRetryTotal);
        elapsed_ns
    } else {
        0
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

        let metrics = self.metrics;
        let fill_start = metrics.now_ns();
        let mut inner_fill_ns = 0_u64;
        let mut fill_timing = ScanFillTiming::default();
        let result = with_registered_snapshot(self.snapshot, || {
            let inner_start = self.scan_timing_detail.then(|| metrics.now_ns());
            let result = self.fill_next_page_with_snapshot(block, &mut fill_timing);
            if let Some(start) = inner_start {
                inner_fill_ns = metrics.now_ns().saturating_sub(start);
            }
            result
        });
        if matches!(result, Ok(SourcePageStatus::Page { .. })) {
            let fill_ns = metrics.now_ns().saturating_sub(fill_start);
            metrics.add(MetricId::ScanPageFillNs, fill_ns);
            if self.scan_timing_detail {
                fill_timing.page_snapshot_ns = fill_ns.saturating_sub(inner_fill_ns);
                metrics.add(MetricId::ScanPageSnapshotNs, fill_timing.page_snapshot_ns);
                fill_timing.record_metrics(metrics, fill_ns);
            }
        }
        result
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
