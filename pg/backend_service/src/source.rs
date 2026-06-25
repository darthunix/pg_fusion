use crate::{with_registered_snapshot, BackendServiceError};
use arrow_layout::{init_block, LayoutPlan};
use arrow_schema::SchemaRef;
use filter::{
    hash_bool_key, hash_bytes_key, hash_decimal128_key, hash_float32_key, hash_float64_key,
    hash_int_key, hash_interval_month_day_nano_key, ProbeDecision, RuntimeFilterExecId,
    RuntimeFilterKeyType, RuntimeFilterPool, RuntimeFilterProbeHandle,
};
use metrics::{MetricId, RuntimeMetrics};
use pgrx::pg_sys;
use row_estimator::PageRowEstimator;
use scan_flow::{BackendPageSource, FlowId, SourcePageStatus};
use slot_encoder::{
    ensure_slot_deformed, with_filter_key, AppendStatus, PageBatchEncoder, SlotFilterKeyRef,
    SlotFilterKeyType,
};
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
    estimator: Option<PageRowEstimator>,
    metrics: RuntimeMetrics,
    runtime_filter_enabled: bool,
    runtime_filters: RuntimeFilterPool,
    runtime_filter_exec_id: RuntimeFilterExecId,
    scan_id: u64,
    runtime_filter_probes: Vec<RuntimeFilterProbeHandle>,
    runtime_filter_needed_attrs: i32,
    session: Option<StreamingScanSession>,
    overflow_slot: *mut pg_sys::TupleTableSlot,
    pending_overflow: pg_sys::HeapTuple,
}

pub(crate) struct SlotScanPageSourceInput {
    pub(crate) snapshot: pgrx::pg_sys::Snapshot,
    pub(crate) spi: ExecutionSpiContext,
    pub(crate) prepared: PreparedScan,
    pub(crate) schema: SchemaRef,
    pub(crate) source_projection: Vec<usize>,
    pub(crate) block_size: u32,
    pub(crate) fetch_batch_rows: usize,
    pub(crate) estimator: Option<PageRowEstimator>,
    pub(crate) metrics: RuntimeMetrics,
    pub(crate) runtime_filter_enabled: bool,
    pub(crate) runtime_filters: RuntimeFilterPool,
    pub(crate) runtime_filter_exec_id: RuntimeFilterExecId,
    pub(crate) scan_id: u64,
}

impl SlotScanPageSource {
    pub(crate) fn new(input: SlotScanPageSourceInput) -> Self {
        let SlotScanPageSourceInput {
            snapshot,
            spi,
            prepared,
            schema,
            source_projection,
            block_size,
            fetch_batch_rows,
            estimator,
            metrics,
            runtime_filter_enabled,
            runtime_filters,
            runtime_filter_exec_id,
            scan_id,
        } = input;
        let single_row_drains = estimator
            .as_ref()
            .is_some_and(PageRowEstimator::has_variable_width);
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
            runtime_filter_enabled,
            runtime_filters,
            runtime_filter_exec_id,
            scan_id,
            runtime_filter_probes: Vec::new(),
            runtime_filter_needed_attrs: 0,
            session: None,
            overflow_slot: std::ptr::null_mut(),
            pending_overflow: std::ptr::null_mut(),
        }
    }

    fn attach_runtime_filter_probes(&mut self) -> Result<(), BackendServiceError> {
        self.runtime_filter_probes.clear();
        self.runtime_filter_needed_attrs = 0;
        if !self.runtime_filter_enabled || !self.runtime_filters.is_attached() {
            return Ok(());
        }

        self.runtime_filters.lookup_probes(
            self.runtime_filter_exec_id,
            self.scan_id,
            &mut self.runtime_filter_probes,
        );
        let mut needed_attrs = 0_i32;
        for probe in &self.runtime_filter_probes {
            let output_column = probe.output_column() as usize;
            let Some(source_column) = self.source_projection.get(output_column).copied() else {
                return Err(BackendServiceError::ProtocolViolation(format!(
                    "runtime filter target output column {output_column} is outside scan projection"
                )));
            };
            needed_attrs = needed_attrs.max(i32::try_from(source_column + 1).map_err(|_| {
                BackendServiceError::ProtocolViolation(format!(
                    "runtime filter source column {source_column} does not fit into i32"
                ))
            })?);
        }
        self.runtime_filter_needed_attrs = needed_attrs;
        Ok(())
    }

    fn fill_next_page_with_snapshot(
        &mut self,
        payload: &mut [u8],
    ) -> Result<SourcePageStatus, BackendServiceError> {
        loop {
            let metrics = self.metrics;
            let estimated_rows_per_page =
                estimate_rows_per_page(&self.estimator, self.fetch_batch_rows)?;
            let source_projection = &self.source_projection;
            let runtime_filter_probes = &self.runtime_filter_probes;
            let runtime_filter_needed_attrs = self.runtime_filter_needed_attrs;
            let session = self.session.as_mut().ok_or_else(|| {
                BackendServiceError::PageSource("slot scan page source is not open".into())
            })?;
            let prepare_start = self.metrics.now_ns();
            let layout = LayoutPlan::from_arrow_schema(
                self.schema.as_ref(),
                estimated_rows_per_page,
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
                PageBatchEncoder::new_projected(session.tuple_desc(), source_projection, payload)
            }?;
            let needed_attrs = encoder.needed_attrs().max(runtime_filter_needed_attrs);
            let page_prepare_ns = self.metrics.now_ns().saturating_sub(prepare_start);
            let mut rows_written = 0usize;
            let mut filter_stats = RuntimeFilterProbeStats::default();

            if !self.pending_overflow.is_null() {
                let overflow_status = append_pending_overflow(
                    self.overflow_slot,
                    &mut self.pending_overflow,
                    &mut encoder,
                )?;
                match overflow_status {
                    AppendStatus::Appended => {
                        rows_written += 1;
                    }
                    AppendStatus::Full => {
                        record_page_retry(metrics);
                        observe_empty_full_page(&mut self.estimator, estimated_rows_per_page)?;
                        continue;
                    }
                }
            }

            loop {
                if rows_written >= max_rows {
                    let finish_start = metrics.now_ns();
                    let encoded = encoder.finish()?;
                    observe_encoded_block(&mut self.estimator, &payload[..encoded.payload_len])?;
                    let page_finish_ns = metrics.now_ns().saturating_sub(finish_start);
                    return Ok(record_finished_scan_page(
                        metrics,
                        page_prepare_ns,
                        page_finish_ns,
                        MetricId::ScanFullPagesTotal,
                        encoded.row_count,
                        encoded.payload_len,
                    ));
                }

                let remaining_rows = max_rows - rows_written;
                let row_budget = if self.single_row_drains {
                    1
                } else {
                    remaining_rows
                };
                // SAFETY: this backend-only callback is controlled by pg_fusion
                // and returns expected failures through Result. A panic here is
                // a bug, not a recoverable row-level PostgreSQL error.
                let drain_result = unsafe {
                    let append_slot = |slot: *mut pg_sys::TupleTableSlot| {
                        if needed_attrs > 0 && i32::from((*slot).tts_nvalid) < needed_attrs {
                            ensure_slot_deformed(slot, needed_attrs)?;
                        }
                        if runtime_filter_rejects_slot(
                            slot,
                            source_projection,
                            runtime_filter_probes,
                            &mut filter_stats,
                        )? {
                            return Ok(SlotSinkAction::Continue);
                        }
                        let status = encoder.append_slot(slot)?;
                        handle_append_slot_status(
                            status,
                            row_budget,
                            &mut rows_written,
                            max_rows,
                            slot,
                            &mut self.pending_overflow,
                        )
                    };
                    session.drain_slots_without_unwind_guard::<BackendServiceError>(
                        row_budget,
                        append_slot,
                    )
                };
                let drain = drain_result?;
                filter_stats.record(metrics);
                self.metrics.increment(MetricId::ScanFetchCallsTotal);
                let has_pending_overflow = !self.pending_overflow.is_null();
                let drain_stopped = drain.stopped;
                let drain_eof = drain.eof;
                let drain_rows_consumed = drain.rows_consumed;

                if has_pending_overflow {
                    if rows_written > 0 {
                        let finish_start = metrics.now_ns();
                        let encoded = encoder.finish()?;
                        observe_encoded_block(
                            &mut self.estimator,
                            &payload[..encoded.payload_len],
                        )?;
                        let page_finish_ns = metrics.now_ns().saturating_sub(finish_start);
                        return Ok(record_finished_scan_page(
                            metrics,
                            page_prepare_ns,
                            page_finish_ns,
                            MetricId::ScanFullPagesTotal,
                            encoded.row_count,
                            encoded.payload_len,
                        ));
                    }

                    record_page_retry(metrics);
                    observe_empty_full_page(&mut self.estimator, estimated_rows_per_page)?;
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
                    observe_encoded_block(&mut self.estimator, &payload[..encoded.payload_len])?;
                    let page_finish_ns = metrics.now_ns().saturating_sub(finish_start);
                    return Ok(record_finished_scan_page(
                        metrics,
                        page_prepare_ns,
                        page_finish_ns,
                        MetricId::ScanEofPagesTotal,
                        encoded.row_count,
                        encoded.payload_len,
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

fn estimate_rows_per_page(
    estimator: &Option<PageRowEstimator>,
    fetch_batch_rows: usize,
) -> Result<u32, BackendServiceError> {
    if let Some(estimator) = estimator {
        return Ok(estimator.estimate()?.rows_per_page);
    }
    u32::try_from(fetch_batch_rows.max(1)).map_err(|_| {
        BackendServiceError::PageSource(format!(
            "scan fetch batch rows {fetch_batch_rows} does not fit into u32"
        ))
    })
}

fn observe_encoded_block(
    estimator: &mut Option<PageRowEstimator>,
    payload: &[u8],
) -> Result<(), BackendServiceError> {
    if let Some(estimator) = estimator {
        estimator.observe_encoded_block(payload)?;
    }
    Ok(())
}

fn observe_empty_full_page(
    estimator: &mut Option<PageRowEstimator>,
    attempted_rows: u32,
) -> Result<(), BackendServiceError> {
    if let Some(estimator) = estimator {
        estimator.observe_empty_full_page(attempted_rows)?;
        return Ok(());
    }
    Err(BackendServiceError::PageSource(
        "empty-schema scan page filled before writing a row".into(),
    ))
}

fn record_finished_scan_page(
    metrics: RuntimeMetrics,
    page_prepare_ns: u64,
    page_finish_ns: u64,
    page_counter: MetricId,
    row_count: usize,
    payload_len: usize,
) -> SourcePageStatus {
    metrics.add(MetricId::ScanPagePrepareNs, page_prepare_ns);
    metrics.add(MetricId::ScanPageFinishNs, page_finish_ns);
    metrics.increment(page_counter);
    metrics.add(MetricId::ScanRowsEncodedTotal, row_count as u64);
    SourcePageStatus::Page { payload_len }
}

fn record_page_retry(metrics: RuntimeMetrics) {
    metrics.increment(MetricId::ScanPageRetryTotal);
}

fn handle_append_slot_status(
    status: AppendStatus,
    row_budget: usize,
    rows_written: &mut usize,
    max_rows: usize,
    slot: *mut pg_sys::TupleTableSlot,
    pending_overflow: &mut pg_sys::HeapTuple,
) -> Result<SlotSinkAction, BackendServiceError> {
    match status {
        AppendStatus::Appended => {
            *rows_written += 1;
            Ok(SlotSinkAction::Continue)
        }
        AppendStatus::Full => {
            if row_budget != 1 {
                return Err(BackendServiceError::PageSource(format!(
                    "slot encoder filled before exhausting row budget: budget={row_budget}, rows_written={}, max_rows={max_rows}",
                    *rows_written
                )));
            }
            *pending_overflow = unsafe { pg_sys::ExecCopySlotHeapTuple(slot) };
            if (*pending_overflow).is_null() {
                return Err(BackendServiceError::PageSource(
                    "ExecCopySlotHeapTuple returned null".into(),
                ));
            }
            Ok(SlotSinkAction::Continue)
        }
    }
}

#[derive(Default)]
struct RuntimeFilterProbeStats {
    probe_rows: u64,
    rejected_rows: u64,
    pass_unfiltered: u64,
}

impl RuntimeFilterProbeStats {
    fn record(&mut self, metrics: RuntimeMetrics) {
        if self.probe_rows != 0 {
            metrics.add(MetricId::RuntimeFilterProbeRowsTotal, self.probe_rows);
            self.probe_rows = 0;
        }
        if self.rejected_rows != 0 {
            metrics.add(
                MetricId::RuntimeFilterProbeRowsRejectedTotal,
                self.rejected_rows,
            );
            self.rejected_rows = 0;
        }
        if self.pass_unfiltered != 0 {
            metrics.add(
                MetricId::RuntimeFilterProbePassUnfilteredTotal,
                self.pass_unfiltered,
            );
            self.pass_unfiltered = 0;
        }
    }
}

fn runtime_filter_rejects_slot(
    slot: *mut pg_sys::TupleTableSlot,
    source_projection: &[usize],
    probes: &[RuntimeFilterProbeHandle],
    stats: &mut RuntimeFilterProbeStats,
) -> Result<bool, BackendServiceError> {
    if probes.is_empty() {
        return Ok(false);
    }

    stats.probe_rows = stats.probe_rows.saturating_add(1);
    for probe in probes {
        let output_column = probe.output_column() as usize;
        let Some(source_column) = source_projection.get(output_column).copied() else {
            return Err(BackendServiceError::ProtocolViolation(format!(
                "runtime filter target output column {output_column} is outside scan projection"
            )));
        };
        let decision = unsafe {
            with_filter_key(
                slot,
                source_column,
                slot_filter_key_type(probe.key_type()),
                |value| match value {
                    Some(value) => probe.decision_for_hash(hash_slot_filter_key(value)),
                    None => probe.decision_for_null(),
                },
            )
        }?;
        match decision {
            ProbeDecision::PassUnfiltered => {
                stats.pass_unfiltered = stats.pass_unfiltered.saturating_add(1);
            }
            ProbeDecision::MaybePresent => {}
            ProbeDecision::DefinitelyAbsent => {
                stats.rejected_rows = stats.rejected_rows.saturating_add(1);
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn slot_filter_key_type(key_type: RuntimeFilterKeyType) -> SlotFilterKeyType {
    match key_type {
        RuntimeFilterKeyType::Boolean => SlotFilterKeyType::Boolean,
        RuntimeFilterKeyType::Int16 => SlotFilterKeyType::Int16,
        RuntimeFilterKeyType::Int32 => SlotFilterKeyType::Int32,
        RuntimeFilterKeyType::Int64 => SlotFilterKeyType::Int64,
        RuntimeFilterKeyType::Float32 => SlotFilterKeyType::Float32,
        RuntimeFilterKeyType::Float64 => SlotFilterKeyType::Float64,
        RuntimeFilterKeyType::Utf8View => SlotFilterKeyType::Utf8View,
        RuntimeFilterKeyType::Uuid => SlotFilterKeyType::Uuid,
        RuntimeFilterKeyType::BinaryView => SlotFilterKeyType::BinaryView,
        RuntimeFilterKeyType::Date32 => SlotFilterKeyType::Date32,
        RuntimeFilterKeyType::Time64Microsecond => SlotFilterKeyType::Time64Microsecond,
        RuntimeFilterKeyType::TimestampMicrosecond => SlotFilterKeyType::TimestampMicrosecond,
        RuntimeFilterKeyType::Decimal128 => SlotFilterKeyType::Decimal128,
        RuntimeFilterKeyType::IntervalMonthDayNano => SlotFilterKeyType::IntervalMonthDayNano,
    }
}

fn hash_slot_filter_key(value: SlotFilterKeyRef<'_>) -> u64 {
    match value {
        SlotFilterKeyRef::Boolean(value) => hash_bool_key(value),
        SlotFilterKeyRef::Int16(value) => hash_int_key(value as i64),
        SlotFilterKeyRef::Int32(value) => hash_int_key(value as i64),
        SlotFilterKeyRef::Int64(value) => hash_int_key(value),
        SlotFilterKeyRef::Float32(value) => hash_float32_key(value),
        SlotFilterKeyRef::Float64(value) => hash_float64_key(value),
        SlotFilterKeyRef::Utf8(value) => hash_bytes_key(value),
        SlotFilterKeyRef::Uuid(value) => hash_bytes_key(value),
        SlotFilterKeyRef::Binary(value) => hash_bytes_key(value),
        SlotFilterKeyRef::Date32(value) => hash_int_key(value as i64),
        SlotFilterKeyRef::Time64Microsecond(value) => hash_int_key(value),
        SlotFilterKeyRef::TimestampMicrosecond(value) => hash_int_key(value),
        SlotFilterKeyRef::Decimal128 { value, scale } => hash_decimal128_key(value, scale),
        SlotFilterKeyRef::IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        } => hash_interval_month_day_nano_key(months, days, nanoseconds),
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
        self.attach_runtime_filter_probes()?;
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
        let result =
            with_registered_snapshot(self.snapshot, || self.fill_next_page_with_snapshot(block));
        if matches!(result, Ok(SourcePageStatus::Page { .. })) {
            let fill_ns = metrics.now_ns().saturating_sub(fill_start);
            metrics.add(MetricId::ScanPageFillNs, fill_ns);
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
        self.runtime_filter_probes.clear();
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

    let status = unsafe { encoder.append_slot(slot)? };
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
