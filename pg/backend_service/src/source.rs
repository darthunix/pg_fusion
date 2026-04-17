use crate::{with_registered_snapshot, BackendServiceError};
use arrow_layout::{init_block, LayoutPlan};
use arrow_schema::SchemaRef;
use row_estimator::PageRowEstimator;
use scan_flow::{BackendPageSource, FlowId, SourcePageStatus};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use slot_scan::{CursorDrainOutcome, CursorRowAction, PreparedScan, PreparedScanCursor};

pub(crate) struct SlotScanPageSource {
    snapshot: pgrx::pg_sys::Snapshot,
    prepared: PreparedScan,
    schema: SchemaRef,
    block_size: u32,
    estimator: PageRowEstimator,
    cursor: Option<PreparedScanCursor>,
}

impl SlotScanPageSource {
    pub(crate) fn new(
        snapshot: pgrx::pg_sys::Snapshot,
        prepared: PreparedScan,
        schema: SchemaRef,
        block_size: u32,
        estimator: PageRowEstimator,
    ) -> Self {
        Self {
            snapshot,
            prepared,
            schema,
            block_size,
            estimator,
            cursor: None,
        }
    }

    fn fill_next_page_with_snapshot(
        &mut self,
        payload: &mut [u8],
    ) -> Result<SourcePageStatus, BackendServiceError> {
        let cursor = self.cursor.as_mut().ok_or_else(|| {
            BackendServiceError::PageSource("slot scan page source is not open".into())
        })?;

        loop {
            let estimate = self.estimator.estimate()?;
            let layout = LayoutPlan::from_arrow_schema(
                self.schema.as_ref(),
                estimate.rows_per_page,
                self.block_size,
            )?;
            init_block(payload, &layout)?;

            let mut encoder = unsafe { PageBatchEncoder::new(cursor.tuple_desc(), payload) }?;
            let mut rows_written = 0usize;
            let outcome = cursor.drain_rows::<BackendServiceError, _>(|slot| {
                match encoder.append_slot(slot)? {
                    AppendStatus::Appended => {
                        rows_written += 1;
                        Ok::<CursorRowAction, BackendServiceError>(CursorRowAction::Continue)
                    }
                    AppendStatus::Full => Ok::<CursorRowAction, BackendServiceError>(
                        CursorRowAction::ReplayCurrentAndStop,
                    ),
                }
            })?;

            match outcome {
                CursorDrainOutcome::Eof => {
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
                CursorDrainOutcome::Stopped => {
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
                    continue;
                }
            }
        }
    }
}

impl BackendPageSource for SlotScanPageSource {
    type Error = BackendServiceError;

    fn open(&mut self, _flow: FlowId) -> Result<(), Self::Error> {
        let cursor = with_registered_snapshot(self.snapshot, || {
            self.prepared
                .open_cursor()
                .map_err(BackendServiceError::PrepareScan)
        })?;
        self.cursor = Some(cursor);
        Ok(())
    }

    fn fill_next_page(&mut self, payload: &mut [u8]) -> Result<SourcePageStatus, Self::Error> {
        let payload_len_u32 = u32::try_from(payload.len()).map_err(|_| {
            BackendServiceError::PageSource(format!(
                "scan page payload size {} does not fit into u32",
                payload.len()
            ))
        })?;
        if payload_len_u32 != self.block_size {
            return Err(BackendServiceError::PageSource(format!(
                "scan payload block size mismatch: expected {}, got {}",
                self.block_size,
                payload.len()
            )));
        }

        with_registered_snapshot(self.snapshot, || self.fill_next_page_with_snapshot(payload))
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        if let Some(cursor) = self.cursor.take() {
            with_registered_snapshot(self.snapshot, || {
                cursor
                    .close()
                    .map(|_| ())
                    .map_err(BackendServiceError::PrepareScan)
            })?;
        }
        Ok(())
    }
}
