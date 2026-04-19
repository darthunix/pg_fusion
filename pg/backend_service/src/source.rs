use crate::{with_registered_snapshot, BackendServiceError};
use arrow_layout::{init_block, LayoutPlan};
use arrow_schema::SchemaRef;
use row_estimator::PageRowEstimator;
use scan_flow::{BackendPageSource, FlowId, SourcePageStatus};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use slot_scan::{PreparedScan, StreamingScanSession};

pub(crate) struct SlotScanPageSource {
    snapshot: pgrx::pg_sys::Snapshot,
    prepared: PreparedScan,
    schema: SchemaRef,
    block_size: u32,
    fetch_batch_rows: usize,
    estimator: PageRowEstimator,
    session: Option<StreamingScanSession>,
}

impl SlotScanPageSource {
    pub(crate) fn new(
        snapshot: pgrx::pg_sys::Snapshot,
        prepared: PreparedScan,
        schema: SchemaRef,
        block_size: u32,
        fetch_batch_rows: usize,
        estimator: PageRowEstimator,
    ) -> Self {
        Self {
            snapshot,
            prepared,
            schema,
            block_size,
            fetch_batch_rows,
            estimator,
            session: None,
        }
    }

    fn fill_next_page_with_snapshot(
        &mut self,
        payload: &mut [u8],
    ) -> Result<SourcePageStatus, BackendServiceError> {
        let session = self.session.as_mut().ok_or_else(|| {
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

            let mut encoder = unsafe { PageBatchEncoder::new(session.tuple_desc(), payload) }?;
            let mut rows_written = 0usize;

            loop {
                let Some(slot) = session.current_slot()? else {
                    if rows_written == 0 {
                        return Ok(SourcePageStatus::Eof);
                    }

                    let encoded = encoder.finish()?;
                    self.estimator
                        .observe_encoded_block(&payload[..encoded.payload_len])?;
                    return Ok(SourcePageStatus::Page {
                        payload_len: encoded.payload_len,
                    });
                };

                match encoder.append_slot(slot)? {
                    AppendStatus::Appended => {
                        rows_written += 1;
                        session.consume_current_row()?;
                    }
                    AppendStatus::Full if rows_written > 0 => {
                        let encoded = encoder.finish()?;
                        self.estimator
                            .observe_encoded_block(&payload[..encoded.payload_len])?;
                        return Ok(SourcePageStatus::Page {
                            payload_len: encoded.payload_len,
                        });
                    }
                    AppendStatus::Full => {
                        self.estimator
                            .observe_empty_full_page(estimate.rows_per_page)?;
                        break;
                    }
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
                .open_streaming_session(self.fetch_batch_rows)
                .map_err(BackendServiceError::PrepareScan)
        })?;
        self.session = Some(session);
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
