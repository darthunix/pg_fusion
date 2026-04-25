use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_layout::{init_block, ColumnSpec, LayoutPlan, TypeTag};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use batch_encoder::BatchPageEncoder;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use issuance::{IssuedOutboundPage, IssuedOwnedFrame, IssuedTx};
use row_estimator::{EstimatorConfig, PageRowEstimator};
use runtime_metrics::{MetricId, RuntimeMetrics};

use crate::WorkerRuntimeError;

/// Static configuration for worker-side result page encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultPageProducerConfig {
    /// Page kind used for the emitted Arrow result pages.
    pub page_kind: transfer::MessageKind,
    /// Page flags used for the emitted Arrow result pages.
    pub page_flags: u16,
    /// Initial rows-per-page estimator prior for variable-width columns.
    pub estimator: EstimatorConfig,
    /// Shared runtime metrics handle. Defaults to no-op outside pg_fusion.
    pub metrics: RuntimeMetrics,
}

impl Default for ResultPageProducerConfig {
    fn default() -> Self {
        Self {
            page_kind: import::ARROW_LAYOUT_BATCH_KIND,
            page_flags: 0,
            estimator: EstimatorConfig::default(),
            metrics: RuntimeMetrics::default(),
        }
    }
}

/// One outbound result-stream step ready for transport.
#[derive(Debug)]
pub enum ResultPageStep {
    /// One detached result page ready to describe over control transport.
    OutboundPage(IssuedOutboundPage),
    /// One terminal close frame for the result stream.
    CloseFrame(IssuedOwnedFrame),
}

/// Stateful worker-side encoder from Arrow `RecordBatch` stream to issued pages.
///
/// This type owns no control-transport slot. Callers are responsible for
/// encoding and sending the returned [`ResultPageStep`] values over the primary
/// execution peer, and for later sending the execution-level terminal control
/// message (`CompleteExecution` or `FailExecution`).
pub struct ResultPageProducer {
    input_schema: SchemaRef,
    transport_schema: SchemaRef,
    specs: Vec<ColumnSpec>,
    estimator: PageRowEstimator,
    config: ResultPageProducerConfig,
    tx: IssuedTx,
    stream: SendableRecordBatchStream,
    pending_batch: Option<RecordBatch>,
    pending_row: usize,
    stream_exhausted: bool,
    close_emitted: bool,
    payload_capacity: u32,
}

impl std::fmt::Debug for ResultPageProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultPageProducer")
            .field("input_schema", &self.input_schema)
            .field("transport_schema", &self.transport_schema)
            .field("page_kind", &self.config.page_kind)
            .field("page_flags", &self.config.page_flags)
            .field("stream_exhausted", &self.stream_exhausted)
            .field("close_emitted", &self.close_emitted)
            .finish_non_exhaustive()
    }
}

impl ResultPageProducer {
    /// Create one result-page producer from an executed physical-plan stream.
    pub fn new(
        stream: SendableRecordBatchStream,
        tx: IssuedTx,
        payload_capacity: u32,
        config: ResultPageProducerConfig,
    ) -> Result<Self, WorkerRuntimeError> {
        let input_schema = stream.schema();
        let (transport_schema, specs) = normalize_result_transport_schema(&input_schema)?;
        let estimator = PageRowEstimator::new(&specs, payload_capacity, config.estimator)?;
        Ok(Self {
            input_schema,
            transport_schema,
            specs,
            estimator,
            config,
            tx,
            stream,
            pending_batch: None,
            pending_row: 0,
            stream_exhausted: false,
            close_emitted: false,
            payload_capacity,
        })
    }

    /// Normalized Arrow schema expected on the transport/result-ingress path.
    pub fn transport_schema(&self) -> SchemaRef {
        Arc::clone(&self.transport_schema)
    }

    /// Produce the next outbound page or close frame.
    ///
    /// Returns `Ok(None)` only after the stream close frame has already been
    /// emitted and the producer is fully exhausted.
    pub fn next_step(&mut self) -> Result<Option<ResultPageStep>, WorkerRuntimeError> {
        loop {
            if let Some(batch) = self.pending_batch.as_ref() {
                if self.pending_row < batch.num_rows() {
                    let step = self.encode_pending_page()?;
                    return Ok(Some(step));
                }
                self.pending_batch = None;
                self.pending_row = 0;
                continue;
            }

            if self.stream_exhausted {
                if self.close_emitted {
                    return Ok(None);
                }
                self.close_emitted = true;
                return Ok(Some(ResultPageStep::CloseFrame(self.tx.close()?)));
            }

            match futures::executor::block_on(self.stream.next()) {
                Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                Some(Ok(batch)) => {
                    self.pending_batch = Some(batch);
                    self.pending_row = 0;
                }
                Some(Err(err)) => return Err(err.into()),
                None => self.stream_exhausted = true,
            }
        }
    }

    fn encode_pending_page(&mut self) -> Result<ResultPageStep, WorkerRuntimeError> {
        let fill_start = self.config.metrics.now_ns();
        let batch = self
            .pending_batch
            .as_ref()
            .expect("pending batch must exist while encoding");

        loop {
            let estimate = self.estimator.estimate()?;
            let plan = LayoutPlan::new(&self.specs, estimate.rows_per_page, self.payload_capacity)?;
            let mut writer = self
                .tx
                .begin(self.config.page_kind, self.config.page_flags)?;

            let (encoded, rows_written) = {
                let payload = writer.payload_mut();
                init_block(payload, &plan)?;
                let mut encoder =
                    BatchPageEncoder::new(self.input_schema.as_ref(), &plan, payload)?;
                let append = encoder.append_batch(batch, self.pending_row)?;
                if append.rows_written == 0 && append.full {
                    self.estimator
                        .observe_empty_full_page(estimate.rows_per_page)?;
                    continue;
                }
                let encoded = encoder.finish()?;
                self.estimator
                    .observe_encoded_block(&payload[..encoded.payload_len])?;
                (encoded, append.rows_written)
            };

            let outbound = writer.finish_with_payload_len(encoded.payload_len)?;
            self.config
                .metrics
                .add_elapsed(MetricId::WorkerResultPageFillNs, fill_start);
            self.pending_row += rows_written;
            if self.pending_row == batch.num_rows() {
                self.pending_batch = None;
                self.pending_row = 0;
            }
            return Ok(ResultPageStep::OutboundPage(outbound));
        }
    }
}

/// Normalize a physical-plan output schema into the transport schema accepted
/// by `batch_encoder` and `slot_import`.
///
/// The transport path preserves primitive types and `Uuid`, but rewrites
/// `Utf8`/`Utf8View` into `Utf8View` and `Binary`/`BinaryView` into
/// `BinaryView`.
pub fn normalize_result_transport_schema(
    input_schema: &SchemaRef,
) -> Result<(SchemaRef, Vec<ColumnSpec>), WorkerRuntimeError> {
    if input_schema.fields().is_empty() {
        return Err(WorkerRuntimeError::EmptyResultSchema);
    }

    let mut fields = Vec::with_capacity(input_schema.fields().len());
    let mut specs = Vec::with_capacity(input_schema.fields().len());

    for (index, field) in input_schema.fields().iter().enumerate() {
        let normalized = normalize_field(index, field)?;
        let type_tag =
            TypeTag::from_arrow_data_type(index, normalized.data_type()).map_err(|_| {
                WorkerRuntimeError::UnsupportedResultColumnType {
                    index,
                    data_type: normalized.data_type().to_string(),
                }
            })?;
        specs.push(ColumnSpec::new(type_tag, normalized.is_nullable()));
        fields.push(normalized);
    }

    Ok((Arc::new(Schema::new(fields)), specs))
}

fn normalize_field(index: usize, field: &Field) -> Result<Field, WorkerRuntimeError> {
    let data_type = match field.data_type() {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => field.data_type().clone(),
        DataType::FixedSizeBinary(width) if *width == 16 => field.data_type().clone(),
        DataType::Utf8 | DataType::Utf8View => DataType::Utf8View,
        DataType::Binary | DataType::BinaryView => DataType::BinaryView,
        other => {
            return Err(WorkerRuntimeError::UnsupportedResultColumnType {
                index,
                data_type: other.to_string(),
            });
        }
    };

    Ok(Field::new(field.name(), data_type, field.is_nullable())
        .with_metadata(field.metadata().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::alloc::{alloc_zeroed, dealloc, Layout};
    use std::pin::Pin;
    use std::ptr::NonNull;
    use std::task::{Context, Poll};

    use arrow_array::{
        ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, StringViewArray,
    };
    use datafusion::physical_plan::RecordBatchStream;
    use datafusion_common::Result as DFResult;
    use futures::Stream;
    use issuance::{IssuanceConfig, IssuancePool, IssueEvent, IssuedRx};
    use pool::{PagePool, PagePoolConfig};
    use transfer::PageRx;

    struct OwnedRegion {
        base: NonNull<u8>,
        layout: Layout,
    }

    impl OwnedRegion {
        fn new(layout: Layout) -> Self {
            let ptr = unsafe { alloc_zeroed(layout) };
            let base = NonNull::new(ptr).expect("allocation must succeed");
            Self { base, layout }
        }
    }

    impl Drop for OwnedRegion {
        fn drop(&mut self) {
            unsafe { dealloc(self.base.as_ptr(), self.layout) };
        }
    }

    #[derive(Debug)]
    struct TestStream {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    }

    impl Stream for TestStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.batches.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(Ok(self.batches.remove(0))))
            }
        }
    }

    impl RecordBatchStream for TestStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    fn init_page_pool(page_size: usize, page_count: u32) -> (OwnedRegion, PagePool) {
        let cfg = PagePoolConfig::new(page_size, page_count).expect("pool config");
        let layout = PagePool::layout(cfg).expect("pool layout");
        let region = OwnedRegion::new(Layout::from_size_align(layout.size, layout.align).unwrap());
        let pool = unsafe { PagePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
        (region, pool)
    }

    fn init_issuance_pool(permit_count: u32) -> (OwnedRegion, IssuancePool) {
        let cfg = IssuanceConfig::new(permit_count).expect("issuance config");
        let layout = IssuancePool::layout(cfg).expect("issuance layout");
        let region = OwnedRegion::new(Layout::from_size_align(layout.size, layout.align).unwrap());
        let pool =
            unsafe { IssuancePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
        (region, pool)
    }

    #[test]
    fn normalizes_variable_width_columns_to_views() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("txt", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, false),
            Field::new("n", DataType::Int64, true),
        ]));

        let (transport_schema, specs) =
            normalize_result_transport_schema(&schema).expect("transport schema");

        assert_eq!(transport_schema.field(0).data_type(), &DataType::Utf8View);
        assert_eq!(transport_schema.field(1).data_type(), &DataType::BinaryView);
        assert_eq!(transport_schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(specs[0], ColumnSpec::new(TypeTag::Utf8View, true));
        assert_eq!(specs[1], ColumnSpec::new(TypeTag::BinaryView, false));
    }

    #[test]
    fn emits_pages_and_terminal_close() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("txt", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![
                    Some(b"one".as_slice()),
                    Some(b"two".as_slice()),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef,
            ],
        )
        .expect("batch");

        let stream: SendableRecordBatchStream = Box::pin(TestStream {
            schema: Arc::clone(&schema),
            batches: vec![batch],
        });

        let (_page_region, page_pool) = init_page_pool(512, 4);
        let (_issuance_region, issuance_pool) = init_issuance_pool(4);
        let page_tx = transfer::PageTx::new(page_pool);
        let payload_capacity =
            u32::try_from(page_tx.payload_capacity()).expect("payload capacity fits u32");
        let tx = IssuedTx::new(page_tx, issuance_pool);
        let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

        let mut producer = ResultPageProducer::new(
            stream,
            tx,
            payload_capacity,
            ResultPageProducerConfig::default(),
        )
        .expect("producer");
        let decoder = import::ArrowPageDecoder::new(producer.transport_schema()).expect("decoder");

        let mut saw_close = false;
        let mut imported_rows = 0usize;
        while let Some(step) = producer.next_step().expect("next step") {
            match step {
                ResultPageStep::OutboundPage(outbound) => {
                    let frame = outbound.frame();
                    outbound.mark_sent();
                    match rx.accept(&frame).expect("accept page") {
                        IssueEvent::Page(page) => {
                            let batch = decoder.import_owned(page).expect("import");
                            let txt = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<StringViewArray>()
                                .expect("string view");
                            imported_rows += batch.num_rows();
                            assert_eq!(txt.value(0), "alpha");
                        }
                        IssueEvent::Closed => panic!("page frame must not close"),
                    }
                }
                ResultPageStep::CloseFrame(frame) => match rx.accept(&frame).expect("accept close")
                {
                    IssueEvent::Closed => saw_close = true,
                    IssueEvent::Page(_) => panic!("close frame must not yield page"),
                },
            }
        }

        assert_eq!(imported_rows, 2);
        assert!(saw_close, "result stream must terminate with close frame");
    }
}
