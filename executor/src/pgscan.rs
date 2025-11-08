use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder,
    Float32Builder, Float64Builder, StringBuilder, Date32Builder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, IntervalMonthDayNanoBuilder,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion_catalog::{Session, TableProvider};
use datafusion::physical_plan::RecordBatchStream;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use futures::Stream;
use tokio::sync::mpsc;
use storage::heap::{HeapPage, PgAttrMeta, decode_tuple_project};
use pgrx_pg_sys as pg_sys;
use crate::shm;

pub type ScanId = u64;

#[derive(Debug, Clone, Copy)]
pub struct HeapBlock {
    pub scan_id: ScanId,
    pub slot_id: u16,
    pub table_oid: u32,
    pub blkno: u32,
    pub num_offsets: u16,
    pub vis_len: u16,
    // Note: page bytes and visibility bitmap reside in shared memory,
    // addressed by `slot_id` with `vis_len` bytes for visibility bitmap.
}

#[derive(Debug, Default)]
pub struct ScanRegistry {
    inner: Mutex<HashMap<ScanId, Entry>>,
}

#[derive(Debug)]
struct Entry {
    sender: mpsc::Sender<HeapBlock>,
    receiver: Option<mpsc::Receiver<HeapBlock>>,
}

impl ScanRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn lock(&self) -> MutexGuard<HashMap<ScanId, Entry>> {
        // Avoid panics on poisoned mutex by recovering the inner state
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reserve space for at least `additional` more scan entries.
    pub fn reserve(&self, additional: usize) {
        let mut map = self.lock();
        map.reserve(additional);
    }

    pub fn register(&self, scan_id: ScanId, capacity: usize) -> mpsc::Sender<HeapBlock> {
        let (tx, rx) = mpsc::channel(capacity);
        let mut map = self.lock();
        map.insert(
            scan_id,
            Entry {
                sender: tx.clone(),
                receiver: Some(rx),
            },
        );
        tx
    }

    pub fn sender(&self, scan_id: ScanId) -> Option<mpsc::Sender<HeapBlock>> {
        let map = self.lock();
        map.get(&scan_id).map(|e| e.sender.clone())
    }

    pub fn take_receiver(&self, scan_id: ScanId) -> Option<mpsc::Receiver<HeapBlock>> {
        let mut map = self.lock();
        map.get_mut(&scan_id).and_then(|e| e.receiver.take())
    }

    /// Close all channels and clear the registry.
    /// Dropping the senders will cause receivers to observe stream termination.
    pub fn close_and_clear(&self) {
        let mut map = self.lock();
        map.clear();
    }

    /// Close a single scan by dropping its sender; receiver will eventually see EOF.
    pub fn close(&self, scan_id: ScanId) {
        let mut map = self.lock();
        let _ = map.remove(&scan_id);
    }
}

// No global registry; registries are per-connection and owned by the server Storage.

#[derive(Debug)]
pub struct PgTableProvider {
    schema: SchemaRef,
    scan_id: ScanId,
    registry: Arc<ScanRegistry>,
}

impl PgTableProvider {
    pub fn new(scan_id: ScanId, schema: SchemaRef, registry: Arc<ScanRegistry>) -> Self {
        Self {
            schema,
            scan_id,
            registry,
        }
    }
}

#[async_trait]
impl TableProvider for PgTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Unsupported])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let exec = PgScanExec::new(self.scan_id, Arc::clone(&self.schema), Arc::clone(&self.registry));
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
pub struct PgScanExec {
    schema: SchemaRef,
    scan_id: ScanId,
    registry: Arc<ScanRegistry>,
    props: PlanProperties,
    attrs: Arc<Vec<PgAttrMeta>>,
}

impl PgScanExec {
    pub fn new(scan_id: ScanId, schema: SchemaRef, registry: Arc<ScanRegistry>) -> Self {
        let eq = EquivalenceProperties::new(schema.clone());
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        let attrs = Arc::new(attrs_from_schema(&schema));
        Self {
            schema,
            scan_id,
            registry,
            props,
            attrs,
        }
    }
}

impl DisplayAs for PgScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "PgScanExec: scan_id={}", self.scan_id),
            DisplayFormatType::Verbose => write!(
                f,
                "PgScanExec: scan_id={}, schema={:?}",
                self.scan_id, self.schema
            ),
        }
    }
}

impl ExecutionPlan for PgScanExec {
    fn name(&self) -> &str {
        "PgScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Plan(
                "PgScanExec has no children".into(),
            ))
        }
    }

    fn execute(&self, _partition: usize, _ctx: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let rx = self.registry.take_receiver(self.scan_id);
        let stream = PgScanStream::new(Arc::clone(&self.schema), Arc::clone(&self.attrs), rx);
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

pub struct PgScanStream {
    schema: SchemaRef,
    attrs: Arc<Vec<PgAttrMeta>>,
    rx: Option<mpsc::Receiver<HeapBlock>>,
}

impl PgScanStream {
    pub fn new(schema: SchemaRef, attrs: Arc<Vec<PgAttrMeta>>, rx: Option<mpsc::Receiver<HeapBlock>>) -> Self {
        Self { schema, attrs, rx }
    }
}

impl Stream for PgScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(rx) = this.rx.as_mut() else {
            return Poll::Ready(None);
        };
        match rx.poll_recv(cx) {
            Poll::Ready(Some(block)) => {
                // Borrow page and (optional) visibility bitmap from shared memory (no copy)
                let page = unsafe { shm::block_slice(block.slot_id as usize) };
                let _vis = if block.vis_len > 0 {
                    Some(unsafe { shm::vis_slice(block.slot_id as usize, block.vis_len as usize) })
                } else { None };

                // Prepare decoding metadata: attrs (precomputed once) and projection (all columns)
                let total_cols = this.schema.fields().len();

                // Create a HeapPage view and iterate tuples
                let hp = unsafe { HeapPage::from_slice(page) };
                let Ok(hp) = hp else {
                    // On error decoding page, return empty batch for resilience
                    let batch = RecordBatch::new_empty(Arc::clone(&this.schema));
                    return Poll::Ready(Some(Ok(batch)));
                };

                // Prepare per-column builders
                let col_count = total_cols;
                let mut builders = make_builders(&this.schema, block.num_offsets as usize)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
                // Use tuples_by_offset to iterate LP_NORMAL tuples in page order
                let mut pairs: Vec<(u16, u16)> = Vec::new();
                let mut it = hp.tuples_by_offset(None, std::ptr::null_mut(), &mut pairs);
                let page_hdr = unsafe { &*(page.as_ptr() as *const pg_sys::PageHeaderData) } as *const pg_sys::PageHeaderData;
                while let Some(tup) = it.next() {
                    // Decode projected columns for tuple using iterator over all columns
                    let iter = unsafe { decode_tuple_project(page_hdr, tup, &this.attrs, 0..total_cols) };
                    let Ok(mut iter) = iter else {
                        continue;
                    };
                    for col_idx in 0..total_cols {
                        match iter.next() {
                            Some(Ok(v)) => append_scalar(&mut builders[col_idx], v),
                            Some(Err(_e)) => append_null(&mut builders[col_idx]),
                            None => append_null(&mut builders[col_idx]),
                        }
                    }
                }

                // Build Arrow arrays and RecordBatch
                let mut arrs = Vec::with_capacity(col_count);
                for b in builders.into_iter() {
                    arrs.push(finish_builder(b));
                }
                let rb = RecordBatch::try_new(Arc::clone(&this.schema), arrs)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
                Poll::Ready(Some(Ok(rb)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn attrs_from_schema(schema: &SchemaRef) -> Vec<PgAttrMeta> {
    schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            datafusion::arrow::datatypes::DataType::Boolean => PgAttrMeta { atttypid: pg_sys::BOOLOID, attlen: 1, attalign: b'c' },
            datafusion::arrow::datatypes::DataType::Utf8 => PgAttrMeta { atttypid: pg_sys::TEXTOID, attlen: -1, attalign: b'i' },
            datafusion::arrow::datatypes::DataType::Int16 => PgAttrMeta { atttypid: pg_sys::INT2OID, attlen: 2, attalign: b's' },
            datafusion::arrow::datatypes::DataType::Int32 => PgAttrMeta { atttypid: pg_sys::INT4OID, attlen: 4, attalign: b'i' },
            datafusion::arrow::datatypes::DataType::Int64 => PgAttrMeta { atttypid: pg_sys::INT8OID, attlen: 8, attalign: b'd' },
            datafusion::arrow::datatypes::DataType::Float32 => PgAttrMeta { atttypid: pg_sys::FLOAT4OID, attlen: 4, attalign: b'i' },
            datafusion::arrow::datatypes::DataType::Float64 => PgAttrMeta { atttypid: pg_sys::FLOAT8OID, attlen: 8, attalign: b'd' },
            datafusion::arrow::datatypes::DataType::Date32 => PgAttrMeta { atttypid: pg_sys::DATEOID, attlen: 4, attalign: b'i' },
            datafusion::arrow::datatypes::DataType::Time64(_) => PgAttrMeta { atttypid: pg_sys::TIMEOID, attlen: 8, attalign: b'd' },
            datafusion::arrow::datatypes::DataType::Timestamp(_, _) => PgAttrMeta { atttypid: pg_sys::TIMESTAMPOID, attlen: 8, attalign: b'd' },
            datafusion::arrow::datatypes::DataType::Interval(_) => PgAttrMeta { atttypid: pg_sys::INTERVALOID, attlen: 16, attalign: b'd' },
            _ => PgAttrMeta { atttypid: pg_sys::InvalidOid, attlen: -1, attalign: b'c' },
        })
        .collect()
}

enum ColBuilder {
    Bool(BooleanBuilder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Time64Us(Time64MicrosecondBuilder),
    TsUs(TimestampMicrosecondBuilder),
    Interval(IntervalMonthDayNanoBuilder),
}

fn make_builders(schema: &SchemaRef, capacity: usize) -> Result<Vec<ColBuilder>, DataFusionError> {
    let mut out = Vec::with_capacity(schema.fields().len());
    for f in schema.fields().iter() {
        let b = match f.data_type() {
            ArrowDataType::Boolean => ColBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
            ArrowDataType::Int16 => ColBuilder::I16(Int16Builder::with_capacity(capacity)),
            ArrowDataType::Int32 => ColBuilder::I32(Int32Builder::with_capacity(capacity)),
            ArrowDataType::Int64 => ColBuilder::I64(Int64Builder::with_capacity(capacity)),
            ArrowDataType::Float32 => ColBuilder::F32(Float32Builder::with_capacity(capacity)),
            ArrowDataType::Float64 => ColBuilder::F64(Float64Builder::with_capacity(capacity)),
            ArrowDataType::Utf8 => ColBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 8)),
            ArrowDataType::Date32 => ColBuilder::Date32(Date32Builder::with_capacity(capacity)),
            ArrowDataType::Time64(_) => ColBuilder::Time64Us(Time64MicrosecondBuilder::with_capacity(capacity)),
            ArrowDataType::Timestamp(_, _) => ColBuilder::TsUs(TimestampMicrosecondBuilder::with_capacity(capacity)),
            ArrowDataType::Interval(_) => ColBuilder::Interval(IntervalMonthDayNanoBuilder::with_capacity(capacity)),
            other => return Err(DataFusionError::Execution(format!("unsupported type in builder: {other:?}"))),
        };
        out.push(b);
    }
    Ok(out)
}

fn append_scalar(b: &mut ColBuilder, v: ScalarValue) {
    match (b, v) {
        (ColBuilder::Bool(b), ScalarValue::Boolean(Some(x))) => b.append_value(x),
        (ColBuilder::Bool(b), _) => b.append_null(),

        (ColBuilder::I16(b), ScalarValue::Int16(Some(x))) => b.append_value(x),
        (ColBuilder::I16(b), _) => b.append_null(),

        (ColBuilder::I32(b), ScalarValue::Int32(Some(x))) => b.append_value(x),
        (ColBuilder::I32(b), ScalarValue::Date32(Some(x))) => b.append_value(x),
        (ColBuilder::I32(b), _) => b.append_null(),

        (ColBuilder::I64(b), ScalarValue::Int64(Some(x))) => b.append_value(x),
        (ColBuilder::I64(b), _) => b.append_null(),

        (ColBuilder::F32(b), ScalarValue::Float32(Some(x))) => b.append_value(x),
        (ColBuilder::F32(b), _) => b.append_null(),

        (ColBuilder::F64(b), ScalarValue::Float64(Some(x))) => b.append_value(x),
        (ColBuilder::F64(b), _) => b.append_null(),

        (ColBuilder::Utf8(b), ScalarValue::Utf8(Some(s))) => b.append_value(&s),
        (ColBuilder::Utf8(b), _) => b.append_null(),

        (ColBuilder::Date32(b), ScalarValue::Date32(Some(x))) => b.append_value(x),
        (ColBuilder::Date32(b), _) => b.append_null(),

        (ColBuilder::Time64Us(b), ScalarValue::Time64Microsecond(Some(x))) => b.append_value(x),
        (ColBuilder::Time64Us(b), _) => b.append_null(),

        (ColBuilder::TsUs(b), ScalarValue::TimestampMicrosecond(Some(x), _)) => b.append_value(x),
        (ColBuilder::TsUs(b), _) => b.append_null(),

        (ColBuilder::Interval(b), ScalarValue::IntervalMonthDayNano(Some(x))) => b.append_value(x),
        (ColBuilder::Interval(b), _) => b.append_null(),
    }
}

fn append_null(b: &mut ColBuilder) { match b {
    ColBuilder::Bool(b) => b.append_null(),
    ColBuilder::I16(b) => b.append_null(),
    ColBuilder::I32(b) => b.append_null(),
    ColBuilder::I64(b) => b.append_null(),
    ColBuilder::F32(b) => b.append_null(),
    ColBuilder::F64(b) => b.append_null(),
    ColBuilder::Utf8(b) => b.append_null(),
    ColBuilder::Date32(b) => b.append_null(),
    ColBuilder::Time64Us(b) => b.append_null(),
    ColBuilder::TsUs(b) => b.append_null(),
    ColBuilder::Interval(b) => b.append_null(),
} }

fn finish_builder(b: ColBuilder) -> ArrayRef { match b {
    ColBuilder::Bool(mut b) => Arc::new(b.finish()),
    ColBuilder::I16(mut b) => Arc::new(b.finish()),
    ColBuilder::I32(mut b) => Arc::new(b.finish()),
    ColBuilder::I64(mut b) => Arc::new(b.finish()),
    ColBuilder::F32(mut b) => Arc::new(b.finish()),
    ColBuilder::F64(mut b) => Arc::new(b.finish()),
    ColBuilder::Utf8(mut b) => Arc::new(b.finish()),
    ColBuilder::Date32(mut b) => Arc::new(b.finish()),
    ColBuilder::Time64Us(mut b) => Arc::new(b.finish()),
    ColBuilder::TsUs(mut b) => Arc::new(b.finish()),
    ColBuilder::Interval(mut b) => Arc::new(b.finish()),
} }

impl RecordBatchStream for PgScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub fn count_scans(plan: &Arc<dyn ExecutionPlan>) -> usize {
    fn visit(node: &Arc<dyn ExecutionPlan>) -> usize {
        let mut n = 0usize;
        if node.as_any().downcast_ref::<PgScanExec>().is_some() {
            n += 1;
        }
        for child in node.children() {
            n += visit(child);
        }
        n
    }
    visit(plan)
}

pub fn for_each_scan<F, E>(plan: &Arc<dyn ExecutionPlan>, mut f: F) -> Result<(), E>
where
    F: FnMut(ScanId, u32) -> Result<(), E>,
{
    fn visit<F, E>(node: &Arc<dyn ExecutionPlan>, f: &mut F) -> Result<(), E>
    where
        F: FnMut(ScanId, u32) -> Result<(), E>,
    {
        if let Some(p) = node.as_any().downcast_ref::<PgScanExec>() {
            let id = p.scan_id;
            let table_oid = id as u32; // current convention
            f(id, table_oid)?;
        }
        for child in node.children() {
            visit(child, f)?;
        }
        Ok(())
    }
    visit(plan, &mut f)
}
