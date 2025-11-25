use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use crate::shm;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics,
};
use datafusion::scalar::ScalarValue;
use datafusion_catalog::{Session, TableProvider};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use futures::Stream;
use pgrx_pg_sys as pg_sys;
use storage::heap::{decode_tuple_project, HeapPage, PgAttrMeta};
use tokio::sync::mpsc;

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
    conn_id: usize,
    next_id: AtomicU64,
    next_slot: AtomicU16,
}

#[derive(Debug)]
struct Entry {
    sender: mpsc::Sender<HeapBlock>,
    receiver: Option<mpsc::Receiver<HeapBlock>>,
    slot_id: u16,
}

impl ScanRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            conn_id: 0,
            next_id: AtomicU64::new(1),
            next_slot: AtomicU16::new(0),
        }
    }

    pub fn with_conn(conn_id: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            conn_id,
            next_id: AtomicU64::new(1),
            next_slot: AtomicU16::new(0),
        }
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

    /// Allocate a unique scan identifier for this connection.
    pub fn allocate_id(&self) -> ScanId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn register(&self, scan_id: ScanId, capacity: usize) -> mpsc::Sender<HeapBlock> {
        // Allocate alternating slot ids 0,1 for concurrent scans
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed) % 2;
        let (tx, rx) = mpsc::channel(capacity);
        let mut map = self.lock();
        map.insert(
            scan_id,
            Entry {
                sender: tx.clone(),
                receiver: Some(rx),
                slot_id: slot,
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

    /// Get the assigned slot id for this scan
    pub fn slot_for(&self, scan_id: ScanId) -> Option<u16> {
        let map = self.lock();
        map.get(&scan_id).map(|e| e.slot_id)
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

    #[inline]
    pub fn conn_id(&self) -> usize {
        self.conn_id
    }
}

// No global registry; registries are per-connection and owned by the server Storage.

#[derive(Debug)]
pub struct PgTableProvider {
    schema: SchemaRef,
    table_oid: u32,
    registry: Arc<ScanRegistry>,
}

impl PgTableProvider {
    pub fn new(table_oid: u32, schema: SchemaRef, registry: Arc<ScanRegistry>) -> Self {
        Self {
            schema,
            table_oid,
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
        // Respect requested projection; DataFusion expects the physical plan schema
        // to match the projected logical input schema.
        let proj_indices: Vec<usize> = match _projection {
            Some(ix) => ix.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        let proj_fields: Vec<datafusion::arrow::datatypes::Field> = proj_indices
            .iter()
            .map(|&i| self.schema.field(i).clone())
            .collect();
        let proj_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(proj_fields));

        // Allocate a fresh, unique scan id for every scan instance (handles self-joins)
        let scan_id = self.registry.allocate_id();
        let exec = PgScanExec::with_projection(
            scan_id,
            self.table_oid,
            Arc::clone(&self.schema), // full schema for decoding
            proj_schema,              // physical output schema
            proj_indices,
            Arc::clone(&self.registry),
        );
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
pub struct PgScanExec {
    // Full table schema (all columns) used to compute attribute metadata for decoding
    _full_schema: SchemaRef,
    // Projected schema exposed by this plan node
    proj_schema: SchemaRef,
    // Postgres table OID for this scan
    table_oid: u32,
    scan_id: ScanId,
    registry: Arc<ScanRegistry>,
    props: PlanProperties,
    // Attribute metadata for the full schema (decoder needs all columns to walk offsets)
    attrs_full: Arc<Vec<PgAttrMeta>>,
    // Indices of columns to project, in terms of full schema
    proj_indices: Arc<Vec<usize>>,
}

impl PgScanExec {
    pub fn with_projection(
        scan_id: ScanId,
        table_oid: u32,
        full_schema: SchemaRef,
        proj_schema: SchemaRef,
        proj_indices: Vec<usize>,
        registry: Arc<ScanRegistry>,
    ) -> Self {
        // Execution plan schema must match the projected schema
        let eq = EquivalenceProperties::new(proj_schema.clone());
        let props = PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let attrs_full = Arc::new(attrs_from_schema(&full_schema));
        Self {
            _full_schema: full_schema,
            proj_schema,
            table_oid,
            scan_id,
            registry,
            props,
            attrs_full,
            proj_indices: Arc::new(proj_indices),
        }
    }
}

impl DisplayAs for PgScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "PgScanExec: scan_id={}", self.scan_id),
            DisplayFormatType::Verbose => write!(
                f,
                "PgScanExec: scan_id={}, table_oid={}, proj_schema={:?}",
                self.scan_id, self.table_oid, self.proj_schema
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

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let rx = self.registry.take_receiver(self.scan_id);
        // Use connection id stored in registry to address per-connection slot buffers
        let conn_id = self.registry.conn_id();
        let stream = PgScanStream::new(
            Arc::clone(&self.proj_schema),
            Arc::clone(&self.attrs_full),
            Arc::clone(&self.proj_indices),
            rx,
            conn_id,
        );
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.proj_schema))
    }
}

pub struct PgScanStream {
    // Schema of output batches
    proj_schema: SchemaRef,
    // Full attribute metadata for decoding
    attrs_full: Arc<Vec<PgAttrMeta>>,
    // Projection indices into full schema
    proj_indices: Arc<Vec<usize>>,
    rx: Option<mpsc::Receiver<HeapBlock>>,
    conn_id: usize,
}

impl PgScanStream {
    pub fn new(
        proj_schema: SchemaRef,
        attrs_full: Arc<Vec<PgAttrMeta>>,
        proj_indices: Arc<Vec<usize>>,
        rx: Option<mpsc::Receiver<HeapBlock>>,
        conn_id: usize,
    ) -> Self {
        Self {
            proj_schema,
            attrs_full,
            proj_indices,
            rx,
            conn_id,
        }
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
                let page = unsafe { shm::block_slice(this.conn_id, block.slot_id as usize) };
                let _vis = if block.vis_len > 0 {
                    Some(unsafe {
                        shm::vis_slice(this.conn_id, block.slot_id as usize, block.vis_len as usize)
                    })
                } else {
                    None
                };

                // Prepare decoding metadata: attrs (precomputed once) and projection (all columns)
                let total_cols = this.proj_schema.fields().len();

                // Create a HeapPage view and iterate tuples
                let hp = unsafe { HeapPage::from_slice(page) };
                let Ok(hp) = hp else {
                    // On error decoding page, return empty batch for resilience
                    let batch = RecordBatch::new_empty(Arc::clone(&this.proj_schema));
                    return Poll::Ready(Some(Ok(batch)));
                };

                // Prepare per-column builders
                let col_count = total_cols;
                let mut builders = make_builders(&this.proj_schema, block.num_offsets as usize)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;
                // Use tuples_by_offset to iterate LP_NORMAL tuples in page order
                let mut pairs: Vec<(u16, u16)> = Vec::new();
                // Pre-scan to populate pairs and log LP_NORMAL count
                {
                    let _ = hp.tuples_by_offset(None, std::ptr::null_mut(), &mut pairs);
                }
                let pairs_len = pairs.len();
                // Create iterator borrowing the filled pairs slice
                let it = hp.tuples_by_offset(None, std::ptr::null_mut(), &mut pairs);
                tracing::trace!(
                    target = "executor::server",
                    blkno = block.blkno,
                    num_offsets = block.num_offsets,
                    lp_normal = pairs_len,
                    "pgscan: tuples_by_offset summary"
                );
                let page_hdr = unsafe { &*(page.as_ptr() as *const pg_sys::PageHeaderData) }
                    as *const pg_sys::PageHeaderData;
                let mut decoded_rows = 0usize;
                for tup in it {
                    // Decode projected columns for tuple using iterator over requested projection
                    let iter = unsafe {
                        decode_tuple_project(
                            page_hdr,
                            tup,
                            &this.attrs_full,
                            this.proj_indices.iter().copied(),
                        )
                    };
                    let Ok(mut iter) = iter else {
                        continue;
                    };
                    // Iterate over projected columns in order
                    for b in builders.iter_mut().take(total_cols) {
                        match iter.next() {
                            Some(Ok(v)) => append_scalar(b, v),
                            Some(Err(_e)) => append_null(b),
                            None => append_null(b),
                        }
                    }
                    decoded_rows += 1;
                }

                // Build Arrow arrays and RecordBatch
                let mut arrs = Vec::with_capacity(col_count);
                for b in builders.into_iter() {
                    arrs.push(finish_builder(b));
                }
                let rb = if this.proj_schema.fields().is_empty() {
                    // Special case: empty projection — use row_count to communicate the number of rows
                    let opts = RecordBatchOptions::new().with_row_count(Some(decoded_rows));
                    RecordBatch::try_new_with_options(Arc::clone(&this.proj_schema), vec![], &opts)
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!("{e}"))
                        })?
                } else {
                    RecordBatch::try_new(Arc::clone(&this.proj_schema), arrs).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("{e}"))
                    })?
                };
                tracing::trace!(
                    target = "executor::server",
                    rows = decoded_rows,
                    blkno = block.blkno,
                    "pgscan: decoded rows"
                );
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
            datafusion::arrow::datatypes::DataType::Boolean => PgAttrMeta {
                atttypid: pg_sys::BOOLOID,
                attlen: 1,
                attalign: b'c',
            },
            datafusion::arrow::datatypes::DataType::Utf8 => PgAttrMeta {
                atttypid: pg_sys::TEXTOID,
                attlen: -1,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Int16 => PgAttrMeta {
                atttypid: pg_sys::INT2OID,
                attlen: 2,
                attalign: b's',
            },
            datafusion::arrow::datatypes::DataType::Int32 => PgAttrMeta {
                atttypid: pg_sys::INT4OID,
                attlen: 4,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Int64 => PgAttrMeta {
                atttypid: pg_sys::INT8OID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Float32 => PgAttrMeta {
                atttypid: pg_sys::FLOAT4OID,
                attlen: 4,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Float64 => PgAttrMeta {
                atttypid: pg_sys::FLOAT8OID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Date32 => PgAttrMeta {
                atttypid: pg_sys::DATEOID,
                attlen: 4,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Time64(_) => PgAttrMeta {
                atttypid: pg_sys::TIMEOID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Timestamp(_, _) => PgAttrMeta {
                atttypid: pg_sys::TIMESTAMPOID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Interval(_) => PgAttrMeta {
                atttypid: pg_sys::INTERVALOID,
                attlen: 16,
                attalign: b'd',
            },
            _ => PgAttrMeta {
                atttypid: pg_sys::InvalidOid,
                attlen: -1,
                attalign: b'c',
            },
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
            ArrowDataType::Utf8 => {
                ColBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 8))
            }
            ArrowDataType::Date32 => ColBuilder::Date32(Date32Builder::with_capacity(capacity)),
            ArrowDataType::Time64(_) => {
                ColBuilder::Time64Us(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            ArrowDataType::Timestamp(_, _) => {
                ColBuilder::TsUs(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            ArrowDataType::Interval(_) => {
                ColBuilder::Interval(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported type in builder: {other:?}"
                )))
            }
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

fn append_null(b: &mut ColBuilder) {
    match b {
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
    }
}

fn finish_builder(b: ColBuilder) -> ArrayRef {
    match b {
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
    }
}

impl RecordBatchStream for PgScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.proj_schema)
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
            let table_oid = p.table_oid;
            f(id, table_oid)?;
        }
        for child in node.children() {
            visit(child, f)?;
        }
        Ok(())
    }
    visit(plan, &mut f)
}
