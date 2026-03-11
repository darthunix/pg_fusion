use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

mod decode;
mod page_iter;
mod visibility;

// use crate::shm;
use self::decode::{attrs_from_schema, decode_block_to_batch};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics,
};
use datafusion_catalog::{Session, TableProvider};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use futures::Stream;
use std::time::Instant;
use storage::heap::PgAttrMeta;
use tokio::sync::mpsc;

type HeapScanId = u64;

#[derive(Debug, Clone)]
pub struct HeapPageBlock {
    pub blkno: u32,
    pub num_offsets: u16,
    pub vis_len: u16,
    pub page: Vec<u8>,
    pub vis: Vec<u8>,
    pub recv_ts: Instant,
    // Note: page bytes and visibility bitmap reside in shared memory,
    // addressed by `slot_id` with `vis_len` bytes for visibility bitmap.
}

#[derive(Debug, Default)]
pub struct HeapScanRegistry {
    inner: Mutex<HashMap<HeapScanId, Entry>>,
    conn_id: usize,
    next_id: AtomicU64,
    next_slot: AtomicU16,
}

#[derive(Clone, Copy, Debug)]
pub struct HeapScanTelemetryHooks {
    pub enabled: fn() -> bool,
    pub now_ns: fn() -> u64,
    pub record_wait: fn(conn_id: usize, delta_ns: u64),
    pub record_decode: fn(conn_id: usize, delta_ns: u64),
}

fn telemetry_disabled() -> bool {
    false
}

fn telemetry_now_zero() -> u64 {
    0
}

fn telemetry_noop(_: usize, _: u64) {}

impl HeapScanTelemetryHooks {
    pub const fn disabled() -> Self {
        Self {
            enabled: telemetry_disabled,
            now_ns: telemetry_now_zero,
            record_wait: telemetry_noop,
            record_decode: telemetry_noop,
        }
    }
}

impl Default for HeapScanTelemetryHooks {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Debug)]
struct Entry {
    sender: mpsc::Sender<HeapPageBlock>,
    receiver: Option<mpsc::Receiver<HeapPageBlock>>,
    slot_id: u16,
}

impl HeapScanRegistry {
    pub fn new() -> Self {
        Self::with_conn(0)
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
    fn lock(&self) -> MutexGuard<HashMap<HeapScanId, Entry>> {
        // Avoid panics on poisoned mutex by recovering the inner state
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Reserve space for at least `additional` more scan entries.
    pub fn reserve(&self, additional: usize) {
        let mut map = self.lock();
        map.reserve(additional);
    }

    /// Allocate a unique scan identifier for this connection.
    pub fn allocate_id(&self) -> HeapScanId {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn register(&self, scan_id: HeapScanId, capacity: usize) -> mpsc::Sender<HeapPageBlock> {
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

    pub fn sender(&self, scan_id: HeapScanId) -> Option<mpsc::Sender<HeapPageBlock>> {
        let map = self.lock();
        map.get(&scan_id).map(|e| e.sender.clone())
    }

    pub fn take_receiver(&self, scan_id: HeapScanId) -> Option<mpsc::Receiver<HeapPageBlock>> {
        let mut map = self.lock();
        map.get_mut(&scan_id).and_then(|e| e.receiver.take())
    }

    /// Get the assigned slot id for this scan
    pub fn slot_for(&self, scan_id: HeapScanId) -> Option<u16> {
        let map = self.lock();
        map.get(&scan_id).map(|e| e.slot_id)
    }

    /// Close a single scan by dropping its sender; receiver will eventually see EOF.
    pub fn close(&self, scan_id: HeapScanId) {
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
pub struct HeapScanProvider {
    schema: SchemaRef,
    table_oid: u32,
    registry: Arc<HeapScanRegistry>,
    telemetry: HeapScanTelemetryHooks,
}

impl HeapScanProvider {
    pub fn new(table_oid: u32, schema: SchemaRef, registry: Arc<HeapScanRegistry>) -> Self {
        Self {
            schema,
            table_oid,
            registry,
            telemetry: HeapScanTelemetryHooks::default(),
        }
    }

    pub fn with_telemetry(
        table_oid: u32,
        schema: SchemaRef,
        registry: Arc<HeapScanRegistry>,
        telemetry: HeapScanTelemetryHooks,
    ) -> Self {
        Self {
            schema,
            table_oid,
            registry,
            telemetry,
        }
    }
}

#[async_trait]
impl TableProvider for HeapScanProvider {
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
        //
        // For identity/full projection we reuse the full schema and skip
        // allocating projection indices.
        let is_identity_projection = _projection.is_none_or(|ix| {
            ix.len() == self.schema.fields().len() && ix.iter().copied().eq(0..ix.len())
        });
        let (proj_schema, proj_indices) = if is_identity_projection {
            (Arc::clone(&self.schema), None)
        } else {
            let proj_indices = _projection.cloned().unwrap_or_default();
            let proj_fields: Vec<datafusion::arrow::datatypes::Field> = proj_indices
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect();
            (
                Arc::new(datafusion::arrow::datatypes::Schema::new(proj_fields)),
                Some(proj_indices),
            )
        };

        // Allocate a fresh, unique scan id for every scan instance (handles self-joins)
        let scan_id = self.registry.allocate_id();
        let exec = HeapScanExec::with_projection(
            scan_id,
            self.table_oid,
            Arc::clone(&self.schema), // full schema for decoding
            proj_schema,              // physical output schema
            proj_indices,
            Arc::clone(&self.registry),
            self.telemetry,
        );
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct HeapScanExec {
    // Full table schema (all columns) used to compute attribute metadata for decoding
    _full_schema: SchemaRef,
    // Projected schema exposed by this plan node
    proj_schema: SchemaRef,
    // Postgres table OID for this scan
    table_oid: u32,
    scan_id: HeapScanId,
    registry: Arc<HeapScanRegistry>,
    props: PlanProperties,
    // Attribute metadata for the full schema (decoder needs all columns to walk offsets)
    attrs_full: Arc<Vec<PgAttrMeta>>,
    // Indices of columns to project, in terms of full schema.
    // `None` means identity/full projection.
    proj_indices: Option<Arc<[usize]>>,
    telemetry: HeapScanTelemetryHooks,
}

impl HeapScanExec {
    fn with_projection(
        scan_id: HeapScanId,
        table_oid: u32,
        full_schema: SchemaRef,
        proj_schema: SchemaRef,
        proj_indices: Option<Vec<usize>>,
        registry: Arc<HeapScanRegistry>,
        telemetry: HeapScanTelemetryHooks,
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
            proj_indices: proj_indices.map(Arc::from),
            telemetry,
        }
    }
}

impl DisplayAs for HeapScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "HeapScanExec: scan_id={}", self.scan_id),
            DisplayFormatType::Verbose => write!(
                f,
                "HeapScanExec: scan_id={}, table_oid={}, proj_schema={:?}",
                self.scan_id, self.table_oid, self.proj_schema
            ),
        }
    }
}

impl ExecutionPlan for HeapScanExec {
    fn name(&self) -> &str {
        "HeapScanExec"
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
                "HeapScanExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let rx = self.registry.take_receiver(self.scan_id);
        let stream = HeapScanStream::new(
            Arc::clone(&self.proj_schema),
            Arc::clone(&self.attrs_full),
            self.proj_indices.as_ref().map(Arc::clone),
            self.registry.conn_id(),
            self.telemetry,
            rx,
        );
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.proj_schema))
    }
}

struct HeapScanStream {
    // Schema of output batches
    proj_schema: SchemaRef,
    // Full attribute metadata for decoding
    attrs_full: Arc<Vec<PgAttrMeta>>,
    // Projection indices into full schema. `None` means identity/full projection.
    proj_indices: Option<Arc<[usize]>>,
    conn_id: usize,
    telemetry: HeapScanTelemetryHooks,
    pending_wait_start_ns: Option<u64>,
    rx: Option<mpsc::Receiver<HeapPageBlock>>,
}

impl HeapScanStream {
    fn new(
        proj_schema: SchemaRef,
        attrs_full: Arc<Vec<PgAttrMeta>>,
        proj_indices: Option<Arc<[usize]>>,
        conn_id: usize,
        telemetry: HeapScanTelemetryHooks,
        rx: Option<mpsc::Receiver<HeapPageBlock>>,
    ) -> Self {
        Self {
            proj_schema,
            attrs_full,
            proj_indices,
            conn_id,
            telemetry,
            pending_wait_start_ns: None,
            rx,
        }
    }
}

impl Stream for HeapScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(rx) = this.rx.as_mut() else {
            return Poll::Ready(None);
        };
        match rx.poll_recv(cx) {
            Poll::Ready(Some(block)) => {
                if let Some(wait_t0) = this.pending_wait_start_ns.take() {
                    if (this.telemetry.enabled)() {
                        (this.telemetry.record_wait)(
                            this.conn_id,
                            (this.telemetry.now_ns)().saturating_sub(wait_t0),
                        );
                    }
                }
                let decode_t0 = (this.telemetry.enabled)().then(|| (this.telemetry.now_ns)());
                let decoded = match decode_block_to_batch(
                    &block,
                    &this.proj_schema,
                    this.attrs_full.as_ref(),
                    this.proj_indices.as_deref(),
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        if let Some(decode_t0) = decode_t0 {
                            (this.telemetry.record_decode)(
                                this.conn_id,
                                (this.telemetry.now_ns)().saturating_sub(decode_t0),
                            );
                        }
                        return Poll::Ready(Some(Err(e)));
                    }
                };
                if tracing::enabled!(target: "executor::server", tracing::Level::TRACE) {
                    let elapsed_us = block.recv_ts.elapsed().as_micros() as u64;
                    tracing::trace!(
                        target = "executor::server",
                        rows = decoded.decoded_rows,
                        blkno = block.blkno,
                        elapsed_us = elapsed_us,
                        "heap_scan: decoded rows"
                    );
                }
                if let Some(decode_t0) = decode_t0 {
                    (this.telemetry.record_decode)(
                        this.conn_id,
                        (this.telemetry.now_ns)().saturating_sub(decode_t0),
                    );
                }
                Poll::Ready(Some(Ok(decoded.batch)))
            }
            Poll::Ready(None) => {
                this.pending_wait_start_ns = None;
                Poll::Ready(None)
            }
            Poll::Pending => {
                if (this.telemetry.enabled)() && this.pending_wait_start_ns.is_none() {
                    this.pending_wait_start_ns = Some((this.telemetry.now_ns)());
                }
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for HeapScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.proj_schema)
    }
}

pub fn heap_attrs_from_schema(schema: &SchemaRef) -> Vec<PgAttrMeta> {
    attrs_from_schema(schema)
}

pub fn decode_heap_page_block(
    block: &HeapPageBlock,
    proj_schema: &SchemaRef,
    attrs_full: &[PgAttrMeta],
    proj_indices: Option<&[usize]>,
) -> DFResult<(RecordBatch, usize)> {
    let out = decode_block_to_batch(block, proj_schema, attrs_full, proj_indices)?;
    Ok((out.batch, out.decoded_rows))
}

pub fn count_heap_scans(plan: &Arc<dyn ExecutionPlan>) -> usize {
    fn visit(node: &Arc<dyn ExecutionPlan>) -> usize {
        let mut n = 0usize;
        if node.as_any().downcast_ref::<HeapScanExec>().is_some() {
            n += 1;
        }
        for child in node.children() {
            n += visit(child);
        }
        n
    }
    visit(plan)
}

pub fn for_each_heap_scan<F, E>(plan: &Arc<dyn ExecutionPlan>, mut f: F) -> Result<(), E>
where
    F: FnMut(HeapScanId, u32) -> Result<(), E>,
{
    fn visit<F, E>(node: &Arc<dyn ExecutionPlan>, f: &mut F) -> Result<(), E>
    where
        F: FnMut(HeapScanId, u32) -> Result<(), E>,
    {
        if let Some(p) = node.as_any().downcast_ref::<HeapScanExec>() {
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
