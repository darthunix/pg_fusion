use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
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
    // addressed by `slot_id`. No heap allocations here.
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

    pub fn register(&self, scan_id: ScanId, capacity: usize) -> mpsc::Sender<HeapBlock> {
        let (tx, rx) = mpsc::channel(capacity);
        let mut map = self.inner.lock().unwrap();
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
        let map = self.inner.lock().unwrap();
        map.get(&scan_id).map(|e| e.sender.clone())
    }

    pub fn take_receiver(&self, scan_id: ScanId) -> Option<mpsc::Receiver<HeapBlock>> {
        let mut map = self.inner.lock().unwrap();
        map.get_mut(&scan_id).and_then(|e| e.receiver.take())
    }
}

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
        Ok(vec![])
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
        Self {
            schema,
            scan_id,
            registry,
            props,
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
        let stream = PgScanStream::new(Arc::clone(&self.schema), rx);
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

pub struct PgScanStream {
    schema: SchemaRef,
    rx: Option<mpsc::Receiver<HeapBlock>>,
}

impl PgScanStream {
    pub fn new(schema: SchemaRef, rx: Option<mpsc::Receiver<HeapBlock>>) -> Self {
        Self { schema, rx }
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
            Poll::Ready(Some(_block)) => {
                // TODO: decode HeapBlock into RecordBatch using storage::heap
                let batch = RecordBatch::new_empty(Arc::clone(&this.schema));
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for PgScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
