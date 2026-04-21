use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use scan_node::{PgScanExecFactory, PgScanId, PgScanSpec};

/// Worker request for one backend-owned scan.
///
/// The worker-side physical scan only receives stable scan identity and output
/// schema. Backend-only state such as snapshots, compiled SQL execution, and
/// table access remains behind `scan_id`.
#[derive(Debug, Clone)]
pub struct OpenScanRequest {
    pub session_epoch: u64,
    pub scan_id: PgScanId,
    pub output_schema: SchemaRef,
    pub page_kind: transfer::MessageKind,
    pub page_flags: u16,
}

/// Runtime-specific source of scan batches.
///
/// Production implementations are expected to send `WorkerToBackend::OpenScan`
/// over the active control slot, drive `scan_flow::WorkerScanRole`, and import
/// pages with `ArrowPageDecoder`. Tests can provide an in-memory source.
pub trait ScanBatchSource: std::fmt::Debug + Send + Sync {
    fn open_scan(&self, request: OpenScanRequest) -> DFResult<SendableRecordBatchStream>;
}

#[derive(Clone)]
pub struct WorkerPgScanExecFactory {
    session_epoch: u64,
    source: Arc<dyn ScanBatchSource>,
    page_kind: transfer::MessageKind,
    page_flags: u16,
}

impl WorkerPgScanExecFactory {
    pub fn new(
        session_epoch: u64,
        source: Arc<dyn ScanBatchSource>,
        page_kind: transfer::MessageKind,
        page_flags: u16,
    ) -> Self {
        Self {
            session_epoch,
            source,
            page_kind,
            page_flags,
        }
    }
}

impl std::fmt::Debug for WorkerPgScanExecFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPgScanExecFactory")
            .field("session_epoch", &self.session_epoch)
            .field("page_kind", &self.page_kind)
            .field("page_flags", &self.page_flags)
            .finish_non_exhaustive()
    }
}

impl PgScanExecFactory for WorkerPgScanExecFactory {
    fn create(&self, spec: Arc<PgScanSpec>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(WorkerPgScanExec::new(
            self.session_epoch,
            spec,
            Arc::clone(&self.source),
            self.page_kind,
            self.page_flags,
        )))
    }
}

#[derive(Debug)]
pub struct WorkerPgScanExec {
    session_epoch: u64,
    spec: Arc<PgScanSpec>,
    output_schema: SchemaRef,
    source: Arc<dyn ScanBatchSource>,
    page_kind: transfer::MessageKind,
    page_flags: u16,
    props: PlanProperties,
}

impl WorkerPgScanExec {
    pub fn new(
        session_epoch: u64,
        spec: Arc<PgScanSpec>,
        source: Arc<dyn ScanBatchSource>,
        page_kind: transfer::MessageKind,
        page_flags: u16,
    ) -> Self {
        let output_schema = spec.arrow_schema();
        let props = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            session_epoch,
            spec,
            output_schema,
            source,
            page_kind,
            page_flags,
            props,
        }
    }

    pub fn scan_id(&self) -> PgScanId {
        self.spec.scan_id
    }

    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }
}

impl DisplayAs for WorkerPgScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "WorkerPgScanExec: scan_id={}", self.spec.scan_id.get())
            }
            DisplayFormatType::Verbose => write!(
                f,
                "WorkerPgScanExec: session_epoch={}, scan_id={}, table_oid={}, schema={:?}",
                self.session_epoch,
                self.spec.scan_id.get(),
                self.spec.table_oid,
                self.output_schema
            ),
        }
    }
}

impl ExecutionPlan for WorkerPgScanExec {
    fn name(&self) -> &str {
        "WorkerPgScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "WorkerPgScanExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan(format!(
                "WorkerPgScanExec exposes one partition, got partition {partition}"
            )));
        }

        self.source.open_scan(OpenScanRequest {
            session_epoch: self.session_epoch,
            scan_id: self.spec.scan_id,
            output_schema: Arc::clone(&self.output_schema),
            page_kind: self.page_kind,
            page_flags: self.page_flags,
        })
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.output_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::RecordBatchStream;
    use datafusion_common::{DFSchema, TableReference};
    use futures::Stream;
    use scan_sql::{CompiledScan, PgRelation};

    #[derive(Debug)]
    struct OneBatchStream {
        schema: SchemaRef,
        batch: Option<RecordBatch>,
    }

    impl Stream for OneBatchStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.batch.take().map(Ok))
        }
    }

    impl RecordBatchStream for OneBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[derive(Debug)]
    struct RecordingSource {
        requests: Mutex<Vec<OpenScanRequest>>,
    }

    impl RecordingSource {
        fn new() -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    impl ScanBatchSource for RecordingSource {
        fn open_scan(&self, request: OpenScanRequest) -> DFResult<SendableRecordBatchStream> {
            let schema = Arc::clone(&request.output_schema);
            self.requests.lock().unwrap().push(request);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
            )?;
            Ok(Box::pin(OneBatchStream {
                schema,
                batch: Some(batch),
            }))
        }
    }

    fn spec(scan_id: u64) -> Arc<PgScanSpec> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let df_schema = DFSchema::try_from_qualified_schema(
            TableReference::partial("public", "users"),
            &schema,
        )
        .unwrap();
        let scan = CompiledScan {
            sql: "SELECT id FROM public.users".into(),
            requested_limit: None,
            sql_limit: None,
            selected_columns: vec![0],
            output_columns: vec![0],
            filter_only_columns: Vec::new(),
            residual_filter_columns: Vec::new(),
            pushed_filters: Vec::new(),
            residual_filters: Vec::new(),
            all_filters_compiled: true,
            uses_dummy_projection: false,
        };

        Arc::new(
            PgScanSpec::try_new(
                scan_id,
                42,
                PgRelation::new(Some("public"), "users"),
                &df_schema,
                scan,
            )
            .unwrap(),
        )
    }

    #[test]
    fn factory_builds_worker_pg_scan_exec() {
        let source = Arc::new(RecordingSource::new());
        let factory = WorkerPgScanExecFactory::new(99, source, 0x4152, 0);
        let plan = factory.create(spec(7)).unwrap();

        let exec = plan
            .as_any()
            .downcast_ref::<WorkerPgScanExec>()
            .expect("worker scan exec");
        assert_eq!(exec.session_epoch(), 99);
        assert_eq!(exec.scan_id(), PgScanId::new(7));
        assert_eq!(exec.name(), "WorkerPgScanExec");
    }

    #[test]
    fn execute_uses_scan_id_and_schema_from_spec() {
        let source = Arc::new(RecordingSource::new());
        let exec = WorkerPgScanExec::new(100, spec(8), source.clone(), 0x4152, 0);
        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();

        assert_eq!(stream.schema().fields().len(), 1);
        let requests = source.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].session_epoch, 100);
        assert_eq!(requests[0].scan_id, PgScanId::new(8));
        assert_eq!(requests[0].page_kind, 0x4152);
        assert_eq!(requests[0].page_flags, 0);
    }

    #[test]
    fn execute_rejects_nonzero_partition() {
        let source = Arc::new(RecordingSource::new());
        let exec = WorkerPgScanExec::new(100, spec(9), source, 0x4152, 0);
        let err = match exec.execute(1, Arc::new(TaskContext::default())) {
            Ok(_) => panic!("nonzero partition should fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("one partition"));
    }
}
