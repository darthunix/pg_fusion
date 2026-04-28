use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, CardinalityEffect, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_expr::logical_plan::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_expr::Expr;
use futures::lock::Mutex as AsyncMutex;
use futures::{stream, StreamExt};

use crate::materialize_record_batch;

/// Stable identifier for one query-local materialized CTE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgCteId(u64);

impl PgCteId {
    /// Create a new CTE id.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw CTE id value.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for PgCteId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

/// DataFusion custom logical node for one read of a materialized CTE.
#[derive(Debug, Clone)]
pub struct PgCteRefNode {
    cte_id: PgCteId,
    name: String,
    input: LogicalPlan,
    schema: DFSchemaRef,
    projection: Option<Vec<usize>>,
    fetch: Option<usize>,
}

impl PgCteRefNode {
    pub fn new(
        cte_id: impl Into<PgCteId>,
        name: impl Into<String>,
        input: LogicalPlan,
        schema: DFSchemaRef,
        projection: Option<Vec<usize>>,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            cte_id: cte_id.into(),
            name: name.into(),
            input,
            schema,
            projection,
            fetch,
        }
    }

    pub fn cte_id(&self) -> PgCteId {
        self.cte_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(self),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::RecordBatchStream;
    use futures::{executor::block_on, Stream, TryStreamExt};

    #[derive(Debug)]
    struct CountingExec {
        calls: Arc<AtomicUsize>,
        batch: RecordBatch,
        props: PlanProperties,
    }

    impl CountingExec {
        fn new(calls: Arc<AtomicUsize>, batch: RecordBatch) -> Self {
            let props = PlanProperties::new(
                EquivalenceProperties::new(batch.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                calls,
                batch,
                props,
            }
        }
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &str {
            "CountingExec"
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
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if children.is_empty() {
                Ok(self)
            } else {
                Err(DataFusionError::Plan("CountingExec has no children".into()))
            }
        }

        fn execute(
            &self,
            partition: usize,
            _ctx: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if partition != 0 {
                return Err(DataFusionError::Plan(format!(
                    "CountingExec exposes one partition, got {partition}"
                )));
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(SingleBatchStream {
                schema: self.batch.schema(),
                batch: Some(self.batch.clone()),
            }))
        }

        fn statistics(&self) -> Result<Statistics> {
            Ok(Statistics::new_unknown(&self.batch.schema()))
        }
    }

    struct SingleBatchStream {
        schema: SchemaRef,
        batch: Option<RecordBatch>,
    }

    impl Stream for SingleBatchStream {
        type Item = Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.batch.take().map(Ok))
        }
    }

    impl RecordBatchStream for SingleBatchStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[test]
    fn cte_exec_materializes_input_once_for_multiple_reads() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let input =
            Arc::new(CountingExec::new(Arc::clone(&calls), batch)) as Arc<dyn ExecutionPlan>;
        let state = Arc::new(MaterializedCteState::default());
        let exec = MaterializedCteExec::new(
            PgCteId::new(1),
            "u".into(),
            input,
            Arc::clone(&schema),
            None,
            None,
            state,
        );

        for _ in 0..2 {
            let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
            let batches: Vec<RecordBatch> = block_on(stream.try_collect()).unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 3);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

impl PartialEq for PgCteRefNode {
    fn eq(&self, other: &Self) -> bool {
        self.cte_id == other.cte_id
            && self.name == other.name
            && self.projection == other.projection
            && self.fetch == other.fetch
            && self.schema.as_ref() == other.schema.as_ref()
    }
}

impl Eq for PgCteRefNode {}

impl Hash for PgCteRefNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cte_id.hash(state);
        self.name.hash(state);
        self.projection.hash(state);
        self.fetch.hash(state);
        self.schema.fields().len().hash(state);
        for (qualifier, field) in self.schema.iter() {
            qualifier.hash(state);
            field.name().hash(state);
            field.data_type().hash(state);
            field.is_nullable().hash(state);
        }
    }
}

impl PartialOrd for PgCteRefNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            (self.cte_id, &self.name, &self.projection, &self.fetch).cmp(&(
                other.cte_id,
                &other.name,
                &other.projection,
                &other.fetch,
            )),
        )
    }
}

impl UserDefinedLogicalNodeCore for PgCteRefNode {
    fn name(&self) -> &str {
        "PgCteRef"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PgCteRef: cte_id={}, name={}, projection={:?}, fetch={:?}",
            self.cte_id.get(),
            self.name,
            self.projection,
            self.fetch
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() {
            return Err(DataFusionError::Plan(
                "PgCteRefNode does not expose rewritable expressions".into(),
            ));
        }
        let [input] = inputs
            .try_into()
            .map_err(|_| DataFusionError::Plan("PgCteRefNode expects exactly one input".into()))?;
        Ok(Self {
            input,
            ..self.clone()
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CteMaterializationRegistry {
    states: Arc<Mutex<HashMap<PgCteId, Arc<MaterializedCteState>>>>,
}

impl CteMaterializationRegistry {
    pub(crate) fn state_for(&self, cte_id: PgCteId) -> Arc<MaterializedCteState> {
        let mut states = self
            .states
            .lock()
            .expect("CTE materialization registry lock poisoned");
        Arc::clone(
            states
                .entry(cte_id)
                .or_insert_with(|| Arc::new(MaterializedCteState::default())),
        )
    }
}

#[derive(Debug, Default)]
pub struct MaterializedCteState {
    result: AsyncMutex<Option<MaterializedCteResult>>,
}

#[derive(Debug, Clone)]
enum MaterializedCteResult {
    Ready(Arc<Vec<RecordBatch>>),
    Failed(String),
}

impl MaterializedCteState {
    async fn get_or_compute(
        &self,
        input: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
    ) -> Result<Arc<Vec<RecordBatch>>> {
        let mut result = self.result.lock().await;
        match result.as_ref() {
            Some(MaterializedCteResult::Ready(batches)) => {
                return Ok(Arc::clone(batches));
            }
            Some(MaterializedCteResult::Failed(message)) => {
                return Err(DataFusionError::Execution(message.clone()));
            }
            None => {}
        }

        match collect_owned_batches(input, ctx).await {
            Ok(batches) => {
                let batches = Arc::new(batches);
                *result = Some(MaterializedCteResult::Ready(Arc::clone(&batches)));
                Ok(batches)
            }
            Err(error) => {
                let message = error.to_string();
                *result = Some(MaterializedCteResult::Failed(message.clone()));
                Err(DataFusionError::Execution(message))
            }
        }
    }
}

async fn collect_owned_batches(
    input: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = input.execute(0, ctx)?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(materialize_record_batch(batch?)?);
    }
    Ok(batches)
}

/// Physical read of a materialized CTE.
#[derive(Debug)]
pub struct MaterializedCteExec {
    cte_id: PgCteId,
    name: String,
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    fetch: Option<usize>,
    state: Arc<MaterializedCteState>,
    props: PlanProperties,
}

impl MaterializedCteExec {
    pub(crate) fn new(
        cte_id: PgCteId,
        name: String,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        fetch: Option<usize>,
        state: Arc<MaterializedCteState>,
    ) -> Self {
        let props = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            cte_id,
            name,
            input,
            output_schema,
            projection,
            fetch,
            state,
            props,
        }
    }
}

impl DisplayAs for MaterializedCteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "CteScanExec: {}", self.name),
            DisplayFormatType::Verbose => write!(
                f,
                "CteScanExec: cte_id={}, name={}, projection={:?}, fetch={:?}",
                self.cte_id.get(),
                self.name,
                self.projection,
                self.fetch
            ),
        }
    }
}

impl ExecutionPlan for MaterializedCteExec {
    fn name(&self) -> &str {
        "CteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children
            .try_into()
            .map_err(|_| DataFusionError::Plan("CteScanExec expects exactly one child".into()))?;
        Ok(Arc::new(Self::new(
            self.cte_id,
            self.name.clone(),
            input,
            Arc::clone(&self.output_schema),
            self.projection.clone(),
            self.fetch,
            Arc::clone(&self.state),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan(format!(
                "CteScanExec exposes one partition, got partition {partition}"
            )));
        }

        let schema = Arc::clone(&self.output_schema);
        let state = CteStreamState::NeedBatches {
            input: Arc::clone(&self.input),
            ctx,
            shared: Arc::clone(&self.state),
            projection: self.projection.clone(),
            fetch: self.fetch,
        };
        let stream = stream::try_unfold(state, next_cte_batch);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.output_schema))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        if self.fetch.is_some() {
            CardinalityEffect::LowerEqual
        } else {
            CardinalityEffect::Equal
        }
    }
}

enum CteStreamState {
    NeedBatches {
        input: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
        shared: Arc<MaterializedCteState>,
        projection: Option<Vec<usize>>,
        fetch: Option<usize>,
    },
    Emit {
        batches: Arc<Vec<RecordBatch>>,
        index: usize,
        projection: Option<Vec<usize>>,
        remaining: Option<usize>,
    },
}

async fn next_cte_batch(state: CteStreamState) -> Result<Option<(RecordBatch, CteStreamState)>> {
    match state {
        CteStreamState::NeedBatches {
            input,
            ctx,
            shared,
            projection,
            fetch,
        } => {
            let batches = shared.get_or_compute(input, ctx).await?;
            emit_next_batch(batches, 0, projection, fetch)
        }
        CteStreamState::Emit {
            batches,
            index,
            projection,
            remaining,
        } => emit_next_batch(batches, index, projection, remaining),
    }
}

fn emit_next_batch(
    batches: Arc<Vec<RecordBatch>>,
    mut index: usize,
    projection: Option<Vec<usize>>,
    mut remaining: Option<usize>,
) -> Result<Option<(RecordBatch, CteStreamState)>> {
    loop {
        if matches!(remaining, Some(0)) || index >= batches.len() {
            return Ok(None);
        }

        let batch = &batches[index];
        index += 1;
        let Some(batch) = project_and_slice_batch(batch, projection.as_deref(), &mut remaining)?
        else {
            continue;
        };
        let next = CteStreamState::Emit {
            batches,
            index,
            projection,
            remaining,
        };
        return Ok(Some((batch, next)));
    }
}

fn project_and_slice_batch(
    batch: &RecordBatch,
    projection: Option<&[usize]>,
    remaining: &mut Option<usize>,
) -> Result<Option<RecordBatch>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }

    let mut batch = if let Some(projection) = projection {
        batch.project(projection).map_err(DataFusionError::from)?
    } else {
        batch.clone()
    };

    if let Some(rows) = remaining.as_mut() {
        if *rows == 0 {
            return Ok(None);
        }
        if batch.num_rows() > *rows {
            batch = batch.slice(0, *rows);
            *rows = 0;
        } else {
            *rows -= batch.num_rows();
        }
    }

    Ok(Some(batch))
}
