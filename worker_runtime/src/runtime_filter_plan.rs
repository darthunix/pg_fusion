use std::any::Any;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, Int16Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::DataType;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use futures::{ready, Stream, StreamExt};
use runtime_filter::{
    hash_int_key, RuntimeFilterBuildHandle, RuntimeFilterKeyType, RuntimeFilterPool,
    RuntimeFilterTarget,
};
use runtime_metrics::{MetricId, RuntimeMetrics};

use crate::scan_exec::WorkerPgScanExec;

pub(crate) fn install_runtime_filters(
    plan: Arc<dyn ExecutionPlan>,
    session_epoch: u64,
    pool: RuntimeFilterPool,
    metrics: RuntimeMetrics,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    let rewritten_children = children
        .into_iter()
        .map(|child| install_runtime_filters(Arc::clone(child), session_epoch, pool, metrics))
        .collect::<DFResult<Vec<_>>>()?;
    let plan = if rewritten_children.is_empty() {
        plan
    } else {
        plan.with_new_children(rewritten_children)?
    };

    let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    maybe_wrap_hash_join(join, session_epoch, pool, metrics).map(|wrapped| wrapped.unwrap_or(plan))
}

fn maybe_wrap_hash_join(
    join: &HashJoinExec,
    session_epoch: u64,
    pool: RuntimeFilterPool,
    metrics: RuntimeMetrics,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    if *join.join_type() != JoinType::Inner || join.null_equals_null() {
        return Ok(None);
    }
    if join.left().output_partitioning().partition_count() != 1 {
        return Ok(None);
    }
    let [(left_expr, right_expr)] = join.on() else {
        return Ok(None);
    };
    let Some(left_col) = left_expr.as_any().downcast_ref::<Column>() else {
        return Ok(None);
    };
    let Some(right_col) = right_expr.as_any().downcast_ref::<Column>() else {
        return Ok(None);
    };
    let Some(right_scan) = join.right().as_any().downcast_ref::<WorkerPgScanExec>() else {
        return Ok(None);
    };
    let Some(key_type) = key_type_for(join.right().schema().field(right_col.index()).data_type())
    else {
        return Ok(None);
    };

    let target = RuntimeFilterTarget {
        session_epoch,
        scan_id: right_scan.scan_id().get(),
        output_column: right_col.index() as u32,
        key_type,
    };
    let Some(handle) = pool
        .allocate_build(target)
        .map_err(|err| DataFusionError::Execution(err.to_string()))?
    else {
        metrics.increment(MetricId::RuntimeFilterPoolExhaustedTotal);
        return Ok(None);
    };
    metrics.increment(MetricId::RuntimeFilterAllocatedTotal);

    let left = Arc::new(RuntimeFilterBuildExec::new(
        Arc::clone(join.left()),
        left_col.index(),
        key_type,
        handle,
        metrics,
    ));
    Ok(Some(Arc::new(HashJoinExec::try_new(
        left,
        Arc::clone(join.right()),
        join.on().to_vec(),
        join.filter().cloned(),
        join.join_type(),
        join.projection.clone(),
        *join.partition_mode(),
        join.null_equals_null(),
    )?)))
}

fn key_type_for(data_type: &DataType) -> Option<RuntimeFilterKeyType> {
    match data_type {
        DataType::Int16 => Some(RuntimeFilterKeyType::Int16),
        DataType::Int32 => Some(RuntimeFilterKeyType::Int32),
        DataType::Int64 => Some(RuntimeFilterKeyType::Int64),
        _ => None,
    }
}

#[derive(Debug)]
struct RuntimeFilterBuildExec {
    input: Arc<dyn ExecutionPlan>,
    key_index: usize,
    key_type: RuntimeFilterKeyType,
    state: Arc<RuntimeFilterBuildState>,
    props: PlanProperties,
}

impl RuntimeFilterBuildExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        key_index: usize,
        key_type: RuntimeFilterKeyType,
        handle: RuntimeFilterBuildHandle,
        metrics: RuntimeMetrics,
    ) -> Self {
        let props = input.properties().clone();
        Self {
            input,
            key_index,
            key_type,
            state: Arc::new(RuntimeFilterBuildState {
                handle,
                metrics,
                closed: AtomicBool::new(false),
            }),
            props,
        }
    }
}

impl DisplayAs for RuntimeFilterBuildExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RuntimeFilterBuildExec: key_index={}", self.key_index)
            }
        }
    }
}

impl ExecutionPlan for RuntimeFilterBuildExec {
    fn name(&self) -> &str {
        "RuntimeFilterBuildExec"
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "RuntimeFilterBuildExec expects exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            props: children[0].properties().clone(),
            input: Arc::clone(&children[0]),
            key_index: self.key_index,
            key_type: self.key_type,
            state: Arc::clone(&self.state),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, ctx)?;
        Ok(Box::pin(RuntimeFilterBuildStream {
            schema: input.schema(),
            input,
            key_index: self.key_index,
            key_type: self.key_type,
            state: Arc::clone(&self.state),
        }))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        self.input.statistics()
    }
}

#[derive(Debug)]
struct RuntimeFilterBuildState {
    handle: RuntimeFilterBuildHandle,
    metrics: RuntimeMetrics,
    closed: AtomicBool,
}

impl RuntimeFilterBuildState {
    fn insert_batch(
        &self,
        batch: &RecordBatch,
        key_index: usize,
        key_type: RuntimeFilterKeyType,
    ) -> DFResult<()> {
        let rows = match key_type {
            RuntimeFilterKeyType::Int16 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Int16 build key had non-Int16 array".into(),
                        )
                    })?;
                insert_ints(array, |idx| array.value(idx) as i64, &self.handle)?
            }
            RuntimeFilterKeyType::Int32 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Int32 build key had non-Int32 array".into(),
                        )
                    })?;
                insert_ints(array, |idx| array.value(idx) as i64, &self.handle)?
            }
            RuntimeFilterKeyType::Int64 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Int64 build key had non-Int64 array".into(),
                        )
                    })?;
                insert_ints(array, |idx| array.value(idx), &self.handle)?
            }
        };
        self.metrics
            .add(MetricId::RuntimeFilterBuildRowsTotal, rows);
        Ok(())
    }

    fn publish_ready(&self) -> DFResult<()> {
        if !self.closed.load(Ordering::Acquire) {
            if let Err(err) = self.handle.publish_ready() {
                self.disable_build();
                return Err(DataFusionError::Execution(err.to_string()));
            }
            self.closed.store(true, Ordering::Release);
            self.metrics.increment(MetricId::RuntimeFilterReadyTotal);
        }
        Ok(())
    }

    fn disable_build(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let _ = self.handle.disable_build();
        }
    }
}

impl Drop for RuntimeFilterBuildState {
    fn drop(&mut self) {
        self.disable_build();
    }
}

struct RuntimeFilterBuildStream {
    schema: arrow_schema::SchemaRef,
    input: SendableRecordBatchStream,
    key_index: usize,
    key_type: RuntimeFilterKeyType,
    state: Arc<RuntimeFilterBuildState>,
}

impl Stream for RuntimeFilterBuildStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if let Err(err) = self
                    .state
                    .insert_batch(&batch, self.key_index, self.key_type)
                {
                    self.state.disable_build();
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(err)) => {
                self.state.disable_build();
                Poll::Ready(Some(Err(err)))
            }
            None => Poll::Ready(self.state.publish_ready().err().map(Err)),
        }
    }
}

impl RecordBatchStream for RuntimeFilterBuildStream {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for RuntimeFilterBuildStream {
    fn drop(&mut self) {
        self.state.disable_build();
    }
}

fn insert_ints<A>(
    array: &A,
    value: impl Fn(usize) -> i64,
    handle: &RuntimeFilterBuildHandle,
) -> DFResult<u64>
where
    A: Array,
{
    let mut rows = 0_u64;
    for index in 0..array.len() {
        if !array.is_null(index) {
            handle
                .insert_hash(hash_int_key(value(index)))
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
            rows += 1;
        }
    }
    Ok(rows)
}
