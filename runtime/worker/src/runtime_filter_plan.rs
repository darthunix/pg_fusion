use std::any::Any;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::{
    Array, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    RecordBatch, StringViewArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, NullEquality, Result as DFResult};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use filter::{
    hash_bool_key, hash_bytes_key, hash_decimal128_key, hash_float32_key, hash_float64_key,
    hash_int_key, hash_interval_month_day_nano_key, RuntimeFilterBuildHandle, RuntimeFilterExecId,
    RuntimeFilterKeyType, RuntimeFilterPool, RuntimeFilterTarget,
};
use futures::{ready, Stream, StreamExt};
use metrics::{MetricId, RuntimeMetrics};

use crate::scan_exec::WorkerPgScanExec;

pub(crate) fn install_runtime_filters(
    plan: Arc<dyn ExecutionPlan>,
    exec_id: RuntimeFilterExecId,
    pool: RuntimeFilterPool,
    metrics: RuntimeMetrics,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    let rewritten_children = children
        .into_iter()
        .map(|child| install_runtime_filters(Arc::clone(child), exec_id, pool, metrics))
        .collect::<DFResult<Vec<_>>>()?;
    let plan = if rewritten_children.is_empty() {
        plan
    } else {
        plan.with_new_children(rewritten_children)?
    };

    let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    maybe_wrap_hash_join(join, exec_id, pool, metrics).map(|wrapped| wrapped.unwrap_or(plan))
}

fn maybe_wrap_hash_join(
    join: &HashJoinExec,
    exec_id: RuntimeFilterExecId,
    pool: RuntimeFilterPool,
    metrics: RuntimeMetrics,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    if *join.join_type() != JoinType::Inner
        || join.null_equality() != NullEquality::NullEqualsNothing
        || join.null_aware
    {
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
    let Some(right_scan) = runtime_filter_probe_scan(join.right()) else {
        return Ok(None);
    };
    let Some(key_type) = key_type_for_pair(
        join.left().schema().field(left_col.index()).data_type(),
        join.right().schema().field(right_col.index()).data_type(),
    ) else {
        return Ok(None);
    };

    let target = RuntimeFilterTarget {
        exec_id,
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
        join.projection
            .as_ref()
            .map(|projection| projection.to_vec()),
        *join.partition_mode(),
        join.null_equality(),
        join.null_aware,
    )?)))
}

fn runtime_filter_probe_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&WorkerPgScanExec> {
    if let Some(scan) = plan.as_any().downcast_ref::<WorkerPgScanExec>() {
        return Some(scan);
    }
    if let Some(cooperative) = plan.as_any().downcast_ref::<CooperativeExec>() {
        return runtime_filter_probe_scan(cooperative.input());
    }
    None
}

fn key_type_for(data_type: &DataType) -> Option<RuntimeFilterKeyType> {
    match data_type {
        DataType::Boolean => Some(RuntimeFilterKeyType::Boolean),
        DataType::Int16 => Some(RuntimeFilterKeyType::Int16),
        DataType::Int32 => Some(RuntimeFilterKeyType::Int32),
        DataType::Int64 => Some(RuntimeFilterKeyType::Int64),
        DataType::Float32 => Some(RuntimeFilterKeyType::Float32),
        DataType::Float64 => Some(RuntimeFilterKeyType::Float64),
        DataType::Utf8View => Some(RuntimeFilterKeyType::Utf8View),
        DataType::FixedSizeBinary(width) if *width == 16 => Some(RuntimeFilterKeyType::Uuid),
        DataType::BinaryView => Some(RuntimeFilterKeyType::BinaryView),
        DataType::Date32 => Some(RuntimeFilterKeyType::Date32),
        DataType::Time64(TimeUnit::Microsecond) => Some(RuntimeFilterKeyType::Time64Microsecond),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Some(RuntimeFilterKeyType::TimestampMicrosecond)
        }
        DataType::Decimal128(_, scale) if *scale >= 0 => Some(RuntimeFilterKeyType::Decimal128),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Some(RuntimeFilterKeyType::IntervalMonthDayNano)
        }
        _ => None,
    }
}

fn key_type_for_pair(left: &DataType, right: &DataType) -> Option<RuntimeFilterKeyType> {
    match (left, right) {
        (DataType::Decimal128(_, left_scale), DataType::Decimal128(_, right_scale))
            if *left_scale >= 0 && *right_scale >= 0 =>
        {
            return Some(RuntimeFilterKeyType::Decimal128);
        }
        (DataType::Decimal128(_, _), _) | (_, DataType::Decimal128(_, _)) => return None,
        _ => {}
    }
    let left = key_type_for(left)?;
    let right = key_type_for(right)?;
    (left == right).then_some(left)
}

#[derive(Debug)]
struct RuntimeFilterBuildExec {
    input: Arc<dyn ExecutionPlan>,
    key_index: usize,
    key_type: RuntimeFilterKeyType,
    state: Arc<RuntimeFilterBuildState>,
    props: Arc<PlanProperties>,
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
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        self.input.partition_statistics(partition)
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
            RuntimeFilterKeyType::Boolean => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Boolean build key had non-Boolean array".into(),
                        )
                    })?;
                insert_hashes(array, |idx| hash_bool_key(array.value(idx)), &self.handle)?
            }
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
                insert_hashes(
                    array,
                    |idx| hash_int_key(array.value(idx) as i64),
                    &self.handle,
                )?
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
                insert_hashes(
                    array,
                    |idx| hash_int_key(array.value(idx) as i64),
                    &self.handle,
                )?
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
                insert_hashes(array, |idx| hash_int_key(array.value(idx)), &self.handle)?
            }
            RuntimeFilterKeyType::Float32 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Float32 build key had non-Float32 array".into(),
                        )
                    })?;
                insert_hashes(
                    array,
                    |idx| hash_float32_key(array.value(idx)),
                    &self.handle,
                )?
            }
            RuntimeFilterKeyType::Float64 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Float64 build key had non-Float64 array".into(),
                        )
                    })?;
                insert_hashes(
                    array,
                    |idx| hash_float64_key(array.value(idx)),
                    &self.handle,
                )?
            }
            RuntimeFilterKeyType::Utf8View => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Utf8View build key had non-Utf8View array".into(),
                        )
                    })?;
                insert_hashes(
                    array,
                    |idx| hash_bytes_key(array.value(idx).as_bytes()),
                    &self.handle,
                )?
            }
            RuntimeFilterKeyType::Uuid => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Uuid build key had non-FixedSizeBinary array".into(),
                        )
                    })?;
                if array.value_length() != 16 {
                    return Err(DataFusionError::Execution(format!(
                        "runtime filter Uuid build key had FixedSizeBinary({}) array",
                        array.value_length()
                    )));
                }
                insert_hashes(array, |idx| hash_bytes_key(array.value(idx)), &self.handle)?
            }
            RuntimeFilterKeyType::BinaryView => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter BinaryView build key had non-BinaryView array".into(),
                        )
                    })?;
                insert_hashes(array, |idx| hash_bytes_key(array.value(idx)), &self.handle)?
            }
            RuntimeFilterKeyType::Date32 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Date32 build key had non-Date32 array".into(),
                        )
                    })?;
                insert_hashes(
                    array,
                    |idx| hash_int_key(array.value(idx) as i64),
                    &self.handle,
                )?
            }
            RuntimeFilterKeyType::Time64Microsecond => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Time64Microsecond build key had non-Time64(Microsecond) array".into(),
                        )
                    })?;
                insert_hashes(array, |idx| hash_int_key(array.value(idx)), &self.handle)?
            }
            RuntimeFilterKeyType::TimestampMicrosecond => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter TimestampMicrosecond build key had non-Timestamp(Microsecond) array".into(),
                        )
                })?;
                insert_hashes(array, |idx| hash_int_key(array.value(idx)), &self.handle)?
            }
            RuntimeFilterKeyType::Decimal128 => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter Decimal128 build key had non-Decimal128 array".into(),
                        )
                    })?;
                let scale = array.scale();
                if scale < 0 {
                    return Err(DataFusionError::Execution(
                        "runtime filter Decimal128 build key had negative scale".into(),
                    ));
                }
                insert_hashes(
                    array,
                    |idx| hash_decimal128_key(array.value(idx), scale),
                    &self.handle,
                )?
            }
            RuntimeFilterKeyType::IntervalMonthDayNano => {
                let array = batch
                    .column(key_index)
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "runtime filter IntervalMonthDayNano build key had non-Interval(MonthDayNano) array".into(),
                        )
                    })?;
                insert_hashes(
                    array,
                    |idx| {
                        let (months, days, nanoseconds) =
                            IntervalMonthDayNanoType::to_parts(array.value(idx));
                        hash_interval_month_day_nano_key(months, days, nanoseconds)
                    },
                    &self.handle,
                )?
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

fn insert_hashes<A>(
    array: &A,
    hash: impl Fn(usize) -> u64,
    handle: &RuntimeFilterBuildHandle,
) -> DFResult<u64>
where
    A: Array,
{
    let mut rows = 0_u64;
    for index in 0..array.len() {
        if !array.is_null(index) {
            handle
                .insert_hash(hash(index))
                .map_err(|err| DataFusionError::Execution(err.to_string()))?;
            rows += 1;
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{alloc_zeroed, dealloc, Layout};
    use std::ptr::NonNull;

    use arrow_array::ArrayRef;
    use arrow_schema::{Field, Schema};
    use filter::{
        BloomParams, ProbeDecision, RuntimeFilterExecId, RuntimeFilterPoolConfig,
        RuntimeFilterTarget,
    };

    struct PoolMemory {
        ptr: NonNull<u8>,
        layout: Layout,
    }

    impl Drop for PoolMemory {
        fn drop(&mut self) {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }

    fn pool_fixture(slot_count: u32) -> (RuntimeFilterPool, PoolMemory) {
        let params = BloomParams::new(1024, 3, 17).unwrap();
        let config = RuntimeFilterPoolConfig::new(slot_count, params);
        let pool_layout = RuntimeFilterPool::layout(config).unwrap();
        let layout = Layout::from_size_align(pool_layout.size, pool_layout.align).unwrap();
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("pool allocation");
        let memory = PoolMemory { ptr, layout };
        let pool =
            unsafe { RuntimeFilterPool::init_in_place(ptr.as_ptr(), pool_layout.size, config) }
                .expect("pool init");
        (pool, memory)
    }

    fn make_exec_id() -> RuntimeFilterExecId {
        RuntimeFilterExecId::new(1, 2, 3, 4)
    }

    fn build_and_probe(
        key_type: RuntimeFilterKeyType,
        data_type: DataType,
        array: ArrayRef,
        present_hash: u64,
    ) {
        let (pool, _memory) = pool_fixture(1);
        let exec_id = make_exec_id();
        let target = RuntimeFilterTarget {
            exec_id,
            scan_id: 2,
            output_column: 0,
            key_type,
        };
        let handle = pool
            .allocate_build(target)
            .expect("allocate")
            .expect("available slot");
        let state = RuntimeFilterBuildState {
            handle,
            metrics: RuntimeMetrics::default(),
            closed: AtomicBool::new(false),
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", data_type, true)])),
            vec![array],
        )
        .expect("batch");

        state.insert_batch(&batch, 0, key_type).expect("insert");
        state.publish_ready().expect("publish");

        let mut probes = Vec::new();
        pool.lookup_probes(exec_id, 2, &mut probes);
        assert_eq!(probes.len(), 1);
        assert_eq!(
            probes[0].decision_for_hash(present_hash),
            ProbeDecision::MaybePresent
        );
        assert_eq!(
            probes[0].decision_for_null(),
            ProbeDecision::DefinitelyAbsent
        );
    }

    #[test]
    fn key_type_pair_accepts_supported_matching_types() {
        let cases = [
            (DataType::Boolean, RuntimeFilterKeyType::Boolean),
            (DataType::Int16, RuntimeFilterKeyType::Int16),
            (DataType::Int32, RuntimeFilterKeyType::Int32),
            (DataType::Int64, RuntimeFilterKeyType::Int64),
            (DataType::Float32, RuntimeFilterKeyType::Float32),
            (DataType::Float64, RuntimeFilterKeyType::Float64),
            (DataType::Utf8View, RuntimeFilterKeyType::Utf8View),
            (DataType::FixedSizeBinary(16), RuntimeFilterKeyType::Uuid),
            (DataType::BinaryView, RuntimeFilterKeyType::BinaryView),
            (DataType::Date32, RuntimeFilterKeyType::Date32),
            (
                DataType::Time64(TimeUnit::Microsecond),
                RuntimeFilterKeyType::Time64Microsecond,
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                RuntimeFilterKeyType::TimestampMicrosecond,
            ),
            (
                DataType::Interval(IntervalUnit::MonthDayNano),
                RuntimeFilterKeyType::IntervalMonthDayNano,
            ),
            (
                DataType::Decimal128(12, 3),
                RuntimeFilterKeyType::Decimal128,
            ),
            (
                DataType::Decimal128(38, 16),
                RuntimeFilterKeyType::Decimal128,
            ),
        ];

        for (data_type, expected) in cases {
            assert_eq!(key_type_for_pair(&data_type, &data_type), Some(expected));
        }
        assert_eq!(
            key_type_for_pair(&DataType::Decimal128(12, 3), &DataType::Decimal128(38, 16)),
            Some(RuntimeFilterKeyType::Decimal128)
        );
    }

    #[test]
    fn key_type_pair_rejects_mismatches_and_unsupported_types() {
        assert_eq!(key_type_for_pair(&DataType::Int32, &DataType::Int64), None);
        assert_eq!(key_type_for_pair(&DataType::Utf8, &DataType::Utf8), None);
        assert_eq!(
            key_type_for_pair(&DataType::Binary, &DataType::Binary),
            None
        );
        assert_eq!(
            key_type_for_pair(
                &DataType::FixedSizeBinary(15),
                &DataType::FixedSizeBinary(15)
            ),
            None
        );
        assert_eq!(
            key_type_for_pair(
                &DataType::FixedSizeBinary(17),
                &DataType::FixedSizeBinary(17)
            ),
            None
        );
        assert_eq!(
            key_type_for_pair(&DataType::Decimal128(12, -1), &DataType::Decimal128(38, 16)),
            None
        );
        assert_eq!(
            key_type_for_pair(&DataType::Decimal128(12, 3), &DataType::Utf8View),
            None
        );
        assert_eq!(
            key_type_for_pair(&DataType::Decimal128(12, 3), &DataType::Decimal128(38, -1)),
            None
        );
        assert_eq!(
            key_type_for_pair(
                &DataType::Interval(IntervalUnit::YearMonth),
                &DataType::Interval(IntervalUnit::YearMonth)
            ),
            None
        );
        assert_eq!(
            key_type_for_pair(
                &DataType::Interval(IntervalUnit::MonthDayNano),
                &DataType::Int64
            ),
            None
        );
    }

    #[test]
    fn build_state_inserts_non_integer_key_arrays() {
        build_and_probe(
            RuntimeFilterKeyType::Boolean,
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            hash_bool_key(true),
        );
        build_and_probe(
            RuntimeFilterKeyType::Float32,
            DataType::Float32,
            Arc::new(Float32Array::from(vec![Some(1.25), None])),
            hash_float32_key(1.25),
        );
        build_and_probe(
            RuntimeFilterKeyType::Float64,
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(-2.5), None])),
            hash_float64_key(-2.5),
        );
        build_and_probe(
            RuntimeFilterKeyType::Utf8View,
            DataType::Utf8View,
            Arc::new(StringViewArray::from(vec![Some("alpha"), None])),
            hash_bytes_key(b"alpha"),
        );
        let uuid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        build_and_probe(
            RuntimeFilterKeyType::Uuid,
            DataType::FixedSizeBinary(16),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(uuid), None].into_iter(),
                    16,
                )
                .expect("uuid array"),
            ),
            hash_bytes_key(&uuid),
        );
        build_and_probe(
            RuntimeFilterKeyType::BinaryView,
            DataType::BinaryView,
            Arc::new(BinaryViewArray::from(vec![
                Some(&b"\x00\x01binary"[..]),
                None,
            ])),
            hash_bytes_key(b"\x00\x01binary"),
        );
        build_and_probe(
            RuntimeFilterKeyType::Date32,
            DataType::Date32,
            Arc::new(Date32Array::from(vec![Some(19_000), None])),
            hash_int_key(19_000),
        );
        build_and_probe(
            RuntimeFilterKeyType::Time64Microsecond,
            DataType::Time64(TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
            hash_int_key(1_000_000),
        );
        build_and_probe(
            RuntimeFilterKeyType::TimestampMicrosecond,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_000_000),
                None,
            ])),
            hash_int_key(1_700_000_000_000_000),
        );
        build_and_probe(
            RuntimeFilterKeyType::IntervalMonthDayNano,
            DataType::Interval(IntervalUnit::MonthDayNano),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNanoType::make_value(2, -3, 4)),
                None,
            ])),
            hash_interval_month_day_nano_key(2, -3, 4),
        );
        build_and_probe(
            RuntimeFilterKeyType::Decimal128,
            DataType::Decimal128(12, 3),
            Arc::new(
                Decimal128Array::from(vec![Some(12345_i128), None])
                    .with_precision_and_scale(12, 3)
                    .expect("decimal array"),
            ),
            hash_decimal128_key(12345, 3),
        );
    }
}
