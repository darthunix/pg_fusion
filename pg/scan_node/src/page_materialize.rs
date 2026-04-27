use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::{make_array, Array, ArrayRef, BinaryViewArray, RecordBatch, StringViewArray};
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{
    with_new_children_if_necessary, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, Result};
use futures::StreamExt;

/// Transparent physical-plan boundary that copies page-backed Arrow batches into
/// ordinary Arrow allocations before an upstream operator can retain them.
#[derive(Debug)]
pub struct PageMaterializeExec {
    input: Arc<dyn ExecutionPlan>,
    props: PlanProperties,
}

impl PageMaterializeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let props = input.properties().clone();
        Self { input, props }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for PageMaterializeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PageMaterializeExec")
    }
}

impl ExecutionPlan for PageMaterializeExec {
    fn name(&self) -> &str {
        "PageMaterializeExec"
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
        match children.as_slice() {
            [input] => Ok(Arc::new(Self::new(Arc::clone(input)))),
            _ => Err(DataFusionError::Plan(
                "PageMaterializeExec expects exactly one child".into(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = self.input.execute(partition, ctx)?;
        let stream = input.map(|batch| batch.and_then(materialize_record_batch));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Insert page materialization at retaining physical operators fed by a
/// page-backed source.
///
/// The source predicate keeps `scan_node` independent from concrete runtime and
/// EXPLAIN scan exec implementations.
pub fn insert_page_materializers<F>(
    plan: Arc<dyn ExecutionPlan>,
    is_page_backed_source: &F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: Fn(&dyn ExecutionPlan) -> bool,
{
    rewrite_page_materializers(plan, is_page_backed_source).map(|(plan, _)| plan)
}

fn rewrite_page_materializers<F>(
    plan: Arc<dyn ExecutionPlan>,
    is_page_backed_source: &F,
) -> Result<(Arc<dyn ExecutionPlan>, bool)>
where
    F: Fn(&dyn ExecutionPlan) -> bool,
{
    let children = plan.children();
    let mut rewritten_children = Vec::with_capacity(children.len());
    let mut child_has_page_source = Vec::with_capacity(children.len());
    for child in children {
        let (rewritten, has_page_source) =
            rewrite_page_materializers(Arc::clone(child), is_page_backed_source)?;
        rewritten_children.push(rewritten);
        child_has_page_source.push(has_page_source);
    }

    let mut rewritten = with_new_children_if_necessary(plan, rewritten_children)?;
    let retaining_children = retaining_child_indexes(rewritten.as_ref());
    if !retaining_children.is_empty() {
        let mut children = rewritten
            .children()
            .into_iter()
            .map(Arc::clone)
            .collect::<Vec<_>>();
        for &index in retaining_children {
            let Some(child) = children.get_mut(index) else {
                continue;
            };
            if child_has_page_source.get(index).copied().unwrap_or(false)
                && !child.as_any().is::<PageMaterializeExec>()
            {
                *child = Arc::new(PageMaterializeExec::new(Arc::clone(child)));
            }
        }
        rewritten = with_new_children_if_necessary(rewritten, children)?;
    }

    let has_page_source =
        is_page_backed_source(rewritten.as_ref()) || child_has_page_source.into_iter().any(|v| v);
    Ok((rewritten, has_page_source))
}

fn retaining_child_indexes(plan: &dyn ExecutionPlan) -> &'static [usize] {
    if plan.as_any().is::<SortExec>()
        || plan.as_any().is::<PartialSortExec>()
        || plan.as_any().is::<WindowAggExec>()
        || plan.as_any().is::<BoundedWindowAggExec>()
        || plan.as_any().is::<CrossJoinExec>()
        || plan.as_any().is::<NestedLoopJoinExec>()
        || plan.as_any().is::<HashJoinExec>()
    {
        &[0]
    } else if plan.as_any().is::<SymmetricHashJoinExec>() {
        &[0, 1]
    } else {
        &[]
    }
}

pub fn materialize_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
    if batch.num_columns() == 0 {
        return Ok(batch);
    }

    let columns = batch
        .columns()
        .iter()
        .map(|array| deep_copy_array(array.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(DataFusionError::from)
}

fn deep_copy_array(array: &dyn Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8View => copy_string_view_array(array),
        DataType::BinaryView => copy_binary_view_array(array),
        DataType::ListView(_) | DataType::LargeListView(_) | DataType::RunEndEncoded(_, _) => {
            Err(DataFusionError::Plan(format!(
                "PageMaterializeExec does not support deep-copying Arrow type {}",
                array.data_type()
            )))
        }
        _ => {
            let data = array.to_data();
            let mut mutable = MutableArrayData::new(vec![&data], false, array.len());
            mutable.extend(0, 0, array.len());
            Ok(make_array(mutable.freeze()))
        }
    }
}

fn copy_string_view_array(array: &dyn Array) -> Result<ArrayRef> {
    let source = array
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            DataFusionError::Plan("expected Utf8View array to be StringViewArray".into())
        })?;
    let mut builder = StringViewBuilder::with_capacity(source.len());
    for index in 0..source.len() {
        if source.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(source.value(index));
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn copy_binary_view_array(array: &dyn Array) -> Result<ArrayRef> {
    let source = array
        .as_any()
        .downcast_ref::<BinaryViewArray>()
        .ok_or_else(|| {
            DataFusionError::Plan("expected BinaryView array to be BinaryViewArray".into())
        })?;
    let mut builder = BinaryViewBuilder::with_capacity(source.len());
    for index in 0..source.len() {
        if source.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(source.value(index));
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, Int32Array};
    use arrow_schema::{Field, Schema, SchemaRef, SortOptions};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::expressions::{col, PhysicalSortExpr};
    use datafusion::physical_plan::joins::CrossJoinExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::Partitioning;

    #[derive(Debug)]
    struct TestExec {
        name: &'static str,
        props: PlanProperties,
    }

    impl TestExec {
        fn new(name: &'static str, schema: SchemaRef) -> Self {
            let props = PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self { name, props }
        }
    }

    #[derive(Debug)]
    struct TestUnaryExec {
        name: &'static str,
        input: Arc<dyn ExecutionPlan>,
        props: PlanProperties,
    }

    impl TestUnaryExec {
        fn new(name: &'static str, input: Arc<dyn ExecutionPlan>) -> Self {
            let props = input.properties().clone();
            Self { name, input, props }
        }
    }

    impl DisplayAs for TestUnaryExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl ExecutionPlan for TestUnaryExec {
        fn name(&self) -> &str {
            self.name
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
            match children.as_slice() {
                [input] => Ok(Arc::new(Self::new(self.name, Arc::clone(input)))),
                _ => Err(DataFusionError::Plan(
                    "TestUnaryExec expects exactly one child".into(),
                )),
            }
        }

        fn execute(
            &self,
            _partition: usize,
            _ctx: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            Err(DataFusionError::Plan(
                "TestUnaryExec is not executable".into(),
            ))
        }

        fn statistics(&self) -> Result<Statistics> {
            self.input.statistics()
        }

        fn maintains_input_order(&self) -> Vec<bool> {
            vec![true]
        }

        fn cardinality_effect(&self) -> CardinalityEffect {
            CardinalityEffect::Equal
        }
    }

    impl DisplayAs for TestExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            self.name
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
                Err(DataFusionError::Plan("TestExec is a leaf".into()))
            }
        }

        fn execute(
            &self,
            _partition: usize,
            _ctx: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            Err(DataFusionError::Plan("TestExec is not executable".into()))
        }

        fn statistics(&self) -> Result<Statistics> {
            Ok(Statistics::new_unknown(self.schema().as_ref()))
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
    }

    fn test_leaf(name: &'static str) -> Arc<dyn ExecutionPlan> {
        Arc::new(TestExec::new(name, schema()))
    }

    fn is_page_source(plan: &dyn ExecutionPlan) -> bool {
        plan.name() == "page"
    }

    #[test]
    fn materialize_record_batch_copies_view_buffers() {
        let long = "this string is longer than twelve bytes";
        let source = StringViewArray::from(vec![Some(long), None, Some("short")]);
        let source_data_buffer_ptr = source.data_buffers()[0].as_ptr();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8View, true)])),
            vec![Arc::new(source)],
        )
        .unwrap();

        let copied = materialize_record_batch(batch).unwrap();
        let copied = copied
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();

        assert_eq!(copied.value(0), long);
        assert!(copied.is_null(1));
        assert_eq!(copied.value(2), "short");
        assert_ne!(copied.data_buffers()[0].as_ptr(), source_data_buffer_ptr);
    }

    #[test]
    fn materialize_record_batch_copies_primitive_buffers() {
        let source = Int32Array::from(vec![Some(1), None, Some(3)]);
        let source_value_ptr = source.values().inner().as_ptr();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
            vec![Arc::new(source)],
        )
        .unwrap();

        let copied = materialize_record_batch(batch).unwrap();
        let copied = copied
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();

        assert_eq!(copied.value(0), 1);
        assert!(copied.is_null(1));
        assert_eq!(copied.value(2), 3);
        assert_ne!(copied.values().inner().as_ptr(), source_value_ptr);
    }

    #[test]
    fn rewrite_materializes_sort_input_above_streaming_chain() {
        let input = Arc::new(TestUnaryExec::new("streaming", test_leaf("page")));
        let sort = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema()).unwrap(),
                options: SortOptions::default(),
            }]
            .into(),
            input,
        )) as Arc<dyn ExecutionPlan>;

        let rewritten = insert_page_materializers(sort, &is_page_source).unwrap();
        let child = rewritten.children()[0];

        assert!(child.as_any().is::<PageMaterializeExec>());
        assert_eq!(child.children()[0].name(), "streaming");
        assert_eq!(child.children()[0].children()[0].name(), "page");
    }

    #[test]
    fn rewrite_materializes_cross_join_left_only() {
        let left = test_leaf("page");
        let right = test_leaf("page");
        let join = Arc::new(CrossJoinExec::new(left, right)) as Arc<dyn ExecutionPlan>;

        let rewritten = insert_page_materializers(join, &is_page_source).unwrap();
        let children = rewritten.children();

        assert!(children[0].as_any().is::<PageMaterializeExec>());
        assert!(!children[1].as_any().is::<PageMaterializeExec>());
    }

    #[test]
    fn rewrite_is_idempotent() {
        let input = test_leaf("page");
        let sort = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema()).unwrap(),
                options: SortOptions::default(),
            }]
            .into(),
            input,
        )) as Arc<dyn ExecutionPlan>;

        let rewritten = insert_page_materializers(sort, &is_page_source).unwrap();
        let rewritten_again = insert_page_materializers(rewritten, &is_page_source).unwrap();
        let child = rewritten_again.children()[0];

        assert!(child.as_any().is::<PageMaterializeExec>());
        assert_eq!(child.children()[0].name(), "page");
        assert!(!child.children()[0].as_any().is::<PageMaterializeExec>());
    }
}
