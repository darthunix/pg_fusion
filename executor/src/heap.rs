use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_catalog::{Session, TableProvider};

/// A placeholder provider for Heap-backed tables. It will be wired to the
/// shared-memory slot buffers and Packet::Heap handling to produce RecordBatches.
#[derive(Debug)]
pub struct HeapTableProvider {
    schema: SchemaRef,
    #[allow(unused)]
    table_id: u32,
}

impl HeapTableProvider {
    pub fn new(table_id: u32, schema: SchemaRef) -> Self {
        Self { schema, table_id }
    }
}

#[async_trait]
impl TableProvider for HeapTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Placeholder: return an empty executor with our schema.
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }
}
