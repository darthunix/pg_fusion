use crate::protocol::metadata::consume_metadata;
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::functions::all_default_functions;
use datafusion::functions::core::planner::CoreFunctionPlanner;
use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::functions_window::all_default_window_functions;
use datafusion::logical_expr::planner::ContextProvider;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::TableReference;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

static BUILDIN: Lazy<Arc<Builtin>> = Lazy::new(|| Arc::new(Builtin::new()));

#[derive(PartialEq, Eq, Hash)]
pub struct Table {
    pub id: u32,
    pub schema: SchemaRef,
}

impl Table {
    pub fn new(id: u32, schema: SchemaRef) -> Self {
        Self { id, schema }
    }
}

impl TableSource for Table {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct Builtin {
    option: ConfigOptions,
    agg_udf: HashMap<String, Arc<AggregateUDF>>,
    scalar_udf: HashMap<String, Arc<ScalarUDF>>,
    window_udf: HashMap<String, Arc<WindowUDF>>,
    expr_planner: Vec<Arc<dyn ExprPlanner>>,
}

impl Builtin {
    fn new() -> Self {
        let option = ConfigOptions::default();
        let mut agg_udf = HashMap::new();
        all_default_aggregate_functions().into_iter().for_each(|f| {
            agg_udf.insert(f.name().to_string(), f);
        });
        let mut scalar_udf = HashMap::new();
        all_default_functions().into_iter().for_each(|f| {
            scalar_udf.insert(f.name().to_string(), f);
        });
        let mut window_udf = HashMap::new();
        all_default_window_functions().into_iter().for_each(|f| {
            window_udf.insert(f.name().to_string(), f);
        });
        Self {
            option,
            agg_udf,
            scalar_udf,
            window_udf,
            expr_planner: vec![Arc::new(CoreFunctionPlanner::default())],
        }
    }
}

pub struct Catalog {
    builtin: Arc<Builtin>,
    tables: AHashMap<TableReference, Arc<dyn TableSource>>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            builtin: Arc::clone(&*BUILDIN),
            tables: AHashMap::new(),
        }
    }
}

impl Catalog {
    pub fn from_stream(stream: &mut impl Read) -> Result<Self> {
        Ok(Self {
            builtin: Arc::clone(&*BUILDIN),
            tables: consume_metadata(stream)?,
        })
    }
}

impl ContextProvider for Catalog {
    fn get_table_source(&self, table: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        match self.tables.get(&table) {
            Some(table) => Ok(Arc::clone(table)),
            _ => {
                let schema = match table.schema() {
                    Some(schema) => &format!("{schema}."),
                    _ => table.table(),
                };
                Err(DataFusionError::Plan(format!(
                    "Table not found: {schema}{}",
                    table.table()
                )))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.builtin.scalar_udf.get(name).map(Arc::clone)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.builtin.agg_udf.get(name).map(Arc::clone)
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.builtin.window_udf.get(name).map(Arc::clone)
    }

    fn options(&self) -> &ConfigOptions {
        &self.builtin.option
    }

    fn udf_names(&self) -> Vec<String> {
        self.builtin.scalar_udf.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.builtin.agg_udf.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.builtin.window_udf.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.builtin.expr_planner
    }
}
