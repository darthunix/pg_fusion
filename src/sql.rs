use crate::data_type::EncodedType;
use crate::ipc::SlotStream;
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use pgrx::pg_sys::Oid;
use rmp::decode::read_array_len;
use rmp::decode::read_bool;
use rmp::decode::read_str_len;
use rmp::decode::{read_u32, read_u8};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::str::from_utf8;
use std::sync::Arc;

static BUILDIN: Lazy<Arc<Builtin>> = Lazy::new(|| Arc::new(Builtin::new()));

#[derive(PartialEq, Eq, Hash)]
pub(crate) struct Table {
    oid: Oid,
    schema: SchemaRef,
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

pub(crate) struct Catalog {
    builtin: Arc<Builtin>,
    tables: AHashMap<TableReference, Arc<dyn TableSource>>,
}

impl Catalog {
    pub(crate) fn from_stream(stream: &mut SlotStream) -> Result<Self> {
        // The header should be consumed before calling this function.
        let table_num = read_array_len(stream)?;
        let mut tables = AHashMap::with_capacity(table_num as usize);
        for _ in 0..table_num {
            let oid = read_u32(stream)?;
            let ns_len = read_str_len(stream)?;
            let ns_bytes = stream.look_ahead(ns_len as usize)?;
            let schema = SmolStr::from(from_utf8(ns_bytes)?);
            stream.rewind(ns_len as usize)?;
            let name_len = read_str_len(stream)?;
            let name_bytes = stream.look_ahead(name_len as usize)?;
            let name = from_utf8(name_bytes)?;
            let table_ref = TableReference::partial(schema.as_str(), name);
            stream.rewind(name_len as usize)?;
            let column_num = read_array_len(stream)?;
            let mut fields = Vec::with_capacity(column_num as usize);
            for _ in 0..column_num {
                let elem_num = read_array_len(stream)?;
                assert_eq!(elem_num, 3);
                let etype = read_u8(stream)?;
                let df_type = EncodedType::try_from(etype)?.to_arrow();
                let is_nullable = read_bool(stream)?;
                let name_len = read_str_len(stream)?;
                let name_bytes = stream.look_ahead(name_len as usize)?;
                let name = from_utf8(name_bytes)?;
                let field = Field::new(name, df_type, is_nullable);
                fields.push(field);
            }
            let schema = Schema::new(fields);
            let table = Table {
                oid: Oid::from(oid),
                schema: Arc::new(schema),
            };
            tables.insert(table_ref, Arc::new(table) as Arc<dyn TableSource>);
        }
        Ok(Self {
            builtin: Arc::clone(&*BUILDIN),
            tables,
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
