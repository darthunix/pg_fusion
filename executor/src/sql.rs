use crate::pgscan::{PgTableProvider, ScanRegistry};
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
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
use protocol::metadata::NAMEDATALEN;
use rmp::decode::{read_array_len, read_bool, read_str_len, read_u32, read_u8};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::io::Read;
use std::str::from_utf8;
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
    registry: Arc<ScanRegistry>,
}

impl Catalog {
    pub fn with_registry(registry: Arc<ScanRegistry>) -> Self {
        Self {
            builtin: Arc::clone(&*BUILDIN),
            tables: AHashMap::new(),
            registry,
        }
    }

    pub fn from_stream(stream: &mut impl Read, registry: Arc<ScanRegistry>) -> Result<Self> {
        // Temporarily create a placeholder to pass registry into provider construction
        let mut catalog = Self {
            builtin: Arc::clone(&*BUILDIN),
            tables: AHashMap::new(),
            registry,
        };
        catalog.tables = catalog.consume_metadata_exec(stream)?;
        Ok(catalog)
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

impl Catalog {
    fn consume_metadata_exec(
        &self,
        stream: &mut impl Read,
    ) -> Result<AHashMap<TableReference, Arc<dyn TableSource>>> {
        let table_num = read_array_len(stream)?;
        let mut tables = AHashMap::with_capacity(table_num as usize);

        let mut schema_buf = SmallVec::<[u8; NAMEDATALEN]>::new();
        let mut name_buf = SmallVec::<[u8; NAMEDATALEN]>::new();

        for _ in 0..table_num {
            let name_part_num = read_array_len(stream)?;
            debug_assert!(name_part_num == 2 || name_part_num == 3);
            let oid = read_u32(stream)?;
            let mut schema = None;
            if name_part_num == 3 {
                let ns_len = read_str_len(stream)?;
                schema_buf.resize(ns_len as usize, 0);
                std::io::Read::read_exact(stream, &mut schema_buf)?;
                schema = Some(from_utf8(schema_buf.as_slice())?);
            }
            let name_len = read_str_len(stream)?;
            name_buf.resize(name_len as usize, 0);
            std::io::Read::read_exact(stream, &mut name_buf)?;
            let name = from_utf8(name_buf.as_slice())?;
            let table_ref = match schema {
                Some(schema) => TableReference::partial(schema, name),
                None => TableReference::bare(name),
            };
            schema_buf.clear();
            name_buf.clear();

            let column_num = read_array_len(stream)?;
            let mut fields = Vec::with_capacity(column_num as usize);
            for _ in 0..column_num {
                let elem_num = read_array_len(stream)?;
                debug_assert_eq!(elem_num, 3);
                let etype = read_u8(stream)?;
                let df_type = protocol::data_type::EncodedType::try_from(etype)?.to_arrow();
                let is_nullable = read_bool(stream)?;
                let name_len = read_str_len(stream)?;
                name_buf.resize(name_len as usize, 0);
                std::io::Read::read_exact(stream, &mut name_buf)?;
                let name = from_utf8(name_buf.as_slice())?;
                let field = datafusion::arrow::datatypes::Field::new(name, df_type, is_nullable);
                name_buf.clear();
                fields.push(field);
            }
            let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
            // Register PgTableProvider backed by ScanRegistry; use OID as scan_id for now.
            let scan_id = oid as u64;
            // Provider uses connection-local registry; injected later via Catalog
            let provider = Arc::new(PgTableProvider::new(
                scan_id,
                Arc::clone(&schema),
                Arc::clone(&self.registry),
            ));
            let source = Arc::new(DefaultTableSource::new(provider));
            tables.insert(table_ref, source as Arc<dyn TableSource>);
        }
        Ok(tables)
    }
}
