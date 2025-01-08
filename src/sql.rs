use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::functions::all_default_functions;
use datafusion::functions::core::planner::CoreFunctionPlanner;
use datafusion::functions_aggregate::all_default_aggregate_functions;
use datafusion::functions_window::all_default_window_functions;
use datafusion::logical_expr::planner::ContextProvider;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;

thread_local! {
    static CATALOG: OnceCell<Catalog> = OnceCell::new();
}

fn sql_to_logical_plan(
    sql: &str,
    params: Vec<ScalarValue>,
) -> Result<LogicalPlan, DataFusionError> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| DataFusionError::SQL(e, None))?;
    assert_eq!(ast.len(), 1);
    let statement = ast.into_iter().next().expect("ast is not empty");

    // Cash metadata provider in a static variable to avoid re-allocation on each query.
    let base_plan = CATALOG.with(|catalog| {
        let catalog = catalog.get_or_init(Catalog::new);
        let sql_to_rel = SqlToRel::new(catalog);
        sql_to_rel.sql_statement_to_plan(statement)
    })?;
    let plan = base_plan.with_param_values(params)?;

    Ok(plan)
}

struct Catalog {
    option: ConfigOptions,
    agg_udf: HashMap<String, Arc<AggregateUDF>>,
    scalar_udf: HashMap<String, Arc<ScalarUDF>>,
    window_udf: HashMap<String, Arc<WindowUDF>>,
    expr_planner: Vec<Arc<dyn ExprPlanner>>,
}

impl Catalog {
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

impl ContextProvider for Catalog {
    fn get_table_source(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        unimplemented!()
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar_udf.get(name).map(|f| Arc::clone(f))
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.agg_udf.get(name).map(|f| Arc::clone(f))
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.window_udf.get(name).map(|f| Arc::clone(f))
    }

    fn options(&self) -> &ConfigOptions {
        &self.option
    }

    fn udf_names(&self) -> Vec<String> {
        self.scalar_udf.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.agg_udf.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.window_udf.keys().cloned().collect()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planner
    }
}
