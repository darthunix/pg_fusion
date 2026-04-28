use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchemaRef, DataFusionError};
use datafusion_expr::logical_plan::{
    build_join_schema, Join, JoinConstraint, JoinType, LogicalPlan, Projection, SubqueryAlias,
    TableScan,
};
use datafusion_expr::Expr;
use df_catalog::ResolvedTable;
use join_order::{rel_bit, Edge, EdgeFlags, JoinKind, Problem, RelSet, RelStats};
use pg_statistics::{
    estimate_equi_join_selectivity, EquiJoinInput, EstimateOptions, PgColumnStats, PgScanEstimate,
    PgUniqueKey,
};
use scan_sql::{compile_scan, CompileScanInput, LimitLowering};

use crate::{PgPlanningTableSource, PlanBuildError, PlanBuilderConfig};

/// Statistics source used by join reordering.
pub trait JoinStatsProvider: Clone + Send + Sync {
    fn estimate_scan_sql(&self, sql: &str) -> Result<PgScanEstimate, PlanBuildError>;

    fn load_column_stats(
        &self,
        relation_oid: u32,
        attnums: &[i16],
    ) -> Result<Vec<PgColumnStats>, PlanBuildError>;

    fn load_unique_keys(&self, relation_oid: u32) -> Result<Vec<PgUniqueKey>, PlanBuildError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LiveJoinStatsProvider;

impl JoinStatsProvider for LiveJoinStatsProvider {
    fn estimate_scan_sql(&self, sql: &str) -> Result<PgScanEstimate, PlanBuildError> {
        pg_statistics::estimate_scan_sql(sql, EstimateOptions::default())
            .map_err(|error| PlanBuildError::Statistics(error.to_string()))
    }

    fn load_column_stats(
        &self,
        relation_oid: u32,
        attnums: &[i16],
    ) -> Result<Vec<PgColumnStats>, PlanBuildError> {
        pg_statistics::load_column_stats(pgrx::pg_sys::Oid::from(relation_oid), attnums)
            .map_err(|error| PlanBuildError::Statistics(error.to_string()))
    }

    fn load_unique_keys(&self, relation_oid: u32) -> Result<Vec<PgUniqueKey>, PlanBuildError> {
        pg_statistics::load_unique_keys(pgrx::pg_sys::Oid::from(relation_oid))
            .map_err(|error| PlanBuildError::Statistics(error.to_string()))
    }
}

pub(crate) fn rewrite_join_order<S>(
    plan: LogicalPlan,
    config: PlanBuilderConfig,
    stats_provider: &S,
) -> Result<LogicalPlan, PlanBuildError>
where
    S: JoinStatsProvider,
{
    if let Some(rewritten) = try_rewrite_component(&plan, config, stats_provider)? {
        return Ok(rewritten);
    }

    let transformed = plan
        .map_children(|child| {
            let child = rewrite_join_order(child, config, stats_provider)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Ok(Transformed::yes(child))
        })
        .map_err(recover_plan_build_error)?;
    Ok(transformed.data)
}

fn try_rewrite_component<S>(
    plan: &LogicalPlan,
    config: PlanBuilderConfig,
    stats_provider: &S,
) -> Result<Option<LogicalPlan>, PlanBuildError>
where
    S: JoinStatsProvider,
{
    if !matches!(plan, LogicalPlan::Join(_)) {
        return Ok(None);
    }

    let Some(mut component) = collect_component(plan)? else {
        return Ok(None);
    };
    if component.leaves.len() < 2 {
        return Ok(None);
    }
    if component.leaves.len() > u8::MAX as usize {
        return Err(PlanBuildError::JoinReorder(format!(
            "join component has {} leaves, maximum supported is {}",
            component.leaves.len(),
            u8::MAX
        )));
    }

    load_leaf_statistics(&mut component, config, stats_provider)?;
    let problem = build_problem(&component)?;
    let solution = join_order::optimize(&problem, config.join_order_config)?;
    let rewritten = build_join_tree(solution.root(), &solution, &component)?;
    preserve_output_order(rewritten, &component.original_schema).map(Some)
}

fn recover_plan_build_error(error: DataFusionError) -> PlanBuildError {
    match error {
        DataFusionError::External(source) => match source.downcast::<PlanBuildError>() {
            Ok(error) => *error,
            Err(source) => PlanBuildError::DataFusion(DataFusionError::External(source)),
        },
        other => PlanBuildError::DataFusion(other),
    }
}

#[derive(Debug, Clone)]
struct Component {
    leaves: Vec<Leaf>,
    predicates: Vec<JoinPredicate>,
    original_schema: DFSchemaRef,
}

impl Component {
    fn shifted(mut self, offset: usize) -> Self {
        for predicate in &mut self.predicates {
            predicate.left_rel += offset;
            predicate.right_rel += offset;
        }
        self
    }
}

#[derive(Debug, Clone)]
struct Leaf {
    plan: LogicalPlan,
    table_scan: TableScan,
    resolved: ResolvedTable,
    output_attnums: Vec<i16>,
    estimate: Option<PgScanEstimate>,
    column_stats: HashMap<i16, PgColumnStats>,
    unique_keys: Vec<PgUniqueKey>,
}

#[derive(Debug, Clone)]
struct JoinPredicate {
    left_rel: usize,
    right_rel: usize,
    left_expr: Expr,
    right_expr: Expr,
    left_attnum: i16,
    right_attnum: i16,
}

#[derive(Debug, Clone, Copy)]
struct ColumnLoc {
    rel: usize,
    output_index: usize,
}

fn collect_component(plan: &LogicalPlan) -> Result<Option<Component>, PlanBuildError> {
    if let LogicalPlan::Projection(projection) = plan {
        if projection
            .expr
            .iter()
            .all(|expr| matches!(expr, Expr::Column(_)))
        {
            if let Some(mut component) = collect_component(&projection.input)? {
                component.original_schema = plan.schema().clone();
                return Ok(Some(component));
            }
        }
        return Ok(None);
    }

    let LogicalPlan::Join(join) = plan else {
        return Ok(table_leaf(plan)?.map(|leaf| Component {
            original_schema: plan.schema().clone(),
            leaves: vec![leaf],
            predicates: Vec::new(),
        }));
    };

    if !is_reorderable_join(join) {
        return Ok(None);
    }

    let Some(left) = collect_component(&join.left)? else {
        return Ok(None);
    };
    let Some(right) = collect_component(&join.right)? else {
        return Ok(None);
    };

    let right_offset = left.leaves.len();
    let right = right.shifted(right_offset);
    let mut leaves = left.leaves;
    leaves.extend(right.leaves);

    let mut predicates = left.predicates;
    predicates.extend(right.predicates);
    for (left_expr, right_expr) in &join.on {
        let Some(left_loc) = locate_join_column(&leaves, left_expr)? else {
            return Ok(None);
        };
        let Some(right_loc) = locate_join_column(&leaves, right_expr)? else {
            return Ok(None);
        };
        if left_loc.rel == right_loc.rel {
            return Ok(None);
        }
        predicates.push(JoinPredicate {
            left_rel: left_loc.rel,
            right_rel: right_loc.rel,
            left_expr: left_expr.clone(),
            right_expr: right_expr.clone(),
            left_attnum: leaves[left_loc.rel].output_attnums[left_loc.output_index],
            right_attnum: leaves[right_loc.rel].output_attnums[right_loc.output_index],
        });
    }

    Ok(Some(Component {
        leaves,
        predicates,
        original_schema: plan.schema().clone(),
    }))
}

fn is_reorderable_join(join: &Join) -> bool {
    join.join_type == JoinType::Inner
        && join.join_constraint == JoinConstraint::On
        && join.filter.is_none()
        && !join.null_equals_null
}

fn table_leaf(plan: &LogicalPlan) -> Result<Option<Leaf>, PlanBuildError> {
    match plan {
        LogicalPlan::TableScan(table_scan) => table_scan_leaf(plan.clone(), table_scan),
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
            if let LogicalPlan::TableScan(table_scan) = input.as_ref() {
                table_scan_leaf(plan.clone(), table_scan)
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn table_scan_leaf(
    plan: LogicalPlan,
    table_scan: &TableScan,
) -> Result<Option<Leaf>, PlanBuildError> {
    let Some(source) = table_scan
        .source
        .as_any()
        .downcast_ref::<PgPlanningTableSource>()
    else {
        return Ok(None);
    };
    let resolved = source.resolved().clone();
    let output_attnums = projected_attnums(&resolved, table_scan)?;
    Ok(Some(Leaf {
        plan,
        table_scan: table_scan.clone(),
        resolved,
        output_attnums,
        estimate: None,
        column_stats: HashMap::new(),
        unique_keys: Vec::new(),
    }))
}

fn projected_attnums(
    resolved: &ResolvedTable,
    table_scan: &TableScan,
) -> Result<Vec<i16>, PlanBuildError> {
    let indices = table_scan
        .projection
        .clone()
        .unwrap_or_else(|| (0..resolved.column_attnums.len()).collect());
    indices
        .into_iter()
        .map(|index| {
            resolved.column_attnums.get(index).copied().ok_or_else(|| {
                PlanBuildError::JoinReorder(format!(
                    "TableScan {} projection index {index} has no PostgreSQL attnum mapping",
                    table_scan.table_name
                ))
            })
        })
        .collect()
}

fn locate_join_column(leaves: &[Leaf], expr: &Expr) -> Result<Option<ColumnLoc>, PlanBuildError> {
    let Expr::Column(column) = expr else {
        return Ok(None);
    };

    let mut found = None;
    for (rel, leaf) in leaves.iter().enumerate() {
        if let Some(output_index) = leaf.plan.schema().maybe_index_of_column(column) {
            if found.is_some() {
                return Err(PlanBuildError::JoinReorder(format!(
                    "join column {column} is ambiguous across reordered leaves"
                )));
            }
            found = Some(ColumnLoc { rel, output_index });
        }
    }
    Ok(found)
}

fn load_leaf_statistics<S>(
    component: &mut Component,
    config: PlanBuilderConfig,
    stats_provider: &S,
) -> Result<(), PlanBuildError>
where
    S: JoinStatsProvider,
{
    let mut needed_attnums = vec![BTreeSet::<i16>::new(); component.leaves.len()];
    for predicate in &component.predicates {
        needed_attnums[predicate.left_rel].insert(predicate.left_attnum);
        needed_attnums[predicate.right_rel].insert(predicate.right_attnum);
    }

    for (index, leaf) in component.leaves.iter_mut().enumerate() {
        let compiled = compile_scan(CompileScanInput {
            relation: &leaf.resolved.relation,
            schema: leaf.resolved.schema.as_ref(),
            identifier_max_bytes: config.identifier_max_bytes,
            projection: leaf.table_scan.projection.as_deref(),
            filters: &leaf.table_scan.filters,
            requested_limit: leaf.table_scan.fetch,
            limit_lowering: LimitLowering::ExternalHint,
        })?;
        leaf.estimate = Some(stats_provider.estimate_scan_sql(&compiled.sql)?);

        let attnums = needed_attnums[index].iter().copied().collect::<Vec<_>>();
        leaf.column_stats = stats_provider
            .load_column_stats(leaf.resolved.table_oid, &attnums)?
            .into_iter()
            .map(|stat| (stat.attnum, stat))
            .collect();
        leaf.unique_keys = stats_provider.load_unique_keys(leaf.resolved.table_oid)?;
    }

    Ok(())
}

fn build_problem(component: &Component) -> Result<Problem, PlanBuildError> {
    let rels = component
        .leaves
        .iter()
        .map(|leaf| {
            let estimate = leaf
                .estimate
                .expect("leaf estimates loaded before problem build");
            RelStats::new(sanitize_rows(estimate.rows), sanitize_bytes(estimate.bytes))
        })
        .collect::<Vec<_>>();

    let mut edge_preds = Vec::new();
    let mut edges = Vec::new();
    for ((left_rel, right_rel), predicates) in grouped_predicates(&component.predicates) {
        let pred_start = edge_preds.len() as u32;
        edge_preds.extend((0..predicates.len()).map(|_| 0));
        let pred_len = predicates.len() as u16;
        let selectivity = estimate_edge_selectivity(component, &predicates)?;
        edges.push(Edge {
            left: rel_bit(left_rel as u8),
            right: rel_bit(right_rel as u8),
            pred_start,
            pred_len,
            kind: JoinKind::Inner,
            flags: EdgeFlags::COMMUTATIVE,
            selectivity,
        });
    }

    Ok(Problem {
        rels,
        edges,
        edge_preds,
    })
}

fn grouped_predicates(predicates: &[JoinPredicate]) -> Vec<((usize, usize), Vec<&JoinPredicate>)> {
    let mut grouped: HashMap<(usize, usize), Vec<&JoinPredicate>> = HashMap::new();
    for predicate in predicates {
        let key = if predicate.left_rel <= predicate.right_rel {
            (predicate.left_rel, predicate.right_rel)
        } else {
            (predicate.right_rel, predicate.left_rel)
        };
        grouped.entry(key).or_default().push(predicate);
    }
    let mut grouped = grouped.into_iter().collect::<Vec<_>>();
    grouped.sort_by_key(|(key, _)| *key);
    grouped
}

fn estimate_edge_selectivity(
    component: &Component,
    predicates: &[&JoinPredicate],
) -> Result<f64, PlanBuildError> {
    let Some(first) = predicates.first() else {
        return Ok(1.0);
    };
    let left_rel = first.left_rel;
    let right_rel = first.right_rel;
    let left = &component.leaves[left_rel];
    let right = &component.leaves[right_rel];
    let left_rows = leaf_rows(left);
    let right_rows = leaf_rows(right);
    let possible_rows = left_rows * right_rows;
    if possible_rows <= 0.0 {
        return Ok(0.0);
    }

    let mut selectivity = 1.0;
    let mut left_join_attnums = BTreeSet::new();
    let mut right_join_attnums = BTreeSet::new();
    for predicate in predicates {
        let (left_pred_attnum, right_pred_attnum) =
            predicate_attnums_for_pair(predicate, left_rel, right_rel)?;
        left_join_attnums.insert(left_pred_attnum);
        right_join_attnums.insert(right_pred_attnum);

        let left_stat = left.column_stats.get(&left_pred_attnum);
        let right_stat = right.column_stats.get(&right_pred_attnum);
        let estimate = estimate_equi_join_selectivity(EquiJoinInput {
            left_rows,
            right_rows,
            left_ndv: left_stat.and_then(|stat| stat.effective_ndv(left_rows)),
            right_ndv: right_stat.and_then(|stat| stat.effective_ndv(right_rows)),
            left_null_frac: left_stat.and_then(|stat| stat.null_frac),
            right_null_frac: right_stat.and_then(|stat| stat.null_frac),
            left_unique: false,
            right_unique: false,
        });
        selectivity *= estimate.selectivity;
    }

    let mut rows = possible_rows * selectivity;
    if unique_key_covered(&left.unique_keys, &left_join_attnums) {
        rows = rows.min(right_rows);
    }
    if unique_key_covered(&right.unique_keys, &right_join_attnums) {
        rows = rows.min(left_rows);
    }
    Ok((rows / possible_rows).clamp(0.0, 1.0))
}

fn predicate_attnums_for_pair(
    predicate: &JoinPredicate,
    left_rel: usize,
    right_rel: usize,
) -> Result<(i16, i16), PlanBuildError> {
    if predicate.left_rel == left_rel && predicate.right_rel == right_rel {
        Ok((predicate.left_attnum, predicate.right_attnum))
    } else if predicate.left_rel == right_rel && predicate.right_rel == left_rel {
        Ok((predicate.right_attnum, predicate.left_attnum))
    } else {
        Err(PlanBuildError::JoinReorder(
            "predicate group contains an edge for a different relation pair".into(),
        ))
    }
}

fn unique_key_covered(keys: &[PgUniqueKey], join_attnums: &BTreeSet<i16>) -> bool {
    keys.iter().any(|key| {
        key.attnums
            .iter()
            .all(|attnum| join_attnums.contains(attnum))
    })
}

fn build_join_tree(
    set: RelSet,
    solution: &join_order::Solution,
    component: &Component,
) -> Result<LogicalPlan, PlanBuildError> {
    if set.count_ones() == 1 {
        let rel = set.trailing_zeros() as usize;
        return Ok(component.leaves[rel].plan.clone());
    }

    let best = solution.best(set).ok_or_else(|| {
        PlanBuildError::JoinReorder(format!("join_order solution has no entry for set {set:#x}"))
    })?;
    let left = build_join_tree(best.left, solution, component)?;
    let right = build_join_tree(best.right, solution, component)?;
    let on = predicates_crossing_split(best.left, best.right, &component.predicates);
    let schema = build_join_schema(left.schema(), right.schema(), &JoinType::Inner)?;
    Ok(LogicalPlan::Join(Join {
        left: Arc::new(left),
        right: Arc::new(right),
        on,
        filter: None,
        join_type: JoinType::Inner,
        join_constraint: JoinConstraint::On,
        schema: datafusion_common::DFSchemaRef::new(schema),
        null_equals_null: false,
    }))
}

fn predicates_crossing_split(
    left_set: RelSet,
    right_set: RelSet,
    predicates: &[JoinPredicate],
) -> Vec<(Expr, Expr)> {
    predicates
        .iter()
        .filter_map(|predicate| {
            let left_bit = rel_bit(predicate.left_rel as u8);
            let right_bit = rel_bit(predicate.right_rel as u8);
            if left_set & left_bit != 0 && right_set & right_bit != 0 {
                Some((predicate.left_expr.clone(), predicate.right_expr.clone()))
            } else if left_set & right_bit != 0 && right_set & left_bit != 0 {
                Some((predicate.right_expr.clone(), predicate.left_expr.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn preserve_output_order(
    plan: LogicalPlan,
    original_schema: &DFSchemaRef,
) -> Result<LogicalPlan, PlanBuildError> {
    if plan.schema().as_ref() == original_schema.as_ref() {
        return Ok(plan);
    }
    let expr = original_schema
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
        .collect::<Vec<_>>();
    Ok(LogicalPlan::Projection(Projection::try_new(
        expr,
        Arc::new(plan),
    )?))
}

fn leaf_rows(leaf: &Leaf) -> f64 {
    sanitize_rows(
        leaf.estimate
            .expect("leaf estimates loaded before selectivity estimation")
            .rows,
    )
}

fn sanitize_rows(rows: f64) -> f64 {
    if rows.is_finite() && rows > 0.0 {
        rows
    } else {
        0.0
    }
}

fn sanitize_bytes(bytes: f64) -> f64 {
    if bytes.is_finite() && bytes > 0.0 {
        bytes
    } else {
        0.0
    }
}
