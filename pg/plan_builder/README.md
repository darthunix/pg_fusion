# plan_builder

`plan_builder` builds backend-side DataFusion logical plans whose PostgreSQL
table leaves are lowered into `scan_node::PgScanNode`.

The crate is intentionally a planning bridge:

- input is SQL text plus positional DataFusion `ScalarValue` params
- relation metadata comes from `df_catalog`
- pushdown SQL is compiled by `scan_sql`
- PostgreSQL scan leaves are represented by `scan_node::PgScanSpec`
- non-recursive CTEs referenced more than once are lowered through
  `scan_node::PgCteRefNode` so the worker can materialize them once and reuse
  the same batches
- snapshot ownership, plan serialization, backend scan serving, and page
  transport are left to later layers
- subquery expressions are accepted when DataFusion optimization rewrites them
  into ordinary relational operators above PostgreSQL scan leaves

The output logical plan is the future `plan_codec` serialization target. It is
self-contained enough to carry the compiled scan SQL, relation identity,
`scan_id`, `table_oid`, and fetch hints, but it does not contain snapshot ids.

## Execution Contract

`PlanBuilder` performs DataFusion logical optimization with
`target_partitions = 1` in v1. This avoids inventing DataFusion-level
multi-partition semantics for one PostgreSQL scan id. It does not disable
PostgreSQL-side parallel planning: `slot_scan` can still prepare and run a
PostgreSQL plan that contains `Gather` or other PostgreSQL parallel scan nodes.

Filter pushdown is deliberately two-stage. The planning `TableSource` reports
filters as exactly pushable so DataFusion attaches them to `TableScan`. During
lowering, `scan_sql` recompiles those filters and returns any unsupported
predicates as residual filters. `plan_builder` restores those residual
predicates above `PgScanNode` and projects away residual-only columns if needed.

Subquery expressions are validated after DataFusion logical optimization. If
DataFusion decorrelates or rewrites them into joins, aggregates, projections,
and filters, `plan_builder` can lower the remaining table leaves normally.
Residual `EXISTS (...)`, `IN (SELECT ...)`, scalar subqueries,
`LogicalPlan::Subquery`, or correlated `OuterReferenceColumn` nodes are still
rejected before lowering so the later `plan_codec` contract only needs to
round-trip ordinary relational operators plus PostgreSQL leaf scans.

DataFusion normally clones CTE plans at every reference. Before calling
`SqlToRel`, `plan_builder` rewrites multi-use CTE definitions to synthetic
planning table sources and plans the original CTE body separately. During scan
lowering, each synthetic source becomes a `PgCteRefNode` with the lowered CTE
definition as its child. This preserves PostgreSQL-style "compute once, read
many" behavior for floating aggregates and other non-bit-stable computations.

## Example

```rust,ignore
use datafusion_common::ScalarValue;
use plan_builder::{PlanBuildInput, PlanBuilder};

let builder = PlanBuilder::new();
let built = builder.build(PlanBuildInput {
    sql: "SELECT id, payload FROM public.orders WHERE id > $1 LIMIT 32",
    params: vec![ScalarValue::Int64(Some(10))],
})?;

for scan in &built.scans {
    println!(
        "scan {} table_oid={} sql={}",
        scan.scan_id.get(),
        scan.table_oid,
        scan.compiled_scan.sql,
    );
}
```

Worker-side physical planning later installs `scan_node::PgScanExtensionPlanner`
and provides a runtime-specific execution factory for the scan specs.
