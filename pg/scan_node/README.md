# scan_node

`scan_node` defines the DataFusion custom logical leaf used for PostgreSQL
scan work that has already been compiled by `scan_sql`.

The crate is intentionally narrow:

- it carries a `PgScanSpec` with `scan_id`, `table_oid`, relation identity,
  `scan_sql::CompiledScan`, logical output schema, and fetch hints
- it provides a DataFusion `UserDefinedLogicalNodeCore` implementation
- it provides an `ExtensionPlanner` hook that delegates physical execution to a
  caller-provided factory
- it provides `PageMaterializeExec` and a physical-plan rewrite that inserts
  copies only before DataFusion operators that can retain page-backed batches

It does not own PostgreSQL snapshots, open backend connections, run `slot_scan`,
serialize plans, or stream pages. Those are later integration layers.

In the intended path, a future backend plan builder compiles a table scan with
`scan_sql`, creates `PgScanSpec`, and sends the logical plan to the worker. The
worker installs `PgScanExtensionPlanner`; the supplied factory then constructs
the runtime execution plan that opens the backend scan by `scan_id`.

`CompiledScan.residual_filters` are not executed inside this node. If residual
filters are semantically required, the plan builder must keep them above the
custom scan node as normal DataFusion expressions.

## Example

```rust,ignore
use std::sync::Arc;

use datafusion_common::DFSchema;
use scan_node::{PgScanNode, PgScanSpec};
use scan_sql::{CompileScanInput, LimitLowering, PgRelation, compile_scan};

// In the real backend planner this schema comes from `df_catalog`.
let source_schema: DFSchema = /* resolved DataFusion schema */;
let relation = PgRelation::new(Some("public"), "orders");

let compiled = compile_scan(CompileScanInput {
    relation: &relation,
    schema: source_schema.as_arrow(),
    identifier_max_bytes: pg_sys::NAMEDATALEN as usize - 1,
    projection: Some(&[0, 2]),
    filters: &filters,
    requested_limit: Some(128),
    limit_lowering: LimitLowering::ExternalHint,
})?;

let spec = Arc::new(PgScanSpec::try_new(
    1,
    table_oid,
    relation,
    &source_schema,
    compiled,
)?);
let logical_plan = PgScanNode::new(spec).into_logical_plan();
```

Worker-side physical planning installs `PgScanExtensionPlanner` with a runtime
factory. The factory receives the same `PgScanSpec` and can open the backend
scan by `scan_id`; later backend code can lower `fetch_hints` into
`slot_scan::ScanOptions`.

After physical planning, callers should run `insert_page_materializers` with a
predicate for their concrete page-backed scan exec type. The rewrite keeps
streaming operators zero-copy and inserts `PageMaterializeExec` at retaining
boundaries such as `SortExec`, window operators, and join build sides. Runtime
planning and backend EXPLAIN both use this rewrite so the rendered plan matches
the worker plan.
