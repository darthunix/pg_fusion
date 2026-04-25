# scan_sql

`scan_sql` compiles DataFusion scan pushdown inputs into PostgreSQL SQL for a
single base-table scan.

The crate is intentionally small:

- input matches `TableProvider::scan()` concerns: relation, Arrow schema,
  projection, filters, a requested fetch/limit hint, and the live PostgreSQL
  identifier byte limit
- output is PostgreSQL `SELECT ... FROM ... WHERE ...` by default, plus
  metadata describing any requested limit
- unsupported expressions are left as residual filters instead of failing the
  compile
- malformed input such as invalid projection indices or unknown columns returns
  a structured error

The crate does not depend on `pgrx`, does not build PostgreSQL planner nodes,
and does not execute the generated SQL. Its job is only to classify and render
scan pushdown.

In the intended architecture, `scan_sql` is the producer for `slot_scan`:
this crate decides which DataFusion scan shapes are safe to lower into
PostgreSQL SQL, and `slot_scan` later executes that trusted SQL as a PostgreSQL
scan runtime.

Contract:

- everything compiled into SQL is expected to execute with PostgreSQL semantics
- everything left in `residual_filters` is expected to execute above the custom
  scan with DataFusion semantics
- the crate does not try to preserve exact DataFusion semantics across that
  engine boundary
- only SQL produced by this whitelist-oriented compiler is intended to flow
  into `slot_scan`
- requested limits are treated as fetch hints by default and are only rendered
  as SQL `LIMIT` when the caller explicitly opts into that lowering

## API shape

The main entry point is:

```rust,ignore
use arrow_schema::Schema;
use datafusion_expr::Expr;
use scan_sql::{CompileScanInput, LimitLowering, PgRelation, compile_scan};

let relation = PgRelation::new(Some("public"), "users");
let compiled = compile_scan(CompileScanInput {
    relation: &relation,
    schema: &schema,
    identifier_max_bytes: 63,
    projection: Some(&[0, 2]),
    filters: &filters,
    requested_limit: Some(100),
    limit_lowering: LimitLowering::ExternalHint,
})?;
```

`CompiledScan` returns:

- `sql`: deterministic PostgreSQL SQL text
- `requested_limit`: fetch hint requested by the caller
- `sql_limit`: limit that was actually rendered into SQL, if any
- `selected_columns`: the requested projection columns from the DataFusion scan
- `output_columns`: the actual SQL `SELECT` column order
- `filter_only_columns`: columns referenced only by pushed filters and not returned
- `residual_filter_columns`: extra columns appended to `SELECT` so residual
  filters can still be re-applied above PostgreSQL
- `pushed_filters`: per-filter SQL fragments that made it into the `WHERE`
- `residual_filters`: filters that must still run above PostgreSQL
- `all_filters_compiled`: true only when every input filter compiled into
  PostgreSQL SQL

For the intended `scan_sql -> slot_scan` path, use
`LimitLowering::ExternalHint`: `CompiledScan.sql` will omit `LIMIT`, while
`CompiledScan.requested_limit` can be lowered to both
`slot_scan::ScanOptions.planner_fetch_hint` and
`slot_scan::ScanOptions.local_row_cap`.

Runtimes that can apply projection after receiving PostgreSQL slots may call
`render_unprojected_scan_sql(...)` to reuse the same pushed filters and
SQL-level limit while rendering `SELECT *`.

## Pushdown rules

The compiler is whitelist-based. It currently supports:

- column references
- scalar literals for common boolean, numeric, text, bytea, date, time, and
  timestamp values
- boolean and comparison operators
- arithmetic and bitwise operators
- `LIKE`, `ILIKE`, `BETWEEN`, `IN`, `IS NULL`, `IS TRUE`, and related
  predicates
- `CASE`
- `CAST` to PostgreSQL-compatible scalar types, excluding temporal targets in v1
- a small scalar-function subset such as `lower`, `upper`, `trim`, `length`,
  `strpos`, `contains`, and `concat`

Unsupported expressions are not rejected outright. They are returned in
`residual_filters`, and `scan_sql` automatically appends any columns needed by
those residual filters to `output_columns` and the SQL `SELECT` list.

Top-level `AND` filters are split, so supported conjuncts are still pushed even
when one sibling expression must remain residual. That split is intentional and
follows PostgreSQL semantics for the pushed portion, not DataFusion exactness.

## Notes

- The compiler targets a single base relation only.
- `LimitLowering::ExternalHint` is the default and recommended mode.
- callers must supply the live PostgreSQL identifier byte limit, typically
  `pg_sys::NAMEDATALEN as usize - 1` in backend-side code, so overlong schema,
  relation, and column names can be rejected before SQL rendering
- `LimitLowering::SqlClause` is an explicit opt-in for consumers that really
  want exact PostgreSQL `LIMIT` semantics in the generated SQL.
- Zero-column projections are rendered with a synthetic dummy select item to
  preserve row cardinality for later integration.
- Timestamp literals with time zones, all temporal cast targets, regex
  operators, non-finite float literals, and other ambiguous PostgreSQL mappings
  are intentionally left residual in v1.
- Empty `IN ()` / `NOT IN ()` lists are folded to `FALSE` / `TRUE` constants
  instead of being rendered as invalid PostgreSQL syntax.
- `scan_sql` is intended for built-in DataFusion expression shapes in our code;
  it does not try to distinguish same-named custom UDFs.
- `schema` field names are expected to match the actual PostgreSQL column names
  used in the generated SQL.
- When this crate is eventually wired into a `TableProvider`, compiled filters
  should be treated as PostgreSQL pushdown results, not as proof of
  `TableProviderFilterPushDown::Exact`.
- The same applies to `requested_limit`: it is a fetch hint for the scan, not
  proof that the scan itself enforces an exact global limit.
- In the default `slot_scan` runtime path, that fetch hint should be lowered
  twice: once into planner-time fast-start bias and once into a run-time soft
  cap.
- `slot_scan` should be treated as a trusted runtime for this compiler output,
  not as a general-purpose sandbox for arbitrary SQL strings.
