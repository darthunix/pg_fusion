---
id: comp-scan-sql-0001
type: fact
scope: scan_sql
tags: ["datafusion", "postgresql", "pushdown", "sql", "scan"]
updated_at: "2026-04-01"
importance: 0.66
---

# Component: scan_sql

- `scan_sql` is a standalone workspace crate under `pg/scan_sql`.
- Scope is intentionally narrow in v1:
  - input is `TableProvider::scan()`-shaped metadata: relation, Arrow schema, projection, filters, and requested limit/fetch hint
  - output is PostgreSQL SQL text for a single base-table `SELECT ... FROM ... WHERE ...` plus limit metadata
  - unsupported expressions are left in `residual_filters`
  - malformed inputs such as invalid projection indices, unknown columns, or wrong relation qualifiers return `CompileError`
- The crate does not depend on `pgrx` and does not execute or plan PostgreSQL queries.
- `scan_sql` is intended to be the trusted upstream producer for `slot_scan`.
  Expression-level safety policy belongs here; `slot_scan` should execute only
  compiler-generated SQL from this crate or another equally trusted source.
- `compile_scan()` currently returns:
  - the rendered SQL string
  - the requested limit/fetch hint from the caller
  - the limit that was actually lowered into SQL, if any
  - selected projection column indices from the DataFusion scan request
  - output column indices in actual PostgreSQL `SELECT` order
  - filter-only column indices referenced only by pushed filters
  - residual-filter column indices that were appended to the output so residual filters can still be re-applied
  - pushed filter fragments with original filter indices
  - residual DataFusion filters
  - an `all_filters_compiled` flag that only means all filters produced PostgreSQL SQL
  - a `uses_dummy_projection` flag for zero-column scans
  - default limit lowering is `ExternalHint`, not SQL `LIMIT`
- Pushdown behavior:
  - top-level `AND` filters are split so supported conjuncts still push down when sibling conjuncts remain residual
  - when filters remain residual, any columns referenced by those residual filters are appended to SQL output so the caller can still evaluate them above the scan
  - supported expression families include columns, common scalar literals, boolean/comparison/arithmetic operators, `LIKE` predicates, `BETWEEN`, `IN`, `CASE`, selected non-temporal casts, and a small scalar-function whitelist
  - empty `IN` lists fold to constant `FALSE` / `TRUE` rather than emitting `IN ()`
  - timestamp literals with time zones, temporal cast targets, regex operators, non-finite float literals, and other PostgreSQL-ambiguous mappings intentionally remain residual in v1
  - compiled SQL is expected to run with PostgreSQL semantics; the crate does not try to preserve exact DataFusion semantics across the engine boundary
  - current PostgreSQL-oriented behavior intentionally includes split top-level `AND`, empty `IN` folding, and `Int8 -> SMALLINT` cast rendering
  - requested limits are treated as fetch hints by default and should be lowered in the default `scan_sql -> slot_scan` path to both `slot_scan::ScanOptions.planner_fetch_hint` and `slot_scan::ScanOptions.local_row_cap`
  - same-named custom DataFusion UDFs are out of scope; the crate is intended for our built-in expression shapes
- Current status:
  - the crate is implemented and unit-tested in isolation
  - `scan::HeapScanProvider` still reports `Unsupported` for filter pushdown and does not use `scan_sql` yet
  - a future backend-side SQL scan crate can consume `CompiledScan` to run PostgreSQL planner/executor scans with pushed projection, filters, and fetch hints
  - that future runtime path should treat `slot_scan` as a trusted executor for compiler-generated SQL, not as a defensive sandbox for arbitrary `SELECT` text
  - when integrated with a `TableProvider`, compiled filters should be treated as PostgreSQL pushdown results rather than proof of DataFusion `Exact` semantics
