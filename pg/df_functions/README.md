# df_functions

PostgreSQL-compatible DataFusion functions used by `pg_fusion` planning and
worker execution.

The crate keeps compatibility functions separate from PostgreSQL-bound code so
logical planning, physical planning, and tests can register the same UDF/UDAF
definitions.

Current overrides include PostgreSQL-compatible `avg` aggregation semantics for
the supported Arrow type surface, checked `int2`/`int4`/`int8` arithmetic for
frontend-lowered `+`, `-`, and `*`, restricted residual regex `~`/`!~`
execution over a deterministic safe subset, a `format(text, ...)` scalar
function for ordinary non-`VARIADIC ARRAY` calls, and `quote_literal(text)`.
