# Compatibility Matrix

[Documentation home](index.md)

This page summarizes how pg_fusion maps PostgreSQL analyzed query semantics
into DataFusion execution. PostgreSQL type OIDs, typmods, collations, resolved
operators, and resolved function overloads remain the source of truth.

DataFusion support alone is not enough. pg_fusion accepts a PostgreSQL
expression only when the current implementation can preserve PostgreSQL
behavior in every place where that expression may run: DataFusion worker
execution, PostgreSQL scan pushdown SQL, residual filters above joins, window
and aggregate execution, and final PostgreSQL tuple-slot projection.

This matrix tracks the current pg_fusion implementation and the
workspace-pinned DataFusion version. It is not a general PostgreSQL or
DataFusion function catalog. A PostgreSQL function or operator with the same
name as a DataFusion function is unsupported unless pg_fusion accepts the
resolved PostgreSQL signature.

## Status Labels

| Label | Meaning |
| --- | --- |
| Supported | Lowered to native DataFusion behavior that matches PostgreSQL for the accepted signature. |
| Supported via pg_fusion UDF | Lowered through a pg_fusion DataFusion UDF/UDAF because native DataFusion behavior differs or is incomplete. |
| Supported in scan SQL | Safe when rendered back into PostgreSQL scan SQL before rows cross into Arrow. |
| Restricted | Supported only for listed typmods, collations, values, argument shapes, or plan positions. |
| Known gap (not supported) | Current implementation gap. Treat this shape as unsupported until the gap is fixed. |
| Unsupported / fail closed | Rejected with a pg_fusion planning error instead of running with uncertain semantics. |

## Type Mapping

| PostgreSQL type | DataFusion / Arrow representation | Status | Notes |
| --- | --- | --- | --- |
| `boolean` | `Boolean` | Supported | Boolean columns, typed NULLs, and boolean predicates are supported. |
| `int2`, `int4`, `int8` | `Int16`, `Int32`, `Int64` | Supported | `+`, `-`, and `*` use checked pg_fusion UDFs for PostgreSQL overflow errors. |
| `float4`, `float8` | `Float32`, `Float64` | Supported | Non-finite constants are allowed so PostgreSQL float aggregate behavior can be preserved. |
| `numeric` | `Decimal128` | Restricted | Finite values only. `NaN`, `Infinity`, and values outside the selected Decimal128 shape fail closed. Bare `numeric` output is canonicalized by trimming trailing fractional zeros; use `numeric(p,s)` when fixed display scale matters. |
| `text` | `Utf8View` | Restricted | Default collation only for DataFusion-side comparison/order semantics. |
| `varchar` | `Utf8View` | Restricted | Typmod casts need PostgreSQL truncation semantics. |
| `bpchar` | `Utf8View` | Restricted | Typmod casts need padding semantics; equality ignores trailing spaces through pg_fusion lowering. |
| `name` | `Utf8View` | Supported | Built-in C collation is accepted for `name`. |
| `bytea` | `BinaryView` | Restricted | Value transport is supported; text-like overloads such as `length(bytea)` are rejected unless explicitly supported. |
| `uuid` | `FixedSizeBinary(16)` | Supported | Transport and equality over supported plans are supported. |
| `date` | `Date32` | Restricted | Non-null constants are still restricted until representation is lossless across all paths. |
| `time` | `Time64(us)` | Restricted | `TIME '24:00:00'` fails closed because PostgreSQL normalizes it through interval arithmetic in scan SQL. |
| `timestamp`, `timestamptz` | `Timestamp(us)` | Restricted | Non-null constants are still restricted until representation is lossless across all paths. |
| `interval` | `MonthDayNano` | Restricted | Finite intervals only; interval infinities fail closed. |
| Other PostgreSQL types | none | Unsupported / fail closed | Not part of the current compatibility surface. |

## Casts

| Cast family | Status | Execution behavior | Scan pushdown behavior |
| --- | --- | --- | --- |
| Numeric and integer casts among supported scalar types | Supported | Lowered to DataFusion casts when result semantics match the accepted PostgreSQL type. | Rendered as PostgreSQL casts when pushed into scan SQL. |
| Cast from integer-like value to `boolean` | Supported | Lowered to `value <> 0` for PostgreSQL-style truth conversion. | Pushdown requires scan SQL renderability of the lowered predicate. |
| Cast from `interval` to text-like output | Supported via pg_fusion UDF | Uses PostgreSQL-style interval text formatting before DataFusion projection. | Not a general scan-pushdown expression. |
| `varchar(n)` | Supported via pg_fusion UDF | DataFusion execution uses a pg_fusion UDF for PostgreSQL truncation. | Base-table predicates render the internal typmod UDF back to `CAST(value AS VARCHAR(n))` when the typmod is the frontend-generated literal. |
| `bpchar(n)` | Supported via pg_fusion UDF | DataFusion execution uses a pg_fusion UDF for PostgreSQL truncation and padding. | Base-table predicates render the internal typmod UDF back to `CAST(value AS CHARACTER(n))` when the typmod is the frontend-generated literal. |
| Text-like casts with unsupported collation | Unsupported / fail closed | Rejected before DataFusion runs with incompatible collation semantics. | Rejected or kept out of pushdown depending on plan position. |
| Temporal constants and casts with lossy representation | Restricted | Fail closed where pg_fusion cannot preserve PostgreSQL value identity. | Fail closed where scan SQL would normalize differently. |

## Operators

| PostgreSQL operator surface | Status | DataFusion behavior | Scan / residual behavior |
| --- | --- | --- | --- |
| Same-type comparisons over supported scalar types | Supported | Lowered to DataFusion comparison operators when PostgreSQL semantics match. | Relation-local predicates may be pushed into PostgreSQL scan SQL. |
| Mixed numeric comparisons | Supported | Numeric operands are cast to a common Decimal128 shape when needed. | Pushdown uses PostgreSQL SQL when renderable. |
| `int2`/`int4`/`int8` `+`, `-`, `*` | Supported via pg_fusion UDF | Checked arithmetic raises PostgreSQL `smallint`, `integer`, or `bigint out of range` errors instead of wrapping. | Internal UDFs should not be pushed unless scan SQL can render equivalent PostgreSQL arithmetic safely. |
| Other arithmetic over supported numeric types | Restricted | Accepted only for resolved `pg_catalog` operators over supported input and result types. | Pushdown is allowed only when scan SQL can render the expression fully. |
| `bpchar` equality and distinctness | Supported via pg_fusion UDF | Operands are compared through a key that ignores trailing padding spaces. | Residual equality filters above joins are allowed; ordering remains restricted. |
| Text ordering and non-default collation comparisons | Unsupported / fail closed | Rejected for DataFusion residual execution because collation semantics are not preserved. | PostgreSQL scan SQL may handle relation-local cases when renderable. |
| `LIKE`/`NOT LIKE`/`ILIKE`/`NOT ILIKE` over text-like values | Supported for scan filters | Lowered to DataFusion LIKE-family operators as the logical representation. Text-sensitive residual filters above joins fail closed instead of relying on DataFusion semantics. | Relation-local predicates render into PostgreSQL scan SQL when the join tree preserves that relation. |
| Regex operators `~`/`!~` over text-like values | Supported for scan filters | Lowered to DataFusion regex operators only as a logical carrier for relation-local scan predicates. Regex residual filters above joins fail closed until pg_fusion provides PostgreSQL-compatible DataFusion regex execution. | Relation-local predicates render into PostgreSQL scan SQL when the join tree preserves that relation. `scan_sql` can also render DataFusion `~*`/`!~*` variants, but pg_frontend does not expose PostgreSQL case-insensitive regex operators yet. |
| User-defined operators with built-in spellings | Unsupported / fail closed | Resolved operator OID must be a supported `pg_catalog` operator. | Prevents silently lowering a user-defined operator to a DataFusion builtin. |

## Scalar Functions

| PostgreSQL function signature | Status | Execution behavior | Scan pushdown behavior |
| --- | --- | --- | --- |
| `abs`, `ceil`/`ceiling`, `floor` over accepted numeric signatures | Supported | Lowered to DataFusion math functions for accepted resolved overloads. | Rendered into PostgreSQL scan SQL when the scan renderer supports the function. |
| `acosh`, `asinh`, `atanh`, `cosh`, `sinh`, `tanh`, `exp`, `ln`, `sqrt` over accepted float signatures | Supported | Lowered to DataFusion math functions for accepted resolved overloads. | Rendered into PostgreSQL scan SQL when supported by the scan renderer. |
| `power(float, float)` returning a supported float type | Supported | Lowered to DataFusion `power`. | Rendered as PostgreSQL `power(left, right)` when pushed. |
| `round(numeric)` and `trunc(numeric)` | Supported | Lowered to Decimal128-compatible execution. | Rendered as PostgreSQL `round(value)` and `trunc(value)` when pushed. |
| `round(numeric, int4)` and `trunc(numeric, int4)` | Supported via pg_fusion UDF | DataFusion execution uses pg_fusion Decimal128 UDFs. | Pushed predicates render the scale argument as PostgreSQL `integer`, preserving the resolved `numeric, int4` overload. |
| `length(text)`, `length(varchar)` | Supported | Lowered to DataFusion character length for accepted text signatures. | Rendered as PostgreSQL `char_length` or `length`. |
| `length(bpchar)` | Supported via pg_fusion UDF | Ignores trailing padding spaces like PostgreSQL. | Rendered as PostgreSQL `length(value)` when pushed. |
| `length(bytea)` | Unsupported / fail closed | Rejected even though PostgreSQL supports a byte-length overload. | Prevents byte-length overloads from using text semantics. |
| `concat`, `concat_ws` over supported argument types | Supported | Lowered to DataFusion string concatenation after PostgreSQL-compatible textification; boolean arguments use the internal `pg_fusion_boolout` UDF for `boolout` text (`t`/`f`). | Pushdown requires full scan SQL rendering. |
| `format` | Supported via pg_fusion UDF | Uses pg_fusion formatting semantics for accepted argument shapes; boolean arguments use PostgreSQL `boolout` text (`t`/`f`). | Pushdown requires full scan SQL rendering; otherwise it stays above scan or fails by position. |
| `quote_literal(text-like)` | Supported via pg_fusion UDF | Uses pg_fusion PostgreSQL-style literal quoting. | Pushdown requires full scan SQL rendering. |
| `reverse(text-like)` | Supported | Lowered to DataFusion unicode reverse for accepted text signatures. | Rendered into PostgreSQL scan SQL when supported by the scan renderer. |
| `nullif(left, right)` | Supported | Lowered to DataFusion `nullif` for accepted resolved expression types. | Rendered as PostgreSQL `nullif(left, right)` when pushed. |
| `random()` | Supported | Lowered to DataFusion `random`. | Not a scan pushdown predicate. |
| Supported function name with unsupported overload | Unsupported / fail closed | Rejected by resolved PostgreSQL signature, for example non-text `length` overloads. | Prevents overloads from using the wrong DataFusion semantics. |
| User-defined functions with supported names | Unsupported / fail closed | Resolved function namespace and signature must match supported PostgreSQL catalog overloads. | Prevents lowering a user function to a DataFusion builtin by name only. |
| Supported function receiving boolean wrappers, NULL tests, or scalar subqueries as arguments | Supported | The adapter infers boolean wrapper result types as `boolean` and scalar subquery arguments from their single visible target type. | Scalar subqueries with anything other than one visible target remain fail-closed. |
| DataFusion functions not listed here | Unsupported / fail closed | Not accepted just because DataFusion has a builtin implementation. | pg_fusion must first add a PostgreSQL signature mapping and semantic tests. |

## Aggregate Functions

| PostgreSQL aggregate surface | Status | Execution behavior | Notes |
| --- | --- | --- | --- |
| `count(*)`, `count(expr)`, `count(DISTINCT expr)` | Supported | Lowered to DataFusion count aggregate variants. | `count(*)` also works as an aggregate window input. |
| `sum(expr)`, `sum(DISTINCT expr)` | Supported | Lowered to DataFusion `sum` after PostgreSQL-compatible argument preparation. | Numeric and integer result behavior is constrained by supported types. |
| `avg(expr)`, `avg(DISTINCT expr)` | Supported via pg_fusion UDF | Uses pg_fusion `avg` UDAF for PostgreSQL-compatible integer, float, decimal, and interval behavior. | PostgreSQL numeric `NaN`/`Infinity` remains unsupported. |
| `min(expr)`, `max(expr)` | Supported | Lowered to DataFusion min/max. | Distinct is accepted but does not change min/max semantics. |
| `stddev_pop`, `stddev_samp`/`stddev`, `var_pop`, `var_samp`/`variance` | Supported | Lowered to DataFusion statistical aggregates after PostgreSQL-compatible float argument preparation. | `DISTINCT` is supported through DataFusion aggregate distinct handling. |
| `regr_count`, `regr_sxx`, `regr_syy`, `regr_sxy`, `regr_avgx`, `regr_avgy`, `regr_r2`, `regr_slope`, `regr_intercept` | Supported | Lowered to DataFusion regression aggregates after PostgreSQL-compatible float argument preparation. | Requires the supported two-argument aggregate shape. |
| `covar_pop`, `covar_samp`, `corr` | Supported | Lowered to DataFusion covariance/correlation aggregates after PostgreSQL-compatible float argument preparation. | Requires the supported two-argument aggregate shape. |
| `string_agg(value, delimiter)` | Restricted | Lowered to DataFusion `string_agg` for the accepted two-argument shape. | Other PostgreSQL `string_agg` variants are unsupported unless explicitly accepted. |
| `GROUPING(...)` | Restricted | Lowered to DataFusion grouping aggregate for grouping-set metadata. | Not supported as a window function. |
| Scalar subquery cardinality guard | Supported via pg_fusion UDAF | Uses a pg_fusion aggregate to enforce PostgreSQL scalar-subquery single-row cardinality. | Internal planning helper, not a user-visible aggregate. |
| Aggregate function name not listed here | Unsupported / fail closed | Rejected by pg_fusion aggregate classifier. | DataFusion aggregate availability alone does not imply support. |
| Listed aggregate with unsupported argument count or shape | Unsupported / fail closed | Rejected during aggregate lowering. | Examples include unsupported `GROUPING()` shapes and unsupported `string_agg` variants. |

## Window Functions

| PostgreSQL window surface | Status | Execution behavior | Notes |
| --- | --- | --- | --- |
| `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist` | Supported | Lowered to DataFusion ranking/window UDFs. | These rank-like functions reject unexpected arguments. |
| `ntile(expr)` | Supported | Lowered to DataFusion `ntile`. | Requires exactly one argument. |
| `first_value(expr)`, `last_value(expr)` | Supported | Lowered to DataFusion value window UDFs. | Require exactly one argument. |
| `lag(expr[, offset[, default]])`, `lead(expr[, offset[, default]])` | Supported | Lowered to DataFusion lead/lag UDFs. | Require one to three arguments. |
| `nth_value(expr, n)` | Supported | Lowered to DataFusion `nth_value`. | Requires exactly two arguments. |
| Aggregate window functions over supported aggregates | Restricted | Lowered to DataFusion aggregate-window definitions, with pg_fusion `avg` where needed. | `FILTER` is accepted only for aggregate window functions. |
| `GROUPING()` as a window function | Unsupported / fail closed | Rejected explicitly. | `GROUPING()` is only supported in aggregate/grouping-set context. |
| Window function name not listed here | Unsupported / fail closed | Rejected by pg_fusion window classifier. | DataFusion window availability alone does not imply support. |

## Expression Wrappers

| Expression wrapper | Status | Notes |
| --- | --- | --- |
| Relabels | Supported | Preserve PostgreSQL type identity while allowing DataFusion-compatible physical representation. |
| `CASE` | Supported | Lowered to DataFusion case expressions for supported branch/result types. |
| `COALESCE` | Supported | Lowered to DataFusion coalesce expressions for supported argument/result types. |
| Array constructors | Restricted | Lowered to DataFusion nested array construction for supported element types and expression shapes. |
| Array subscripts | Restricted | Lowered to DataFusion array element extraction for supported array/index expression shapes. |
| Boolean expressions, NULL tests, and boolean tests in predicates | Supported | Produce PostgreSQL boolean semantics for ordinary predicates. |
| Boolean expressions, NULL tests, and boolean tests as scalar function arguments | Supported | Classifier paths infer these wrapper results as `boolean`. |
| Scalar subqueries in supported predicate/projected shapes | Restricted | Supported only where pg_fusion can enforce PostgreSQL scalar-subquery cardinality. |
| Scalar subqueries as scalar function arguments | Restricted | The classifier infers the type from the single visible target; other target shapes fail closed. |
| Parameters | Unsupported / fail closed | Bound and prepared-statement parameters are not part of the current public support surface. |
| Unsupported residual filters above joins | Unsupported / fail closed | Expressions that require PostgreSQL-specific text, regex, collation, or uncovered function semantics fail closed instead of running in DataFusion. Relation-local regex predicates must render into PostgreSQL scan SQL. |
