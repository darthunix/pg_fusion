# `row_estimator_seed`

`row_estimator_seed` is a narrow PostgreSQL-specific bridge from live catalog
statistics to `row_estimator::EstimatorConfig`.

It does one thing:

- for physical `Utf8View` / `BinaryView` columns backed by ordinary relation
  attributes, look up PostgreSQL width statistics when they are meaningful for
  encoded tail usage
- if every projected variable-width physical column has a positive width
  statistic, use the sum as `initial_tail_bytes_per_row`
- otherwise leave the caller-provided default unchanged

The crate is intentionally small:

- no adaptive learning state
- no page encoding
- no transport or execution runtime
- no SPI / SQL dependency for statistics lookup

Statistics come from `pg_statistic.stawidth` using direct syscache lookups.
For inherited or partitioned relations, the lookup prefers `stainherit = true`
and falls back to `stainherit = false` when the inherited row is absent.

Synthetic physical columns, such as dummy projection sentinels, are treated as
"no usable statistic" and therefore preserve the caller default.
PostgreSQL `name` columns are treated the same way: they are valid `Utf8View`
inputs for the encoder, but their fixed `NameData` heap width is not a useful
prior for encoded shared-tail bytes, so seeding falls back to the caller
default.

## Examples

Seed one estimator config from relation-backed projected columns:

```rust,ignore
use arrow_layout::TypeTag;
use pgrx::pg_sys;
use row_estimator::EstimatorConfig;
use row_estimator_seed::{ProjectedColumnRef, seed_estimator_config};

fn seed_for_scan(relation_oid: pg_sys::Oid) -> Result<EstimatorConfig, row_estimator_seed::SeedError> {
    let default = EstimatorConfig {
        initial_tail_bytes_per_row: 64,
    };

    seed_estimator_config(
        relation_oid,
        &[
            ProjectedColumnRef::relation_attribute("payload", TypeTag::Utf8View),
            ProjectedColumnRef::relation_attribute("bytes", TypeTag::BinaryView),
        ],
        default,
    )
}
```

If both columns have positive `pg_statistic.stawidth`, the returned config will
use their sum as `initial_tail_bytes_per_row`. If one statistic is missing,
non-positive, or not meaningful for encoded tail usage such as PostgreSQL
`name`, the returned config stays at the caller default.

Fallback behavior for synthetic or stats-free layouts:

```rust,ignore
use arrow_layout::TypeTag;
use pgrx::pg_sys;
use row_estimator::EstimatorConfig;
use row_estimator_seed::{ProjectedColumnRef, seed_estimator_config};

let default = EstimatorConfig {
    initial_tail_bytes_per_row: 96,
};

let seeded = seed_estimator_config(
    pg_sys::InvalidOid,
    &[ProjectedColumnRef::synthetic(TypeTag::Utf8View)],
    default,
)?;

assert_eq!(seeded, default);
```

This is the intended behavior for dummy projection sentinels and any other
physical variable-width column that is not backed by a real PostgreSQL
attribute.
