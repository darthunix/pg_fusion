# `df_catalog`

`df_catalog` is a narrow backend-side PostgreSQL catalog resolver for future
DataFusion planning.

It resolves a single `TableReference` against live PostgreSQL catalogs and
returns:

- `table_oid`
- canonical `scan_sql::PgRelation`
- Arrow `SchemaRef`

The crate is intentionally small:

- no snapshot handling
- no transport or serialization
- no DataFusion `ContextProvider` / `TableSource`
- no whole-catalog preload or cache

## What This Crate Is For

`df_catalog` is the planning-time catalog surface for the
`scan_sql -> slot_scan` execution path.

Its outputs are meant for:

- DataFusion table binding and logical planning
- PostgreSQL scan SQL compilation via `scan_sql`
- relation identity / diagnostics during backend planning

It is not meant for:

- direct heap-page decoding
- reconstructing physical PostgreSQL attribute layout
- recovering dropped-column positions or other physical tuple metadata from
  `SchemaRef`

Consumers that need physical row metadata must obtain it from the execution
runtime, for example from the live `TupleDesc` exposed by `slot_scan` during
cursor execution.

Resolution is lazy and per-table. Bare names use PostgreSQL search-path
semantics; bare temp-table matches are normalized back to the logical
`pg_temp` alias; schema-qualified names resolve directly; catalog-qualified
names are rejected.

Schema and table identifiers longer than PostgreSQL's max identifier length are
rejected explicitly instead of relying on PostgreSQL truncation behavior.

Supported relation kinds in the current resolver:

- ordinary tables
- partitioned tables
- materialized views

Partitioned parents are supported under this contract because execution goes
through PostgreSQL SQL planning and `slot_scan`, not direct heap-fork reads.
PostgreSQL expands the parent into child scans during planning, and `slot_scan`
validates the resulting portal plan shape at run time.

Supported temporal types in the current schema surface:

- `date`
- `time`
- `timestamp`
- `timestamptz`
- `interval`

Currently unsupported:

- `timetz`

## Example

```rust
use datafusion_common::TableReference;
use df_catalog::{CatalogResolver, PgrxCatalogResolver};

fn resolve_orders() -> Result<(), df_catalog::ResolveError> {
    let resolver = PgrxCatalogResolver::new();

    let bare = resolver.resolve_table(&TableReference::bare("orders"))?;
    println!(
        "oid={} relation={}.{} cols={}",
        bare.table_oid,
        bare.relation.schema.as_deref().unwrap_or("<search_path>"),
        bare.relation.table,
        bare.schema.fields().len()
    );

    let qualified = resolver.resolve_table(&TableReference::partial("public", "orders"))?;
    assert_eq!(qualified.relation.schema.as_deref(), Some("public"));

    Ok(())
}
```
