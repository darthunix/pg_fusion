# row_encoder

`row_encoder` is the PostgreSQL-free hot write path for producing
`arrow_layout` pages from typed row values.

The crate does not know about `TupleTableSlot`, detoasting, SPI, DataFusion, or
shared-memory ownership. Callers provide an initialized `arrow_layout` block and
a `RowSource`; the encoder writes fixed-width values, validity bits,
`Utf8View`/`BinaryView` slots, and long view payload bytes directly into that
block.

`pg/slot_encoder` is the PostgreSQL adapter on top of this crate. It validates a
`TupleDesc`, deforms slots, detoasts PostgreSQL varlena values, then passes
typed `CellRef` values into `row_encoder`.

## Typical Usage

```rust,ignore
use arrow_layout::{init_block, ColumnSpec, LayoutPlan, TypeTag};
use row_encoder::{CellRef, PageRowEncoder, RowSource};

struct OneRow(i32);

impl RowSource for OneRow {
    type Error = row_encoder::RowEncodeError;

    fn with_cell<R>(
        &mut self,
        index: usize,
        f: impl FnOnce(CellRef<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        assert_eq!(index, 0);
        f(CellRef::Int32(self.0))
    }
}

let plan = LayoutPlan::new(&[ColumnSpec::new(TypeTag::Int32, false)], 1024, 65516)?;
let mut payload = vec![0; plan.block_size() as usize];
init_block(&mut payload, &plan)?;

let mut encoder = PageRowEncoder::new(&mut payload)?;
let mut row = OneRow(42);
encoder.append_row(&mut row)?;
let encoded = encoder.finish()?;
assert_eq!(encoded.row_count, 1);
```

## Q05 Encoding Benchmark

The q05 benchmark is a standalone `cargo bench` target that does not require a
running PostgreSQL server:

```sh
PG_FUSION_TPCH_DIR=benches/tpch/data/sf_0_01 \
  cargo bench -p row_encoder --bench q05_encode
```

If `PG_FUSION_TPCH_DIR` is unset, the benchmark looks for
`benches/tpch/data/sf_0_01` relative to the repository root. If no CSV data is
available, it still runs deterministic synthetic q05-like fixtures.

The live fixture mirrors the columns encoded by the q05 PostgreSQL leaf scans:

- `lineitem`: `l_orderkey`, `l_suppkey`, `l_extendedprice`, `l_discount`
- `orders`: `o_orderkey`, `o_custkey` after the q05 order-date predicate
- `customer`: `c_custkey`, `c_nationkey`
- `supplier`: `s_suppkey`, `s_nationkey`
- `nation`: `n_nationkey`, `n_name`, `n_regionkey`
- `region`: `r_regionkey` after `r_name = 'ASIA'`

Use this benchmark as the first-pass CPU baseline for PostgreSQL-free Arrow
page writing. PostgreSQL slot deformation and datum extraction still require
PostgreSQL-side profiling.
