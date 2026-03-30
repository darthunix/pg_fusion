# page_arrow_layout

`page_arrow_layout` defines the shared binary layout for the zero-copy Arrow page format used by `pg_slot_arrow` and `page_arrow`.

It is intentionally narrow:

- shared raw layout contract
- no Arrow array construction
- no dependency on `page_transfer`, `page_arrow`, `pg_slot_arrow`, or DataFusion

The crate provides:

- stable `#[repr(C)]` raw structs and constants
- Arrow-schema-to-layout planning helpers
- zero-allocation block access via `BlockRef` / `BlockMut`
- block initialization and validation helpers
- `ByteView` inline / out-of-line constructors and parsers

The public API is re-exported from the crate root. Internally the code is split
into smaller modules (`constants`, `types`, `plan`, `raw`, `access`,
`validate`), but consumers normally use `page_arrow_layout::*`.

The format is a front-and-tail page layout:

- `BlockHeader` + `ColumnDesc[]` live at the front of the page
- the front region reserves fixed-size per-column buffers for all rows up to `max_rows`
- fixed-width values, validity bitmaps, and `ByteView` slots live in that front region
- long `Utf8View` / `BinaryView` payloads live in one shared tail arena
- the tail arena grows toward smaller offsets and all long views use `buffer_index = 0`

```text
low offsets
+--------------------------------------------------------------+
| BlockHeader                                                  |
+--------------------------------------------------------------+
| ColumnDesc[0..N-1]                                           |
+--------------------------------------------------------------+
| front region                                                 |
| preplanned at layout time for all rows up to `max_rows`      |
|                                                              |
|   [col0 validity][col0 values/view slots]                    |
|   [col1 validity][col1 values/view slots]                    |
|   [col2 validity][col2 values/view slots]                    |
|   ...                                                        |
+--------------------------------------------------------------+ <- pool_base
|                      free space                              |
+--------------------------------------------------------------+ <- tail_cursor
| shared tail arena for long Utf8View/BinaryView payloads      |
| grows toward smaller offsets                                 |
+--------------------------------------------------------------+
high offsets / block end
```

Important directional detail:

- the front region occupies lower offsets and is fully reserved during planning
- fixed-width columns and view-slot buffers do not "grow" during append; appends only fill the next row index inside already-reserved regions
- only `tail_cursor` moves at append time, and it moves toward smaller offsets
- front-region offsets use a fixed `BUFFER_ALIGNMENT_BIAS = 12`, so when the block lives inside a `page_transfer` payload with its current 20-byte in-page header, 8-byte and 16-byte Arrow buffers are still physically aligned for zero-copy import

V1 surface:

- `bool`
- `int16`
- `int32`
- `int64`
- `float32`
- `float64`
- `uuid`
- `Utf8View`
- `BinaryView`

## Typical Usage

The crate is usually used in two phases:

1. plan the block shape once for a schema and `max_rows`
2. initialize or open concrete page bytes and read/write in place

### Plan and initialize a block

```rust
use arrow_schema::{DataType, Field, Schema};
use page_arrow_layout::{init_block, BlockMut, LayoutPlan};

let schema = Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8View, true),
]);

let plan = LayoutPlan::from_arrow_schema(&schema, 128, 4096)?;
let mut block_bytes = vec![0u8; plan.block_size() as usize];
init_block(&mut block_bytes, &plan)?;

let block = BlockMut::open(&mut block_bytes)?;
assert_eq!(block.max_rows(), 128);
assert_eq!(block.row_count(), 0);
# Ok::<(), page_arrow_layout::LayoutError>(())
```

### Write rows directly into the block

`BlockMut` is the low-level producer-side API. Fixed-width values are written
into the reserved front region. Long `Utf8View` / `BinaryView` payloads are
allocated from the shared tail arena and then referenced by a `ByteView`.

```rust
use page_arrow_layout::{init_block, bitmap_set, BlockMut, ByteView, LayoutPlan};

# use arrow_schema::{DataType, Field, Schema};
# let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, true), Field::new("name", DataType::Utf8View, true)]);
# let plan = LayoutPlan::from_arrow_schema(&schema, 8, 1024)?;
# let mut block_bytes = vec![0u8; 1024];
# init_block(&mut block_bytes, &plan)?;
let mut block = BlockMut::open(&mut block_bytes)?;

// Row 0, column 0: nullable boolean = true
block.set_validity(0, 0, true)?;
let values = block.values_bytes_mut(0)?;
bitmap_set(values, 0, true);

// Row 0, column 1: long Utf8View stored in the shared tail arena
let value = b"this string does not fit inline";
block.set_validity(1, 0, true)?;
let start = block.tail_alloc(value.len() as u32)?.expect("tail capacity");
block.tail_bytes_mut(start, value.len() as u32)?.copy_from_slice(value);
let view = ByteView::new_outline(value, start - block.pool_base())?;
block.write_view(1, 0, view)?;

block.set_row_count(1)?;
block.validate()?;
# Ok::<(), page_arrow_layout::LayoutError>(())
```

### Read and validate a block without allocation

```rust
use arrow_schema::{DataType, Field, Schema};
use page_arrow_layout::{init_block, BlockRef, LayoutPlan};

# let schema = Schema::new(vec![Field::new("name", DataType::Utf8View, true)]);
# let plan = LayoutPlan::from_arrow_schema(&schema, 4, 512)?;
# let mut block_bytes = vec![0u8; 512];
# init_block(&mut block_bytes, &plan)?;
let block = BlockRef::open(&block_bytes)?;
let header = block.header();
let first_desc = block.desc(0)?;
let first_layout = block.column_layout(0)?;

if first_layout.flags.is_nullable() {
    let is_valid = block.validity(0, 0)?;
    let _ = is_valid;
}

if first_layout.type_tag.is_view() && block.row_count() > 0 {
    let view = block.view(0, 0)?;
    let _ = view;
}
# Ok::<(), page_arrow_layout::LayoutError>(())
```

At this layer there is intentionally no Arrow array construction. Consumers
such as `page_arrow` use `BlockRef` to validate and slice page-backed buffers
without copying.

## View Types

`Utf8View` and `BinaryView` are the only variable-width types in the v1 layout.

Each row stores one fixed-size 16-byte `ByteView` slot in the front region:

```text
+----------------+---------------------------------------------+
| len: i32       | data[12]                                    |
+----------------+---------------------------------------------+
```

Interpretation depends on `len`:

- if `len <= 12`, the value is inline in `data[..len]`
- if `len > 12`, the slot is out-of-line:
  - `data[0..4]` stores the first 4 payload bytes as `prefix4`
  - `data[4..8]` stores `buffer_index`, which is always `0` in v1
  - `data[8..12]` stores `offset` from `pool_base` into the shared tail arena

This means:

- all long string/binary payloads from all view columns share one logical variadic data buffer
- view offsets do not need to be monotonic and payloads from different columns may be interleaved in the shared tail arena
- consumers must treat the shared tail arena as immutable after publication
- plain `Utf8` / `Binary` are intentionally out of scope for this layout
