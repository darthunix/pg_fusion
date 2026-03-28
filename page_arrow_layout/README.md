# page_arrow_layout

`page_arrow_layout` defines the shared binary layout for the next zero-copy Arrow page format.

It is intentionally narrow:

- layout contract only
- no page mutation logic
- no Arrow array construction
- no dependency on `page_transfer`, `page_arrow`, `pg_slot_arrow`, or DataFusion

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
| ColumnDesc[0..N-1]                                            |
+--------------------------------------------------------------+
| front region                                                  |
| preplanned at layout time for all rows up to `max_rows`       |
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

The crate provides:

- stable `#[repr(C)]` raw structs and constants
- Arrow-schema-to-layout planning helpers
- layout validation helpers
- `ByteView` inline / out-of-line constructors and parsers

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
