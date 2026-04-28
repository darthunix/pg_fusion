use crate::{AppendStatus, CellRef, PageRowEncoder, RowEncodeError, RowSource};
use arrow_layout::{init_block, BlockRef, ColumnSpec, LayoutPlan, TypeTag};

struct VecRow<'a> {
    cells: &'a [CellRef<'a>],
}

impl RowSource for VecRow<'_> {
    type Error = RowEncodeError;

    fn with_cell<R>(
        &mut self,
        index: usize,
        f: impl FnOnce(CellRef<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        f(self.cells[index])
    }
}

fn init_payload(specs: &[ColumnSpec], max_rows: u32, block_size: usize) -> Vec<u8> {
    let plan = LayoutPlan::new(specs, max_rows, block_size as u32).expect("layout plan");
    let mut payload = vec![0u8; block_size];
    init_block(&mut payload, &plan).expect("init block");
    payload
}

#[test]
fn appends_fixed_and_view_values() {
    let specs = [
        ColumnSpec::new(TypeTag::Int32, false),
        ColumnSpec::new(TypeTag::Utf8View, false),
        ColumnSpec::new(TypeTag::Float64, false),
    ];
    let mut payload = init_payload(&specs, 4, 1024);
    let mut encoder = PageRowEncoder::new(&mut payload).expect("encoder");
    let mut row = VecRow {
        cells: &[
            CellRef::Int32(42),
            CellRef::Utf8(b"q05"),
            CellRef::Float64(12.5),
        ],
    };

    assert_eq!(
        encoder.append_row(&mut row).expect("append"),
        AppendStatus::Appended
    );
    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 1);

    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 1);
}

#[test]
fn full_rolls_back_long_view_tail() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut payload = init_payload(&specs, 2, 128);
    let mut encoder = PageRowEncoder::new(&mut payload).expect("encoder");
    let before = encoder.tail_cursor_for_tests();
    let long = vec![b'x'; 128];
    let mut row = VecRow {
        cells: &[CellRef::Utf8(&long)],
    };

    assert_eq!(
        encoder.append_row(&mut row).expect("append"),
        AppendStatus::Full
    );
    assert_eq!(encoder.tail_cursor_for_tests(), before);
}

#[test]
fn rejects_null_for_non_nullable_column() {
    let specs = [ColumnSpec::new(TypeTag::Int32, false)];
    let mut payload = init_payload(&specs, 1, 128);
    let mut encoder = PageRowEncoder::new(&mut payload).expect("encoder");
    let mut row = VecRow {
        cells: &[CellRef::Null],
    };

    let error = encoder.append_row(&mut row).expect_err("null error");
    assert!(matches!(
        error,
        RowEncodeError::NullInNonNullableColumn { index: 0 }
    ));
}
