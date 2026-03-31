use super::{ArrowPageDecoder, ConfigError, ImportError, ARROW_LAYOUT_BATCH_KIND};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, RecordBatchOptions,
    StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use layout::{init_block, BlockMut, BlockRef, ByteView, LayoutPlan, ViewWriteStatus};
use pgrx_pg_sys as pg_sys;
use pool::{PagePool, PagePoolConfig, RegionLayout};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use std::alloc::{alloc, dealloc, Layout};
use std::io::Write;
use std::ptr::NonNull;
use std::sync::Arc;
use transfer::{encode_frame, FrameDecoder, PageRx, PageTx, ReceiveEvent, ReceivedPage};

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn new(region: RegionLayout) -> Self {
        let layout = Layout::from_size_align(region.size, region.align).expect("layout");
        let base = unsafe { alloc(layout) };
        let base = NonNull::new(base).expect("allocation failed");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

fn cfg(page_size: usize, page_count: u32) -> PagePoolConfig {
    PagePoolConfig::new(page_size, page_count).expect("valid pool config")
}

fn init_pool(config: PagePoolConfig) -> (OwnedRegion, PagePool) {
    let layout = PagePool::layout(config).expect("pool layout");
    let region = OwnedRegion::new(layout);
    let pool =
        unsafe { PagePool::init_in_place(region.base, layout.size, config) }.expect("pool init");
    (region, pool)
}

fn send_page_via(tx: &PageTx, rx: &PageRx, kind: u16, flags: u16, payload: &[u8]) -> ReceivedPage {
    let mut writer = tx.begin(kind, flags).expect("begin");
    writer.write_all(payload).expect("write payload");
    let outbound = writer.finish().expect("finish");
    let frame = encode_frame(outbound.frame()).expect("encode frame");
    outbound.mark_sent();

    let mut decoder = FrameDecoder::new();
    let frame = decoder
        .push(&frame)
        .next()
        .expect("frame")
        .expect("decoded frame");
    match rx.accept(frame).expect("accept") {
        ReceiveEvent::Page(page) => page,
        ReceiveEvent::Closed => panic!("unexpected close"),
    }
}

fn send_page(pool: PagePool, kind: u16, flags: u16, payload: &[u8]) -> ReceivedPage {
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);
    send_page_via(&tx, &rx, kind, flags, payload)
}

#[derive(Clone, Copy)]
struct TestAttr {
    oid: pg_sys::Oid,
    attlen: i16,
    attbyval: bool,
    attalign: u8,
}

struct OwnedTupleDesc {
    ptr: pg_sys::TupleDesc,
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedTupleDesc {
    fn new(attrs: &[TestAttr]) -> Self {
        let size = std::mem::size_of::<pg_sys::TupleDescData>()
            + attrs.len() * std::mem::size_of::<pg_sys::FormData_pg_attribute>();
        let layout =
            Layout::from_size_align(size, std::mem::align_of::<pg_sys::TupleDescData>()).unwrap();
        let base = unsafe { alloc(layout) };
        let base = NonNull::new(base).expect("tuple desc allocation");
        unsafe { base.as_ptr().write_bytes(0, size) };
        let ptr = base.as_ptr().cast::<pg_sys::TupleDescData>();

        unsafe {
            (*ptr).natts = attrs.len() as i32;
            let attrs_ptr = (*ptr).attrs.as_mut_ptr();
            for (index, spec) in attrs.iter().copied().enumerate() {
                let attr = &mut *attrs_ptr.add(index);
                *attr = pg_sys::FormData_pg_attribute::default();
                attr.atttypid = spec.oid;
                attr.attlen = spec.attlen;
                attr.attnum = (index + 1) as i16;
                attr.attbyval = spec.attbyval;
                attr.attalign = spec.attalign as i8;
            }
        }

        Self { ptr, base, layout }
    }
}

impl Drop for OwnedTupleDesc {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

struct OwnedSlot {
    slot: Box<pg_sys::TupleTableSlot>,
    values: Vec<pg_sys::Datum>,
    isnull: Vec<bool>,
    _cells: Vec<MockCell>,
}

impl OwnedSlot {
    fn from_cells(tupdesc: pg_sys::TupleDesc, mut cells: Vec<MockCell>) -> Self {
        let mut values = Vec::with_capacity(cells.len());
        let mut isnull = Vec::with_capacity(cells.len());
        for cell in &mut cells {
            let (datum, is_null) = cell.datum();
            values.push(datum);
            isnull.push(is_null);
        }
        let mut slot = Box::new(unsafe {
            std::mem::MaybeUninit::<pg_sys::TupleTableSlot>::zeroed().assume_init()
        });
        slot.tts_tupleDescriptor = tupdesc;
        slot.tts_nvalid = values.len() as i16;
        let mut owned = Self {
            slot,
            values,
            isnull,
            _cells: cells,
        };
        owned.slot.tts_values = owned.values.as_mut_ptr();
        owned.slot.tts_isnull = owned.isnull.as_mut_ptr();
        owned
    }

    fn as_mut_ptr(&mut self) -> *mut pg_sys::TupleTableSlot {
        &mut *self.slot
    }
}

enum MockCell {
    Null,
    Bool(bool),
    I32(i32),
    Uuid(Box<[u8; 16]>),
}

impl MockCell {
    fn datum(&mut self) -> (pg_sys::Datum, bool) {
        match self {
            Self::Null => (pg_sys::Datum::null(), true),
            Self::Bool(value) => (pg_sys::Datum::from(*value), false),
            Self::I32(value) => (pg_sys::Datum::from(*value), false),
            Self::Uuid(value) => (pg_sys::Datum::from(value.as_mut_ptr()), false),
        }
    }
}

fn uuid_array(values: [Option<[u8; 16]>; 4]) -> FixedSizeBinaryArray {
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 16).expect("uuid")
}

fn mixed_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), true),
        Field::new("txt", DataType::Utf8View, true),
        Field::new("bin", DataType::BinaryView, true),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int16Array::from(vec![Some(-7), None, Some(9), Some(12)])),
        Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(-40)])),
        Arc::new(Int64Array::from(vec![
            Some(100),
            None,
            Some(300),
            Some(-400),
        ])),
        Arc::new(Float32Array::from(vec![
            Some(1.5),
            None,
            Some(-2.25),
            Some(0.0),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(3.5),
            None,
            Some(-4.75),
            Some(8.25),
        ])),
        Arc::new(uuid_array([
            Some([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            None,
            Some([16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]),
            Some([0; 16]),
        ])),
        Arc::new(StringViewArray::from(vec![
            Some("short"),
            None,
            Some("this string is definitely longer than twelve bytes"),
            Some(""),
        ])),
        Arc::new(BinaryViewArray::from(vec![
            Some(&b"\x01\x02"[..]),
            None,
            Some(&b"this binary payload is also longer than twelve bytes"[..]),
            Some(&b""[..]),
        ])),
    ];

    RecordBatch::try_new(schema, columns).expect("record batch")
}

fn empty_schema_batch(row_count: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .expect("record batch")
}

fn encode_layout_payload(batch: &RecordBatch, block_size: usize) -> Vec<u8> {
    let plan = LayoutPlan::from_arrow_schema(
        batch.schema().as_ref(),
        u32::try_from(batch.num_rows()).expect("rows"),
        u32::try_from(block_size).expect("block size"),
    )
    .expect("layout plan");
    let mut payload = vec![0u8; block_size];
    init_block(&mut payload, &plan).expect("init block");

    {
        let mut block = BlockMut::open(&mut payload).expect("open block");
        for row in 0..u32::try_from(batch.num_rows()).expect("rows") {
            for col in 0..batch.num_columns() {
                let layout = block.column_layout(col).expect("column layout");
                let array = batch.column(col);
                if !array.is_valid(row as usize) {
                    block.write_null(col, row).expect("write null");
                    continue;
                }

                match layout.type_tag {
                    layout::TypeTag::Boolean => {
                        let value = array
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .expect("bool")
                            .value(row as usize);
                        block.write_bool(col, row, value).expect("write bool");
                    }
                    layout::TypeTag::Int16 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .expect("i16")
                            .value(row as usize)
                            .to_le_bytes();
                        block.write_fixed(col, row, &value).expect("write fixed");
                    }
                    layout::TypeTag::Int32 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("i32")
                            .value(row as usize)
                            .to_le_bytes();
                        block.write_fixed(col, row, &value).expect("write fixed");
                    }
                    layout::TypeTag::Int64 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("i64")
                            .value(row as usize)
                            .to_le_bytes();
                        block.write_fixed(col, row, &value).expect("write fixed");
                    }
                    layout::TypeTag::Float32 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .expect("f32")
                            .value(row as usize)
                            .to_bits()
                            .to_le_bytes();
                        block.write_fixed(col, row, &value).expect("write fixed");
                    }
                    layout::TypeTag::Float64 => {
                        let value = array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("f64")
                            .value(row as usize)
                            .to_bits()
                            .to_le_bytes();
                        block.write_fixed(col, row, &value).expect("write fixed");
                    }
                    layout::TypeTag::Uuid => {
                        let value = array
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .expect("uuid")
                            .value(row as usize);
                        block.write_fixed(col, row, value).expect("write fixed");
                    }
                    layout::TypeTag::Utf8View => {
                        let value = array
                            .as_any()
                            .downcast_ref::<StringViewArray>()
                            .expect("utf8 view")
                            .value(row as usize)
                            .as_bytes();
                        assert_eq!(
                            block.write_view_bytes(col, row, value).expect("write view"),
                            ViewWriteStatus::Written
                        );
                    }
                    layout::TypeTag::BinaryView => {
                        let value = array
                            .as_any()
                            .downcast_ref::<BinaryViewArray>()
                            .expect("binary view")
                            .value(row as usize);
                        assert_eq!(
                            block.write_view_bytes(col, row, value).expect("write view"),
                            ViewWriteStatus::Written
                        );
                    }
                }
            }
            block.commit_current_row().expect("commit row");
        }
        block.validate().expect("validate block");
    }

    payload
}

#[test]
fn imports_mixed_batch_zero_copy() {
    let batch = mixed_batch();
    let payload = encode_layout_payload(&batch, 4096);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    assert_eq!(pool.snapshot().leased_pages, 1);

    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let imported = decoder.import(page).expect("import");
    assert_eq!(imported, batch);
    assert_eq!(pool.snapshot().leased_pages, 1);

    let column = imported.column(0).clone();
    drop(imported);
    assert_eq!(pool.snapshot().leased_pages, 1);
    drop(column);
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn import_does_not_block_accepting_next_page() {
    let batch = mixed_batch();
    let payload = encode_layout_payload(&batch, 4096);

    let (_region, pool) = init_pool(cfg(8192, 2));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");

    let page1 = send_page_via(&tx, &rx, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let imported1 = decoder.import(page1).expect("import page1");
    assert_eq!(imported1, batch);
    assert_eq!(pool.snapshot().leased_pages, 1);

    let page2 = send_page_via(&tx, &rx, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let imported2 = decoder.import(page2).expect("import page2");
    assert_eq!(imported2, batch);
    assert_eq!(pool.snapshot().leased_pages, 2);

    drop(imported2);
    assert_eq!(pool.snapshot().leased_pages, 1);
    drop(imported1);
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn imports_empty_schema_batch_as_owned_fallback() {
    let batch = empty_schema_batch(3);
    let payload = encode_layout_payload(&batch, 512);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    assert_eq!(pool.snapshot().leased_pages, 1);

    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let imported = decoder.import(page).expect("import");
    assert_eq!(imported, batch);
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_wrong_kind() {
    let batch = mixed_batch();
    let payload = encode_layout_payload(&batch, 4096);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, 9, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("wrong kind");
    assert!(matches!(
        err,
        ImportError::WrongKind {
            expected: ARROW_LAYOUT_BATCH_KIND,
            actual: 9
        }
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_nonzero_flags() {
    let batch = mixed_batch();
    let payload = encode_layout_payload(&batch, 4096);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 1, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("nonzero flags");
    assert!(matches!(err, ImportError::UnsupportedFlags { actual: 1 }));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_plain_utf8_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("txt", DataType::Utf8, true)]));
    let err = ArrowPageDecoder::new(schema).expect_err("unsupported schema");
    assert!(matches!(
        err,
        ConfigError::UnsupportedArrowType { index: 0, .. }
    ));
}

#[test]
fn rejects_schema_type_mismatch() {
    let batch = mixed_batch();
    let payload = encode_layout_payload(&batch, 4096);
    let mismatch_schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i16", DataType::Int32, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), true),
        Field::new("txt", DataType::Utf8View, true),
        Field::new("bin", DataType::BinaryView, true),
    ]));

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(mismatch_schema).expect("decoder");
    let err = decoder.import(page).expect_err("schema mismatch");
    assert!(matches!(
        err,
        ImportError::SchemaTypeMismatch {
            index: 1,
            actual: layout::TypeTag::Int16,
            ..
        }
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_invalid_view_buffer_index() {
    let batch = mixed_batch();
    let mut payload = encode_layout_payload(&batch, 4096);
    let block = BlockRef::open(&payload).expect("block");
    let layout = block.column_layout(7).expect("txt layout");
    let row = 2usize;
    let slot_off =
        usize::try_from(layout.values_off).expect("offset") + row * std::mem::size_of::<ByteView>();
    payload[slot_off + 4..slot_off + 8].copy_from_slice(&1i32.to_le_bytes());

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("invalid view");
    assert!(matches!(
        err,
        ImportError::Arrow(arrow_schema::ArrowError::InvalidArgumentError(_))
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_null_bitmap_count_mismatch() {
    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BooleanArray::from(vec![Some(true), Some(false)]))],
    )
    .expect("batch");
    let mut payload = encode_layout_payload(&batch, 512);

    {
        let mut block = BlockMut::open(&mut payload).expect("block");
        block.set_validity(0, 1, false).expect("clear bit");
        block.validate().expect("validate");
    }

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(schema).expect("decoder");
    let err = decoder.import(page).expect_err("null bitmap mismatch");
    assert!(matches!(
        err,
        ImportError::NullBitmapCountMismatch {
            index: 0,
            expected: 0,
            actual: 1
        }
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_view_offset_before_allocated_tail() {
    let batch = mixed_batch();
    let mut payload = encode_layout_payload(&batch, 4096);

    {
        let mut block = BlockMut::open(&mut payload).expect("block");
        let long_value = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("string views")
            .value(2);
        block
            .write_view(
                7,
                2,
                ByteView::new_outline(long_value.as_bytes(), 0).expect("view"),
            )
            .expect("write view");
        block.validate().expect("validate");
    }

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder
        .import(page)
        .expect_err("view before allocated tail");
    assert!(matches!(
        err,
        ImportError::ViewOffsetBeforeAllocatedTail {
            index: 7,
            row: 2,
            offset: 0,
            ..
        }
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn imports_slot_encoder_produced_payload() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), true),
    ]));
    let attrs = [
        TestAttr {
            oid: pg_sys::BOOLOID,
            attlen: 1,
            attbyval: true,
            attalign: b'c',
        },
        TestAttr {
            oid: pg_sys::INT4OID,
            attlen: 4,
            attbyval: true,
            attalign: b'i',
        },
        TestAttr {
            oid: pg_sys::UUIDOID,
            attlen: 16,
            attbyval: false,
            attalign: b'c',
        },
    ];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let plan = LayoutPlan::from_arrow_schema(schema.as_ref(), 3, 4096).expect("plan");
    let mut payload = vec![0u8; 4096];
    init_block(&mut payload, &plan).expect("init block");

    let expected = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(Int32Array::from(vec![Some(11), None, Some(-22)])),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [
                        Some([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                        None,
                        Some([16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]),
                    ]
                    .into_iter(),
                    16,
                )
                .expect("uuid"),
            ),
        ],
    )
    .expect("expected batch");

    unsafe {
        let mut encoder = PageBatchEncoder::new(tuple_desc.ptr, &mut payload).expect("encoder");
        let mut row1 = OwnedSlot::from_cells(
            tuple_desc.ptr,
            vec![
                MockCell::Bool(true),
                MockCell::I32(11),
                MockCell::Uuid(Box::new([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                ])),
            ],
        );
        let mut row2 = OwnedSlot::from_cells(
            tuple_desc.ptr,
            vec![MockCell::Null, MockCell::Null, MockCell::Null],
        );
        let mut row3 = OwnedSlot::from_cells(
            tuple_desc.ptr,
            vec![
                MockCell::Bool(false),
                MockCell::I32(-22),
                MockCell::Uuid(Box::new([
                    16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1,
                ])),
            ],
        );
        assert_eq!(
            encoder
                .append_slot(row1.as_mut_ptr())
                .expect("append row 1"),
            AppendStatus::Appended
        );
        assert_eq!(
            encoder
                .append_slot(row2.as_mut_ptr())
                .expect("append row 2"),
            AppendStatus::Appended
        );
        assert_eq!(
            encoder
                .append_slot(row3.as_mut_ptr())
                .expect("append row 3"),
            AppendStatus::Appended
        );
        let encoded = encoder.finish().expect("finish");
        assert_eq!(encoded.row_count, 3);
        assert_eq!(encoded.payload_len, 4096);
    }

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_LAYOUT_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(schema).expect("decoder");
    let imported = decoder.import(page).expect("import");
    assert_eq!(imported, expected);
    drop(imported);
    assert_eq!(pool.snapshot().leased_pages, 0);
}
