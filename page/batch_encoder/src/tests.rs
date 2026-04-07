use super::{AppendResult, BatchPageEncoder, ConfigError, EncodeError};
use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use arrow_layout::constants::VIEW_INLINE_LEN;
use arrow_layout::{init_block, BlockRef, ColumnSpec, LayoutPlan, TypeTag};
use arrow_schema::{DataType, Field, Schema};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

struct CountingAllocator;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        TRACK_ALLOCATIONS.with(|tracking| {
            if tracking.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        TRACK_ALLOCATIONS.with(|tracking| {
            if tracking.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        TRACK_ALLOCATIONS.with(|tracking| {
            if tracking.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }
}

struct AllocationTrackingGuard;

impl AllocationTrackingGuard {
    fn start() -> Self {
        TRACK_ALLOCATIONS.with(|tracking| assert!(!tracking.get(), "nested allocation tracking"));
        TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));
        ALLOCATION_COUNT.with(|count| count.set(0));
        Self
    }
}

impl Drop for AllocationTrackingGuard {
    fn drop(&mut self) {
        TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    }
}

fn count_thread_allocations<F, T>(f: F) -> (usize, T)
where
    F: FnOnce() -> T,
{
    TRACK_ALLOCATIONS.with(|_| {});
    ALLOCATION_COUNT.with(|_| {});
    let _guard = AllocationTrackingGuard::start();
    let result = f();
    let allocations = ALLOCATION_COUNT.with(|count| count.get());
    (allocations, result)
}

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), true),
        Field::new("txt", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
    ]))
}

fn layout_plan(max_rows: u32, block_size: u32) -> LayoutPlan {
    LayoutPlan::new(
        &[
            ColumnSpec::new(TypeTag::Boolean, true),
            ColumnSpec::new(TypeTag::Int16, true),
            ColumnSpec::new(TypeTag::Int32, true),
            ColumnSpec::new(TypeTag::Int64, true),
            ColumnSpec::new(TypeTag::Float32, true),
            ColumnSpec::new(TypeTag::Float64, true),
            ColumnSpec::new(TypeTag::Uuid, true),
            ColumnSpec::new(TypeTag::Utf8View, true),
            ColumnSpec::new(TypeTag::BinaryView, true),
        ],
        max_rows,
        block_size,
    )
    .expect("plan")
}

fn mixed_batch() -> RecordBatch {
    let schema = input_schema();
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
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [
                    Some([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                    None,
                    Some([16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]),
                    Some([0; 16]),
                ]
                .into_iter(),
                16,
            )
            .expect("uuid"),
        ),
        Arc::new(StringArray::from(vec![
            Some("short"),
            None,
            Some("this string is definitely longer than twelve bytes"),
            Some(""),
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(&b"\x01\x02"[..]),
            None,
            Some(&b"this binary payload is also longer than twelve bytes"[..]),
            Some(&b""[..]),
        ])),
    ];

    RecordBatch::try_new(schema, columns).expect("record batch")
}

fn init_payload(plan: &LayoutPlan) -> Vec<u8> {
    let mut payload = vec![0u8; usize::try_from(plan.block_size()).expect("block size")];
    init_block(&mut payload, plan).expect("init block");
    payload
}

#[test]
fn new_accepts_plain_utf8_and_binary_for_view_layout() {
    let schema = input_schema();
    let plan = layout_plan(8, 2048);
    let mut payload = init_payload(&plan);

    BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");
}

#[test]
fn new_rejects_schema_plan_type_mismatch() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8View, true)]));
    let plan =
        LayoutPlan::new(&[ColumnSpec::new(TypeTag::BinaryView, true)], 4, 256).expect("plan");
    let mut payload = init_payload(&plan);

    let err = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect_err("mismatch");
    match err {
        ConfigError::SchemaPlanTypeMismatch {
            index, type_tag, ..
        } => {
            assert_eq!(index, 0);
            assert_eq!(type_tag, TypeTag::BinaryView);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn append_batch_writes_mixed_rows() {
    let batch = mixed_batch();
    let output_schema = Arc::new(Schema::new(vec![
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
    let plan = layout_plan(8, 4096);
    let mut payload = init_payload(&plan);
    let schema = batch.schema();
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");

    let appended = encoder.append_batch(&batch, 0).expect("append batch");
    assert_eq!(
        appended,
        AppendResult {
            rows_written: 4,
            full: false
        }
    );
    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 4);

    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 4);
    assert!(block.bool_value(0, 0).expect("bool"));
    assert!(!block.validity(0, 1).expect("null bool"));
    assert_eq!(
        i32::from_ne_bytes(
            block.fixed_value(2, 2).expect("i32")[..4]
                .try_into()
                .unwrap()
        ),
        30
    );
    assert_eq!(
        i64::from_ne_bytes(
            block.fixed_value(3, 3).expect("i64")[..8]
                .try_into()
                .unwrap()
        ),
        -400
    );
    let txt0 = block.view(7, 0).expect("txt0");
    assert!(txt0.is_inline().expect("txt0 inline"));
    let txt2 = block.view(7, 2).expect("txt2");
    assert!(!txt2.is_inline().expect("txt2 inline"));
    let bin2 = block.view(8, 2).expect("bin2");
    assert!(!bin2.is_inline().expect("bin2 inline"));
    assert_eq!(block.null_count(7).expect("txt null count"), 1);
    assert_eq!(block.null_count(8).expect("bin null count"), 1);

    let import_plan =
        LayoutPlan::from_arrow_schema(output_schema.as_ref(), 8, 4096).expect("import plan");
    assert_eq!(import_plan.block_size(), plan.block_size());
}

#[test]
fn append_batch_accepts_view_inputs() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("txt", DataType::Utf8View, true),
        Field::new("bin", DataType::BinaryView, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringViewArray::from(vec![
                Some("inline"),
                Some("this string is long enough for outline storage"),
            ])) as ArrayRef,
            Arc::new(BinaryViewArray::from(vec![
                Some(&b"\x01\x02"[..]),
                Some(&b"this binary payload is long enough for outline storage"[..]),
            ])) as ArrayRef,
        ],
    )
    .expect("batch");
    let plan = LayoutPlan::new(
        &[
            ColumnSpec::new(TypeTag::Utf8View, true),
            ColumnSpec::new(TypeTag::BinaryView, true),
        ],
        4,
        1024,
    )
    .expect("plan");
    let mut payload = init_payload(&plan);
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");

    let appended = encoder.append_batch(&batch, 0).expect("append");
    assert_eq!(appended.rows_written, 2);
    assert!(!appended.full);
    let _encoded = encoder.finish().expect("finish");

    let block = BlockRef::open(&payload).expect("block");
    assert!(block
        .view(0, 0)
        .expect("txt0")
        .is_inline()
        .expect("txt0 inline"));
    assert!(!block
        .view(0, 1)
        .expect("txt1")
        .is_inline()
        .expect("txt1 inline"));
    assert!(block
        .view(1, 0)
        .expect("bin0")
        .is_inline()
        .expect("bin0 inline"));
    assert!(!block
        .view(1, 1)
        .expect("bin1")
        .is_inline()
        .expect("bin1 inline"));
}

#[test]
fn append_batch_stops_at_exact_tail_fit() {
    let schema = Arc::new(Schema::new(vec![Field::new("txt", DataType::Utf8, true)]));
    let plan = LayoutPlan::new(&[ColumnSpec::new(TypeTag::Utf8View, true)], 4, 166).expect("plan");
    let per_row_len = usize::try_from(plan.shared_pool_capacity() / 2 + 1).expect("len");
    assert!(per_row_len > VIEW_INLINE_LEN);
    let long = "x".repeat(per_row_len);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![
            Some(long.as_str()),
            Some(long.as_str()),
            Some(long.as_str()),
        ])) as ArrayRef],
    )
    .expect("batch");
    let mut payload = init_payload(&plan);
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");

    let appended = encoder.append_batch(&batch, 0).expect("append");
    assert_eq!(appended.rows_written, 1);
    assert!(appended.full);
    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 1);

    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.view(0, 0).expect("view").len().expect("len"),
        per_row_len
    );
}

#[test]
fn append_batch_returns_retryable_full_for_empty_page_overestimate() {
    let schema = Arc::new(Schema::new(vec![Field::new("txt", DataType::Utf8, true)]));
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let plan = LayoutPlan::new(&specs, 2, 160).expect("plan");
    let single_row_plan = LayoutPlan::new(&specs, 1, 160).expect("single-row plan");
    let retryable_len = usize::try_from(plan.shared_pool_capacity()).expect("capacity") + 4;
    assert!(retryable_len > VIEW_INLINE_LEN);
    assert!(retryable_len <= usize::try_from(single_row_plan.shared_pool_capacity()).unwrap());
    let retryable = "x".repeat(retryable_len);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some(retryable.as_str())])) as ArrayRef],
    )
    .expect("batch");
    let mut payload = init_payload(&plan);
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");

    let appended = encoder.append_batch(&batch, 0).expect("retryable full");
    assert_eq!(
        appended,
        AppendResult {
            rows_written: 0,
            full: true
        }
    );

    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 0);
    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.tail_cursor(), block.block_size());
}

#[test]
fn append_batch_rejects_row_that_cannot_fit_even_with_single_row_layout() {
    let schema = Arc::new(Schema::new(vec![Field::new("txt", DataType::Utf8, true)]));
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let plan = LayoutPlan::new(&specs, 1, 160).expect("plan");
    let oversized_len = usize::try_from(plan.shared_pool_capacity()).expect("capacity") + 1;
    let oversized = "x".repeat(oversized_len);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some(oversized.as_str())])) as ArrayRef],
    )
    .expect("batch");
    let mut payload = init_payload(&plan);
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");

    let err = encoder.append_batch(&batch, 0).expect_err("oversized row");
    match err {
        EncodeError::RowTooLargeForPage {
            row,
            required_tail,
            page_tail_capacity,
        } => {
            assert_eq!(row, 0);
            assert_eq!(
                required_tail,
                u32::try_from(oversized_len).expect("u32 len")
            );
            assert_eq!(page_tail_capacity, plan.shared_pool_capacity());
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 0);
    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 0);
    assert_eq!(block.tail_cursor(), block.block_size());
}

#[test]
fn append_batch_is_allocation_free_after_construction() {
    let batch = mixed_batch();
    let plan = layout_plan(8, 4096);
    let mut payload = init_payload(&plan);
    let schema = batch.schema();
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");

    let (allocations, appended) =
        count_thread_allocations(|| encoder.append_batch(&batch, 0).expect("append"));
    assert_eq!(allocations, 0);
    assert_eq!(appended.rows_written, 4);
}

#[test]
fn finish_is_allocation_free() {
    let schema = Arc::new(Schema::new(vec![Field::new("i32", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef],
    )
    .expect("batch");
    let plan = LayoutPlan::new(&[ColumnSpec::new(TypeTag::Int32, true)], 4, 256).expect("plan");
    let mut payload = init_payload(&plan);
    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");
    encoder.append_batch(&batch, 0).expect("append");

    let (allocations, encoded) = count_thread_allocations(|| encoder.finish().expect("finish"));
    assert_eq!(allocations, 0);
    assert_eq!(encoded.row_count, 3);
}

#[test]
fn append_batch_can_continue_from_start_row() {
    let schema = Arc::new(Schema::new(vec![Field::new("i64", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
        ])) as ArrayRef],
    )
    .expect("batch");
    let plan = LayoutPlan::new(&[ColumnSpec::new(TypeTag::Int64, true)], 2, 256).expect("plan");

    let mut first_payload = init_payload(&plan);
    let mut first =
        BatchPageEncoder::new(schema.as_ref(), &plan, &mut first_payload).expect("encoder");
    let first_append = first.append_batch(&batch, 0).expect("append");
    assert_eq!(first_append.rows_written, 2);
    assert!(first_append.full);
    first.finish().expect("finish");

    let mut second_payload = init_payload(&plan);
    let mut second =
        BatchPageEncoder::new(schema.as_ref(), &plan, &mut second_payload).expect("encoder");
    let second_append = second
        .append_batch(&batch, first_append.rows_written)
        .expect("append second");
    assert_eq!(second_append.rows_written, 2);
    assert!(!second_append.full);
    second.finish().expect("finish second");

    let block = BlockRef::open(&second_payload).expect("block");
    assert_eq!(
        i64::from_ne_bytes(
            block.fixed_value(0, 0).expect("value0")[..8]
                .try_into()
                .unwrap()
        ),
        30
    );
    assert_eq!(
        i64::from_ne_bytes(
            block.fixed_value(0, 1).expect("value1")[..8]
                .try_into()
                .unwrap()
        ),
        40
    );
}
