use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use arrow_layout::constants::VIEW_INLINE_LEN;
use arrow_layout::{init_block, ColumnSpec, LayoutPlan, TypeTag};
use batch_encoder::BatchPageEncoder;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};

const BLOCK_SIZE: u32 = 8192;
const FIXED_ROWS: usize = 128;
const MIXED_ROWS: usize = 48;
const SHORT_TEXT: &str = "short";
const LONG_TEXT: &str = "this string is intentionally longer than the inline byte-view threshold";
const SHORT_BINARY: &[u8] = b"\x01\x02\x03\x04";
const LONG_BINARY: &[u8] = b"this binary payload is also intentionally longer than inline";

struct BenchFixture {
    name: &'static str,
    schema: Arc<Schema>,
    plan: LayoutPlan,
    batch: RecordBatch,
    rows_expected: usize,
}

impl BenchFixture {
    fn measure_append_batch(&self, iterations: u64) -> Duration {
        let mut payload = vec![0u8; usize::try_from(self.plan.block_size()).expect("block size")];
        let mut total = Duration::ZERO;

        for _ in 0..iterations {
            init_block(&mut payload, &self.plan).expect("init block");
            let mut encoder = BatchPageEncoder::new(self.schema.as_ref(), &self.plan, &mut payload)
                .expect("encoder");

            let start = Instant::now();
            let result = encoder
                .append_batch(black_box(&self.batch), black_box(0))
                .expect("append batch");
            total += start.elapsed();

            assert_eq!(result.rows_written, self.rows_expected, "rows written");
            assert!(!result.full, "fixture unexpectedly filled the page");

            let encoded = encoder.finish().expect("finish");
            assert_eq!(encoded.row_count, self.rows_expected, "encoded row count");
            black_box(encoded.payload_len);
        }

        total
    }
}

fn fixed_width_fixture() -> BenchFixture {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), true),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(
            (0..FIXED_ROWS)
                .map(|row| Some(row % 2 == 0))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int16Array::from(
            (0..FIXED_ROWS)
                .map(|row| {
                    if row % 5 == 0 {
                        None
                    } else {
                        Some(row as i16 - 64)
                    }
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            (0..FIXED_ROWS)
                .map(|row| Some((row as i32 * 17) - 1000))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            (0..FIXED_ROWS)
                .map(|row| Some((row as i64 * 101) - 10_000))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float32Array::from(
            (0..FIXED_ROWS)
                .map(|row| Some((row as f32 * 1.25) - 3.5))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            (0..FIXED_ROWS)
                .map(|row| Some((row as f64 * 2.5) - 7.0))
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                (0..FIXED_ROWS).map(|row| {
                    if row % 7 == 0 {
                        None
                    } else {
                        Some([
                            row as u8,
                            row.wrapping_add(1) as u8,
                            row.wrapping_add(2) as u8,
                            row.wrapping_add(3) as u8,
                            row.wrapping_add(4) as u8,
                            row.wrapping_add(5) as u8,
                            row.wrapping_add(6) as u8,
                            row.wrapping_add(7) as u8,
                            row.wrapping_add(8) as u8,
                            row.wrapping_add(9) as u8,
                            row.wrapping_add(10) as u8,
                            row.wrapping_add(11) as u8,
                            row.wrapping_add(12) as u8,
                            row.wrapping_add(13) as u8,
                            row.wrapping_add(14) as u8,
                            row.wrapping_add(15) as u8,
                        ])
                    }
                }),
                16,
            )
            .expect("uuid array"),
        ),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns).expect("record batch");
    let plan = LayoutPlan::new(
        &[
            ColumnSpec::new(TypeTag::Boolean, true),
            ColumnSpec::new(TypeTag::Int16, true),
            ColumnSpec::new(TypeTag::Int32, true),
            ColumnSpec::new(TypeTag::Int64, true),
            ColumnSpec::new(TypeTag::Float32, true),
            ColumnSpec::new(TypeTag::Float64, true),
            ColumnSpec::new(TypeTag::Uuid, true),
        ],
        FIXED_ROWS as u32,
        BLOCK_SIZE,
    )
    .expect("layout plan");

    BenchFixture {
        name: "fixed_width_128rows_8k",
        schema,
        plan,
        batch,
        rows_expected: FIXED_ROWS,
    }
}

fn text_value(row: usize) -> Option<&'static str> {
    match row % 3 {
        0 => None,
        1 => Some(if SHORT_TEXT.len() <= VIEW_INLINE_LEN {
            SHORT_TEXT
        } else {
            unreachable!("SHORT_TEXT must stay inline")
        }),
        _ => Some(LONG_TEXT),
    }
}

fn binary_value(row: usize) -> Option<&'static [u8]> {
    match row % 3 {
        0 => None,
        1 => Some(if SHORT_BINARY.len() <= VIEW_INLINE_LEN {
            SHORT_BINARY
        } else {
            unreachable!("SHORT_BINARY must stay inline")
        }),
        _ => Some(LONG_BINARY),
    }
}

fn build_mixed_fixed_columns(rows: usize) -> Vec<ArrayRef> {
    vec![
        Arc::new(BooleanArray::from(
            (0..rows)
                .map(|row| Some((row & 1) == 0))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int16Array::from(
            (0..rows)
                .map(|row| {
                    if row % 6 == 0 {
                        None
                    } else {
                        Some(row as i16 - 24)
                    }
                })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            (0..rows)
                .map(|row| Some((row as i32 * 31) - 500))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            (0..rows)
                .map(|row| Some((row as i64 * 257) - 50_000))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float32Array::from(
            (0..rows)
                .map(|row| Some((row as f32 * 0.75) - 10.0))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            (0..rows)
                .map(|row| Some((row as f64 * 1.5) - 20.0))
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                (0..rows).map(|row| {
                    if row % 8 == 0 {
                        None
                    } else {
                        Some([
                            row as u8, 0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07, 0x18, 0x29, 0x3A,
                            0x4B, 0x5C, 0x6D, 0x7E, 0x8F,
                        ])
                    }
                }),
                16,
            )
            .expect("uuid array"),
        ),
    ]
}

fn mixed_views_fixture() -> BenchFixture {
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

    let mut columns = build_mixed_fixed_columns(MIXED_ROWS);
    columns.push(Arc::new(StringViewArray::from(
        (0..MIXED_ROWS).map(text_value).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(BinaryViewArray::from(
        (0..MIXED_ROWS).map(binary_value).collect::<Vec<_>>(),
    )));

    let batch = RecordBatch::try_new(schema.clone(), columns).expect("record batch");
    let plan = LayoutPlan::new(
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
        MIXED_ROWS as u32,
        BLOCK_SIZE,
    )
    .expect("layout plan");

    BenchFixture {
        name: "mixed_views_48rows_8k",
        schema,
        plan,
        batch,
        rows_expected: MIXED_ROWS,
    }
}

fn plain_lowering_fixture() -> BenchFixture {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), true),
        Field::new("txt", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
    ]));

    let mut columns = build_mixed_fixed_columns(MIXED_ROWS);
    columns.push(Arc::new(StringArray::from(
        (0..MIXED_ROWS).map(text_value).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(BinaryArray::from(
        (0..MIXED_ROWS).map(binary_value).collect::<Vec<_>>(),
    )));

    let batch = RecordBatch::try_new(schema.clone(), columns).expect("record batch");
    let plan = LayoutPlan::new(
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
        MIXED_ROWS as u32,
        BLOCK_SIZE,
    )
    .expect("layout plan");

    BenchFixture {
        name: "plain_lowering_48rows_8k",
        schema,
        plan,
        batch,
        rows_expected: MIXED_ROWS,
    }
}

fn bench_batch_encode(c: &mut Criterion) {
    let fixtures = [
        fixed_width_fixture(),
        mixed_views_fixture(),
        plain_lowering_fixture(),
    ];

    let mut group = c.benchmark_group("batch_encode");
    for fixture in &fixtures {
        group.throughput(Throughput::Elements(fixture.rows_expected as u64));
        group.bench_function(fixture.name, |b| {
            b.iter_custom(|iters| fixture.measure_append_batch(iters))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_batch_encode);
criterion_main!(benches);
