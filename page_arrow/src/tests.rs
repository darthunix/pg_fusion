use super::{
    ArrowPageDecoder, ConfigError, ImportError, ARROW_IPC_BATCH_KIND, BATCH_PAGE_HEADER_LEN,
    BATCH_PAGE_MAGIC, BATCH_PAGE_VERSION,
};
use arrow_array::{
    types::IntervalMonthDayNano, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, NullArray, RecordBatch,
    RecordBatchOptions, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_ipc::{CompressionType, MetadataVersion};
use arrow_schema::{DataType, Field, Schema};
use page_pool::{PagePool, PagePoolConfig, RegionLayout};
use page_transfer::{encode_frame, FrameDecoder, PageRx, PageTx, ReceiveEvent, ReceivedPage};
use std::alloc::{alloc, dealloc, Layout};
use std::io::Write;
use std::ptr::NonNull;
use std::sync::Arc;

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

fn encode_batch_payload(batch: &RecordBatch, options: &IpcWriteOptions) -> Vec<u8> {
    let generator = IpcDataGenerator::default();
    let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);
    let (_, encoded) = generator
        .encoded_batch(batch, &mut dict_tracker, options)
        .expect("encode batch");
    let mut payload = Vec::new();
    payload.extend_from_slice(&BATCH_PAGE_MAGIC.to_le_bytes());
    payload.extend_from_slice(&BATCH_PAGE_VERSION.to_le_bytes());
    payload.extend_from_slice(&0u16.to_le_bytes());

    let mut message = Vec::new();
    let (meta_len, _body_len) =
        write_message(&mut message, encoded, options).expect("write message");
    payload.extend_from_slice(&(meta_len as u32).to_le_bytes());
    payload.extend_from_slice(&message);
    payload
}

fn send_page(pool: PagePool, kind: u16, flags: u16, payload: &[u8]) -> ReceivedPage {
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);
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

fn scalar_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new(
            "t",
            DataType::Time64(arrow_schema::TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "iv",
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            true,
        ),
    ]));

    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(Int16Array::from(vec![Some(-7), None, Some(9)])),
        Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
        Arc::new(Int64Array::from(vec![Some(100), None, Some(300)])),
        Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-2.25)])),
        Arc::new(Float64Array::from(vec![Some(3.5), None, Some(-4.75)])),
        Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])),
        Arc::new(Date32Array::from(vec![Some(10), None, Some(20)])),
        Arc::new(Time64MicrosecondArray::from(vec![Some(11), None, Some(22)])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(111),
            None,
            Some(222),
        ])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(IntervalMonthDayNano::new(1, 2, 3)),
            None,
            Some(IntervalMonthDayNano::new(4, 5, 6)),
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

fn null_only_batch(row_count: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(row_count))]).expect("record batch")
}

#[test]
fn imports_scalar_batch_zero_copy() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let payload = encode_batch_payload(&batch, &options);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
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
fn imports_empty_schema_batch_as_owned_fallback() {
    let batch = empty_schema_batch(3);
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let payload = encode_batch_payload(&batch, &options);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    assert_eq!(pool.snapshot().leased_pages, 1);

    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let imported = decoder.import(page).expect("import");
    assert_eq!(imported, batch);
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn imports_null_only_batch_as_owned_fallback() {
    let batch = null_only_batch(3);
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let payload = encode_batch_payload(&batch, &options);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    assert_eq!(pool.snapshot().leased_pages, 1);

    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let imported = decoder.import(page).expect("import");
    assert_eq!(imported, batch);
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_wrong_kind() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let payload = encode_batch_payload(&batch, &options);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, 9, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("wrong kind");
    assert!(matches!(
        err,
        ImportError::WrongKind {
            expected: ARROW_IPC_BATCH_KIND,
            actual: 9
        }
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_nonzero_flags() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let payload = encode_batch_payload(&batch, &options);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 1, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("nonzero flags");
    assert!(matches!(err, ImportError::UnsupportedFlags { actual: 1 }));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_dictionary_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let err = ArrowPageDecoder::new(schema).expect_err("dictionary schema");
    assert!(matches!(err, ConfigError::UnsupportedDictionarySchema));
}

#[test]
fn rejects_compressed_payload() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5)
        .expect("ipc opts")
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .expect("compression");
    let payload = encode_batch_payload(&batch, &options);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("compressed payload");
    assert!(matches!(err, ImportError::UnsupportedCompression));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_misaligned_payload_without_copy_fallback() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let payload = encode_batch_payload(&batch, &options);
    let meta_len = u32::from_le_bytes(payload[8..12].try_into().expect("meta len"));
    let misaligned = meta_len + 8;
    assert_eq!(meta_len % 16, 0);
    assert_ne!(misaligned % 16, 0);
    let mut payload = payload;
    payload[8..12].copy_from_slice(&misaligned.to_le_bytes());

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("misaligned payload");
    assert!(matches!(
        err,
        ImportError::MisalignedIpcBody {
            actual: actual_meta_len
        } if actual_meta_len == misaligned as usize
    ));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_bad_batch_header() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let mut payload = encode_batch_payload(&batch, &options);
    payload[0..4].copy_from_slice(&0u32.to_le_bytes());

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("bad header");
    assert!(matches!(err, ImportError::InvalidMagic { .. }));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_payload_with_trailing_bytes_after_declared_body() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let mut payload = encode_batch_payload(&batch, &options);
    payload.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("trailing payload bytes");
    assert!(matches!(err, ImportError::TrailingPayloadBytes { .. }));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn rejects_truncated_declared_body() {
    let batch = scalar_batch();
    let options = IpcWriteOptions::try_new(16, false, MetadataVersion::V5).expect("ipc opts");
    let mut payload = encode_batch_payload(&batch, &options);
    payload.truncate(payload.len() - 8);

    let (_region, pool) = init_pool(cfg(8192, 1));
    let page = send_page(pool, ARROW_IPC_BATCH_KIND, 0, &payload);
    let decoder = ArrowPageDecoder::new(batch.schema()).expect("decoder");
    let err = decoder.import(page).expect_err("truncated declared body");
    assert!(matches!(err, ImportError::TruncatedIpcBody { .. }));
    assert_eq!(pool.snapshot().leased_pages, 0);
}

#[test]
fn batch_header_size_keeps_ipc_data_aligned() {
    assert_eq!(BATCH_PAGE_HEADER_LEN, 12);
    assert_eq!((20 + BATCH_PAGE_HEADER_LEN) % 16, 0);
}
