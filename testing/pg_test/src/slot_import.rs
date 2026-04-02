use anyhow::{anyhow, bail, Result as AnyResult};
use arrow_layout::{init_block, BlockMut, LayoutPlan, ViewWriteStatus};
use arrow_schema::{DataType, Field, Schema};
use import::ARROW_LAYOUT_BATCH_KIND;
use pgrx::prelude::*;
use pgrx::varlena::{text_to_rust_str_unchecked, varlena_to_byte_slice};
use pgrx::PgMemoryContexts;
use pool::{PagePool, PagePoolConfig, RegionLayout};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use slot_import::{ArrowSlotProjector, ConfigError, ProjectError};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;
use transfer::{encode_frame, FrameDecoder, PageRx, PageTx, ReceiveEvent, ReceivedPage};

const SLOT_IMPORT_TABLE: &str = "pg_temp.slot_import_fixture";
const SLOT_IMPORT_NAME_TABLE: &str = "pg_temp.slot_import_name_fixture";
const SLOT_IMPORT_VARCHAR_TABLE: &str = "pg_temp.slot_import_varchar_fixture";
const SLOT_IMPORT_BPCHAR_TABLE: &str = "pg_temp.slot_import_bpchar_fixture";
const SLOT_IMPORT_PAGE_SIZE: usize = 8192;
const SLOT_IMPORT_PAGE_COUNT: u32 = 4;

#[derive(Clone, Copy, Debug, PartialEq)]
struct FixtureRow {
    bool_value: Option<bool>,
    int16_value: Option<i16>,
    int32_value: Option<i32>,
    int64_value: Option<i64>,
    float32_value: Option<f32>,
    float64_value: Option<f64>,
    uuid_value: Option<[u8; 16]>,
    text_value: Option<&'static str>,
    binary_value: Option<&'static [u8]>,
}

#[derive(Debug, PartialEq)]
struct DecodedRow {
    bool_value: Option<bool>,
    int16_value: Option<i16>,
    int32_value: Option<i32>,
    int64_value: Option<i64>,
    float32_value: Option<f32>,
    float64_value: Option<f64>,
    uuid_value: Option<[u8; 16]>,
    text_value: Option<String>,
    binary_value: Option<Vec<u8>>,
}

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn new(region: RegionLayout) -> Self {
        let layout = Layout::from_size_align(region.size, region.align).expect("region layout");
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("region allocation");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

struct ImportHarness {
    _region: OwnedRegion,
    pool: PagePool,
    tx: PageTx,
    rx: PageRx,
    payload_capacity: usize,
}

struct OpenRelation {
    rel: pg_sys::Relation,
}

impl OpenRelation {
    fn open(qualified: &str) -> Self {
        let rel = unsafe { relation_open_by_name(qualified) };
        Self { rel }
    }

    fn tuple_desc(&self) -> pg_sys::TupleDesc {
        unsafe { (*self.rel).rd_att }
    }
}

impl Drop for OpenRelation {
    fn drop(&mut self) {
        unsafe { pg_sys::relation_close(self.rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
    }
}

struct OwnedMinimalSlot {
    slot: *mut pg_sys::TupleTableSlot,
}

impl OwnedMinimalSlot {
    fn new(tupdesc: pg_sys::TupleDesc) -> AnyResult<Self> {
        let slot =
            unsafe { pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsMinimalTuple) };
        if slot.is_null() {
            bail!("MakeSingleTupleTableSlot returned null");
        }
        Ok(Self { slot })
    }

    fn store(&mut self, tuple: &OwnedMinimalTuple) {
        unsafe {
            pg_sys::ExecStoreMinimalTuple(tuple.ptr, self.slot, false);
        }
    }

    fn as_mut_ptr(&mut self) -> *mut pg_sys::TupleTableSlot {
        self.slot
    }
}

impl Drop for OwnedMinimalSlot {
    fn drop(&mut self) {
        unsafe { pg_sys::ExecDropSingleTupleTableSlot(self.slot) };
    }
}

struct OwnedVirtualSlot {
    slot: *mut pg_sys::TupleTableSlot,
}

impl OwnedVirtualSlot {
    fn new(tupdesc: pg_sys::TupleDesc) -> AnyResult<Self> {
        let slot = unsafe { pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsVirtual) };
        if slot.is_null() {
            bail!("MakeSingleTupleTableSlot(TTSOpsVirtual) returned null");
        }
        Ok(Self { slot })
    }

    fn as_mut_ptr(&mut self) -> *mut pg_sys::TupleTableSlot {
        self.slot
    }
}

impl Drop for OwnedVirtualSlot {
    fn drop(&mut self) {
        unsafe { pg_sys::ExecDropSingleTupleTableSlot(self.slot) };
    }
}

struct OwnedMinimalTuple {
    ptr: pg_sys::MinimalTuple,
}

impl OwnedMinimalTuple {
    fn new(tupdesc: pg_sys::TupleDesc, row: &FixtureRow) -> AnyResult<Self> {
        let mut values = [pg_sys::Datum::null(); 9];
        let mut nulls = [true; 9];
        let mut uuid_storage = row.uuid_value;
        let text = row.text_value.map(pgrx::varlena::rust_str_to_text_p);
        let bytea = row
            .binary_value
            .map(pgrx::varlena::rust_byte_slice_to_bytea);

        if let Some(value) = row.bool_value {
            values[0] = pg_sys::Datum::from(value);
            nulls[0] = false;
        }
        if let Some(value) = row.int16_value {
            values[1] = pg_sys::Datum::from(value);
            nulls[1] = false;
        }
        if let Some(value) = row.int32_value {
            values[2] = pg_sys::Datum::from(value);
            nulls[2] = false;
        }
        if let Some(value) = row.int64_value {
            values[3] = pg_sys::Datum::from(value);
            nulls[3] = false;
        }
        if let Some(value) = row.float32_value {
            values[4] = pg_sys::Datum::from(value.to_bits());
            nulls[4] = false;
        }
        if let Some(value) = row.float64_value {
            values[5] = pg_sys::Datum::from(value.to_bits());
            nulls[5] = false;
        }
        if let Some(uuid) = uuid_storage.as_mut() {
            values[6] = pg_sys::Datum::from(uuid.as_mut_ptr());
            nulls[6] = false;
        }
        if let Some(text) = text.as_ref() {
            values[7] = pg_sys::Datum::from(text.as_ptr());
            nulls[7] = false;
        }
        if let Some(bytea) = bytea.as_ref() {
            values[8] = pg_sys::Datum::from(bytea.as_ptr());
            nulls[8] = false;
        }

        let ptr = unsafe {
            pg_sys::heap_form_minimal_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr())
        };
        if ptr.is_null() {
            bail!("heap_form_minimal_tuple returned null");
        }
        Ok(Self { ptr })
    }
}

impl Drop for OwnedMinimalTuple {
    fn drop(&mut self) {
        unsafe { pg_sys::heap_free_minimal_tuple(self.ptr) };
    }
}

fn fixture_rows() -> [FixtureRow; 4] {
    [
        FixtureRow {
            bool_value: Some(true),
            int16_value: Some(7),
            int32_value: Some(11),
            int64_value: Some(17),
            float32_value: Some(1.5),
            float64_value: Some(3.25),
            uuid_value: Some([1; 16]),
            text_value: Some("short"),
            binary_value: Some(b"\x00\x01"),
        },
        FixtureRow {
            bool_value: None,
            int16_value: None,
            int32_value: None,
            int64_value: None,
            float32_value: None,
            float64_value: None,
            uuid_value: None,
            text_value: None,
            binary_value: None,
        },
        FixtureRow {
            bool_value: Some(false),
            int16_value: Some(-8),
            int32_value: Some(-42),
            int64_value: Some(-9001),
            float32_value: Some(-2.5),
            float64_value: Some(-6.5),
            uuid_value: Some([2; 16]),
            text_value: Some("this string is definitely longer than twelve bytes"),
            binary_value: Some(b"this binary payload is also longer than twelve bytes"),
        },
        FixtureRow {
            bool_value: Some(true),
            int16_value: Some(0),
            int32_value: Some(0),
            int64_value: Some(0),
            float32_value: Some(0.0),
            float64_value: Some(0.0),
            uuid_value: Some([0; 16]),
            text_value: Some(""),
            binary_value: Some(b""),
        },
    ]
}

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i2", DataType::Int16, true),
        Field::new("i4", DataType::Int32, true),
        Field::new("i8", DataType::Int64, true),
        Field::new("f4", DataType::Float32, true),
        Field::new("f8", DataType::Float64, true),
        Field::new("u", DataType::FixedSizeBinary(16), true),
        Field::new("t", DataType::Utf8View, true),
        Field::new("bytes", DataType::BinaryView, true),
    ]))
}

fn reset_slot_import_table() {
    Spi::run("DROP TABLE IF EXISTS pg_temp.slot_import_fixture").unwrap();
    Spi::run(
        "CREATE TEMP TABLE slot_import_fixture (
            b boolean,
            i2 int2,
            i4 int4,
            i8 int8,
            f4 real,
            f8 double precision,
            u uuid,
            t text,
            bytes bytea
        )",
    )
    .unwrap();
}

fn reset_slot_import_name_table() {
    Spi::run("DROP TABLE IF EXISTS pg_temp.slot_import_name_fixture").unwrap();
    Spi::run("CREATE TEMP TABLE slot_import_name_fixture (n name)").unwrap();
}

fn reset_slot_import_varchar_table() {
    Spi::run("DROP TABLE IF EXISTS pg_temp.slot_import_varchar_fixture").unwrap();
    Spi::run("CREATE TEMP TABLE slot_import_varchar_fixture (v varchar(4))").unwrap();
}

fn reset_slot_import_bpchar_table() {
    Spi::run("DROP TABLE IF EXISTS pg_temp.slot_import_bpchar_fixture").unwrap();
    Spi::run("CREATE TEMP TABLE slot_import_bpchar_fixture (v char(5))").unwrap();
}

unsafe fn relation_open_by_name(qualified: &str) -> pg_sys::Relation {
    let sql = format!("SELECT '{}'::regclass::oid", qualified);
    let relid: pg_sys::Oid = Spi::get_one(&sql).unwrap().unwrap();
    pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
}

fn init_import_harness() -> AnyResult<ImportHarness> {
    let config = PagePoolConfig::new(SLOT_IMPORT_PAGE_SIZE, SLOT_IMPORT_PAGE_COUNT)?;
    let region_layout = PagePool::layout(config)?;
    let region = OwnedRegion::new(region_layout);
    let pool = unsafe { PagePool::init_in_place(region.base, region_layout.size, config) }?;
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);
    let payload_capacity = {
        let mut writer = tx.begin(ARROW_LAYOUT_BATCH_KIND, 0)?;
        writer.payload_mut().len()
    };
    Ok(ImportHarness {
        _region: region,
        pool,
        tx,
        rx,
        payload_capacity,
    })
}

fn fixture_plan(max_rows: usize, payload_capacity: usize) -> AnyResult<LayoutPlan> {
    Ok(LayoutPlan::from_arrow_schema(
        fixture_schema().as_ref(),
        u32::try_from(max_rows)?,
        u32::try_from(payload_capacity)?,
    )?)
}

fn build_fixture_tuples(tupdesc: pg_sys::TupleDesc) -> AnyResult<Vec<OwnedMinimalTuple>> {
    fixture_rows()
        .iter()
        .map(|row| OwnedMinimalTuple::new(tupdesc, row))
        .collect()
}

fn encode_rows_into_page(
    writer: &mut transfer::PageWriter,
    tupdesc: pg_sys::TupleDesc,
    plan: &LayoutPlan,
    slot: &mut OwnedMinimalSlot,
    rows: &[&OwnedMinimalTuple],
) -> AnyResult<usize> {
    let payload_len = {
        let payload = writer.payload_mut();
        init_block(payload, plan)?;
        let mut encoder = unsafe { PageBatchEncoder::new(tupdesc, payload)? };
        for row in rows {
            slot.store(row);
            match encoder.append_slot(slot.as_mut_ptr())? {
                AppendStatus::Appended => {}
                AppendStatus::Full => bail!("page encoder reported Full before all rows fit"),
            }
        }
        encoder.finish()?.payload_len
    };
    Ok(payload_len)
}

fn send_encoded_page(
    tx: &PageTx,
    rx: &PageRx,
    tupdesc: pg_sys::TupleDesc,
    plan: &LayoutPlan,
    slot: &mut OwnedMinimalSlot,
    rows: &[&OwnedMinimalTuple],
) -> AnyResult<ReceivedPage> {
    let mut writer = tx.begin(ARROW_LAYOUT_BATCH_KIND, 0)?;
    let payload_len = encode_rows_into_page(&mut writer, tupdesc, plan, slot, rows)?;
    let outbound = writer.finish_with_payload_len(payload_len)?;
    let frame = encode_frame(outbound.frame())?;
    outbound.mark_sent();

    let mut decoder = FrameDecoder::new();
    let frame = decoder
        .push(&frame)
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("no frame decoded"))?;
    match rx.accept(frame)? {
        ReceiveEvent::Page(page) => Ok(page),
        ReceiveEvent::Closed => bail!("unexpected close frame"),
    }
}

fn send_manual_page(
    tx: &PageTx,
    rx: &PageRx,
    schema: &Schema,
    long_value: &str,
) -> AnyResult<ReceivedPage> {
    let plan = LayoutPlan::from_arrow_schema(schema, 1, 1024)?;
    let mut writer = tx.begin(ARROW_LAYOUT_BATCH_KIND, 0)?;
    {
        let payload = writer.payload_mut();
        init_block(payload, &plan)?;
        let mut block = BlockMut::open(payload)?;
        assert_eq!(
            block.write_view_bytes(0, 0, long_value.as_bytes())?,
            ViewWriteStatus::Written
        );
        block.commit_current_row()?;
        block.validate()?;
    }
    let payload_len = writer.payload_mut().len();
    let outbound = writer.finish_with_payload_len(payload_len)?;
    let frame = encode_frame(outbound.frame())?;
    outbound.mark_sent();

    let mut decoder = FrameDecoder::new();
    let frame = decoder
        .push(&frame)
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("no frame decoded"))?;
    match rx.accept(frame)? {
        ReceiveEvent::Page(page) => Ok(page),
        ReceiveEvent::Closed => bail!("unexpected close frame"),
    }
}

unsafe fn decode_slot_row(slot: *mut pg_sys::TupleTableSlot) -> DecodedRow {
    let slot_ref = &*slot;
    let natts = slot_ref.tts_nvalid as usize;
    let values = slice::from_raw_parts(slot_ref.tts_values, natts);
    let isnull = slice::from_raw_parts(slot_ref.tts_isnull, natts);

    let uuid_value = if isnull[6] {
        None
    } else {
        let ptr = values[6].cast_mut_ptr::<u8>();
        let mut out = [0u8; 16];
        out.copy_from_slice(slice::from_raw_parts(ptr, 16));
        Some(out)
    };
    let text_value = if isnull[7] {
        None
    } else {
        Some(text_to_rust_str_unchecked(values[7].cast_mut_ptr()).to_owned())
    };
    let binary_value = if isnull[8] {
        None
    } else {
        Some(varlena_to_byte_slice(values[8].cast_mut_ptr()).to_vec())
    };

    DecodedRow {
        bool_value: (!isnull[0]).then_some(values[0].value() != 0),
        int16_value: (!isnull[1]).then_some(values[1].value() as i16),
        int32_value: (!isnull[2]).then_some(values[2].value() as i32),
        int64_value: (!isnull[3]).then_some(values[3].value() as i64),
        float32_value: (!isnull[4]).then_some(f32::from_bits(values[4].value() as u32)),
        float64_value: (!isnull[5]).then_some(f64::from_bits(values[5].value() as u64)),
        uuid_value,
        text_value,
        binary_value,
    }
}

unsafe fn decode_first_text_value(slot: *mut pg_sys::TupleTableSlot) -> Option<String> {
    let slot_ref = &*slot;
    let values = slice::from_raw_parts(slot_ref.tts_values, slot_ref.tts_nvalid as usize);
    let isnull = slice::from_raw_parts(slot_ref.tts_isnull, slot_ref.tts_nvalid as usize);
    if isnull[0] {
        None
    } else {
        Some(text_to_rust_str_unchecked(values[0].cast_mut_ptr()).to_owned())
    }
}

fn expected_decoded_rows() -> Vec<DecodedRow> {
    fixture_rows()
        .into_iter()
        .map(|row| DecodedRow {
            bool_value: row.bool_value,
            int16_value: row.int16_value,
            int32_value: row.int32_value,
            int64_value: row.int64_value,
            float32_value: row.float32_value,
            float64_value: row.float64_value,
            uuid_value: row.uuid_value,
            text_value: row.text_value.map(str::to_owned),
            binary_value: row.binary_value.map(|bytes| bytes.to_vec()),
        })
        .collect()
}

pub fn slot_import_roundtrips_slot_encoder_page_into_virtual_slot() {
    reset_slot_import_table();
    let relation = OpenRelation::open(SLOT_IMPORT_TABLE);
    let tupdesc = relation.tuple_desc();
    let mut input_slot = OwnedMinimalSlot::new(tupdesc).expect("input slot");
    let rows = build_fixture_tuples(tupdesc).expect("fixture tuples");
    let harness = init_import_harness().expect("harness");
    let plan = fixture_plan(rows.len(), harness.payload_capacity).expect("layout plan");
    let page = send_encoded_page(
        &harness.tx,
        &harness.rx,
        tupdesc,
        &plan,
        &mut input_slot,
        &rows.iter().collect::<Vec<_>>(),
    )
    .expect("encoded page");

    let per_tuple_memory = PgMemoryContexts::new("slot_import_roundtrip");
    let mut projector =
        unsafe { ArrowSlotProjector::new(fixture_schema(), tupdesc, per_tuple_memory.value()) }
            .expect("projector");
    let mut cursor = projector.open_page(page).expect("cursor");
    let mut output_slot = OwnedVirtualSlot::new(tupdesc).expect("output slot");

    let mut decoded = Vec::new();
    while let Some(slot) = unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }.expect("row")
    {
        decoded.push(unsafe { decode_slot_row(slot) });
        assert_eq!(harness.pool.snapshot().leased_pages, 1);
    }

    assert_eq!(decoded, expected_decoded_rows());
    assert_eq!(harness.pool.snapshot().leased_pages, 0);
}

pub fn slot_import_releases_page_on_first_none_after_last_row() {
    reset_slot_import_table();
    let relation = OpenRelation::open(SLOT_IMPORT_TABLE);
    let tupdesc = relation.tuple_desc();
    let mut input_slot = OwnedMinimalSlot::new(tupdesc).expect("input slot");
    let rows = build_fixture_tuples(tupdesc).expect("fixture tuples");
    let harness = init_import_harness().expect("harness");
    let plan = fixture_plan(rows.len(), harness.payload_capacity).expect("layout plan");
    let page = send_encoded_page(
        &harness.tx,
        &harness.rx,
        tupdesc,
        &plan,
        &mut input_slot,
        &rows.iter().collect::<Vec<_>>(),
    )
    .expect("encoded page");

    let per_tuple_memory = PgMemoryContexts::new("slot_import_release");
    let mut projector =
        unsafe { ArrowSlotProjector::new(fixture_schema(), tupdesc, per_tuple_memory.value()) }
            .expect("projector");
    let mut cursor = projector.open_page(page).expect("cursor");
    let mut output_slot = OwnedVirtualSlot::new(tupdesc).expect("output slot");

    for _ in 0..rows.len() {
        let slot = unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }
            .expect("row")
            .expect("expected row");
        let _ = slot;
        assert_eq!(harness.pool.snapshot().leased_pages, 1);
    }

    assert!(unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }
        .expect("eof")
        .is_none());
    assert_eq!(harness.pool.snapshot().leased_pages, 0);
}

pub fn slot_import_uuid_is_page_backed_but_text_and_bytea_are_copied() {
    reset_slot_import_table();
    let relation = OpenRelation::open(SLOT_IMPORT_TABLE);
    let tupdesc = relation.tuple_desc();
    let mut input_slot = OwnedMinimalSlot::new(tupdesc).expect("input slot");
    let rows = build_fixture_tuples(tupdesc).expect("fixture tuples");
    let harness = init_import_harness().expect("harness");
    let plan = fixture_plan(rows.len(), harness.payload_capacity).expect("layout plan");
    let page = send_encoded_page(
        &harness.tx,
        &harness.rx,
        tupdesc,
        &plan,
        &mut input_slot,
        &rows.iter().collect::<Vec<_>>(),
    )
    .expect("encoded page");
    let page_start = page.payload().as_ptr() as usize;
    let page_end = page_start + page.payload().len();

    let per_tuple_memory = PgMemoryContexts::new("slot_import_pointers");
    let mut projector =
        unsafe { ArrowSlotProjector::new(fixture_schema(), tupdesc, per_tuple_memory.value()) }
            .expect("projector");
    let mut cursor = projector.open_page(page).expect("cursor");
    let mut output_slot = OwnedVirtualSlot::new(tupdesc).expect("output slot");

    let slot = unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }
        .expect("row")
        .expect("expected first row");
    let slot_ref = unsafe { &*slot };
    let values =
        unsafe { slice::from_raw_parts(slot_ref.tts_values, slot_ref.tts_nvalid as usize) };

    let uuid_ptr = values[6].cast_mut_ptr::<u8>() as usize;
    let text_ptr = values[7].cast_mut_ptr::<u8>() as usize;
    let bytea_ptr = values[8].cast_mut_ptr::<u8>() as usize;

    assert!((page_start..page_end).contains(&uuid_ptr));
    assert!(!(page_start..page_end).contains(&text_ptr));
    assert!(!(page_start..page_end).contains(&bytea_ptr));

    while unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }
        .expect("advance")
        .is_some()
    {}
    assert_eq!(harness.pool.snapshot().leased_pages, 0);
}

pub fn slot_import_rejects_schema_tupledesc_mismatch() {
    reset_slot_import_table();
    let relation = OpenRelation::open(SLOT_IMPORT_TABLE);
    let tupdesc = relation.tuple_desc();
    let schema = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Int32, true),
        Field::new("i2", DataType::Int16, true),
        Field::new("i4", DataType::Int32, true),
        Field::new("i8", DataType::Int64, true),
        Field::new("f4", DataType::Float32, true),
        Field::new("f8", DataType::Float64, true),
        Field::new("u", DataType::FixedSizeBinary(16), true),
        Field::new("t", DataType::Utf8View, true),
        Field::new("bytes", DataType::BinaryView, true),
    ]));

    let per_tuple_memory = PgMemoryContexts::new("slot_import_mismatch");
    let err = match unsafe { ArrowSlotProjector::new(schema, tupdesc, per_tuple_memory.value()) } {
        Ok(_) => panic!("expected schema mismatch"),
        Err(err) => err,
    };
    assert!(matches!(
        err,
        ConfigError::PgLayoutTypeMismatch { index: 0, .. }
    ));
}

pub fn slot_import_name_overflow_errors() {
    reset_slot_import_name_table();
    let relation = OpenRelation::open(SLOT_IMPORT_NAME_TABLE);
    let tupdesc = relation.tuple_desc();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Utf8View, true)]));
    let long_name = "this-name-is-definitely-longer-than-postgresql-name-allows-so-it-errors";

    let harness = init_import_harness().expect("harness");
    let page =
        send_manual_page(&harness.tx, &harness.rx, schema.as_ref(), long_name).expect("page");
    let per_tuple_memory = PgMemoryContexts::new("slot_import_name");
    let mut projector = unsafe { ArrowSlotProjector::new(schema, tupdesc, per_tuple_memory.value()) }
        .expect("projector");
    let mut cursor = projector.open_page(page).expect("cursor");
    let mut output_slot = OwnedVirtualSlot::new(tupdesc).expect("output slot");

    let err =
        unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }.expect_err("name overflow");
    assert!(matches!(err, ProjectError::NameTooLong { index: 0, .. }));
}

pub fn slot_import_varchar_typmod_rejects_overlength_values() {
    reset_slot_import_varchar_table();
    let relation = OpenRelation::open(SLOT_IMPORT_VARCHAR_TABLE);
    let tupdesc = relation.tuple_desc();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8View, true)]));
    let harness = init_import_harness().expect("harness");
    let page = send_manual_page(&harness.tx, &harness.rx, schema.as_ref(), "abcde").expect("page");

    let per_tuple_memory = PgMemoryContexts::new("slot_import_varchar_typmod");
    let mut projector = unsafe { ArrowSlotProjector::new(schema, tupdesc, per_tuple_memory.value()) }
        .expect("projector");
    let mut cursor = projector.open_page(page).expect("cursor");
    let mut output_slot = OwnedVirtualSlot::new(tupdesc).expect("output slot");

    let err =
        unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }.expect_err("varchar overflow");
    assert!(matches!(err, ProjectError::Postgres(_)));
    assert!(err
        .to_string()
        .contains("value too long for type character varying(4)"));
}

pub fn slot_import_bpchar_typmod_pads_values() {
    reset_slot_import_bpchar_table();
    let relation = OpenRelation::open(SLOT_IMPORT_BPCHAR_TABLE);
    let tupdesc = relation.tuple_desc();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8View, true)]));
    let harness = init_import_harness().expect("harness");
    let page = send_manual_page(&harness.tx, &harness.rx, schema.as_ref(), "abc").expect("page");

    let per_tuple_memory = PgMemoryContexts::new("slot_import_bpchar_typmod");
    let mut projector = unsafe { ArrowSlotProjector::new(schema, tupdesc, per_tuple_memory.value()) }
        .expect("projector");
    let mut cursor = projector.open_page(page).expect("cursor");
    let mut output_slot = OwnedVirtualSlot::new(tupdesc).expect("output slot");

    let slot = unsafe { cursor.next_into_slot(output_slot.as_mut_ptr()) }
        .expect("bpchar row")
        .expect("expected row");
    let value = unsafe { decode_first_text_value(slot) }.expect("non-null bpchar");
    assert_eq!(value, "abc  ");
}
