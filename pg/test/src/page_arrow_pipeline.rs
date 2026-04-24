use anyhow::Result as AnyResult;
use anyhow::{anyhow, bail};
use arrow_array::{
    ArrayRef, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array,
    RecordBatch, StringViewArray,
};
use arrow_layout::{init_block, LayoutPlan};
use arrow_schema::{DataType, Field, Schema};
use import::{ArrowPageDecoder, ARROW_LAYOUT_BATCH_KIND};
use pgrx::prelude::*;
use pgrx::varlena::{rust_byte_slice_to_bytea, rust_str_to_text_p};
use pool::{PagePool, PagePoolConfig, RegionLayout};
use slot_encoder::{AppendStatus, PageBatchEncoder};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::{ptr::NonNull, sync::Arc};
use transfer::{encode_frame, FrameDecoder, PageRx, PageTx, ReceiveEvent, ReceivedPage};

const PIPELINE_RELATION: &str = "pg_temp.page_arrow_pipeline_slot";
const PIPELINE_PAGE_SIZE: usize = 8192;
const PIPELINE_PAGE_COUNT: u32 = 4;

unsafe fn relation_open_by_name(qualified: &str) -> pg_sys::Relation {
    let sql = format!("SELECT '{}'::regclass::oid", qualified);
    let relid: pg_sys::Oid = Spi::get_one(&sql).unwrap().unwrap();
    pg_sys::relation_open(relid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
}

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn new(region: RegionLayout) -> Self {
        let layout = Layout::from_size_align(region.size, region.align).expect("region layout");
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("region allocation failed");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
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

struct OwnedMinimalTuple {
    ptr: pg_sys::MinimalTuple,
}

impl OwnedMinimalTuple {
    fn new(tupdesc: pg_sys::TupleDesc, row: &PipelineRow) -> AnyResult<Self> {
        let mut values = [pg_sys::Datum::null(); 6];
        let mut nulls = [true; 6];
        let mut uuid_storage = row.uuid_value;
        let text = row.text_value.map(rust_str_to_text_p);
        let bytea = row.binary_value.map(rust_byte_slice_to_bytea);

        if let Some(value) = row.bool_value {
            values[0] = pg_sys::Datum::from(value);
            nulls[0] = false;
        }
        if let Some(value) = row.int_value {
            values[1] = pg_sys::Datum::from(value);
            nulls[1] = false;
        }
        if let Some(value) = row.float_value {
            values[2] = pg_sys::Datum::from(value.to_bits());
            nulls[2] = false;
        }
        if let Some(uuid) = uuid_storage.as_mut() {
            values[3] = pg_sys::Datum::from(uuid.as_mut_ptr());
            nulls[3] = false;
        }
        if let Some(text) = text.as_ref() {
            values[4] = pg_sys::Datum::from(text.as_ptr());
            nulls[4] = false;
        }
        if let Some(bytea) = bytea.as_ref() {
            values[5] = pg_sys::Datum::from(bytea.as_ptr());
            nulls[5] = false;
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

#[derive(Clone, Copy)]
struct PipelineRow {
    bool_value: Option<bool>,
    int_value: Option<i32>,
    float_value: Option<f64>,
    uuid_value: Option<[u8; 16]>,
    text_value: Option<&'static str>,
    binary_value: Option<&'static [u8]>,
}

struct PipelineHarness {
    _region: OwnedRegion,
    pool: PagePool,
    tx: PageTx,
    rx: PageRx,
    payload_capacity: usize,
}

fn pipeline_rows() -> [PipelineRow; 4] {
    [
        PipelineRow {
            bool_value: Some(true),
            int_value: Some(11),
            float_value: Some(3.25),
            uuid_value: Some([1; 16]),
            text_value: Some("short"),
            binary_value: Some(b"\x00\x01"),
        },
        PipelineRow {
            bool_value: None,
            int_value: None,
            float_value: None,
            uuid_value: None,
            text_value: None,
            binary_value: None,
        },
        PipelineRow {
            bool_value: Some(false),
            int_value: Some(-42),
            float_value: Some(-6.5),
            uuid_value: Some([2; 16]),
            text_value: Some("this string is definitely longer than twelve bytes"),
            binary_value: Some(b"this binary payload is also longer than twelve bytes"),
        },
        PipelineRow {
            bool_value: Some(true),
            int_value: Some(0),
            float_value: Some(0.0),
            uuid_value: Some([0; 16]),
            text_value: Some(""),
            binary_value: Some(b""),
        },
    ]
}

fn pipeline_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i", DataType::Int32, true),
        Field::new("d", DataType::Float64, true),
        Field::new("u", DataType::FixedSizeBinary(16), true),
        Field::new("t", DataType::Utf8View, true),
        Field::new("bytes", DataType::BinaryView, true),
    ]))
}

fn expected_pipeline_batch() -> RecordBatch {
    let schema = pipeline_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])),
        Arc::new(Int32Array::from(vec![Some(11), None, Some(-42), Some(0)])),
        Arc::new(Float64Array::from(vec![
            Some(3.25),
            None,
            Some(-6.5),
            Some(0.0),
        ])),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some([1; 16]), None, Some([2; 16]), Some([0; 16])].into_iter(),
                16,
            )
            .expect("uuid array"),
        ),
        Arc::new(StringViewArray::from(vec![
            Some("short"),
            None,
            Some("this string is definitely longer than twelve bytes"),
            Some(""),
        ])),
        Arc::new(BinaryViewArray::from(vec![
            Some(&b"\x00\x01"[..]),
            None,
            Some(&b"this binary payload is also longer than twelve bytes"[..]),
            Some(&b""[..]),
        ])),
    ];
    RecordBatch::try_new(schema, columns).expect("expected batch")
}

fn reset_pipeline_table() {
    Spi::run("DROP TABLE IF EXISTS page_arrow_pipeline_slot").unwrap();
    Spi::run(
        "CREATE TEMP TABLE page_arrow_pipeline_slot (
            b boolean,
            i int4,
            d double precision,
            u uuid,
            t text,
            bytes bytea
        )",
    )
    .unwrap();
}

fn init_pipeline_harness() -> AnyResult<PipelineHarness> {
    let config = PagePoolConfig::new(PIPELINE_PAGE_SIZE, PIPELINE_PAGE_COUNT)?;
    let region_layout = PagePool::layout(config)?;
    let region = OwnedRegion::new(region_layout);
    let pool = unsafe { PagePool::init_in_place(region.base, region_layout.size, config) }?;
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);
    let payload_capacity = {
        let mut writer = tx.begin(ARROW_LAYOUT_BATCH_KIND, 0)?;
        writer.payload_mut().len()
    };
    Ok(PipelineHarness {
        _region: region,
        pool,
        tx,
        rx,
        payload_capacity,
    })
}

fn pipeline_plan(max_rows: usize, payload_capacity: usize) -> AnyResult<LayoutPlan> {
    Ok(LayoutPlan::from_arrow_schema(
        pipeline_schema().as_ref(),
        u32::try_from(max_rows)?,
        u32::try_from(payload_capacity)?,
    )?)
}

fn build_fixture_tuples(tupdesc: pg_sys::TupleDesc) -> AnyResult<Vec<OwnedMinimalTuple>> {
    pipeline_rows()
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

pub(crate) fn page_arrow_pipeline_roundtrip_inside_postgres() {
    reset_pipeline_table();
    let relation = OpenRelation::open(PIPELINE_RELATION);
    let tupdesc = relation.tuple_desc();
    let mut slot = OwnedMinimalSlot::new(tupdesc).expect("slot");
    let rows = build_fixture_tuples(tupdesc).expect("fixture tuples");
    let harness = init_pipeline_harness().expect("pipeline harness");
    let plan = pipeline_plan(rows.len(), harness.payload_capacity).expect("plan");
    let decoder = ArrowPageDecoder::new(pipeline_schema()).expect("decoder");
    let expected = expected_pipeline_batch();

    let first_page = send_encoded_page(
        &harness.tx,
        &harness.rx,
        tupdesc,
        &plan,
        &mut slot,
        &rows.iter().collect::<Vec<_>>(),
    )
    .expect("first page");
    assert_eq!(harness.pool.snapshot().leased_pages, 1);

    let imported_first = decoder.import(first_page).expect("import first");
    assert_eq!(imported_first, expected);
    assert_eq!(harness.pool.snapshot().leased_pages, 1);

    let first_column = imported_first.column(0).clone();
    let second_page = send_encoded_page(
        &harness.tx,
        &harness.rx,
        tupdesc,
        &plan,
        &mut slot,
        &rows.iter().collect::<Vec<_>>(),
    )
    .expect("second page");
    let imported_second = decoder.import(second_page).expect("import second");
    assert_eq!(imported_second, expected);
    assert_eq!(harness.pool.snapshot().leased_pages, 2);

    drop(imported_first);
    assert_eq!(harness.pool.snapshot().leased_pages, 2);
    drop(first_column);
    assert_eq!(harness.pool.snapshot().leased_pages, 1);
    drop(imported_second);
    assert_eq!(harness.pool.snapshot().leased_pages, 0);
}
