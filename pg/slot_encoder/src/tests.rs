use super::{set_test_database_encoding, AppendStatus, ConfigError, EncodeError, PageBatchEncoder};
use arrow_layout::{init_block, BlockRef, ColumnSpec, LayoutPlan, TypeTag};
use pgrx_pg_sys as pg_sys;
use std::alloc::{alloc_zeroed, dealloc, GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::{Mutex, MutexGuard};

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
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("tuple desc alloc");
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
    fn new(tupdesc: pg_sys::TupleDesc, values: Vec<pg_sys::Datum>, isnull: Vec<bool>) -> Self {
        let mut slot =
            Box::new(unsafe { MaybeUninit::<pg_sys::TupleTableSlot>::zeroed().assume_init() });
        slot.tts_tupleDescriptor = tupdesc;
        slot.tts_nvalid = values.len() as i16;
        let mut owned = Self {
            slot,
            values,
            isnull,
            _cells: Vec::new(),
        };
        owned.slot.tts_values = owned.values.as_mut_ptr();
        owned.slot.tts_isnull = owned.isnull.as_mut_ptr();
        owned
    }

    fn from_cells(tupdesc: pg_sys::TupleDesc, mut cells: Vec<MockCell>) -> Self {
        let mut values = Vec::with_capacity(cells.len());
        let mut isnull = Vec::with_capacity(cells.len());
        for cell in &mut cells {
            let (datum, is_null) = cell.datum();
            values.push(datum);
            isnull.push(is_null);
        }
        let mut slot =
            Box::new(unsafe { MaybeUninit::<pg_sys::TupleTableSlot>::zeroed().assume_init() });
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
    F64(f64),
    Utf8(Vec<u8>),
    Binary(Vec<u8>),
    Uuid(Box<[u8; 16]>),
    Name(Box<pg_sys::NameData>),
}

impl MockCell {
    fn datum(&mut self) -> (pg_sys::Datum, bool) {
        match self {
            Self::Null => (pg_sys::Datum::null(), true),
            Self::Bool(value) => (pg_sys::Datum::from(*value), false),
            Self::I32(value) => (pg_sys::Datum::from(*value), false),
            Self::F64(value) => (pg_sys::Datum::from(value.to_bits()), false),
            Self::Utf8(value) | Self::Binary(value) => {
                (pg_sys::Datum::from(value.as_mut_ptr()), false)
            }
            Self::Uuid(value) => (pg_sys::Datum::from(value.as_mut_ptr()), false),
            Self::Name(value) => (
                pg_sys::Datum::from(value.as_mut() as *mut pg_sys::NameData),
                false,
            ),
        }
    }
}

static ENCODING_LOCK: Mutex<()> = Mutex::new(());

struct EncodingGuard {
    previous: i32,
    _lock: MutexGuard<'static, ()>,
}

impl EncodingGuard {
    fn utf8() -> Self {
        let lock = ENCODING_LOCK.lock().expect("encoding lock");
        let previous = set_test_database_encoding(pg_sys::pg_enc::PG_UTF8 as i32);
        Self {
            previous,
            _lock: lock,
        }
    }

    fn set(encoding: i32) -> Self {
        let lock = ENCODING_LOCK.lock().expect("encoding lock");
        let previous = set_test_database_encoding(encoding);
        Self {
            previous,
            _lock: lock,
        }
    }
}

impl Drop for EncodingGuard {
    fn drop(&mut self) {
        let _ = set_test_database_encoding(self.previous);
    }
}

fn short_varlena(data: &[u8]) -> Vec<u8> {
    if data.len() + 1 < 0x80 {
        let total = data.len() + 1;
        let mut out = Vec::with_capacity(total);
        out.push(((total as u8) << 1) | 0x01);
        out.extend_from_slice(data);
        out
    } else {
        let total = data.len() + 4;
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&(((total as u32) << 2).to_le_bytes()));
        out.extend_from_slice(data);
        out
    }
}

fn name_data(value: &str) -> Box<pg_sys::NameData> {
    let mut name = Box::new(pg_sys::NameData::default());
    let bytes = value.as_bytes();
    for (dst, src) in name.data.iter_mut().zip(bytes.iter().copied()) {
        *dst = src as i8;
    }
    name
}

fn init_payload(specs: &[ColumnSpec], max_rows: u32, block_size: usize) -> Vec<u8> {
    let plan = LayoutPlan::new(specs, max_rows, block_size as u32).expect("layout plan");
    let mut payload = vec![0u8; block_size];
    init_block(&mut payload, &plan).expect("init block");
    payload
}

fn bool_at(block: &BlockRef<'_>, col: usize, row: u32) -> bool {
    block.bool_value(col, row).expect("bool value")
}

fn i32_at(block: &BlockRef<'_>, col: usize, row: u32) -> i32 {
    let bytes = block.fixed_value(col, row).expect("fixed value");
    i32::from_ne_bytes(bytes.try_into().expect("i32 bytes"))
}

fn f64_at(block: &BlockRef<'_>, col: usize, row: u32) -> f64 {
    let bytes = block.fixed_value(col, row).expect("fixed value");
    f64::from_bits(u64::from_ne_bytes(bytes.try_into().expect("f64 bytes")))
}

fn uuid_at(block: &BlockRef<'_>, col: usize, row: u32) -> [u8; 16] {
    block
        .fixed_value(col, row)
        .expect("fixed value")
        .try_into()
        .expect("uuid bytes")
}

fn view_bytes(block: &BlockRef<'_>, col: usize, row: u32) -> Option<Vec<u8>> {
    if !block.validity(col, row).expect("validity") {
        return None;
    }
    let view = block.view(col, row).expect("view");
    if let Some(bytes) = view.inline_bytes().expect("inline") {
        return Some(bytes.to_vec());
    }
    let offset = view.offset().expect("offset").expect("outline offset") as usize;
    let len = view.len().expect("len");
    let pool = block.shared_pool().expect("pool");
    Some(pool[offset..offset + len].to_vec())
}

#[test]
fn encodes_rows_directly_into_layout_block() {
    let _encoding = EncodingGuard::utf8();
    let specs = [
        ColumnSpec::new(TypeTag::Boolean, true),
        ColumnSpec::new(TypeTag::Int32, true),
        ColumnSpec::new(TypeTag::Float64, true),
        ColumnSpec::new(TypeTag::Utf8View, true),
        ColumnSpec::new(TypeTag::BinaryView, true),
        ColumnSpec::new(TypeTag::Uuid, true),
        ColumnSpec::new(TypeTag::Utf8View, true),
    ];
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
            oid: pg_sys::FLOAT8OID,
            attlen: 8,
            attbyval: true,
            attalign: b'd',
        },
        TestAttr {
            oid: pg_sys::TEXTOID,
            attlen: -1,
            attbyval: false,
            attalign: b'i',
        },
        TestAttr {
            oid: pg_sys::BYTEAOID,
            attlen: -1,
            attbyval: false,
            attalign: b'i',
        },
        TestAttr {
            oid: pg_sys::UUIDOID,
            attlen: 16,
            attbyval: false,
            attalign: b'c',
        },
        TestAttr {
            oid: pg_sys::NAMEOID,
            attlen: 64,
            attbyval: false,
            attalign: b'c',
        },
    ];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut payload = init_payload(&specs, 4, 1024);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");

    let mut rows = vec![
        OwnedSlot::from_cells(
            tuple_desc.ptr,
            vec![
                MockCell::Bool(true),
                MockCell::I32(11),
                MockCell::F64(3.25),
                MockCell::Utf8(short_varlena(b"alpha")),
                MockCell::Binary(short_varlena(b"\x00\x01")),
                MockCell::Uuid(Box::new([1; 16])),
                MockCell::Name(name_data("first")),
            ],
        ),
        OwnedSlot::from_cells(
            tuple_desc.ptr,
            vec![
                MockCell::Null,
                MockCell::Null,
                MockCell::Null,
                MockCell::Null,
                MockCell::Null,
                MockCell::Null,
                MockCell::Null,
            ],
        ),
        OwnedSlot::from_cells(
            tuple_desc.ptr,
            vec![
                MockCell::Bool(false),
                MockCell::I32(22),
                MockCell::F64(-6.5),
                MockCell::Utf8(short_varlena(b"abcdefghijklmnop")),
                MockCell::Binary(short_varlena(
                    b"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c",
                )),
                MockCell::Uuid(Box::new([2; 16])),
                MockCell::Name(name_data("second")),
            ],
        ),
    ];

    for slot in &mut rows {
        assert_eq!(
            encoder.append_slot(slot.as_mut_ptr()).expect("append"),
            AppendStatus::Appended
        );
    }
    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 3);
    assert_eq!(encoded.payload_len, payload.len());

    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 3);
    assert!(bool_at(&block, 0, 0));
    assert!(!block.validity(0, 1).expect("validity"));
    assert!(!bool_at(&block, 0, 2));
    assert_eq!(i32_at(&block, 1, 0), 11);
    assert_eq!(i32_at(&block, 1, 2), 22);
    assert_eq!(f64_at(&block, 2, 0), 3.25);
    assert_eq!(f64_at(&block, 2, 2), -6.5);
    assert_eq!(view_bytes(&block, 3, 0).as_deref(), Some(&b"alpha"[..]));
    assert_eq!(
        view_bytes(&block, 3, 2).as_deref(),
        Some(&b"abcdefghijklmnop"[..])
    );
    assert_eq!(view_bytes(&block, 4, 0).as_deref(), Some(&b"\x00\x01"[..]));
    assert_eq!(
        view_bytes(&block, 4, 2).as_deref(),
        Some(&b"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c"[..])
    );
    assert_eq!(uuid_at(&block, 5, 0), [1; 16]);
    assert_eq!(uuid_at(&block, 5, 2), [2; 16]);
    assert_eq!(view_bytes(&block, 6, 0).as_deref(), Some(&b"first"[..]));
    assert_eq!(view_bytes(&block, 6, 2).as_deref(), Some(&b"second"[..]));
    assert_eq!(block.null_count(0).expect("null count"), 1);
}

#[test]
fn append_slot_reports_full_without_committing_the_overflowing_row() {
    let _encoding = EncodingGuard::utf8();
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let attrs = [TestAttr {
        oid: pg_sys::TEXTOID,
        attlen: -1,
        attbyval: false,
        attalign: b'i',
    }];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut payload = init_payload(&specs, 4, 160);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");

    let mut small =
        OwnedSlot::from_cells(tuple_desc.ptr, vec![MockCell::Utf8(short_varlena(b"ok"))]);
    let mut huge = OwnedSlot::from_cells(
        tuple_desc.ptr,
        vec![MockCell::Utf8(short_varlena(&[b'x'; 96]))],
    );

    assert_eq!(
        encoder.append_slot(small.as_mut_ptr()).expect("small"),
        AppendStatus::Appended
    );
    let tail_after_small = encoder.tail_cursor();
    assert_eq!(
        encoder.append_slot(huge.as_mut_ptr()).expect("huge"),
        AppendStatus::Full
    );
    let encoded = encoder.finish().expect("finish");
    assert_eq!(encoded.row_count, 1);

    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.tail_cursor(), tail_after_small);
    assert_eq!(view_bytes(&block, 0, 0).as_deref(), Some(&b"ok"[..]));
}

#[test]
fn append_slot_reads_fixed_width_and_name_values() {
    let _encoding = EncodingGuard::utf8();
    let specs = [
        ColumnSpec::new(TypeTag::Boolean, true),
        ColumnSpec::new(TypeTag::Int32, true),
        ColumnSpec::new(TypeTag::Utf8View, true),
    ];
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
            oid: pg_sys::NAMEOID,
            attlen: 64,
            attbyval: false,
            attalign: b'c',
        },
    ];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut slot_name = name_data("slot_name");
    let values = vec![
        pg_sys::Datum::from(true),
        pg_sys::Datum::from(42i32),
        pg_sys::Datum::from(slot_name.as_mut() as *mut pg_sys::NameData),
    ];
    let isnull = vec![false, false, false];
    let mut slot = OwnedSlot::new(tuple_desc.ptr, values, isnull);

    let mut payload = init_payload(&specs, 2, 512);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");
    assert_eq!(
        encoder.append_slot(slot.as_mut_ptr()).expect("append slot"),
        AppendStatus::Appended
    );
    encoder.finish().expect("finish");

    let block = BlockRef::open(&payload).expect("block");
    assert!(bool_at(&block, 0, 0));
    assert_eq!(i32_at(&block, 1, 0), 42);
    assert_eq!(view_bytes(&block, 2, 0).as_deref(), Some(&b"slot_name"[..]));
}

#[test]
fn append_slot_rejects_mismatched_tuple_desc() {
    let specs = [ColumnSpec::new(TypeTag::Boolean, true)];
    let encoder_attrs = [TestAttr {
        oid: pg_sys::BOOLOID,
        attlen: 1,
        attbyval: true,
        attalign: b'c',
    }];
    let slot_attrs = [TestAttr {
        oid: pg_sys::INT4OID,
        attlen: 4,
        attbyval: true,
        attalign: b'i',
    }];
    let encoder_desc = OwnedTupleDesc::new(&encoder_attrs);
    let slot_desc = OwnedTupleDesc::new(&slot_attrs);
    let values = vec![pg_sys::Datum::from(true)];
    let isnull = vec![false];
    let mut slot = OwnedSlot::new(slot_desc.ptr, values, isnull);

    let mut payload = init_payload(&specs, 2, 256);
    let mut encoder =
        unsafe { PageBatchEncoder::new(encoder_desc.ptr, &mut payload) }.expect("encoder");
    let error = encoder
        .append_slot(slot.as_mut_ptr())
        .expect_err("mismatch");
    assert!(matches!(error, EncodeError::SlotTupleDescMismatch));
}

#[test]
fn append_slot_accepts_equivalent_tuple_desc_pointer() {
    let specs = [ColumnSpec::new(TypeTag::Boolean, true)];
    let attrs = [TestAttr {
        oid: pg_sys::BOOLOID,
        attlen: 1,
        attbyval: true,
        attalign: b'c',
    }];
    let encoder_desc = OwnedTupleDesc::new(&attrs);
    let slot_desc = OwnedTupleDesc::new(&attrs);
    let values = vec![pg_sys::Datum::from(true)];
    let isnull = vec![false];
    let mut slot = OwnedSlot::new(slot_desc.ptr, values, isnull);

    let mut payload = init_payload(&specs, 2, 256);
    let mut encoder =
        unsafe { PageBatchEncoder::new(encoder_desc.ptr, &mut payload) }.expect("encoder");
    assert_eq!(
        encoder
            .append_slot(slot.as_mut_ptr())
            .expect("equivalent desc"),
        AppendStatus::Appended
    );
}

#[test]
fn encodes_empty_short_varlena_values() {
    let _encoding = EncodingGuard::utf8();
    let specs = [
        ColumnSpec::new(TypeTag::Utf8View, true),
        ColumnSpec::new(TypeTag::BinaryView, true),
    ];
    let attrs = [
        TestAttr {
            oid: pg_sys::TEXTOID,
            attlen: -1,
            attbyval: false,
            attalign: b'i',
        },
        TestAttr {
            oid: pg_sys::BYTEAOID,
            attlen: -1,
            attbyval: false,
            attalign: b'i',
        },
    ];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut payload = init_payload(&specs, 2, 256);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");

    let mut slot = OwnedSlot::from_cells(
        tuple_desc.ptr,
        vec![
            MockCell::Utf8(short_varlena(b"")),
            MockCell::Binary(short_varlena(b"")),
        ],
    );
    assert_eq!(
        encoder.append_slot(slot.as_mut_ptr()).expect("append"),
        AppendStatus::Appended
    );
    encoder.finish().expect("finish");

    let block = BlockRef::open(&payload).expect("block");
    assert_eq!(view_bytes(&block, 0, 0).as_deref(), Some(&b""[..]));
    assert_eq!(view_bytes(&block, 1, 0).as_deref(), Some(&b""[..]));
}

#[test]
fn new_rejects_non_utf8_server_encoding_for_text_views() {
    let _encoding = EncodingGuard::set(1);
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let attrs = [TestAttr {
        oid: pg_sys::TEXTOID,
        attlen: -1,
        attbyval: false,
        attalign: b'i',
    }];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut payload = init_payload(&specs, 1, 256);

    let error = unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect_err("reject");
    assert!(matches!(
        error,
        ConfigError::NonUtf8ServerEncoding { encoding: 1 }
    ));
}

#[test]
fn new_is_allocation_free_after_block_initialization() {
    let _encoding = EncodingGuard::utf8();
    let specs = [
        ColumnSpec::new(TypeTag::Boolean, true),
        ColumnSpec::new(TypeTag::Int32, true),
        ColumnSpec::new(TypeTag::Utf8View, true),
    ];
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
            oid: pg_sys::TEXTOID,
            attlen: -1,
            attbyval: false,
            attalign: b'i',
        },
    ];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut payload = init_payload(&specs, 4, 1024);

    let (allocations, encoder) = count_thread_allocations(|| {
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder")
    });
    assert_eq!(allocations, 0);
    let _ = encoder;
}

#[test]
fn append_slot_mixed_is_allocation_free_after_encoder_construction() {
    let _encoding = EncodingGuard::utf8();
    let specs = [
        ColumnSpec::new(TypeTag::Boolean, true),
        ColumnSpec::new(TypeTag::Utf8View, true),
        ColumnSpec::new(TypeTag::BinaryView, true),
        ColumnSpec::new(TypeTag::Uuid, true),
    ];
    let attrs = [
        TestAttr {
            oid: pg_sys::BOOLOID,
            attlen: 1,
            attbyval: true,
            attalign: b'c',
        },
        TestAttr {
            oid: pg_sys::TEXTOID,
            attlen: -1,
            attbyval: false,
            attalign: b'i',
        },
        TestAttr {
            oid: pg_sys::BYTEAOID,
            attlen: -1,
            attbyval: false,
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
    let mut payload = init_payload(&specs, 4, 2048);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");
    let mut slot = OwnedSlot::from_cells(
        tuple_desc.ptr,
        vec![
            MockCell::Bool(true),
            MockCell::Utf8(short_varlena(b"abcdefghijklmno")),
            MockCell::Binary(short_varlena(
                b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c",
            )),
            MockCell::Uuid(Box::new([9; 16])),
        ],
    );

    let (allocations, status) =
        count_thread_allocations(|| encoder.append_slot(slot.as_mut_ptr()).expect("append slot"));
    assert_eq!(allocations, 0);
    assert_eq!(status, AppendStatus::Appended);
}

#[test]
fn append_slot_fixed_width_is_allocation_free() {
    let specs = [
        ColumnSpec::new(TypeTag::Boolean, true),
        ColumnSpec::new(TypeTag::Int32, true),
    ];
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
    ];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let values = vec![pg_sys::Datum::from(true), pg_sys::Datum::from(42i32)];
    let isnull = vec![false, false];
    let mut slot = OwnedSlot::new(tuple_desc.ptr, values, isnull);
    let mut payload = init_payload(&specs, 2, 512);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");

    let (allocations, status) =
        count_thread_allocations(|| encoder.append_slot(slot.as_mut_ptr()).expect("append slot"));
    assert_eq!(allocations, 0);
    assert_eq!(status, AppendStatus::Appended);
}

#[test]
fn finish_is_allocation_free() {
    let _encoding = EncodingGuard::utf8();
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let attrs = [TestAttr {
        oid: pg_sys::TEXTOID,
        attlen: -1,
        attbyval: false,
        attalign: b'i',
    }];
    let tuple_desc = OwnedTupleDesc::new(&attrs);
    let mut payload = init_payload(&specs, 2, 512);
    let mut encoder =
        unsafe { PageBatchEncoder::new(tuple_desc.ptr, &mut payload) }.expect("encoder");
    let mut slot = OwnedSlot::from_cells(
        tuple_desc.ptr,
        vec![MockCell::Utf8(short_varlena(b"alpha"))],
    );
    assert_eq!(
        encoder.append_slot(slot.as_mut_ptr()).expect("append"),
        AppendStatus::Appended
    );

    let (allocations, encoded) = count_thread_allocations(|| encoder.finish().expect("finish"));
    assert_eq!(allocations, 0);
    assert_eq!(encoded.row_count, 1);
    assert_eq!(encoded.payload_len, payload.len());
}
