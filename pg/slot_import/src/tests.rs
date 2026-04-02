use super::{set_test_database_encoding, ArrowSlotProjector, ConfigError};
use arrow_schema::{DataType, Field, Schema};
use pgrx_pg_sys as pg_sys;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Copy)]
struct TestAttr {
    oid: pg_sys::Oid,
    attlen: i16,
    attbyval: bool,
    attalign: u8,
    atttypmod: i32,
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
                attr.atttypmod = spec.atttypmod;
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

static ENCODING_LOCK: Mutex<()> = Mutex::new(());

struct EncodingGuard {
    previous: i32,
    _lock: MutexGuard<'static, ()>,
}

impl EncodingGuard {
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

fn dummy_memory_context() -> pg_sys::MemoryContext {
    NonNull::<u8>::dangling().as_ptr().cast()
}

#[test]
fn rejects_text_like_projector_on_non_utf8_database_encoding() {
    let _encoding = EncodingGuard::set(pg_sys::pg_enc::PG_SQL_ASCII as i32);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8View, true)]));
    let tuple_desc = OwnedTupleDesc::new(&[TestAttr {
        oid: pg_sys::TEXTOID,
        attlen: -1,
        attbyval: false,
        attalign: b'i',
        atttypmod: -1,
    }]);

    let result = unsafe { ArrowSlotProjector::new(schema, tuple_desc.ptr, dummy_memory_context()) };
    assert!(matches!(
        result,
        Err(ConfigError::NonUtf8ServerEncoding { encoding })
        if encoding == pg_sys::pg_enc::PG_SQL_ASCII as i32
    ));
}

#[test]
fn allows_non_text_projector_on_non_utf8_database_encoding() {
    let _encoding = EncodingGuard::set(pg_sys::pg_enc::PG_LATIN1 as i32);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let tuple_desc = OwnedTupleDesc::new(&[TestAttr {
        oid: pg_sys::INT4OID,
        attlen: 4,
        attbyval: true,
        attalign: b'i',
        atttypmod: -1,
    }]);

    unsafe { ArrowSlotProjector::new(schema, tuple_desc.ptr, dummy_memory_context()) }
        .expect("non-text projector should not depend on database encoding");
}
