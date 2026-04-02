use crate::{ConfigError, ProjectError};
use arrow_array::{
    Array, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringViewArray,
};
use arrow_layout::TypeTag;
use arrow_schema::SchemaRef;
use import::ArrowPageDecoder;
use pgrx::fcinfo::direct_function_call_as_datum;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::varlena::{rust_byte_slice_to_bytea, rust_str_to_text_p};
use pgrx::{PgMemoryContexts, PgTryBuilder};
use std::convert::TryFrom;
use std::panic::AssertUnwindSafe;
use std::{ptr, slice};
use transfer::ReceivedPage;

#[cfg(test)]
use std::sync::atomic::{AtomicI32, Ordering};

/// Reusable projector from page-backed Arrow batches into PostgreSQL virtual slots.
///
/// The caller owns both the `TupleDesc` and the per-tuple `MemoryContext`. Both
/// must remain valid for the lifetime of this projector and any cursors opened
/// from it. Only one cursor may be open from a projector at a time.
pub struct ArrowSlotProjector {
    tuple_desc: pg_sys::TupleDesc,
    per_tuple_memory: pg_sys::MemoryContext,
    decoder: ArrowPageDecoder,
    columns: Vec<ColumnProjector>,
}

/// One page-backed cursor that projects rows into a caller-owned virtual slot.
///
/// The slot contents returned by [`Self::next_into_slot`] remain valid until
/// the next call on the same cursor or until the cursor is dropped.
pub struct PageSlotCursor<'a> {
    projector: &'a mut ArrowSlotProjector,
    page: Option<ImportedPage>,
    next_row: usize,
}

#[derive(Clone, Copy, Debug)]
enum ColumnProjector {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Uuid,
    TextLike(TextLikeProjector),
    Bytea,
}

#[derive(Clone, Copy, Debug)]
enum TextLikeKind {
    Text,
    Varchar,
    Bpchar,
    Name,
}

#[derive(Clone, Copy, Debug)]
struct TextLikeProjector {
    kind: TextLikeKind,
    atttypmod: i32,
}

struct ImportedPage {
    row_count: usize,
    columns: Vec<PageColumnView>,
}

enum PageColumnView {
    Boolean(BooleanArray),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Uuid(FixedSizeBinaryArray),
    Utf8View(StringViewArray),
    BinaryView(BinaryViewArray),
}

impl ArrowSlotProjector {
    /// Construct a projector for page-backed Arrow batches with the given
    /// target PostgreSQL tuple descriptor and per-tuple memory context.
    ///
    /// The supplied `schema` must already match the supported `arrow_layout`
    /// surface expected from `slot_encoder`. Text-like `Utf8View` mappings are
    /// accepted only when the current PostgreSQL database encoding is `UTF8`.
    ///
    /// # Safety
    ///
    /// `tuple_desc` and `per_tuple_memory` must both be valid PostgreSQL
    /// pointers for the entire lifetime of this projector and any cursor opened
    /// from it. `per_tuple_memory` must be reserved exclusively for this
    /// projector and its cursors. The caller must also ensure that rows
    /// projected into a target slot are not used after the next call on the
    /// same cursor.
    pub unsafe fn new(
        schema: SchemaRef,
        tuple_desc: pg_sys::TupleDesc,
        per_tuple_memory: pg_sys::MemoryContext,
    ) -> Result<Self, ConfigError> {
        if tuple_desc.is_null() {
            return Err(ConfigError::NullTupleDesc);
        }
        if per_tuple_memory.is_null() {
            return Err(ConfigError::NullPerTupleMemoryContext);
        }

        let decoder = ArrowPageDecoder::new(schema.clone())?;
        let tuple_natts = usize::try_from(unsafe { (*tuple_desc).natts }).unwrap_or(0);
        let schema_natts = schema.fields().len();
        if tuple_natts != schema_natts {
            return Err(ConfigError::SchemaColumnCountMismatch {
                expected: tuple_natts,
                actual: schema_natts,
            });
        }

        let mut columns = Vec::with_capacity(schema_natts);
        let mut checked_utf8_encoding = false;
        for (index, field) in schema.fields().iter().enumerate() {
            let attr = tuple_desc_attr(tuple_desc, index);
            if attr.attisdropped {
                return Err(ConfigError::DroppedAttribute { index });
            }

            let type_tag = match TypeTag::from_arrow_data_type(index, field.data_type()) {
                Ok(type_tag) => type_tag,
                Err(_) => unreachable!("ArrowPageDecoder already validated the schema"),
            };

            let projector = projector_for_attr(index, attr.atttypid, attr.atttypmod, type_tag)?;
            if !checked_utf8_encoding && matches!(projector, ColumnProjector::TextLike(_)) {
                let encoding = database_encoding();
                if encoding != pg_sys::pg_enc::PG_UTF8 as i32 {
                    return Err(ConfigError::NonUtf8ServerEncoding { encoding });
                }
                checked_utf8_encoding = true;
            }
            columns.push(projector);
        }

        Ok(Self {
            tuple_desc,
            per_tuple_memory,
            decoder,
            columns,
        })
    }

    /// Open one received page for row-by-row projection.
    pub fn open_page(&mut self, page: ReceivedPage) -> Result<PageSlotCursor<'_>, ProjectError> {
        let batch = self.decoder.import(page)?;
        let row_count = batch.num_rows();
        let mut columns = Vec::with_capacity(self.columns.len());

        for (index, projector) in self.columns.iter().enumerate() {
            let array = batch.column(index);
            let view = match projector {
                ColumnProjector::Boolean => PageColumnView::Boolean(
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .cloned()
                        .ok_or(ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "BooleanArray",
                        })?,
                ),
                ColumnProjector::Int16 => PageColumnView::Int16(
                    array.as_any().downcast_ref::<Int16Array>().cloned().ok_or(
                        ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "Int16Array",
                        },
                    )?,
                ),
                ColumnProjector::Int32 => PageColumnView::Int32(
                    array.as_any().downcast_ref::<Int32Array>().cloned().ok_or(
                        ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "Int32Array",
                        },
                    )?,
                ),
                ColumnProjector::Int64 => PageColumnView::Int64(
                    array.as_any().downcast_ref::<Int64Array>().cloned().ok_or(
                        ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "Int64Array",
                        },
                    )?,
                ),
                ColumnProjector::Float32 => PageColumnView::Float32(
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .cloned()
                        .ok_or(ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "Float32Array",
                        })?,
                ),
                ColumnProjector::Float64 => PageColumnView::Float64(
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .cloned()
                        .ok_or(ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "Float64Array",
                        })?,
                ),
                ColumnProjector::Uuid => PageColumnView::Uuid(
                    array
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .cloned()
                        .ok_or(ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "FixedSizeBinaryArray(16)",
                        })?,
                ),
                ColumnProjector::TextLike(_) => PageColumnView::Utf8View(
                    array
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .cloned()
                        .ok_or(ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "StringViewArray",
                        })?,
                ),
                ColumnProjector::Bytea => PageColumnView::BinaryView(
                    array
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .cloned()
                        .ok_or(ProjectError::ImportedArrayTypeMismatch {
                            index,
                            expected: "BinaryViewArray",
                        })?,
                ),
            };
            columns.push(view);
        }

        Ok(PageSlotCursor {
            projector: self,
            page: Some(ImportedPage { row_count, columns }),
            next_row: 0,
        })
    }

    fn project_row(
        &self,
        page: &ImportedPage,
        row: usize,
        values: &mut [pg_sys::Datum],
        isnull: &mut [bool],
    ) -> Result<(), ProjectError> {
        for (index, (column, view)) in self.columns.iter().zip(page.columns.iter()).enumerate() {
            if view.is_null(row) {
                values[index] = pg_sys::Datum::null();
                isnull[index] = true;
                continue;
            }

            match (column, view) {
                (ColumnProjector::Boolean, PageColumnView::Boolean(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row));
                }
                (ColumnProjector::Int16, PageColumnView::Int16(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row));
                }
                (ColumnProjector::Int32, PageColumnView::Int32(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row));
                }
                (ColumnProjector::Int64, PageColumnView::Int64(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row));
                }
                (ColumnProjector::Float32, PageColumnView::Float32(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row).to_bits());
                }
                (ColumnProjector::Float64, PageColumnView::Float64(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row).to_bits());
                }
                (ColumnProjector::Uuid, PageColumnView::Uuid(array)) => {
                    values[index] = pg_sys::Datum::from(array.value(row).as_ptr() as *mut u8);
                }
                (ColumnProjector::TextLike(projector), PageColumnView::Utf8View(array)) => {
                    values[index] = text_datum(*projector, array.value(row), index)?;
                }
                (ColumnProjector::Bytea, PageColumnView::BinaryView(array)) => {
                    let bytea = rust_byte_slice_to_bytea(array.value(row));
                    values[index] = pg_sys::Datum::from(bytea.as_ptr());
                }
                _ => unreachable!("imported page columns must match the configured projector"),
            }

            isnull[index] = false;
        }

        Ok(())
    }
}

impl PageSlotCursor<'_> {
    /// Project the next row from the current page into `slot`.
    ///
    /// On success this returns `Some(slot)` until the current page is
    /// exhausted. The first call after the last row clears the slot, resets the
    /// per-tuple memory context, releases the page-backed arrays, and returns
    /// `None`.
    ///
    /// # Safety
    ///
    /// `slot` must be a live PostgreSQL `TTSOpsVirtual` slot whose tuple
    /// descriptor exactly matches the projector tuple descriptor used to create
    /// this cursor.
    pub unsafe fn next_into_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<Option<*mut pg_sys::TupleTableSlot>, ProjectError> {
        validate_slot(slot, self.projector.tuple_desc)?;

        unsafe { pg_sys::ExecClearTuple(slot) };
        unsafe { pg_sys::MemoryContextReset(self.projector.per_tuple_memory) };

        let Some(page) = self.page.as_ref() else {
            return Ok(None);
        };

        if self.next_row >= page.row_count {
            self.page = None;
            return Ok(None);
        }

        let slot_ref = unsafe { &mut *slot };
        let values =
            unsafe { slice::from_raw_parts_mut(slot_ref.tts_values, self.projector.columns.len()) };
        let isnull =
            unsafe { slice::from_raw_parts_mut(slot_ref.tts_isnull, self.projector.columns.len()) };
        values.fill(pg_sys::Datum::null());
        isnull.fill(true);

        let mut per_tuple_memory = PgMemoryContexts::For(self.projector.per_tuple_memory);
        let project = unsafe {
            per_tuple_memory.switch_to(|_| {
                self.projector
                    .project_row(page, self.next_row, values, isnull)
            })
        };
        if let Err(err) = project {
            values.fill(pg_sys::Datum::null());
            isnull.fill(true);
            slot_ref.tts_nvalid = 0;
            unsafe { pg_sys::ExecClearTuple(slot) };
            unsafe { pg_sys::MemoryContextReset(self.projector.per_tuple_memory) };
            return Err(err);
        }

        slot_ref.tts_nvalid = self.projector.columns.len() as pg_sys::AttrNumber;
        self.next_row += 1;
        Ok(Some(unsafe { pg_sys::ExecStoreVirtualTuple(slot) }))
    }
}

impl PageColumnView {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Boolean(array) => array.is_null(row),
            Self::Int16(array) => array.is_null(row),
            Self::Int32(array) => array.is_null(row),
            Self::Int64(array) => array.is_null(row),
            Self::Float32(array) => array.is_null(row),
            Self::Float64(array) => array.is_null(row),
            Self::Uuid(array) => array.is_null(row),
            Self::Utf8View(array) => array.is_null(row),
            Self::BinaryView(array) => array.is_null(row),
        }
    }
}

fn projector_for_attr(
    index: usize,
    oid: pg_sys::Oid,
    atttypmod: i32,
    type_tag: TypeTag,
) -> Result<ColumnProjector, ConfigError> {
    let projector = match (type_tag, oid) {
        (TypeTag::Boolean, oid) if oid == pg_sys::BOOLOID => ColumnProjector::Boolean,
        (TypeTag::Int16, oid) if oid == pg_sys::INT2OID => ColumnProjector::Int16,
        (TypeTag::Int32, oid) if oid == pg_sys::INT4OID => ColumnProjector::Int32,
        (TypeTag::Int64, oid) if oid == pg_sys::INT8OID => ColumnProjector::Int64,
        (TypeTag::Float32, oid) if oid == pg_sys::FLOAT4OID => ColumnProjector::Float32,
        (TypeTag::Float64, oid) if oid == pg_sys::FLOAT8OID => ColumnProjector::Float64,
        (TypeTag::Uuid, oid) if oid == pg_sys::UUIDOID => ColumnProjector::Uuid,
        (TypeTag::Utf8View, oid) if oid == pg_sys::TEXTOID => {
            ColumnProjector::TextLike(TextLikeProjector {
                kind: TextLikeKind::Text,
                atttypmod,
            })
        }
        (TypeTag::Utf8View, oid) if oid == pg_sys::VARCHAROID => {
            ColumnProjector::TextLike(TextLikeProjector {
                kind: TextLikeKind::Varchar,
                atttypmod,
            })
        }
        (TypeTag::Utf8View, oid) if oid == pg_sys::BPCHAROID => {
            ColumnProjector::TextLike(TextLikeProjector {
                kind: TextLikeKind::Bpchar,
                atttypmod,
            })
        }
        (TypeTag::Utf8View, oid) if oid == pg_sys::NAMEOID => {
            ColumnProjector::TextLike(TextLikeProjector {
                kind: TextLikeKind::Name,
                atttypmod,
            })
        }
        (TypeTag::BinaryView, oid) if oid == pg_sys::BYTEAOID => ColumnProjector::Bytea,
        _ => {
            return Err(ConfigError::PgLayoutTypeMismatch {
                index,
                oid: oid.to_u32(),
                type_tag,
            });
        }
    };
    Ok(projector)
}

unsafe fn validate_slot(
    slot: *mut pg_sys::TupleTableSlot,
    tuple_desc: pg_sys::TupleDesc,
) -> Result<(), ProjectError> {
    if slot.is_null() {
        return Err(ProjectError::NullSlot);
    }

    let slot_ref = unsafe { &*slot };
    if !ptr::eq(slot_ref.tts_ops, &raw const pg_sys::TTSOpsVirtual) {
        return Err(ProjectError::UnsupportedSlotOps);
    }
    if !ptr::eq(slot_ref.tts_tupleDescriptor, tuple_desc) {
        return Err(ProjectError::SlotTupleDescMismatch);
    }
    if slot_ref.tts_values.is_null() {
        return Err(ProjectError::SlotValuesNotInitialized);
    }
    if slot_ref.tts_isnull.is_null() {
        return Err(ProjectError::SlotNullsNotInitialized);
    }

    Ok(())
}

unsafe fn tuple_desc_attr(
    tuple_desc: pg_sys::TupleDesc,
    index: usize,
) -> pg_sys::FormData_pg_attribute {
    unsafe { *(*tuple_desc).attrs.as_ptr().add(index) }
}

fn text_datum(
    projector: TextLikeProjector,
    value: &str,
    index: usize,
) -> Result<pg_sys::Datum, ProjectError> {
    match projector.kind {
        TextLikeKind::Text => {
            let text = rust_str_to_text_p(value);
            Ok(pg_sys::Datum::from(text.as_ptr()))
        }
        TextLikeKind::Varchar => {
            apply_text_typmod(pg_sys::varchar, "varchar", value, projector.atttypmod)
        }
        TextLikeKind::Bpchar => {
            apply_text_typmod(pg_sys::bpchar, "bpchar", value, projector.atttypmod)
        }
        TextLikeKind::Name => {
            let max_len = (pg_sys::NAMEDATALEN as usize).saturating_sub(1);
            if value.len() > max_len {
                return Err(ProjectError::NameTooLong {
                    index,
                    len: value.len(),
                    max_len,
                });
            }

            let ptr = unsafe { pg_sys::palloc0(std::mem::size_of::<pg_sys::NameData>()) }
                as *mut pg_sys::NameData;
            unsafe {
                ptr::copy_nonoverlapping(
                    value.as_ptr(),
                    (*ptr).data.as_mut_ptr().cast::<u8>(),
                    value.len(),
                );
            }
            Ok(pg_sys::Datum::from(ptr))
        }
    }
}

fn apply_text_typmod(
    func: unsafe fn(pg_sys::FunctionCallInfo) -> pg_sys::Datum,
    label: &'static str,
    value: &str,
    atttypmod: i32,
) -> Result<pg_sys::Datum, ProjectError> {
    let text = rust_str_to_text_p(value);
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        direct_function_call_as_datum(
            func,
            &[
                Some(pg_sys::Datum::from(text.as_ptr())),
                Some(pg_sys::Datum::from(atttypmod)),
                Some(pg_sys::Datum::from(false)),
            ],
        )
        .ok_or_else(|| ProjectError::Postgres(format!("{label} returned null datum")))
    }))
    .catch_others(|error| Err(project_error_from_caught_error(error)))
    .execute()
}

fn project_error_from_caught_error(error: CaughtError) -> ProjectError {
    let message = match error {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report.message().to_owned(),
    };
    ProjectError::Postgres(message)
}

#[cfg(not(test))]
fn database_encoding() -> i32 {
    unsafe { pg_sys::GetDatabaseEncoding() }
}

#[cfg(test)]
static TEST_DATABASE_ENCODING: AtomicI32 = AtomicI32::new(pg_sys::pg_enc::PG_UTF8 as i32);

#[cfg(test)]
fn database_encoding() -> i32 {
    TEST_DATABASE_ENCODING.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) fn set_test_database_encoding(encoding: i32) -> i32 {
    TEST_DATABASE_ENCODING.swap(encoding, Ordering::Relaxed)
}
