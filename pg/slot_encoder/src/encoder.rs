use crate::datum::{
    database_encoding, pg_oid_needs_detoast, read_bool, read_f32, read_f64, read_fixed_bytes,
    read_i16, read_i32, read_i64, read_name_bytes, read_packed_varlena, validate_pg_layout_type,
    with_detoasted_slot_datum,
};
use crate::{ConfigError, EncodeError};
use arrow_layout::TypeTag;
use pgrx_pg_sys as pg_sys;
use row_encoder::{CellRef, FixedWidthCell, FixedWidthRowSource, PageRowEncoder, RowSource};
use std::ptr;
use std::time::Instant;

pub use row_encoder::{AppendStatus, EncodedBatch};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EncodeProfile {
    pub append_precheck_ns: u64,
    pub append_precheck_total: u64,
    pub tupledesc_check_ns: u64,
    pub tupledesc_check_total: u64,
    pub slot_deform_ns: u64,
    pub slot_deform_total: u64,
    pub cell_extract_ns: u64,
    pub cells_extracted_total: u64,
    pub varlena_detoast_ns: u64,
    pub varlena_detoast_total: u64,
    pub page_write_ns: u64,
    pub row_encode_outer_ns: u64,
    pub row_encode_outer_total: u64,
}

impl EncodeProfile {
    pub fn classified_ns(self) -> u64 {
        self.append_precheck_ns
            .saturating_add(self.tupledesc_check_ns)
            .saturating_add(self.slot_deform_ns)
            .saturating_add(self.cell_extract_ns)
            .saturating_add(self.varlena_detoast_ns)
            .saturating_add(self.page_write_ns)
            .saturating_add(self.row_encode_outer_ns)
    }
}

#[derive(Clone, Copy)]
struct RowEncodeProfileStart {
    start: Instant,
    cell_extract_ns: u64,
    varlena_detoast_ns: u64,
    page_write_ns: u64,
}

impl RowEncodeProfileStart {
    fn new(profile: &EncodeProfile) -> Self {
        Self {
            start: Instant::now(),
            cell_extract_ns: profile.cell_extract_ns,
            varlena_detoast_ns: profile.varlena_detoast_ns,
            page_write_ns: profile.page_write_ns,
        }
    }

    fn classified_delta(self, profile: &EncodeProfile) -> u64 {
        profile
            .cell_extract_ns
            .saturating_sub(self.cell_extract_ns)
            .saturating_add(
                profile
                    .varlena_detoast_ns
                    .saturating_sub(self.varlena_detoast_ns),
            )
            .saturating_add(profile.page_write_ns.saturating_sub(self.page_write_ns))
    }
}

/// Direct writer from PostgreSQL `TupleTableSlot` rows into an
/// `arrow_layout` block.
///
/// `PageBatchEncoder` is the PostgreSQL adapter over `row_encoder`: it
/// validates the `TupleDesc`, deforms enough slot attributes, detoasts varlena
/// values, and feeds typed cells into the PostgreSQL-free page writer.
#[derive(Debug)]
pub struct PageBatchEncoder<'payload> {
    tuple_desc: pg_sys::TupleDesc,
    attrs_ptr: *mut pg_sys::FormData_pg_attribute,
    needed_attrs: i32,
    projection_ptr: *const usize,
    projection_len: usize,
    inner: PageRowEncoder<'payload>,
    accepted_slot_desc: Option<pg_sys::TupleDesc>,
    fixed_width_fast_path: bool,
}

impl<'payload> PageBatchEncoder<'payload> {
    /// Creates a new encoder over an initialized `arrow_layout` block.
    ///
    /// This validates that the target block and PostgreSQL `TupleDesc` have the
    /// same column count and compatible logical types.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must point to a valid PostgreSQL `TupleDescData` whose
    /// attribute array remains alive for the lifetime of the encoder.
    pub unsafe fn new(
        tuple_desc: pg_sys::TupleDesc,
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        unsafe { Self::new_inner(tuple_desc, None, payload) }
    }

    /// Creates a new encoder over a projected view of a PostgreSQL slot.
    ///
    /// `source_columns[output_index]` is the zero-based attribute index in the
    /// incoming slot that should be written into the corresponding output
    /// layout column. The slice must remain alive until the encoder is dropped.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must point to a valid PostgreSQL `TupleDescData` whose
    /// attribute array remains alive for the lifetime of the encoder.
    pub unsafe fn new_projected(
        tuple_desc: pg_sys::TupleDesc,
        source_columns: &[usize],
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        unsafe { Self::new_inner(tuple_desc, Some(source_columns), payload) }
    }

    unsafe fn new_inner(
        tuple_desc: pg_sys::TupleDesc,
        source_columns: Option<&[usize]>,
        payload: &'payload mut [u8],
    ) -> Result<Self, ConfigError> {
        if tuple_desc.is_null() {
            return Err(ConfigError::NullTupleDesc);
        }

        let inner = PageRowEncoder::new(payload)?;
        let layout_cols = inner.column_count();
        let tuple_desc_cols = unsafe { (*tuple_desc).natts as usize };
        if let Some(source_columns) = source_columns {
            if layout_cols != source_columns.len() {
                return Err(ConfigError::ProjectionLengthMismatch {
                    layout_cols,
                    projection_cols: source_columns.len(),
                });
            }
        } else if layout_cols != tuple_desc_cols {
            return Err(ConfigError::ColumnCountMismatch {
                layout_cols,
                tuple_desc_cols,
            });
        }

        let attrs_ptr = unsafe { (*tuple_desc).attrs.as_mut_ptr() };
        let mut needs_utf8 = false;
        let mut fixed_width_fast_path = layout_cols > 0;
        let mut max_needed_attr = 0usize;
        for index in 0..layout_cols {
            let source_index = source_columns.map_or(index, |columns| columns[index]);
            if source_index >= tuple_desc_cols {
                return Err(ConfigError::ProjectionIndexOutOfBounds {
                    index,
                    source_index,
                    tuple_desc_cols,
                });
            }

            let attr = unsafe { &*attrs_ptr.add(source_index) };
            if attr.attisdropped {
                return if source_columns.is_some() {
                    Err(ConfigError::ProjectedDroppedAttribute {
                        index,
                        source_index,
                    })
                } else {
                    Err(ConfigError::DroppedAttribute { index })
                };
            }

            let type_tag = inner.column_type_tag(index)?;
            validate_pg_layout_type(index, attr.atttypid, type_tag)?;
            if type_tag == TypeTag::Utf8View {
                needs_utf8 = true;
            }
            if inner.column_is_nullable(index)?
                || !matches!(
                    type_tag,
                    TypeTag::Int16
                        | TypeTag::Int32
                        | TypeTag::Int64
                        | TypeTag::Float32
                        | TypeTag::Float64
                )
            {
                fixed_width_fast_path = false;
            }
            max_needed_attr = max_needed_attr.max(source_index + 1);
        }

        if needs_utf8 {
            let encoding = database_encoding();
            if encoding != pg_sys::pg_enc::PG_UTF8 as i32 {
                return Err(ConfigError::NonUtf8ServerEncoding { encoding });
            }
        }

        Ok(Self {
            tuple_desc,
            attrs_ptr,
            needed_attrs: i32::try_from(max_needed_attr)
                .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?,
            projection_ptr: source_columns.map_or(ptr::null(), |columns| columns.as_ptr()),
            projection_len: source_columns.map_or(0, <[usize]>::len),
            inner,
            accepted_slot_desc: None,
            fixed_width_fast_path,
        })
    }

    /// Appends one row from a PostgreSQL `TupleTableSlot`.
    ///
    /// The slot may be undeformed or partially deformed; the encoder will ask
    /// PostgreSQL to deform enough attributes for the target layout when
    /// needed.
    ///
    /// Returns [`AppendStatus::Full`] when the row does not fit into the
    /// current block. In that case the caller should finalize the current
    /// block, allocate a fresh one, and retry the same slot there.
    pub fn append_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<AppendStatus, EncodeError> {
        self.append_slot_inner(slot, None)
    }

    pub fn append_slot_profiled(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        profile: &mut EncodeProfile,
    ) -> Result<AppendStatus, EncodeError> {
        self.append_slot_inner(slot, Some(profile))
    }

    fn append_slot_inner(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        mut profile: Option<&mut EncodeProfile>,
    ) -> Result<AppendStatus, EncodeError> {
        if let Some(profile) = profile.as_mut() {
            profile.append_precheck_total = profile.append_precheck_total.saturating_add(1);
        }
        let precheck_start = profile.as_ref().map(|_| Instant::now());
        if slot.is_null() {
            record_append_precheck(&mut profile, precheck_start);
            return Err(EncodeError::NullSlot);
        }
        let actual_tuple_desc = unsafe { (*slot).tts_tupleDescriptor };
        if actual_tuple_desc.is_null() {
            record_append_precheck(&mut profile, precheck_start);
            return Err(EncodeError::NullSlotTupleDesc);
        }
        record_append_precheck(&mut profile, precheck_start);

        let tupledesc_start = profile.as_ref().map(|_| Instant::now());
        let tupledesc_result = self.validate_slot_tuple_desc(actual_tuple_desc);
        record_tupledesc_check(&mut profile, tupledesc_start);
        tupledesc_result?;

        let precheck_start = profile.as_ref().map(|_| Instant::now());
        let needed_attrs = usize::try_from(self.needed_attrs)
            .map_err(|_| arrow_layout::LayoutError::SizeOverflow)?;
        let valid = unsafe { (*slot).tts_nvalid as usize };
        if valid < needed_attrs {
            record_append_precheck(&mut profile, precheck_start);
            let deform_start = profile.as_ref().map(|_| Instant::now());
            unsafe { deform_slot_to(slot, self.needed_attrs)? };
            if let (Some(profile), Some(start)) = (profile.as_mut(), deform_start) {
                profile.slot_deform_ns = profile.slot_deform_ns.saturating_add(elapsed_ns(start));
                profile.slot_deform_total = profile.slot_deform_total.saturating_add(1);
            }
        } else {
            record_append_precheck(&mut profile, precheck_start);
        }

        let precheck_start = profile.as_ref().map(|_| Instant::now());
        let values = unsafe { (*slot).tts_values };
        let isnulls = unsafe { (*slot).tts_isnull };
        if values.is_null() || isnulls.is_null() {
            record_append_precheck(&mut profile, precheck_start);
            return Err(EncodeError::InvalidSlotStorage);
        }
        record_append_precheck(&mut profile, precheck_start);

        let row_encode_start = profile
            .as_ref()
            .map(|profile| RowEncodeProfileStart::new(profile));
        let mut source = PgSlotRow {
            attrs_ptr: self.attrs_ptr,
            projection_ptr: self.projection_ptr,
            projection_len: self.projection_len,
            values,
            isnulls,
            profile,
        };
        let result = if self.fixed_width_fast_path {
            self.inner.append_fixed_width_row(&mut source)
        } else {
            self.inner.append_row(&mut source)
        };
        if let (Some(start), Some(profile)) = (row_encode_start, source.profile.as_mut()) {
            let row_encode_ns = elapsed_ns(start.start);
            let classified_ns = start.classified_delta(profile);
            let unclassified_ns = row_encode_ns.saturating_sub(classified_ns);
            if self.fixed_width_fast_path {
                profile.page_write_ns = profile.page_write_ns.saturating_add(unclassified_ns);
            } else {
                profile.row_encode_outer_ns =
                    profile.row_encode_outer_ns.saturating_add(unclassified_ns);
            }
            profile.row_encode_outer_total = profile.row_encode_outer_total.saturating_add(1);
        }
        result
    }

    fn source_index(&self, output_index: usize) -> usize {
        if self.projection_len == 0 {
            output_index
        } else {
            debug_assert!(output_index < self.projection_len);
            unsafe { *self.projection_ptr.add(output_index) }
        }
    }

    fn validate_slot_tuple_desc(
        &mut self,
        actual_tuple_desc: pg_sys::TupleDesc,
    ) -> Result<(), EncodeError> {
        if actual_tuple_desc == self.tuple_desc
            || self.accepted_slot_desc == Some(actual_tuple_desc)
        {
            return Ok(());
        }

        let actual_cols = unsafe { (*actual_tuple_desc).natts as usize };
        if self.projection_len == 0 && actual_cols != self.inner.column_count() {
            return Err(EncodeError::SlotTupleDescMismatch);
        }

        let actual_attrs_ptr = unsafe { (*actual_tuple_desc).attrs.as_mut_ptr() };
        for index in 0..self.inner.column_count() {
            let source_index = self.source_index(index);
            if source_index >= actual_cols {
                return Err(EncodeError::SlotTupleDescMismatch);
            }

            let expected = unsafe { &*self.attrs_ptr.add(source_index) };
            let actual = unsafe { &*actual_attrs_ptr.add(source_index) };
            if actual.attisdropped
                || actual.atttypid != expected.atttypid
                || actual.attlen != expected.attlen
                || actual.attbyval != expected.attbyval
            {
                return Err(EncodeError::SlotTupleDescMismatch);
            }
        }

        self.accepted_slot_desc = Some(actual_tuple_desc);
        Ok(())
    }

    /// Finalizes the block and returns the written row count and payload
    /// length.
    pub fn finish(self) -> Result<EncodedBatch, EncodeError> {
        self.inner.finish().map_err(Into::into)
    }

    #[cfg(test)]
    pub(crate) fn tail_cursor(&self) -> u32 {
        self.inner.tail_cursor_for_tests()
    }

    #[cfg(test)]
    pub(crate) fn fixed_width_fast_path_for_tests(&self) -> bool {
        self.fixed_width_fast_path
    }
}

fn record_append_precheck(profile: &mut Option<&mut EncodeProfile>, start: Option<Instant>) {
    if let (Some(profile), Some(start)) = (profile.as_mut(), start) {
        profile.append_precheck_ns = profile.append_precheck_ns.saturating_add(elapsed_ns(start));
    }
}

fn record_tupledesc_check(profile: &mut Option<&mut EncodeProfile>, start: Option<Instant>) {
    if let (Some(profile), Some(start)) = (profile.as_mut(), start) {
        profile.tupledesc_check_ns = profile.tupledesc_check_ns.saturating_add(elapsed_ns(start));
        profile.tupledesc_check_total = profile.tupledesc_check_total.saturating_add(1);
    }
}

unsafe fn deform_slot_to(
    slot: *mut pg_sys::TupleTableSlot,
    needed_attrs: i32,
) -> Result<(), EncodeError> {
    if needed_attrs <= 0 {
        return Ok(());
    }

    let ops = unsafe { (*slot).tts_ops };
    if ops.is_null() {
        return Err(EncodeError::SlotAttrOpsUnavailable {
            attnum: needed_attrs as usize,
        });
    }
    let Some(getsomeattrs) = (unsafe { (*ops).getsomeattrs }) else {
        return Err(EncodeError::SlotAttrOpsUnavailable {
            attnum: needed_attrs as usize,
        });
    };

    unsafe {
        getsomeattrs(slot, needed_attrs);
    }

    let valid = i32::from(unsafe { (*slot).tts_nvalid });
    if valid < needed_attrs {
        unsafe {
            pg_sys::slot_getmissingattrs(slot, valid, needed_attrs);
            (*slot).tts_nvalid = needed_attrs as i16;
        }
    }

    if i32::from(unsafe { (*slot).tts_nvalid }) < needed_attrs {
        return Err(EncodeError::SlotAttrAccess {
            attnum: needed_attrs as usize,
        });
    }

    Ok(())
}

struct PgSlotRow<'profile> {
    attrs_ptr: *mut pg_sys::FormData_pg_attribute,
    projection_ptr: *const usize,
    projection_len: usize,
    values: *mut pg_sys::Datum,
    isnulls: *mut bool,
    profile: Option<&'profile mut EncodeProfile>,
}

impl PgSlotRow<'_> {
    fn source_index(&self, output_index: usize) -> usize {
        if self.projection_len == 0 {
            output_index
        } else {
            debug_assert!(output_index < self.projection_len);
            unsafe { *self.projection_ptr.add(output_index) }
        }
    }

    fn profile_enabled(&self) -> bool {
        self.profile.is_some()
    }

    fn record_extract_time(&mut self, start: Option<Instant>) {
        if let (Some(profile), Some(start)) = (self.profile.as_mut(), start) {
            profile.cell_extract_ns = profile.cell_extract_ns.saturating_add(elapsed_ns(start));
        }
    }

    fn record_cell_extracted(&mut self, start: Option<Instant>) {
        if let (Some(profile), Some(start)) = (self.profile.as_mut(), start) {
            profile.cell_extract_ns = profile.cell_extract_ns.saturating_add(elapsed_ns(start));
            profile.cells_extracted_total = profile.cells_extracted_total.saturating_add(1);
        }
    }

    fn record_detoast(&mut self, start: Option<Instant>) {
        if let (Some(profile), Some(start)) = (self.profile.as_mut(), start) {
            profile.varlena_detoast_ns =
                profile.varlena_detoast_ns.saturating_add(elapsed_ns(start));
            profile.varlena_detoast_total = profile.varlena_detoast_total.saturating_add(1);
        }
    }

    fn write_cell<R>(
        &mut self,
        extract_start: Option<Instant>,
        cell: CellRef<'_>,
        f: impl FnOnce(CellRef<'_>) -> Result<R, EncodeError>,
    ) -> Result<R, EncodeError> {
        self.record_cell_extracted(extract_start);
        let write_start = self.profile_enabled().then(Instant::now);
        let result = f(cell);
        if let (Some(profile), Some(start)) = (self.profile.as_mut(), write_start) {
            profile.page_write_ns = profile.page_write_ns.saturating_add(elapsed_ns(start));
        }
        result
    }
}

impl FixedWidthRowSource for PgSlotRow<'_> {
    type Error = EncodeError;

    fn fixed_width_cell(
        &mut self,
        index: usize,
        type_tag: TypeTag,
    ) -> Result<FixedWidthCell, Self::Error> {
        let extract_start = self.profile_enabled().then(Instant::now);
        let source_idx = self.source_index(index);
        let attr = unsafe { &*self.attrs_ptr.add(source_idx) };
        let is_null = unsafe { *self.isnulls.add(source_idx) };
        if is_null {
            self.record_cell_extracted(extract_start);
            return Err(EncodeError::NullInNonNullableColumn { index });
        }

        let datum = unsafe { *self.values.add(source_idx) };
        let cell = match type_tag {
            TypeTag::Int16 => FixedWidthCell::Int16(unsafe { read_i16(datum, attr.attbyval) }),
            TypeTag::Int32 => FixedWidthCell::Int32(unsafe { read_i32(datum, attr.attbyval) }),
            TypeTag::Int64 => FixedWidthCell::Int64(unsafe { read_i64(datum, attr.attbyval) }),
            TypeTag::Float32 => FixedWidthCell::Float32(unsafe { read_f32(datum, attr.attbyval) }),
            TypeTag::Float64 => FixedWidthCell::Float64(unsafe { read_f64(datum, attr.attbyval) }),
            _ => return Err(EncodeError::UnsupportedRowAccess { index }),
        };
        self.record_cell_extracted(extract_start);
        Ok(cell)
    }
}

impl RowSource for PgSlotRow<'_> {
    type Error = EncodeError;

    fn with_cell<R>(
        &mut self,
        index: usize,
        f: impl FnOnce(CellRef<'_>) -> Result<R, Self::Error>,
    ) -> Result<R, Self::Error> {
        let extract_start = self.profile_enabled().then(Instant::now);
        let source_idx = self.source_index(index);
        let attr = unsafe { &*self.attrs_ptr.add(source_idx) };
        let is_null = unsafe { *self.isnulls.add(source_idx) };
        if is_null {
            return self.write_cell(extract_start, CellRef::Null, f);
        }

        let datum = unsafe { *self.values.add(source_idx) };
        match attr.atttypid {
            oid if oid == pg_sys::BOOLOID => self.write_cell(
                extract_start,
                CellRef::Boolean(unsafe { read_bool(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::INT2OID => self.write_cell(
                extract_start,
                CellRef::Int16(unsafe { read_i16(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::INT4OID => self.write_cell(
                extract_start,
                CellRef::Int32(unsafe { read_i32(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::INT8OID => self.write_cell(
                extract_start,
                CellRef::Int64(unsafe { read_i64(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::FLOAT4OID => self.write_cell(
                extract_start,
                CellRef::Float32(unsafe { read_f32(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::FLOAT8OID => self.write_cell(
                extract_start,
                CellRef::Float64(unsafe { read_f64(datum, attr.attbyval) }),
                f,
            ),
            oid if oid == pg_sys::UUIDOID => {
                let bytes = unsafe { read_fixed_bytes(datum, 16, index)? };
                self.write_cell(extract_start, CellRef::Uuid(bytes), f)
            }
            oid if oid == pg_sys::NAMEOID => {
                let bytes = unsafe { read_name_bytes(datum, index)? };
                self.write_cell(extract_start, CellRef::Utf8(bytes), f)
            }
            oid if pg_oid_needs_detoast(oid) => {
                self.record_extract_time(extract_start);
                let detoast_start = self.profile_enabled().then(Instant::now);
                with_detoasted_slot_datum(datum, index, |detoasted| {
                    self.record_detoast(detoast_start);
                    let extract_start = self.profile_enabled().then(Instant::now);
                    let bytes = unsafe { read_packed_varlena(detoasted, index)? };
                    if oid == pg_sys::BYTEAOID {
                        self.write_cell(extract_start, CellRef::Binary(bytes), f)
                    } else {
                        self.write_cell(extract_start, CellRef::Utf8(bytes), f)
                    }
                })
            }
            _ => Err(EncodeError::UnsupportedRowAccess { index }),
        }
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}
