use anyhow::{anyhow, bail, Result};
use common::FusionError;
use core::cmp::min;
use core::ffi::c_void;
use core::mem::size_of;
use core::ptr;
use core::slice;
use datafusion_common::ScalarValue;
use pg_sys::BLCKSZ;
use pgrx_pg_sys as pg_sys;
use pgrx_pg_sys::ItemIdData;
use pgrx_pg_sys::PageHeaderData;
use protocol::failure::OutOfBound;
use smol_str::format_smolstr;
use std::vec::Vec;

pub struct HeapPage<'bytes> {
    data: &'bytes [u8],
}

impl<'bytes> HeapPage<'bytes> {
    /// # Safety
    /// - The underlying memory must contain a valid heap page located at
    ///   `base + 0` with proper alignment.
    pub unsafe fn from_slice(data: &'bytes [u8]) -> Result<Self> {
        if data.len() < BLCKSZ as usize {
            bail!(FusionError::BufferTooSmall(data.len()));
        }
        Ok(Self { data })
    }

    fn header(&self) -> &'bytes PageHeaderData {
        unsafe { &*(self.data.as_ptr() as *const PageHeaderData) }
    }

    /// Check if the page header has the `PD_ALL_VISIBLE` flag set,
    /// meaning all tuples on the page are visible to all transactions.
    pub fn is_all_visible(&self) -> bool {
        let hdr = self.header();
        (hdr.pd_flags & (pg_sys::PD_ALL_VISIBLE as u16)) != 0
    }

    /// Number of line pointers (`ItemIdData`) on this page, derived from
    /// `pd_lower` and `PageHeaderData` size.
    pub fn lp_count(&self) -> usize {
        let hdr = self.header();
        let lower = hdr.pd_lower as usize;
        let header_sz = size_of::<PageHeaderData>();
        if lower < header_sz || lower > self.data.len() {
            return 0;
        }
        (lower - header_sz) / size_of::<ItemIdData>()
    }

    /// Returns an iterator over the line pointer array (`pd_linp`) yielding
    /// slices of live tuple bytes (LP_NORMAL) from the page data region.
    ///
    /// `filter` allows custom filtering of line pointers; when present, it is
    /// called with the `ItemIdData` and opaque context `ctx` and should return
    /// true to include the tuple or false to skip it.
    pub fn tuples(&self, filter: Option<LpFilter>, ctx: *mut c_void) -> TupleSliceIter<'bytes> {
        let count = self.lp_count();
        let start =
            unsafe { self.data.as_ptr().add(size_of::<PageHeaderData>()) as *const ItemIdData };
        let end = unsafe { start.add(count) };
        let hdr = self.header();
        let upper = hdr.pd_upper as usize;
        let special = hdr.pd_special as usize;
        TupleSliceIter {
            cur: start,
            end,
            page: self.data,
            upper,
            special,
            filter,
            ctx,
        }
    }

    /// Returns tuple slices ordered by ascending `lp_off`, filling the caller-provided
    /// `(off,len)` buffer referenced by a mutable pointer and returning an iterator that
    /// borrows the filled pairs slice. This allows reuse of the same allocation across
    /// multiple pages.
    pub fn tuples_by_offset(
        &self,
        filter: Option<LpFilter>,
        ctx: *mut c_void,
        pairs: &'bytes mut Vec<(u16, u16)>,
    ) -> SortedTupleSliceIterBorrowed<'bytes> {
        let cnt = self.lp_count();
        if pairs.capacity() < cnt {
            pairs.reserve(cnt - pairs.capacity());
        }
        pairs.clear();

        let hdr = self.header();
        let upper = hdr.pd_upper as usize;
        let special = hdr.pd_special as usize;
        let special_limit = core::cmp::min(special, self.data.len());

        let mut cur =
            unsafe { self.data.as_ptr().add(size_of::<PageHeaderData>()) as *const ItemIdData };
        let end = unsafe { cur.add(cnt) };
        while cur < end {
            let item = unsafe { ptr::read(cur) };
            cur = unsafe { cur.add(1) };
            if (item.lp_flags() as u32) != pg_sys::LP_NORMAL {
                continue;
            }
            let off = item.lp_off() as usize;
            let len = item.lp_len() as usize;
            if len == 0 {
                continue;
            }
            if off < upper {
                continue;
            }
            let end_off = match off.checked_add(len) {
                Some(v) => v,
                None => continue,
            };
            if end_off > special_limit {
                continue;
            }
            if let Some(f) = filter {
                if !f(&item, ctx) {
                    continue;
                }
            }
            pairs.push((off as u16, len as u16));
        }

        pairs.sort_unstable_by_key(|&(off, _)| off);

        SortedTupleSliceIterBorrowed {
            page: self.data,
            pairs: pairs.as_slice(),
            idx: 0,
        }
    }
}

/// Callback to filter a line pointer (ItemIdData) using opaque context pointer.
pub type LpFilter = fn(item: &ItemIdData, ctx: *mut c_void) -> bool;

/// Iterator over `pd_linp` yielding `&[u8]` slices for LP_NORMAL entries.
pub struct TupleSliceIter<'block> {
    cur: *const ItemIdData,
    end: *const ItemIdData,
    page: &'block [u8],
    upper: usize,
    special: usize,
    filter: Option<LpFilter>,
    ctx: *mut c_void,
}

impl TupleSliceIter<'_> {
    #[inline]
    fn base_ptr(&self) -> *const u8 {
        self.page.as_ptr()
    }
    #[inline]
    fn page_len(&self) -> usize {
        self.page.len()
    }
}

impl<'block> Iterator for TupleSliceIter<'block> {
    type Item = &'block [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while self.cur < self.end {
            // Safety: constructed within `line_pointers`, `cur < end` and points to valid item
            let item = unsafe { ptr::read(self.cur) };
            self.cur = unsafe { self.cur.add(1) };

            // Filter only live tuples
            let flags = item.lp_flags() as u32;
            if flags != pg_sys::LP_NORMAL {
                continue;
            }

            // Derive offset and length; validate within data region
            let off = item.lp_off() as usize;
            let len = item.lp_len() as usize;

            // basic sanity: nonzero length and within page bounds
            if len == 0 {
                continue;
            }
            let end_off = match off.checked_add(len) {
                Some(v) => v,
                None => continue,
            };

            // Clip special to page_len to be defensive
            let special_limit = min(self.special, self.page_len());

            // Enforce: pd_upper <= off && end_off <= pd_special
            if off < self.upper || end_off > special_limit {
                continue;
            }

            // User filter (if any)
            if let Some(f) = self.filter {
                if !f(&item, self.ctx) {
                    continue;
                }
            }

            let ptr = unsafe { self.base_ptr().add(off) };
            let slice = unsafe { slice::from_raw_parts(ptr, len) };
            return Some(slice);
        }
        None
    }
}

/// Iterator over tuple slices sorted by `lp_off`.
pub struct SortedTupleSliceIter<'bytes> {
    page: &'bytes [u8],
    pairs: Vec<(u16, u16)>,
    idx: usize,
}

impl<'bytes> Iterator for SortedTupleSliceIter<'bytes> {
    type Item = &'bytes [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.pairs.len() {
            return None;
        }
        let (off, len) = self.pairs[self.idx];
        self.idx += 1;
        let ptr = unsafe { self.page.as_ptr().add(off as usize) };
        Some(unsafe { slice::from_raw_parts(ptr, len as usize) })
    }
}

/// Iterator over tuple slices sorted by `lp_off` that borrows a pairs slice
pub struct SortedTupleSliceIterBorrowed<'bytes> {
    page: &'bytes [u8],
    pairs: &'bytes [(u16, u16)],
    idx: usize,
}

impl<'bytes> Iterator for SortedTupleSliceIterBorrowed<'bytes> {
    type Item = &'bytes [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.pairs.len() {
            return None;
        }
        let (off, len) = self.pairs[self.idx];
        self.idx += 1;
        let ptr = unsafe { self.page.as_ptr().add(off as usize) };
        Some(unsafe { slice::from_raw_parts(ptr, len as usize) })
    }
}

/// Compact attribute metadata needed to decode a heap tuple into values.
#[derive(Clone, Copy, Debug)]
pub struct PgAttrMeta {
    pub atttypid: pg_sys::Oid,
    pub attlen: i16,
    pub attalign: u8, // 'c','s','i','d'
}

#[inline]
fn align_up(off: usize, align_char: u8) -> usize {
    let a = match align_char as char {
        'c' => 1usize,
        's' => 2usize,
        'i' => 4usize,
        'd' => 8usize,
        _ => 1usize,
    };
    let mask = a - 1;
    (off + mask) & !mask
}

#[inline]
fn decode_fixed_width(atttypid: pg_sys::Oid, bytes: &[u8]) -> Result<Option<ScalarValue>> {
    use pg_sys::*;
    const UNIX_EPOCH_USEC_FROM_PG: i64 = 946_684_800_i64 * 1_000_000; // 1970-01-01 - 2000-01-01
    const UNIX_EPOCH_DAYS_FROM_PG: i32 = 10_957; // days between 1970-01-01 and 2000-01-01

    let v = match atttypid {
        x if x == BOOLOID => ScalarValue::Boolean(Some(bytes[0] != 0)),
        // PostgreSQL internal single-byte "char" type (not BPCHAR)
        x if x == CHAROID => {
            let ch = bytes[0] as char;
            ScalarValue::Utf8(Some(ch.to_string()))
        }
        x if x == INT2OID => {
            let mut a = [0u8; 2];
            a.copy_from_slice(bytes);
            ScalarValue::Int16(Some(i16::from_ne_bytes(a)))
        }
        x if x == INT4OID => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            ScalarValue::Int32(Some(i32::from_ne_bytes(a)))
        }
        // OID maps to Int32 for now (DataFusion UInt32 available but keep consistent with common integer use)
        x if x == OIDOID => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            let v = u32::from_ne_bytes(a) as i32;
            ScalarValue::Int32(Some(v))
        }
        x if x == INT8OID => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            ScalarValue::Int64(Some(i64::from_ne_bytes(a)))
        }
        x if x == FLOAT4OID => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            ScalarValue::Float32(Some(f32::from_ne_bytes(a)))
        }
        x if x == FLOAT8OID => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            ScalarValue::Float64(Some(f64::from_ne_bytes(a)))
        }
        x if x == DATEOID => {
            let mut a = [0u8; 4];
            a.copy_from_slice(bytes);
            let pg_days = i32::from_ne_bytes(a);
            // Preserve infinities as sentinels, otherwise convert from PG epoch (2000-01-01) to UNIX (1970-01-01)
            let unix_days = if pg_days == i32::MIN || pg_days == i32::MAX {
                pg_days
            } else {
                pg_days.saturating_add(UNIX_EPOCH_DAYS_FROM_PG)
            };
            ScalarValue::Date32(Some(unix_days))
        }
        x if x == TIMEOID => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            let usec = i64::from_ne_bytes(a);
            ScalarValue::Time64Microsecond(Some(usec))
        }
        x if x == TIMESTAMPOID || x == TIMESTAMPTZOID => {
            let mut a = [0u8; 8];
            a.copy_from_slice(bytes);
            let pg_usec = i64::from_ne_bytes(a);
            let unix_usec = pg_usec.saturating_add(UNIX_EPOCH_USEC_FROM_PG);
            ScalarValue::TimestampMicrosecond(Some(unix_usec), None)
        }
        x if x == INTERVALOID => {
            // struct Interval { TimeOffset time; int32 day; int32 month; }
            // time is microseconds
            let mut t = [0u8; 8];
            let mut d = [0u8; 4];
            let mut m = [0u8; 4];
            t.copy_from_slice(&bytes[0..8]);
            d.copy_from_slice(&bytes[8..12]);
            m.copy_from_slice(&bytes[12..16]);
            let usec = i64::from_ne_bytes(t);
            let day_count = i32::from_ne_bytes(d);
            let month_count = i32::from_ne_bytes(m);
            let nanos = usec.saturating_mul(1000);
            ScalarValue::IntervalMonthDayNano(Some(
                datafusion_common::arrow::array::types::IntervalMonthDayNano {
                    months: month_count,
                    days: day_count,
                    nanoseconds: nanos,
                },
            ))
        }
        // name is fixed-length (NAMEDATALEN) with trailing NULs
        x if x == NAMEOID => {
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            let s = std::str::from_utf8(&bytes[..end])
                .map_err(|e| anyhow::anyhow!("invalid utf8 in name: {}", e))?;
            ScalarValue::Utf8(Some(s.to_string()))
        }
        _ => return Ok(None),
    };
    Ok(Some(v))
}

#[inline]
fn decode_varlena_inline(atttypid: pg_sys::Oid, data: &[u8]) -> Result<Option<ScalarValue>> {
    use pg_sys::*;
    let v = match atttypid {
        x if x == TEXTOID || x == VARCHAROID || x == BPCHAROID => {
            match std::str::from_utf8(data) {
                Ok(s) => ScalarValue::Utf8(Some(s.to_string())),
                Err(e) => {
                    // text must be valid UTF-8; treat invalid bytes as an error (likely not inline text)
                    return Err(anyhow::anyhow!("invalid utf8 in varlena text: {}", e));
                }
            }
        }
        // BYTEA not part of protocol types; return None to let caller handle as Null or error
        x if x == BYTEAOID => return Ok(None),
        _ => return Ok(None),
    };
    Ok(Some(v))
}

#[inline]
fn typed_null_for(atttypid: pg_sys::Oid) -> ScalarValue {
    use pg_sys::*;
    match atttypid {
        x if x == BOOLOID => ScalarValue::Boolean(None),
        x if x == CHAROID => ScalarValue::Utf8(None),
        x if x == INT2OID => ScalarValue::Int16(None),
        x if x == INT4OID => ScalarValue::Int32(None),
        x if x == OIDOID => ScalarValue::Int32(None),
        x if x == INT8OID => ScalarValue::Int64(None),
        x if x == FLOAT4OID => ScalarValue::Float32(None),
        x if x == FLOAT8OID => ScalarValue::Float64(None),
        x if x == TEXTOID || x == VARCHAROID || x == BPCHAROID || x == NAMEOID => ScalarValue::Utf8(None),
        x if x == DATEOID => ScalarValue::Date32(None),
        x if x == TIMEOID => ScalarValue::Time64Microsecond(None),
        x if x == TIMESTAMPOID || x == TIMESTAMPTZOID => {
            ScalarValue::TimestampMicrosecond(None, None)
        }
        x if x == INTERVALOID => ScalarValue::IntervalMonthDayNano(None),
        _ => ScalarValue::Null,
    }
}

// Generic, iterator-driven projection decoder (no projection materialization required).
pub struct DecodedIter<'bytes, I: Iterator<Item = usize>> {
    _page_hdr: *const PageHeaderData,
    tuple: &'bytes [u8],
    attrs: &'bytes [PgAttrMeta],
    iter: I,
    last_attno: usize,
    off: usize,
    hasnull: bool,
    bits_ptr: *const u8,
    hoff: usize,
    pending_err: Option<anyhow::Error>,
}

impl<'bytes, I: Iterator<Item = usize>> DecodedIter<'bytes, I> {
    unsafe fn init(
        page_hdr: *const PageHeaderData,
        tuple: &'bytes [u8],
        attrs: &'bytes [PgAttrMeta],
        iter: I,
    ) -> Result<Self> {
        if tuple.len() < size_of::<pg_sys::HeapTupleHeaderData>() {
            bail!(FusionError::BufferTooSmall(tuple.len()));
        }
        let hdr = &*(tuple.as_ptr() as *const pg_sys::HeapTupleHeaderData);
        let infomask: u16 = hdr.t_infomask;
        let hasnull = (infomask & (pg_sys::HEAP_HASNULL as u16)) != 0;
        let hoff = hdr.t_hoff as usize;
        if hoff > tuple.len() {
            bail!(FusionError::BufferTooSmall(tuple.len()));
        }
        let bits_ptr = if hasnull {
            hdr.t_bits.as_ptr()
        } else {
            core::ptr::null()
        };
        Ok(Self {
            _page_hdr: page_hdr,
            tuple,
            attrs,
            iter,
            last_attno: 0,
            off: hoff,
            hasnull,
            bits_ptr,
            hoff,
            pending_err: None,
        })
    }

    #[inline]
    unsafe fn att_is_null(&self, attno: usize) -> bool {
        if !self.hasnull {
            return false;
        }
        let byte = *self.bits_ptr.add(attno >> 3);
        ((byte >> (attno & 0x07)) & 0x01) == 0
    }

    #[inline]
    fn varlena_is_1b(b0: u8) -> bool {
        if cfg!(target_endian = "little") {
            (b0 & 0x01) == 0x01
        } else {
            (b0 & 0x80) == 0x80
        }
    }
    #[inline]
    fn varlena_is_1b_external(b0: u8) -> bool {
        if cfg!(target_endian = "little") {
            b0 == 0x01
        } else {
            b0 == 0x80
        }
    }
    #[inline]
    fn varlena_1b_data_len(b0: u8) -> usize {
        if cfg!(target_endian = "little") {
            (b0 as usize) >> 1
        } else {
            (b0 as usize) & 0x7F
        }
    }
    #[inline]
    fn varlena_4b_total_len(hdr_u32: u32) -> usize {
        if cfg!(target_endian = "little") {
            (hdr_u32 >> 2) as usize
        } else {
            (hdr_u32 & 0x3FFF_FFFF) as usize
        }
    }
    #[inline]
    fn varlena_4b_is_compressed(hdr_u32: u32) -> bool {
        if cfg!(target_endian = "little") {
            (hdr_u32 & 0x03) == 0x02
        } else {
            ((hdr_u32 >> 30) & 0x03) == 0x01
        }
    }

    unsafe fn decode_att(&mut self, att_idx: usize) -> Result<ScalarValue> {
        if att_idx < self.last_attno {
            self.last_attno = 0;
            self.off = self.hoff;
        }
        while self.last_attno < att_idx {
            let meta = self
                .attrs
                .get(self.last_attno)
                .ok_or_else(|| OutOfBound(format_smolstr!("attr {}", self.last_attno)))?;
            if !self.att_is_null(self.last_attno) {
                if meta.attlen > 0 {
                    self.off = align_up(self.off, meta.attalign);
                    let len = meta.attlen as usize;
                    if self.off + len > self.tuple.len() {
                        bail!(FusionError::BufferTooSmall(self.tuple.len()));
                    }
                    self.off += len;
                } else {
                    if self.off >= self.tuple.len() {
                        bail!(FusionError::BufferTooSmall(self.tuple.len()));
                    }
                    let b0 = self.tuple[self.off];
                    if Self::varlena_is_1b(b0) {
                        let is_external = Self::varlena_is_1b_external(b0);
                        if is_external {
                            let total = 1usize + 18usize;
                            self.off = self.off.saturating_add(total);
                        } else {
                            let total = Self::varlena_1b_data_len(b0);
                            if self.off + total > self.tuple.len() {
                                bail!(FusionError::BufferTooSmall(self.tuple.len()));
                            }
                            self.off += total;
                        }
                    } else {
                        if self.off + 4 > self.tuple.len() {
                            bail!(FusionError::BufferTooSmall(self.tuple.len()));
                        }
                        let hdr_u32 = u32::from_ne_bytes(
                            self.tuple[self.off..self.off + 4].try_into().unwrap(),
                        );
                        let total_len = Self::varlena_4b_total_len(hdr_u32);
                        if total_len < 4 || self.off + total_len > self.tuple.len() {
                            bail!(FusionError::BufferTooSmall(self.tuple.len()));
                        }
                        self.off += total_len;
                    }
                }
            }
            self.last_attno += 1;
        }
        let meta = self
            .attrs
            .get(att_idx)
            .ok_or_else(|| OutOfBound(format_smolstr!("attr {}", att_idx)))?;
        if self.att_is_null(att_idx) {
            self.last_attno = att_idx + 1;
            return Ok(typed_null_for(meta.atttypid));
        }
        self.off = align_up(self.off, meta.attalign);
        if meta.attlen > 0 {
            let len = meta.attlen as usize;
            if self.off + len > self.tuple.len() {
                bail!(FusionError::BufferTooSmall(self.tuple.len()));
            }
            let v = decode_fixed_width(meta.atttypid, &self.tuple[self.off..self.off + len])?
                .unwrap_or(ScalarValue::Null);
            self.off += len;
            self.last_attno = att_idx + 1;
            return Ok(v);
        }
        if self.off >= self.tuple.len() {
            bail!(FusionError::BufferTooSmall(self.tuple.len()));
        }
        let b0 = self.tuple[self.off];
        if Self::varlena_is_1b(b0) {
            let is_external = Self::varlena_is_1b_external(b0);
            if is_external {
                let total = 1usize + 18usize;
                self.off = self.off.saturating_add(total);
                self.last_attno = att_idx + 1;
                bail!(anyhow!("external toasted value is not supported"));
            }
            let total = Self::varlena_1b_data_len(b0);
            if self.off + total > self.tuple.len() {
                bail!(FusionError::BufferTooSmall(self.tuple.len()));
            }
            let data = &self.tuple[self.off + 1..self.off + total];
            let v = decode_varlena_inline(meta.atttypid, data)?.unwrap_or(ScalarValue::Null);
            self.off += total;
            self.last_attno = att_idx + 1;
            Ok(v)
        } else {
            self.off = align_up(self.off, meta.attalign);
            if self.off + 4 > self.tuple.len() {
                bail!(FusionError::BufferTooSmall(self.tuple.len()));
            }
            let hdr_u32 =
                u32::from_ne_bytes(self.tuple[self.off..self.off + 4].try_into().unwrap());
            let total_len = Self::varlena_4b_total_len(hdr_u32);
            if total_len < 4 || self.off + total_len > self.tuple.len() {
                bail!(FusionError::BufferTooSmall(self.tuple.len()));
            }
            let is_compressed = Self::varlena_4b_is_compressed(hdr_u32);
            self.off += total_len;
            self.last_attno = att_idx + 1;
            if is_compressed {
                bail!(anyhow!("compressed varlena is not supported"));
            }
            let data = &self.tuple[self.off - total_len + 4..self.off];
            let v = decode_varlena_inline(meta.atttypid, data)?.unwrap_or(ScalarValue::Null);
            Ok(v)
        }
    }
}

impl<I: Iterator<Item = usize>> Iterator for DecodedIter<'_, I> {
    type Item = Result<ScalarValue>;
    fn next(&mut self) -> Option<Self::Item> {
        let att_idx = self.iter.next()?;
        if let Some(err) = self.pending_err.take() {
            return Some(Err(err));
        }
        Some(unsafe { self.decode_att(att_idx) })
    }
}

/// Create a decoder over a custom iterator of attribute indices (projection),
/// avoiding projection materialization.
///
/// # Safety
/// - `page_hdr` must point to a valid page header corresponding to the `tuple` buffer.
/// - `tuple` must contain a valid `HeapTupleHeaderData` and data area sized consistently with `attrs`.
/// - `attrs` must match the table schema for decoding fixed/varlena attributes.
/// - `indices` must yield in-range attribute positions.
pub unsafe fn decode_tuple_project<'bytes, I: Iterator<Item = usize>>(
    page_hdr: *const PageHeaderData,
    tuple: &'bytes [u8],
    attrs: &'bytes [PgAttrMeta],
    indices: I,
) -> Result<DecodedIter<'bytes, I>> {
    DecodedIter::init(page_hdr, tuple, attrs, indices)
}
