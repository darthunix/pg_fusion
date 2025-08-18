use anyhow::{bail, Result};
use common::FusionError;
use core::cmp::min;
use core::mem::size_of;
use core::ffi::c_void;
use core::ptr;
use core::slice;
use pg_sys::BLCKSZ;
use pgrx_pg_sys as pg_sys;
use pgrx_pg_sys::ItemIdData;
use pgrx_pg_sys::PageHeaderData;
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

    /// Returns tuple slices ordered by ascending `lp_off`.
    /// Builds a compact index of `(off, len)` pairs and sorts it once.
    ///
    /// Allocation: a single `Vec<(u16,u16)>` sized up to `lp_count()`.
    ///
    /// `filter` works the same as in `tuples()` and is applied while building
    /// the sorted index of line pointers.
    pub fn tuples_by_offset(
        &self,
        filter: Option<LpFilter>,
        ctx: *mut c_void,
    ) -> SortedTupleSliceIter<'bytes> {
        let cnt = self.lp_count();
        let mut pairs: Vec<(u16, u16)> = Vec::with_capacity(cnt);

        let hdr = self.header();
        let upper = hdr.pd_upper as usize;
        let special = hdr.pd_special as usize;
        let special_limit = core::cmp::min(special, self.data.len());

        unsafe {
            let mut cur = self.data.as_ptr().add(size_of::<PageHeaderData>()) as *const ItemIdData;
            let end = cur.add(cnt);
            while cur < end {
                let item = ptr::read(cur);
                cur = cur.add(1);
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
        }

        pairs.sort_unstable_by_key(|&(off, _)| off);

        SortedTupleSliceIter {
            page: self.data,
            pairs,
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
pub struct SortedTupleSliceIter<'a> {
    page: &'a [u8],
    pairs: Vec<(u16, u16)>,
    idx: usize,
}

impl<'a> Iterator for SortedTupleSliceIter<'a> {
    type Item = &'a [u8];

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
