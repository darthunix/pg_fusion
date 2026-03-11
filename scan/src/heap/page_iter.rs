use super::visibility::VisibilityMask;
use pgrx_pg_sys as pg_sys;

#[derive(Clone, Copy, Debug)]
pub(super) struct TupleRef<'a> {
    pub(super) bytes: &'a [u8],
}

pub(super) struct VisibleTupleIter<'page, 'vis> {
    page: &'page [u8],
    vis: Option<&'vis VisibilityMask<'vis>>,
    header_sz: usize,
    item_sz: usize,
    upper: usize,
    special_limit: usize,
    cnt: usize,
    pos: usize,
}

#[inline]
fn unpack_itemid(raw: u32) -> (u16, u8, u16) {
    if cfg!(target_endian = "little") {
        let off = (raw & 0x7FFF) as u16;
        let flags = ((raw >> 15) & 0x03) as u8;
        let len = ((raw >> 17) & 0x7FFF) as u16;
        (off, flags, len)
    } else {
        let off = ((raw >> 17) & 0x7FFF) as u16;
        let flags = ((raw >> 15) & 0x03) as u8;
        let len = (raw & 0x7FFF) as u16;
        (off, flags, len)
    }
}

pub(super) fn iter_visible_tuples<'page, 'vis>(
    page: &'page [u8],
    vis: Option<&'vis VisibilityMask<'vis>>,
) -> VisibleTupleIter<'page, 'vis> {
    let header_sz = core::mem::size_of::<pg_sys::PageHeaderData>();
    let item_sz = core::mem::size_of::<pg_sys::ItemIdData>();

    if page.len() < header_sz {
        return VisibleTupleIter {
            page,
            vis,
            header_sz,
            item_sz,
            upper: 0,
            special_limit: 0,
            cnt: 0,
            pos: 1,
        };
    }

    let hdr = unsafe { &*(page.as_ptr() as *const pg_sys::PageHeaderData) };
    let lower = hdr.pd_lower as usize;
    let upper = hdr.pd_upper as usize;
    let special_limit = core::cmp::min(hdr.pd_special as usize, page.len());
    let cnt = if lower >= header_sz {
        (lower - header_sz) / item_sz
    } else {
        0
    };

    VisibleTupleIter {
        page,
        vis,
        header_sz,
        item_sz,
        upper,
        special_limit,
        cnt,
        pos: 1,
    }
}

impl<'page> Iterator for VisibleTupleIter<'page, '_> {
    type Item = TupleRef<'page>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos <= self.cnt {
            let pos = self.pos;
            self.pos += 1;

            if let Some(mask) = self.vis {
                if !mask.is_visible(pos) {
                    continue;
                }
            }

            let raw_ptr = unsafe {
                self.page
                    .as_ptr()
                    .add(self.header_sz + (pos - 1) * self.item_sz)
            } as *const u32;
            let raw: u32 = unsafe { core::ptr::read(raw_ptr) };
            let (off_u16, flags_u8, len_u16) = unpack_itemid(raw);
            if (flags_u8 as u32) != pg_sys::LP_NORMAL {
                continue;
            }

            let off = off_u16 as usize;
            let len = len_u16 as usize;
            if len == 0 || off < self.upper {
                continue;
            }

            let end_off = match off.checked_add(len) {
                Some(v) => v,
                None => continue,
            };
            if end_off > self.special_limit {
                continue;
            }

            return Some(TupleRef {
                bytes: &self.page[off..end_off],
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{iter_visible_tuples, VisibilityMask};
    use pgrx_pg_sys as pg_sys;

    fn pack_itemid(off: u16, flags: u8, len: u16) -> u32 {
        if cfg!(target_endian = "little") {
            ((off as u32) & 0x7FFF)
                | (((flags as u32) & 0x03) << 15)
                | (((len as u32) & 0x7FFF) << 17)
        } else {
            (((off as u32) & 0x7FFF) << 17)
                | (((flags as u32) & 0x03) << 15)
                | ((len as u32) & 0x7FFF)
        }
    }

    fn one_tuple_page() -> Vec<u8> {
        let mut page = vec![0u8; pg_sys::BLCKSZ as usize];
        let header_sz = core::mem::size_of::<pg_sys::PageHeaderData>();
        let item_sz = core::mem::size_of::<pg_sys::ItemIdData>();
        let off = 256usize;
        let data = b"pgfusion";

        page[off..off + data.len()].copy_from_slice(data);

        let hdr = unsafe { &mut *(page.as_mut_ptr() as *mut pg_sys::PageHeaderData) };
        hdr.pd_lower = (header_sz + item_sz) as u16;
        hdr.pd_upper = 64;
        hdr.pd_special = page.len() as u16;

        let raw = pack_itemid(off as u16, pg_sys::LP_NORMAL as u8, data.len() as u16);
        let item_ptr = unsafe { page.as_mut_ptr().add(header_sz) as *mut u32 };
        unsafe { core::ptr::write(item_ptr, raw) };
        page
    }

    #[test]
    fn iterates_visible_tuple_without_mask() {
        let page = one_tuple_page();
        let mut it = iter_visible_tuples(&page, None);
        let tup = it.next().expect("tuple");
        assert_eq!(tup.bytes, b"pgfusion");
        assert!(it.next().is_none());
    }

    #[test]
    fn honors_visibility_mask() {
        let page = one_tuple_page();
        let hidden = VisibilityMask::new(&[0], 1);
        assert!(iter_visible_tuples(&page, Some(&hidden)).next().is_none());

        let shown = VisibilityMask::new(&[1], 1);
        assert!(iter_visible_tuples(&page, Some(&shown)).next().is_some());
    }

    #[test]
    fn drops_tuple_outside_special() {
        let mut page = one_tuple_page();
        let hdr = unsafe { &mut *(page.as_mut_ptr() as *mut pg_sys::PageHeaderData) };
        hdr.pd_special = 128;
        assert!(iter_visible_tuples(&page, None).next().is_none());
    }
}
