pub(crate) unsafe fn bitmap_get_raw(base: *const u8, index: u32) -> bool {
    let byte = unsafe { *base.add((index / 8) as usize) };
    let bit = 1u8 << (index % 8);
    (byte & bit) != 0
}

pub(crate) unsafe fn bitmap_set_raw(base: *mut u8, index: u32, value: bool) {
    let byte = unsafe { &mut *base.add((index / 8) as usize) };
    let bit = 1u8 << (index % 8);
    if value {
        *byte |= bit;
    } else {
        *byte &= !bit;
    }
}
