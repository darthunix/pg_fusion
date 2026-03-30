//! Public bitmap helpers used by producer- and consumer-side code.

/// Returns the number of bytes needed to store a bitmap for `row_count` rows.
pub fn bitmap_bytes(row_count: u32) -> u32 {
    row_count.div_ceil(8)
}

/// Returns the bit value at `index` from a packed bitmap.
pub fn bitmap_get(bytes: &[u8], index: u32) -> bool {
    let byte_index = usize::try_from(index / 8).expect("u32 fits into usize");
    let bit_index = index % 8;
    bytes
        .get(byte_index)
        .map(|byte| (byte & (1 << bit_index)) != 0)
        .unwrap_or(false)
}

/// Sets or clears the bit at `index` in a packed bitmap.
pub fn bitmap_set(bytes: &mut [u8], index: u32, value: bool) {
    let byte_index = usize::try_from(index / 8).expect("u32 fits into usize");
    let bit_index = index % 8;
    if let Some(byte) = bytes.get_mut(byte_index) {
        if value {
            *byte |= 1 << bit_index;
        } else {
            *byte &= !(1 << bit_index);
        }
    }
}
