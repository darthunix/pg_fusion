#[derive(Clone, Copy, Debug)]
pub(super) struct VisibilityMask<'a> {
    bytes: &'a [u8],
    max_offsets: usize,
}

impl<'a> VisibilityMask<'a> {
    pub(super) fn new(bytes: &'a [u8], num_offsets: u16) -> Self {
        let max_offsets = usize::from(num_offsets);
        let required = max_offsets.div_ceil(8);
        let len = bytes.len().min(required);
        Self {
            bytes: &bytes[..len],
            max_offsets,
        }
    }

    #[inline]
    pub(super) fn is_visible(&self, lp_pos_1b: usize) -> bool {
        if lp_pos_1b == 0 || lp_pos_1b > self.max_offsets {
            return false;
        }
        let idx0 = lp_pos_1b - 1;
        let byte = idx0 / 8;
        if byte >= self.bytes.len() {
            return false;
        }
        let bit = 1u8 << ((idx0 % 8) as u8);
        (self.bytes[byte] & bit) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::VisibilityMask;

    #[test]
    fn visibility_mask_lsb_first_bits() {
        let mask = VisibilityMask::new(&[0b0000_0101], 8);
        assert!(mask.is_visible(1));
        assert!(!mask.is_visible(2));
        assert!(mask.is_visible(3));
        assert!(!mask.is_visible(4));
    }

    #[test]
    fn visibility_mask_bounds() {
        let mask = VisibilityMask::new(&[0xFF], 3);
        assert!(!mask.is_visible(0));
        assert!(mask.is_visible(1));
        assert!(mask.is_visible(3));
        assert!(!mask.is_visible(4));
    }

    #[test]
    fn visibility_mask_clamps_input_bytes() {
        let mask = VisibilityMask::new(&[0xFF, 0xFF], 1);
        assert!(mask.is_visible(1));
        assert!(!mask.is_visible(9));
    }
}
