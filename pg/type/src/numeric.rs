//! Direct conversion between PostgreSQL `numeric` on-disk layout and Arrow
//! Decimal128.
//!
//! This module is ported from PostgreSQL's `numeric.c` internals.
//! PostgreSQL-runtime free: callers detoast the datum and pass
//! the resulting varlena bytes, or copy encoded varlena bytes into PostgreSQL
//! memory; the conversion here is pure and safe.

use thiserror::Error;

/// 4-byte varlena header size (`VARHDRSZ`).
const VARHDRSZ: usize = 4;
/// `sizeof(NumericDigit)` (int16).
const NUMERIC_DIGIT_BYTES: usize = 2;
/// Decimal digits packed into one base-NBASE `NumericDigit` (NBASE = 10000).
const DEC_DIGITS: i32 = 4;
const NBASE: u32 = 10_000;

const NUMERIC_SIGN_MASK: u16 = 0xC000;
const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_SPECIAL: u16 = 0xC000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
const NUMERIC_SHORT_DSCALE_MAX: i32 = 0x1F80 >> NUMERIC_SHORT_DSCALE_SHIFT;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: i32 = 0x003F;
const NUMERIC_SHORT_WEIGHT_MAX: i32 = NUMERIC_SHORT_WEIGHT_MASK;
const NUMERIC_SHORT_WEIGHT_MIN: i32 = -(NUMERIC_SHORT_WEIGHT_MASK + 1);
const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
const NUMERIC_DSCALE_MAX: i32 = NUMERIC_DSCALE_MASK as i32;
const NUMERIC_HDRSZ: usize = VARHDRSZ + 4;
const NUMERIC_HDRSZ_SHORT: usize = VARHDRSZ + 2;
pub const NUMERIC_DECIMAL128_MAX_GROUPS: usize = 43;
pub const NUMERIC_DECIMAL128_MAX_VARLENA_BYTES: usize =
    NUMERIC_HDRSZ + NUMERIC_DECIMAL128_MAX_GROUPS * NUMERIC_DIGIT_BYTES;

#[cfg(target_endian = "little")]
fn varlena_4b_len_word(len: usize) -> Result<u32, NumericEncodeError> {
    u32::try_from(len)
        .ok()
        .and_then(|len| len.checked_shl(2))
        .ok_or(NumericEncodeError::OutOfRange)
}

#[cfg(target_endian = "big")]
fn varlena_4b_len_word(len: usize) -> Result<u32, NumericEncodeError> {
    u32::try_from(len).map_err(|_| NumericEncodeError::OutOfRange)
}

/// 10^n for n in 0..=38 (10^38 < i128::MAX; 10^39 overflows).
const POW10_I128: [i128; 39] = {
    let mut table = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

fn pow10_i128(exp: i32) -> Option<i128> {
    match exp {
        0..39 => Some(POW10_I128[exp as usize]),
        _ => None,
    }
}

/// Why a `numeric` value cannot be represented as the requested Decimal128.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericDecodeError {
    /// NaN / +Inf / -Inf, which have no Decimal128 representation.
    Special,
    /// The value does not fit the target precision/scale exactly (too many
    /// significant digits, or a non-zero digit below the target scale).
    OutOfRange,
}

/// Why a Decimal128 value cannot be encoded as PostgreSQL `numeric`.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum NumericEncodeError {
    /// The requested scale/weight cannot fit PostgreSQL's numeric header.
    #[error("Decimal128 value cannot be encoded as PostgreSQL numeric")]
    OutOfRange,
}

/// Stack-backed output buffer for Decimal128-to-PostgreSQL numeric encoding.
pub struct NumericVarlenaBuf {
    bytes: [u8; NUMERIC_DECIMAL128_MAX_VARLENA_BYTES],
    len: usize,
}

impl NumericVarlenaBuf {
    pub const fn new() -> Self {
        Self {
            bytes: [0; NUMERIC_DECIMAL128_MAX_VARLENA_BYTES],
            len: 0,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl Default for NumericVarlenaBuf {
    fn default() -> Self {
        Self::new()
    }
}

fn numeric_header(varlena: &[u8]) -> Option<u16> {
    let header_bytes = varlena.get(VARHDRSZ..VARHDRSZ + 2)?;
    Some(u16::from_ne_bytes(header_bytes.try_into().unwrap()))
}

/// Reads the PostgreSQL display scale (`dscale`) from a finite `numeric`.
///
/// `varlena` must be the full detoasted numeric varlena including its 4-byte
/// header. Special numeric values return [`NumericDecodeError::Special`].
pub fn numeric_display_scale(varlena: &[u8]) -> Result<i32, NumericDecodeError> {
    let Some(n_header) = numeric_header(varlena) else {
        return Ok(0);
    };
    if (n_header & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL {
        return Err(NumericDecodeError::Special);
    }
    if (n_header & NUMERIC_SHORT) != 0 {
        Ok(i32::from((n_header & 0x1F80) >> NUMERIC_SHORT_DSCALE_SHIFT))
    } else {
        Ok(i32::from(n_header & NUMERIC_DSCALE_MASK))
    }
}

/// Decodes a detoasted PostgreSQL `numeric` varlena into an Arrow `Decimal128`
/// unscaled integer at `target_scale`.
///
/// `varlena` must be the full numeric varlena including its 4-byte header (i.e.
/// `&detoasted[..VARSIZE]`). The value equals
/// `sign * sum(digits[i] * NBASE^(weight - i))`, so each base-NBASE digit lands
/// at decimal exponent `DEC_DIGITS * (weight - i) + target_scale` in the target
/// unscaled integer. Digits that fall entirely below the target scale must be
/// zero (exact conversion); the magnitude must fit `max_precision` significant
/// digits. Both conditions otherwise yield [`NumericDecodeError::OutOfRange`],
/// matching the previous decimal text path.
pub fn numeric_to_decimal128(
    varlena: &[u8],
    target_scale: i8,
    max_precision: u8,
) -> Result<i128, NumericDecodeError> {
    // The numeric header word sits right after the 4-byte varlena header.
    let Some(n_header) = numeric_header(varlena) else {
        return Ok(0);
    };

    if (n_header & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL {
        // NaN / +Inf / -Inf.
        return Err(NumericDecodeError::Special);
    }

    let is_short = (n_header & NUMERIC_SHORT) != 0;
    let header_size = if is_short { VARHDRSZ + 2 } else { VARHDRSZ + 4 };
    if varlena.len() < header_size {
        return Ok(0);
    }
    let ndigits = (varlena.len() - header_size) / NUMERIC_DIGIT_BYTES;

    let (weight, negative) = if is_short {
        let raw = i32::from(n_header) & NUMERIC_SHORT_WEIGHT_MASK;
        // 7-bit signed weight: sign-extend through the reserved mask bits.
        let weight = if (n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK) != 0 {
            raw | !NUMERIC_SHORT_WEIGHT_MASK
        } else {
            raw
        };
        (weight, (n_header & NUMERIC_SHORT_SIGN_MASK) != 0)
    } else {
        let weight_bytes = &varlena[VARHDRSZ + 2..VARHDRSZ + 4];
        let weight = i32::from(i16::from_ne_bytes(weight_bytes.try_into().unwrap()));
        (weight, (n_header & NUMERIC_SIGN_MASK) == NUMERIC_NEG)
    };

    if ndigits == 0 {
        return Ok(0);
    }
    let scale = i32::from(target_scale);

    let mut result: i128 = 0;
    for i in 0..ndigits {
        let off = header_size + i * NUMERIC_DIGIT_BYTES;
        let digit = i128::from(i16::from_ne_bytes(
            varlena[off..off + 2].try_into().unwrap(),
        ));
        // Exponent of this digit's least-significant decimal place in the
        // target unscaled integer.
        let exp = DEC_DIGITS * (weight - i as i32) + scale;
        if exp >= 0 {
            if digit != 0 {
                let scaled = digit
                    .checked_mul(pow10_i128(exp).ok_or(NumericDecodeError::OutOfRange)?)
                    .ok_or(NumericDecodeError::OutOfRange)?;
                result = result
                    .checked_add(scaled)
                    .ok_or(NumericDecodeError::OutOfRange)?;
            }
        } else if exp <= -DEC_DIGITS {
            // Entire digit is below the target scale; must be zero to be exact.
            if digit != 0 {
                return Err(NumericDecodeError::OutOfRange);
            }
        } else {
            // Digit straddles the target-scale boundary: the low (-exp) decimal
            // places must be zero; the rest contribute at exponent 0.
            let div = POW10_I128[(-exp) as usize];
            if digit % div != 0 {
                return Err(NumericDecodeError::OutOfRange);
            }
            result = result
                .checked_add(digit / div)
                .ok_or(NumericDecodeError::OutOfRange)?;
        }
    }

    // Enforce the column precision (significant digit count).
    if result >= pow10_i128(i32::from(max_precision)).ok_or(NumericDecodeError::OutOfRange)? {
        return Err(NumericDecodeError::OutOfRange);
    }
    Ok(if negative { -result } else { result })
}

/// Encodes an Arrow Decimal128 unscaled integer as PostgreSQL `numeric`
/// varlena bytes.
///
/// `scale` is the Arrow Decimal128 scale: the represented value is
/// `value * 10^-scale`. When `trim_trailing_zeros` is true, insignificant
/// fractional decimal zeroes are removed from PostgreSQL's display scale,
/// matching pg_fusion's typmodless `numeric` result policy.
pub fn decimal128_to_numeric_varlena(
    value: i128,
    scale: i8,
    trim_trailing_zeros: bool,
    out: &mut NumericVarlenaBuf,
) -> Result<&[u8], NumericEncodeError> {
    out.len = 0;
    let negative = value.is_negative();
    let mut magnitude = value.unsigned_abs();
    let mut dscale = i32::from(scale).max(0);

    if trim_trailing_zeros && dscale > 0 {
        if magnitude == 0 {
            dscale = 0;
        } else {
            while dscale > 0 && magnitude.is_multiple_of(10) {
                magnitude /= 10;
                dscale -= 1;
            }
        }
    }
    if dscale > NUMERIC_DSCALE_MAX {
        return Err(NumericEncodeError::OutOfRange);
    }

    let mut groups = [0u16; NUMERIC_DECIMAL128_MAX_GROUPS];
    let mut groups_len = base10000_groups_le(magnitude, &mut groups)?;
    let right_groups = if scale >= 0 {
        let right_groups = (dscale + DEC_DIGITS - 1) / DEC_DIGITS;
        let pad = right_groups * DEC_DIGITS - dscale;
        multiply_base10000_groups_by_pow10(&mut groups, &mut groups_len, pad)?;
        right_groups
    } else {
        let decimal_shift = -i32::from(scale);
        multiply_base10000_groups_by_pow10(
            &mut groups,
            &mut groups_len,
            decimal_shift % DEC_DIGITS,
        )?;
        prepend_zero_groups(
            &mut groups,
            &mut groups_len,
            (decimal_shift / DEC_DIGITS) as usize,
        )?;
        0
    };

    let mut weight = groups_len as i32 - right_groups - 1;
    let mut start = 0usize;
    while start < groups_len && groups[start] == 0 {
        start += 1;
    }
    let mut end = groups_len;
    while end > start && groups[end - 1] == 0 {
        end -= 1;
        weight -= 1;
    }
    if start == end {
        weight = 0;
    }

    build_numeric_varlena(
        &groups[start..end],
        weight,
        dscale,
        negative && start != end,
        out,
    )
}

fn base10000_groups_le(
    mut magnitude: u128,
    groups: &mut [u16; NUMERIC_DECIMAL128_MAX_GROUPS],
) -> Result<usize, NumericEncodeError> {
    let mut len = 0usize;
    while magnitude != 0 {
        if len == groups.len() {
            return Err(NumericEncodeError::OutOfRange);
        }
        groups[len] = (magnitude % u128::from(NBASE)) as u16;
        len += 1;
        magnitude /= u128::from(NBASE);
    }
    Ok(len)
}

fn multiply_base10000_groups_by_pow10(
    groups: &mut [u16; NUMERIC_DECIMAL128_MAX_GROUPS],
    len: &mut usize,
    exp: i32,
) -> Result<(), NumericEncodeError> {
    let factor = match exp {
        0 => return Ok(()),
        1 => 10u32,
        2 => 100u32,
        3 => 1_000u32,
        _ => return Err(NumericEncodeError::OutOfRange),
    };
    let mut carry = 0u32;
    for group in groups.iter_mut().take(*len) {
        let value = u32::from(*group) * factor + carry;
        *group = (value % NBASE) as u16;
        carry = value / NBASE;
    }
    if carry != 0 {
        if *len == groups.len() {
            return Err(NumericEncodeError::OutOfRange);
        }
        groups[*len] = carry as u16;
        *len += 1;
    }
    Ok(())
}

fn prepend_zero_groups(
    groups: &mut [u16; NUMERIC_DECIMAL128_MAX_GROUPS],
    len: &mut usize,
    count: usize,
) -> Result<(), NumericEncodeError> {
    if count == 0 || *len == 0 {
        return Ok(());
    }
    if *len + count > groups.len() {
        return Err(NumericEncodeError::OutOfRange);
    }
    groups.copy_within(0..*len, count);
    groups[..count].fill(0);
    *len += count;
    Ok(())
}

fn build_numeric_varlena<'a>(
    digits_le: &[u16],
    weight: i32,
    dscale: i32,
    negative: bool,
    out: &'a mut NumericVarlenaBuf,
) -> Result<&'a [u8], NumericEncodeError> {
    if !(0..=NUMERIC_DSCALE_MAX).contains(&dscale) {
        return Err(NumericEncodeError::OutOfRange);
    }
    let can_short = dscale <= NUMERIC_SHORT_DSCALE_MAX
        && (NUMERIC_SHORT_WEIGHT_MIN..=NUMERIC_SHORT_WEIGHT_MAX).contains(&weight);
    if !can_short && i16::try_from(weight).is_err() {
        return Err(NumericEncodeError::OutOfRange);
    }

    let header_size = if can_short {
        NUMERIC_HDRSZ_SHORT
    } else {
        NUMERIC_HDRSZ
    };
    let len = header_size + digits_le.len() * NUMERIC_DIGIT_BYTES;
    if len > out.bytes.len() {
        return Err(NumericEncodeError::OutOfRange);
    }
    let len_word = varlena_4b_len_word(len)?;
    out.len = len;
    {
        let buf = &mut out.bytes[..len];
        buf[0..VARHDRSZ].copy_from_slice(&len_word.to_ne_bytes());

        if can_short {
            let mut header = NUMERIC_SHORT
                | ((dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT)
                | ((weight as u16) & NUMERIC_SHORT_WEIGHT_MASK as u16);
            if negative {
                header |= NUMERIC_SHORT_SIGN_MASK;
            }
            if weight < 0 {
                header |= NUMERIC_SHORT_WEIGHT_SIGN_MASK;
            }
            buf[VARHDRSZ..VARHDRSZ + 2].copy_from_slice(&header.to_ne_bytes());
        } else {
            let sign_dscale = (if negative { NUMERIC_NEG } else { NUMERIC_POS })
                | ((dscale as u16) & NUMERIC_DSCALE_MASK);
            buf[VARHDRSZ..VARHDRSZ + 2].copy_from_slice(&sign_dscale.to_ne_bytes());
            buf[VARHDRSZ + 2..VARHDRSZ + 4].copy_from_slice(&(weight as i16).to_ne_bytes());
        }

        let mut offset = header_size;
        for digit in digits_le.iter().rev() {
            buf[offset..offset + NUMERIC_DIGIT_BYTES].copy_from_slice(&digit.to_ne_bytes());
            offset += NUMERIC_DIGIT_BYTES;
        }
    }
    Ok(out.as_slice())
}

#[cfg(test)]
mod tests {
    use super::{
        decimal128_to_numeric_varlena, numeric_to_decimal128, NumericDecodeError,
        NumericVarlenaBuf, NUMERIC_DECIMAL128_MAX_VARLENA_BYTES, NUMERIC_DSCALE_MASK, NUMERIC_NEG,
        NUMERIC_POS, NUMERIC_SHORT, NUMERIC_SHORT_DSCALE_SHIFT, NUMERIC_SHORT_SIGN_MASK,
        NUMERIC_SHORT_WEIGHT_MASK, NUMERIC_SHORT_WEIGHT_SIGN_MASK, VARHDRSZ,
    };

    // Builds a short-format `numeric` varlena (4-byte header, native byte order)
    // for `sum(digits[i] * 10000^(weight - i))` with the given sign/dscale.
    fn build_numeric_short(digits: &[i16], weight: i32, dscale: u16, negative: bool) -> Vec<u8> {
        let total = 6 + digits.len() * 2; // VARHDRSZ + n_header + digits
        let mut buf = vec![0u8; total];
        // 4-byte varlena header uses PostgreSQL's endian-specific packing.
        buf[0..4].copy_from_slice(&super::varlena_4b_len_word(total).unwrap().to_ne_bytes());
        let mut n_header = NUMERIC_SHORT | ((dscale & 0x3F) << NUMERIC_SHORT_DSCALE_SHIFT);
        if negative {
            n_header |= NUMERIC_SHORT_SIGN_MASK;
        }
        n_header |= (weight as u16) & 0x003F;
        if weight < 0 {
            n_header |= NUMERIC_SHORT_WEIGHT_SIGN_MASK;
        }
        buf[4..6].copy_from_slice(&n_header.to_ne_bytes());
        for (i, digit) in digits.iter().enumerate() {
            let off = 6 + i * 2;
            buf[off..off + 2].copy_from_slice(&digit.to_ne_bytes());
        }
        buf
    }

    fn decode(
        digits: &[i16],
        weight: i32,
        dscale: u16,
        negative: bool,
        precision: u8,
        scale: i8,
    ) -> i128 {
        let buf = build_numeric_short(digits, weight, dscale, negative);
        numeric_to_decimal128(&buf, scale, precision).expect("decode decimal")
    }

    fn try_decode(
        digits: &[i16],
        weight: i32,
        dscale: u16,
        negative: bool,
        precision: u8,
        scale: i8,
    ) -> Result<i128, NumericDecodeError> {
        let buf = build_numeric_short(digits, weight, dscale, negative);
        numeric_to_decimal128(&buf, scale, precision)
    }

    fn encoded_header(buf: &[u8]) -> u16 {
        u16::from_ne_bytes(buf[VARHDRSZ..VARHDRSZ + 2].try_into().unwrap())
    }

    fn encoded_is_short(buf: &[u8]) -> bool {
        encoded_header(buf) & NUMERIC_SHORT != 0
    }

    fn encoded_dscale(buf: &[u8]) -> u16 {
        let header = encoded_header(buf);
        if encoded_is_short(buf) {
            (header & 0x1F80) >> NUMERIC_SHORT_DSCALE_SHIFT
        } else {
            header & NUMERIC_DSCALE_MASK
        }
    }

    fn encoded_weight(buf: &[u8]) -> i32 {
        let header = encoded_header(buf);
        if encoded_is_short(buf) {
            let raw = i32::from(header) & NUMERIC_SHORT_WEIGHT_MASK;
            if header & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
                raw | !NUMERIC_SHORT_WEIGHT_MASK
            } else {
                raw
            }
        } else {
            i32::from(i16::from_ne_bytes(
                buf[VARHDRSZ + 2..VARHDRSZ + 4].try_into().unwrap(),
            ))
        }
    }

    fn encoded_digits(buf: &[u8]) -> Vec<u16> {
        let header_size = if encoded_is_short(buf) {
            VARHDRSZ + 2
        } else {
            VARHDRSZ + 4
        };
        buf[header_size..]
            .chunks_exact(2)
            .map(|chunk| u16::from_ne_bytes(chunk.try_into().unwrap()))
            .collect()
    }

    fn encode(value: i128, scale: i8, trim_trailing_zeros: bool) -> Vec<u8> {
        let mut out = NumericVarlenaBuf::new();
        decimal128_to_numeric_varlena(value, scale, trim_trailing_zeros, &mut out)
            .expect("encode decimal");
        out.as_slice().to_vec()
    }

    #[test]
    #[cfg(target_endian = "little")]
    fn varlena_4b_len_word_uses_little_endian_packing() {
        assert_eq!(super::varlena_4b_len_word(42).unwrap(), 42 << 2);
    }

    #[test]
    #[cfg(target_endian = "big")]
    fn varlena_4b_len_word_uses_big_endian_packing() {
        assert_eq!(super::varlena_4b_len_word(42).unwrap(), 42);
    }

    #[test]
    fn reads_numeric_display_scale() {
        let buf = build_numeric_short(&[1200], 0, 4, false);
        assert_eq!(super::numeric_display_scale(&buf).unwrap(), 4);
    }

    #[test]
    fn scales_finite_numeric() {
        // 123.45 -> digits [123, 4500] (.4500), weight 0, dscale 2.
        assert_eq!(decode(&[123, 4500], 0, 2, false, 10, 2), 12345);
        assert_eq!(
            decode(&[123, 4500], 0, 2, false, 38, 16),
            1234500000000000000
        );
        // -123.40
        assert_eq!(decode(&[123, 4000], 0, 2, true, 10, 2), -12340);
        // 0.0001 -> single digit 1 at weight -1, padded to scale 6.
        assert_eq!(decode(&[1], -1, 4, false, 10, 6), 100);
        // integer 42 scaled up to scale 3.
        assert_eq!(decode(&[42], 0, 0, false, 10, 3), 42000);
        // 1.23 (digit .2300 straddles the scale-2 boundary, low places zero).
        assert_eq!(decode(&[1, 2300], 0, 2, false, 10, 2), 123);
        // zero (no digits).
        assert_eq!(decode(&[], 0, 0, false, 10, 2), 0);
    }

    #[test]
    fn rejects_out_of_range() {
        // 1.234 at target scale 2: the sub-scale digit (.234) is non-zero.
        assert_eq!(
            try_decode(&[1, 2340], 0, 3, false, 10, 2),
            Err(NumericDecodeError::OutOfRange)
        );
        // 100000 in NUMERIC(5,0): exceeds precision (6 significant digits).
        assert_eq!(
            try_decode(&[10], 1, 0, false, 5, 0),
            Err(NumericDecodeError::OutOfRange)
        );
    }

    #[test]
    fn encodes_decimal128_scaled_values() {
        let buf = encode(12345, 2, false);
        assert!(encoded_is_short(&buf));
        assert_eq!(encoded_dscale(&buf), 2);
        assert_eq!(encoded_weight(&buf), 0);
        assert_eq!(encoded_digits(&buf), vec![123, 4500]);
        assert_eq!(numeric_to_decimal128(&buf, 2, 10).unwrap(), 12345);

        let buf = encode(-50, 2, false);
        assert_eq!(encoded_dscale(&buf), 2);
        assert_eq!(numeric_to_decimal128(&buf, 2, 10).unwrap(), -50);
    }

    #[test]
    fn encodes_decimal128_with_trimmed_bare_numeric_scale() {
        let buf = encode(1230000, 6, true);
        assert_eq!(encoded_dscale(&buf), 2);
        assert_eq!(encoded_digits(&buf), vec![1, 2300]);
        assert_eq!(numeric_to_decimal128(&buf, 6, 38).unwrap(), 1230000);

        let buf = encode(1000000, 6, true);
        assert_eq!(encoded_dscale(&buf), 0);
        assert_eq!(encoded_digits(&buf), vec![1]);
        assert_eq!(numeric_to_decimal128(&buf, 6, 38).unwrap(), 1000000);
    }

    #[test]
    fn encodes_decimal128_zero_with_requested_display_scale() {
        let buf = encode(0, 6, false);
        assert_eq!(encoded_dscale(&buf), 6);
        assert_eq!(encoded_digits(&buf), Vec::<u16>::new());
        assert_eq!(numeric_to_decimal128(&buf, 6, 38).unwrap(), 0);

        let buf = encode(0, 6, true);
        assert_eq!(encoded_dscale(&buf), 0);
        assert_eq!(encoded_digits(&buf), Vec::<u16>::new());
        assert_eq!(numeric_to_decimal128(&buf, 6, 38).unwrap(), 0);
    }

    #[test]
    fn encodes_decimal128_negative_scale() {
        let buf = encode(42, -2, false);
        assert_eq!(encoded_dscale(&buf), 0);
        assert_eq!(encoded_digits(&buf), vec![4200]);
        assert_eq!(numeric_to_decimal128(&buf, -2, 38).unwrap(), 42);
        assert_eq!(numeric_to_decimal128(&buf, 0, 38).unwrap(), 4200);
    }

    #[test]
    fn encodes_decimal128_long_header_when_display_scale_requires_it() {
        let buf = encode(1, 70, false);
        assert!(!encoded_is_short(&buf));
        assert_eq!(encoded_dscale(&buf), 70);
        assert_eq!(encoded_header(&buf) & 0xC000, NUMERIC_POS);
        assert_eq!(numeric_to_decimal128(&buf, 70, 38).unwrap(), 1);

        let buf = encode(-1, 70, false);
        assert!(!encoded_is_short(&buf));
        assert_eq!(encoded_header(&buf) & 0xC000, NUMERIC_NEG);
        assert_eq!(numeric_to_decimal128(&buf, 70, 38).unwrap(), -1);
    }

    #[test]
    fn decimal128_encoder_buffer_covers_extreme_scales() {
        let mut out = NumericVarlenaBuf::new();
        decimal128_to_numeric_varlena(i128::MIN, -128, false, &mut out).unwrap();
        assert!(out.as_slice().len() <= NUMERIC_DECIMAL128_MAX_VARLENA_BYTES);
        assert_eq!(encoded_dscale(out.as_slice()), 0);

        let max_precision_38 = super::POW10_I128[38] - 1;
        decimal128_to_numeric_varlena(-max_precision_38, -128, false, &mut out).unwrap();
        assert!(out.as_slice().len() <= NUMERIC_DECIMAL128_MAX_VARLENA_BYTES);
        assert_eq!(
            numeric_to_decimal128(out.as_slice(), -128, 38).unwrap(),
            -max_precision_38
        );

        decimal128_to_numeric_varlena(1, 127, false, &mut out).unwrap();
        assert!(out.as_slice().len() <= NUMERIC_DECIMAL128_MAX_VARLENA_BYTES);
        assert!(!encoded_is_short(out.as_slice()));
        assert_eq!(encoded_dscale(out.as_slice()), 127);
        assert_eq!(numeric_to_decimal128(out.as_slice(), 127, 38).unwrap(), 1);
    }
}
