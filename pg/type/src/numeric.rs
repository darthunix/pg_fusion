//! Direct decode of PostgreSQL `numeric` on-disk layout into Arrow Decimal128.
//!
//! This module is ported from PostgreSQL's `numeric.c` internals.
//! PostgreSQL-runtime free: callers detoast the datum and pass
//! the resulting varlena bytes; the decode here is pure and safe.

/// 4-byte varlena header size (`VARHDRSZ`).
const VARHDRSZ: usize = 4;
/// `sizeof(NumericDigit)` (int16).
const NUMERIC_DIGIT_BYTES: usize = 2;
/// Decimal digits packed into one base-NBASE `NumericDigit` (NBASE = 10000).
const DEC_DIGITS: i32 = 4;

const NUMERIC_SIGN_MASK: u16 = 0xC000;
const NUMERIC_SPECIAL: u16 = 0xC000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: i32 = 0x003F;

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
/// matching the previous `numeric_out` text path.
pub fn numeric_to_decimal128(
    varlena: &[u8],
    target_scale: i8,
    max_precision: u8,
) -> Result<i128, NumericDecodeError> {
    // The numeric header word sits right after the 4-byte varlena header.
    let Some(header_bytes) = varlena.get(VARHDRSZ..VARHDRSZ + 2) else {
        return Ok(0);
    };
    let n_header = u16::from_ne_bytes(header_bytes.try_into().unwrap());

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

#[cfg(test)]
mod tests {
    use super::{
        numeric_to_decimal128, NumericDecodeError, NUMERIC_SHORT, NUMERIC_SHORT_SIGN_MASK,
        NUMERIC_SHORT_WEIGHT_SIGN_MASK,
    };

    const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;

    // Builds a short-format `numeric` varlena (4-byte header, native byte order)
    // for `sum(digits[i] * 10000^(weight - i))` with the given sign/dscale.
    fn build_numeric_short(digits: &[i16], weight: i32, dscale: u16, negative: bool) -> Vec<u8> {
        let total = 6 + digits.len() * 2; // VARHDRSZ + n_header + digits
        let mut buf = vec![0u8; total];
        // 4-byte varlena header: little-endian length lives in the high 30 bits.
        buf[0..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
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
}
