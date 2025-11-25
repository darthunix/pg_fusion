use crate::{write_header, ControlPacket, Direction, Flag, Header, Tape};
use anyhow::{bail, Result};
use rmp::decode::{read_array_len, read_i16, read_i32, read_u32, read_u8};
use rmp::encode::{write_array_len, write_i16, write_i32, write_u32, write_u8};
use std::io::Read;

/// Wire description of a PostgreSQL attribute layout sufficient to build HeapTuple on the executor side.
#[derive(Clone, Debug, Default)]
pub struct PgAttrWire {
    pub atttypid: u32,
    pub typmod: i32,
    pub attlen: i16,
    /// Alignment in bytes: 1,2,4,8 (mapped from typalign 'c','s','i','d').
    pub attalign: u8,
    pub attbyval: bool,
    pub nullable: bool,
}

#[inline]
fn write_bool(stream: &mut impl std::io::Write, v: bool) -> Result<()> {
    rmp::encode::write_bool(stream, v)?;
    Ok(())
}

#[inline]
fn read_bool(stream: &mut impl Read) -> Result<bool> {
    Ok(rmp::decode::read_bool(stream)?)
}

/// Send per-attribute layout information from backend to executor.
pub fn prepare_column_layout(stream: &mut impl Tape, attrs: &[PgAttrWire]) -> Result<()> {
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    write_array_len(stream, u32::try_from(attrs.len())?)?;
    for a in attrs {
        write_u32(stream, a.atttypid)?;
        write_i32(stream, a.typmod)?;
        write_i16(stream, a.attlen)?;
        write_u8(stream, a.attalign)?;
        write_bool(stream, a.attbyval)?;
        write_bool(stream, a.nullable)?;
    }
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToServer,
        tag: ControlPacket::ColumnLayout as u8,
        length,
        flag: Flag::Last,
    };
    stream.rollback();
    write_header(stream, &header)?;
    stream.fast_forward(length as u32)?;
    debug_assert_eq!(stream.uncommitted_len(), len_final);
    stream.flush()?;
    debug_assert_eq!(stream.len(), len_final);
    debug_assert_eq!(stream.uncommitted_len(), 0);
    Ok(())
}

/// Read per-attribute layout information on the executor side.
pub fn consume_column_layout(stream: &mut impl Read) -> Result<Vec<PgAttrWire>> {
    let n = read_array_len(stream)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let atttypid = read_u32(stream)?;
        let typmod = read_i32(stream)?;
        let attlen = read_i16(stream)?;
        let attalign = read_u8(stream)?;
        let attbyval = read_bool(stream)?;
        let nullable = read_bool(stream)?;
        out.push(PgAttrWire {
            atttypid,
            typmod,
            attlen,
            attalign,
            attbyval,
            nullable,
        });
    }
    Ok(out)
}

/// A simplified, protocol-level field representation used to build a wire tuple.
/// The actual on-wire layout aligns and packs values based on `PgAttrWire` metadata.
#[derive(Clone, Debug)]
pub enum Field<'a> {
    Null,
    ByVal1([u8; 1]),
    ByVal2([u8; 2]),
    ByVal4([u8; 4]),
    ByVal8([u8; 8]),
    /// Already-encoded datum bytes (e.g., varlena with header) — copied as-is.
    ByRef(&'a [u8]),
}

#[inline]
const fn max_align(off: usize, align: u8) -> usize {
    let a = align as usize;
    off.div_ceil(a) * a
}

/// WireMinimalTuple header (protocol-specific), designed to be cheaply converted
/// into a PostgreSQL MinimalTuple by the backend.
/// Layout:
/// - u16: nattrs
/// - u8: flags (bit0 = has_nulls)
/// - u8: hoff (aligned offset where data area begins)
/// - u16: bitmap_bytes
/// - u32: data_bytes
///   Followed by:
/// - [bitmap_bytes] optional null bitmap (LSB-first per attribute)
/// - [data_bytes] attribute payload area with proper alignment per `PgAttrWire.attalign`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireHeader {
    pub nattrs: u16,
    pub flags: u8,
    pub hoff: u8,
    pub bitmap_bytes: u16,
    pub data_bytes: u32,
}

#[inline]
fn write_u16_le(w: &mut impl std::io::Write, v: u16) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

#[inline]
fn write_u32_le(w: &mut impl std::io::Write, v: u32) -> Result<()> {
    w.write_all(&v.to_le_bytes())?;
    Ok(())
}

#[inline]
fn read_u16_le(r: &mut impl Read) -> Result<u16> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b)?;
    Ok(u16::from_le_bytes(b))
}

#[inline]
fn read_u32_le(r: &mut impl Read) -> Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

/// Compute data area size for provided fields and attribute layout.
fn compute_data_len(attrs: &[PgAttrWire], fields: &[Field<'_>], hoff: usize) -> Result<usize> {
    if attrs.len() != fields.len() {
        bail!(
            "attrs/fields length mismatch: {} vs {}",
            attrs.len(),
            fields.len()
        );
    }
    let mut off = hoff;
    for (a, f) in attrs.iter().zip(fields.iter()) {
        // Align to attalign boundary
        off = max_align(off, a.attalign);
        match (a.attlen, f) {
            (_, Field::Null) => {}
            (1, Field::ByVal1(_)) => off += 1,
            (2, Field::ByVal2(_)) => off += 2,
            (4, Field::ByVal4(_)) => off += 4,
            (8, Field::ByVal8(_)) => off += 8,
            // For varlena, encode length prefix (u32 LE) followed by bytes
            (-1, Field::ByRef(bytes)) => off += 4 + bytes.len(),
            (len, Field::ByRef(bytes)) if len > 0 => off += bytes.len(),
            _ => bail!(
                "unsupported field/attlen combination for atttypid {}",
                a.atttypid
            ),
        }
    }
    Ok(off - hoff)
}

/// Encode a tuple into protocol-specific wire format.
/// Backend will read this and construct a MinimalTuple or HeapTuple in palloc'd memory.
pub fn encode_wire_tuple(
    mut out: &mut impl std::io::Write,
    attrs: &[PgAttrWire],
    fields: &[Field<'_>],
) -> Result<()> {
    let nattrs = u16::try_from(attrs.len())?;
    let has_nulls = fields.iter().any(|f| matches!(f, Field::Null));
    let bitmap_bytes: usize = if has_nulls {
        attrs.len().div_ceil(8)
    } else {
        0
    };
    // Header is 2 + 1 + 1 + 2 + 4 = 10 bytes; data hoff aligned to MAXALIGN(10 + bitmap)
    let hdr_bytes = 10usize;
    // PG MAXALIGN is 8 on most platforms; align header+bitmap to 8.
    let hoff = max_align(hdr_bytes + bitmap_bytes, 8);
    let data_len = compute_data_len(attrs, fields, hoff)?;

    // Write header
    write_u16_le(&mut out, nattrs)?;
    out.write_all(&[if has_nulls { 1 } else { 0 }])?;
    out.write_all(&[u8::try_from(hoff).map_err(|_| anyhow::anyhow!("hoff too large"))?])?;
    write_u16_le(&mut out, u16::try_from(bitmap_bytes)?)?;
    write_u32_le(&mut out, u32::try_from(data_len)?)?;

    // Write bitmap (if any)
    if has_nulls {
        let mut bits = vec![0u8; bitmap_bytes];
        for (i, f) in fields.iter().enumerate() {
            if matches!(f, Field::Null) {
                let byte = i / 8;
                let bit = i % 8;
                bits[byte] |= 1u8 << bit;
            }
        }
        out.write_all(&bits)?;
    }
    // Pad to hoff
    let written = hdr_bytes + bitmap_bytes;
    if hoff > written {
        let pad = vec![0u8; hoff - written];
        out.write_all(&pad)?;
    }
    // Write data area
    let mut off = hoff;
    for (a, f) in attrs.iter().zip(fields.iter()) {
        let aligned = max_align(off, a.attalign);
        if aligned > off {
            let pad = vec![0u8; aligned - off];
            out.write_all(&pad)?;
            off = aligned;
        }
        match f {
            Field::Null => {}
            Field::ByVal1(bytes) => {
                out.write_all(bytes)?;
                off += 1;
            }
            Field::ByVal2(bytes) => {
                out.write_all(bytes)?;
                off += 2;
            }
            Field::ByVal4(bytes) => {
                out.write_all(bytes)?;
                off += 4;
            }
            Field::ByVal8(bytes) => {
                out.write_all(bytes)?;
                off += 8;
            }
            Field::ByRef(bytes) => {
                if a.attlen == -1 {
                    // varlena: write length prefix (u32 LE), then bytes
                    write_u32_le(&mut out, u32::try_from(bytes.len())?)?;
                    out.write_all(bytes)?;
                    off += 4 + bytes.len();
                } else {
                    out.write_all(bytes)?;
                    off += bytes.len();
                }
            }
        }
    }
    debug_assert_eq!(off, hoff + data_len);
    Ok(())
}

/// Decode protocol-level wire tuple header and return slices into the bitmap and data.
pub fn decode_wire_tuple(mut input: &[u8]) -> Result<(WireHeader, &[u8], &[u8])> {
    let nattrs = read_u16_le(&mut input)?;
    let mut flag = [0u8; 1];
    input.read_exact(&mut flag)?;
    let flags = flag[0];
    let mut b = [0u8; 1];
    input.read_exact(&mut b)?;
    let hoff = b[0];
    let bitmap_bytes = read_u16_le(&mut input)?;
    let data_bytes = read_u32_le(&mut input)?;
    let header = WireHeader {
        nattrs,
        flags,
        hoff,
        bitmap_bytes,
        data_bytes,
    };
    let has_nulls = (flags & 0x01) != 0;
    let bitmap = if has_nulls {
        let (bm, rest) = input.split_at(bitmap_bytes as usize);
        input = rest;
        bm
    } else {
        &[]
    };
    // Skip to hoff (padding)
    let hdr_bytes = 10usize;
    let written = hdr_bytes + (bitmap_bytes as usize);
    if (hoff as usize) < written {
        bail!("invalid header: hoff < header+bitmap");
    }
    let pad = (hoff as usize) - written;
    if pad > 0 {
        // Advance without allocating
        let (_, rest) = input.split_at(pad);
        input = rest;
    }
    let (data, _rest) = input.split_at(data_bytes as usize);
    Ok((header, bitmap, data))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs_int4_text() -> Vec<PgAttrWire> {
        vec![
            PgAttrWire {
                atttypid: 23, // INT4OID
                typmod: -1,
                attlen: 4,
                attalign: 4,
                attbyval: true,
                nullable: false,
            },
            PgAttrWire {
                atttypid: 25, // TEXTOID (varlena)
                typmod: -1,
                attlen: -1,
                attalign: 4,
                attbyval: false,
                nullable: true,
            },
        ]
    }

    #[test]
    fn test_encode_decode_no_nulls() -> Result<()> {
        let attrs = vec![PgAttrWire {
            atttypid: 23,
            typmod: -1,
            attlen: 4,
            attalign: 4,
            attbyval: true,
            nullable: false,
        }];
        let mut out = Vec::new();
        let fields = [Field::ByVal4(1234i32.to_le_bytes())];
        encode_wire_tuple(&mut out, &attrs, &fields)?;
        let (hdr, bitmap, data) = decode_wire_tuple(&out)?;
        assert_eq!(hdr.nattrs, 1);
        assert_eq!(hdr.flags & 0x01, 0);
        assert_eq!(bitmap.len(), 0);
        assert!(hdr.hoff as usize >= 10);
        assert_eq!(data.len() as u32, hdr.data_bytes);
        // Data should start at an aligned offset and contain 4 bytes of the value
        assert_eq!(data, &1234i32.to_le_bytes());
        Ok(())
    }

    #[test]
    fn test_encode_decode_with_null_bitmap_and_varlena() -> Result<()> {
        let attrs = attrs_int4_text();
        // ByRef for varlena encodes: u32 LE length + raw bytes; executor sends raw UTF-8 "hi"
        let fields = [Field::Null, Field::ByRef(b"hi")];
        let mut out = Vec::new();
        encode_wire_tuple(&mut out, &attrs, &fields)?;
        let (hdr, bitmap, data) = decode_wire_tuple(&out)?;
        assert_eq!(hdr.nattrs, 2);
        assert_ne!(hdr.flags & 0x01, 0); // has nulls
        assert_eq!(bitmap.len(), 1);
        // bit0 set (first attr is null)
        assert_eq!(bitmap[0] & 0x01, 0x01);
        // Data: [u32 LE 2] + b"hi"
        assert_eq!(data.len(), 4 + 2);
        assert_eq!(&data[0..4], &(2u32).to_le_bytes());
        assert_eq!(&data[4..], b"hi");
        Ok(())
    }
}
