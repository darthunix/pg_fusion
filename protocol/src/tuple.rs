use crate::{write_header, ControlPacket, Direction, Flag, Header, Tape};
use anyhow::Result;
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
