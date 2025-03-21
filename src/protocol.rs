use crate::error::FusionError;
use crate::ipc::{Bus, Slot, SlotNumber, SlotStream, DATA_SIZE};
use crate::worker::worker_id;
use anyhow::Result;
use datafusion_sql::TableReference;
use pgrx::pg_sys::ProcSendSignal;
use pgrx::prelude::*;
use rmp::decode::{read_pfix, read_str_len, read_u16};
use rmp::encode::{write_array_len, write_bin_len, write_pfix, write_str, write_u16, RmpWrite};

#[repr(u8)]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Direction {
    #[default]
    ToWorker = 0,
    ToBackend = 1,
}

impl TryFrom<u8> for Direction {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Direction::ToWorker),
            1 => Ok(Direction::ToBackend),
            _ => Err(FusionError::DeserializeU8("direction".to_string(), value)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Packet {
    #[default]
    None = 0,
    Bind = 1,
    Failure = 2,
    Metadata = 3,
    Parse = 4,
    Plan = 5,
}

impl TryFrom<u8> for Packet {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Packet::None),
            1 => Ok(Packet::Bind),
            2 => Ok(Packet::Failure),
            3 => Ok(Packet::Metadata),
            4 => Ok(Packet::Parse),
            5 => Ok(Packet::Plan),
            _ => Err(FusionError::DeserializeU8("packet".to_string(), value)),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Default, Debug, PartialEq)]
pub(crate) enum Flag {
    More = 0,
    #[default]
    Last = 1,
}

impl TryFrom<u8> for Flag {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Flag::More),
            1 => Ok(Flag::Last),
            _ => Err(FusionError::DeserializeU8("flag".to_string(), value)),
        }
    }
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct Header {
    pub(crate) direction: Direction,
    pub(crate) packet: Packet,
    pub(crate) flag: Flag,
    pub(crate) length: u16,
}

impl Header {
    const fn estimate_size() -> usize {
        // direction (1 byte) + packet(1 byte) + flag (1 byte) + length (3 bytes)
        1 + 1 + 1 + 3
    }

    const fn payload_max_size() -> usize {
        DATA_SIZE - Self::estimate_size()
    }
}

fn signal(slot_id: SlotNumber, direction: Direction) {
    match direction {
        Direction::ToWorker => {
            unsafe { ProcSendSignal(worker_id()) };
        }
        Direction::ToBackend => {
            let id = Bus::new().slot(slot_id).owner();
            unsafe { ProcSendSignal(id) };
        }
    }
}

pub(crate) fn consume_header(stream: &mut SlotStream) -> Result<Header> {
    assert_eq!(stream.position(), 0);
    let direction = Direction::try_from(read_pfix(stream)?)?;
    let packet = Packet::try_from(read_pfix(stream)?)?;
    let flag = Flag::try_from(read_pfix(stream)?)?;
    let length = read_u16(stream)?;
    Ok(Header {
        direction,
        packet,
        flag,
        length,
    })
}

pub(crate) fn write_header(stream: &mut SlotStream, header: &Header) -> Result<()> {
    write_pfix(stream, header.direction.to_owned() as u8)?;
    write_pfix(stream, header.packet.to_owned() as u8)?;
    write_pfix(stream, header.flag.to_owned() as u8)?;
    write_u16(stream, header.length.to_owned())?;
    Ok(())
}

/// Reads the query from the stream, but leaves the stream position at the beginning of the query.
/// It is required to return the reference to the query bytes without copying them. It is the
/// caller's responsibility to move the stream position to the end of the query.
///
/// Returns the query and its length.
pub(crate) fn read_query(stream: &mut SlotStream) -> Result<(&str, u32)> {
    let len = read_str_len(stream)?;
    let buf = stream.look_ahead(len as usize)?;
    let query = std::str::from_utf8(buf)?;
    Ok((query, len))
}

fn prepare_query(stream: &mut SlotStream, query: &str) -> Result<()> {
    stream.reset();
    // slot: header - bin marker - bin length - query bytes
    let length = 1 + 1 + query.len();
    if length > Header::payload_max_size() {
        return Err(FusionError::PayloadTooLarge(query.len()).into());
    }
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Parse,
        length: length as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_str(stream, query)?;
    Ok(())
}

pub(crate) fn send_query(slot_id: SlotNumber, mut stream: SlotStream, query: &str) -> Result<()> {
    prepare_query(&mut stream, query)?;
    // Unlock the slot after writing the query.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToWorker);
    Ok(())
}

pub fn read_error(stream: &mut SlotStream) -> Result<String> {
    let len = read_str_len(stream)?;
    let buf = stream.look_ahead(len as usize)?;
    let message = std::str::from_utf8(buf)?.to_string();
    Ok(message)
}

fn prepare_error(stream: &mut SlotStream, message: &str) -> Result<()> {
    stream.reset();
    let length = 1 + 1 + u32::try_from(message.len())?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Failure,
        length: length as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_str(stream, message)?;
    Ok(())
}

pub(crate) fn send_error(slot_id: SlotNumber, mut stream: SlotStream, message: &str) -> Result<()> {
    prepare_error(&mut stream, message)?;
    // Unlock the slot after writing the error message.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToBackend);
    Ok(())
}

#[inline]
fn write_c_str(stream: &mut SlotStream, s: &str) -> Result<()> {
    let len = u32::try_from(s.len())?;
    write_bin_len(stream, len + 1)?;
    stream.write_bytes(s.as_bytes())?;
    write_pfix(stream, 0)?;
    Ok(())
}

/// Writes a table as null-terminated strings to the stream.
/// It would be used by the Rust wrappers to the C code, so
/// if we serialize the table and schema as null-terminated
/// strings, we can avoid copying on deserialization.
#[inline]
fn write_table(stream: &mut SlotStream, table: &TableReference) -> Result<()> {
    match table {
        TableReference::Bare { table } => {
            write_array_len(stream, 1)?;
            write_c_str(stream, table)?;
        }
        TableReference::Full { schema, table, .. } | TableReference::Partial { schema, table } => {
            write_array_len(stream, 2)?;
            write_c_str(stream, schema)?;
            write_c_str(stream, table)?;
        }
    }
    Ok(())
}

pub(crate) fn write_tables(stream: &mut SlotStream, tables: &[&TableReference]) -> Result<()> {
    let length = u32::try_from(tables.len())?;
    assert!(length > 0);
    write_array_len(stream, length)?;
    for table in tables {
        write_table(stream, table)?;
    }
    Ok(())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;
    use std::ptr::addr_of_mut;

    const SLOT_SIZE: usize = 8204;

    #[pg_test]
    fn test_header() {
        let header = Header {
            direction: Direction::ToWorker,
            packet: Packet::None,
            length: 42,
            flag: Flag::Last,
        };
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let ptr = addr_of_mut!(slot_buf) as *mut u8;
        Slot::init(ptr, slot_buf.len());
        let slot = Slot::from_bytes(ptr, slot_buf.len());
        let mut stream: SlotStream = slot.into();
        write_header(&mut stream, &header).unwrap();
        stream.reset();
        let new_header = consume_header(&mut stream).unwrap();
        assert_eq!(header, new_header);
    }

    #[pg_test]
    fn test_query() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let ptr = addr_of_mut!(slot_buf) as *mut u8;
        Slot::init(ptr, slot_buf.len());
        let slot = Slot::from_bytes(ptr, slot_buf.len());
        let sql = "SELECT 1";
        let mut stream: SlotStream = slot.into();
        prepare_query(&mut stream, sql).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToWorker);
        assert_eq!(header.packet, Packet::Parse);
        assert_eq!(header.flag, Flag::Last);
        assert_eq!(header.length, 2 + sql.len() as u16);
        let (query, len) = read_query(&mut stream).unwrap();
        assert_eq!(query, sql);
        assert_eq!(len as usize, sql.len());
    }

    #[pg_test]
    fn test_error() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let ptr = addr_of_mut!(slot_buf) as *mut u8;
        Slot::init(ptr, slot_buf.len());
        let slot = Slot::from_bytes(ptr, slot_buf.len());
        let message = "An error occurred";
        let mut stream: SlotStream = slot.into();
        prepare_error(&mut stream, message).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToBackend);
        assert_eq!(header.packet, Packet::Failure);
        assert_eq!(header.flag, Flag::Last);
        assert_eq!(header.length, 2 + message.len() as u16);
        let error = read_error(&mut stream).unwrap();
        assert_eq!(error, message);
    }
}
