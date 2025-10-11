pub mod data_type;

use anyhow::Result;
use common::FusionError;
use rmp::decode::{read_pfix, read_u16};
use rmp::encode::{write_bin_len, write_pfix, write_u16, RmpWrite};
use std::io::{Read, Write};

pub mod bind;
pub mod columns;
pub mod explain;
pub mod failure;
pub mod heap;
pub mod metadata;
pub mod parse;
pub mod exec;

pub const DATA_SIZE: usize = 8 * 1024;

pub trait Tape: Read + Write {
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn len(&self) -> usize;
    fn uncommitted_len(&self) -> usize;
    fn peek(&self, buffer: &mut [u8]) -> usize;
    fn rollback(&mut self);
    fn rewind(&mut self, len: u32) -> Result<()>;
    fn fast_forward(&mut self, len: u32) -> Result<()>;
}

#[repr(u8)]
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Direction {
    #[default]
    ToServer = 0,
    ToClient = 1,
}

impl TryFrom<u8> for Direction {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Direction::ToServer),
            1 => Ok(Direction::ToClient),
            _ => Err(FusionError::Deserialize("direction".into(), value.into())),
        }
    }
}

// Control packets correspond to planning/metadata/coordination messages.
#[repr(u8)]
#[derive(Clone, Debug, PartialEq)]
pub enum ControlPacket {
    None = 0,
    Bind = 1,
    Failure = 2,
    Metadata = 3,
    Parse = 4,
    Explain = 5,
    Columns = 6,
    Optimize = 7,
    Translate = 8,
    BeginScan = 10,
    ExecScan = 11,
    EndScan = 12,
}

impl TryFrom<u8> for ControlPacket {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(ControlPacket::None),
            1 => Ok(ControlPacket::Bind),
            2 => Ok(ControlPacket::Failure),
            3 => Ok(ControlPacket::Metadata),
            4 => Ok(ControlPacket::Parse),
            5 => Ok(ControlPacket::Explain),
            6 => Ok(ControlPacket::Columns),
            7 => Ok(ControlPacket::Optimize),
            8 => Ok(ControlPacket::Translate),
            10 => Ok(ControlPacket::BeginScan),
            11 => Ok(ControlPacket::ExecScan),
            12 => Ok(ControlPacket::EndScan),
            _ => Err(FusionError::Deserialize(
                "control_packet".into(),
                value.into(),
            )),
        }
    }
}

// Data packets correspond to execution-time data transfer.
#[repr(u8)]
#[derive(Clone, Debug, PartialEq)]
pub enum DataPacket {
    Heap = 9,
}

impl TryFrom<u8> for DataPacket {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            9 => Ok(DataPacket::Heap),
            _ => Err(FusionError::Deserialize("data_packet".into(), value.into())),
        }
    }
}

/// Helper to check whether a tag is a data packet value.
#[inline]
pub fn is_data_tag(tag: u8) -> bool {
    matches!(tag, x if x == DataPacket::Heap as u8)
}

#[repr(u8)]
#[derive(Clone, Default, Debug, PartialEq)]
pub enum Flag {
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
            _ => Err(FusionError::Deserialize("flag".into(), value.into())),
        }
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct Header {
    pub direction: Direction,
    pub tag: u8,
    pub flag: Flag,
    pub length: u16,
}

impl Header {
    const fn estimate_size() -> usize {
        // direction (1 byte) + packet(1 byte) + flag (1 byte) + length (3 bytes)
        1 + 1 + 1 + 3
    }

    pub const fn payload_max_size() -> usize {
        DATA_SIZE - Self::estimate_size()
    }
}

pub fn consume_header(stream: &mut impl Read) -> Result<Header> {
    let direction = Direction::try_from(read_pfix(stream)?)?;
    let tag = read_pfix(stream)?;
    let flag = Flag::try_from(read_pfix(stream)?)?;
    let length = read_u16(stream)?;
    Ok(Header {
        direction,
        tag,
        flag,
        length,
    })
}

pub fn write_header(stream: &mut impl Write, header: &Header) -> Result<()> {
    write_pfix(stream, header.direction.to_owned() as u8)?;
    write_pfix(stream, header.tag)?;
    write_pfix(stream, header.flag.to_owned() as u8)?;
    write_u16(stream, header.length.to_owned())?;
    Ok(())
}

#[inline]
pub fn write_c_str(stream: &mut impl Write, s: &str) -> Result<()> {
    let len = u32::try_from(s.len())?;
    write_bin_len(stream, len + 1)?;
    stream.write_bytes(s.as_bytes())?;
    write_pfix(stream, 0)?;
    Ok(())
}

#[inline]
fn str_prefix_len(len: u32) -> u32 {
    if len < 32 {
        1
    } else if len < 256 {
        2
    } else if len <= u16::MAX as u32 {
        3
    } else {
        5
    }
}
