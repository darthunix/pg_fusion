use crate::{str_prefix_len, write_header, ControlPacket, Direction, Flag, Header};
use anyhow::Result;
use rmp::decode::read_bin_len;
use rmp::encode::write_bin_len;
use smol_str::SmolStr;
use std::error::Error;
use std::fmt;
use std::io::{Read, Write};

pub fn prepare_error(stream: &mut impl Write, message: &str) -> Result<()> {
    let len = u32::try_from(message.len())?;
    let header = Header {
        direction: Direction::ToClient,
        tag: ControlPacket::Failure as u8,
        length: u16::try_from(1 + str_prefix_len(len) + len)?,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_bin_len(stream, len)?;
    stream.write_all(message.as_bytes())?;
    stream.flush()?;
    Ok(())
}

pub fn read_error(stream: &mut impl Read) -> Result<String> {
    // Errors are encoded as MsgPack bin with explicit length
    let len = read_bin_len(stream)? as usize;
    let mut buf = vec![0u8; len];
    std::io::Read::read_exact(stream, &mut buf)?;
    Ok(String::from_utf8(buf)?)
}

pub fn request_failure(stream: &mut impl Write) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        tag: ControlPacket::Failure as u8,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    stream.flush()?;
    Ok(())
}

/// Error indicating an out-of-bounds access (e.g., attribute index beyond available columns).
#[derive(Debug, Clone)]
pub struct OutOfBound(pub SmolStr);

impl fmt::Display for OutOfBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Out of bound: {}", self.0)
    }
}

impl Error for OutOfBound {}
