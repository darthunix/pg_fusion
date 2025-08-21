use crate::{str_prefix_len, write_header, Direction, Flag, Header, Packet};
use anyhow::Result;
use smol_str::SmolStr;
use std::fmt;
use std::error::Error;
use rmp::decode::read_str_len;
use rmp::encode::write_bin_len;
use std::io::{Read, Write};

pub fn prepare_error(stream: &mut impl Write, message: &str) -> Result<()> {
    let len = u32::try_from(message.len())?;
    let header = Header {
        direction: Direction::ToClient,
        packet: Packet::Failure,
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
    let len = read_str_len(stream)?;
    let mut buf = vec![0u8; len as usize];
    let read = stream.read(&mut buf)?;
    debug_assert_eq!(read, len as usize);
    Ok(String::from_utf8(buf)?)
}

pub fn request_failure(stream: &mut impl Write) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        packet: Packet::Failure,
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
