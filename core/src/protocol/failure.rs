use crate::protocol::{write_header, ByteStream, Direction, Flag, Header, Packet};
use anyhow::Result;
use rmp::decode::read_str_len;
use rmp::encode::write_str;

pub fn read_error(stream: &mut impl ByteStream) -> Result<String> {
    let len = read_str_len(stream)?;
    let buf = stream.look_ahead(len as usize)?;
    let message = std::str::from_utf8(buf)?.to_string();
    Ok(message)
}

pub fn prepare_error(stream: &mut impl ByteStream, message: &str) -> Result<()> {
    stream.reset()?;
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
