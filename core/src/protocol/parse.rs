use crate::error::FusionError;
use crate::protocol::{write_header, ByteStream, Direction, Flag, Header, Packet};
use anyhow::Result;
use rmp::decode::read_str_len;
use rmp::encode::write_str;

/// Reads the query from the stream, but leaves the stream position at the beginning of the query.
/// It is required to return the reference to the query bytes without copying them. It is the
/// caller's responsibility to move the stream position to the end of the query.
///
/// Returns the query and its length.
pub fn read_query(stream: &mut impl ByteStream) -> Result<(&str, u32)> {
    let len = read_str_len(stream)?;
    let buf = stream.look_ahead(len as usize)?;
    let query = std::str::from_utf8(buf)?;
    Ok((query, len))
}

pub fn prepare_query(stream: &mut impl ByteStream, query: &str) -> Result<()> {
    stream.reset()?;
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
