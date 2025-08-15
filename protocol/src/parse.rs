use crate::{write_c_str, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use rmp::decode::read_bin_len;
use std::io::Read;

pub fn read_query(stream: &mut impl Read) -> Result<String> {
    // Query is serialized as a C string (bin payload + trailing nul)
    let len = read_bin_len(stream)? as usize;
    let mut buf = vec![0u8; len];
    let read = std::io::Read::read(stream, &mut buf)?;
    debug_assert_eq!(read, len);
    if let Some(&0) = buf.last() {
        buf.pop();
    }
    Ok(String::from_utf8(buf)?)
}

pub fn prepare_query(stream: &mut impl Tape, query: &str) -> Result<()> {
    // We don't know the length of the query yet. So we write
    // an invalid header to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    write_c_str(stream, query)?;
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToServer,
        packet: Packet::Parse,
        length,
        flag: Flag::Last,
    };
    stream.rollback();
    write_header(stream, &header)?;
    stream.fast_forward(length as u32)?;
    debug_assert_eq!(stream.uncommitted_len(), len_final);
    stream.flush()?;
    debug_assert_eq!(stream.uncommitted_len(), 0);
    Ok(())
}
