use crate::{write_c_str, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use rmp::decode::read_bin_len;
use rmp::encode::{write_bin_len, write_pfix};
use std::io::{Read, Write};

pub fn prepare_explain(stream: &mut impl Tape, explain: &str) -> Result<()> {
    // We don't know the length of the string yet. So we write invalid header
    // to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    write_c_str(stream, explain)?;
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToClient,
        packet: Packet::Explain,
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

pub fn request_explain(stream: &mut impl Write) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        packet: Packet::Explain,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    stream.flush()?;
    Ok(())
}

pub fn consume_explain(stream: &mut impl Read) -> Result<String> {
    let len = read_bin_len(stream)?;
    let mut buf = vec![0u8; len as usize];
    let read = stream.read(&mut buf)?;
    debug_assert_eq!(read, len as usize);
    let explain = String::from_utf8(buf)?;
    Ok(explain)
}
