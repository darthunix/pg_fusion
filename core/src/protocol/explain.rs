use crate::protocol::{write_c_str, write_header, ByteStream, Direction, Flag, Header, Packet};
use anyhow::Result;

pub fn prepare_explain(stream: &mut impl ByteStream, explain: &str) -> Result<()> {
    stream.reset()?;
    let header = Header::default();
    write_header(stream, &header)?;
    let pos_init = stream.position();
    write_c_str(stream, explain)?;
    let pos_final = stream.position();
    let length = u16::try_from(pos_final - pos_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Explain,
        length,
        flag: Flag::Last,
    };
    stream.reset()?;
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}
