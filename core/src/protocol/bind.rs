use crate::data_type::read_scalar_value;
use crate::protocol::{write_header, ByteStream, Direction, Flag, Header, Packet};
use anyhow::Result;
use datafusion::scalar::ScalarValue;
use rmp::decode::read_array_len;

pub fn read_params(stream: &mut impl ByteStream) -> Result<Vec<ScalarValue>> {
    let len = read_array_len(stream)?;
    let mut params = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let value = read_scalar_value(stream)?;
        params.push(value);
    }
    Ok(params)
}

pub fn request_params(stream: &mut impl ByteStream) -> Result<()> {
    stream.reset()?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Bind,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    Ok(())
}
