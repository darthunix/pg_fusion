use crate::data_type::{read_scalar_value, write_scalar_value};
use crate::{write_header, ControlPacket, Direction, Flag, Header, Tape};
use anyhow::Result;
use datafusion::scalar::ScalarValue;
use rmp::decode::read_array_len;
use rmp::encode::write_array_len;
use std::io::Write;

pub fn read_params(stream: &mut impl Tape) -> Result<Vec<ScalarValue>> {
    let len = read_array_len(stream)?;
    let mut params = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let value = read_scalar_value(stream)?;
        params.push(value);
    }
    Ok(params)
}

pub fn request_params(stream: &mut impl Write) -> Result<()> {
    let header = Header {
        direction: Direction::ToClient,
        tag: ControlPacket::Bind as u8,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    stream.flush()?;
    Ok(())
}

pub fn prepare_params<ParamIterBuilder, ParamIter>(
    stream: &mut impl Tape,
    param_iter: ParamIterBuilder,
) -> Result<()>
where
    ParamIterBuilder: Fn() -> (usize, ParamIter),
    ParamIter: Iterator<Item = Result<ScalarValue>>,
{
    // We don't know the length of the parameters yet. So we write
    // an invalid header to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    let (num, iter) = param_iter();
    write_array_len(stream, u32::try_from(num)?)?;
    for param in iter {
        write_scalar_value(stream, &param?)?;
    }
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToServer,
        tag: ControlPacket::Bind as u8,
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
