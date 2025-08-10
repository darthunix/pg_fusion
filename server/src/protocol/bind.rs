use crate::data_type::{read_scalar_value, write_scalar_value};
use crate::protocol::{write_header, Direction, Flag, Header, Packet, Tape};
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
        packet: Packet::Bind,
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
        packet: Packet::Bind,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::data_type::write_scalar_value;
    use crate::layout::lockfree_buffer_layout;
    use crate::protocol::{consume_header, Direction, Flag, Header, Packet, Tape};
    use datafusion::scalar::ScalarValue;
    use std::alloc::{alloc, dealloc};
    use std::io::Write;

    #[test]
    fn test_read_params() {
        let layout = lockfree_buffer_layout(16).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mut buffer = LockFreeBuffer::from_layout(base, layout);
            assert!(buffer.is_empty());
            assert_eq!(buffer.uncommitted_len(), 0);

            let expected = vec![
                ScalarValue::Int32(Some(42)),
                ScalarValue::Utf8(Some("test".to_string())),
                ScalarValue::Boolean(None),
            ];

            // Array length 3
            buffer.write_all(b"\x93").unwrap();
            assert_eq!(buffer.uncommitted_len(), 1);
            // Push three scalar values
            write_scalar_value(&mut buffer, &expected[0]).unwrap();
            assert_eq!(buffer.uncommitted_len(), 7);
            write_scalar_value(&mut buffer, &expected[1]).unwrap();
            assert_eq!(buffer.uncommitted_len(), 13);
            write_scalar_value(&mut buffer, &expected[2]).unwrap();
            assert_eq!(buffer.uncommitted_len(), 15);
            buffer.flush().unwrap();
            assert_eq!(buffer.len(), 15);

            let params = read_params(&mut buffer);
            assert_eq!(params.unwrap(), expected);
            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn test_request_params() {
        let layout = lockfree_buffer_layout(16).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mut buffer = LockFreeBuffer::from_layout(base, layout);
            assert!(buffer.is_empty());
            assert_eq!(buffer.uncommitted_len(), 0);

            request_params(&mut buffer).unwrap();
            assert_eq!(buffer.len(), Header::estimate_size());
            let expected_header = Header {
                direction: Direction::ToClient,
                packet: Packet::Bind,
                length: 0,
                flag: Flag::Last,
            };
            let header = consume_header(&mut buffer).unwrap();
            assert_eq!(header, expected_header);
            assert!(buffer.is_empty());
            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn test_prepare_params() {
        let layout = lockfree_buffer_layout(120).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mut buffer = LockFreeBuffer::from_layout(base, layout);

            prepare_params(&mut buffer, || {
                (1, vec![Ok(ScalarValue::Int32(Some(1)))].into_iter())
            })
            .expect("Failed to prepare params");
            let header = consume_header(&mut buffer).expect("Failed to consume header");
            assert_eq!(header.direction, Direction::ToServer);
            assert_eq!(header.packet, Packet::Bind);
            let params = read_params(&mut buffer).expect("Failed to read parameters");
            assert_eq!(params.len(), 1);
            assert_eq!(params[0], ScalarValue::Int32(Some(1)));
            dealloc(base, layout.layout);
        }
    }
}
