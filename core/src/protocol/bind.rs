use crate::data_type::read_scalar_value;
use crate::protocol::{write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use datafusion::scalar::ScalarValue;
use rmp::decode::read_array_len;
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
        direction: Direction::ToBackend,
        packet: Packet::Bind,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::data_type::write_scalar_value;
    use crate::protocol::{consume_header, Direction, Flag, Header, Packet, Tape};
    use datafusion::scalar::ScalarValue;
    use std::io::Write;

    #[test]
    fn test_read_params() {
        let mut bytes = vec![0u8; 8 + 16];
        let mut buffer = LockFreeBuffer::new(&mut bytes);
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
    }

    #[test]
    fn test_request_params() {
        let mut bytes = vec![0u8; 8 + 16];
        let mut buffer = LockFreeBuffer::new(&mut bytes);
        assert!(buffer.is_empty());
        assert_eq!(buffer.uncommitted_len(), 0);

        request_params(&mut buffer).unwrap();
        assert_eq!(buffer.len(), Header::estimate_size());
        let expected_header = Header {
            direction: Direction::ToBackend,
            packet: Packet::Bind,
            length: 0,
            flag: Flag::Last,
        };
        let header = consume_header(&mut buffer).unwrap();
        assert_eq!(header, expected_header);
        assert!(buffer.is_empty());
    }
}
