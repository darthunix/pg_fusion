use crate::protocol::{str_prefix_len, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use rmp::decode::read_str_len;
use rmp::encode::write_str;
use std::io::Write;

pub fn read_error(stream: &mut impl Tape) -> Result<String> {
    let len = read_str_len(stream)?;
    let mut buf = vec![0; len as usize];
    let read_len = stream.peek(&mut buf);
    debug_assert_eq!(read_len, len as usize);
    let message = String::from_utf8(buf)?;
    Ok(message)
}

pub fn prepare_error(stream: &mut impl Write, message: &str) -> Result<()> {
    let data_len = u32::try_from(message.len())?;
    let length = str_prefix_len(data_len) + data_len;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Failure,
        length: length as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_str(stream, message)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::protocol::consume_header;
    use std::io::Read;

    #[test]
    fn test_prepare_error() {
        let mut bytes = vec![0u8; 8 + 32];
        let mut buffer = LockFreeBuffer::new(&mut bytes);
        assert!(buffer.is_empty());
        assert_eq!(buffer.uncommitted_len(), 0);

        prepare_error(&mut buffer, "error message").unwrap();
        assert_eq!(buffer.uncommitted_len(), 0);
        const MESSAGE: &[u8] = b"\xaderror message";
        let header = consume_header(&mut buffer).unwrap();
        let expected_header = Header {
            direction: Direction::ToBackend,
            packet: Packet::Failure,
            length: MESSAGE.len() as u16,
            flag: Flag::Last,
        };
        assert_eq!(header, expected_header);
        let mut data = [0u8; MESSAGE.len()];
        let len = buffer.read(&mut data).unwrap();
        assert_eq!(len, MESSAGE.len());
        assert_eq!(&data, MESSAGE);
    }

    #[test]
    fn test_read_error() {
        let mut bytes = vec![0u8; 8 + 32];
        let mut buffer = LockFreeBuffer::new(&mut bytes);

        prepare_error(&mut buffer, "error message").unwrap();
        let _ = consume_header(&mut buffer).unwrap();
        let msg = read_error(&mut buffer).unwrap();
        assert_eq!(msg, "error message");
    }
}
