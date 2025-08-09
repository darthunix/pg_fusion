use crate::protocol::{str_prefix_len, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use rmp::decode::read_str_len;
use rmp::encode::write_str;
use smol_str::SmolStr;
use std::io::Write;
use std::str::from_utf8;

/// Reads the query from the stream, but leaves the stream position at the beginning of the query.
/// It is required to return the reference to the query bytes without copying them. It is the
/// caller's responsibility to move the stream position to the end of the query.
///
/// Returns the query and its length.
pub fn read_query(stream: &mut impl Tape) -> Result<SmolStr> {
    let len = read_str_len(stream)?;
    let mut buf = vec![0; len as usize];
    let read_len = stream.read(&mut buf)?;
    debug_assert_eq!(read_len, len as usize);
    let query = SmolStr::from(from_utf8(&buf)?);
    Ok(query)
}

pub fn prepare_query(stream: &mut impl Write, query: &str) -> Result<()> {
    let data_len = u32::try_from(query.len())?;
    let length = str_prefix_len(data_len) + data_len;
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Parse,
        length: length as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_str(stream, query)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::protocol::consume_header;
    use crate::layout::lockfree_buffer_layout;
    use std::alloc::{alloc, dealloc};
    use std::io::Read;

    #[test]
    fn test_prepare_query() {
        let layout = lockfree_buffer_layout(32).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mem = std::slice::from_raw_parts_mut(base, layout.layout.size());
            let mut buffer = LockFreeBuffer::new(mem);
            assert!(buffer.is_empty());
            assert_eq!(buffer.uncommitted_len(), 0);

            prepare_query(&mut buffer, "select 1").unwrap();
            assert_eq!(buffer.uncommitted_len(), 0);
            const MESSAGE: &[u8] = b"\xa8select 1";
            let header = consume_header(&mut buffer).unwrap();
            let expected_header = Header {
                direction: Direction::ToWorker,
                packet: Packet::Parse,
                length: MESSAGE.len() as u16,
                flag: Flag::Last,
            };
            assert_eq!(header, expected_header);
            let mut data = [0u8; MESSAGE.len()];
            let len = buffer.read(&mut data).unwrap();
            assert_eq!(len, MESSAGE.len());
            assert_eq!(&data, MESSAGE);
            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn test_read_query() {
        let layout = lockfree_buffer_layout(32).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mem = std::slice::from_raw_parts_mut(base, layout.layout.size());
            let mut buffer = LockFreeBuffer::new(mem);

            prepare_query(&mut buffer, "select 1").unwrap();
            let _ = consume_header(&mut buffer).unwrap();
            let msg = read_query(&mut buffer).unwrap();
            assert_eq!(msg, "select 1");
            dealloc(base, layout.layout);
        }
    }
}
