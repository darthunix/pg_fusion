use crate::protocol::{write_c_str, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use std::io::Write;

pub fn request_explain(stream: &mut impl Write) -> Result<()> {
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Explain,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    stream.flush()?;
    Ok(())
}

pub fn prepare_explain(stream: &mut impl Tape, explain: &str) -> Result<()> {
    let header = Header::default();
    write_header(stream, &header)?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    write_c_str(stream, explain)?;
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Explain,
        length,
        flag: Flag::Last,
    };
    stream.rollback();
    write_header(stream, &header)?;
    stream.fast_forward(length as u32)?;
    debug_assert_eq!(stream.uncommitted_len(), len_final);
    stream.flush()?;
    debug_assert_eq!(stream.len(), len_final);
    debug_assert_eq!(stream.uncommitted_len(), 0);
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
    fn test_request_explain() {
        let layout = lockfree_buffer_layout(32).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mut buffer = LockFreeBuffer::from_layout(base, layout);
            request_explain(&mut buffer).expect("Failed to request explain");
            let expected_header = Header {
                direction: Direction::ToWorker,
                packet: Packet::Explain,
                length: 0,
                flag: Flag::Last,
            };
            let header = consume_header(&mut buffer).expect("Failed to consume header");
            assert_eq!(header, expected_header);
            dealloc(base, layout.layout);
        }
    }

    #[test]
    fn test_prepare_explain() {
        let layout = lockfree_buffer_layout(32).unwrap();
        unsafe {
            let base = alloc(layout.layout);
            assert!(!base.is_null());
            std::ptr::write_bytes(base, 0, layout.layout.size());
            let mut buffer = LockFreeBuffer::from_layout(base, layout);
            assert!(buffer.is_empty());
            assert_eq!(buffer.uncommitted_len(), 0);

            prepare_explain(&mut buffer, "query plan").unwrap();
            assert_eq!(buffer.uncommitted_len(), 0);
            const PLAN: &[u8] = b"\xc4\x0bquery plan\0";
            let header = consume_header(&mut buffer).unwrap();
            let expected_header = Header {
                direction: Direction::ToBackend,
                packet: Packet::Explain,
                length: PLAN.len() as u16,
                flag: Flag::Last,
            };
            assert_eq!(header, expected_header);
            let mut data = [0u8; PLAN.len()];
            let len = buffer.read(&mut data).unwrap();
            assert_eq!(len, PLAN.len());
            assert_eq!(&data, PLAN);
            dealloc(base, layout.layout);
        }
    }
}
