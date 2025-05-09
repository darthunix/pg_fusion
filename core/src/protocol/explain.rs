use crate::protocol::{write_c_str, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;

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
    use std::io::Read;

    #[test]
    fn test_prepare_explain() {
        let mut bytes = vec![0u8; 8 + 32];
        let mut buffer = LockFreeBuffer::new(&mut bytes);
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
    }
}
