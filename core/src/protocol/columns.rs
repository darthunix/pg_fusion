use crate::data_type::EncodedType;
use crate::protocol::{write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use datafusion::arrow::datatypes::Fields;
use rmp::encode::{write_array_len, write_bin_len, write_pfix, write_u8};

pub fn prepare_columns(stream: &mut impl Tape, columns: &Fields) -> Result<()> {
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    write_array_len(stream, u32::try_from(columns.len())?)?;
    for column in columns {
        write_u8(stream, EncodedType::try_from(column.data_type())? as u8)?;
        let len = u32::try_from(column.name().len() + 1)?;
        write_bin_len(stream, len)?;
        stream.write_all(column.name().as_bytes())?;
        write_pfix(stream, 0)?;
    }
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Columns,
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
    use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder};
    use std::io::Read;

    #[test]
    fn test_prepare_columns() {
        let mut bytes = vec![0u8; 8 + 32];
        let mut buffer = LockFreeBuffer::new(&mut bytes);
        assert!(buffer.is_empty());
        assert_eq!(buffer.uncommitted_len(), 0);

        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("a", DataType::Boolean, false));
        builder.push(Field::new("b", DataType::Int32, true));
        let columns = builder.finish().fields;

        prepare_columns(&mut buffer, &columns).unwrap();
        assert_eq!(buffer.uncommitted_len(), 0);
        let header = consume_header(&mut buffer).unwrap();
        let expected_header = Header {
            direction: Direction::ToBackend,
            packet: Packet::Columns,
            length: 13,
            flag: Flag::Last,
        };
        assert_eq!(header, expected_header);
        let mut data = [0u8; 13];
        let len = buffer.read(&mut data).unwrap();
        assert_eq!(len, 13);
        assert_eq!(&data, b"\x92\xcc\0\xc4\x02a\0\xcc\x03\xc4\x02b\0");
    }
}
