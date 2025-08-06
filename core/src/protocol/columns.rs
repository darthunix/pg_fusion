use crate::data_type::EncodedType;
use crate::protocol::metadata::NAMEDATALEN;
use crate::protocol::{write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use datafusion::arrow::datatypes::Fields;
use rmp::decode::{read_array_len, read_bin_len, read_u8};
use rmp::encode::{write_array_len, write_bin_len, write_pfix, write_u8};
use smallvec::SmallVec;
use std::io::Read;
use std::os::raw::c_void;

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

pub fn consume_columns<ColumnRepack>(
    stream: &mut impl Read,
    opaque: *mut c_void,
    repack: ColumnRepack,
) -> Result<()>
where
    ColumnRepack: Fn(i16, u8, &[u8], *mut c_void) -> Result<()>,
{
    let column_len = read_array_len(stream)?;
    debug_assert!(column_len < i16::MAX as u32);
    for pos in 0..column_len {
        let etype = read_u8(stream)?;
        let mut column_name = SmallVec::<[u8; NAMEDATALEN]>::new();
        let column_len = read_bin_len(stream)?;
        column_name.resize(column_len as usize, 0);
        let len = stream.read(&mut column_name)?;
        debug_assert_eq!(column_len as usize, len);
        repack(pos as i16, etype, column_name.as_slice(), opaque)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::protocol::consume_header;
    use datafusion::arrow::datatypes::{DataType, Field, SchemaBuilder};
    use std::io::{Cursor, Read};

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

    #[test]
    fn test_consume_columns() {
        let data = b"\x92\xcc\0\xc4\x02a\0\xcc\x03\xc4\x02b\0";
        let mut csr = Cursor::new(&data);
        type Payload = (i16, u8, [u8; 2]);
        let mut result = Vec::new();
        let result_ptr = &mut result as *mut Vec<Payload> as *mut c_void;
        let repack = |pos: i16, etype: u8, name: &[u8], ptr: *mut c_void| -> Result<()> {
            let result: &mut Vec<Payload> = unsafe { &mut *(ptr as *mut Vec<Payload>) };
            let mut buffer: [u8; 2] = [0u8; 2];
            buffer[0] = name[0];
            buffer[1] = name[1];
            result.push((pos, etype, buffer));
            Ok(())
        };
        consume_columns(&mut csr, result_ptr, repack).expect("Failed to consume columns");
        let expected_result: Vec<Payload> = vec![(0, 0, *b"a\0"), (1, 3, *b"b\0")];
        assert_eq!(result, expected_result);
    }
}
