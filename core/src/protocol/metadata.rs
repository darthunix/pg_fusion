use crate::data_type::EncodedType;
use crate::protocol::{write_c_str, write_header, Direction, Flag, Header, Packet, Tape};
use crate::sql::Table;
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::logical_expr::TableSource;
use datafusion_sql::TableReference;
use rmp::decode::{read_array_len, read_bool, read_str_len, read_u32, read_u8};
use rmp::encode::write_array_len;
use smallvec::SmallVec;
use std::io::{Read, Write};
use std::str::from_utf8;
use std::sync::Arc;

/// Writes a table reference as null-terminated strings to
/// the stream. It would be used by the Rust wrappers to the
/// C code, so if we serialize the table and schema as
/// null-terminated strings, we can avoid copying on
/// deserialization.
#[inline]
pub fn write_table_ref(stream: &mut impl Write, table: &TableReference) -> Result<()> {
    match table {
        TableReference::Bare { table } => {
            write_array_len(stream, 1)?;
            write_c_str(stream, table)?;
        }
        TableReference::Full { schema, table, .. } | TableReference::Partial { schema, table } => {
            write_array_len(stream, 2)?;
            write_c_str(stream, schema)?;
            write_c_str(stream, table)?;
        }
    }
    Ok(())
}

pub fn prepare_table_refs(stream: &mut impl Tape, tables: &[TableReference]) -> Result<()> {
    // We don't know the length of the tables yet. So we write an invalid header
    // to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());
    write_array_len(stream, u32::try_from(tables.len())?)?;
    for table in tables {
        write_table_ref(stream, table)?;
    }
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Metadata,
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

pub fn prepare_empty_metadata(stream: &mut impl Write) -> Result<()> {
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Metadata,
        // The length of a zero element array in msgpack.
        length: size_of::<u8>() as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_array_len(stream, 0)?;
    stream.flush()?;
    Ok(())
}

#[inline]
pub fn consume_metadata(
    stream: &mut impl Read,
) -> Result<AHashMap<TableReference, Arc<dyn TableSource>>> {
    // The header should be consumed before calling this function.
    let table_num = read_array_len(stream)?;
    let mut tables = AHashMap::with_capacity(table_num as usize);

    // Reserve on the stack the default size of NAMEDATALEN.
    let mut schema_buf = SmallVec::<[u8; 64]>::new();
    let mut name_buf = SmallVec::<[u8; 64]>::new();

    for _ in 0..table_num {
        let name_part_num = read_array_len(stream)?;
        debug_assert!(name_part_num == 2 || name_part_num == 3);
        let oid = read_u32(stream)?;
        let mut schema = None;
        if name_part_num == 3 {
            let ns_len = read_str_len(stream)?;
            schema_buf.resize(ns_len as usize, 0);
            stream.read_exact(&mut schema_buf)?;
            schema = Some(from_utf8(schema_buf.as_slice())?);
        }
        let name_len = read_str_len(stream)?;
        name_buf.resize(name_len as usize, 0);
        stream.read_exact(&mut name_buf)?;
        let name = from_utf8(name_buf.as_slice())?;
        let table_ref = match schema {
            Some(schema) => TableReference::partial(schema, name),
            None => TableReference::bare(name),
        };
        schema_buf.clear();
        name_buf.clear();

        let column_num = read_array_len(stream)?;
        let mut fields = Vec::with_capacity(column_num as usize);
        for _ in 0..column_num {
            let elem_num = read_array_len(stream)?;
            debug_assert_eq!(elem_num, 3);
            let etype = read_u8(stream)?;
            let df_type = EncodedType::try_from(etype)?.to_arrow();
            let is_nullable = read_bool(stream)?;
            let name_len = read_str_len(stream)?;
            name_buf.resize(name_len as usize, 0);
            stream.read_exact(&mut name_buf)?;
            let name = from_utf8(name_buf.as_slice())?;
            let field = Field::new(name, df_type, is_nullable);
            name_buf.clear();
            fields.push(field);
        }
        let schema = Schema::new(fields);
        let table = Table::new(oid, Arc::new(schema));
        tables.insert(table_ref, Arc::new(table) as Arc<dyn TableSource>);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::protocol::consume_header;
    use datafusion::arrow::datatypes::DataType;
    use std::io::{Cursor, Read};

    #[test]
    fn test_prepare_table_refs() {
        let mut bytes = vec![0u8; 8 + 37];
        let mut buffer = LockFreeBuffer::new(&mut bytes);

        let expected_tables = vec![
            TableReference::partial("public", "table1"),
            TableReference::bare("table2"),
        ];
        prepare_table_refs(&mut buffer, expected_tables.as_slice()).unwrap();
        assert_eq!(buffer.uncommitted_len(), 0);
        let header = consume_header(&mut buffer).unwrap();
        const MESSAGE: &[u8] = b"\x92\x92\xc4\x07public\0\xc4\x07table1\0\x91\xc4\x07table2\0";
        let expected_header = Header {
            direction: Direction::ToBackend,
            packet: Packet::Metadata,
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
    fn test_prepare_empty_metadata() {
        let mut bytes = vec![0u8; 8 + 8];
        let mut buffer = LockFreeBuffer::new(&mut bytes);

        prepare_empty_metadata(&mut buffer).unwrap();
        assert_eq!(buffer.uncommitted_len(), 0);
        let header = consume_header(&mut buffer).unwrap();
        let expected_header = Header {
            direction: Direction::ToWorker,
            packet: Packet::Metadata,
            length: 1,
            flag: Flag::Last,
        };
        assert_eq!(header, expected_header);
    }

    #[test]
    fn test_consume_metadata() {
        let t1_ref = TableReference::partial("public", "t1");
        let a = Field::new("a", DataType::Int64, true);
        let b = Field::new("b", DataType::Utf8, false);
        let t1 = Table::new(42, Arc::new(Schema::new(vec![a, b])));

        let t2_ref = TableReference::bare("t2");
        let t2 = Table::new(666, Arc::new(Schema::new(Vec::<Field>::new())));

        let mut expected_tables = AHashMap::new();
        expected_tables.insert(t1_ref.clone(), Arc::new(t1) as Arc<dyn TableSource>);
        expected_tables.insert(t2_ref.clone(), Arc::new(t2) as Arc<dyn TableSource>);

        let mut message: Vec<u8> = Vec::new();
        message.extend_from_slice(b"\x92");
        message.extend_from_slice(b"\x93\xce\x00\x00\x00\x2a\xa6public\xa2t1");
        message.extend_from_slice(b"\x92\x93\xcc\x04\xc3\xa1a\x93\xcc\x01\xc2\xa1b");
        message.extend_from_slice(b"\x92\xce\x00\x00\x02\x9a\xa2t2");
        message.extend_from_slice(b"\x90");

        let tables = consume_metadata(&mut Cursor::new(message)).unwrap();
        assert_eq!(tables.len(), expected_tables.len());
        let t1 = tables
            .get(&t1_ref)
            .unwrap()
            .as_any()
            .downcast_ref::<Table>()
            .unwrap();
        assert_eq!(t1.id, 42);
        assert_eq!(t1.schema().fields().len(), 2);
        assert_eq!(t1.schema().field(0).name(), "a");
        assert_eq!(t1.schema().field(1).name(), "b");
        assert_eq!(t1.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(t1.schema().field(1).data_type(), &DataType::Utf8);
        assert!(t1.schema().field(0).is_nullable());
        assert!(!t1.schema().field(1).is_nullable());
        let t2 = tables
            .get(&t2_ref)
            .unwrap()
            .as_any()
            .downcast_ref::<Table>()
            .unwrap();
        assert_eq!(t2.id, 666);
        assert_eq!(t2.schema().fields().len(), 0);
    }
}
