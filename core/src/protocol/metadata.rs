use std::str::from_utf8;
use std::sync::Arc;

use crate::data_type::EncodedType;
use crate::protocol::{write_header, ByteStream, Direction, Flag, Header, Packet};
use crate::sql::Table;
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::logical_expr::TableSource;
use datafusion_sql::TableReference;
use rmp::decode::{read_array_len, read_bool, read_str_len, read_u32, read_u8};
use rmp::encode::write_array_len;
use smol_str::SmolStr;

use super::write_c_str;

/// Writes a table reference as null-terminated strings to
/// the stream. It would be used by the Rust wrappers to the
/// C code, so if we serialize the table and schema as
/// null-terminated strings, we can avoid copying on
/// deserialization.
#[inline]
pub fn write_table_ref(stream: &mut impl ByteStream, table: &TableReference) -> Result<()> {
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

pub fn prepare_table_refs(stream: &mut impl ByteStream, tables: &[TableReference]) -> Result<()> {
    stream.reset()?;
    // We don't know the length of the tables yet. So we write an invalid header
    // to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let pos_init = stream.position();
    write_array_len(stream, u32::try_from(tables.len())?)?;
    for table in tables {
        write_table_ref(stream, table)?;
    }
    let pos_final = stream.position();
    let length = u16::try_from(pos_final - pos_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Metadata,
        length,
        flag: Flag::Last,
    };
    stream.reset()?;
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}

pub fn prepare_empty_metadata(stream: &mut impl ByteStream) -> Result<()> {
    stream.reset()?;
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Metadata,
        // The length of a zero element array in msgpack.
        length: size_of::<u8>() as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_array_len(stream, 0)?;
    Ok(())
}

#[inline]
pub fn consume_metadata(
    stream: &mut impl ByteStream,
) -> Result<AHashMap<TableReference, Arc<dyn TableSource>>> {
    // The header should be consumed before calling this function.
    let table_num = read_array_len(stream)?;
    let mut tables = AHashMap::with_capacity(table_num as usize);
    for _ in 0..table_num {
        let name_part_num = read_array_len(stream)?;
        assert!(name_part_num == 2 || name_part_num == 3);
        let oid = read_u32(stream)?;
        let mut schema = None;
        if name_part_num == 3 {
            let ns_len = read_str_len(stream)?;
            let ns_bytes = stream.look_ahead(ns_len as usize)?;
            schema = Some(SmolStr::new(from_utf8(ns_bytes)?));
            stream.rewind(ns_len as usize)?;
        }
        let name_len = read_str_len(stream)?;
        let name_bytes = stream.look_ahead(name_len as usize)?;
        let name = from_utf8(name_bytes)?;
        let table_ref = match schema {
            Some(schema) => TableReference::partial(schema, name),
            None => TableReference::bare(name),
        };
        stream.rewind(name_len as usize)?;
        let column_num = read_array_len(stream)?;
        let mut fields = Vec::with_capacity(column_num as usize);
        for _ in 0..column_num {
            let elem_num = read_array_len(stream)?;
            assert_eq!(elem_num, 3);
            let etype = read_u8(stream)?;
            let df_type = EncodedType::try_from(etype)?.to_arrow();
            let is_nullable = read_bool(stream)?;
            let name_len = read_str_len(stream)?;
            let name_bytes = stream.look_ahead(name_len as usize)?;
            let name = from_utf8(name_bytes)?;
            let field = Field::new(name, df_type, is_nullable);
            stream.rewind(name_len as usize)?;
            fields.push(field);
        }
        let schema = Schema::new(fields);
        let table = Table::new(oid, Arc::new(schema));
        tables.insert(table_ref, Arc::new(table) as Arc<dyn TableSource>);
    }
    Ok(tables)
}
