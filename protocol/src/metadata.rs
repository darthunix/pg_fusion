use crate::{write_c_str, write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::{bail, Result};
use common::FusionError;
use datafusion_sql::TableReference;
use rmp::decode::{read_array_len, read_bin_len};
use rmp::encode::write_array_len;
use smallvec::SmallVec;
use std::io::{Read, Write};

pub const NAMEDATALEN: usize = 64;

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
        direction: Direction::ToClient,
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
        direction: Direction::ToServer,
        packet: Packet::Metadata,
        length: size_of::<u8>() as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_array_len(stream, 0)?;
    stream.flush()?;
    Ok(())
}

pub fn process_metadata_with_response<SchemaTableLookup, TableLookup, TableSerialize>(
    input: &mut impl Read,
    output: &mut impl Tape,
    schema_table_lookup: SchemaTableLookup,
    table_lookup: TableLookup,
    table_serialize: TableSerialize,
) -> Result<()>
where
    SchemaTableLookup: Fn(&[u8], &[u8]) -> Result<u32>,
    TableLookup: Fn(&[u8]) -> Result<u32>,
    TableSerialize: Fn(u32, bool, &mut dyn Write) -> Result<()>,
{
    write_header(output, &Header::default())?;
    let len_init = output.uncommitted_len();
    debug_assert_eq!(len_init, Header::estimate_size());

    let table_num = read_array_len(input)?;
    write_array_len(output, table_num)?;

    for _ in 0..table_num {
        let elem_num = read_array_len(input)?;
        let (table_id, need_schema) = match elem_num {
            1 => {
                let mut table_name = SmallVec::<[u8; NAMEDATALEN]>::new();
                let table_len = read_bin_len(input)?;
                table_name.resize(table_len as usize, 0);
                let len = input.read(&mut table_name)?;
                debug_assert_eq!(table_len as usize, len);
                (table_lookup(table_name.as_slice())?, false)
            }
            2 => {
                let mut schema_name = SmallVec::<[u8; NAMEDATALEN]>::new();
                let mut table_name = SmallVec::<[u8; NAMEDATALEN]>::new();
                let schema_len = read_bin_len(input)?;
                schema_name.resize(schema_len as usize, 0);
                let len = input.read(&mut schema_name)?;
                debug_assert_eq!(schema_len as usize, len);
                let table_len = read_bin_len(input)?;
                table_name.resize(table_len as usize, 0);
                let len = input.read(&mut table_name)?;
                debug_assert_eq!(table_len as usize, len);
                let table_id = schema_table_lookup(schema_name.as_slice(), table_name.as_slice())?;
                (table_id, true)
            }
            _ => {
                bail!(FusionError::InvalidName(
                    "Table".into(),
                    "support only 'schema.table' format".into(),
                ));
            }
        };
        table_serialize(table_id, need_schema, output)?;
    }
    let len_final = output.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToServer,
        packet: Packet::Metadata,
        length,
        flag: Flag::Last,
    };
    output.rollback();
    write_header(output, &header)?;
    output.fast_forward(length as u32)?;
    debug_assert_eq!(output.uncommitted_len(), len_final);
    output.flush()?;
    debug_assert_eq!(output.uncommitted_len(), 0);
    Ok(())
}
