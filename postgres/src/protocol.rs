use std::str::from_utf8;
use std::sync::Arc;

use crate::data_type::{datum_to_scalar, read_scalar_value, write_scalar_value, EncodedType};
use crate::error::FusionError;
use crate::ipc::{max_backends, worker_id, Bus, Slot, SlotNumber, SlotStream, DATA_SIZE};
use crate::sql::Table;
use ahash::AHashMap;
use anyhow::Result;
use datafusion::arrow::datatypes::{Field, Fields, Schema};
use datafusion::logical_expr::TableSource;
use datafusion::scalar::ScalarValue;
use datafusion_sql::TableReference;
use pgrx::pg_sys::{Oid, ParamExternData, ProcSendSignal};
use pgrx::prelude::*;
use pgrx::{pg_guard, PgRelation};
use rmp::decode::{
    read_array_len, read_bool, read_pfix, read_str_len, read_u16, read_u32, read_u8,
};
use rmp::encode::{
    write_array_len, write_bin_len, write_bool, write_pfix, write_str, write_u16, write_u32,
    write_u8, RmpWrite,
};
use smol_str::SmolStr;

#[repr(u8)]
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum Direction {
    #[default]
    ToWorker = 0,
    ToBackend = 1,
}

impl TryFrom<u8> for Direction {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Direction::ToWorker),
            1 => Ok(Direction::ToBackend),
            _ => Err(FusionError::Deserialize(
                "direction".to_string(),
                value.into(),
            )),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Packet {
    #[default]
    None = 0,
    Bind = 1,
    Failure = 2,
    Metadata = 3,
    Parse = 4,
    Explain = 5,
    Columns = 6,
}

impl TryFrom<u8> for Packet {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Packet::None),
            1 => Ok(Packet::Bind),
            2 => Ok(Packet::Failure),
            3 => Ok(Packet::Metadata),
            4 => Ok(Packet::Parse),
            5 => Ok(Packet::Explain),
            6 => Ok(Packet::Columns),
            _ => Err(FusionError::Deserialize("packet".to_string(), value.into())),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Default, Debug, PartialEq)]
pub(crate) enum Flag {
    More = 0,
    #[default]
    Last = 1,
}

impl TryFrom<u8> for Flag {
    type Error = FusionError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        assert!(value < 128);
        match value {
            0 => Ok(Flag::More),
            1 => Ok(Flag::Last),
            _ => Err(FusionError::Deserialize("flag".to_string(), value.into())),
        }
    }
}

#[derive(Default, Debug, PartialEq)]
pub(crate) struct Header {
    pub(crate) direction: Direction,
    pub(crate) packet: Packet,
    pub(crate) flag: Flag,
    pub(crate) length: u16,
}

impl Header {
    const fn estimate_size() -> usize {
        // direction (1 byte) + packet(1 byte) + flag (1 byte) + length (3 bytes)
        1 + 1 + 1 + 3
    }

    const fn payload_max_size() -> usize {
        DATA_SIZE - Self::estimate_size()
    }
}

pub(crate) fn signal(slot_id: SlotNumber, direction: Direction) {
    match direction {
        Direction::ToWorker => {
            unsafe { ProcSendSignal(worker_id()) };
        }
        Direction::ToBackend => {
            let id = Bus::new(max_backends() as usize).slot(slot_id).owner();
            unsafe { ProcSendSignal(id) };
        }
    }
}

// HEADER

pub(crate) fn consume_header(stream: &mut SlotStream) -> Result<Header> {
    assert_eq!(stream.position(), 0);
    let direction = Direction::try_from(read_pfix(stream)?)?;
    let packet = Packet::try_from(read_pfix(stream)?)?;
    let flag = Flag::try_from(read_pfix(stream)?)?;
    let length = read_u16(stream)?;
    Ok(Header {
        direction,
        packet,
        flag,
        length,
    })
}

pub(crate) fn write_header(stream: &mut SlotStream, header: &Header) -> Result<()> {
    write_pfix(stream, header.direction.to_owned() as u8)?;
    write_pfix(stream, header.packet.to_owned() as u8)?;
    write_pfix(stream, header.flag.to_owned() as u8)?;
    write_u16(stream, header.length.to_owned())?;
    Ok(())
}

// PARSE

/// Reads the query from the stream, but leaves the stream position at the beginning of the query.
/// It is required to return the reference to the query bytes without copying them. It is the
/// caller's responsibility to move the stream position to the end of the query.
///
/// Returns the query and its length.
pub(crate) fn read_query(stream: &mut SlotStream) -> Result<(&str, u32)> {
    let len = read_str_len(stream)?;
    let buf = stream.look_ahead(len as usize)?;
    let query = std::str::from_utf8(buf)?;
    Ok((query, len))
}

pub(crate) fn prepare_query(stream: &mut SlotStream, query: &str) -> Result<()> {
    stream.reset();
    // slot: header - bin marker - bin length - query bytes
    let length = 1 + 1 + query.len();
    if length > Header::payload_max_size() {
        return Err(FusionError::PayloadTooLarge(query.len()).into());
    }
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Parse,
        length: length as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_str(stream, query)?;
    Ok(())
}

pub(crate) fn send_query(slot_id: SlotNumber, mut stream: SlotStream, query: &str) -> Result<()> {
    prepare_query(&mut stream, query)?;
    // Unlock the slot after writing the query.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToWorker);
    Ok(())
}

// BIND

pub(crate) fn prepare_params(stream: &mut SlotStream, params: &[ParamExternData]) -> Result<()> {
    stream.reset();
    // We don't know the length of the parameters yet. So we write an invalid header
    // to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let pos_init = stream.position();
    write_array_len(stream, u32::try_from(params.len())?)?;
    for param in params {
        let value = datum_to_scalar(param.value, param.ptype, param.isnull)?;
        write_scalar_value(stream, &value)?;
    }
    let pos_final = stream.position();
    let length = u16::try_from(pos_final - pos_init)?;
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Bind,
        length,
        flag: Flag::Last,
    };
    stream.reset();
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}

pub(crate) fn read_params(stream: &mut SlotStream) -> Result<Vec<ScalarValue>> {
    let len = read_array_len(stream)?;
    let mut params = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let value = read_scalar_value(stream)?;
        params.push(value);
    }
    Ok(params)
}

pub(crate) fn send_params(
    slot_id: SlotNumber,
    mut stream: SlotStream,
    params: &[ParamExternData],
) -> Result<()> {
    prepare_params(&mut stream, params)?;
    // Unlock the slot after writing the parameters.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToWorker);
    Ok(())
}

pub(crate) fn request_params(stream: &mut SlotStream) -> Result<()> {
    stream.reset();
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Bind,
        length: 0,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    Ok(())
}

// FAILURE

pub(crate) fn read_error(stream: &mut SlotStream) -> Result<String> {
    let len = read_str_len(stream)?;
    let buf = stream.look_ahead(len as usize)?;
    let message = std::str::from_utf8(buf)?.to_string();
    Ok(message)
}

fn prepare_error(stream: &mut SlotStream, message: &str) -> Result<()> {
    stream.reset();
    let length = 1 + 1 + u32::try_from(message.len())?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Failure,
        length: length as u16,
        flag: Flag::Last,
    };
    write_header(stream, &header)?;
    write_str(stream, message)?;
    Ok(())
}

pub(crate) fn send_error(slot_id: SlotNumber, mut stream: SlotStream, message: &str) -> Result<()> {
    prepare_error(&mut stream, message)?;
    // Unlock the slot after writing the error message.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToBackend);
    Ok(())
}

#[inline]
fn write_c_str(stream: &mut SlotStream, s: &str) -> Result<()> {
    let len = u32::try_from(s.len())?;
    write_bin_len(stream, len + 1)?;
    stream.write_bytes(s.as_bytes())?;
    write_pfix(stream, 0)?;
    Ok(())
}

// METADATA

/// Writes a table reference as null-terminated strings to
/// the stream. It would be used by the Rust wrappers to the
/// C code, so if we serialize the table and schema as
/// null-terminated strings, we can avoid copying on
/// deserialization.
#[inline]
fn write_table_ref(stream: &mut SlotStream, table: &TableReference) -> Result<()> {
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

pub(crate) fn prepare_table_refs(stream: &mut SlotStream, tables: &[TableReference]) -> Result<()> {
    stream.reset();
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
    stream.reset();
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}

#[inline]
#[pg_guard]
fn serialize_table(rel_oid: Oid, need_schema: bool, stream: &mut SlotStream) -> Result<()> {
    // The destructor will release the lock.
    let rel = unsafe { PgRelation::with_lock(rel_oid, pg_sys::AccessShareLock as i32) };
    if need_schema {
        write_array_len(stream, 3)?;
        write_u32(stream, rel_oid.as_u32())?;
        write_str(stream, rel.namespace())?;
        write_str(stream, rel.name())?;
    } else {
        write_array_len(stream, 2)?;
        write_u32(stream, rel_oid.as_u32())?;
        write_str(stream, rel.name())?;
    }
    let tuple_desc = rel.tuple_desc();
    let attr_num = u32::try_from(tuple_desc.iter().filter(|a| !a.is_dropped()).count())?;
    write_array_len(stream, attr_num)?;
    for attr in tuple_desc.iter() {
        if attr.is_dropped() {
            continue;
        }
        write_array_len(stream, 3)?;
        let etype = EncodedType::try_from(attr.type_oid().value())?;
        write_u8(stream, etype as u8)?;
        let is_nullable = !attr.attnotnull;
        write_bool(stream, is_nullable)?;
        let name = attr.name();
        write_str(stream, name)?;
    }
    Ok(())
}

pub(crate) type NeedSchema = bool;

pub(crate) fn prepare_metadata(
    rel_oids: &[(Oid, NeedSchema)],
    stream: &mut SlotStream,
) -> Result<()> {
    stream.reset();
    // We don't know the length of the table metadata yet. So we write
    // an invalid header to replace it with the correct one later.
    write_header(stream, &Header::default())?;
    let pos_init = stream.position();
    write_array_len(stream, rel_oids.len() as u32)?;
    for &(rel_oid, need_schema) in rel_oids {
        serialize_table(rel_oid, need_schema, stream)?;
    }
    let pos_final = stream.position();
    let length = u16::try_from(pos_final - pos_init)?;
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Metadata,
        length,
        flag: Flag::Last,
    };
    stream.reset();
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}

pub(crate) fn prepare_empty_metadata(stream: &mut SlotStream) -> Result<()> {
    stream.reset();
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

pub(crate) fn send_metadata(
    slot_id: SlotNumber,
    mut stream: SlotStream,
    rel_oids: &[(Oid, NeedSchema)],
) -> Result<()> {
    prepare_metadata(rel_oids, &mut stream)?;
    // Unlock the slot after writing the metadata.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToWorker);
    Ok(())
}

#[inline]
pub(crate) fn consume_metadata(
    stream: &mut SlotStream,
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
        let table = Table::new(Oid::from(oid), Arc::new(schema));
        tables.insert(table_ref, Arc::new(table) as Arc<dyn TableSource>);
    }
    Ok(tables)
}

// EXPLAIN

pub(crate) fn prepare_explain(stream: &mut SlotStream, explain: &str) -> Result<()> {
    stream.reset();
    let header = Header::default();
    write_header(stream, &header)?;
    let pos_init = stream.position();
    write_c_str(stream, explain)?;
    let pos_final = stream.position();
    let length = u16::try_from(pos_final - pos_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Explain,
        length,
        flag: Flag::Last,
    };
    stream.reset();
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}

pub(crate) fn request_explain(slot_id: SlotNumber, mut stream: SlotStream) -> Result<()> {
    let header = Header {
        direction: Direction::ToWorker,
        packet: Packet::Explain,
        length: 0,
        flag: Flag::Last,
    };
    write_header(&mut stream, &header)?;
    // Unlock the slot after writing the explain.
    let _guard = Slot::from(stream);
    signal(slot_id, Direction::ToWorker);
    Ok(())
}

// COLUMNS

pub(crate) fn prepare_columns(stream: &mut SlotStream, columns: &Fields) -> Result<()> {
    stream.reset();
    write_header(stream, &Header::default())?;
    let pos_init = stream.position();
    write_array_len(stream, u32::try_from(columns.len())?)?;
    for column in columns {
        write_u8(stream, EncodedType::try_from(column.data_type())? as u8)?;
        let len = u32::try_from(column.name().len() + 1)?;
        write_bin_len(stream, len)?;
        stream.write_bytes(column.name().as_bytes())?;
        write_pfix(stream, 0)?;
    }
    let pos_final = stream.position();
    let length = u16::try_from(pos_final - pos_init)?;
    let header = Header {
        direction: Direction::ToBackend,
        packet: Packet::Columns,
        length,
        flag: Flag::Last,
    };
    stream.reset();
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::ipc::tests::{make_slot, SLOT_SIZE};

    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use pgrx::pg_sys::{Datum, Oid};
    use rmp::decode::{read_bin_len, read_bool, read_u32};

    #[pg_test]
    fn test_header() {
        let header = Header {
            direction: Direction::ToWorker,
            packet: Packet::Parse,
            length: 42,
            flag: Flag::Last,
        };
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();
        write_header(&mut stream, &header).unwrap();
        stream.reset();
        let new_header = consume_header(&mut stream).unwrap();
        assert_eq!(header, new_header);
    }

    #[pg_test]
    fn test_query() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();
        let sql = "SELECT 1";
        prepare_query(&mut stream, sql).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToWorker);
        assert_eq!(header.packet, Packet::Parse);
        assert_eq!(header.flag, Flag::Last);
        assert_eq!(header.length, 2 + sql.len() as u16);
        let (query, len) = read_query(&mut stream).unwrap();
        assert_eq!(query, sql);
        assert_eq!(len as usize, sql.len());
    }

    #[pg_test]
    fn test_params() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();
        let p1 = ParamExternData {
            value: Datum::from(1),
            ptype: pg_sys::INT4OID,
            isnull: false,
            pflags: 0,
        };
        let p2 = ParamExternData {
            value: Datum::from(0),
            ptype: pg_sys::INT4OID,
            isnull: true,
            pflags: 0,
        };
        prepare_params(&mut stream, &[p1, p2]).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToWorker);
        assert_eq!(header.packet, Packet::Bind);
        assert_eq!(header.flag, Flag::Last);
        let params = read_params(&mut stream).unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], ScalarValue::Int32(Some(1)));
        assert_eq!(params[1], ScalarValue::Int32(None));
    }

    #[pg_test]
    fn test_error() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();
        let message = "An error occurred";
        prepare_error(&mut stream, message).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToBackend);
        assert_eq!(header.packet, Packet::Failure);
        assert_eq!(header.flag, Flag::Last);
        assert_eq!(header.length, 2 + message.len() as u16);
        let error = read_error(&mut stream).unwrap();
        assert_eq!(error, message);
    }

    #[pg_test]
    fn test_table_request() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();
        let t1 = TableReference::bare("table1");
        let t2 = TableReference::partial("schema", "table2");
        let tables = vec![t1, t2];
        prepare_table_refs(&mut stream, &tables).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToBackend);
        assert_eq!(header.packet, Packet::Metadata);
        assert_eq!(header.flag, Flag::Last);

        // check table deserialization
        let table_num = read_array_len(&mut stream).unwrap();
        assert_eq!(table_num, 2);
        // table1
        let elem_num = read_array_len(&mut stream).unwrap();
        assert_eq!(elem_num, 1);
        let t1_len = read_bin_len(&mut stream).unwrap();
        assert_eq!(t1_len as usize, "table1".len() + 1);
        let t1 = stream.look_ahead(t1_len as usize).unwrap();
        assert_eq!(t1, b"table1\0");
        stream.rewind(t1_len as usize).unwrap();
        // schema.table2
        let elem_num = read_array_len(&mut stream).unwrap();
        assert_eq!(elem_num, 2);
        let s_len = read_bin_len(&mut stream).unwrap();
        assert_eq!(s_len as usize, "schema".len() + 1);
        let s = stream.look_ahead(s_len as usize).unwrap();
        assert_eq!(s, b"schema\0");
        stream.rewind(s_len as usize).unwrap();
        let t2_len = read_bin_len(&mut stream).unwrap();
        assert_eq!(t2_len as usize, "table2".len() + 1);
        let t2 = stream.look_ahead(t2_len as usize).unwrap();
        assert_eq!(t2, b"table2\0");
    }

    #[pg_test]
    fn test_metadata_response() {
        Spi::run("create table if not exists t1(a int not null, b text);").unwrap();
        Spi::run("create schema if not exists s1;").unwrap();
        Spi::run("create table if not exists s1.t2(a int);").unwrap();
        let t1_oid = Spi::get_one::<Oid>("select 't1'::regclass::oid;")
            .unwrap()
            .unwrap();
        let t2_oid = Spi::get_one::<Oid>("select 's1.t2'::regclass::oid;")
            .unwrap()
            .unwrap();

        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();

        prepare_metadata(&[(t1_oid, false), (t2_oid, true)], &mut stream).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToWorker);
        assert_eq!(header.packet, Packet::Metadata);
        assert_eq!(header.flag, Flag::Last);

        // Check table metadata deserialization
        let table_num = read_array_len(&mut stream).unwrap();
        assert_eq!(table_num, 2);
        // t1
        let name_part_num = read_array_len(&mut stream).unwrap();
        assert_eq!(name_part_num, 2);
        let oid = read_u32(&mut stream).unwrap();
        assert_eq!(oid, t1_oid.as_u32());
        let name_len = read_str_len(&mut stream).unwrap();
        let name = stream.look_ahead(name_len as usize).unwrap();
        assert_eq!(name, b"t1");
        stream.rewind(name_len as usize).unwrap();
        let attr_num = read_array_len(&mut stream).unwrap();
        assert_eq!(attr_num, 2);
        // a
        let elem_num = read_array_len(&mut stream).unwrap();
        assert_eq!(elem_num, 3);
        let etype = read_u8(&mut stream).unwrap();
        assert_eq!(etype, EncodedType::Int32 as u8);
        let is_nullable = read_bool(&mut stream).unwrap();
        assert!(!is_nullable);
        let name_len = read_str_len(&mut stream).unwrap();
        assert_eq!(name_len, "a".len() as u32);
        let name = stream.look_ahead(name_len as usize).unwrap();
        assert_eq!(name, b"a");
        stream.rewind(name_len as usize).unwrap();
        // b
        let elem_num = read_array_len(&mut stream).unwrap();
        assert_eq!(elem_num, 3);
        let etype = read_u8(&mut stream).unwrap();
        assert_eq!(etype, EncodedType::Utf8 as u8);
        let is_nullable = read_bool(&mut stream).unwrap();
        assert!(is_nullable);
        let name_len = read_str_len(&mut stream).unwrap();
        assert_eq!(name_len, "b".len() as u32);
        let name = stream.look_ahead(name_len as usize).unwrap();
        assert_eq!(name, b"b");
        stream.rewind(name_len as usize).unwrap();

        // s1.t2
        let name_part_num = read_array_len(&mut stream).unwrap();
        assert_eq!(name_part_num, 3);
        let oid = read_u32(&mut stream).unwrap();
        assert_eq!(oid, t2_oid.as_u32());
        let ns_len = read_str_len(&mut stream).unwrap();
        assert_eq!(ns_len, "s1".len() as u32);
        let ns = stream.look_ahead(ns_len as usize).unwrap();
        assert_eq!(ns, b"s1");
        stream.rewind(ns_len as usize).unwrap();
        let name_len = read_str_len(&mut stream).unwrap();
        assert_eq!(name_len, "t2".len() as u32);
        let name = stream.look_ahead(name_len as usize).unwrap();
        assert_eq!(name, b"t2");
        stream.rewind(name_len as usize).unwrap();
        let attr_num = read_array_len(&mut stream).unwrap();
        assert_eq!(attr_num, 1);
        // a
        let elem_num = read_array_len(&mut stream).unwrap();
        assert_eq!(elem_num, 3);
        let etype = read_u8(&mut stream).unwrap();
        assert_eq!(etype, EncodedType::Int32 as u8);
        let is_nullable = read_bool(&mut stream).unwrap();
        assert!(is_nullable);
        let name_len = read_str_len(&mut stream).unwrap();
        assert_eq!(name_len, "a".len() as u32);
        let name = stream.look_ahead(name_len as usize).unwrap();
        assert_eq!(name, b"a");
        stream.rewind(name_len as usize).unwrap();
    }

    #[pg_test]
    fn test_metadata_to_tables() {
        Spi::run("create table if not exists t1(a int not null, b text);").unwrap();
        Spi::run("create schema if not exists s1;").unwrap();
        Spi::run("create table if not exists s1.t2(a int);").unwrap();
        let t1_oid = Spi::get_one::<Oid>("select 't1'::regclass::oid;")
            .unwrap()
            .unwrap();
        let t2_oid = Spi::get_one::<Oid>("select 's1.t2'::regclass::oid;")
            .unwrap()
            .unwrap();

        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();

        prepare_metadata(&[(t1_oid, false), (t2_oid, true)], &mut stream).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToWorker);
        assert_eq!(header.packet, Packet::Metadata);
        assert_eq!(header.flag, Flag::Last);

        let tables = consume_metadata(&mut stream).unwrap();
        assert_eq!(tables.len(), 2);
        // t1
        let t1 = tables.get(&TableReference::bare("t1")).unwrap();
        assert_eq!(t1.schema().fields().len(), 2);
        assert_eq!(t1.schema().field(0).name(), "a");
        assert_eq!(t1.schema().field(1).name(), "b");
        assert_eq!(t1.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(t1.schema().field(1).data_type(), &DataType::Utf8);
        assert!(!t1.schema().field(0).is_nullable());
        assert!(t1.schema().field(1).is_nullable());
        // s1.t2
        let t2 = tables.get(&TableReference::partial("s1", "t2")).unwrap();
        assert_eq!(t2.schema().fields().len(), 1);
        assert_eq!(t2.schema().field(0).name(), "a");
        assert_eq!(t2.schema().field(0).data_type(), &DataType::Int32);
        assert!(t2.schema().field(0).is_nullable());
    }

    #[pg_test]
    fn test_explain() {
        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut slot_buf).into();
        let orig_explain = r#"Projection: * [a:Int32;N, b:Utf8]
  Filter: foo.a = $1 [a:Int32;N, b:Utf8]
    TableScan: foo [a:Int32;N, b:Utf8]"#;
        prepare_explain(&mut stream, orig_explain).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        assert_eq!(header.direction, Direction::ToBackend);
        assert_eq!(header.packet, Packet::Explain);
        assert_eq!(header.flag, Flag::Last);
        let explain_len = read_bin_len(&mut stream).unwrap();
        assert_eq!(explain_len as usize, orig_explain.len() + 1);
        let explain = stream.look_ahead(explain_len as usize).unwrap();
        let expected = format!("{}\0", orig_explain);
        assert_eq!(explain, expected.as_bytes());
    }
}
