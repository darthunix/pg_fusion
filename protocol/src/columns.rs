use crate::data_type::EncodedType;
use crate::metadata::NAMEDATALEN;
use crate::{write_header, ControlPacket, Direction, Flag, Header, Tape};
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
        // Also send nullability so backend can reflect it in ColumnLayout
        rmp::encode::write_bool(stream, column.is_nullable())?;
    }
    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToClient,
        tag: ControlPacket::Columns as u8,
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
    mut repack: ColumnRepack,
) -> Result<()>
where
    ColumnRepack: FnMut(i16, u8, bool, &[u8], *mut c_void) -> Result<()>,
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
        let nullable = rmp::decode::read_bool(stream)?;
        repack(pos as i16, etype, nullable, column_name.as_slice(), opaque)?;
    }
    Ok(())
}
