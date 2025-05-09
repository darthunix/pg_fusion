use crate::{
    data_type::EncodedType,
    protocol::{write_header, ByteStream, Direction, Flag, Header, Packet},
};
use anyhow::Result;
use datafusion::arrow::datatypes::Fields;
use rmp::encode::{write_array_len, write_bin_len, write_pfix, write_u8};

pub fn prepare_columns(stream: &mut impl ByteStream, columns: &Fields) -> Result<()> {
    stream.reset()?;
    write_header(stream, &Header::default())?;
    let pos_init = stream.position();
    write_array_len(stream, u32::try_from(columns.len())?)?;
    for column in columns {
        write_u8(stream, EncodedType::try_from(column.data_type())? as u8)?;
        let len = u32::try_from(column.name().len() + 1)?;
        write_bin_len(stream, len)?;
        stream.write_all(column.name().as_bytes())?;
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
    stream.reset()?;
    write_header(stream, &header)?;
    stream.rewind(length as usize)?;
    Ok(())
}
