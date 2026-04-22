use crate::error::{DecodeError, EncodeError};
use rmp::decode::{
    read_array_len, read_marker, read_str_len, read_u16, read_u64, read_u8, RmpRead,
};
use rmp::encode::{write_array_len, write_nil, write_str, write_u16, write_u64, write_u8};
use rmp::Marker;
use std::io::{self, Write};

pub(crate) fn read_array_len_from(source: &mut &[u8]) -> Result<u32, DecodeError> {
    read_array_len(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

pub(crate) fn read_u8_from(source: &mut &[u8]) -> Result<u8, DecodeError> {
    read_u8(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

pub(crate) fn read_u32_from(source: &mut &[u8]) -> Result<u32, DecodeError> {
    rmp::decode::read_u32(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

pub(crate) fn read_u16_from(source: &mut &[u8]) -> Result<u16, DecodeError> {
    read_u16(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

pub(crate) fn read_u64_from(source: &mut &[u8]) -> Result<u64, DecodeError> {
    read_u64(source).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

pub(crate) fn read_str_from<'a>(source: &mut &'a [u8]) -> Result<&'a str, DecodeError> {
    let len = read_str_len(source).map_err(|error| DecodeError::MsgPack(error.to_string()))?;
    let len = usize::try_from(len)
        .map_err(|_| DecodeError::MsgPack("string length does not fit into usize".to_string()))?;
    if source.len() < len {
        return Err(DecodeError::MsgPack(format!(
            "truncated string payload: expected {len} bytes, got {}",
            source.len()
        )));
    }
    let (bytes, tail) = source.split_at(len);
    *source = tail;
    std::str::from_utf8(bytes).map_err(|error| DecodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_array_len_to<W: Write>(sink: &mut W, len: u32) -> Result<(), EncodeError> {
    write_array_len(sink, len)
        .map(|_| ())
        .map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_u8_to<W: Write>(sink: &mut W, value: u8) -> Result<(), EncodeError> {
    write_u8(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_u32_to<W: Write>(sink: &mut W, value: u32) -> Result<(), EncodeError> {
    rmp::encode::write_u32(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_u16_to<W: Write>(sink: &mut W, value: u16) -> Result<(), EncodeError> {
    write_u16(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_u64_to<W: Write>(sink: &mut W, value: u64) -> Result<(), EncodeError> {
    write_u64(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_str_to<W: Write>(sink: &mut W, value: &str) -> Result<(), EncodeError> {
    write_str(sink, value).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_nil_to<W: Write>(sink: &mut W) -> Result<(), EncodeError> {
    write_nil(sink).map_err(|error| EncodeError::MsgPack(error.to_string()))
}

pub(crate) fn write_optional_u64_to<W: Write>(
    sink: &mut W,
    value: Option<u64>,
) -> Result<(), EncodeError> {
    match value {
        Some(value) => write_u64_to(sink, value),
        None => write_nil_to(sink),
    }
}

pub(crate) fn read_optional_u64_from(source: &mut &[u8]) -> Result<Option<u64>, DecodeError> {
    match read_marker(source).map_err(|error| DecodeError::MsgPack(format!("{error:?}")))? {
        Marker::Null => Ok(None),
        Marker::FixPos(value) => Ok(Some(value as u64)),
        Marker::U8 => source
            .read_data_u8()
            .map(|value| Some(value as u64))
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        Marker::U16 => source
            .read_data_u16()
            .map(|value| Some(value as u64))
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        Marker::U32 => source
            .read_data_u32()
            .map(|value| Some(value as u64))
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        Marker::U64 => source
            .read_data_u64()
            .map(Some)
            .map_err(|error| DecodeError::MsgPack(error.to_string())),
        marker => Err(DecodeError::MsgPack(format!(
            "expected nil or unsigned integer detail, got {marker:?}"
        ))),
    }
}

pub(crate) fn expect_message_len(actual: u32, expected: u32) -> Result<(), DecodeError> {
    if actual != expected {
        return Err(DecodeError::InvalidArrayLen { expected, actual });
    }
    Ok(())
}

pub(crate) fn encoded_len_with<F>(encode: F) -> Result<usize, EncodeError>
where
    F: FnOnce(&mut CountingWriter) -> Result<(), EncodeError>,
{
    let mut sink = CountingWriter::default();
    encode(&mut sink)?;
    Ok(sink.written)
}

pub(crate) fn encode_into_with_len<F>(
    expected: usize,
    out: &mut [u8],
    encode: F,
) -> Result<usize, EncodeError>
where
    F: FnOnce(&mut [u8]) -> Result<(), EncodeError>,
{
    if out.len() < expected {
        return Err(EncodeError::BufferTooSmall {
            expected,
            actual: out.len(),
        });
    }

    let writer = &mut out[..expected];
    encode(writer)?;
    Ok(expected)
}

#[derive(Default)]
pub(crate) struct CountingWriter {
    written: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.written = self.written.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
