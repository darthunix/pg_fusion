use anyhow::Result;
use std::io::{Read, Write};

/// Write a single result row into a stream as a length-prefixed MessagePack payload.
/// The payload must be a MessagePack array of `natts` values, where each value
/// corresponds to a column (nil for NULL).
struct CountingWrite(usize);
impl Write for CountingWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> { self.0 += buf.len(); Ok(buf.len()) }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

/// Write a single-row MessagePack frame with exactly one Utf8 string value.
pub fn write_row1_str(stream: &mut impl Write, s: &str) -> Result<()> {
    // 1st pass: count bytes
    let mut cw = CountingWrite(0);
    rmp::encode::write_array_len(&mut cw, 1)?;
    rmp::encode::write_str(&mut cw, s)?;
    let len = cw.0 as u32;
    // Prefix as MessagePack u32 (always 5 bytes)
    rmp::encode::write_u32(stream, len)?;
    rmp::encode::write_array_len(stream, 1)?;
    rmp::encode::write_str(stream, s)?;
    stream.flush()?;
    Ok(())
}

/// Write an EOF sentinel frame (length=0) to signal no more rows.
pub fn write_eof(stream: &mut impl Write) -> Result<()> {
    rmp::encode::write_u32(stream, 0u32)?;
    stream.flush()?;
    Ok(())
}

/// Read a frame length prefix (LE u32). If 0, indicates EOF sentinel.
pub fn read_frame_len(stream: &mut impl Read) -> Result<u32> {
    let v = rmp::decode::read_u32(stream)?;
    Ok(v)
}
