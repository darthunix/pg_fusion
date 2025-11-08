use crate::{write_header, Direction, Flag, Header};
use anyhow::Result;
use rmp::encode::write_u64;

#[inline]
fn write_empty_control(stream: &mut impl std::io::Write, tag: u8) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        tag,
        flag: Flag::Last,
        length: 0,
    };
    write_header(stream, &header)?;
    // Ensure tail is committed for lock-free buffers implementing Write
    stream.flush()?;
    Ok(())
}

pub fn request_begin_scan(stream: &mut impl std::io::Write) -> Result<()> {
    write_empty_control(stream, crate::ControlPacket::BeginScan as u8)
}

pub fn request_exec_scan(stream: &mut impl std::io::Write) -> Result<()> {
    write_empty_control(stream, crate::ControlPacket::ExecScan as u8)
}

pub fn request_end_scan(stream: &mut impl std::io::Write) -> Result<()> {
    write_empty_control(stream, crate::ControlPacket::EndScan as u8)
}

#[inline]
fn write_empty_to_client(stream: &mut impl std::io::Write, tag: u8) -> Result<()> {
    let header = Header {
        direction: Direction::ToClient,
        tag,
        flag: Flag::Last,
        length: 0,
    };
    write_header(stream, &header)?;
    // Ensure tail is committed for lock-free buffers implementing Write
    stream.flush()?;
    Ok(())
}

pub fn prepare_exec_ready(stream: &mut impl std::io::Write) -> Result<()> {
    write_empty_to_client(stream, crate::ControlPacket::ExecReady as u8)
}

/// Send a per-scan EOF notification from backend to executor as a data packet.
/// Payload: u64 scan_id.
pub fn prepare_scan_eof(stream: &mut impl std::io::Write, scan_id: u64) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        tag: crate::DataPacket::Eof as u8,
        flag: Flag::Last,
        length: 9, // msgpack u64
    };
    crate::write_header(stream, &header)?;
    write_u64(stream, scan_id)?;
    stream.flush()?;
    Ok(())
}
