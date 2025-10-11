use crate::{write_header, Direction, Flag, Header};
use anyhow::Result;

#[inline]
fn write_empty_control(stream: &mut impl std::io::Write, tag: u8) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        tag,
        flag: Flag::Last,
        length: 0,
    };
    write_header(stream, &header)
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

