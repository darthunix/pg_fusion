// moved from block.rs; heap-page oriented messages
use crate::{write_header, Direction, Flag, Header, Packet, Tape};
use anyhow::Result;
use rmp::decode::{read_u16, read_u32};
use rmp::encode::{write_u16, write_u32};
use std::io::{Read, Write};

/// Request a new table heap page to be copied into a shared-memory slot.
///
/// Sent by the executor (server) to the backend (client).
/// Payload layout:
/// - u32: table OID
/// - u16: slot id in shared memory where BLCKSZ bytes must be written
pub fn request_heap_block(stream: &mut impl Write, table_oid: u32, slot_id: u16) -> Result<()> {
    let header = Header {
        direction: Direction::ToClient,
        packet: Packet::Heap,
        flag: Flag::Last,
        length: (size_of::<u32>() + size_of::<u16>()) as u16,
    };
    write_header(stream, &header)?;
    write_u32(stream, table_oid)?;
    write_u16(stream, slot_id)?;
    stream.flush()?;
    Ok(())
}

/// Consume a heap page request on the backend.
pub fn read_heap_block_request(stream: &mut impl Read) -> Result<(u32, u16)> {
    let table_oid = read_u32(stream)?;
    let slot_id = read_u16(stream)?;
    Ok((table_oid, slot_id))
}

/// Response with visibility bitmap for the copied heap page.
///
/// Sent by the backend (client) to the executor (server) after it copies
/// the page into the provided `slot_id` and evaluates tuple visibility.
///
/// Payload layout:
/// - u16: slot id (echo)
/// - u32: table OID (echo)
/// - u32: block number
/// - u16: number of meaningful offsets on the page (`num_offsets`),
///        equal to `PageGetMaxOffsetNumber(page)` in PostgreSQL.
///        Offsets are 1-based (1..=num_offsets). The bitmap encodes
///        visibility for each offset; trailing pad bits in the last
///        byte (if any) must be ignored.
/// - u16: bitmap length in bytes
/// - [u8; bitmap_len]: visibility bitmap (1 bit per offset, LSB-first)
pub fn prepare_heap_block_bitmap(
    stream: &mut impl Tape,
    slot_id: u16,
    table_oid: u32,
    blkno: u32,
    // Number of meaningful ItemId slots on the page (1..=num_offsets).
    // This is the max offset number (aka `PageGetMaxOffsetNumber`).
    num_offsets: u16,
    bitmap: &[u8],
) -> Result<()> {
    // Two-phase header write since bitmap is variable-sized
    write_header(stream, &Header::default())?;
    let len_init = stream.uncommitted_len();
    write_u16(stream, slot_id)?;
    write_u32(stream, table_oid)?;
    write_u32(stream, blkno)?;
    // Encode the count of offsets (not the index of the last bit).
    // Consumer must read ceil(num_offsets/8) bytes from `bitmap`.
    write_u16(stream, num_offsets)?;
    write_u16(stream, u16::try_from(bitmap.len())?)?;
    stream.write_all(bitmap)?;

    let len_final = stream.uncommitted_len();
    let length = u16::try_from(len_final - len_init)?;
    let header = Header {
        direction: Direction::ToServer,
        packet: Packet::Heap,
        flag: Flag::Last,
        length,
    };
    stream.rollback();
    write_header(stream, &header)?;
    stream.fast_forward(length as u32)?;
    stream.flush()?;
    Ok(())
}

pub struct BlockBitmap {
    pub slot_id: u16,
    pub table_oid: u32,
    pub blkno: u32,
    /// Number of meaningful offsets on the page (1..=num_offsets).
    pub num_offsets: u16,
    pub bitmap: Vec<u8>,
}

/// Read the heap page bitmap response on the executor.
pub fn read_heap_block_bitmap(stream: &mut impl Read) -> Result<BlockBitmap> {
    let slot_id = read_u16(stream)?;
    let table_oid = read_u32(stream)?;
    let blkno = read_u32(stream)?;
    let num_offsets = read_u16(stream)?;
    let bitmap_len = read_u16(stream)? as usize;
    let mut bitmap = vec![0u8; bitmap_len];
    let read = std::io::Read::read(stream, &mut bitmap)?;
    debug_assert_eq!(read, bitmap_len);
    Ok(BlockBitmap {
        slot_id,
        table_oid,
        blkno,
        num_offsets,
        bitmap,
    })
}

/// Signal end-of-scan: no more heap pages available for the relation.
///
/// To make slot ownership explicit even with deeper pipelines, EOF echoes the
/// `slot_id` (2 bytes) as payload. This lets the consumer free exactly that slot.
pub fn prepare_heap_block_eof(stream: &mut impl Write, slot_id: u16) -> Result<()> {
    let header = Header {
        direction: Direction::ToServer,
        packet: Packet::Heap,
        flag: Flag::Last,
        length: size_of::<u16>() as u16,
    };
    write_header(stream, &header)?;
    write_u16(stream, slot_id)?;
    stream.flush()?;
    Ok(())
}

/// Read EOF payload and return the echoed `slot_id`.
pub fn read_heap_block_eof(stream: &mut impl Read) -> Result<u16> {
    let slot = read_u16(stream)?;
    Ok(slot)
}
