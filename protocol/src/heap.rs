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

// Removed BlockBitmap to prefer zero-copy flow via HeapBitmapMeta

/// Zero-copy friendly metadata for a heap page bitmap.
/// The actual bitmap bytes remain unread in the stream, allowing callers that
/// control the underlying buffer (e.g., a lock-free ring) to access them
/// without allocation and advance the read head manually.
pub struct HeapBitmapMeta {
    pub slot_id: u16,
    pub table_oid: u32,
    pub blkno: u32,
    pub num_offsets: u16,
    pub bitmap_len: u16,
}

// Removed read_heap_block_bitmap allocation-heavy function

/// Read only metadata for the heap page bitmap, leaving the bitmap bytes unread.
pub fn read_heap_block_bitmap_meta(stream: &mut impl Read) -> Result<HeapBitmapMeta> {
    let slot_id = read_u16(stream)?;
    let table_oid = read_u32(stream)?;
    let blkno = read_u32(stream)?;
    let num_offsets = read_u16(stream)?;
    let bitmap_len = read_u16(stream)?;
    Ok(HeapBitmapMeta {
        slot_id,
        table_oid,
        blkno,
        num_offsets,
        bitmap_len,
    })
}

/// Iterator over set-bit positions (1-based offsets) in a heap visibility bitmap.
///
/// Consumes `bitmap_len` bytes from `reader`, interpreting each bit LSB-first as
/// an offset position starting at 1. Yields positions `usize` in ascending order
/// for bits that are set and within `1..=num_offsets`. Trailing pad bits (beyond
/// `num_offsets`) are ignored but still consumed from the reader.
pub struct HeapBitmapIter<'a, R: Read + ?Sized> {
    reader: &'a mut R,
    remaining_bytes: usize,
    next_offset: usize,
    total_offsets: usize,
    current_byte: u8,
    bit_index: u8,
    have_byte: bool,
}

impl<'a, R: Read + ?Sized> Iterator for HeapBitmapIter<'a, R> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.have_byte || self.bit_index >= 8 {
                if self.remaining_bytes == 0 {
                    return None;
                }
                let mut b = [0u8; 1];
                let n = std::io::Read::read(self.reader, &mut b).ok()?;
                if n == 0 {
                    return None;
                }
                self.current_byte = b[0];
                self.bit_index = 0;
                self.have_byte = true;
                self.remaining_bytes -= 1;
            }

            // Inspect the current bit, then advance.
            let pos = self.next_offset;
            let set = (self.current_byte & (1u8 << self.bit_index)) != 0;
            self.bit_index += 1;
            self.next_offset += 1;

            if pos > self.total_offsets {
                // Beyond meaningful offsets: ignore but continue consuming.
                continue;
            }
            if set {
                return Some(pos);
            }
            // Otherwise, loop for the next bit/byte.
        }
    }
}

/// Create an iterator over set positions in the heap bitmap.
pub fn heap_bitmap_positions<'a, R: Read + ?Sized>(
    reader: &'a mut R,
    num_offsets: u16,
    bitmap_len: u16,
) -> HeapBitmapIter<'a, R> {
    HeapBitmapIter {
        reader,
        remaining_bytes: bitmap_len as usize,
        next_offset: 1,
        total_offsets: num_offsets as usize,
        current_byte: 0,
        bit_index: 8,
        have_byte: false,
    }
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
