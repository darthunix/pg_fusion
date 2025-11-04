use executor::buffer::LockFreeBuffer;
use executor::layout::lockfree_buffer_layout;
use protocol::heap::{
    heap_bitmap_positions, prepare_heap_block_bitmap, prepare_heap_block_eof,
    read_heap_block_bitmap_meta, read_heap_block_eof, read_heap_block_request, request_heap_block,
};
use protocol::{consume_header, DataPacket, Direction, Flag};

#[test]
fn test_request_and_read_block_request_lockfree() {
    // Allocate aligned memory for LockFreeBuffer using layout helper
    let layout = lockfree_buffer_layout(256).expect("layout");
    let base = unsafe { std::alloc::alloc_zeroed(layout.layout) };
    assert!(!base.is_null());
    let mut buf = unsafe { LockFreeBuffer::from_layout(base, layout) };

    let scan_id = 1001u64;
    let table_oid = 42u32;
    let slot_id = 7u16;
    request_heap_block(&mut buf, scan_id, table_oid, slot_id).expect("request_heap_block");

    let header = consume_header(&mut buf).expect("header");
    assert_eq!(header.direction, Direction::ToClient);
    assert_eq!(header.tag, DataPacket::Heap as u8);
    assert_eq!(header.flag, Flag::Last);
    assert_eq!(
        header.length as usize,
        core::mem::size_of::<u64>() + core::mem::size_of::<u32>() + core::mem::size_of::<u16>()
    );

    let (sid, t, s) = read_heap_block_request(&mut buf).expect("read request");
    assert_eq!(sid, scan_id);
    assert_eq!(t, table_oid);
    assert_eq!(s, slot_id);

    unsafe { std::alloc::dealloc(base, layout.layout) };
}

#[test]
fn test_prepare_and_read_block_bitmap_lockfree() {
    let layout = lockfree_buffer_layout(512).expect("layout");
    let base = unsafe { std::alloc::alloc_zeroed(layout.layout) };
    assert!(!base.is_null());
    let mut buf = unsafe { LockFreeBuffer::from_layout(base, layout) };

    let scan_id = 55u64;
    let slot_id = 3u16;
    let table_oid = 777u32;
    let blkno = 1234u32;
    let num_offsets = 17u16; // 17 offsets -> bitmap length 3 bytes

    // Construct a 3-byte bitmap (LSB-first within each byte)
    let byte0: u8 = (1 << 0) | (1 << 2) | (1 << 7); // offsets 1,3,8
    let byte1: u8 = (1 << 1) | (1 << 5); // offsets 10,14
    let byte2: u8 = 1; // offset 17
    let bitmap = [byte0, byte1, byte2];

    // Convert bitmap slice to positions iterator
    let positions = (1usize..=num_offsets as usize).filter(|off| {
        let idx = off - 1;
        let b = bitmap[idx / 8];
        let bit = 1u8 << (idx as u8 % 8);
        (b & bit) != 0
    });
    prepare_heap_block_bitmap(&mut buf, scan_id, slot_id, table_oid, blkno, num_offsets, positions)
        .expect("prepare_heap_block_bitmap");

    let header = consume_header(&mut buf).expect("header");
    assert_eq!(header.direction, Direction::ToServer);
    assert_eq!(header.tag, DataPacket::Heap as u8);
    assert_eq!(header.flag, Flag::Last);

    let meta = read_heap_block_bitmap_meta(&mut buf, header.length).expect("bitmap meta");
    assert_eq!(meta.slot_id, slot_id);
    assert_eq!(meta.scan_id, scan_id);
    assert_eq!(meta.table_oid, table_oid);
    assert_eq!(meta.blkno, blkno);
    assert_eq!(meta.num_offsets, num_offsets);
    assert_eq!(meta.bitmap_len as usize, bitmap.len());
    // Iterate set positions directly from the buffer (consuming bytes)
    let positions: Vec<usize> =
        heap_bitmap_positions(&mut buf, meta.num_offsets, meta.bitmap_len).collect();
    // trailing u16 (bitmap length) should remain in the buffer; read and verify
    let trailing_len = rmp::decode::read_u16(&mut buf).expect("read trailing msgpack u16");
    assert_eq!(trailing_len as usize, bitmap.len());
    assert_eq!(positions, vec![1, 3, 8, 10, 14, 17]);

    unsafe { std::alloc::dealloc(base, layout.layout) };
}

#[test]
fn test_prepare_block_eof_lockfree() {
    let layout = lockfree_buffer_layout(64).expect("layout");
    let base = unsafe { std::alloc::alloc_zeroed(layout.layout) };
    assert!(!base.is_null());
    let mut buf = unsafe { LockFreeBuffer::from_layout(base, layout) };

    let scan_id = 999u64;
    let slot_id = 1u16;
    prepare_heap_block_eof(&mut buf, scan_id, slot_id).expect("eof");

    let header = consume_header(&mut buf).expect("header");
    assert_eq!(header.direction, Direction::ToServer);
    assert_eq!(header.tag, DataPacket::Heap as u8);
    assert_eq!(header.length, (core::mem::size_of::<u64>() + core::mem::size_of::<u16>()) as u16);
    let (sid, echoed) = read_heap_block_eof(&mut buf).expect("read eof");
    assert_eq!(sid, scan_id);
    assert_eq!(echoed, slot_id);

    unsafe { std::alloc::dealloc(base, layout.layout) };
}
