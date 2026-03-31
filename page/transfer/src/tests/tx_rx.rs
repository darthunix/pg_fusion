use super::*;
use std::io::Write;
use std::thread;

#[test]
fn writer_appends_unknown_length_and_finish_sets_header() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    let mut writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin writer");
    assert_eq!(writer.position(), 0);
    writer.write_all(b"hello").expect("write hello");
    writer.write_all(b" world").expect("write world");
    assert_eq!(writer.position(), 11);
    assert_eq!(writer.remaining(), 64 - PAGE_HEADER_LEN - 11);

    let outbound = writer.finish().expect("finish writer");
    let descriptor = outbound.descriptor();
    let bytes = unsafe { pool.page_bytes(descriptor) }.expect("page bytes");
    let header = decode_page_header(&bytes[..PAGE_HEADER_LEN]).expect("decode page header");
    assert_eq!(header.kind, TEST_KIND);
    assert_eq!(header.flags, TEST_FLAGS);
    assert_eq!(header.payload_len, 11);
    assert_eq!(
        &bytes[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 11],
        b"hello world"
    );

    let frame = outbound.frame();
    outbound.mark_sent();
    match rx.accept(frame).expect("accept frame") {
        ReceiveEvent::Page(page) => {
            assert_eq!(page.kind(), TEST_KIND);
            assert_eq!(page.flags(), TEST_FLAGS);
            assert_eq!(page.payload(), b"hello world");
            page.release().expect("release page");
        }
        ReceiveEvent::Closed => panic!("expected page"),
    }

    assert_eq!(pool.snapshot().free_pages, 1);
}

#[test]
fn payload_mut_and_explicit_length_finish_set_header() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    let mut writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin writer");
    writer.payload_mut()[..11].copy_from_slice(b"hello world");

    let outbound = writer
        .finish_with_payload_len(11)
        .expect("finish explicit length");
    let descriptor = outbound.descriptor();
    let bytes = unsafe { pool.page_bytes(descriptor) }.expect("page bytes");
    let header = decode_page_header(&bytes[..PAGE_HEADER_LEN]).expect("decode page header");
    assert_eq!(header.kind, TEST_KIND);
    assert_eq!(header.flags, TEST_FLAGS);
    assert_eq!(header.payload_len, 11);
    assert_eq!(
        &bytes[PAGE_HEADER_LEN..PAGE_HEADER_LEN + 11],
        b"hello world"
    );

    let frame = outbound.frame();
    outbound.mark_sent();
    match rx.accept(frame).expect("accept frame") {
        ReceiveEvent::Page(page) => {
            assert_eq!(page.payload(), b"hello world");
            page.release().expect("release page");
        }
        ReceiveEvent::Closed => panic!("expected page"),
    }
}

#[test]
fn explicit_length_finish_rejects_payloads_beyond_capacity() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let payload_cap = 64 - PAGE_HEADER_LEN;

    let writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin writer");
    assert!(matches!(
        writer.finish_with_payload_len(payload_cap + 1),
        Err(TxError::PayloadExceedsCapacity { actual, capacity })
            if actual == payload_cap + 1 && capacity == payload_cap
    ));
    assert_eq!(pool.snapshot().free_pages, 1);
}

#[test]
fn writer_drop_rolls_back_and_write_all_errors_when_full() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let payload_cap = 64 - PAGE_HEADER_LEN;

    {
        let mut writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin writer");
        writer
            .write_all(&vec![1u8; payload_cap])
            .expect("fill payload");
        assert_eq!(writer.write(&[1]).expect("extra write"), 0);
        assert!(writer.write_all(&[2]).is_err());
    }

    assert_eq!(pool.snapshot().free_pages, 1);
}

#[test]
fn begin_rejects_page_payload_capacity_larger_than_u32() {
    let max_page_size = PAGE_HEADER_LEN + (u32::MAX as usize);
    assert!(validate_page_size(max_page_size).is_ok());

    let too_large = max_page_size + 1;
    assert!(matches!(
        validate_page_size(too_large),
        Err(TxError::PayloadCapacityTooLarge { capacity, max })
            if capacity == too_large - PAGE_HEADER_LEN && max == u32::MAX as usize
    ));
}

#[test]
fn sender_enforces_single_active_handle_and_close_is_terminal() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    let writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin");
    assert!(matches!(
        tx.begin(TEST_KIND, TEST_FLAGS),
        Err(TxError::Busy)
    ));
    assert!(matches!(tx.close(), Err(TxError::Busy)));
    drop(writer);

    let mut writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin again");
    writer.write_all(b"x").expect("write");
    let outbound = writer.finish().expect("finish");
    let frame = outbound.frame();
    outbound.mark_sent();

    let page = match rx.accept(frame).expect("accept page") {
        ReceiveEvent::Page(page) => page,
        ReceiveEvent::Closed => panic!("expected page"),
    };

    let close = tx.close().expect("close");
    match rx.accept(close).expect("accept close") {
        ReceiveEvent::Closed => {}
        ReceiveEvent::Page(_) => panic!("expected close"),
    }
    drop(page);
    assert!(matches!(
        tx.begin(TEST_KIND, TEST_FLAGS),
        Err(TxError::Closed)
    ));
    assert!(matches!(
        rx.accept(OwnedFrame::Close(crate::wire::CloseFrame {
            transfer_id: 3
        })),
        Err(RxError::Closed)
    ));
}

#[test]
fn receiver_accepts_next_page_while_previous_page_is_alive() {
    let (_region, pool) = init_pool(cfg(64, 2));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    let mut writer = tx.begin(TEST_KIND, 0).expect("begin page1");
    writer.write_all(b"page1").expect("write page1");
    let outbound = writer.finish().expect("finish page1");
    let frame = outbound.frame();
    outbound.mark_sent();

    let page1 = match rx.accept(frame).expect("accept page1") {
        ReceiveEvent::Page(page) => page,
        ReceiveEvent::Closed => panic!("expected page1"),
    };
    assert_eq!(page1.payload(), b"page1");

    let mut writer = tx.begin(TEST_KIND, 0).expect("begin page2");
    writer.write_all(b"page2").expect("write page2");
    let outbound = writer.finish().expect("finish page2");
    let frame = outbound.frame();
    outbound.mark_sent();

    let page2 = match rx.accept(frame).expect("accept page2") {
        ReceiveEvent::Page(page) => page,
        ReceiveEvent::Closed => panic!("expected page2"),
    };
    assert_eq!(page2.payload(), b"page2");

    drop(page2);
    assert_eq!(pool.snapshot().free_pages, 1);
    drop(page1);
    assert_eq!(pool.snapshot().free_pages, 2);
}

#[test]
fn receiver_rejects_wrong_pool_and_previous_pool_lifetime() {
    let (_region_a, pool_a) = init_pool(cfg(64, 1));
    let (_region_b, pool_b) = init_pool(cfg(64, 1));

    let tx_a = PageTx::new(pool_a);
    let rx_b = PageRx::new(pool_b);

    let mut writer = tx_a.begin(TEST_KIND, 0).expect("begin");
    writer.write_all(b"abc").expect("write");
    let outbound = writer.finish().expect("finish");
    let frame = outbound.frame();
    outbound.mark_sent();

    assert!(matches!(
        rx_b.accept(frame),
        Err(RxError::Access(pool::AccessError::PoolMismatch { .. }))
    ));

    let config = cfg(64, 1);
    let layout = PagePool::layout(config).expect("layout");
    let region = OwnedRegion::new(layout);
    let pool_old =
        unsafe { PagePool::init_in_place(region.base, layout.size, config) }.expect("old init");
    let tx_old = PageTx::new(pool_old);
    let mut writer = tx_old.begin(TEST_KIND, 0).expect("begin old");
    writer.write_all(b"old").expect("write old");
    let outbound = writer.finish().expect("finish old");
    let frame = outbound.frame();
    outbound.mark_sent();

    let pool_new =
        unsafe { PagePool::init_in_place(region.base, layout.size, config) }.expect("new init");
    let rx_new = PageRx::new(pool_new);
    assert!(matches!(
        rx_new.accept(frame),
        Err(RxError::Access(pool::AccessError::PoolMismatch { .. }))
    ));
}

#[test]
fn receiver_rejects_unexpected_transfer_id_and_invalid_page_header() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    assert!(matches!(
        rx.accept(OwnedFrame::Close(crate::wire::CloseFrame {
            transfer_id: 2
        })),
        Err(RxError::UnexpectedTransferId {
            expected: 1,
            actual: 2
        })
    ));

    let mut writer = tx.begin(TEST_KIND, 0).expect("begin");
    writer.write_all(b"abc").expect("write");
    let outbound = writer.finish().expect("finish");
    let frame = outbound.frame();
    let desc = outbound.descriptor();
    outbound.mark_sent();

    let bytes = unsafe { pool.page_bytes_mut(desc) }.expect("mut page bytes");
    bytes[1] ^= 1;
    assert!(matches!(
        rx.accept(frame),
        Err(RxError::InvalidPage(InvalidPageError::InvalidMagic { .. }))
    ));
    assert_eq!(pool.snapshot().free_pages, 1);
    let writer = tx
        .begin(TEST_KIND, 0)
        .expect("pool reusable after invalid page");
    drop(writer);
}

#[test]
fn receiver_rejects_oversized_payload_len() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    let mut writer = tx.begin(TEST_KIND, 0).expect("begin");
    writer.write_all(b"abc").expect("write");
    let outbound = writer.finish().expect("finish");
    let frame = outbound.frame();
    let desc = outbound.descriptor();
    outbound.mark_sent();

    let state_word = unsafe { pool.page_bytes_mut(desc) }.expect("page bytes");
    let header = PageHeader {
        kind: TEST_KIND,
        flags: 0,
        payload_len: (64 - PAGE_HEADER_LEN + 1) as u32,
    };
    encode_page_header(header, &mut state_word[..PAGE_HEADER_LEN]).expect("encode header");
    assert!(matches!(
        rx.accept(frame),
        Err(RxError::InvalidPage(
            InvalidPageError::PayloadTooLarge { .. }
        ))
    ));
    assert_eq!(pool.snapshot().free_pages, 1);
    let writer = tx
        .begin(TEST_KIND, 0)
        .expect("pool reusable after oversized payload reject");
    drop(writer);
}

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

#[test]
fn handles_are_send_and_sync_where_expected() {
    assert_send::<PageTx>();
    assert_sync::<PageTx>();
    assert_send::<PageRx>();
    assert_sync::<PageRx>();
    assert_send::<PageWriter>();
    assert_send::<OutboundPage>();
    assert_send::<ReceivedPage>();
    assert_sync::<ReceivedPage>();
}

#[test]
fn owned_wrappers_can_cross_thread_boundaries() {
    let (_region, pool) = init_pool(cfg(64, 1));
    let tx = PageTx::new(pool);
    let rx = PageRx::new(pool);

    let writer = tx.begin(TEST_KIND, TEST_FLAGS).expect("begin");
    let outbound = thread::spawn(move || {
        let mut writer = writer;
        writer.write_all(b"thread hop").expect("write");
        writer.finish().expect("finish")
    })
    .join()
    .expect("writer thread");

    let frame = outbound.frame();
    outbound.mark_sent();

    let page = match rx.accept(frame).expect("accept frame") {
        ReceiveEvent::Page(page) => page,
        ReceiveEvent::Closed => panic!("expected page"),
    };
    let payload = thread::spawn(move || {
        assert_eq!(page.kind(), TEST_KIND);
        assert_eq!(page.flags(), TEST_FLAGS);
        let payload = page.payload().to_vec();
        page.release().expect("release");
        payload
    })
    .join()
    .expect("page thread");

    assert_eq!(payload, b"thread hop");
    assert_eq!(pool.snapshot().free_pages, 1);
}
