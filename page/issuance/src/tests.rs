use crate::{
    encode_issued_frame, IssuanceConfig, IssuancePool, IssueEvent, IssuedFrameDecoder, IssuedRx,
    IssuedTx,
};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_layout::init_block;
use arrow_schema::{DataType, Field, Schema};
use batch_encoder::BatchPageEncoder;
use import::ArrowPageDecoder;
use pool::{PagePool, PagePoolConfig};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;
use transfer::{PageRx, PageTx};

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn new(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).expect("layout");
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("allocation");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

fn init_page_pool(page_size: usize, page_count: u32) -> (OwnedRegion, PagePool) {
    let cfg = PagePoolConfig::new(page_size, page_count).expect("pool config");
    let layout = PagePool::layout(cfg).expect("pool layout");
    let region = OwnedRegion::new(layout.size, layout.align);
    let pool = unsafe { PagePool::init_in_place(region.base, layout.size, cfg) }.expect("pool");
    (region, pool)
}

fn init_issuance_pool(permit_count: u32) -> (OwnedRegion, IssuancePool) {
    let cfg = IssuanceConfig::new(permit_count).expect("issuance config");
    let layout = IssuancePool::layout(cfg).expect("issuance layout");
    let region = OwnedRegion::new(layout.size, layout.align);
    let pool =
        unsafe { IssuancePool::init_in_place(region.base, layout.size, cfg) }.expect("issuance");
    (region, pool)
}

#[test]
fn permit_pool_acquire_release_roundtrip() {
    let (_region, pool) = init_issuance_pool(2);
    let a = pool.try_acquire().expect("acquire a");
    let b = pool.try_acquire().expect("acquire b");
    assert_eq!(pool.snapshot().leased_permits, 2);
    assert!(pool.try_acquire().is_err());
    drop(a);
    assert_eq!(pool.snapshot().leased_permits, 1);
    drop(b);
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.leased_permits, 0);
    assert_eq!(snapshot.release_ok, 2);
}

#[test]
fn issued_received_page_drop_returns_permit() {
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut writer = tx.begin(7, 0).expect("begin");
    writer.payload_mut()[..5].copy_from_slice(b"hello");
    let outbound = writer.finish_with_payload_len(5).expect("finish");
    let frame = encode_issued_frame(outbound.frame()).expect("encode");
    outbound.mark_sent();
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    let mut decoder = IssuedFrameDecoder::new();
    let frame = decoder
        .push(&frame)
        .next()
        .expect("frame")
        .expect("decoded frame");
    let page = match rx.accept(&frame).expect("accept") {
        IssueEvent::Page(page) => page,
        IssueEvent::Closed => panic!("unexpected close"),
    };
    assert_eq!(page.payload(), b"hello");
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);
    drop(page);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn imported_batch_holds_permit_until_last_arrow_owner_drops() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef],
    )
    .expect("record batch");

    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut writer = tx.begin(import::ARROW_LAYOUT_BATCH_KIND, 0).expect("begin");
    let plan = arrow_layout::LayoutPlan::from_arrow_schema(
        schema.as_ref(),
        8,
        u32::try_from(writer.payload_mut().len()).expect("payload len fits u32"),
    )
    .expect("plan");
    let payload_len = {
        let payload = writer.payload_mut();
        init_block(payload, &plan).expect("init block");
        let mut encoder =
            BatchPageEncoder::new(schema.as_ref(), &plan, payload).expect("batch encoder");
        let appended = encoder.append_batch(&batch, 0).expect("append");
        assert_eq!(appended.rows_written, 3);
        assert!(!appended.full);
        encoder.finish().expect("finish").payload_len
    };
    let outbound = writer
        .finish_with_payload_len(payload_len)
        .expect("finish with len");
    let frame = encode_issued_frame(outbound.frame()).expect("encode");
    outbound.mark_sent();
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    let mut decoder = IssuedFrameDecoder::new();
    let frame = decoder
        .push(&frame)
        .next()
        .expect("frame")
        .expect("decoded frame");
    let page = match rx.accept(&frame).expect("accept") {
        IssueEvent::Page(page) => page,
        IssueEvent::Closed => panic!("unexpected close"),
    };

    let imported = ArrowPageDecoder::new(Arc::clone(&schema))
        .expect("decoder")
        .import_owned(page)
        .expect("import");
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    let first_column = imported.column(0).clone();
    drop(imported);
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);
    drop(first_column);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn pool_identity_mismatch_rejects_frame_without_releasing_permit() {
    let (_page_region, page_pool) = init_page_pool(4096, 1);
    let (_sender_issuance_region, sender_pool) = init_issuance_pool(1);
    let (_receiver_issuance_region, receiver_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(page_pool), sender_pool);
    let good_rx = IssuedRx::new(PageRx::new(page_pool), sender_pool);
    let wrong_rx = IssuedRx::new(PageRx::new(page_pool), receiver_pool);

    let mut writer = tx.begin(7, 0).expect("begin");
    writer.payload_mut()[..4].copy_from_slice(b"ping");
    let outbound = writer.finish_with_payload_len(4).expect("finish");
    let frame_bytes = encode_issued_frame(outbound.frame()).expect("encode");
    outbound.mark_sent();

    assert_eq!(sender_pool.snapshot().leased_permits, 1);
    assert_eq!(receiver_pool.snapshot().leased_permits, 0);

    let mut decoder = IssuedFrameDecoder::new();
    let frame = decoder
        .push(&frame_bytes)
        .next()
        .expect("frame")
        .expect("decoded frame");

    assert!(matches!(
        wrong_rx.accept(&frame),
        Err(crate::IssuedRxError::PermitPoolMismatch {
            expected: _,
            actual: _
        })
    ));
    assert_eq!(sender_pool.snapshot().leased_permits, 1);
    assert_eq!(receiver_pool.snapshot().leased_permits, 0);

    let page = match good_rx.accept(&frame).expect("accept on matching pool") {
        IssueEvent::Page(page) => page,
        IssueEvent::Closed => panic!("unexpected close"),
    };
    assert_eq!(page.payload(), b"ping");
    drop(page);
    assert_eq!(sender_pool.snapshot().leased_permits, 0);
}

#[test]
fn access_errors_keep_permit_attached_to_frame() {
    let (_sender_page_region, sender_page_pool) = init_page_pool(4096, 1);
    let (_receiver_page_region, receiver_page_pool) = init_page_pool(4096, 1);
    let (_issuance_region, issuance_pool) = init_issuance_pool(1);
    let tx = IssuedTx::new(PageTx::new(sender_page_pool), issuance_pool);
    let good_rx = IssuedRx::new(PageRx::new(sender_page_pool), issuance_pool);
    let wrong_rx = IssuedRx::new(PageRx::new(receiver_page_pool), issuance_pool);

    let mut writer = tx.begin(7, 0).expect("begin");
    writer.payload_mut()[..4].copy_from_slice(b"pong");
    let outbound = writer.finish_with_payload_len(4).expect("finish");
    let frame_bytes = encode_issued_frame(outbound.frame()).expect("encode");
    outbound.mark_sent();

    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    let mut decoder = IssuedFrameDecoder::new();
    let frame = decoder
        .push(&frame_bytes)
        .next()
        .expect("frame")
        .expect("decoded frame");

    assert!(matches!(
        wrong_rx.accept(&frame),
        Err(crate::IssuedRxError::Rx(transfer::RxError::Access(_)))
    ));
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);

    let page = match good_rx
        .accept(&frame)
        .expect("accept on matching page pool")
    {
        IssueEvent::Page(page) => page,
        IssueEvent::Closed => panic!("unexpected close"),
    };
    assert_eq!(page.payload(), b"pong");
    drop(page);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}

#[test]
fn retryable_accept_errors_keep_permit_attached_to_frame() {
    let (_page_region, page_pool) = init_page_pool(4096, 2);
    let (_issuance_region, issuance_pool) = init_issuance_pool(2);
    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let rx = IssuedRx::new(PageRx::new(page_pool), issuance_pool);

    let mut writer1 = tx.begin(7, 0).expect("begin writer1");
    writer1.payload_mut()[..3].copy_from_slice(b"one");
    let outbound1 = writer1.finish_with_payload_len(3).expect("finish writer1");
    let frame1 = encode_issued_frame(outbound1.frame()).expect("encode frame1");
    outbound1.mark_sent();

    let mut writer2 = tx.begin(7, 0).expect("begin writer2");
    writer2.payload_mut()[..3].copy_from_slice(b"two");
    let outbound2 = writer2.finish_with_payload_len(3).expect("finish writer2");
    let frame2 = encode_issued_frame(outbound2.frame()).expect("encode frame2");
    outbound2.mark_sent();

    assert_eq!(issuance_pool.snapshot().leased_permits, 2);

    let mut decoder = IssuedFrameDecoder::new();
    let decoded1 = decoder
        .push(&frame1)
        .next()
        .expect("decoded frame1")
        .expect("valid frame1");
    let decoded2 = decoder
        .push(&frame2)
        .next()
        .expect("decoded frame2")
        .expect("valid frame2");

    assert!(matches!(
        rx.accept(&decoded2),
        Err(crate::IssuedRxError::Rx(
            transfer::RxError::UnexpectedTransferId {
                expected: 1,
                actual: 2,
            }
        ))
    ));
    assert_eq!(issuance_pool.snapshot().leased_permits, 2);

    let page1 = match rx.accept(&decoded1).expect("accept frame1") {
        IssueEvent::Page(page) => page,
        IssueEvent::Closed => panic!("unexpected close"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 2);

    let page2 = match rx.accept(&decoded2).expect("retry frame2") {
        IssueEvent::Page(page) => page,
        IssueEvent::Closed => panic!("unexpected close"),
    };
    assert_eq!(issuance_pool.snapshot().leased_permits, 2);

    drop(page1);
    assert_eq!(issuance_pool.snapshot().leased_permits, 1);
    drop(page2);
    assert_eq!(issuance_pool.snapshot().leased_permits, 0);
}
