use super::*;

#[test]
fn transport_frame_roundtrip_and_incremental_decode() {
    let frame = OwnedFrame::Page(PageFrame {
        transfer_id: 17,
        descriptor: page_pool::PageDescriptor {
            pool_id: 9,
            page_id: 3,
            generation: 44,
        },
    });
    let encoded = encode_frame(frame).expect("encode");
    assert_eq!(encoded.len(), TRANSPORT_HEADER_LEN);
    let decoded = decode_owned_frame(&encoded).expect("decode");
    assert_eq!(decoded, frame);

    let mut decoder = FrameDecoder::new();
    for (index, byte) in encoded.iter().copied().enumerate() {
        let chunk = [byte];
        let item = {
            let mut frames = decoder.push(&chunk);
            let item = frames.next();
            assert!(frames.next().is_none());
            item
        };

        if index + 1 < TRANSPORT_HEADER_LEN {
            assert!(item.is_none());
            assert_eq!(decoder.buffered_len(), index + 1);
        } else {
            assert_eq!(item, Some(Ok(frame)));
            assert_eq!(decoder.buffered_len(), 0);
        }
    }
}

#[test]
fn close_frame_roundtrip() {
    let frame = OwnedFrame::Close(crate::wire::CloseFrame { transfer_id: 5 });
    let encoded = encode_frame(frame).expect("encode");
    let decoded = decode_owned_frame(&encoded).expect("decode");
    assert_eq!(decoded, frame);
}

#[test]
fn decode_rejects_bad_magic_and_nonzero_flags() {
    let frame = OwnedFrame::Close(crate::wire::CloseFrame { transfer_id: 1 });
    let mut encoded = encode_frame(frame).expect("encode");
    encoded[1] ^= 0x01;
    assert!(matches!(
        decode_owned_frame(&encoded),
        Err(crate::DecodeError::InvalidMagic { .. })
    ));

    let mut encoded = encode_frame(frame).expect("encode");
    encoded[12] = 1;
    assert!(matches!(
        decode_owned_frame(&encoded),
        Err(crate::DecodeError::NonZeroFlags { actual: 1 })
    ));
}

#[test]
fn page_header_roundtrip() {
    let header = PageHeader {
        kind: TEST_KIND,
        flags: TEST_FLAGS,
        payload_len: 123,
    };
    let mut bytes = [0u8; PAGE_HEADER_LEN];
    encode_page_header(header, &mut bytes).expect("encode page header");
    assert_eq!(bytes.len(), HEADER_LEN);
    let decoded = decode_page_header(&bytes).expect("decode page header");
    assert_eq!(decoded, header);
}

#[test]
fn push_decodes_multiple_concatenated_frames() {
    let frame1 = OwnedFrame::Page(PageFrame {
        transfer_id: 1,
        descriptor: page_pool::PageDescriptor {
            pool_id: 10,
            page_id: 2,
            generation: 3,
        },
    });
    let frame2 = OwnedFrame::Close(crate::wire::CloseFrame { transfer_id: 2 });

    let encoded1 = encode_frame(frame1).expect("encode frame1");
    let encoded2 = encode_frame(frame2).expect("encode frame2");
    let bytes = [encoded1.as_slice(), encoded2.as_slice()].concat();

    let mut decoder = FrameDecoder::new();
    {
        let mut frames = decoder.push(&bytes);
        assert_eq!(frames.next(), Some(Ok(frame1)));
        assert_eq!(frames.next(), Some(Ok(frame2)));
        assert!(frames.next().is_none());
    }
    assert_eq!(decoder.buffered_len(), 0);
}

#[test]
fn push_buffers_partial_tail_between_calls() {
    let frame1 = OwnedFrame::Page(PageFrame {
        transfer_id: 11,
        descriptor: page_pool::PageDescriptor {
            pool_id: 5,
            page_id: 1,
            generation: 7,
        },
    });
    let frame2 = OwnedFrame::Page(PageFrame {
        transfer_id: 12,
        descriptor: page_pool::PageDescriptor {
            pool_id: 5,
            page_id: 2,
            generation: 8,
        },
    });
    let encoded1 = encode_frame(frame1).expect("encode frame1");
    let encoded2 = encode_frame(frame2).expect("encode frame2");
    let split = 13;
    let bytes = [encoded1.as_slice(), &encoded2[..split]].concat();

    let mut decoder = FrameDecoder::new();
    {
        let mut frames = decoder.push(&bytes);
        assert_eq!(frames.next(), Some(Ok(frame1)));
        assert!(frames.next().is_none());
    }
    assert_eq!(decoder.buffered_len(), split);

    {
        let mut frames = decoder.push(&encoded2[split..]);
        assert_eq!(frames.next(), Some(Ok(frame2)));
        assert!(frames.next().is_none());
    }
    assert_eq!(decoder.buffered_len(), 0);
}

#[test]
fn push_returns_error_after_valid_frame_in_same_chunk() {
    let frame1 = OwnedFrame::Page(PageFrame {
        transfer_id: 21,
        descriptor: page_pool::PageDescriptor {
            pool_id: 6,
            page_id: 4,
            generation: 9,
        },
    });
    let frame2 = OwnedFrame::Close(crate::wire::CloseFrame { transfer_id: 22 });

    let encoded1 = encode_frame(frame1).expect("encode frame1");
    let mut encoded2 = encode_frame(frame2).expect("encode frame2");
    encoded2[12] = 1;
    let bytes = [encoded1.as_slice(), encoded2.as_slice()].concat();

    let mut decoder = FrameDecoder::new();
    {
        let mut frames = decoder.push(&bytes);
        assert_eq!(frames.next(), Some(Ok(frame1)));
        assert!(matches!(
            frames.next(),
            Some(Err(crate::DecodeError::NonZeroFlags { actual: 1 }))
        ));
        assert!(frames.next().is_none());
    }
    assert_eq!(decoder.buffered_len(), 0);
}
