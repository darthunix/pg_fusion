use super::*;

fn plan_descriptor() -> PlanFlowDescriptor {
    PlanFlowDescriptor {
        plan_id: 42,
        page_kind: 0x4152,
        page_flags: 3,
    }
}

fn producer_descriptors() -> [ProducerDescriptorWire; 2] {
    [
        ProducerDescriptorWire {
            producer_id: 11,
            role: ProducerRole::Leader,
        },
        ProducerDescriptorWire {
            producer_id: 12,
            role: ProducerRole::Worker,
        },
    ]
}

fn encode_backend(message: BackendToWorker) -> Vec<u8> {
    let mut buf = vec![0u8; encoded_len_backend_to_worker(message)];
    let len = encode_backend_to_worker_into(message, &mut buf).expect("encode");
    assert_eq!(len, buf.len());
    buf
}

fn encode_worker(message: WorkerToBackend<'_>) -> Vec<u8> {
    let mut buf = vec![0u8; encoded_len_worker_to_backend(message)];
    let len = encode_worker_to_backend_into(message, &mut buf).expect("encode");
    assert_eq!(len, buf.len());
    buf
}

fn encode_raw_open_scan(
    session_epoch: u64,
    scan_id: u64,
    page_kind: MessageKind,
    page_flags: u16,
    producer_bytes: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_array_len_to(&mut buf, WORKER_TO_BACKEND_OPEN_SCAN_LEN).expect("array len");
    write_magic_and_version_to(&mut buf).expect("magic+version");
    write_u8_to(&mut buf, WORKER_TO_BACKEND_OPEN_SCAN_TAG).expect("tag");
    write_u64_to(&mut buf, session_epoch).expect("session");
    write_u64_to(&mut buf, scan_id).expect("scan id");
    write_u16_to(&mut buf, page_kind).expect("page kind");
    write_u16_to(&mut buf, page_flags).expect("page flags");
    buf.extend_from_slice(producer_bytes);
    buf
}

fn encode_raw_producer_set(entries: &[(u16, u8)]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_array_len_to(
        &mut buf,
        u32::try_from(entries.len()).expect("producer len"),
    )
    .expect("producer set len");
    for &(producer_id, role) in entries {
        write_array_len_to(&mut buf, PRODUCER_DESCRIPTOR_LEN).expect("producer len");
        write_u16_to(&mut buf, producer_id).expect("producer id");
        write_u8_to(&mut buf, role).expect("producer role");
    }
    buf
}

fn encode_raw_producer_set_array16(entries: &[(u16, u8)]) -> Vec<u8> {
    let mut buf = Vec::new();
    let len = u16::try_from(entries.len()).expect("producer len");
    buf.push(0xdc);
    buf.extend_from_slice(&len.to_be_bytes());
    for &(producer_id, role) in entries {
        write_array_len_to(&mut buf, PRODUCER_DESCRIPTOR_LEN).expect("producer len");
        write_u16_to(&mut buf, producer_id).expect("producer id");
        write_u8_to(&mut buf, role).expect("producer role");
    }
    buf
}

fn encode_raw_producer_set_array32(entries: &[(u16, u8)]) -> Vec<u8> {
    let mut buf = Vec::new();
    let len = u32::try_from(entries.len()).expect("producer len");
    buf.push(0xdd);
    buf.extend_from_slice(&len.to_be_bytes());
    for &(producer_id, role) in entries {
        write_array_len_to(&mut buf, PRODUCER_DESCRIPTOR_LEN).expect("producer len");
        write_u16_to(&mut buf, producer_id).expect("producer id");
        write_u8_to(&mut buf, role).expect("producer role");
    }
    buf
}

fn decode_open_scan(message: WorkerToBackendRef<'_>) -> (u64, u64, ScanFlowDescriptorRef<'_>) {
    match message {
        WorkerToBackendRef::OpenScan {
            session_epoch,
            scan_id,
            scan,
        } => (session_epoch, scan_id, scan),
        other => panic!("expected OpenScan, got {other:?}"),
    }
}

fn to_plan_open(session_epoch: u64, descriptor: PlanFlowDescriptor) -> plan_flow::PlanOpen {
    plan_flow::PlanOpen::new(
        plan_flow::FlowId {
            session_epoch,
            plan_id: descriptor.plan_id,
        },
        descriptor.page_kind,
        descriptor.page_flags,
    )
}

fn to_scan_open(
    session_epoch: u64,
    scan_id: u64,
    descriptor: ScanFlowDescriptorRef<'_>,
) -> scan_flow::ScanOpen {
    let producers = descriptor
        .producers()
        .iter()
        .map(|producer| scan_flow::ProducerDescriptor {
            producer_id: producer.producer_id,
            role: match producer.role {
                ProducerRole::Leader => scan_flow::ProducerRoleKind::Leader,
                ProducerRole::Worker => scan_flow::ProducerRoleKind::Worker,
            },
        })
        .collect();

    scan_flow::ScanOpen::new(
        scan_flow::FlowId {
            session_epoch,
            scan_id,
        },
        descriptor.page_kind,
        descriptor.page_flags,
        producers,
    )
    .expect("valid scan open")
}

#[test]
fn classify_session_orders_epochs() {
    assert_eq!(classify_session(7, 7), SessionDisposition::Current);
    assert_eq!(classify_session(7, 6), SessionDisposition::Stale);
    assert_eq!(classify_session(7, 8), SessionDisposition::Future);
}

#[test]
fn backend_start_execution_round_trips() {
    let message = BackendToWorker::StartExecution {
        session_epoch: 9,
        plan: plan_descriptor(),
    };
    let encoded = encode_backend(message);
    let decoded = decode_backend_to_worker(&encoded).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn backend_fail_execution_round_trips_with_detail() {
    let message = BackendToWorker::FailExecution {
        session_epoch: 9,
        code: ExecutionFailureCode::ProtocolViolation,
        detail: Some(123),
    };
    let encoded = encode_backend(message);
    let decoded = decode_backend_to_worker(&encoded).expect("decode");
    assert_eq!(decoded, message);
}

#[test]
fn backend_to_worker_max_encoded_len_covers_all_current_messages() {
    let max_encoded = [
        encoded_len_backend_to_worker(BackendToWorker::StartExecution {
            session_epoch: u64::MAX,
            plan: PlanFlowDescriptor {
                plan_id: u64::MAX,
                page_kind: u16::MAX,
                page_flags: u16::MAX,
            },
        }),
        encoded_len_backend_to_worker(BackendToWorker::CancelExecution {
            session_epoch: u64::MAX,
        }),
        encoded_len_backend_to_worker(BackendToWorker::FailExecution {
            session_epoch: u64::MAX,
            code: ExecutionFailureCode::Internal,
            detail: Some(u64::MAX),
        }),
    ]
    .into_iter()
    .max()
    .expect("backend message lengths");

    assert_eq!(max_encoded, MAX_BACKEND_TO_WORKER_ENCODED_LEN);
}

#[test]
fn worker_open_scan_round_trips_with_borrowed_producers() {
    let producers = producer_descriptors();
    let message = WorkerToBackend::OpenScan {
        session_epoch: 5,
        scan_id: 77,
        scan: ScanFlowDescriptor::new(0x4411, 9, &producers).expect("valid scan descriptor"),
    };
    let encoded = encode_worker(message);
    let decoded = decode_worker_to_backend(&encoded).expect("decode");
    let (session_epoch, scan_id, scan) = decode_open_scan(decoded);
    assert_eq!(session_epoch, 5);
    assert_eq!(scan_id, 77);
    assert_eq!(scan.page_kind, 0x4411);
    assert_eq!(scan.page_flags, 9);
    let decoded_producers: Vec<_> = scan.producers().iter().collect();
    assert_eq!(decoded_producers.as_slice(), &producers);
}

#[test]
fn worker_open_scan_round_trips_many_producers() {
    let mut producers = Vec::with_capacity(130);
    producers.push(ProducerDescriptorWire {
        producer_id: 0,
        role: ProducerRole::Leader,
    });
    for producer_id in 1..130u16 {
        producers.push(ProducerDescriptorWire {
            producer_id,
            role: ProducerRole::Worker,
        });
    }

    let message = WorkerToBackend::OpenScan {
        session_epoch: 21,
        scan_id: 301,
        scan: ScanFlowDescriptor::new(0x5001, 2, &producers).expect("valid scan descriptor"),
    };
    let encoded = encode_worker(message);
    let decoded = decode_worker_to_backend(&encoded).expect("decode");
    let (session_epoch, scan_id, scan) = decode_open_scan(decoded);
    assert_eq!(session_epoch, 21);
    assert_eq!(scan_id, 301);
    assert_eq!(scan.producers().len(), 130);
    let decoded_producers: Vec<_> = scan.producers().iter().collect();
    assert_eq!(decoded_producers, producers);
}

#[test]
fn worker_fail_execution_round_trips_without_detail() {
    let message = WorkerToBackend::FailExecution {
        session_epoch: 12,
        code: ExecutionFailureCode::TransportRestarted,
        detail: None,
    };
    let encoded = encode_worker(message);
    let decoded = decode_worker_to_backend(&encoded).expect("decode");
    assert_eq!(
        decoded,
        WorkerToBackendRef::FailExecution {
            session_epoch: 12,
            code: ExecutionFailureCode::TransportRestarted,
            detail: None,
        }
    );
}

#[test]
fn encoded_len_matches_written_backend_message() {
    let message = BackendToWorker::CancelExecution { session_epoch: 3 };
    let expected = encoded_len_backend_to_worker(message);
    let mut buf = vec![0u8; expected];
    let actual = encode_backend_to_worker_into(message, &mut buf).expect("encode");
    assert_eq!(actual, expected);
}

#[test]
fn encoded_len_matches_written_worker_message() {
    let producers = producer_descriptors();
    let message = WorkerToBackend::OpenScan {
        session_epoch: 5,
        scan_id: 17,
        scan: ScanFlowDescriptor::new(0x2001, 1, &producers).expect("valid scan descriptor"),
    };
    let expected = encoded_len_worker_to_backend(message);
    let mut buf = vec![0u8; expected];
    let actual = encode_worker_to_backend_into(message, &mut buf).expect("encode");
    assert_eq!(actual, expected);
}

#[test]
fn plan_descriptor_reconstructs_plan_open() {
    let session_epoch = 14;
    let descriptor = plan_descriptor();
    let open = to_plan_open(session_epoch, descriptor);
    assert_eq!(open.flow.session_epoch, session_epoch);
    assert_eq!(open.flow.plan_id, descriptor.plan_id);
    assert_eq!(open.page_kind, descriptor.page_kind);
    assert_eq!(open.page_flags, descriptor.page_flags);
}

#[test]
fn scan_descriptor_reconstructs_scan_open() {
    let producers = producer_descriptors();
    let encoded = encode_worker(WorkerToBackend::OpenScan {
        session_epoch: 8,
        scan_id: 99,
        scan: ScanFlowDescriptor::new(0x0202, 7, &producers).expect("valid scan descriptor"),
    });
    let decoded = decode_worker_to_backend(&encoded).expect("decode");
    let (session_epoch, scan_id, scan) = decode_open_scan(decoded);
    let open = to_scan_open(session_epoch, scan_id, scan);
    assert_eq!(open.flow.session_epoch, session_epoch);
    assert_eq!(open.flow.scan_id, scan_id);
    assert_eq!(open.page_kind, 0x0202);
    assert_eq!(open.page_flags, 7);
    assert_eq!(open.producers.len(), 2);
    assert_eq!(open.producers[0].producer_id, 11);
    assert_eq!(open.producers[1].producer_id, 12);
}

#[test]
fn decode_rejects_bad_magic() {
    let mut encoded = encode_backend(BackendToWorker::CancelExecution { session_epoch: 1 });
    let magic_index = encoded
        .iter()
        .position(|&byte| byte == b'P')
        .expect("magic byte");
    encoded[magic_index] = b'X';
    let err = decode_backend_to_worker(&encoded).expect_err("bad magic");
    assert!(matches!(err, DecodeError::InvalidMagic { .. }));
}

#[test]
fn decode_rejects_bad_version() {
    let mut encoded = encode_backend(BackendToWorker::CancelExecution { session_epoch: 1 });
    let magic_index = encoded
        .iter()
        .position(|&byte| byte == b'P')
        .expect("magic byte");
    let version_index = magic_index + RUNTIME_PROTOCOL_MAGIC.len() + 1;
    encoded[version_index] = 2;
    let err = decode_backend_to_worker(&encoded).expect_err("bad version");
    assert!(matches!(err, DecodeError::UnsupportedVersion { .. }));
}

#[test]
fn decode_rejects_trailing_bytes() {
    let mut encoded = encode_backend(BackendToWorker::CancelExecution { session_epoch: 1 });
    encoded.push(0);
    let err = decode_backend_to_worker(&encoded).expect_err("trailing");
    assert!(matches!(err, DecodeError::TrailingBytes { remaining: 1 }));
}

#[test]
fn decode_rejects_empty_producer_set() {
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &encode_raw_producer_set(&[]));
    let err = decode_worker_to_backend(&encoded).expect_err("empty producers");
    assert_eq!(err, DecodeError::EmptyProducerSet);
}

#[test]
fn decode_rejects_duplicate_producer_id() {
    let encoded = encode_raw_open_scan(
        2,
        3,
        0x0101,
        0,
        &encode_raw_producer_set(&[
            (7, ProducerRole::Leader as u8),
            (7, ProducerRole::Worker as u8),
        ]),
    );
    let err = decode_worker_to_backend(&encoded).expect_err("duplicate producer");
    assert_eq!(err, DecodeError::DuplicateProducerId { producer_id: 7 });
}

#[test]
fn decode_rejects_multiple_leaders() {
    let encoded = encode_raw_open_scan(
        2,
        3,
        0x0101,
        0,
        &encode_raw_producer_set(&[
            (1, ProducerRole::Leader as u8),
            (2, ProducerRole::Leader as u8),
        ]),
    );
    let err = decode_worker_to_backend(&encoded).expect_err("multiple leaders");
    assert_eq!(err, DecodeError::MultipleLeaders);
}

#[test]
fn decode_rejects_invalid_producer_role() {
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &encode_raw_producer_set(&[(1, 9)]));
    let err = decode_worker_to_backend(&encoded).expect_err("invalid role");
    assert_eq!(err, DecodeError::InvalidProducerRole { actual: 9 });
}

#[test]
fn decode_preserves_nonminimal_array16_producer_header() {
    let producer_bytes = encode_raw_producer_set_array16(&[
        (11, ProducerRole::Leader as u8),
        (12, ProducerRole::Worker as u8),
    ]);
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &producer_bytes);
    let decoded = decode_worker_to_backend(&encoded).expect("decode");
    let (_, _, scan) = decode_open_scan(decoded);
    let decoded_producers: Vec<_> = scan.producers().iter().collect();
    assert_eq!(decoded_producers.as_slice(), &producer_descriptors());
    assert_eq!(scan.producers().as_encoded(), producer_bytes.as_slice());
}

#[test]
fn decode_preserves_nonminimal_array32_producer_header() {
    let producer_bytes = encode_raw_producer_set_array32(&[
        (11, ProducerRole::Leader as u8),
        (12, ProducerRole::Worker as u8),
    ]);
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &producer_bytes);
    let decoded = decode_worker_to_backend(&encoded).expect("decode");
    let (_, _, scan) = decode_open_scan(decoded);
    let decoded_producers: Vec<_> = scan.producers().iter().collect();
    assert_eq!(decoded_producers.as_slice(), &producer_descriptors());
    assert_eq!(scan.producers().as_encoded(), producer_bytes.as_slice());
}

#[test]
fn scan_descriptor_constructor_rejects_empty_producer_set() {
    let err = ScanFlowDescriptor::new(0x0101, 0, &[]).expect_err("empty producer set");
    assert_eq!(err, ProducerSetError::EmptyProducerSet);
}

#[test]
fn scan_descriptor_constructor_rejects_duplicate_producer_id() {
    let producers = [
        ProducerDescriptorWire {
            producer_id: 5,
            role: ProducerRole::Leader,
        },
        ProducerDescriptorWire {
            producer_id: 5,
            role: ProducerRole::Worker,
        },
    ];
    let err = ScanFlowDescriptor::new(0x0101, 0, &producers).expect_err("duplicate producer id");
    assert_eq!(
        err,
        ProducerSetError::DuplicateProducerId { producer_id: 5 }
    );
}

#[test]
fn scan_descriptor_constructor_rejects_multiple_leaders() {
    let producers = [
        ProducerDescriptorWire {
            producer_id: 1,
            role: ProducerRole::Leader,
        },
        ProducerDescriptorWire {
            producer_id: 2,
            role: ProducerRole::Leader,
        },
    ];
    let err = ScanFlowDescriptor::new(0x0101, 0, &producers).expect_err("multiple leaders");
    assert_eq!(err, ProducerSetError::MultipleLeaders);
}

#[test]
fn scan_descriptor_constructor_accepts_valid_producer_set() {
    let producers = producer_descriptors();
    let descriptor = ScanFlowDescriptor::new(0x0101, 0, &producers).expect("valid producer set");
    assert_eq!(descriptor.page_kind, 0x0101);
    assert_eq!(descriptor.page_flags, 0);
    assert_eq!(descriptor.producers(), &producers);
}

#[test]
fn max_message_len_matches_control_transport_rule() {
    assert_eq!(CONTROL_TRANSPORT_PAYLOAD_OVERHEAD, 5);
    assert_eq!(max_message_len_for_ring_capacity(0), 0);
    assert_eq!(max_message_len_for_ring_capacity(5), 0);
    assert_eq!(max_message_len_for_ring_capacity(64), 59);
}
