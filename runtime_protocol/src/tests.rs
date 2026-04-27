use super::*;
use crate::envelope::{
    write_runtime_header_to, BACKEND_EXECUTION_START_TAG, BACKEND_SCAN_FAILED_TAG,
    WORKER_SCAN_OPEN_TAG,
};
use crate::msgpack::{
    write_array_len_to, write_str_to, write_u16_to, write_u32_to, write_u64_to, write_u8_to,
};
use crate::scan::{PRODUCER_DESCRIPTOR_LEN, SCAN_CHANNEL_DESCRIPTOR_LEN};
use transfer::MessageKind;

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

fn backend_peer(slot_id: u32, generation: u64, lease_epoch: u64) -> BackendLeaseSlotWire {
    BackendLeaseSlotWire::new(slot_id, generation, lease_epoch)
}

fn scan_channels() -> [ScanChannelDescriptorWire; 2] {
    [
        ScanChannelDescriptorWire {
            scan_id: 7,
            producer_id: 0,
            role: ProducerRole::Leader,
            peer: backend_peer(11, 22, 33),
        },
        ScanChannelDescriptorWire {
            scan_id: 8,
            producer_id: 0,
            role: ProducerRole::Leader,
            peer: backend_peer(12, 22, 34),
        },
    ]
}

fn encode_backend(message: BackendExecutionToWorker<'_>) -> Vec<u8> {
    let mut buf = vec![0u8; encoded_len_backend_execution_to_worker(message)];
    let len = encode_backend_execution_to_worker_into(message, &mut buf).expect("encode");
    assert_eq!(len, buf.len());
    buf
}

fn encode_worker_execution(message: WorkerExecutionToBackend) -> Vec<u8> {
    let mut buf = vec![0u8; encoded_len_worker_execution_to_backend(message)];
    let len = encode_worker_execution_to_backend_into(message, &mut buf).expect("encode");
    assert_eq!(len, buf.len());
    buf
}

fn encode_worker_scan(message: WorkerScanToBackend<'_>) -> Vec<u8> {
    let mut buf = vec![0u8; encoded_len_worker_scan_to_backend(message)];
    let len = encode_worker_scan_to_backend_into(message, &mut buf).expect("encode");
    assert_eq!(len, buf.len());
    buf
}

fn encode_backend_scan(message: BackendScanToWorker<'_>) -> Vec<u8> {
    let mut buf = vec![0u8; encoded_len_backend_scan_to_worker(message)];
    let len = encode_backend_scan_to_worker_into(message, &mut buf).expect("encode");
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
    write_runtime_header_to(
        &mut buf,
        RuntimeMessageFamily::WorkerScanToBackend,
        WORKER_SCAN_OPEN_TAG,
    )
    .expect("runtime header");
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

fn encode_raw_scan_channel_set(entries: &[(u64, u16, u8, BackendLeaseSlotWire)]) -> Vec<u8> {
    let mut buf = Vec::new();
    write_array_len_to(
        &mut buf,
        u32::try_from(entries.len()).expect("scan channel len"),
    )
    .expect("scan channel set len");
    for &(scan_id, producer_id, role, peer) in entries {
        write_array_len_to(&mut buf, SCAN_CHANNEL_DESCRIPTOR_LEN).expect("scan channel len");
        write_u64_to(&mut buf, scan_id).expect("scan id");
        write_u16_to(&mut buf, producer_id).expect("producer id");
        write_u8_to(&mut buf, role).expect("producer role");
        write_u32_to(&mut buf, peer.slot_id()).expect("slot id");
        write_u64_to(&mut buf, peer.generation()).expect("generation");
        write_u64_to(&mut buf, peer.lease_epoch()).expect("lease epoch");
    }
    buf
}

fn encode_raw_backend_start_execution(
    session_epoch: u64,
    plan: PlanFlowDescriptor,
    scan_channel_bytes: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_runtime_header_to(
        &mut buf,
        RuntimeMessageFamily::BackendExecutionToWorker,
        BACKEND_EXECUTION_START_TAG,
    )
    .expect("runtime header");
    write_u64_to(&mut buf, session_epoch).expect("session");
    write_u64_to(&mut buf, plan.plan_id).expect("plan id");
    write_u16_to(&mut buf, plan.page_kind).expect("page kind");
    write_u16_to(&mut buf, plan.page_flags).expect("page flags");
    buf.extend_from_slice(scan_channel_bytes);
    buf
}

fn encode_raw_backend_scan_failed(
    session_epoch: u64,
    scan_id: u64,
    producer_id: u16,
    message: &str,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_runtime_header_to(
        &mut buf,
        RuntimeMessageFamily::BackendScanToWorker,
        BACKEND_SCAN_FAILED_TAG,
    )
    .expect("runtime header");
    write_u64_to(&mut buf, session_epoch).expect("session");
    write_u64_to(&mut buf, scan_id).expect("scan id");
    write_u16_to(&mut buf, producer_id).expect("producer id");
    write_str_to(&mut buf, message).expect("message");
    buf
}

fn decode_open_scan(message: WorkerScanToBackendRef<'_>) -> (u64, u64, ScanFlowDescriptorRef<'_>) {
    match message {
        WorkerScanToBackendRef::OpenScan {
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
fn backend_start_execution_round_trips_with_empty_scan_map() {
    let message = BackendExecutionToWorker::StartExecution {
        session_epoch: 9,
        plan: plan_descriptor(),
        scans: ScanChannelSet::empty(),
    };
    let encoded = encode_backend(message);
    let decoded = decode_backend_execution_to_worker(&encoded).expect("decode");
    assert_eq!(
        decoded,
        BackendExecutionToWorkerRef::StartExecution {
            session_epoch: 9,
            plan: plan_descriptor(),
            scans: ScanChannelSetRef::empty(),
        }
    );
}

#[test]
fn backend_start_execution_round_trips_with_scan_channels() {
    let channels = scan_channels();
    let message = BackendExecutionToWorker::StartExecution {
        session_epoch: 9,
        plan: plan_descriptor(),
        scans: ScanChannelSet::new(&channels).expect("valid scan set"),
    };
    let encoded = encode_backend(message);
    let decoded = decode_backend_execution_to_worker(&encoded).expect("decode");
    let BackendExecutionToWorkerRef::StartExecution {
        session_epoch,
        plan,
        scans,
    } = decoded
    else {
        panic!("expected start execution");
    };
    assert_eq!(session_epoch, 9);
    assert_eq!(plan, plan_descriptor());
    let decoded_channels: Vec<_> = scans.iter().collect();
    assert_eq!(decoded_channels.as_slice(), &channels);
}

#[test]
fn scan_channel_set_rejects_duplicate_scan_id() {
    let channels = [
        ScanChannelDescriptorWire {
            scan_id: 7,
            producer_id: 0,
            role: ProducerRole::Leader,
            peer: backend_peer(1, 2, 3),
        },
        ScanChannelDescriptorWire {
            scan_id: 7,
            producer_id: 0,
            role: ProducerRole::Worker,
            peer: backend_peer(4, 5, 6),
        },
    ];
    let err = ScanChannelSet::new(&channels).expect_err("duplicate scan id");
    assert_eq!(
        err,
        ScanChannelSetError::DuplicateProducer {
            scan_id: 7,
            producer_id: 0
        }
    );
}

#[test]
fn scan_channel_set_rejects_out_of_order_scan_id() {
    let channels = [
        ScanChannelDescriptorWire {
            scan_id: 8,
            producer_id: 0,
            role: ProducerRole::Leader,
            peer: backend_peer(1, 2, 3),
        },
        ScanChannelDescriptorWire {
            scan_id: 7,
            producer_id: 0,
            role: ProducerRole::Leader,
            peer: backend_peer(4, 5, 6),
        },
    ];
    let err = ScanChannelSet::new(&channels).expect_err("out-of-order scan id");
    assert_eq!(
        err,
        ScanChannelSetError::ChannelOutOfOrder {
            previous_scan_id: 8,
            previous_producer_id: 0,
            current_scan_id: 7,
            current_producer_id: 0,
        }
    );
}

#[test]
fn scan_channel_set_rejects_missing_leader() {
    let channels = [
        ScanChannelDescriptorWire {
            scan_id: 7,
            producer_id: 0,
            role: ProducerRole::Worker,
            peer: backend_peer(1, 2, 3),
        },
        ScanChannelDescriptorWire {
            scan_id: 7,
            producer_id: 1,
            role: ProducerRole::Worker,
            peer: backend_peer(4, 5, 6),
        },
    ];
    let err = ScanChannelSet::new(&channels).expect_err("missing scan leader");
    assert_eq!(err, ScanChannelSetError::MissingLeader { scan_id: 7 });
}

#[test]
fn decode_backend_start_execution_rejects_duplicate_scan_id() {
    let encoded = encode_raw_backend_start_execution(
        9,
        plan_descriptor(),
        &encode_raw_scan_channel_set(&[
            (7, 0, ProducerRole::Leader as u8, backend_peer(1, 2, 3)),
            (7, 0, ProducerRole::Worker as u8, backend_peer(4, 5, 6)),
        ]),
    );
    let err = decode_backend_execution_to_worker(&encoded).expect_err("duplicate scan id");
    assert_eq!(
        err,
        DecodeError::DuplicateScanProducer {
            scan_id: 7,
            producer_id: 0
        }
    );
}

#[test]
fn decode_backend_start_execution_rejects_out_of_order_scan_id() {
    let encoded = encode_raw_backend_start_execution(
        9,
        plan_descriptor(),
        &encode_raw_scan_channel_set(&[
            (8, 0, ProducerRole::Leader as u8, backend_peer(1, 2, 3)),
            (7, 0, ProducerRole::Leader as u8, backend_peer(4, 5, 6)),
        ]),
    );
    let err = decode_backend_execution_to_worker(&encoded).expect_err("out-of-order scan id");
    assert_eq!(
        err,
        DecodeError::ScanChannelOutOfOrder {
            previous_scan_id: 8,
            previous_producer_id: 0,
            current_scan_id: 7,
            current_producer_id: 0,
        }
    );
}

#[test]
fn decode_backend_start_execution_rejects_missing_leader() {
    let encoded = encode_raw_backend_start_execution(
        9,
        plan_descriptor(),
        &encode_raw_scan_channel_set(&[
            (7, 0, ProducerRole::Worker as u8, backend_peer(1, 2, 3)),
            (7, 1, ProducerRole::Worker as u8, backend_peer(4, 5, 6)),
        ]),
    );
    let err = decode_backend_execution_to_worker(&encoded).expect_err("missing scan leader");
    assert_eq!(err, DecodeError::MissingScanChannelLeader { scan_id: 7 });
}

#[test]
fn backend_fail_execution_round_trips_with_detail() {
    let message = BackendExecutionToWorker::FailExecution {
        session_epoch: 9,
        code: ExecutionFailureCode::ProtocolViolation,
        detail: Some(123),
    };
    let encoded = encode_backend(message);
    let decoded = decode_backend_execution_to_worker(&encoded).expect("decode");
    assert_eq!(
        decoded,
        BackendExecutionToWorkerRef::FailExecution {
            session_epoch: 9,
            code: ExecutionFailureCode::ProtocolViolation,
            detail: Some(123),
        }
    );
}

#[test]
fn worker_open_scan_round_trips_with_borrowed_producers() {
    let producers = producer_descriptors();
    let message = WorkerScanToBackend::OpenScan {
        session_epoch: 5,
        scan_id: 77,
        scan: ScanFlowDescriptor::new(0x4411, 9, &producers).expect("valid scan descriptor"),
    };
    let encoded = encode_worker_scan(message);
    let decoded = decode_worker_scan_to_backend(&encoded).expect("decode");
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

    let message = WorkerScanToBackend::OpenScan {
        session_epoch: 21,
        scan_id: 301,
        scan: ScanFlowDescriptor::new(0x5001, 2, &producers).expect("valid scan descriptor"),
    };
    let encoded = encode_worker_scan(message);
    let decoded = decode_worker_scan_to_backend(&encoded).expect("decode");
    let (session_epoch, scan_id, scan) = decode_open_scan(decoded);
    assert_eq!(session_epoch, 21);
    assert_eq!(scan_id, 301);
    assert_eq!(scan.producers().len(), 130);
    let decoded_producers: Vec<_> = scan.producers().iter().collect();
    assert_eq!(decoded_producers, producers);
}

#[test]
fn worker_open_scan_with_max_scan_workers_fits_minimum_ring() {
    let mut producers = Vec::with_capacity(33);
    producers.push(ProducerDescriptorWire {
        producer_id: 0,
        role: ProducerRole::Leader,
    });
    for producer_id in 1..33u16 {
        producers.push(ProducerDescriptorWire {
            producer_id,
            role: ProducerRole::Worker,
        });
    }

    let message = WorkerScanToBackend::OpenScan {
        session_epoch: 21,
        scan_id: 301,
        scan: ScanFlowDescriptor::new(0x5001, 2, &producers).expect("valid scan descriptor"),
    };
    let encoded = encode_worker_scan(message);
    assert!(
        encoded.len()
            <= max_message_len_for_ring_capacity(MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY),
        "encoded OpenScan is {} bytes, minimum scan ring payload budget is {} bytes",
        encoded.len(),
        max_message_len_for_ring_capacity(MIN_SCAN_WORKER_TO_BACKEND_RING_CAPACITY)
    );
}

#[test]
fn backend_scan_finished_round_trips() {
    let message = BackendScanToWorker::ScanFinished {
        session_epoch: 5,
        scan_id: 77,
        producer_id: 0,
    };
    let encoded = encode_backend_scan(message);
    let decoded = decode_backend_scan_to_worker(&encoded).expect("decode");
    assert_eq!(
        decoded,
        BackendScanToWorkerRef::ScanFinished {
            session_epoch: 5,
            scan_id: 77,
            producer_id: 0,
        }
    );
}

#[test]
fn backend_scan_failed_round_trips_with_borrowed_message() {
    let message = BackendScanToWorker::ScanFailed {
        session_epoch: 5,
        scan_id: 77,
        producer_id: 0,
        message: "boom",
    };
    let encoded = encode_backend_scan(message);
    let decoded = decode_backend_scan_to_worker(&encoded).expect("decode");
    assert_eq!(
        decoded,
        BackendScanToWorkerRef::ScanFailed {
            session_epoch: 5,
            scan_id: 77,
            producer_id: 0,
            message: "boom",
        }
    );
}

#[test]
fn backend_scan_failed_accepts_max_bounded_message_len() {
    let message_text = "x".repeat(MAX_SCAN_FAILURE_MESSAGE_LEN);
    let message = BackendScanToWorker::ScanFailed {
        session_epoch: u64::MAX,
        scan_id: u64::MAX,
        producer_id: u16::MAX,
        message: &message_text,
    };
    let encoded = encode_backend_scan(message);
    let decoded = decode_backend_scan_to_worker(&encoded).expect("decode");
    assert_eq!(
        decoded,
        BackendScanToWorkerRef::ScanFailed {
            session_epoch: u64::MAX,
            scan_id: u64::MAX,
            producer_id: u16::MAX,
            message: &message_text,
        }
    );
}

#[test]
fn backend_scan_failed_rejects_message_over_bounded_len() {
    let message_text = "x".repeat(MAX_SCAN_FAILURE_MESSAGE_LEN + 1);
    let message = BackendScanToWorker::ScanFailed {
        session_epoch: 1,
        scan_id: 2,
        producer_id: 3,
        message: &message_text,
    };
    let mut buf = vec![0_u8; 512];
    let err = encode_backend_scan_to_worker_into(message, &mut buf)
        .expect_err("too long scan failure text");
    assert_eq!(
        err,
        EncodeError::ScanFailureMessageTooLong {
            actual: MAX_SCAN_FAILURE_MESSAGE_LEN + 1,
            maximum: MAX_SCAN_FAILURE_MESSAGE_LEN,
        }
    );
}

#[test]
fn worker_fail_execution_round_trips_without_detail() {
    let message = WorkerExecutionToBackend::FailExecution {
        session_epoch: 12,
        code: ExecutionFailureCode::TransportRestarted,
        detail: None,
    };
    let encoded = encode_worker_execution(message);
    let decoded = decode_worker_execution_to_backend(&encoded).expect("decode");
    assert_eq!(
        decoded,
        WorkerExecutionToBackend::FailExecution {
            session_epoch: 12,
            code: ExecutionFailureCode::TransportRestarted,
            detail: None,
        }
    );
}

#[test]
fn encoded_len_matches_written_backend_message() {
    let message = BackendExecutionToWorker::CancelExecution { session_epoch: 3 };
    let expected = encoded_len_backend_execution_to_worker(message);
    let mut buf = vec![0u8; expected];
    let actual = encode_backend_execution_to_worker_into(message, &mut buf).expect("encode");
    assert_eq!(actual, expected);
}

#[test]
fn encoded_len_matches_written_worker_execution_message() {
    let message = WorkerExecutionToBackend::CompleteExecution { session_epoch: 5 };
    let expected = encoded_len_worker_execution_to_backend(message);
    let mut buf = vec![0u8; expected];
    let actual = encode_worker_execution_to_backend_into(message, &mut buf).expect("encode");
    assert_eq!(actual, expected);
}

#[test]
fn encoded_len_matches_written_worker_scan_message() {
    let producers = producer_descriptors();
    let message = WorkerScanToBackend::OpenScan {
        session_epoch: 5,
        scan_id: 17,
        scan: ScanFlowDescriptor::new(0x2001, 1, &producers).expect("valid scan descriptor"),
    };
    let expected = encoded_len_worker_scan_to_backend(message);
    let mut buf = vec![0u8; expected];
    let actual = encode_worker_scan_to_backend_into(message, &mut buf).expect("encode");
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
    let encoded = encode_worker_scan(WorkerScanToBackend::OpenScan {
        session_epoch: 8,
        scan_id: 99,
        scan: ScanFlowDescriptor::new(0x0202, 7, &producers).expect("valid scan descriptor"),
    });
    let decoded = decode_worker_scan_to_backend(&encoded).expect("decode");
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
    let mut encoded =
        encode_backend(BackendExecutionToWorker::CancelExecution { session_epoch: 1 });
    encoded[0] ^= 0x01;
    let err = decode_backend_execution_to_worker(&encoded).expect_err("bad magic");
    assert!(matches!(err, DecodeError::InvalidMagic { .. }));
}

#[test]
fn decode_rejects_bad_version() {
    let mut encoded =
        encode_backend(BackendExecutionToWorker::CancelExecution { session_epoch: 1 });
    encoded[4] ^= 0x01;
    let err = decode_backend_execution_to_worker(&encoded).expect_err("bad version");
    assert!(matches!(err, DecodeError::UnsupportedVersion { .. }));
}

#[test]
fn decode_rejects_trailing_bytes() {
    let mut encoded =
        encode_backend(BackendExecutionToWorker::CancelExecution { session_epoch: 1 });
    encoded.push(0);
    let err = decode_backend_execution_to_worker(&encoded).expect_err("trailing");
    assert!(matches!(err, DecodeError::TrailingBytes { remaining: 1 }));
}

#[test]
fn decode_rejects_wrong_message_family() {
    let encoded = encode_worker_scan(WorkerScanToBackend::CancelScan {
        session_epoch: 2,
        scan_id: 3,
    });
    let err = decode_backend_execution_to_worker(&encoded).expect_err("wrong family");
    assert!(matches!(err, DecodeError::UnexpectedMessageFamily { .. }));
}

#[test]
fn decode_backend_scan_rejects_wrong_message_family() {
    let encoded = encode_worker_scan(WorkerScanToBackend::CancelScan {
        session_epoch: 2,
        scan_id: 3,
    });
    let err = decode_backend_scan_to_worker(&encoded).expect_err("wrong family");
    assert!(matches!(err, DecodeError::UnexpectedMessageFamily { .. }));
}

#[test]
fn decode_backend_scan_failed_rejects_trailing_bytes() {
    let mut encoded = encode_raw_backend_scan_failed(2, 3, 0, "boom");
    encoded.push(0);
    let err = decode_backend_scan_to_worker(&encoded).expect_err("trailing");
    assert!(matches!(err, DecodeError::TrailingBytes { remaining: 1 }));
}

#[test]
fn decode_rejects_empty_producer_set() {
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &encode_raw_producer_set(&[]));
    let err = decode_worker_scan_to_backend(&encoded).expect_err("empty producers");
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
    let err = decode_worker_scan_to_backend(&encoded).expect_err("duplicate producer");
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
    let err = decode_worker_scan_to_backend(&encoded).expect_err("multiple leaders");
    assert_eq!(err, DecodeError::MultipleLeaders);
}

#[test]
fn decode_rejects_invalid_producer_role() {
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &encode_raw_producer_set(&[(1, 9)]));
    let err = decode_worker_scan_to_backend(&encoded).expect_err("invalid role");
    assert_eq!(err, DecodeError::InvalidProducerRole { actual: 9 });
}

#[test]
fn decode_preserves_nonminimal_array16_producer_header() {
    let producer_bytes = encode_raw_producer_set_array16(&[
        (11, ProducerRole::Leader as u8),
        (12, ProducerRole::Worker as u8),
    ]);
    let encoded = encode_raw_open_scan(2, 3, 0x0101, 0, &producer_bytes);
    let decoded = decode_worker_scan_to_backend(&encoded).expect("decode");
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
    let decoded = decode_worker_scan_to_backend(&encoded).expect("decode");
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
