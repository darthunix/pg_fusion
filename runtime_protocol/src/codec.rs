//! Encode and decode entrypoints for runtime-protocol messages.
//!
//! These functions are the main API surface used by higher layers. They keep
//! the public wire contract flat at the crate root via re-exports, while this
//! module groups the implementation by operation rather than by type
//! definition.

use crate::envelope::{
    decode_runtime_header, expect_runtime_family, write_runtime_header_to,
    BACKEND_EXECUTION_CANCEL_TAG, BACKEND_EXECUTION_FAIL_TAG, BACKEND_EXECUTION_START_TAG,
    BACKEND_SCAN_FAILED_TAG, BACKEND_SCAN_FINISHED_TAG, WORKER_EXECUTION_COMPLETE_TAG,
    WORKER_EXECUTION_FAIL_TAG, WORKER_SCAN_CANCEL_TAG, WORKER_SCAN_OPEN_TAG,
};
use crate::error::{DecodeError, EncodeError};
use crate::message::{
    BackendExecutionToWorker, BackendExecutionToWorkerRef, BackendScanToWorker,
    BackendScanToWorkerRef, ExecutionFailureCode, RuntimeMessageFamily, WorkerExecutionToBackend,
    WorkerScanToBackend, WorkerScanToBackendRef,
};
use crate::msgpack::{
    encode_into_with_len, encoded_len_with, read_optional_u64_from, read_str_from, read_u16_from,
    read_u64_from, read_u8_from, write_optional_u64_to, write_str_to, write_u16_to, write_u64_to,
    write_u8_to,
};
use crate::scan::{
    write_producer_slice_to, write_scan_channel_slice_to, PlanFlowDescriptor, ScanFlowDescriptorRef,
};
use crate::validation::{decode_producer_set_ref, decode_scan_channel_set_ref};

/// Return the exact encoded length of one backend-execution control message.
pub fn encoded_len_backend_execution_to_worker(message: BackendExecutionToWorker<'_>) -> usize {
    try_encoded_len_backend_execution_to_worker(message)
        .expect("runtime_protocol backend execution length must fit into usize")
}

/// Return the exact encoded length of one worker-execution control message.
pub fn encoded_len_worker_execution_to_backend(message: WorkerExecutionToBackend) -> usize {
    try_encoded_len_worker_execution_to_backend(message)
        .expect("runtime_protocol worker execution length must fit into usize")
}

/// Return the exact encoded length of one worker-scan control message.
pub fn encoded_len_worker_scan_to_backend(message: WorkerScanToBackend<'_>) -> usize {
    try_encoded_len_worker_scan_to_backend(message)
        .expect("runtime_protocol worker scan length must fit into usize")
}

/// Return the exact encoded length of one backend-scan terminal message.
pub fn encoded_len_backend_scan_to_worker(message: BackendScanToWorker<'_>) -> usize {
    try_encoded_len_backend_scan_to_worker(message)
        .expect("runtime_protocol backend scan length must fit into usize")
}

/// Encode one backend-execution control message into `out`.
pub fn encode_backend_execution_to_worker_into(
    message: BackendExecutionToWorker<'_>,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_backend_execution_to_worker(message)?,
        out,
        |mut writer| encode_backend_execution_to_worker_to(message, &mut writer),
    )
}

/// Encode one worker-execution control message into `out`.
pub fn encode_worker_execution_to_backend_into(
    message: WorkerExecutionToBackend,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_worker_execution_to_backend(message)?,
        out,
        |mut writer| encode_worker_execution_to_backend_to(message, &mut writer),
    )
}

/// Encode one worker-scan control message into `out`.
pub fn encode_worker_scan_to_backend_into(
    message: WorkerScanToBackend<'_>,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_worker_scan_to_backend(message)?,
        out,
        |mut writer| encode_worker_scan_to_backend_to(message, &mut writer),
    )
}

/// Encode one backend-scan terminal message into `out`.
pub fn encode_backend_scan_to_worker_into(
    message: BackendScanToWorker<'_>,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    encode_into_with_len(
        try_encoded_len_backend_scan_to_worker(message)?,
        out,
        |mut writer| encode_backend_scan_to_worker_to(message, &mut writer),
    )
}

/// Decode only the runtime envelope header and return its message family.
pub fn decode_runtime_message_family(bytes: &[u8]) -> Result<RuntimeMessageFamily, DecodeError> {
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    Ok(header.family)
}

/// Decode one borrowed backend-execution control message.
pub fn decode_backend_execution_to_worker(
    bytes: &[u8],
) -> Result<BackendExecutionToWorkerRef<'_>, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(
        header.family,
        RuntimeMessageFamily::BackendExecutionToWorker,
    )?;

    let session_epoch = read_u64_from(&mut source)?;
    let message = match header.tag {
        BACKEND_EXECUTION_START_TAG => {
            let plan = PlanFlowDescriptor {
                plan_id: read_u64_from(&mut source)?,
                page_kind: read_u16_from(&mut source)?,
                page_flags: read_u16_from(&mut source)?,
            };
            let scans = decode_scan_channel_set_ref(original, &mut source)?;
            BackendExecutionToWorkerRef::StartExecution {
                session_epoch,
                plan,
                scans,
            }
        }
        BACKEND_EXECUTION_CANCEL_TAG => {
            BackendExecutionToWorkerRef::CancelExecution { session_epoch }
        }
        BACKEND_EXECUTION_FAIL_TAG => BackendExecutionToWorkerRef::FailExecution {
            session_epoch,
            code: ExecutionFailureCode::try_from(read_u8_from(&mut source)?)?,
            detail: read_optional_u64_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

/// Decode one worker-execution control message.
pub fn decode_worker_execution_to_backend(
    bytes: &[u8],
) -> Result<WorkerExecutionToBackend, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(
        header.family,
        RuntimeMessageFamily::WorkerExecutionToBackend,
    )?;

    let session_epoch = read_u64_from(&mut source)?;
    let message = match header.tag {
        WORKER_EXECUTION_COMPLETE_TAG => {
            WorkerExecutionToBackend::CompleteExecution { session_epoch }
        }
        WORKER_EXECUTION_FAIL_TAG => WorkerExecutionToBackend::FailExecution {
            session_epoch,
            code: ExecutionFailureCode::try_from(read_u8_from(&mut source)?)?,
            detail: read_optional_u64_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

/// Decode one borrowed worker-scan control message.
pub fn decode_worker_scan_to_backend(
    bytes: &[u8],
) -> Result<WorkerScanToBackendRef<'_>, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(header.family, RuntimeMessageFamily::WorkerScanToBackend)?;

    let session_epoch = read_u64_from(&mut source)?;
    let message = match header.tag {
        WORKER_SCAN_OPEN_TAG => {
            let scan_id = read_u64_from(&mut source)?;
            let page_kind = read_u16_from(&mut source)?;
            let page_flags = read_u16_from(&mut source)?;
            let producers = decode_producer_set_ref(original, &mut source)?;
            WorkerScanToBackendRef::OpenScan {
                session_epoch,
                scan_id,
                scan: ScanFlowDescriptorRef::new(page_kind, page_flags, producers),
            }
        }
        WORKER_SCAN_CANCEL_TAG => WorkerScanToBackendRef::CancelScan {
            session_epoch,
            scan_id: read_u64_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

/// Decode one borrowed backend-scan terminal message.
pub fn decode_backend_scan_to_worker(
    bytes: &[u8],
) -> Result<BackendScanToWorkerRef<'_>, DecodeError> {
    let original = bytes;
    let mut source = bytes;
    let header = decode_runtime_header(&mut source)?;
    expect_runtime_family(header.family, RuntimeMessageFamily::BackendScanToWorker)?;

    let session_epoch = read_u64_from(&mut source)?;
    let scan_id = read_u64_from(&mut source)?;
    let producer_id = read_u16_from(&mut source)?;
    let message = match header.tag {
        BACKEND_SCAN_FINISHED_TAG => BackendScanToWorkerRef::ScanFinished {
            session_epoch,
            scan_id,
            producer_id,
        },
        BACKEND_SCAN_FAILED_TAG => BackendScanToWorkerRef::ScanFailed {
            session_epoch,
            scan_id,
            producer_id,
            message: read_str_from(&mut source)?,
        },
        actual => return Err(DecodeError::UnexpectedTag { actual }),
    };

    ensure_no_trailing_bytes(original, source)?;
    Ok(message)
}

fn try_encoded_len_backend_execution_to_worker(
    message: BackendExecutionToWorker<'_>,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_backend_execution_to_worker_to(message, sink))
}

fn try_encoded_len_worker_execution_to_backend(
    message: WorkerExecutionToBackend,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_worker_execution_to_backend_to(message, sink))
}

fn try_encoded_len_worker_scan_to_backend(
    message: WorkerScanToBackend<'_>,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_worker_scan_to_backend_to(message, sink))
}

fn try_encoded_len_backend_scan_to_worker(
    message: BackendScanToWorker<'_>,
) -> Result<usize, EncodeError> {
    encoded_len_with(|sink| encode_backend_scan_to_worker_to(message, sink))
}

fn encode_backend_execution_to_worker_to<W: std::io::Write>(
    message: BackendExecutionToWorker<'_>,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        BackendExecutionToWorker::StartExecution {
            session_epoch,
            plan,
            scans,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendExecutionToWorker,
                BACKEND_EXECUTION_START_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, plan.plan_id)?;
            write_u16_to(sink, plan.page_kind)?;
            write_u16_to(sink, plan.page_flags)?;
            write_scan_channel_slice_to(sink, scans.channels())?;
        }
        BackendExecutionToWorker::CancelExecution { session_epoch } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendExecutionToWorker,
                BACKEND_EXECUTION_CANCEL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
        }
        BackendExecutionToWorker::FailExecution {
            session_epoch,
            code,
            detail,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendExecutionToWorker,
                BACKEND_EXECUTION_FAIL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u8_to(sink, code as u8)?;
            write_optional_u64_to(sink, detail)?;
        }
    }
    Ok(())
}

fn encode_worker_execution_to_backend_to<W: std::io::Write>(
    message: WorkerExecutionToBackend,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        WorkerExecutionToBackend::CompleteExecution { session_epoch } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerExecutionToBackend,
                WORKER_EXECUTION_COMPLETE_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
        }
        WorkerExecutionToBackend::FailExecution {
            session_epoch,
            code,
            detail,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerExecutionToBackend,
                WORKER_EXECUTION_FAIL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u8_to(sink, code as u8)?;
            write_optional_u64_to(sink, detail)?;
        }
    }
    Ok(())
}

fn encode_worker_scan_to_backend_to<W: std::io::Write>(
    message: WorkerScanToBackend<'_>,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        WorkerScanToBackend::OpenScan {
            session_epoch,
            scan_id,
            scan,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerScanToBackend,
                WORKER_SCAN_OPEN_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, scan_id)?;
            write_u16_to(sink, scan.page_kind)?;
            write_u16_to(sink, scan.page_flags)?;
            write_producer_slice_to(sink, scan.producers())?;
        }
        WorkerScanToBackend::CancelScan {
            session_epoch,
            scan_id,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::WorkerScanToBackend,
                WORKER_SCAN_CANCEL_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, scan_id)?;
        }
    }
    Ok(())
}

fn encode_backend_scan_to_worker_to<W: std::io::Write>(
    message: BackendScanToWorker<'_>,
    sink: &mut W,
) -> Result<(), EncodeError> {
    match message {
        BackendScanToWorker::ScanFinished {
            session_epoch,
            scan_id,
            producer_id,
        } => {
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendScanToWorker,
                BACKEND_SCAN_FINISHED_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, scan_id)?;
            write_u16_to(sink, producer_id)?;
        }
        BackendScanToWorker::ScanFailed {
            session_epoch,
            scan_id,
            producer_id,
            message,
        } => {
            if message.len() > crate::MAX_SCAN_FAILURE_MESSAGE_LEN {
                return Err(EncodeError::ScanFailureMessageTooLong {
                    actual: message.len(),
                    maximum: crate::MAX_SCAN_FAILURE_MESSAGE_LEN,
                });
            }
            write_runtime_header_to(
                sink,
                RuntimeMessageFamily::BackendScanToWorker,
                BACKEND_SCAN_FAILED_TAG,
            )?;
            write_u64_to(sink, session_epoch)?;
            write_u64_to(sink, scan_id)?;
            write_u16_to(sink, producer_id)?;
            write_str_to(sink, message)?;
        }
    }
    Ok(())
}

fn ensure_no_trailing_bytes(original: &[u8], remaining: &[u8]) -> Result<(), DecodeError> {
    if remaining.is_empty() {
        return Ok(());
    }
    let consumed = original.len() - remaining.len();
    let _ = consumed;
    Err(DecodeError::TrailingBytes {
        remaining: remaining.len(),
    })
}
