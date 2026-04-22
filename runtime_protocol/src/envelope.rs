use crate::error::{DecodeError, EncodeError};
use crate::message::RuntimeMessageFamily;
use std::io::Write;

const RUNTIME_PROTOCOL_MAGIC: u32 = 0x5046_5232;
const RUNTIME_PROTOCOL_VERSION: u16 = 3;
pub const RUNTIME_ENVELOPE_HEADER_LEN: usize = 8;

pub(crate) const BACKEND_EXECUTION_START_TAG: u8 = 1;
pub(crate) const BACKEND_EXECUTION_CANCEL_TAG: u8 = 2;
pub(crate) const BACKEND_EXECUTION_FAIL_TAG: u8 = 3;

pub(crate) const WORKER_EXECUTION_COMPLETE_TAG: u8 = 1;
pub(crate) const WORKER_EXECUTION_FAIL_TAG: u8 = 2;
pub(crate) const WORKER_SCAN_OPEN_TAG: u8 = 1;
pub(crate) const WORKER_SCAN_CANCEL_TAG: u8 = 2;
pub(crate) const BACKEND_SCAN_FINISHED_TAG: u8 = 1;
pub(crate) const BACKEND_SCAN_FAILED_TAG: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeEnvelopeHeader {
    pub(crate) family: RuntimeMessageFamily,
    pub(crate) tag: u8,
}

pub(crate) fn decode_runtime_header(
    source: &mut &[u8],
) -> Result<RuntimeEnvelopeHeader, DecodeError> {
    if source.len() < RUNTIME_ENVELOPE_HEADER_LEN {
        return Err(DecodeError::TruncatedEnvelope {
            expected: RUNTIME_ENVELOPE_HEADER_LEN,
            actual: source.len(),
        });
    }

    let (header, tail) = source.split_at(RUNTIME_ENVELOPE_HEADER_LEN);
    *source = tail;

    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != RUNTIME_PROTOCOL_MAGIC {
        return Err(DecodeError::InvalidMagic {
            expected: RUNTIME_PROTOCOL_MAGIC,
            actual: magic,
        });
    }

    let version = u16::from_be_bytes([header[4], header[5]]);
    if version != RUNTIME_PROTOCOL_VERSION {
        return Err(DecodeError::UnsupportedVersion {
            expected: RUNTIME_PROTOCOL_VERSION,
            actual: version,
        });
    }

    Ok(RuntimeEnvelopeHeader {
        family: RuntimeMessageFamily::try_from(header[6])?,
        tag: header[7],
    })
}

pub(crate) fn write_runtime_header_to<W: Write>(
    sink: &mut W,
    family: RuntimeMessageFamily,
    tag: u8,
) -> Result<(), EncodeError> {
    sink.write_all(&RUNTIME_PROTOCOL_MAGIC.to_be_bytes())
        .map_err(|error| EncodeError::Envelope(error.to_string()))?;
    sink.write_all(&RUNTIME_PROTOCOL_VERSION.to_be_bytes())
        .map_err(|error| EncodeError::Envelope(error.to_string()))?;
    sink.write_all(&[family as u8, tag])
        .map_err(|error| EncodeError::Envelope(error.to_string()))?;
    Ok(())
}

pub(crate) fn expect_runtime_family(
    actual: RuntimeMessageFamily,
    expected: RuntimeMessageFamily,
) -> Result<(), DecodeError> {
    if actual == expected {
        Ok(())
    } else {
        Err(DecodeError::UnexpectedMessageFamily {
            actual: actual as u8,
        })
    }
}
