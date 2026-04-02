use crate::error::{DecodeError, EncodeError};
use rmp::decode::{read_array_len, read_u16, read_u32, read_u64, read_u8};
use rmp::encode::{write_array_len, write_u16, write_u32, write_u64, write_u8};
use std::io::Cursor;

pub(crate) const ISSUED_TRANSPORT_MAGIC: u32 = 0x5049_4631;
pub(crate) const ISSUED_TRANSPORT_VERSION: u16 = 1;
pub(crate) const ISSUED_TRANSPORT_FLAGS_V1: u8 = 0;
pub(crate) const ISSUED_HEADER_ARRAY_LEN: u32 = 10;
pub const ISSUED_HEADER_LEN: usize = 59;

/// Issuance-aware transport frame kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IssuedFrameKind {
    Page = 1,
    Close = 2,
}

impl IssuedFrameKind {
    pub(crate) fn from_u8(value: u8) -> Result<Self, DecodeError> {
        match value {
            1 => Ok(Self::Page),
            2 => Ok(Self::Close),
            actual => Err(DecodeError::InvalidFrameKind { actual }),
        }
    }
}

/// Page control frame paired with an issuance permit id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IssuedPageFrame {
    /// Shared issuance-pool instance identity recorded by the sender.
    ///
    /// Receivers must match this against their attached [`crate::IssuancePool`]
    /// before they accept the detached page.
    pub pool_instance_id: u64,
    /// Permit id scoped within `pool_instance_id`.
    pub permit_id: u32,
    pub inner: transfer::PageFrame,
}

/// Owned issuance transport frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IssuedOwnedFrame {
    Page(IssuedPageFrame),
    Close(transfer::CloseFrame),
}

impl IssuedOwnedFrame {
    /// Return the monotonic transfer sequence number carried by this frame.
    pub fn transfer_id(&self) -> u64 {
        match self {
            Self::Page(frame) => frame.inner.transfer_id,
            Self::Close(frame) => frame.transfer_id,
        }
    }
}

pub(crate) fn encode_issued_owned_frame(
    frame: IssuedOwnedFrame,
    out: &mut [u8],
) -> Result<usize, EncodeError> {
    if out.len() < ISSUED_HEADER_LEN {
        return Err(EncodeError::BufferTooSmall {
            expected: ISSUED_HEADER_LEN,
            actual: out.len(),
        });
    }

    let mut cursor = &mut out[..ISSUED_HEADER_LEN];
    let (frame_kind, transfer_id, pool_instance_id, permit_id, descriptor) = match frame {
        IssuedOwnedFrame::Page(frame) => (
            IssuedFrameKind::Page,
            frame.inner.transfer_id,
            frame.pool_instance_id,
            frame.permit_id,
            frame.inner.descriptor,
        ),
        IssuedOwnedFrame::Close(frame) => (
            IssuedFrameKind::Close,
            frame.transfer_id,
            0,
            0,
            pool::PageDescriptor {
                pool_id: 0,
                page_id: 0,
                generation: 0,
            },
        ),
    };

    write_array_len(&mut cursor, ISSUED_HEADER_ARRAY_LEN)
        .map_err(|_| header_len_error(cursor.len()))?;
    write_u32(&mut cursor, ISSUED_TRANSPORT_MAGIC).map_err(|_| header_len_error(cursor.len()))?;
    write_u16(&mut cursor, ISSUED_TRANSPORT_VERSION).map_err(|_| header_len_error(cursor.len()))?;
    write_u8(&mut cursor, frame_kind as u8).map_err(|_| header_len_error(cursor.len()))?;
    write_u8(&mut cursor, ISSUED_TRANSPORT_FLAGS_V1).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, transfer_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, pool_instance_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u32(&mut cursor, permit_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, descriptor.pool_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u32(&mut cursor, descriptor.page_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, descriptor.generation).map_err(|_| header_len_error(cursor.len()))?;

    if !cursor.is_empty() {
        return Err(header_len_error(cursor.len()));
    }
    Ok(ISSUED_HEADER_LEN)
}

pub(crate) fn decode_issued_owned_frame(bytes: &[u8]) -> Result<IssuedOwnedFrame, DecodeError> {
    let mut cursor = Cursor::new(bytes);
    let array_len = read_array_len(&mut cursor).map_err(|_| DecodeError::BadArrayLen {
        expected: ISSUED_HEADER_ARRAY_LEN,
        actual: 0,
    })?;
    if array_len != ISSUED_HEADER_ARRAY_LEN {
        return Err(DecodeError::BadArrayLen {
            expected: ISSUED_HEADER_ARRAY_LEN,
            actual: array_len,
        });
    }

    let magic = read_u32(&mut cursor).map_err(|_| DecodeError::InvalidMagic {
        expected: ISSUED_TRANSPORT_MAGIC,
        actual: 0,
    })?;
    if magic != ISSUED_TRANSPORT_MAGIC {
        return Err(DecodeError::InvalidMagic {
            expected: ISSUED_TRANSPORT_MAGIC,
            actual: magic,
        });
    }

    let version = read_u16(&mut cursor).map_err(|_| DecodeError::UnsupportedVersion {
        expected: ISSUED_TRANSPORT_VERSION,
        actual: 0,
    })?;
    if version != ISSUED_TRANSPORT_VERSION {
        return Err(DecodeError::UnsupportedVersion {
            expected: ISSUED_TRANSPORT_VERSION,
            actual: version,
        });
    }

    let kind = IssuedFrameKind::from_u8(
        read_u8(&mut cursor).map_err(|_| DecodeError::InvalidFrameKind { actual: 0 })?,
    )?;
    let flags = read_u8(&mut cursor).map_err(|_| DecodeError::NonZeroFlags { actual: 0 })?;
    if flags != ISSUED_TRANSPORT_FLAGS_V1 {
        return Err(DecodeError::NonZeroFlags { actual: flags });
    }

    let transfer_id = read_u64(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
        expected: 0,
        actual: 0,
    })?;
    let pool_instance_id = read_u64(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
        expected: 0,
        actual: 0,
    })?;
    let permit_id = read_u32(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
        expected: 0,
        actual: 0,
    })?;
    let pool_id = read_u64(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
        expected: 0,
        actual: 0,
    })?;
    let page_id = read_u32(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
        expected: 0,
        actual: 0,
    })?;
    let generation = read_u64(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
        expected: 0,
        actual: 0,
    })?;

    if cursor.position() as usize != bytes.len() {
        return Err(DecodeError::TrailingBytes {
            expected: cursor.position() as usize,
            actual: bytes.len(),
        });
    }

    match kind {
        IssuedFrameKind::Page => Ok(IssuedOwnedFrame::Page(IssuedPageFrame {
            pool_instance_id,
            permit_id,
            inner: transfer::PageFrame {
                transfer_id,
                descriptor: pool::PageDescriptor {
                    pool_id,
                    page_id,
                    generation,
                },
            },
        })),
        IssuedFrameKind::Close => {
            if pool_instance_id != 0
                || permit_id != 0
                || pool_id != 0
                || page_id != 0
                || generation != 0
            {
                return Err(DecodeError::NonZeroCloseFields);
            }
            Ok(IssuedOwnedFrame::Close(transfer::CloseFrame {
                transfer_id,
            }))
        }
    }
}

fn header_len_error(remaining: usize) -> EncodeError {
    EncodeError::HeaderEncodeFailed {
        expected: ISSUED_HEADER_LEN,
        written: ISSUED_HEADER_LEN - remaining,
    }
}
