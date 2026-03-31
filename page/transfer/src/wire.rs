use crate::error::{DecodeError, EncodeError};
use pool::PageDescriptor;
use rmp::decode::{read_array_len, read_u16, read_u32, read_u64, read_u8};
use rmp::encode::{write_array_len, write_u16, write_u32, write_u64, write_u8};
use std::io::Cursor;

pub(crate) const TRANSPORT_MAGIC: u32 = 0x5054_4631;
pub(crate) const TRANSPORT_VERSION: u16 = 1;
pub(crate) const TRANSPORT_FLAGS_V1: u8 = 0;
pub(crate) const TRANSPORT_HEADER_ARRAY_LEN: u32 = 8;
/// Fixed encoded size of one transport control frame in bytes.
pub const TRANSPORT_HEADER_LEN: usize = 45;

/// Transport-level control frame kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Page = 1,
    Close = 2,
}

impl FrameKind {
    pub(crate) fn from_u8(value: u8) -> Result<Self, DecodeError> {
        match value {
            1 => Ok(Self::Page),
            2 => Ok(Self::Close),
            actual => Err(DecodeError::InvalidFrameKind { actual }),
        }
    }
}

/// Control frame that transfers ownership of one detached page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageFrame {
    /// Monotonic sender-assigned transfer sequence number.
    pub transfer_id: u64,
    /// Detached page descriptor handed to the receiver.
    pub descriptor: PageDescriptor,
}

/// Terminal control frame indicating that no further page frames will follow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseFrame {
    /// Monotonic sender-assigned transfer sequence number.
    pub transfer_id: u64,
}

/// Owned transport frame decoded from or ready for the carrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedFrame {
    Page(PageFrame),
    Close(CloseFrame),
}

impl OwnedFrame {
    /// Return the frame's transfer sequence number.
    pub fn transfer_id(&self) -> u64 {
        match self {
            Self::Page(frame) => frame.transfer_id,
            Self::Close(frame) => frame.transfer_id,
        }
    }

    /// Return the frame's discriminant.
    pub fn kind(&self) -> FrameKind {
        match self {
            Self::Page(_) => FrameKind::Page,
            Self::Close(_) => FrameKind::Close,
        }
    }
}

pub(crate) fn encode_owned_frame(frame: OwnedFrame, out: &mut [u8]) -> Result<usize, EncodeError> {
    if out.len() < TRANSPORT_HEADER_LEN {
        return Err(EncodeError::BufferTooSmall {
            expected: TRANSPORT_HEADER_LEN,
            actual: out.len(),
        });
    }

    let mut cursor = &mut out[..TRANSPORT_HEADER_LEN];
    let (frame_kind, transfer_id, descriptor) = match frame {
        OwnedFrame::Page(frame) => (FrameKind::Page, frame.transfer_id, frame.descriptor),
        OwnedFrame::Close(frame) => (
            FrameKind::Close,
            frame.transfer_id,
            PageDescriptor {
                pool_id: 0,
                page_id: 0,
                generation: 0,
            },
        ),
    };

    write_array_len(&mut cursor, TRANSPORT_HEADER_ARRAY_LEN)
        .map_err(|_| header_len_error(cursor.len()))?;
    write_u32(&mut cursor, TRANSPORT_MAGIC).map_err(|_| header_len_error(cursor.len()))?;
    write_u16(&mut cursor, TRANSPORT_VERSION).map_err(|_| header_len_error(cursor.len()))?;
    write_u8(&mut cursor, frame_kind as u8).map_err(|_| header_len_error(cursor.len()))?;
    write_u8(&mut cursor, TRANSPORT_FLAGS_V1).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, transfer_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, descriptor.pool_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u32(&mut cursor, descriptor.page_id).map_err(|_| header_len_error(cursor.len()))?;
    write_u64(&mut cursor, descriptor.generation).map_err(|_| header_len_error(cursor.len()))?;

    if !cursor.is_empty() {
        return Err(header_len_error(cursor.len()));
    }

    Ok(TRANSPORT_HEADER_LEN)
}

pub(crate) fn decode_owned_frame(bytes: &[u8]) -> Result<OwnedFrame, DecodeError> {
    let mut cursor = Cursor::new(bytes);
    let array_len = read_array_len(&mut cursor).map_err(|_| DecodeError::BadArrayLen {
        expected: TRANSPORT_HEADER_ARRAY_LEN,
        actual: 0,
    })?;
    if array_len != TRANSPORT_HEADER_ARRAY_LEN {
        return Err(DecodeError::BadArrayLen {
            expected: TRANSPORT_HEADER_ARRAY_LEN,
            actual: array_len,
        });
    }

    let magic = read_u32(&mut cursor).map_err(|_| DecodeError::InvalidMagic {
        expected: TRANSPORT_MAGIC,
        actual: 0,
    })?;
    if magic != TRANSPORT_MAGIC {
        return Err(DecodeError::InvalidMagic {
            expected: TRANSPORT_MAGIC,
            actual: magic,
        });
    }

    let version = read_u16(&mut cursor).map_err(|_| DecodeError::UnsupportedVersion {
        expected: TRANSPORT_VERSION,
        actual: 0,
    })?;
    if version != TRANSPORT_VERSION {
        return Err(DecodeError::UnsupportedVersion {
            expected: TRANSPORT_VERSION,
            actual: version,
        });
    }

    let kind = FrameKind::from_u8(
        read_u8(&mut cursor).map_err(|_| DecodeError::InvalidFrameKind { actual: 0 })?,
    )?;

    let flags = read_u8(&mut cursor).map_err(|_| DecodeError::NonZeroFlags { actual: 0 })?;
    if flags != TRANSPORT_FLAGS_V1 {
        return Err(DecodeError::NonZeroFlags { actual: flags });
    }

    let transfer_id = read_u64(&mut cursor).map_err(|_| DecodeError::TrailingBytes {
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
        FrameKind::Page => Ok(OwnedFrame::Page(PageFrame {
            transfer_id,
            descriptor: PageDescriptor {
                pool_id,
                page_id,
                generation,
            },
        })),
        FrameKind::Close => {
            if pool_id != 0 || page_id != 0 || generation != 0 {
                return Err(DecodeError::NonZeroCloseDescriptor);
            }
            Ok(OwnedFrame::Close(CloseFrame { transfer_id }))
        }
    }
}

fn header_len_error(remaining: usize) -> EncodeError {
    EncodeError::HeaderEncodeFailed {
        expected: TRANSPORT_HEADER_LEN,
        written: TRANSPORT_HEADER_LEN - remaining,
    }
}
