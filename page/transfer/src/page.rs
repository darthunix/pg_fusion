use crate::error::{EncodeError, InvalidPageError};
use rmp::decode::{read_array_len, read_u16, read_u32};
use rmp::encode::{write_array_len, write_u16, write_u32};
use std::io::Cursor;

pub type MessageKind = u16;

pub(crate) const PAGE_MAGIC: u32 = 0x5054_5031;
pub(crate) const PAGE_VERSION: u16 = 1;
pub(crate) const PAGE_HEADER_ARRAY_LEN: u32 = 5;
pub(crate) const PAGE_HEADER_LEN: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PageHeader {
    pub(crate) kind: MessageKind,
    pub(crate) flags: u16,
    pub(crate) payload_len: u32,
}

pub(crate) fn encode_page_header(header: PageHeader, out: &mut [u8]) -> Result<(), EncodeError> {
    if out.len() < PAGE_HEADER_LEN {
        return Err(EncodeError::BufferTooSmall {
            expected: PAGE_HEADER_LEN,
            actual: out.len(),
        });
    }

    let mut cursor = &mut out[..PAGE_HEADER_LEN];
    write_array_len(&mut cursor, PAGE_HEADER_ARRAY_LEN).map_err(|_| {
        EncodeError::HeaderEncodeFailed {
            expected: PAGE_HEADER_LEN,
            written: PAGE_HEADER_LEN - cursor.len(),
        }
    })?;
    write_u32(&mut cursor, PAGE_MAGIC).map_err(|_| EncodeError::HeaderEncodeFailed {
        expected: PAGE_HEADER_LEN,
        written: PAGE_HEADER_LEN - cursor.len(),
    })?;
    write_u16(&mut cursor, PAGE_VERSION).map_err(|_| EncodeError::HeaderEncodeFailed {
        expected: PAGE_HEADER_LEN,
        written: PAGE_HEADER_LEN - cursor.len(),
    })?;
    write_u16(&mut cursor, header.kind).map_err(|_| EncodeError::HeaderEncodeFailed {
        expected: PAGE_HEADER_LEN,
        written: PAGE_HEADER_LEN - cursor.len(),
    })?;
    write_u16(&mut cursor, header.flags).map_err(|_| EncodeError::HeaderEncodeFailed {
        expected: PAGE_HEADER_LEN,
        written: PAGE_HEADER_LEN - cursor.len(),
    })?;
    write_u32(&mut cursor, header.payload_len).map_err(|_| EncodeError::HeaderEncodeFailed {
        expected: PAGE_HEADER_LEN,
        written: PAGE_HEADER_LEN - cursor.len(),
    })?;

    if !cursor.is_empty() {
        return Err(EncodeError::HeaderEncodeFailed {
            expected: PAGE_HEADER_LEN,
            written: PAGE_HEADER_LEN - cursor.len(),
        });
    }

    Ok(())
}

pub(crate) fn decode_page_header(bytes: &[u8]) -> Result<PageHeader, InvalidPageError> {
    let mut cursor = Cursor::new(bytes);
    let array_len = read_array_len(&mut cursor).map_err(|_| InvalidPageError::BadArrayLen {
        expected: PAGE_HEADER_ARRAY_LEN,
        actual: 0,
    })?;
    if array_len != PAGE_HEADER_ARRAY_LEN {
        return Err(InvalidPageError::BadArrayLen {
            expected: PAGE_HEADER_ARRAY_LEN,
            actual: array_len,
        });
    }

    let magic = read_u32(&mut cursor).map_err(|_| InvalidPageError::InvalidMagic {
        expected: PAGE_MAGIC,
        actual: 0,
    })?;
    if magic != PAGE_MAGIC {
        return Err(InvalidPageError::InvalidMagic {
            expected: PAGE_MAGIC,
            actual: magic,
        });
    }

    let version = read_u16(&mut cursor).map_err(|_| InvalidPageError::UnsupportedVersion {
        expected: PAGE_VERSION,
        actual: 0,
    })?;
    if version != PAGE_VERSION {
        return Err(InvalidPageError::UnsupportedVersion {
            expected: PAGE_VERSION,
            actual: version,
        });
    }

    let kind = read_u16(&mut cursor).map_err(|_| InvalidPageError::BadArrayLen {
        expected: PAGE_HEADER_ARRAY_LEN,
        actual: array_len,
    })?;
    let flags = read_u16(&mut cursor).map_err(|_| InvalidPageError::BadArrayLen {
        expected: PAGE_HEADER_ARRAY_LEN,
        actual: array_len,
    })?;
    let payload_len = read_u32(&mut cursor).map_err(|_| InvalidPageError::BadArrayLen {
        expected: PAGE_HEADER_ARRAY_LEN,
        actual: array_len,
    })?;

    if cursor.position() as usize != bytes.len() {
        return Err(InvalidPageError::TrailingBytes {
            expected: cursor.position() as usize,
            actual: bytes.len(),
        });
    }

    Ok(PageHeader {
        kind,
        flags,
        payload_len,
    })
}
