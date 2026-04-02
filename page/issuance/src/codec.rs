use crate::error::{DecodeError, EncodeError};
use crate::wire::{
    decode_issued_owned_frame, encode_issued_owned_frame, IssuedOwnedFrame, ISSUED_HEADER_LEN,
};

/// Encode one issued frame into its fixed-size transport header.
pub fn encode_issued_frame(
    frame: IssuedOwnedFrame,
) -> Result<[u8; ISSUED_HEADER_LEN], EncodeError> {
    let mut buf = [0u8; ISSUED_HEADER_LEN];
    let written = encode_issued_owned_frame(frame, &mut buf)?;
    if written != ISSUED_HEADER_LEN {
        return Err(EncodeError::HeaderEncodeFailed {
            expected: ISSUED_HEADER_LEN,
            written,
        });
    }
    Ok(buf)
}

pub struct IssuedFrameDecoder {
    header: [u8; ISSUED_HEADER_LEN],
    filled: usize,
}

impl Default for IssuedFrameDecoder {
    fn default() -> Self {
        Self {
            header: [0u8; ISSUED_HEADER_LEN],
            filled: 0,
        }
    }
}

/// Iterator over zero or more issued frames decoded from one input chunk.
pub struct IssuedFrameIter<'decoder, 'input> {
    decoder: &'decoder mut IssuedFrameDecoder,
    input: &'input [u8],
    done: bool,
}

impl IssuedFrameDecoder {
    /// Create a fresh decoder for fixed-size issued headers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one input chunk into the decoder.
    pub fn push<'input>(&mut self, bytes: &'input [u8]) -> IssuedFrameIter<'_, 'input> {
        IssuedFrameIter {
            decoder: self,
            input: bytes,
            done: false,
        }
    }

    fn decode_next(&mut self, input: &mut &[u8]) -> Result<Option<IssuedOwnedFrame>, DecodeError> {
        if self.filled == 0 && input.len() >= ISSUED_HEADER_LEN {
            let (frame_bytes, rest) = input.split_at(ISSUED_HEADER_LEN);
            *input = rest;
            return decode_issued_owned_frame(frame_bytes).map(Some);
        }

        if input.is_empty() {
            return Ok(None);
        }

        let needed = ISSUED_HEADER_LEN - self.filled;
        let copied = needed.min(input.len());
        self.header[self.filled..self.filled + copied].copy_from_slice(&input[..copied]);
        self.filled += copied;
        *input = &input[copied..];

        if self.filled < ISSUED_HEADER_LEN {
            return Ok(None);
        }

        let result = decode_issued_owned_frame(&self.header);
        self.filled = 0;
        result.map(Some)
    }
}

impl Iterator for IssuedFrameIter<'_, '_> {
    type Item = Result<IssuedOwnedFrame, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.decoder.decode_next(&mut self.input) {
            Ok(Some(frame)) => Some(Ok(frame)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }
}
