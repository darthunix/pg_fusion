use crate::error::{DecodeError, EncodeError};
use crate::wire::{decode_owned_frame, encode_owned_frame, OwnedFrame, TRANSPORT_HEADER_LEN};

/// Encode one transport control frame into its fixed-size wire representation.
///
/// The returned byte array contains only the transport header. Message payload
/// bytes are stored in the shared page referenced by the frame's descriptor.
pub fn encode_frame(frame: OwnedFrame) -> Result<[u8; TRANSPORT_HEADER_LEN], EncodeError> {
    let mut buf = [0u8; TRANSPORT_HEADER_LEN];
    let written = encode_owned_frame(frame, &mut buf)?;
    if written != TRANSPORT_HEADER_LEN {
        return Err(EncodeError::HeaderEncodeFailed {
            expected: TRANSPORT_HEADER_LEN,
            written,
        });
    }
    Ok(buf)
}

/// Incremental decoder for fixed-size transport frames.
///
/// `FrameDecoder` keeps at most one partial header in a fixed scratch buffer.
/// Use [`FrameDecoder::push`] with each chunk received from a carrier and
/// iterate the returned [`FrameIter`] to extract any complete frames from that
/// chunk.
pub struct FrameDecoder {
    header: [u8; TRANSPORT_HEADER_LEN],
    filled: usize,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self {
            header: [0u8; TRANSPORT_HEADER_LEN],
            filled: 0,
        }
    }
}

/// Zero-allocation iterator over decoded frames from one `push()` call.
///
/// The iterator borrows the decoder mutably for its lifetime. Drop it before
/// calling [`FrameDecoder::push`] again.
pub struct FrameIter<'decoder, 'input> {
    decoder: &'decoder mut FrameDecoder,
    input: &'input [u8],
    done: bool,
}

impl FrameDecoder {
    /// Create an empty decoder with no buffered partial frame.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one carrier chunk into the decoder and iterate over any completed
    /// frames in stream order.
    pub fn push<'input>(&mut self, bytes: &'input [u8]) -> FrameIter<'_, 'input> {
        FrameIter {
            decoder: self,
            input: bytes,
            done: false,
        }
    }

    /// Return how many bytes of the next frame header are currently buffered.
    pub fn buffered_len(&self) -> usize {
        self.filled
    }

    fn decode_next(&mut self, input: &mut &[u8]) -> Result<Option<OwnedFrame>, DecodeError> {
        if self.filled == 0 && input.len() >= TRANSPORT_HEADER_LEN {
            let (frame_bytes, rest) = input.split_at(TRANSPORT_HEADER_LEN);
            *input = rest;
            return decode_owned_frame(frame_bytes).map(Some);
        }

        if input.is_empty() {
            return Ok(None);
        }

        let needed = TRANSPORT_HEADER_LEN - self.filled;
        let copied = needed.min(input.len());
        self.header[self.filled..self.filled + copied].copy_from_slice(&input[..copied]);
        self.filled += copied;
        *input = &input[copied..];

        if self.filled < TRANSPORT_HEADER_LEN {
            return Ok(None);
        }

        let result = decode_owned_frame(&self.header);
        self.filled = 0;
        result.map(Some)
    }
}

impl Iterator for FrameIter<'_, '_> {
    type Item = Result<OwnedFrame, DecodeError>;

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
