//! Sans-IO page handoff protocol and page message wrappers over `pool`.
//!
//! `transfer` does not move page bytes through the transport. Instead, it
//! writes a message into a shared-memory page acquired from [`pool`] and
//! then transfers ownership of that page between processes with a small control
//! frame. The control frame is transport-agnostic and can be sent over a
//! socket, pipe, or shared-memory queue.
//!
//! V1 intentionally keeps the model simple:
//!
//! - one `PageTx` talks to one `PageRx`
//! - one sender-side writer or unsent outbound page may exist at a time
//! - receiver accepts frames sequentially, but accepted pages may outlive the
//!   receiver state and not block later accepts
//! - the carrier is assumed reliable and ordered
//! - received pages are read-only
//! - there are no acks, retries, or credits
//!
//! Page messages reserve a fixed-size RMP header at the front of the page, then
//! either append payload bytes through [`std::io::Write`] or fill the payload
//! slice returned by [`PageWriter::payload_mut`]. The payload length is encoded
//! into the in-page header only when [`PageWriter::finish`] or
//! [`PageWriter::finish_with_payload_len`] is called.
//!
//! The crate remains runtime-agnostic:
//!
//! - `PageTx` and `PageRx` are synchronous handles backed by atomics
//! - transport I/O stays outside the crate
//! - the owned wrappers may be moved across threads or held across `.await`
//!   points in an async runtime
//!
//! Typical send/receive flow:
//!
//! 1. Create a [`PageWriter`] with [`PageTx::begin`].
//! 2. Append bytes into the page through [`std::io::Write`] or write directly
//!    into [`PageWriter::payload_mut`].
//! 3. Turn the page into an [`OutboundPage`] with [`PageWriter::finish`] or
//!    [`PageWriter::finish_with_payload_len`].
//! 4. Encode the control frame with [`encode_frame`] or feed bytes through
//!    [`FrameDecoder`].
//! 5. Accept the frame on the other side with [`PageRx::accept`].
//! 6. Read the page payload through [`ReceivedPage::payload`] and release it
//!    later with [`ReceivedPage::release`]. Holding the page does not block
//!    subsequent [`PageRx::accept`] calls.

mod codec;
mod error;
mod page;
mod rx;
mod tx;
mod wire;

#[cfg(test)]
mod tests;

pub use codec::{encode_frame, FrameDecoder, FrameIter};
pub use error::{DecodeError, EncodeError, InvalidPageError, RxError, TxError};
pub use page::MessageKind;
pub use rx::{PageRx, ReceiveEvent, ReceivedPage};
pub use tx::{OutboundPage, PageTx, PageWriter};
pub use wire::{CloseFrame, FrameKind, OwnedFrame, PageFrame};
