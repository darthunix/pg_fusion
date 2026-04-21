//! Shared-memory permit issuance for page-backed transfer.
//!
//! `issuance` layers a global non-blocking permit budget over [`transfer`].
//! A permit is acquired before a page handoff starts and is only returned when
//! the final received page owner is dropped or explicitly released.
//!
//! The crate intentionally does not provide fairness, wait queues, or retries.
//!
//! Typical flow:
//!
//! 1. Attach one shared [`IssuancePool`].
//! 2. Wrap sender and receiver as [`IssuedTx`] and [`IssuedRx`].
//! 3. Encode and ship [`IssuedOwnedFrame`] values instead of raw `transfer`
//!    frames.
//! 4. Keep the returned [`IssuedReceivedPage`] alive, or hand it to
//!    [`import::ArrowPageDecoder::import_owned`] so the permit remains leased
//!    for as long as page-backed Arrow buffers exist.
//!
//! Page frames also carry the sender's issuance-pool identity. Receivers reject
//! frames whose encoded pool id does not match their attached [`IssuancePool`]
//! before they touch the detached page or its permit.
//!
//! ```ignore
//! let issued_tx = issuance::IssuedTx::new(page_tx, permits);
//! let issued_rx = issuance::IssuedRx::new(page_rx, permits);
//!
//! let mut writer = issued_tx.begin(import::ARROW_LAYOUT_BATCH_KIND, 0)?;
//! // fill writer.payload_mut()
//! let outbound = writer.finish()?;
//! let wire = issuance::encode_issued_frame(outbound.frame())?;
//! outbound.mark_sent();
//!
//! let mut decoder = issuance::IssuedFrameDecoder::new();
//! let frame = decoder.push(&wire).next().transpose()?.unwrap();
//! let page = match issued_rx.accept(&frame)? {
//!     issuance::IssueEvent::Page(page) => page,
//!     issuance::IssueEvent::Closed => unreachable!(),
//! };
//!
//! let batch = import::ArrowPageDecoder::new(schema)?.import_owned(page)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod codec;
mod error;
mod pool;
mod rx;
mod tx;
mod wire;

#[cfg(test)]
mod tests;

pub use codec::{decode_issued_frame, encode_issued_frame, IssuedFrameDecoder, IssuedFrameIter};
pub use error::{
    AcquireError, AttachError, ConfigError, DecodeError, EncodeError, InitError, IssuedRxError,
    IssuedTxError, ReleaseError,
};
pub use pool::{IssuanceConfig, IssuancePool, IssuanceSnapshot, PermitLease, RegionLayout};
pub use rx::{IssueEvent, IssuedReceivedPage, IssuedRx};
pub use tx::{IssuedOutboundPage, IssuedTx, IssuedWriter};
pub use wire::{IssuedOwnedFrame, IssuedPageFrame, ISSUED_HEADER_LEN};
