use crate::error::{AcquireError, IssuedTxError};
use crate::pool::{IssuancePool, PermitLease};
use crate::wire::{IssuedOwnedFrame, IssuedPageFrame};
use std::fmt;
use std::io::{self, Write};

/// Sender wrapper that acquires one issuance permit per outbound page.
#[derive(Clone)]
pub struct IssuedTx {
    tx: transfer::PageTx,
    permits: IssuancePool,
}

/// Permit-aware page writer returned by [`IssuedTx::begin`].
pub struct IssuedWriter {
    writer: transfer::PageWriter,
    permit: Option<PermitLease>,
    permits: IssuancePool,
}

/// Detached outbound page paired with its leased permit.
pub struct IssuedOutboundPage {
    outbound: Option<transfer::OutboundPage>,
    permits: IssuancePool,
    permit_id: u32,
    sent: bool,
}

impl fmt::Debug for IssuedOutboundPage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedOutboundPage")
            .field("transfer_id", &self.transfer_id())
            .field("permit_id", &self.permit_id)
            .field("sent", &self.sent)
            .finish()
    }
}

impl IssuedTx {
    /// Wrap an attached [`transfer::PageTx`] with permit acquisition.
    pub fn new(tx: transfer::PageTx, permits: IssuancePool) -> Self {
        Self { tx, permits }
    }

    /// Return the writable page payload capacity in bytes.
    pub fn payload_capacity(&self) -> usize {
        self.tx.payload_capacity()
    }

    /// Begin writing one page after acquiring a permit.
    ///
    /// If no permit is available, this returns
    /// [`IssuedTxError::NoPermits`] without touching the underlying
    /// [`transfer::PageTx`].
    pub fn begin(
        &self,
        kind: transfer::MessageKind,
        flags: u16,
    ) -> Result<IssuedWriter, IssuedTxError> {
        let permit = self.permits.try_acquire().map_err(|err| match err {
            AcquireError::Empty => IssuedTxError::NoPermits,
            other => IssuedTxError::Acquire(other),
        })?;
        let writer = match self.tx.begin(kind, flags) {
            Ok(writer) => writer,
            Err(err) => return Err(IssuedTxError::Tx(err)),
        };
        Ok(IssuedWriter {
            writer,
            permit: Some(permit),
            permits: self.permits,
        })
    }

    pub fn close(&self) -> Result<IssuedOwnedFrame, IssuedTxError> {
        match self.tx.close()? {
            transfer::OwnedFrame::Close(frame) => Ok(IssuedOwnedFrame::Close(frame)),
            transfer::OwnedFrame::Page(_) => unreachable!("PageTx::close must return close frame"),
        }
    }
}

impl IssuedWriter {
    /// Borrow the writable page payload.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.writer.payload_mut()
    }

    /// Remaining payload capacity in bytes.
    pub fn remaining(&self) -> usize {
        self.writer.remaining()
    }

    /// Current payload write position in bytes.
    pub fn position(&self) -> usize {
        self.writer.position()
    }

    /// Return the in-page message kind that will be written.
    pub fn kind(&self) -> transfer::MessageKind {
        self.writer.kind()
    }

    /// Return the in-page message flags that will be written.
    pub fn flags(&self) -> u16 {
        self.writer.flags()
    }

    /// Finish with the current write position as payload length.
    pub fn finish(self) -> Result<IssuedOutboundPage, IssuedTxError> {
        let payload_len = self.writer.position();
        self.finish_with_payload_len(payload_len)
    }

    /// Finish with an explicit payload length.
    pub fn finish_with_payload_len(
        mut self,
        payload_len: usize,
    ) -> Result<IssuedOutboundPage, IssuedTxError> {
        let outbound = self.writer.finish_with_payload_len(payload_len)?;
        let permit_id = self
            .permit
            .take()
            .expect("issued writer must hold a permit")
            .into_id();
        Ok(IssuedOutboundPage {
            outbound: Some(outbound),
            permits: self.permits,
            permit_id,
            sent: false,
        })
    }
}

impl Write for IssuedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)
    }
}

impl IssuedOutboundPage {
    /// Return the monotonic transfer id for this outbound page.
    pub fn transfer_id(&self) -> u64 {
        self.outbound
            .as_ref()
            .expect("outbound page must exist until mark_sent")
            .transfer_id()
    }

    /// Return the permit id currently attached to this outbound page.
    pub fn permit_id(&self) -> u32 {
        self.permit_id
    }

    /// Build the issued control frame describing this detached page.
    pub fn frame(&self) -> IssuedOwnedFrame {
        let outbound = self
            .outbound
            .as_ref()
            .expect("outbound page must exist until mark_sent");
        IssuedOwnedFrame::Page(IssuedPageFrame {
            pool_instance_id: self.permits.instance_id(),
            permit_id: self.permit_id,
            inner: transfer::PageFrame {
                transfer_id: outbound.transfer_id(),
                descriptor: outbound.descriptor(),
            },
        })
    }

    /// Mark the detached page as sent.
    ///
    /// After this call, the receiver side becomes responsible for releasing
    /// the permit along with the detached page.
    pub fn mark_sent(mut self) {
        let outbound = self
            .outbound
            .take()
            .expect("outbound page must exist until mark_sent");
        outbound.mark_sent();
        self.sent = true;
    }
}

impl Drop for IssuedOutboundPage {
    fn drop(&mut self) {
        if !self.sent {
            let outbound = self.outbound.take();
            drop(outbound);
            let _ = self.permits.release(self.permit_id);
        }
    }
}
